// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::slice::Iter;

use super::{EvalContext, Result, ScalarFunc};
use coprocessor::codec::Datum;
use regex::{bytes::Regex as BytesRegex, Regex};

const MAX_RECURSE_LEVEL: usize = 1024;

impl ScalarFunc {
    /// NOTE: LIKE compare target with pattern as bytes, even if they have different
    /// charsets. This behaviour is for keeping compatible with TiDB. But MySQL
    /// compare them as bytes only if any charset of pattern or target is binary,
    /// otherwise MySQL will compare decoded string.
    pub fn like(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let target = try_opt!(self.children[0].eval_string(ctx, row));
        let pattern = try_opt!(self.children[1].eval_string(ctx, row));
        let escape = try_opt!(self.children[2].eval_int(ctx, row)) as u32;
        Ok(Some(like(&target, &pattern, escape, 0)? as i64))
    }

    pub fn regexp(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let target = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        let pattern = try_opt!(self.children[1].eval_string_and_decode(ctx, row));
        let pattern = format!("(?i){}", &pattern);

        // TODO: cache compiled result
        Ok(Some(Regex::new(&pattern)?.is_match(&target) as i64))
    }

    pub fn regexp_binary(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let target = try_opt!(self.children[0].eval_string(ctx, row));
        let pattern = try_opt!(self.children[1].eval_string_and_decode(ctx, row));

        // TODO: cache compiled result
        Ok(Some(BytesRegex::new(&pattern)?.is_match(&target) as i64))
    }
}

// Do match until '%' is found.
#[inline]
fn partial_like(tcs: &mut Iter<u8>, pcs: &mut Iter<u8>, escape: u32) -> Option<bool> {
    loop {
        match pcs.next().cloned() {
            None => return Some(tcs.next().is_none()),
            Some(b'%') => return None,
            Some(c) => {
                let (npc, escape) = if u32::from(c) == escape {
                    pcs.next().map_or((c, false), |&c| (c, true))
                } else {
                    (c, false)
                };
                let nsc = match tcs.next() {
                    None => return Some(false),
                    Some(&c) => c,
                };
                if nsc != npc && (npc != b'_' || escape) {
                    return Some(false);
                }
            }
        }
    }
}

fn like(target: &[u8], pattern: &[u8], escape: u32, recurse_level: usize) -> Result<bool> {
    let mut tcs = target.iter();
    let mut pcs = pattern.iter();
    loop {
        if let Some(res) = partial_like(&mut tcs, &mut pcs, escape) {
            return Ok(res);
        }
        let next_char = loop {
            match pcs.next().cloned() {
                Some(b'%') => {}
                Some(b'_') => if tcs.next().is_none() {
                    return Ok(false);
                },
                // So the pattern should be some thing like 'xxx%'
                None => return Ok(true),
                Some(c) => {
                    break if u32::from(c) == escape {
                        pcs.next().map_or(escape, |&c| u32::from(c))
                    } else {
                        u32::from(c)
                    };
                }
            }
        };
        if recurse_level >= MAX_RECURSE_LEVEL {
            // TODO: maybe we should test if stack is actually about to overflow.
            return Err(box_err!(
                "recurse level should not be larger than {}",
                MAX_RECURSE_LEVEL
            ));
        }
        // Pattern must be something like "%xxx".
        loop {
            let s = match tcs.next() {
                None => return Ok(false),
                Some(&s) => u32::from(s),
            };
            if s == next_char && like(tcs.as_slice(), pcs.as_slice(), escape, recurse_level + 1)? {
                return Ok(true);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use coprocessor::codec::Datum;
    use coprocessor::dag::expr::tests::{datum_expr, scalar_func_expr};
    use coprocessor::dag::expr::{EvalContext, Expression};
    use tipb::expression::ScalarFuncSig;

    #[test]
    fn test_like() {
        let cases = vec![
            (r#"hello"#, r#"%HELLO%"#, '\\', false),
            (r#"Hello, World"#, r#"Hello, World"#, '\\', true),
            (r#"Hello, World"#, r#"Hello, %"#, '\\', true),
            (r#"Hello, World"#, r#"%, World"#, '\\', true),
            (r#"test"#, r#"te%st"#, '\\', true),
            (r#"test"#, r#"te%%st"#, '\\', true),
            (r#"test"#, r#"test%"#, '\\', true),
            (r#"test"#, r#"%test%"#, '\\', true),
            (r#"test"#, r#"t%e%s%t"#, '\\', true),
            (r#"test"#, r#"_%_%_%_"#, '\\', true),
            (r#"test"#, r#"_%_%st"#, '\\', true),
            (r#"C:"#, r#"%\"#, '\\', false),
            (r#"C:\"#, r#"%\"#, '\\', true),
            (r#"C:\Programs"#, r#"%\"#, '\\', false),
            (r#"C:\Programs\"#, r#"%\"#, '\\', true),
            (r#"C:"#, r#"%\\"#, '\\', false),
            (r#"C:\"#, r#"%\\"#, '\\', true),
            (r#"C:\Programs"#, r#"%\\"#, '\\', false),
            (r#"C:\Programs\"#, r#"%\\"#, '\\', true),
            (r#"C:\Programs\"#, r#"%Prog%"#, '\\', true),
            (r#"C:\Programs\"#, r#"%Pr_g%"#, '\\', true),
            (r#"C:\Programs\"#, r#"%%\"#, '%', true),
            (r#"C:\Programs%"#, r#"%%%"#, '%', true),
            (r#"C:\Programs%"#, r#"%%%%"#, '%', true),
            (r#"hello"#, r#"\%"#, '\\', false),
            (r#"%"#, r#"\%"#, '\\', true),
            (r#"3hello"#, r#"%%hello"#, '%', true),
            (r#"3hello"#, r#"3%hello"#, '3', false),
            (r#"3hello"#, r#"__hello"#, '_', false),
            (r#"3hello"#, r#"%_hello"#, '%', true),
        ];
        let mut ctx = EvalContext::default();
        for (target_str, pattern_str, escape, exp) in cases {
            let target = datum_expr(Datum::Bytes(target_str.as_bytes().to_vec()));
            let pattern = datum_expr(Datum::Bytes(pattern_str.as_bytes().to_vec()));
            let escape = datum_expr(Datum::I64(escape as i64));
            let op = scalar_func_expr(ScalarFuncSig::LikeSig, &[target, pattern, escape]);
            let op = Expression::build(&ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::from(exp);
            assert_eq!(got, exp, "{:?} like {:?}", target_str, pattern_str);
        }
    }

    #[test]
    fn test_regexp() {
        let cases = vec![
            ("a", r"^$", false),
            ("a", r"a", true),
            ("b", r"a", false),
            ("aA", r"Aa", true),
            ("aaa", r".", true),
            ("ab", r"^.$", false),
            ("b", r"..", false),
            ("aab", r".ab", true),
            ("abcd", r".*", true),
            ("你", r"^.$", true),
            ("你好", r"你好", true),
            ("你好", r"^你好$", true),
            ("你好", r"^您好$", false),
        ];
        let mut ctx = EvalContext::default();
        for (target_str, pattern_str, exp) in cases {
            let target = datum_expr(Datum::Bytes(target_str.as_bytes().to_vec()));
            let pattern = datum_expr(Datum::Bytes(pattern_str.as_bytes().to_vec()));
            let op = scalar_func_expr(ScalarFuncSig::RegexpSig, &[target, pattern]);
            let op = Expression::build(&ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::from(exp);
            assert_eq!(got, exp, "{:?} rlike {:?}", target_str, pattern_str);
        }
    }

    #[test]
    fn test_regexp_binary() {
        let cases = vec![
            ("a".to_owned().into_bytes(), r"^$", false),
            ("a".to_owned().into_bytes(), r"a", true),
            ("b".to_owned().into_bytes(), r"a", false),
            ("aA".to_owned().into_bytes(), r"Aa", false),
            ("aaa".to_owned().into_bytes(), r".", true),
            ("ab".to_owned().into_bytes(), r"^.$", false),
            ("b".to_owned().into_bytes(), r"..", false),
            ("aab".to_owned().into_bytes(), r".ab", true),
            ("abcd".to_owned().into_bytes(), r".*", true),
            (vec![0x7f], r"^.$", true), // dot should match one byte which is less than 128
            (vec![0xf0], r"^.$", false), // dot can't match one byte greater than 128
            // dot should match "你" even if the char has 3 bytes.
            ("你".to_owned().into_bytes(), r"^.$", true),
            ("你好".to_owned().into_bytes(), r"你好", true),
            ("你好".to_owned().into_bytes(), r"^你好$", true),
            ("你好".to_owned().into_bytes(), r"^您好$", false),
            (
                vec![255, 255, 0xE4, 0xBD, 0xA0, 0xE5, 0xA5, 0xBD],
                r"你好",
                true,
            ),
        ];
        let mut ctx = EvalContext::default();
        for (target_str, pattern_str, exp) in cases {
            let target = datum_expr(Datum::Bytes(target_str.clone()));
            let pattern = datum_expr(Datum::Bytes(pattern_str.as_bytes().to_vec()));
            let op = scalar_func_expr(ScalarFuncSig::RegexpBinarySig, &[target, pattern]);
            let op = Expression::build(&ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::from(exp);
            assert_eq!(got, exp, "{:?} binary rlike {:?}", target_str, pattern_str);
        }
    }
}
