// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use regex::{bytes::Regex as BytesRegex, Regex};

use super::{EvalContext, Result, ScalarFunc};
use crate::codec::Datum;
use crate::expr_util;

impl ScalarFunc {
    /// NOTE: LIKE compare target with pattern as bytes, even if they have different
    /// charsets. This behaviour is for keeping compatible with TiDB. But MySQL
    /// compare them as bytes only if any charset of pattern or target is binary,
    /// otherwise MySQL will compare decoded string.
    pub fn like(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let target = try_opt!(self.children[0].eval_string(ctx, row));
        let pattern = try_opt!(self.children[1].eval_string(ctx, row));
        let escape = try_opt!(self.children[2].eval_int(ctx, row)) as u32;
        Ok(Some(
            expr_util::like::like(&target, &pattern, escape, 0)? as i64
        ))
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

#[cfg(test)]
mod tests {
    use crate::codec::Datum;
    use crate::expr::tests::{datum_expr, scalar_func_expr};
    use crate::expr::{EvalContext, Expression};
    use tipb::ScalarFuncSig;

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
