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

use std::borrow::Cow;
use std::collections::VecDeque;
use std::i64;

use hex::{self, FromHex};

use cop_datatype::prelude::*;
use cop_datatype::FieldTypeFlag;

use super::{EvalContext, Result, ScalarFunc};
use coprocessor::codec::Datum;

const SPACE: u8 = 0o40u8;

enum TrimDirection {
    Both = 1,
    Leading,
    Trailing,
}

impl TrimDirection {
    fn from_i64(i: i64) -> Option<Self> {
        match i {
            1 => Some(TrimDirection::Both),
            2 => Some(TrimDirection::Leading),
            3 => Some(TrimDirection::Trailing),
            _ => None,
        }
    }
}

impl ScalarFunc {
    #[inline]
    pub fn length(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        Ok(Some(input.len() as i64))
    }

    #[inline]
    pub fn bit_length(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        Ok(Some(input.len() as i64 * 8))
    }

    #[inline]
    pub fn ascii(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        if input.len() == 0 {
            Ok(Some(0))
        } else {
            Ok(Some(i64::from(input[0])))
        }
    }

    #[inline]
    pub fn char_length(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        if self.children[0].field_type().is_binary_string_like() {
            let input = try_opt!(self.children[0].eval_string(ctx, row));
            return Ok(Some(input.len() as i64));
        }
        let input = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        Ok(Some(input.chars().count() as i64))
    }

    #[inline]
    pub fn bin<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let i = try_opt!(self.children[0].eval_int(ctx, row));
        Ok(Some(Cow::Owned(format!("{:b}", i).into_bytes())))
    }

    #[inline]
    pub fn concat<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let mut output: Vec<u8> = Vec::new();
        for expr in &self.children {
            let input = try_opt!(expr.eval_string(ctx, row));
            output.extend_from_slice(&input);
        }
        Ok(Some(Cow::Owned(output)))
    }

    #[inline]
    pub fn ltrim<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let val = try_opt!(self.children[0].eval_string(ctx, row));
        let pos = val.iter().position(|&x| x != SPACE);
        if let Some(i) = pos {
            match val {
                Cow::Borrowed(val) => Ok(Some(Cow::Borrowed(&val[i..]))),
                Cow::Owned(val) => Ok(Some(Cow::Owned(val[i..].to_owned()))),
            }
        } else {
            Ok(Some(Cow::Owned(b"".to_vec())))
        }
    }

    #[inline]
    pub fn rtrim<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let val = try_opt!(self.children[0].eval_string(ctx, row));
        let pos = val.iter().rev().position(|&x| x != SPACE);
        if let Some(i) = pos {
            match val {
                Cow::Borrowed(val) => Ok(Some(Cow::Borrowed(&val[..val.len() - i]))),
                Cow::Owned(val) => Ok(Some(Cow::Owned(val[..val.len() - i].to_owned()))),
            }
        } else {
            Ok(Some(Cow::Owned(b"".to_vec())))
        }
    }

    pub fn left<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        let i = try_opt!(self.children[1].eval_int(ctx, row));
        if i <= 0 {
            return Ok(Some(Cow::Owned(b"".to_vec())));
        }
        if s.chars().count() > i as usize {
            let t = s.chars();
            return Ok(Some(Cow::Owned(
                t.take(i as usize).collect::<String>().into_bytes(),
            )));
        }
        Ok(Some(Cow::Owned(s.to_string().into_bytes())))
    }

    #[inline]
    pub fn reverse<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        Ok(Some(Cow::Owned(
            s.chars().rev().collect::<String>().into_bytes(),
        )))
    }

    #[inline]
    pub fn reverse_binary<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let mut s = try_opt!(self.children[0].eval_string(ctx, row));
        s.to_mut().reverse();
        Ok(Some(s))
    }

    pub fn right<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        let i = try_opt!(self.children[1].eval_int(ctx, row));
        if i <= 0 {
            return Ok(Some(Cow::Owned(b"".to_vec())));
        }
        let len = s.chars().count();
        let i = i as usize;
        if len > i {
            let idx = s
                .char_indices()
                .nth(len - i)
                .map(|(idx, _)| idx)
                .unwrap_or_else(|| s.len());
            return Ok(Some(Cow::Owned(s[idx..].to_string().into_bytes())));
        }
        Ok(Some(Cow::Owned(s.to_string().into_bytes())))
    }

    #[inline]
    pub fn upper<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        if self.children[0].field_type().is_binary_string_like() {
            let s = try_opt!(self.children[0].eval_string(ctx, row));
            return Ok(Some(s));
        }
        let s = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        Ok(Some(Cow::Owned(s.to_uppercase().into_bytes())))
    }

    #[inline]
    pub fn lower<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        if self.children[0].field_type().is_binary_string_like() {
            let s = try_opt!(self.children[0].eval_string(ctx, row));
            return Ok(Some(s));
        }
        let s = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        Ok(Some(Cow::Owned(s.to_lowercase().into_bytes())))
    }

    #[inline]
    pub fn hex_int_arg<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let i = try_opt!(self.children[0].eval_int(ctx, row));
        Ok(Some(Cow::Owned(format!("{:X}", i).into_bytes())))
    }

    #[inline]
    pub fn hex_str_arg<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string(ctx, row));
        Ok(Some(Cow::Owned(hex::encode_upper(s.to_vec()).into_bytes())))
    }

    #[inline]
    pub fn un_hex<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string(ctx, row));
        let hex_string = if s.len() % 2 == 1 {
            // Add a '0' to the front, if the length is not the multiple of 2
            let mut vec = vec![b'0'];
            vec.extend_from_slice(&s);
            vec
        } else {
            s.to_vec()
        };
        let result = Vec::from_hex(hex_string);
        result.map(|t| Some(Cow::Owned(t))).or(Ok(None))
    }

    #[inline]
    pub fn elt<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let i = try_opt!(self.children[0].eval_int(ctx, row));
        if i <= 0 || i + 1 > self.children.len() as i64 {
            return Ok(None);
        }
        self.children[i as usize].eval_string(ctx, row)
    }

    #[inline]
    pub fn trim_1_arg<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        trim(&s, " ", TrimDirection::Both)
    }

    #[inline]
    pub fn trim_2_args<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        let pat = try_opt!(self.children[1].eval_string_and_decode(ctx, row));
        trim(&s, &pat, TrimDirection::Both)
    }

    #[inline]
    pub fn trim_3_args<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        let pat = try_opt!(self.children[1].eval_string_and_decode(ctx, row));
        let direction = try_opt!(self.children[2].eval_int(ctx, row));
        match TrimDirection::from_i64(direction) {
            Some(d) => trim(&s, &pat, d),
            _ => Err(box_err!("invalid direction value: {}", direction)),
        }
    }

    #[inline]
    pub fn substring_index<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        let delim = try_opt!(self.children[1].eval_string_and_decode(ctx, row));
        let count = try_opt!(self.children[2].eval_int(ctx, row));
        if delim.is_empty() || count == 0 {
            return Ok(Some(Cow::Borrowed(b"")));
        }
        let r = if self.children[2]
            .field_type()
            .flag()
            .contains(FieldTypeFlag::UNSIGNED)
        {
            substring_index_positive(&s, delim.as_ref(), count as u64 as usize)
        } else if count >= 0 {
            substring_index_positive(&s, delim.as_ref(), count as usize)
        } else {
            let count = if count == i64::min_value() {
                i64::max_value() as usize + 1
            } else {
                -count as usize
            };
            substring_index_negative(&s, delim.as_ref(), count)
        };
        Ok(Some(Cow::Owned(r.into_bytes())))
    }
}

#[inline]
fn substring_index_positive(s: &str, delim: &str, count: usize) -> String {
    let mut bg = 0;
    let mut cnt = 0;
    let mut last = 0;
    while cnt < count {
        if let Some(idx) = s[bg..].find(delim) {
            last = bg + idx;
            bg = last + delim.len();
            cnt += 1;
        } else {
            last = s.len();
            break;
        }
    }
    s[..last].to_string()
}

#[inline]
fn substring_index_negative(s: &str, delim: &str, count: usize) -> String {
    let mut bg = 0;
    let mut positions = VecDeque::with_capacity(count.min(128));
    positions.push_back(0);
    while let Some(idx) = s[bg..].find(delim) {
        bg = bg + idx + delim.len();
        if positions.len() == count {
            positions.pop_front();
        }
        positions.push_back(bg);
    }
    s[positions[0]..].to_string()
}

#[inline]
fn trim<'a>(s: &str, pat: &str, direction: TrimDirection) -> Result<Option<Cow<'a, [u8]>>> {
    let r = match direction {
        TrimDirection::Leading => s.trim_left_matches(pat),
        TrimDirection::Trailing => s.trim_right_matches(pat),
        _ => s.trim_left_matches(pat).trim_right_matches(pat),
    };
    Ok(Some(Cow::Owned(r.to_string().into_bytes())))
}

#[cfg(test)]
mod tests {
    use cop_datatype::{Collation, FieldTypeFlag, FieldTypeTp};
    use tipb::expression::{Expr, ScalarFuncSig};

    use super::TrimDirection;
    use coprocessor::codec::mysql::charset::CHARSET_BIN;
    use coprocessor::codec::Datum;
    use coprocessor::dag::expr::tests::{
        col_expr, datum_expr, eval_func, scalar_func_expr, string_datum_expr_with_tp,
    };
    use coprocessor::dag::expr::{EvalContext, Expression};

    #[test]
    fn test_length() {
        let cases = vec![
            ("", 0i64),
            ("你好", 6i64),
            ("TiKV", 4i64),
            ("あなたのことが好きです", 33i64),
            ("분산 데이터베이스", 25i64),
            ("россия в мире  кубок", 38i64),
            ("قاعدة البيانات", 27i64),
        ];

        let mut ctx = EvalContext::default();
        for (input_str, exp) in cases {
            let input = datum_expr(Datum::Bytes(input_str.as_bytes().to_vec()));
            let op = scalar_func_expr(ScalarFuncSig::Length, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::from(exp);
            assert_eq!(got, exp, "length('{:?}')", input_str);
        }

        // test NULL case
        let input = datum_expr(Datum::Null);
        let op = scalar_func_expr(ScalarFuncSig::Length, &[input]);
        let op = Expression::build(&mut ctx, op).unwrap();
        let got = op.eval(&mut ctx, &[]).unwrap();
        let exp = Datum::Null;
        assert_eq!(got, exp, "length(NULL)");
    }

    #[test]
    fn test_bit_length() {
        let cases = vec![
            ("", 0i64),
            ("你好", 48i64),
            ("TiKV", 32i64),
            ("あなたのことが好きです", 264i64),
            ("분산 데이터베이스", 200i64),
            ("россия в мире  кубок", 304i64),
            ("قاعدة البيانات", 216i64),
        ];

        let mut ctx = EvalContext::default();
        for (input_str, exp) in cases {
            let input = datum_expr(Datum::Bytes(input_str.as_bytes().to_vec()));
            let op = scalar_func_expr(ScalarFuncSig::BitLength, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::from(exp);
            assert_eq!(got, exp, "bit_length('{:?}')", input_str);
        }

        // test NULL case
        let input = datum_expr(Datum::Null);
        let op = scalar_func_expr(ScalarFuncSig::BitLength, &[input]);
        let op = Expression::build(&mut ctx, op).unwrap();
        let got = op.eval(&mut ctx, &[]).unwrap();
        let exp = Datum::Null;
        assert_eq!(got, exp, "bit_length(NULL)");
    }

    #[test]
    fn test_bin() {
        let cases = vec![
            (Datum::I64(10), Datum::Bytes(b"1010".to_vec())),
            (Datum::I64(0), Datum::Bytes(b"0".to_vec())),
            (Datum::I64(1), Datum::Bytes(b"1".to_vec())),
            (Datum::I64(365), Datum::Bytes(b"101101101".to_vec())),
            (Datum::I64(1024), Datum::Bytes(b"10000000000".to_vec())),
            (Datum::Null, Datum::Null),
            (
                Datum::I64(i64::max_value()),
                Datum::Bytes(
                    b"111111111111111111111111111111111111111111111111111111111111111".to_vec(),
                ),
            ),
            (
                Datum::I64(i64::min_value()),
                Datum::Bytes(
                    b"1000000000000000000000000000000000000000000000000000000000000000".to_vec(),
                ),
            ),
            (
                Datum::I64(-1),
                Datum::Bytes(
                    b"1111111111111111111111111111111111111111111111111111111111111111".to_vec(),
                ),
            ),
            (
                Datum::I64(-365),
                Datum::Bytes(
                    b"1111111111111111111111111111111111111111111111111111111010010011".to_vec(),
                ),
            ),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::Bin, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_ltrim() {
        let cases = vec![
            ("   bar   ", "bar   "),
            ("   b   ar   ", "b   ar   "),
            ("bar", "bar"),
            ("    ", ""),
            ("\t  bar", "\t  bar"),
            ("\r  bar", "\r  bar"),
            ("\n  bar", "\n  bar"),
            ("  \tbar", "\tbar"),
            ("", ""),
            ("  你好", "你好"),
            ("  你  好", "你  好"),
            (
                "  분산 데이터베이스    ",
                "분산 데이터베이스    ",
            ),
            (
                "   あなたのことが好きです   ",
                "あなたのことが好きです   ",
            ),
        ];

        let mut ctx = EvalContext::default();
        for (input_str, exp) in cases {
            let input = datum_expr(Datum::Bytes(input_str.as_bytes().to_vec()));
            let op = scalar_func_expr(ScalarFuncSig::LTrim, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::Bytes(exp.as_bytes().to_vec());
            assert_eq!(got, exp, "ltrim('{:?}')", input_str);
        }

        // test NULL case
        let input = datum_expr(Datum::Null);
        let op = scalar_func_expr(ScalarFuncSig::LTrim, &[input]);
        let op = Expression::build(&mut ctx, op).unwrap();
        let got = op.eval(&mut ctx, &[]).unwrap();
        let exp = Datum::Null;
        assert_eq!(got, exp, "ltrim(NULL)");
    }

    #[test]
    fn test_rtrim() {
        let cases = vec![
            ("   bar   ", "   bar"),
            ("bar", "bar"),
            ("ba  r", "ba  r"),
            ("    ", ""),
            ("  bar\t  ", "  bar\t"),
            (" bar   \t", " bar   \t"),
            ("bar   \r", "bar   \r"),
            ("bar   \n", "bar   \n"),
            ("", ""),
            ("  你好  ", "  你好"),
            ("  你  好  ", "  你  好"),
            (
                "  분산 데이터베이스    ",
                "  분산 데이터베이스",
            ),
            (
                "   あなたのことが好きです   ",
                "   あなたのことが好きです",
            ),
        ];

        let mut ctx = EvalContext::default();
        for (input_str, exp) in cases {
            let input = datum_expr(Datum::Bytes(input_str.as_bytes().to_vec()));
            let op = scalar_func_expr(ScalarFuncSig::RTrim, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::Bytes(exp.as_bytes().to_vec());
            assert_eq!(got, exp, "rtrim('{:?}')", input_str);
        }

        // test NULL case
        let input = datum_expr(Datum::Null);
        let op = scalar_func_expr(ScalarFuncSig::RTrim, &[input]);
        let op = Expression::build(&mut ctx, op).unwrap();
        let got = op.eval(&mut ctx, &[]).unwrap();
        let exp = Datum::Null;
        assert_eq!(got, exp, "rtrim(NULL)");
    }

    #[test]
    fn test_reverse() {
        let cases = vec![
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::Bytes(b"olleh".to_vec()),
            ),
            (Datum::Bytes(b"".to_vec()), Datum::Bytes(b"".to_vec())),
            (
                Datum::Bytes("数据库".as_bytes().to_vec()),
                Datum::Bytes("库据数".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("忠犬ハチ公".as_bytes().to_vec()),
                Datum::Bytes("公チハ犬忠".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("あなたのことが好きです".as_bytes().to_vec()),
                Datum::Bytes("すでき好がとこのたなあ".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("Bayern München".as_bytes().to_vec()),
                Datum::Bytes("nehcnüM nreyaB".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("Η Αθηνά  ".as_bytes().to_vec()),
                Datum::Bytes("  άνηθΑ Η".as_bytes().to_vec()),
            ),
            (Datum::Null, Datum::Null),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let op = scalar_func_expr(ScalarFuncSig::Reverse, &[datum_expr(arg)]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_reverse_binary() {
        let cases = vec![
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::Bytes(b"olleh".to_vec()),
            ),
            (Datum::Bytes(b"".to_vec()), Datum::Bytes(b"".to_vec())),
            (
                Datum::Bytes("中国".as_bytes().to_vec()),
                Datum::Bytes(vec![0o275u8, 0o233u8, 0o345u8, 0o255u8, 0o270u8, 0o344u8]),
            ),
            (Datum::Null, Datum::Null),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let input = string_datum_expr_with_tp(
                arg,
                FieldTypeTp::VarString,
                FieldTypeFlag::BINARY,
                -1,
                CHARSET_BIN.to_owned(),
                Collation::Binary,
            );
            let op = scalar_func_expr(ScalarFuncSig::ReverseBinary, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_left() {
        let cases = vec![
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::I64(0),
                Datum::Bytes(b"".to_vec()),
            ),
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::I64(1),
                Datum::Bytes(b"h".to_vec()),
            ),
            (
                Datum::Bytes("数据库".as_bytes().to_vec()),
                Datum::I64(2),
                Datum::Bytes("数据".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("忠犬ハチ公".as_bytes().to_vec()),
                Datum::I64(3),
                Datum::Bytes("忠犬ハ".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("数据库".as_bytes().to_vec()),
                Datum::I64(100),
                Datum::Bytes("数据库".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("数据库".as_bytes().to_vec()),
                Datum::I64(-1),
                Datum::Bytes(b"".to_vec()),
            ),
            (Datum::Null, Datum::I64(-1), Datum::Null),
            (Datum::Bytes(b"hello".to_vec()), Datum::Null, Datum::Null),
        ];

        let mut ctx = EvalContext::default();
        for (arg1, arg2, exp) in cases {
            let arg1 = datum_expr(arg1);
            let arg2 = datum_expr(arg2);
            let op = scalar_func_expr(ScalarFuncSig::Left, &[arg1, arg2]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_right() {
        let cases = vec![
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::I64(0),
                Datum::Bytes(b"".to_vec()),
            ),
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::I64(1),
                Datum::Bytes(b"o".to_vec()),
            ),
            (
                Datum::Bytes("数据库".as_bytes().to_vec()),
                Datum::I64(2),
                Datum::Bytes("据库".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("忠犬ハチ公".as_bytes().to_vec()),
                Datum::I64(3),
                Datum::Bytes("ハチ公".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("数据库".as_bytes().to_vec()),
                Datum::I64(100),
                Datum::Bytes("数据库".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("数据库".as_bytes().to_vec()),
                Datum::I64(-1),
                Datum::Bytes(b"".to_vec()),
            ),
            (Datum::Null, Datum::I64(-1), Datum::Null),
            (Datum::Bytes(b"hello".to_vec()), Datum::Null, Datum::Null),
        ];
        let mut ctx = EvalContext::default();
        for (arg1, arg2, exp) in cases {
            let arg1 = datum_expr(arg1);
            let arg2 = datum_expr(arg2);
            let op = scalar_func_expr(ScalarFuncSig::Right, &[arg1, arg2]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_ascii() {
        let cases = vec![
            (Datum::Bytes(b"1010".to_vec()), Datum::I64(49)),
            (Datum::Bytes(b"-1".to_vec()), Datum::I64(45)),
            (Datum::Bytes(b"".to_vec()), Datum::I64(0)),
            (Datum::Bytes(b"999".to_vec()), Datum::I64(57)),
            (Datum::Bytes(b"hello".to_vec()), Datum::I64(104)),
            (Datum::Bytes("Grüße".as_bytes().to_vec()), Datum::I64(71)),
            (Datum::Bytes("München".as_bytes().to_vec()), Datum::I64(77)),
            (Datum::Null, Datum::Null),
            (
                Datum::Bytes("数据库".as_bytes().to_vec()),
                Datum::I64(230),
            ),
            (
                Datum::Bytes("忠犬ハチ公".as_bytes().to_vec()),
                Datum::I64(229),
            ),
            (
                Datum::Bytes("Αθήνα".as_bytes().to_vec()),
                Datum::I64(206),
            ),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::ASCII, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_upper() {
        // Test non-bianry string case
        let cases = vec![
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::Bytes(b"HELLO".to_vec()),
            ),
            (Datum::Bytes(b"123".to_vec()), Datum::Bytes(b"123".to_vec())),
            (
                Datum::Bytes("café".as_bytes().to_vec()),
                Datum::Bytes("CAFÉ".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("数据库".as_bytes().to_vec()),
                Datum::Bytes("数据库".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes(
                    "ночь на окраине москвы"
                        .as_bytes()
                        .to_vec(),
                ),
                Datum::Bytes(
                    "НОЧЬ НА ОКРАИНЕ МОСКВЫ"
                        .as_bytes()
                        .to_vec(),
                ),
            ),
            (
                Datum::Bytes("قاعدة البيانات".as_bytes().to_vec()),
                Datum::Bytes("قاعدة البيانات".as_bytes().to_vec()),
            ),
            (Datum::Null, Datum::Null),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::Upper, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }

        // Test binary string case
        let cases = vec![
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::Bytes(b"hello".to_vec()),
            ),
            (Datum::Bytes(b"123".to_vec()), Datum::Bytes(b"123".to_vec())),
            (
                Datum::Bytes("café".as_bytes().to_vec()),
                Datum::Bytes("café".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("数据库".as_bytes().to_vec()),
                Datum::Bytes("数据库".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes(
                    "ночь на окраине москвы"
                        .as_bytes()
                        .to_vec(),
                ),
                Datum::Bytes(
                    "ночь на окраине москвы"
                        .as_bytes()
                        .to_vec(),
                ),
            ),
            (
                Datum::Bytes("قاعدة البيانات".as_bytes().to_vec()),
                Datum::Bytes("قاعدة البيانات".as_bytes().to_vec()),
            ),
            (Datum::Null, Datum::Null),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = string_datum_expr_with_tp(
                input,
                FieldTypeTp::VarString,
                FieldTypeFlag::BINARY,
                -1,
                CHARSET_BIN.to_owned(),
                Collation::Binary,
            );
            let op = scalar_func_expr(ScalarFuncSig::Upper, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_lower() {
        // Test non-bianry string case
        let cases = vec![
            (
                Datum::Bytes(b"HELLO".to_vec()),
                Datum::Bytes(b"hello".to_vec()),
            ),
            (Datum::Bytes(b"123".to_vec()), Datum::Bytes(b"123".to_vec())),
            (
                Datum::Bytes("CAFÉ".as_bytes().to_vec()),
                Datum::Bytes("café".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("数据库".as_bytes().to_vec()),
                Datum::Bytes("数据库".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes(
                    "НОЧЬ НА ОКРАИНЕ МОСКВЫ"
                        .as_bytes()
                        .to_vec(),
                ),
                Datum::Bytes(
                    "ночь на окраине москвы"
                        .as_bytes()
                        .to_vec(),
                ),
            ),
            (
                Datum::Bytes("قاعدة البيانات".as_bytes().to_vec()),
                Datum::Bytes("قاعدة البيانات".as_bytes().to_vec()),
            ),
            (Datum::Null, Datum::Null),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::Lower, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }

        // Test binary string case
        let cases = vec![
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::Bytes(b"hello".to_vec()),
            ),
            (
                Datum::Bytes("CAFÉ".as_bytes().to_vec()),
                Datum::Bytes("CAFÉ".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("数据库".as_bytes().to_vec()),
                Datum::Bytes("数据库".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes(
                    "НОЧЬ НА ОКРАИНЕ МОСКВЫ"
                        .as_bytes()
                        .to_vec(),
                ),
                Datum::Bytes(
                    "НОЧЬ НА ОКРАИНЕ МОСКВЫ"
                        .as_bytes()
                        .to_vec(),
                ),
            ),
            (
                Datum::Bytes("قاعدة البيانات".as_bytes().to_vec()),
                Datum::Bytes("قاعدة البيانات".as_bytes().to_vec()),
            ),
            (Datum::Null, Datum::Null),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = string_datum_expr_with_tp(
                input,
                FieldTypeTp::VarString,
                FieldTypeFlag::BINARY,
                -1,
                CHARSET_BIN.to_owned(),
                Collation::Binary,
            );
            let op = scalar_func_expr(ScalarFuncSig::Lower, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_concat() {
        let cases = vec![
            (
                vec![
                    Datum::Bytes(b"abc".to_vec()),
                    Datum::Bytes(b"defg".to_vec()),
                ],
                Datum::Bytes(b"abcdefg".to_vec()),
            ),
            (
                vec![
                    Datum::Bytes("忠犬ハチ公".as_bytes().to_vec()),
                    Datum::Bytes("CAFÉ".as_bytes().to_vec()),
                    Datum::Bytes("数据库".as_bytes().to_vec()),
                    Datum::Bytes("قاعدة البيانات".as_bytes().to_vec()),
                    Datum::Bytes( "НОЧЬ НА ОКРАИНЕ МОСКВЫ".as_bytes().to_vec()),
                ],
                Datum::Bytes(
                    "忠犬ハチ公CAFÉ数据库قاعدة البياناتНОЧЬ НА ОКРАИНЕ МОСКВЫ"
                        .as_bytes()
                        .to_vec(),
                ),
            ),
            (
                vec![
                    Datum::Bytes(b"abc".to_vec()),
                    Datum::Bytes("CAFÉ".as_bytes().to_vec()),
                    Datum::Bytes("数据库".as_bytes().to_vec()),
                ],
                Datum::Bytes("abcCAFÉ数据库".as_bytes().to_vec()),
            ),
            (
                vec![
                    Datum::Bytes(b"abc".to_vec()),
                    Datum::Null,
                    Datum::Bytes(b"defg".to_vec()),
                ],
                Datum::Null,
            ),
            (vec![Datum::Null], Datum::Null),
        ];
        let mut ctx = EvalContext::default();
        for (row, exp) in cases {
            let children: Vec<Expr> = row.iter().map(|d| datum_expr(d.clone())).collect();
            let mut expr = scalar_func_expr(ScalarFuncSig::Concat, &children);
            let e = Expression::build(&mut ctx, expr).unwrap();
            let res = e.eval(&mut ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_char_length() {
        // Test non-bianry string case
        let cases = vec![
            (Datum::Bytes(b"HELLO".to_vec()), Datum::I64(5)),
            (Datum::Bytes(b"123".to_vec()), Datum::I64(3)),
            (Datum::Bytes(b"".to_vec()), Datum::I64(0)),
            (Datum::Bytes("CAFÉ".as_bytes().to_vec()), Datum::I64(4)),
            (Datum::Bytes("数据库".as_bytes().to_vec()), Datum::I64(3)),
            (
                Datum::Bytes(
                    "НОЧЬ НА ОКРАИНЕ МОСКВЫ"
                        .as_bytes()
                        .to_vec(),
                ),
                Datum::I64(22),
            ),
            (
                Datum::Bytes("قاعدة البيانات".as_bytes().to_vec()),
                Datum::I64(14),
            ),
            (Datum::Null, Datum::Null),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::CharLength, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }

        // Test binary string case
        let cases = vec![
            (Datum::Bytes(b"HELLO".to_vec()), Datum::I64(5)),
            (Datum::Bytes(b"123".to_vec()), Datum::I64(3)),
            (Datum::Bytes(b"".to_vec()), Datum::I64(0)),
            (Datum::Bytes("CAFÉ".as_bytes().to_vec()), Datum::I64(5)),
            (Datum::Bytes("数据库".as_bytes().to_vec()), Datum::I64(9)),
            (
                Datum::Bytes(
                    "НОЧЬ НА ОКРАИНЕ МОСКВЫ"
                        .as_bytes()
                        .to_vec(),
                ),
                Datum::I64(41),
            ),
            (
                Datum::Bytes("قاعدة البيانات".as_bytes().to_vec()),
                Datum::I64(27),
            ),
            (Datum::Null, Datum::Null),
        ];
        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = string_datum_expr_with_tp(
                input,
                FieldTypeTp::VarString,
                FieldTypeFlag::BINARY,
                -1,
                CHARSET_BIN.to_owned(),
                Collation::Binary,
            );
            let op = scalar_func_expr(ScalarFuncSig::CharLength, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_hex_int_arg() {
        let cases = vec![
            (Datum::I64(12), Datum::Bytes(b"C".to_vec())),
            (Datum::I64(0x12), Datum::Bytes(b"12".to_vec())),
            (Datum::I64(0b1100), Datum::Bytes(b"C".to_vec())),
            (Datum::I64(0), Datum::Bytes(b"0".to_vec())),
            (Datum::I64(-1), Datum::Bytes(b"FFFFFFFFFFFFFFFF".to_vec())),
            (Datum::Null, Datum::Null),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::HexIntArg, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_hex_str_arg() {
        let cases = vec![
            (
                Datum::Bytes(b"abc".to_vec()),
                Datum::Bytes(b"616263".to_vec()),
            ),
            (
                Datum::Bytes("你好".as_bytes().to_vec()),
                Datum::Bytes(b"E4BDA0E5A5BD".to_vec()),
            ),
            (Datum::Null, Datum::Null),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::HexStrArg, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_un_hex() {
        let cases = vec![
            (
                Datum::Bytes(b"4D7953514C".to_vec()),
                Datum::Bytes(b"MySQL".to_vec()),
            ),
            (
                Datum::Bytes(b"1267".to_vec()),
                Datum::Bytes(vec![0x12, 0x67]),
            ),
            (
                Datum::Bytes(b"126".to_vec()),
                Datum::Bytes(vec![0x01, 0x26]),
            ),
            (Datum::Bytes(b"".to_vec()), Datum::Bytes(b"".to_vec())),
            (Datum::Bytes(b"string".to_vec()), Datum::Null),
            (Datum::Bytes("你好".as_bytes().to_vec()), Datum::Null),
            (Datum::Null, Datum::Null),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::UnHex, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_elt() {
        let cases = vec![
            (
                vec![
                    Datum::I64(1),
                    Datum::Bytes(b"DataBase".to_vec()),
                    Datum::Bytes(b"Hello World!".to_vec()),
                ],
                Datum::Bytes(b"DataBase".to_vec()),
            ),
            (
                vec![
                    Datum::I64(2),
                    Datum::Bytes(b"DataBase".to_vec()),
                    Datum::Bytes(b"Hello World!".to_vec()),
                ],
                Datum::Bytes(b"Hello World!".to_vec()),
            ),
            (
                vec![
                    Datum::Null,
                    Datum::Bytes(b"DataBase".to_vec()),
                    Datum::Bytes(b"Hello World!".to_vec()),
                ],
                Datum::Null,
            ),
            (
                vec![
                    Datum::I64(1),
                    Datum::Null,
                    Datum::Bytes(b"Hello World!".to_vec()),
                ],
                Datum::Null,
            ),
            (
                vec![
                    Datum::I64(3),
                    Datum::Null,
                    Datum::Bytes(b"Hello World!".to_vec()),
                ],
                Datum::Null,
            ),
            (
                vec![
                    Datum::I64(0),
                    Datum::Null,
                    Datum::Bytes(b"Hello World!".to_vec()),
                ],
                Datum::Null,
            ),
            (
                vec![
                    Datum::I64(-1),
                    Datum::Null,
                    Datum::Bytes(b"Hello World!".to_vec()),
                ],
                Datum::Null,
            ),
            (
                vec![
                    Datum::I64(4),
                    Datum::Null,
                    Datum::Bytes(b"Hello".to_vec()),
                    Datum::Bytes(b"Hola".to_vec()),
                    Datum::Bytes("Cześć".as_bytes().to_vec()),
                    Datum::Bytes("你好".as_bytes().to_vec()),
                    Datum::Bytes("Здравствуйте".as_bytes().to_vec()),
                    Datum::Bytes(b"Hello World!".to_vec()),
                ],
                Datum::Bytes("Cześć".as_bytes().to_vec()),
            ),
        ];

        let mut ctx = EvalContext::default();
        for (args, exp) in cases {
            let children: Vec<Expr> = (0..args.len()).map(|id| col_expr(id as i64)).collect();
            let op = scalar_func_expr(ScalarFuncSig::Elt, &children);
            let e = Expression::build(&mut ctx, op).unwrap();
            let res = e.eval(&mut ctx, &args).unwrap();
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_trim_1_arg() {
        let tests = vec![
            ("   bar   ", "bar"),
            ("\t   bar   \n", "\t   bar   \n"),
            ("\r   bar   \t", "\r   bar   \t"),
            ("   \tbar\n     ", "\tbar\n"),
            ("", ""),
        ];
        for (s, exp) in tests {
            let s = Datum::Bytes(s.as_bytes().to_vec());
            let exp = Datum::Bytes(exp.as_bytes().to_vec());
            let got = eval_func(ScalarFuncSig::Trim1Arg, &[s]).unwrap();
            assert_eq!(got, exp);
        }

        let got = eval_func(ScalarFuncSig::Trim1Arg, &[Datum::Null]).unwrap();
        assert_eq!(got, Datum::Null);
    }

    #[test]
    fn test_trim_2_args() {
        let tests = vec![
            ("xxxbarxxx", "x", "bar"),
            ("bar", "x", "bar"),
            ("   bar   ", "", "   bar   "),
            ("", "x", ""),
            ("张三和张三", "张三", "和"),
        ];
        for (s, pat, exp) in tests {
            let s = Datum::Bytes(s.as_bytes().to_vec());
            let pat = Datum::Bytes(pat.as_bytes().to_vec());
            let exp = Datum::Bytes(exp.as_bytes().to_vec());
            let got = eval_func(ScalarFuncSig::Trim2Args, &[s, pat]).unwrap();
            assert_eq!(got, exp);
        }

        let invalid_tests = vec![
            (Datum::Null, Datum::Bytes(b"x".to_vec()), Datum::Null),
            (Datum::Bytes(b"bar".to_vec()), Datum::Null, Datum::Null),
        ];
        for (s, pat, exp) in invalid_tests {
            let got = eval_func(ScalarFuncSig::Trim2Args, &[s, pat]).unwrap();
            assert_eq!(got, exp);
        }
    }
    #[test]
    fn test_trim_3_args() {
        let tests = vec![
            ("xxxbarxxx", "x", TrimDirection::Leading as i64, "barxxx"),
            ("barxxyz", "xyz", TrimDirection::Trailing as i64, "barx"),
            ("xxxbarxxx", "x", TrimDirection::Both as i64, "bar"),
        ];
        for (s, pat, direction, exp) in tests {
            let s = Datum::Bytes(s.as_bytes().to_vec());
            let pat = Datum::Bytes(pat.as_bytes().to_vec());
            let direction = Datum::I64(direction);
            let exp = Datum::Bytes(exp.as_bytes().to_vec());

            let got = eval_func(ScalarFuncSig::Trim3Args, &[s, pat, direction]).unwrap();
            assert_eq!(got, exp);
        }

        let invalid_tests = vec![
            (
                Datum::Null,
                Datum::Bytes(b"x".to_vec()),
                Datum::I64(TrimDirection::Leading as i64),
                Datum::Null,
            ),
            (
                Datum::Bytes(b"bar".to_vec()),
                Datum::Null,
                Datum::I64(TrimDirection::Leading as i64),
                Datum::Null,
            ),
        ];
        for (s, pat, direction, exp) in invalid_tests {
            let got = eval_func(ScalarFuncSig::Trim3Args, &[s, pat, direction]).unwrap();
            assert_eq!(got, exp);
        }

        // test invalid direction value
        let args = [
            Datum::Bytes(b"bar".to_vec()),
            Datum::Bytes(b"b".to_vec()),
            Datum::I64(0),
        ];
        let got = eval_func(ScalarFuncSig::Trim3Args, &args);
        assert!(got.is_err());
    }

    #[test]
    fn test_substring_index() {
        let tests = vec![
            ("www.pingcap.com", ".", 2, "www.pingcap"),
            ("www.pingcap.com", ".", -2, "pingcap.com"),
            ("www.pingcap.com", ".", -3, "www.pingcap.com"),
            ("www.pingcap.com", ".", 0, ""),
            ("www.pingcap.com", ".", 100, "www.pingcap.com"),
            ("www.pingcap.com", ".", -100, "www.pingcap.com"),
            ("www.pingcap.com", "d", 0, ""),
            ("www.pingcap.com", "d", 1, "www.pingcap.com"),
            ("www.pingcap.com", "d", -1, "www.pingcap.com"),
            ("www.pingcap.com", "", 0, ""),
            ("www.pingcap.com", "", 1, ""),
            ("www.pingcap.com", "", -1, ""),
            ("1aaa1", "aa", 1, "1"),
            ("1aaa1", "aa", 2, "1aaa1"),
            ("1aaaaaa1", "aa", 2, "1aa"),
            ("1aaa1", "aa", -1, "a1"),
            ("1aaaaaa1", "aa", -1, "1"),
            ("1aaa1", "aa", -2, "1aaa1"),
            ("1aaaaaa1", "aa", -2, "aa1"),
            ("aaa1aa1aa", "aa", -3, "a1aa1aa"),
            ("aaa1aa1aa", "aa", i64::max_value(), "aaa1aa1aa"),
            ("aaa1aa1aa", "aa", i64::min_value() + 1, "aaa1aa1aa"),
            ("aaa1aa1aa", "aa", i64::min_value(), "aaa1aa1aa"),
            // empty parts after split
            ("...", ".", 1, ""),
            ("...", ".", 2, "."),
            ("...", ".", 3, ".."),
            ("...", ".", 4, "..."),
            ("...", ".", -1, ""),
            ("...", ".", -2, "."),
            ("...", ".", -3, ".."),
            ("...", ".", -4, "..."),
            // weird boundary conditions
            ("...www...pingcap...com...", ".", 3, ".."),
            ("...www...pingcap...com...", ".", 4, "...www"),
            ("...www...pingcap...com...", ".", 5, "...www."),
            ("...www...pingcap...com...", ".", -3, ".."),
            ("...www...pingcap...com...", ".", -4, "com..."),
            ("...www...pingcap...com...", ".", -5, ".com..."),
            ("", ".", 0, ""),
            ("", ".", 1, ""),
            ("", ".", -1, ""),
        ];
        for (s, delim, count, exp) in tests {
            let s = Datum::Bytes(s.as_bytes().to_vec());
            let delim = Datum::Bytes(delim.as_bytes().to_vec());
            let count = Datum::I64(count);
            let got = eval_func(ScalarFuncSig::SubstringIndex, &[s, delim, count]).unwrap();
            assert_eq!(got, Datum::Bytes(exp.as_bytes().to_vec()));
        }

        // u64 count
        let args = [
            Datum::Bytes(b"www.pingcap.com".to_vec()),
            Datum::Bytes(b".".to_vec()),
            Datum::U64(u64::max_value()),
        ];
        let got = eval_func(ScalarFuncSig::SubstringIndex, &args).unwrap();
        assert_eq!(got, Datum::Bytes(b"www.pingcap.com".to_vec()));

        let invalid_tests = vec![
            (
                Datum::Null,
                Datum::Bytes(b"".to_vec()),
                Datum::I64(1),
                Datum::Null,
            ),
            (
                Datum::Bytes(b"www.pingcap.com".to_vec()),
                Datum::Null,
                Datum::I64(1),
                Datum::Null,
            ),
            (
                Datum::Bytes(b"www.pingcap.com".to_vec()),
                Datum::Bytes(b"".to_vec()),
                Datum::Null,
                Datum::Null,
            ),
        ];
        for (s, delim, count, exp) in invalid_tests {
            let got = eval_func(ScalarFuncSig::SubstringIndex, &[s, delim, count]).unwrap();
            assert_eq!(got, exp);
        }
    }
}
