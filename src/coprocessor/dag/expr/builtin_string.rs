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
use std::i64;

use super::{EvalContext, Result, ScalarFunc};
use coprocessor::codec::Datum;

const SPACE: u8 = 0o40u8;

impl ScalarFunc {
    pub fn length(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        Ok(Some(input.len() as i64))
    }

    pub fn bit_length(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        Ok(Some(input.len() as i64 * 8))
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
    pub fn ltrim<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let val = try_opt!(self.children[0].eval_string(ctx, row));
        let pos = val.iter().position(|&x| x != SPACE);
        if let Some(i) = pos {
            Ok(Some(Cow::Owned(val[i..].to_vec())))
        } else {
            Ok(Some(val))
        }
    }

    pub fn rtrim<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let val = try_opt!(self.children[0].eval_string(ctx, row));
        let pos = val.iter().rev().position(|&x| x != SPACE);
        if let Some(i) = pos {
            Ok(Some(Cow::Owned(val[..val.len() - i].to_vec())))
        } else {
            Ok(Some(Cow::Owned(b"".to_vec())))
        }
    }
}

#[cfg(test)]
mod test {
    use coprocessor::codec::Datum;
    use coprocessor::dag::expr::test::{datum_expr, scalar_func_expr};
    use coprocessor::dag::expr::{EvalContext, Expression};
    use tipb::expression::ScalarFuncSig;

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
}
