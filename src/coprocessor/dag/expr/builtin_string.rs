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

use hex::FromHex;
use std::{i64, str};

use super::{EvalContext, Result, ScalarFunc};
use coprocessor::codec::mysql::types;
use coprocessor::codec::Datum;
use std::borrow::Cow;

impl ScalarFunc {
    pub fn length(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        Ok(Some(input.len() as i64))
    }

    pub fn bit_length(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        Ok(Some(input.len() as i64 * 8))
    }

    pub fn ascii(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        if input.len() == 0 {
            Ok(Some(0))
        } else {
            Ok(Some(i64::from(input[0])))
        }
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
    pub fn upper<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string(ctx, row));
        if types::is_binary_str(self.children[0].get_tp()) {
            return Ok(Some(s));
        }
        Ok(Some(Cow::Owned(
            str::from_utf8(&s)?.to_uppercase().into_bytes(),
        )))
    }

    #[inline]
    pub fn lower<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string(ctx, row));
        if types::is_binary_str(self.children[0].get_tp()) {
            return Ok(Some(s));
        }
        Ok(Some(Cow::Owned(
            str::from_utf8(&s)?.to_lowercase().into_bytes(),
        )))
    }

    #[inline]
    pub fn un_hex<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string(ctx, row));
        let hex_string = if s.len() % 2 == 1 {
            let mut vec = vec![b'0'];
            vec.extend_from_slice(&s);
            vec
        } else {
            s.to_vec()
        };
        let result = Vec::from_hex(hex_string);
        result.map(|t| Some(Cow::Owned(t))).or(Ok(None))
    }
}

#[cfg(test)]
mod test {
    use coprocessor::codec::mysql::charset::{CHARSET_BIN, COLLATION_BIN_ID};
    use coprocessor::codec::mysql::types::{BINARY_FLAG, VAR_STRING};
    use coprocessor::codec::Datum;
    use coprocessor::dag::expr::test::{datum_expr, scalar_func_expr, string_datum_expr_with_tp};
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
                VAR_STRING,
                BINARY_FLAG,
                -1,
                CHARSET_BIN.to_owned(),
                COLLATION_BIN_ID,
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
                VAR_STRING,
                BINARY_FLAG,
                -1,
                CHARSET_BIN.to_owned(),
                COLLATION_BIN_ID,
            );
            let op = scalar_func_expr(ScalarFuncSig::Lower, &[input]);
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
}
