// Copyright 2017 PingCAP, Inc.
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

use super::{Error, EvalContext, Result, ScalarFunc};
use coprocessor::codec::mysql::Decimal;
use coprocessor::codec::Datum;
use crc::{crc32, Hasher32};
use std::borrow::Cow;
use std::i64;

impl ScalarFunc {
    #[inline]
    pub fn abs_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let n = try_opt!(self.children[0].eval_real(ctx, row));
        Ok(Some(n.abs()))
    }

    #[inline]
    pub fn abs_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let d = try_opt!(self.children[0].eval_decimal(ctx, row));
        let result: Result<Decimal> = d.into_owned().abs().into();
        result.map(|t| Some(Cow::Owned(t)))
    }

    #[inline]
    pub fn abs_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let n = try_opt!(self.children[0].eval_int(ctx, row));
        if n == i64::MIN {
            return Err(Error::overflow("BIGINT", &format!("abs({})", n)));
        }
        Ok(Some(n.abs()))
    }

    #[inline]
    pub fn abs_uint(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        self.children[0].eval_int(ctx, row)
    }

    #[inline]
    pub fn ceil_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let n = try_opt!(self.children[0].eval_real(ctx, row));
        Ok(Some(n.ceil()))
    }

    #[inline]
    pub fn ceil_dec_to_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let d = try_opt!(self.children[0].eval_decimal(ctx, row));
        let d: Result<Decimal> = d.ceil().into();
        d.and_then(|dec| dec.as_i64_with_ctx(ctx)).map(Some)
    }

    #[inline]
    pub fn ceil_dec_to_dec<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let d = try_opt!(self.children[0].eval_decimal(ctx, row));
        let result: Result<Decimal> = d.ceil().into();
        result.map(|t| Some(Cow::Owned(t)))
    }

    #[inline]
    pub fn ceil_int_to_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        self.children[0].eval_int(ctx, row)
    }

    #[inline]
    pub fn floor_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let n = try_opt!(self.children[0].eval_real(ctx, row));
        Ok(Some(n.floor()))
    }

    #[inline]
    pub fn floor_dec_to_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let d = try_opt!(self.children[0].eval_decimal(ctx, row));
        let d: Result<Decimal> = d.floor().into();
        d.and_then(|dec| dec.as_i64_with_ctx(ctx)).map(Some)
    }

    #[inline]
    pub fn floor_dec_to_dec<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let d = try_opt!(self.children[0].eval_decimal(ctx, row));
        let result: Result<Decimal> = d.floor().into();
        result.map(|t| Some(Cow::Owned(t)))
    }

    #[inline]
    pub fn floor_int_to_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        self.children[0].eval_int(ctx, row)
    }

    #[inline]
    pub fn log2(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let n = try_opt!(self.children[0].eval_real(ctx, row));
        let res = n.log2();
        if res.is_finite() {
            Ok(Some(res))
        } else {
            Ok(None)
        }
    }

    #[inline]
    pub fn log10(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let n = try_opt!(self.children[0].eval_real(ctx, row));
        let res = n.log10();
        if res.is_finite() {
            Ok(Some(res))
        } else {
            Ok(None)
        }
    }

    #[inline]
    pub fn crc32(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let d = try_opt!(self.children[0].eval_string(ctx, row));
        let mut digest = crc32::Digest::new(crc32::IEEE);
        digest.write(&d);
        Ok(Some(i64::from(digest.sum32())))
    }
}

#[cfg(test)]
mod test {
    use coprocessor::codec::mysql::types;
    use coprocessor::codec::{convert, mysql, Datum};
    use coprocessor::dag::expr::test::{check_overflow, datum_expr, scalar_func_expr, str2dec};
    use coprocessor::dag::expr::{EvalConfig, EvalContext, Expression, FLAG_IGNORE_TRUNCATE};
    use std::sync::Arc;
    use std::{f64, i64, u64};
    use tipb::expression::ScalarFuncSig;

    #[test]
    fn test_abs() {
        let tests = vec![
            (ScalarFuncSig::AbsInt, Datum::I64(-3), Datum::I64(3)),
            (
                ScalarFuncSig::AbsInt,
                Datum::I64(i64::MAX),
                Datum::I64(i64::MAX),
            ),
            (
                ScalarFuncSig::AbsUInt,
                Datum::U64(u64::MAX),
                Datum::U64(u64::MAX),
            ),
            (ScalarFuncSig::AbsReal, Datum::F64(3.5), Datum::F64(3.5)),
            (ScalarFuncSig::AbsReal, Datum::F64(-3.5), Datum::F64(3.5)),
            (ScalarFuncSig::AbsDecimal, str2dec("1.1"), str2dec("1.1")),
            (ScalarFuncSig::AbsDecimal, str2dec("-1.1"), str2dec("1.1")),
        ];
        let mut ctx = EvalContext::default();
        for (sig, arg, exp) in tests {
            let arg = datum_expr(arg);
            let mut f = scalar_func_expr(sig, &[arg]);
            if sig == ScalarFuncSig::AbsUInt {
                f.mut_field_type().set_flag(types::UNSIGNED_FLAG as u32);
            }
            let op = Expression::build(&mut ctx, f).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_overflow_abs() {
        let tests = vec![(ScalarFuncSig::AbsInt, Datum::I64(i64::MIN))];
        let mut ctx = EvalContext::default();
        for tt in tests {
            let arg = datum_expr(tt.1);
            let op = Expression::build(&mut ctx, scalar_func_expr(tt.0, &[arg])).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap_err();
            assert!(check_overflow(got).is_ok());
        }
    }

    #[test]
    fn test_ceil() {
        let tests = vec![
            (ScalarFuncSig::CeilReal, Datum::F64(3.45), Datum::F64(4f64)),
            (
                ScalarFuncSig::CeilReal,
                Datum::F64(-3.45),
                Datum::F64(-3f64),
            ),
            (
                ScalarFuncSig::CeilReal,
                Datum::F64(f64::MAX),
                Datum::F64(f64::MAX),
            ),
            (
                ScalarFuncSig::CeilReal,
                Datum::F64(f64::MIN),
                Datum::F64(f64::MIN),
            ),
            (
                ScalarFuncSig::CeilIntToInt,
                Datum::I64(i64::MIN),
                Datum::I64(i64::MIN),
            ),
            (
                ScalarFuncSig::CeilIntToInt,
                Datum::U64(u64::MAX),
                Datum::U64(u64::MAX),
            ),
            (
                ScalarFuncSig::CeilIntToDec,
                Datum::I64(i64::MIN),
                str2dec("-9223372036854775808"),
            ),
            (
                ScalarFuncSig::CeilDecToInt,
                str2dec("123.456"),
                Datum::I64(124),
            ),
            (
                ScalarFuncSig::CeilDecToInt,
                str2dec("-123.456"),
                Datum::I64(-123),
            ),
            (
                ScalarFuncSig::CeilDecToDec,
                str2dec("9223372036854775808"),
                str2dec("9223372036854775808"),
            ),
        ];

        // for ceil decimal to int.
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::new(FLAG_IGNORE_TRUNCATE).unwrap()));
        for (sig, arg, exp) in tests {
            let arg = datum_expr(arg);
            let mut op =
                Expression::build(&mut ctx, scalar_func_expr(sig, &[arg.clone()])).unwrap();
            if mysql::has_unsigned_flag(arg.get_field_type().get_flag()) {
                op.mut_tp().set_flag(types::UNSIGNED_FLAG as u32);
            }
            if sig == ScalarFuncSig::CeilIntToDec || sig == ScalarFuncSig::CeilDecToDec {
                op.mut_tp().set_flen(convert::UNSPECIFIED_LENGTH);
                op.mut_tp().set_decimal(convert::UNSPECIFIED_LENGTH);
            }
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_floor() {
        let tests = vec![
            (ScalarFuncSig::FloorReal, Datum::F64(3.45), Datum::F64(3f64)),
            (
                ScalarFuncSig::FloorReal,
                Datum::F64(-3.45),
                Datum::F64(-4f64),
            ),
            (
                ScalarFuncSig::FloorReal,
                Datum::F64(f64::MAX),
                Datum::F64(f64::MAX),
            ),
            (
                ScalarFuncSig::FloorReal,
                Datum::F64(f64::MIN),
                Datum::F64(f64::MIN),
            ),
            (
                ScalarFuncSig::FloorIntToInt,
                Datum::I64(i64::MIN),
                Datum::I64(i64::MIN),
            ),
            (
                ScalarFuncSig::FloorIntToInt,
                Datum::U64(u64::MAX),
                Datum::U64(u64::MAX),
            ),
            (
                ScalarFuncSig::FloorIntToDec,
                Datum::I64(i64::MIN),
                str2dec("-9223372036854775808"),
            ),
            (
                ScalarFuncSig::FloorDecToInt,
                str2dec("123.456"),
                Datum::I64(123),
            ),
            (
                ScalarFuncSig::FloorDecToInt,
                str2dec("-123.456"),
                Datum::I64(-124),
            ),
            (
                ScalarFuncSig::FloorDecToDec,
                str2dec("9223372036854775808"),
                str2dec("9223372036854775808"),
            ),
        ];
        // for ceil decimal to int.
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::new(FLAG_IGNORE_TRUNCATE).unwrap()));
        for (sig, arg, exp) in tests {
            let arg = datum_expr(arg);
            let mut op =
                Expression::build(&mut ctx, scalar_func_expr(sig, &[arg.clone()])).unwrap();
            if mysql::has_unsigned_flag(arg.get_field_type().get_flag()) {
                op.mut_tp().set_flag(types::UNSIGNED_FLAG as u32);
            }
            if sig == ScalarFuncSig::FloorIntToDec || sig == ScalarFuncSig::FloorDecToDec {
                op.mut_tp().set_flen(convert::UNSPECIFIED_LENGTH);
                op.mut_tp().set_decimal(convert::UNSPECIFIED_LENGTH);
            }
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_log2() {
        let tests = vec![
            (ScalarFuncSig::Log2, Datum::F64(16f64), Datum::F64(4f64)),
            (
                ScalarFuncSig::Log2,
                Datum::F64(5f64),
                Datum::F64(2.321928094887362f64),
            ),
            (ScalarFuncSig::Log2, Datum::F64(-1.234f64), Datum::Null),
            (ScalarFuncSig::Log2, Datum::F64(0f64), Datum::Null),
        ];
        let mut ctx = EvalContext::default();
        for (sig, arg, exp) in tests {
            let arg = datum_expr(arg);
            let f = scalar_func_expr(sig, &[arg]);
            let op = Expression::build(&mut ctx, f).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_log10() {
        let tests = vec![
            (ScalarFuncSig::Log10, Datum::F64(100f64), Datum::F64(2f64)),
            (
                ScalarFuncSig::Log10,
                Datum::F64(101f64),
                Datum::F64(2.0043213737826426f64),
            ),
            (ScalarFuncSig::Log10, Datum::F64(-0.23323f64), Datum::Null),
            (ScalarFuncSig::Log10, Datum::F64(0f64), Datum::Null),
        ];
        let mut ctx = EvalContext::default();
        for (sig, arg, exp) in tests {
            let arg = datum_expr(arg);
            let f = scalar_func_expr(sig, &[arg]);
            let op = Expression::build(&mut ctx, f).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_crc32() {
        let cases: Vec<(&'static str, i64)> = vec![
            ("", 0),
            ("-1", 808273962),
            ("mysql", 2501908538),
            ("MySQL", 3259397556),
            ("hello", 907060870),
            ("❤️", 4067711813),
        ];

        let mut ctx = EvalContext::default();

        for (arg, exp) in cases {
            let arg = datum_expr(Datum::Bytes(arg.as_bytes().to_vec()));
            let op = scalar_func_expr(ScalarFuncSig::CRC32, &[arg]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::I64(exp);
            assert_eq!(got, exp);
        }
    }
}
