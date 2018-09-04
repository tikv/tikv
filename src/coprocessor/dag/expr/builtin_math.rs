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
use num::traits::Pow;
use std::borrow::Cow;
use std::{f64, i64};

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
    pub fn pi(&self, _ctx: &mut EvalContext, _row: &[Datum]) -> Result<Option<f64>> {
        Ok(Some(f64::consts::PI))
    }

    #[inline]
    pub fn crc32(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let d = try_opt!(self.children[0].eval_string(ctx, row));
        let mut digest = crc32::Digest::new(crc32::IEEE);
        digest.write(&d);
        Ok(Some(i64::from(digest.sum32())))
    }

    #[inline]
    pub fn sign(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let f = try_opt!(self.children[0].eval_real(ctx, row));
        if f > 0f64 {
            Ok(Some(1))
        } else if f == 0f64 {
            Ok(Some(0))
        } else {
            Ok(Some(-1))
        }
    }

    #[inline]
    pub fn sqrt(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let f = try_opt!(self.children[0].eval_real(ctx, row)) as f64;
        if f < 0f64 {
            Ok(None)
        } else {
            Ok(Some(f.sqrt()))
        }
    }

    #[inline]
    pub fn cos(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let n = try_opt!(self.children[0].eval_real(ctx, row));
        Ok(Some(n.cos()))
    }

    #[inline]
    pub fn tan(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let n = try_opt!(self.children[0].eval_real(ctx, row));
        Ok(Some(n.tan()))
    }

    #[inline]
    pub fn atan_1_arg(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let f = try_opt!(self.children[0].eval_real(ctx, row));
        Ok(Some(f.atan()))
    }

    #[inline]
    pub fn atan_2_args(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let y = try_opt!(self.children[0].eval_real(ctx, row));
        let x = try_opt!(self.children[1].eval_real(ctx, row));
        Ok(Some(y.atan2(x)))
    }

    #[inline]
    pub fn sin(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let n = try_opt!(self.children[0].eval_real(ctx, row));
        Ok(Some(n.sin()))
    }

    #[inline]
    pub fn pow(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let x = try_opt!(self.children[0].eval_real(ctx, row));
        let y = try_opt!(self.children[1].eval_real(ctx, row));
        let pow = x.pow(y);
        if pow.is_infinite() || pow.is_nan() {
            return Err(Error::overflow("DOUBLE", &format!("{}.pow({})", x, y)));
        }
        Ok(Some(pow))
    }

    #[inline]
    pub fn asin(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let n = try_opt!(self.children[0].eval_real(ctx, row)) as f64;
        if n >= -1f64 && n <= 1f64 {
            Ok(Some(n.asin()))
        } else {
            Ok(None)
        }
    }

    #[inline]
    pub fn acos(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let n = try_opt!(self.children[0].eval_real(ctx, row)) as f64;
        if n >= -1f64 && n <= 1f64 {
            Ok(Some(n.acos()))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod test {
    use coprocessor::codec::mysql::types;
    use coprocessor::codec::{convert, mysql, Datum};
    use coprocessor::dag::expr::test::{check_overflow, datum_expr, scalar_func_expr, str2dec};
    use coprocessor::dag::expr::{EvalConfig, EvalContext, Expression};
    use std::f64::consts::{FRAC_1_SQRT_2, PI};
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
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::default_for_test()));
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
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::new().set_ignore_truncate(true)));
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
    fn test_pi() {
        let mut ctx = EvalContext::default();
        let op = Expression::build(&mut ctx, scalar_func_expr(ScalarFuncSig::PI, &[])).unwrap();
        let got = op.eval(&mut ctx, &[]).unwrap();
        assert_eq!(got, Datum::F64(f64::consts::PI));
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

    #[test]
    fn test_sign() {
        let tests = vec![
            (Datum::F64(42f64), Datum::I64(1)),
            (Datum::F64(0f64), Datum::I64(0)),
            (Datum::F64(-47f64), Datum::I64(-1)),
            (Datum::Null, Datum::Null),
        ];

        let mut ctx = EvalContext::default();

        for (arg, exp) in tests {
            let arg = datum_expr(arg);
            let op = scalar_func_expr(ScalarFuncSig::Sign, &[arg]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_sqrt() {
        let tests = vec![
            (Datum::F64(64f64), Datum::F64(8f64)),
            (Datum::F64(2f64), Datum::F64(f64::consts::SQRT_2)),
            (Datum::F64(-16f64), Datum::Null),
            (Datum::Null, Datum::Null),
        ];

        let mut ctx = EvalContext::default();

        for (arg, exp) in tests {
            let arg = datum_expr(arg);
            let op = scalar_func_expr(ScalarFuncSig::Sqrt, &[arg]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_cos() {
        let tests = vec![
            (ScalarFuncSig::Cos, Datum::F64(0f64), 1f64),
            (ScalarFuncSig::Cos, Datum::F64(f64::consts::PI / 2f64), 0f64),
            (ScalarFuncSig::Cos, Datum::F64(f64::consts::PI), -1f64),
            (ScalarFuncSig::Cos, Datum::F64(-f64::consts::PI), -1f64),
        ];
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::new().set_ignore_truncate(true)));
        for (sig, arg, exp) in tests {
            let arg = datum_expr(arg);
            let expr = scalar_func_expr(sig, &[arg.clone()]);
            let op = Expression::build(&mut ctx, expr).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            match got {
                Datum::F64(result) => assert!((result - exp).abs() < f64::EPSILON),
                _ => panic!("F64 result was expected"),
            }
        }
    }

    #[test]
    fn test_tan() {
        let tests = vec![
            (ScalarFuncSig::Tan, Datum::F64(0.0_f64), 0.0_f64),
            (
                ScalarFuncSig::Tan,
                Datum::F64(f64::consts::PI / 4.0_f64),
                1.0_f64,
            ),
            (
                ScalarFuncSig::Tan,
                Datum::F64(-f64::consts::PI / 4.0_f64),
                -1.0_f64,
            ),
            (ScalarFuncSig::Tan, Datum::F64(f64::consts::PI), 0.0_f64),
            (
                ScalarFuncSig::Tan,
                Datum::F64((f64::consts::PI * 3.0) / 4.0),
                f64::tan((f64::consts::PI * 3.0) / 4.0), //in mysql and rust, it equals -1.0000000000000002, not -1
            ),
        ];
        let tests_invalid_f64 = vec![
            (ScalarFuncSig::Tan, Datum::F64(f64::INFINITY)),
            (ScalarFuncSig::Tan, Datum::F64(f64::NAN)),
        ];
        let mut ctx = EvalContext::default();
        for (sig, arg, exp) in tests {
            let arg = datum_expr(arg);
            let f = scalar_func_expr(sig, &[arg]);
            let op = Expression::build(&mut ctx, f).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert!((got.f64() - exp).abs() < f64::EPSILON);
        }
        for (sig, arg) in tests_invalid_f64 {
            let arg = datum_expr(arg);
            let f = scalar_func_expr(sig, &[arg]);
            let op = Expression::build(&mut ctx, f).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert!(got.f64().is_nan());
        }
    }

    #[test]
    fn test_atan_1_arg() {
        let tests = vec![
            (Datum::Null, Datum::Null),
            (Datum::F64(1.0_f64), Datum::F64(f64::consts::PI / 4.0_f64)),
            (Datum::F64(-1.0_f64), Datum::F64(-f64::consts::PI / 4.0_f64)),
            (Datum::F64(f64::MAX), Datum::F64(f64::consts::PI / 2.0_f64)),
            (Datum::F64(f64::MIN), Datum::F64(-f64::consts::PI / 2.0_f64)),
            (Datum::F64(0.0_f64), Datum::F64(0.0_f64)),
        ];

        let mut ctx = EvalContext::default();

        for (arg, exp) in tests {
            let arg = datum_expr(arg);
            let op = scalar_func_expr(ScalarFuncSig::Atan1Arg, &[arg]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_atan_2_args() {
        let tests = vec![
            (Datum::Null, Datum::Null, Datum::Null),
            (
                Datum::F64(0.0_f64),
                Datum::F64(0.0_f64),
                Datum::F64(0.0_f64),
            ),
            (
                Datum::F64(0.0_f64),
                Datum::F64(-1.0_f64),
                Datum::F64(f64::consts::PI),
            ),
            (
                Datum::F64(1.0_f64),
                Datum::F64(-1.0_f64),
                Datum::F64(3.0_f64 * f64::consts::PI / 4.0_f64),
            ),
            (
                Datum::F64(-1.0_f64),
                Datum::F64(1.0_f64),
                Datum::F64(-f64::consts::PI / 4.0_f64),
            ),
            (
                Datum::F64(1.0_f64),
                Datum::F64(0.0_f64),
                Datum::F64(f64::consts::PI / 2.0_f64),
            ),
        ];

        let mut ctx = EvalContext::default();

        for (arg0, arg1, exp) in tests {
            let arg0 = datum_expr(arg0);
            let arg1 = datum_expr(arg1);
            let op = scalar_func_expr(ScalarFuncSig::Atan2Args, &[arg0, arg1]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_sin() {
        let tests = vec![
            (ScalarFuncSig::Sin, Datum::F64(0.0_f64), 0.0_f64),
            (ScalarFuncSig::Sin, Datum::F64(PI / 4.0_f64), FRAC_1_SQRT_2),
            (ScalarFuncSig::Sin, Datum::F64(PI / 2.0_f64), 1.0_f64),
            (ScalarFuncSig::Sin, Datum::F64(PI), 0.0_f64),
        ];
        let tests_invalid_f64 = vec![
            (ScalarFuncSig::Sin, Datum::F64(f64::INFINITY)),
            (ScalarFuncSig::Sin, Datum::F64(f64::NAN)),
        ];
        let mut ctx = EvalContext::default();
        for (sig, arg, exp) in tests {
            let arg = datum_expr(arg);
            let f = scalar_func_expr(sig, &[arg]);
            let op = Expression::build(&mut ctx, f).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert!((got.f64() - exp).abs() < f64::EPSILON);
        }
        for (sig, arg) in tests_invalid_f64 {
            let arg = datum_expr(arg);
            let f = scalar_func_expr(sig, &[arg]);
            let op = Expression::build(&mut ctx, f).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert!(got.f64().is_nan());
        }
    }

    #[test]
    fn test_pow() {
        let tests = vec![
            (
                ScalarFuncSig::Pow,
                Datum::F64(1.0),
                Datum::F64(3.0),
                Datum::F64(1.0),
            ),
            (
                ScalarFuncSig::Pow,
                Datum::F64(3.0),
                Datum::F64(0.0),
                Datum::F64(1.0),
            ),
            (
                ScalarFuncSig::Pow,
                Datum::F64(2.0),
                Datum::F64(4.0),
                Datum::F64(16.0),
            ),
        ];
        let mut ctx = EvalContext::default();
        for (sig, arg0, arg1, exp) in tests {
            let arg0 = datum_expr(arg0);
            let arg1 = datum_expr(arg1);
            let mut f = scalar_func_expr(sig, &[arg0, arg1]);
            let op = Expression::build(&mut ctx, f).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_pow_overflow() {
        let tests = vec![(ScalarFuncSig::Pow, Datum::F64(2.0), Datum::F64(300000000.0))];
        let mut ctx = EvalContext::default();
        for (sig, arg0, arg1) in tests {
            let arg0 = datum_expr(arg0);
            let arg1 = datum_expr(arg1);
            let mut f = scalar_func_expr(sig, &[arg0, arg1]);
            let op = Expression::build(&mut ctx, f).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap_err();
            assert!(check_overflow(got).is_ok());
        }
    }

    #[test]
    fn test_asin() {
        let tests = vec![
            (ScalarFuncSig::Asin, Datum::F64(0.0_f64), 0.0_f64),
            (
                ScalarFuncSig::Asin,
                Datum::F64(1f64),
                f64::consts::PI / 2.0_f64,
            ),
            (
                ScalarFuncSig::Asin,
                Datum::F64(-1f64),
                -f64::consts::PI / 2.0_f64,
            ),
            (
                ScalarFuncSig::Asin,
                Datum::F64(f64::consts::SQRT_2 / 2.0_f64),
                f64::consts::PI / 4.0_f64,
            ),
        ];
        let tests_invalid_f64 = vec![
            (ScalarFuncSig::Asin, Datum::F64(f64::INFINITY), Datum::Null),
            (ScalarFuncSig::Asin, Datum::F64(f64::NAN), Datum::Null),
            (ScalarFuncSig::Asin, Datum::F64(2f64), Datum::Null),
            (ScalarFuncSig::Asin, Datum::F64(-2f64), Datum::Null),
        ];

        let mut ctx = EvalContext::default();
        for (sig, arg, exp) in tests {
            let arg = datum_expr(arg);
            let f = scalar_func_expr(sig, &[arg]);
            let op = Expression::build(&mut ctx, f).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert!((got.f64() - exp).abs() < f64::EPSILON);
        }
        for (sig, arg, exp) in tests_invalid_f64 {
            let arg = datum_expr(arg);
            let f = scalar_func_expr(sig, &[arg]);
            let op = Expression::build(&mut ctx, f).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_acos() {
        let tests = vec![
            (
                ScalarFuncSig::Acos,
                Datum::F64(0f64),
                f64::consts::PI / 2.0_f64,
            ),
            (ScalarFuncSig::Acos, Datum::F64(1f64), 0.0_f64),
            (ScalarFuncSig::Acos, Datum::F64(-1f64), f64::consts::PI),
            (
                ScalarFuncSig::Acos,
                Datum::F64(f64::consts::SQRT_2 / 2.0_f64),
                f64::consts::PI / 4.0_f64,
            ),
        ];
        let tests_invalid_f64 = vec![
            (ScalarFuncSig::Acos, Datum::F64(f64::INFINITY), Datum::Null),
            (ScalarFuncSig::Acos, Datum::F64(f64::NAN), Datum::Null),
            (ScalarFuncSig::Acos, Datum::F64(2f64), Datum::Null),
            (ScalarFuncSig::Acos, Datum::F64(-2f64), Datum::Null),
        ];

        let mut ctx = EvalContext::default();
        for (sig, arg, exp) in tests {
            let arg = datum_expr(arg);
            let f = scalar_func_expr(sig, &[arg]);
            let op = Expression::build(&mut ctx, f).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert!((got.f64() - exp).abs() < f64::EPSILON);
        }
        for (sig, arg, exp) in tests_invalid_f64 {
            let arg = datum_expr(arg);
            let f = scalar_func_expr(sig, &[arg]);
            let op = Expression::build(&mut ctx, f).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }
}
