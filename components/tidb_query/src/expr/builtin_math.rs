// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;
use std::{f64, i64};

use crc::{crc32, Hasher32};
use num::traits::Pow;
use rand::{Rng, SeedableRng};
use rand_xorshift::XorShiftRng;
use time;

use super::{Error, EvalContext, Result, ScalarFunc};
use crate::codec::mysql::{Decimal, RoundMode, DEFAULT_FSP};
use crate::codec::Datum;

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
    pub fn log_1_arg(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let n = try_opt!(self.children[0].eval_real(ctx, row));
        let res = n.ln();
        if res.is_finite() {
            Ok(Some(res))
        } else {
            Ok(None)
        }
    }

    #[inline]
    pub fn log_2_args(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let base = try_opt!(self.children[0].eval_real(ctx, row));
        let n = try_opt!(self.children[1].eval_real(ctx, row));
        let res = n.log(base);
        if res.is_finite() {
            Ok(Some(res))
        } else {
            Ok(None)
        }
    }

    #[inline]
    pub fn pi(&self, _ctx: &mut EvalContext, _row: &[Datum]) -> Result<Option<f64>> {
        Ok(Some(f64::consts::PI))
    }

    #[inline]
    pub fn rand(&self, _: &mut EvalContext, _: &[Datum]) -> Result<Option<f64>> {
        let mut cus_rng = self.cus_rng.rng.borrow_mut();
        if cus_rng.is_none() {
            let mut rand = get_rand(None);
            let res = rand.gen::<f64>();
            *cus_rng = Some(rand);
            Ok(Some(res))
        } else {
            let rand = cus_rng.as_mut().unwrap();
            let res = rand.gen::<f64>();
            Ok(Some(res))
        }
    }

    #[inline]
    pub fn rand_with_seed(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let seed = match self.children[0].eval_int(ctx, row)? {
            Some(v) => Some(v as u64),
            _ => None,
        };

        let mut cus_rng = self.cus_rng.rng.borrow_mut();
        if cus_rng.is_none() {
            let mut rand = get_rand(seed);
            let res = rand.gen::<f64>();
            *cus_rng = Some(rand);
            Ok(Some(res))
        } else {
            let rand = cus_rng.as_mut().unwrap();
            let res = rand.gen::<f64>();
            Ok(Some(res))
        }
    }

    #[inline]
    pub fn crc32(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let d = try_opt!(self.children[0].eval_string(ctx, row));
        let mut digest = crc32::Digest::new(crc32::IEEE);
        digest.write(&d);
        Ok(Some(i64::from(digest.sum32())))
    }

    #[inline]
    pub fn round_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        self.children[0].eval_int(ctx, row)
    }

    #[inline]
    pub fn round_dec<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let number = try_opt!(self.children[0].eval_decimal(ctx, row));
        let result: Result<Decimal> = number
            .into_owned()
            .round(DEFAULT_FSP, RoundMode::HalfEven)
            .into();
        result.map(|t| Some(Cow::Owned(t)))
    }

    #[inline]
    pub fn round_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let number = try_opt!(self.children[0].eval_real(ctx, row));
        Ok(Some(number.round()))
    }

    #[inline]
    pub fn round_with_frac_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let number = try_opt!(self.children[0].eval_int(ctx, row));
        let digits = try_opt!(self.children[1].eval_int(ctx, row));

        if digits >= 0 {
            Ok(Some(number))
        } else {
            let power = 10.0_f64.powi(-digits as i32);
            let frac = number as f64 / power;
            Ok(Some((frac.round() * power) as i64))
        }
    }

    #[inline]
    pub fn round_with_frac_real(
        &self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<f64>> {
        let number = try_opt!(self.children[0].eval_real(ctx, row));
        let digits = try_opt!(self.children[1].eval_int(ctx, row));

        let power = 10.0_f64.powi(-digits as i32);
        let frac = number / power;
        Ok(Some(frac.round() * power))
    }

    #[inline]
    pub fn round_with_frac_dec<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let number = try_opt!(self.children[0].eval_decimal(ctx, row));
        let digits = try_opt!(self.children[1].eval_int(ctx, row));

        let result: Result<Decimal> = number
            .into_owned()
            .round(digits as i8, RoundMode::HalfEven)
            .into();
        result.map(|t| Some(Cow::Owned(t)))
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

    #[inline]
    pub fn cot(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let n = try_opt!(self.children[0].eval_real(ctx, row));
        let tan = n.tan();
        if tan != 0.0 {
            let cot = 1.0 / tan;
            if !cot.is_infinite() && !cot.is_nan() {
                return Ok(Some(cot));
            }
        }
        Err(Error::overflow("DOUBLE", &format!("cot({})", n)))
    }

    #[inline]
    pub fn degrees(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let n = try_opt!(self.children[0].eval_real(ctx, row));
        Ok(Some(n.to_degrees()))
    }

    #[inline]
    pub fn truncate_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let x = try_opt!(self.children[0].eval_int(ctx, row));
        let d = try_opt!(self.children[1].eval_int(ctx, row));
        let d = if self.children[1].is_unsigned() { 0 } else { d };
        if d >= 0 {
            Ok(Some(x))
        } else if self.children[0].is_unsigned() {
            if d < -19 {
                return Ok(Some(0));
            }
            let x = x as u64;
            let shift = 10_u64.pow(-d as u32);
            Ok(Some((x / shift * shift) as i64))
        } else {
            if d < -18 {
                return Ok(Some(0));
            }
            let shift = 10_i64.pow(-d as u32);
            Ok(Some(x / shift * shift))
        }
    }

    #[inline]
    pub fn truncate_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let x = try_opt!(self.children[0].eval_real(ctx, row));
        let d = try_opt!(self.children[1].eval_int(ctx, row));
        let d = if self.children[1].is_unsigned() {
            (d as u64).min(i32::max_value() as u64) as i32
        } else if d >= 0 {
            d.min(i64::from(i32::max_value())) as i32
        } else {
            d.max(i64::from(i32::min_value())) as i32
        };
        let m = 10_f64.powi(d);
        let tmp = x * m;
        let r = if tmp == 0_f64 {
            0_f64
        } else if tmp.is_infinite() {
            x
        } else {
            tmp.trunc() / m
        };
        Ok(Some(r))
    }

    #[inline]
    pub fn truncate_decimal(
        &self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<Cow<'_, Decimal>>> {
        let x = try_opt!(self.children[0].eval_decimal(ctx, row));
        let d = try_opt!(self.children[1].eval_int(ctx, row));
        let d = if self.children[1].is_unsigned() {
            (d as u64).min(127) as i8
        } else if d >= 0 {
            d.min(127) as i8
        } else {
            d.max(-128) as i8
        };
        let r: Result<Decimal> = x.into_owned().round(d, RoundMode::Truncate).into();
        r.map(|t| Some(Cow::Owned(t)))
    }

    #[inline]
    pub fn radians(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let x = try_opt!(self.children[0].eval_real(ctx, row));
        Ok(Some(x * f64::consts::PI / 180_f64))
    }

    #[inline]
    pub fn exp(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let x = try_opt!(self.children[0].eval_real(ctx, row));
        let r = x.exp();
        if r.is_infinite() || r.is_nan() {
            return Err(Error::overflow("DOUBLE", &format!("exp({})", x)));
        }
        Ok(Some(r))
    }

    #[inline]
    pub fn conv<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let n = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        let mut from_base = try_opt!(self.children[1].eval_int(ctx, row));
        let mut to_base = try_opt!(self.children[2].eval_int(ctx, row));

        let mut negative = false;
        let mut signed = false;
        let mut ignore_sign = false;

        if from_base < 0 {
            from_base = -from_base;
            signed = true;
        }
        if to_base < 0 {
            to_base = -to_base;
            ignore_sign = true;
        }
        if from_base > 36 || from_base < 2 || to_base > 36 || to_base < 2 {
            return Ok(None);
        }

        let n = n.trim_start();
        let mut start = 0;
        let mut end = n.len();
        for (idx, c) in n.char_indices() {
            if idx == 0 {
                negative = c == '-';
                if c == '+' || c == '-' {
                    start = 1;
                    continue;
                }
            }
            if !c.is_digit(from_base as u32) {
                end = idx;
                break;
            }
        }
        let n = n.get(start..end).unwrap();
        if n.is_empty() {
            return Ok(Some(Cow::Borrowed(b"0")));
        }

        let mut value = u64::from_str_radix(n, from_base as u32).unwrap();
        if signed {
            value = if negative {
                value.min(-i64::min_value() as u64)
            } else {
                value.min(i64::max_value() as u64)
            };
        }
        let mut value = value as i64;
        if negative {
            value = -value;
        }
        negative = value < 0;

        if negative && ignore_sign {
            value = -value;
        }
        let mut r = format_radix(value as u64, to_base as u32);
        if negative && ignore_sign {
            r.insert(0, '-');
        }
        Ok(Some(Cow::Owned(r.into_bytes())))
    }
}

fn format_radix(mut x: u64, radix: u32) -> String {
    let mut r = vec![];
    loop {
        let m = x % u64::from(radix);
        x /= u64::from(radix);
        r.push(
            std::char::from_digit(m as u32, radix)
                .unwrap()
                .to_ascii_uppercase(),
        );
        if x == 0 {
            break;
        }
    }
    r.iter().rev().collect::<String>()
}

fn get_rand(arg: Option<u64>) -> XorShiftRng {
    let seed = match arg {
        Some(v) => v,
        None => {
            let current_time = time::get_time();
            let nsec = current_time.nsec as u64;
            let sec = (current_time.sec * 1000000000) as u64;
            sec + nsec
        }
    };
    SeedableRng::seed_from_u64(seed)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::f64::consts::{FRAC_1_SQRT_2, PI};
    use std::{f64, i64, u64};

    use tidb_query_datatype::{self, FieldTypeAccessor, FieldTypeFlag};
    use tipb::ScalarFuncSig;

    use crate::codec::Datum;
    use crate::expr::tests::{check_overflow, eval_func, eval_func_with, str2dec};

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
        for (sig, arg, exp) in tests {
            let got = eval_func_with(sig, &[arg], |op, _| {
                if sig == ScalarFuncSig::AbsUInt {
                    op.mut_field_type()
                        .as_mut_accessor()
                        .set_flag(FieldTypeFlag::UNSIGNED);
                }
            })
            .unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_overflow_abs() {
        let got = eval_func(ScalarFuncSig::AbsInt, &[Datum::I64(i64::MIN)]).unwrap_err();
        assert!(check_overflow(got).is_ok());
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
        for (sig, arg, exp) in tests {
            let got = eval_func_with(sig, &[arg], |op, args| {
                if args[0]
                    .get_field_type()
                    .as_accessor()
                    .flag()
                    .contains(FieldTypeFlag::UNSIGNED)
                {
                    op.mut_field_type()
                        .as_mut_accessor()
                        .set_flag(FieldTypeFlag::UNSIGNED);
                }
                if sig == ScalarFuncSig::CeilIntToDec || sig == ScalarFuncSig::CeilDecToDec {
                    op.mut_field_type()
                        .as_mut_accessor()
                        .set_flen(tidb_query_datatype::UNSPECIFIED_LENGTH)
                        .set_decimal(tidb_query_datatype::UNSPECIFIED_LENGTH);
                }
            })
            .unwrap();
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
        for (sig, arg, exp) in tests {
            let got = eval_func_with(sig, &[arg], |op, args| {
                if args[0]
                    .get_field_type()
                    .as_accessor()
                    .flag()
                    .contains(FieldTypeFlag::UNSIGNED)
                {
                    op.mut_field_type()
                        .as_mut_accessor()
                        .set_flag(FieldTypeFlag::UNSIGNED);
                }
                if sig == ScalarFuncSig::FloorIntToDec || sig == ScalarFuncSig::FloorDecToDec {
                    op.mut_field_type()
                        .as_mut_accessor()
                        .set_flen(tidb_query_datatype::UNSPECIFIED_LENGTH)
                        .set_decimal(tidb_query_datatype::UNSPECIFIED_LENGTH);
                }
            })
            .unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_log2() {
        let tests = vec![
            (Datum::F64(16_f64), 4_f64),
            (Datum::F64(5_f64), 2.321928094887362_f64),
        ];

        let tests_invalid_f64 = vec![
            (Datum::F64(-1.234_f64), Datum::Null),
            (Datum::F64(0_f64), Datum::Null),
            (Datum::Null, Datum::Null),
        ];

        for (arg, exp) in tests {
            let got = eval_func(ScalarFuncSig::Log2, &[arg]).unwrap();
            assert!((got.f64() - exp).abs() < f64::EPSILON);
        }

        for (arg, exp) in tests_invalid_f64 {
            let got = eval_func(ScalarFuncSig::Log2, &[arg]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_log10() {
        let tests = vec![
            (Datum::F64(100_f64), 2_f64),
            (Datum::F64(101_f64), 2.0043213737826426_f64),
        ];

        let tests_invalid_f64 = vec![
            (Datum::F64(-0.23323_f64), Datum::Null),
            (Datum::F64(0_f64), Datum::Null),
            (Datum::Null, Datum::Null),
        ];

        for (arg, exp) in tests {
            let got = eval_func(ScalarFuncSig::Log10, &[arg]).unwrap();
            assert!((got.f64() - exp).abs() < f64::EPSILON);
        }

        for (arg, exp) in tests_invalid_f64 {
            let got = eval_func(ScalarFuncSig::Log10, &[arg]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_log_1_arg() {
        let tests = vec![
            (Datum::F64(f64::consts::E), 1.0_f64),
            (Datum::F64(100_f64), 4.605170185988092_f64),
        ];

        let tests_invalid_f64 = vec![
            (Datum::F64(-1.0_f64), Datum::Null),
            (Datum::F64(0_f64), Datum::Null),
            (Datum::Null, Datum::Null),
        ];

        for (arg, exp) in tests {
            let got = eval_func(ScalarFuncSig::Log1Arg, &[arg]).unwrap();
            assert!((got.f64() - exp).abs() < f64::EPSILON);
        }

        for (arg, exp) in tests_invalid_f64 {
            let got = eval_func(ScalarFuncSig::Log1Arg, &[arg]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_log_2_args() {
        let tests = vec![
            (Datum::F64(10.0_f64), Datum::F64(100.0_f64), 2.0_f64),
            (Datum::F64(2.0_f64), Datum::F64(1.0_f64), 0.0_f64),
            (Datum::F64(0.5_f64), Datum::F64(0.25_f64), 2.0_f64),
        ];

        let tests_invalid_f64 = vec![
            (Datum::F64(-0.23323_f64), Datum::F64(2.0_f64), Datum::Null),
            (Datum::Null, Datum::Null, Datum::Null),
            (Datum::F64(2.0_f64), Datum::Null, Datum::Null),
            (Datum::Null, Datum::F64(2.0_f64), Datum::Null),
        ];

        for (arg0, arg1, exp) in tests {
            let got = eval_func(ScalarFuncSig::Log2Args, &[arg0, arg1]).unwrap();
            assert!((got.f64() - exp).abs() < f64::EPSILON);
        }

        for (arg0, arg1, exp) in tests_invalid_f64 {
            let got = eval_func(ScalarFuncSig::Log2Args, &[arg0, arg1]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_pi() {
        let got = eval_func(ScalarFuncSig::Pi, &[]).unwrap();
        assert_eq!(got, Datum::F64(f64::consts::PI));
    }

    #[test]
    fn test_rand() {
        let got = eval_func(ScalarFuncSig::Rand, &[Datum::Null])
            .unwrap()
            .as_real()
            .unwrap();

        assert!(got.is_some());
        assert!(got.unwrap() < 1.0);
        assert!(got.unwrap() >= 0.0);
    }

    #[test]
    fn test_rand_with_seed() {
        let seed: i64 = 20160101;
        let expect = eval_func(ScalarFuncSig::RandWithSeed, &[Datum::I64(seed)])
            .unwrap()
            .as_real()
            .unwrap()
            .unwrap()
            .to_bits();
        for _ in 1..3 {
            let got = eval_func(ScalarFuncSig::RandWithSeed, &[Datum::I64(seed)])
                .unwrap()
                .as_real()
                .unwrap();

            assert!(got.is_some());
            assert_eq!(got.unwrap().to_bits(), expect);
        }
        let mut set: HashSet<u64> = HashSet::new();
        let test_cnt = 1024;
        for i in seed + 1..=seed + test_cnt {
            let got = eval_func(ScalarFuncSig::RandWithSeed, &[Datum::I64(i)])
                .unwrap()
                .as_real()
                .unwrap()
                .unwrap()
                .to_bits();
            set.insert(got);
        }
        // If this assert failed, try to find another seed and retry.
        // If `test_cnt-set.len()` is not very large,
        // then this fail may be legal but not logical error of the code.
        assert_eq!(set.len(), test_cnt as usize);
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

        for (arg, exp) in cases {
            let arg = Datum::Bytes(arg.as_bytes().to_vec());
            let got = eval_func(ScalarFuncSig::Crc32, &[arg]).unwrap();
            let exp = Datum::I64(exp);
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_round() {
        let tests = vec![
            (ScalarFuncSig::RoundInt, Datum::I64(1), Datum::I64(1)),
            (
                ScalarFuncSig::RoundInt,
                Datum::I64(i64::MAX),
                Datum::I64(i64::MAX),
            ),
            (
                ScalarFuncSig::RoundInt,
                Datum::I64(i64::MIN),
                Datum::I64(i64::MIN),
            ),
            (
                ScalarFuncSig::RoundDec,
                str2dec("123.456"),
                str2dec("123.000"),
            ),
            (
                ScalarFuncSig::RoundDec,
                str2dec("123.656"),
                str2dec("124.000"),
            ),
            (
                ScalarFuncSig::RoundDec,
                str2dec("-123.456"),
                str2dec("-123.000"),
            ),
            (ScalarFuncSig::RoundReal, Datum::F64(3.45), Datum::F64(3f64)),
            (
                ScalarFuncSig::RoundReal,
                Datum::F64(-3.45),
                Datum::F64(-3f64),
            ),
            (
                ScalarFuncSig::RoundReal,
                Datum::F64(f64::MAX),
                Datum::F64(f64::MAX),
            ),
            (
                ScalarFuncSig::RoundReal,
                Datum::F64(f64::MIN),
                Datum::F64(f64::MIN),
            ),
        ];

        for (sig, arg, exp) in tests {
            let got = eval_func(sig, &[arg]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_round_frac() {
        let tests = vec![
            (
                ScalarFuncSig::RoundWithFracInt,
                Datum::I64(23),
                Datum::I64(2),
                Datum::I64(23),
            ),
            (
                ScalarFuncSig::RoundWithFracInt,
                Datum::I64(23),
                Datum::I64(-1),
                Datum::I64(20),
            ),
            (
                ScalarFuncSig::RoundWithFracInt,
                Datum::I64(-27),
                Datum::I64(-1),
                Datum::I64(-30),
            ),
            (
                ScalarFuncSig::RoundWithFracInt,
                Datum::I64(-27),
                Datum::I64(-2),
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::RoundWithFracInt,
                Datum::I64(-27),
                Datum::I64(-2),
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::RoundWithFracDec,
                str2dec("150.000"),
                Datum::I64(2),
                str2dec("150.00"),
            ),
            (
                ScalarFuncSig::RoundWithFracDec,
                str2dec("150.257"),
                Datum::I64(1),
                str2dec("150.3"),
            ),
            (
                ScalarFuncSig::RoundWithFracDec,
                str2dec("153.257"),
                Datum::I64(-1),
                str2dec("150"),
            ),
        ];

        let real_tests = vec![
            (Datum::F64(-1.298_f64), Datum::I64(1), -1.3_f64),
            (Datum::F64(-1.298_f64), Datum::I64(0), -1.0_f64),
            (Datum::F64(23.298_f64), Datum::I64(2), 23.30_f64),
            (Datum::F64(23.298_f64), Datum::I64(-1), 20.0_f64),
        ];

        for (sig, arg0, arg1, exp) in tests {
            let got = eval_func(sig, &[arg0, arg1]).unwrap();
            assert_eq!(got, exp);
        }

        for (arg0, arg1, exp) in real_tests {
            let got = eval_func(ScalarFuncSig::RoundWithFracReal, &[arg0, arg1]).unwrap();
            assert!((got.f64() - exp).abs() < f64::EPSILON);
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

        for (arg, exp) in tests {
            let got = eval_func(ScalarFuncSig::Sign, &[arg]).unwrap();
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

        for (arg, exp) in tests {
            let got = eval_func(ScalarFuncSig::Sqrt, &[arg]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_cos() {
        let tests = vec![
            (Datum::F64(0f64), 1f64),
            (Datum::F64(f64::consts::PI / 2f64), 0f64),
            (Datum::F64(f64::consts::PI), -1f64),
            (Datum::F64(-f64::consts::PI), -1f64),
        ];
        for (arg, exp) in tests {
            let got = eval_func(ScalarFuncSig::Cos, &[arg]).unwrap();
            match got {
                Datum::F64(result) => assert!((result - exp).abs() < f64::EPSILON),
                _ => panic!("F64 result was expected"),
            }
        }
    }

    #[test]
    fn test_tan() {
        let tests = vec![
            (Datum::F64(0.0_f64), 0.0_f64),
            (Datum::F64(f64::consts::PI / 4.0_f64), 1.0_f64),
            (Datum::F64(-f64::consts::PI / 4.0_f64), -1.0_f64),
            (Datum::F64(f64::consts::PI), 0.0_f64),
            (
                Datum::F64((f64::consts::PI * 3.0) / 4.0),
                f64::tan((f64::consts::PI * 3.0) / 4.0), //in mysql and rust, it equals -1.0000000000000002, not -1
            ),
        ];
        let tests_invalid_f64 = vec![Datum::F64(f64::INFINITY), Datum::F64(f64::NAN)];
        for (arg, exp) in tests {
            let got = eval_func(ScalarFuncSig::Tan, &[arg]).unwrap();
            assert!((got.f64() - exp).abs() < f64::EPSILON);
        }
        for arg in tests_invalid_f64 {
            let got = eval_func(ScalarFuncSig::Tan, &[arg]).unwrap();
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

        for (arg, exp) in tests {
            let got = eval_func(ScalarFuncSig::Atan1Arg, &[arg]).unwrap();
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

        for (arg0, arg1, exp) in tests {
            let got = eval_func(ScalarFuncSig::Atan2Args, &[arg0, arg1]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_sin() {
        let tests = vec![
            (Datum::F64(0.0_f64), 0.0_f64),
            (Datum::F64(PI / 4.0_f64), FRAC_1_SQRT_2),
            (Datum::F64(PI / 2.0_f64), 1.0_f64),
            (Datum::F64(PI), 0.0_f64),
        ];
        let tests_invalid_f64 = vec![Datum::F64(f64::INFINITY), Datum::F64(f64::NAN)];
        for (arg, exp) in tests {
            let got = eval_func(ScalarFuncSig::Sin, &[arg]).unwrap();
            assert!((got.f64() - exp).abs() < f64::EPSILON);
        }
        for arg in tests_invalid_f64 {
            let got = eval_func(ScalarFuncSig::Sin, &[arg]).unwrap();
            assert!(got.f64().is_nan());
        }
    }

    #[test]
    fn test_pow() {
        let tests = vec![
            (Datum::F64(1.0), Datum::F64(3.0), Datum::F64(1.0)),
            (Datum::F64(3.0), Datum::F64(0.0), Datum::F64(1.0)),
            (Datum::F64(2.0), Datum::F64(4.0), Datum::F64(16.0)),
        ];
        for (arg0, arg1, exp) in tests {
            let got = eval_func(ScalarFuncSig::Pow, &[arg0, arg1]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_pow_overflow() {
        let tests = vec![(Datum::F64(2.0), Datum::F64(300000000.0))];
        for (arg0, arg1) in tests {
            let got = eval_func(ScalarFuncSig::Pow, &[arg0, arg1]).unwrap_err();
            assert!(check_overflow(got).is_ok());
        }
    }

    #[test]
    fn test_asin() {
        let tests = vec![
            (Datum::F64(0.0_f64), 0.0_f64),
            (Datum::F64(1f64), f64::consts::PI / 2.0_f64),
            (Datum::F64(-1f64), -f64::consts::PI / 2.0_f64),
            (
                Datum::F64(f64::consts::SQRT_2 / 2.0_f64),
                f64::consts::PI / 4.0_f64,
            ),
        ];
        let tests_invalid_f64 = vec![
            (Datum::F64(f64::INFINITY), Datum::Null),
            (Datum::F64(f64::NAN), Datum::Null),
            (Datum::F64(2f64), Datum::Null),
            (Datum::F64(-2f64), Datum::Null),
        ];

        for (arg, exp) in tests {
            let got = eval_func(ScalarFuncSig::Asin, &[arg]).unwrap();
            assert!((got.f64() - exp).abs() < f64::EPSILON);
        }
        for (arg, exp) in tests_invalid_f64 {
            let got = eval_func(ScalarFuncSig::Asin, &[arg]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_acos() {
        let tests = vec![
            (Datum::F64(0f64), f64::consts::PI / 2.0_f64),
            (Datum::F64(1f64), 0.0_f64),
            (Datum::F64(-1f64), f64::consts::PI),
            (
                Datum::F64(f64::consts::SQRT_2 / 2.0_f64),
                f64::consts::PI / 4.0_f64,
            ),
        ];
        let tests_invalid_f64 = vec![
            (Datum::F64(f64::INFINITY), Datum::Null),
            (Datum::F64(f64::NAN), Datum::Null),
            (Datum::F64(2f64), Datum::Null),
            (Datum::F64(-2f64), Datum::Null),
        ];

        for (arg, exp) in tests {
            let got = eval_func(ScalarFuncSig::Acos, &[arg]).unwrap();
            assert!((got.f64() - exp).abs() < f64::EPSILON);
        }
        for (arg, exp) in tests_invalid_f64 {
            let got = eval_func(ScalarFuncSig::Acos, &[arg]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_cot() {
        let tests = vec![
            (Datum::F64(-1.0), -0.6420926159343308_f64),
            (Datum::F64(1.0), 0.6420926159343308_f64),
            (
                Datum::F64(f64::consts::PI / 4.0_f64),
                1.0_f64 / f64::tan(f64::consts::PI / 4.0_f64),
            ),
            (
                Datum::F64(f64::consts::PI / 2.0_f64),
                1.0_f64 / f64::tan(f64::consts::PI / 2.0_f64),
            ),
            (
                Datum::F64(f64::consts::PI),
                1.0_f64 / f64::tan(f64::consts::PI),
            ),
        ];

        for (arg, exp) in tests {
            let got = eval_func(ScalarFuncSig::Cot, &[arg]).unwrap();
            assert!((got.f64() - exp).abs() < f64::EPSILON);
        }

        // for overflow
        let tests_invalid_f64 = vec![Datum::F64(0.0)];

        for arg in tests_invalid_f64 {
            let got = eval_func(ScalarFuncSig::Cot, &[arg]).unwrap_err();
            assert!(check_overflow(got).is_ok());
        }
    }

    #[test]
    fn test_degrees() {
        let tests = vec![
            (Datum::F64(0.0), -0.0_f64),
            (Datum::F64(1.0), 57.29577951308232_f64),
            (Datum::F64(f64::consts::PI), 180.0_f64),
            (Datum::F64(-f64::consts::PI / 2.0_f64), -90.0_f64),
        ];

        for (arg, exp) in tests {
            let got = eval_func(ScalarFuncSig::Degrees, &[arg]).unwrap();
            assert!((got.f64() - exp).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn test_truncate_int() {
        let tests = vec![
            (Datum::I64(1028), Datum::I64(0), Datum::I64(1028)),
            (Datum::I64(1028), Datum::I64(5), Datum::I64(1028)),
            (Datum::I64(1028), Datum::I64(-2), Datum::I64(1000)),
            (Datum::I64(1028), Datum::I64(309), Datum::I64(1028)),
            (
                Datum::I64(1028),
                Datum::I64(i64::min_value()),
                Datum::I64(0),
            ),
            (
                Datum::I64(1028),
                Datum::U64(u64::max_value()),
                Datum::I64(1028),
            ),
            (
                Datum::U64(18446744073709551615),
                Datum::I64(-2),
                Datum::U64(18446744073709551600),
            ),
            (
                Datum::U64(18446744073709551615),
                Datum::I64(-20),
                Datum::U64(0),
            ),
            (
                Datum::U64(18446744073709551615),
                Datum::I64(2),
                Datum::U64(18446744073709551615),
            ),
        ];
        for (x, d, exp) in tests {
            let got = eval_func_with(ScalarFuncSig::TruncateInt, &[x, d], |op, args| {
                if args[0]
                    .get_field_type()
                    .as_accessor()
                    .flag()
                    .contains(FieldTypeFlag::UNSIGNED)
                {
                    op.mut_field_type()
                        .as_mut_accessor()
                        .set_flag(FieldTypeFlag::UNSIGNED);
                }
            })
            .unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_truncate_real() {
        let tests = vec![
            (Datum::F64(-1.23), Datum::I64(0), Datum::F64(-1.0)),
            (Datum::F64(1.58), Datum::I64(0), Datum::F64(1.0)),
            (Datum::F64(1.298), Datum::I64(1), Datum::F64(1.2)),
            (Datum::F64(123.2), Datum::I64(-1), Datum::F64(120.0)),
            (Datum::F64(123.2), Datum::I64(100), Datum::F64(123.2)),
            (Datum::F64(123.2), Datum::I64(-100), Datum::F64(0.0)),
            (
                Datum::F64(123.2),
                Datum::I64(i64::max_value()),
                Datum::F64(123.2),
            ),
            (
                Datum::F64(123.2),
                Datum::I64(i64::min_value()),
                Datum::F64(0.0),
            ),
            (
                Datum::F64(123.2),
                Datum::U64(u64::max_value()),
                Datum::F64(123.2),
            ),
            (Datum::F64(-1.23), Datum::I64(0), Datum::F64(-1.0)),
            (
                Datum::F64(1.797693134862315708145274237317043567981e+308),
                Datum::I64(2),
                Datum::F64(1.797693134862315708145274237317043567981e+308),
            ),
        ];
        for (x, d, exp) in tests {
            let got = eval_func(ScalarFuncSig::TruncateReal, &[x, d]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_truncate_decimal() {
        let tests = vec![
            (str2dec("-1.23"), Datum::I64(0), str2dec("-1")),
            (str2dec("-1.23"), Datum::I64(1), str2dec("-1.2")),
            (str2dec("-11.23"), Datum::I64(-1), str2dec("-10")),
            (str2dec("1.58"), Datum::I64(0), str2dec("1")),
            (str2dec("1.58"), Datum::I64(1), str2dec("1.5")),
            (str2dec("23.298"), Datum::I64(-1), str2dec("20")),
            (str2dec("23.298"), Datum::I64(-100), str2dec("0")),
            (str2dec("23.298"), Datum::I64(100), str2dec("23.298")),
            (str2dec("23.298"), Datum::I64(200), str2dec("23.298")),
            (str2dec("23.298"), Datum::I64(-200), str2dec("0")),
            (
                str2dec("23.298"),
                Datum::U64(u64::max_value()),
                str2dec("23.298"),
            ),
            (
                str2dec("1.999999999999999999999999999999"),
                Datum::I64(31),
                str2dec("1.999999999999999999999999999999"),
            ),
            (
                str2dec("99999999999999999999999999999999999999999999999999999999999999999"),
                Datum::I64(-66),
                str2dec("0"),
            ),
            (
                str2dec("99999999999999999999999999999999999.999999999999999999999999999999"),
                Datum::I64(31),
                str2dec("99999999999999999999999999999999999.999999999999999999999999999999"),
            ),
            (
                str2dec("99999999999999999999999999999999999.999999999999999999999999999999"),
                Datum::I64(-36),
                str2dec("0"),
            ),
        ];
        for (x, d, exp) in tests {
            let got = eval_func(ScalarFuncSig::TruncateDecimal, &[x, d]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_radians() {
        let tests = vec![
            (Datum::F64(0_f64), Datum::F64(0_f64)),
            (Datum::F64(180_f64), Datum::F64(PI)),
            (Datum::F64(-360_f64), Datum::F64(-2_f64 * PI)),
            (Datum::Null, Datum::Null),
            (Datum::F64(f64::INFINITY), Datum::F64(f64::INFINITY)),
        ];
        for (arg, exp) in tests {
            let got = eval_func(ScalarFuncSig::Radians, &[arg]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_exp() {
        let tests = vec![
            (Datum::F64(1_f64), Datum::F64(f64::consts::E)),
            (Datum::F64(1.23_f64), Datum::F64(3.4212295362896734)),
            (Datum::F64(-1.23_f64), Datum::F64(0.2922925776808594)),
            (Datum::F64(0_f64), Datum::F64(1_f64)),
        ];
        for (x, expected) in tests {
            let got = eval_func(ScalarFuncSig::Exp, &[x]).unwrap();
            assert_eq!(got, expected);
        }

        let overflow_tests = vec![Datum::F64(100000_f64), Datum::F64(f64::NAN)];
        for x in overflow_tests {
            let got = eval_func(ScalarFuncSig::Exp, &[x]).unwrap_err();
            assert!(check_overflow(got).is_ok());
        }
    }

    #[test]
    fn test_conv() {
        let tests = vec![
            ("a", 16, 2, "1010"),
            ("6E", 18, 8, "172"),
            ("-17", 10, -18, "-H"),
            ("  -17", 10, -18, "-H"),
            ("-17", 10, 18, "2D3FGB0B9CG4BD1H"),
            ("+18aZ", 7, 36, "1"),
            ("  +18aZ", 7, 36, "1"),
            ("18446744073709551615", -10, 16, "7FFFFFFFFFFFFFFF"),
            ("12F", -10, 16, "C"),
            ("  FF ", 16, 10, "255"),
            ("TIDB", 10, 8, "0"),
            ("aa", 10, 2, "0"),
            (" A", -10, 16, "0"),
            ("a6a", 10, 8, "0"),
            ("16九a", 10, 8, "20"),
            ("+", 10, 8, "0"),
            ("-", 10, 8, "0"),
        ];
        for (n, f, t, e) in tests {
            let n = Datum::Bytes(n.as_bytes().to_vec());
            let f = Datum::I64(f);
            let t = Datum::I64(t);
            let e = Datum::Bytes(e.as_bytes().to_vec());
            let got = eval_func(ScalarFuncSig::Conv, &[n, f, t]).unwrap();
            assert_eq!(got, e);
        }

        let invalid_tests = vec![
            (Datum::Null, Datum::I64(10), Datum::I64(10), Datum::Null),
            (
                Datum::Bytes(b"a6a".to_vec()),
                Datum::I64(1),
                Datum::I64(8),
                Datum::Null,
            ),
        ];
        for (n, f, t, e) in invalid_tests {
            let got = eval_func(ScalarFuncSig::Conv, &[n, f, t]).unwrap();
            assert_eq!(got, e);
        }
    }
}
