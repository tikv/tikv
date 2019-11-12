use tidb_query_codegen::rpn_fn;

use crate::codec::data_type::*;
use crate::codec::mysql::{RoundMode, DEFAULT_FSP};
use crate::codec::{self, Error};
use crate::expr::EvalContext;
use crate::Result;

#[inline]
#[rpn_fn]
pub fn abs<A: Abs>(arg: &Option<A::Type>) -> Result<Option<A::Type>> {
    if let Some(arg) = arg {
        A::abs(arg)
    } else {
        Ok(None)
    }
}

pub trait Abs {
    type Type: Evaluable;

    fn abs(arg: &Self::Type) -> Result<Option<Self::Type>>;
}

pub struct AbsInt;

impl Abs for AbsInt {
    type Type = Int;

    #[inline]
    fn abs(arg: &Self::Type) -> Result<Option<Self::Type>> {
        match (*arg).checked_abs() {
            None => Err(Error::overflow("BIGINT", &format!("abs({})", *arg)).into()),
            Some(arg_abs) => Ok(Some(arg_abs)),
        }
    }
}

pub struct AbsUint;

impl Abs for AbsUint {
    type Type = Int;

    #[inline]
    fn abs(arg: &Self::Type) -> Result<Option<Self::Type>> {
        Ok(Some(*arg))
    }
}

pub struct AbsReal;

impl Abs for AbsReal {
    type Type = Real;

    #[inline]
    fn abs(arg: &Self::Type) -> Result<Option<Self::Type>> {
        Ok(Some(num_traits::Signed::abs(arg)))
    }
}

pub struct AbsDecimal;

impl Abs for AbsDecimal {
    type Type = Decimal;

    #[inline]
    fn abs(arg: &Self::Type) -> Result<Option<Self::Type>> {
        let res: codec::Result<Decimal> = arg.to_owned().abs().into();
        Ok(Some(res?))
    }
}

#[inline]
#[rpn_fn(capture = [ctx])]
pub fn ceil<C: Ceil>(ctx: &mut EvalContext, arg: &Option<C::Input>) -> Result<Option<C::Output>> {
    if let Some(arg) = arg {
        C::ceil(ctx, arg)
    } else {
        Ok(None)
    }
}

pub trait Ceil {
    type Input: Evaluable;
    type Output: Evaluable;

    fn ceil(_ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>>;
}

pub struct CeilReal;

impl Ceil for CeilReal {
    type Input = Real;
    type Output = Real;

    #[inline]
    fn ceil(_ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(Some(Real::from(arg.ceil())))
    }
}

pub struct CeilDecToDec;

impl Ceil for CeilDecToDec {
    type Input = Decimal;
    type Output = Decimal;

    #[inline]
    fn ceil(ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(arg.ceil().into_result(ctx).map(Some)?)
    }
}

pub struct CeilDecToInt;

impl Ceil for CeilDecToInt {
    type Input = Decimal;
    type Output = Int;

    #[inline]
    fn ceil(ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(arg
            .ceil()
            .into_result(ctx)
            .and_then(|decimal| decimal.as_i64_with_ctx(ctx))
            .map(Some)?)
    }
}

pub struct CeilIntToInt;

impl Ceil for CeilIntToInt {
    type Input = Int;
    type Output = Int;

    #[inline]
    fn ceil(_ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(Some(*arg))
    }
}

#[inline]
#[rpn_fn(capture = [ctx])]
pub fn floor<T: Floor>(ctx: &mut EvalContext, arg: &Option<T::Input>) -> Result<Option<T::Output>> {
    if let Some(arg) = arg {
        T::floor(ctx, arg)
    } else {
        Ok(None)
    }
}

pub trait Floor {
    type Input: Evaluable;
    type Output: Evaluable;

    fn floor(_ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>>;
}

pub struct FloorReal;

impl Floor for FloorReal {
    type Input = Real;
    type Output = Real;

    #[inline]
    fn floor(_ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(Some(Real::from(arg.floor())))
    }
}

pub struct FloorDecToInt;

impl Floor for FloorDecToInt {
    type Input = Decimal;
    type Output = Int;

    #[inline]
    fn floor(ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(arg
            .floor()
            .into_result(ctx)
            .and_then(|decimal| decimal.as_i64_with_ctx(ctx))
            .map(Some)?)
    }
}

pub struct FloorDecToDec;

impl Floor for FloorDecToDec {
    type Input = Decimal;
    type Output = Decimal;

    #[inline]
    fn floor(ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(arg.floor().into_result(ctx).map(Some)?)
    }
}

pub struct FloorIntToInt;

impl Floor for FloorIntToInt {
    type Input = Int;
    type Output = Int;

    #[inline]
    fn floor(_ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(Some(*arg))
    }
}

#[rpn_fn]
#[inline]
fn round_int(arg: &Option<Int>) -> Result<Option<Int>> {
    Ok(*arg)
}

#[rpn_fn]
#[inline]
fn round_real(arg: &Option<Real>) -> Result<Option<Real>> {
    Ok(match arg {
        Some(arg) => Some(Real::from(arg.round())),
        None => None,
    })
}

#[rpn_fn]
#[inline]
fn round_dec(arg: &Option<Decimal>) -> Result<Option<Decimal>> {
    Ok(match arg {
        Some(arg) => {
            let result: codec::Result<Decimal> = arg
                .to_owned()
                .round(DEFAULT_FSP, RoundMode::HalfEven)
                .into();
            Some(result?)
        }
        None => None,
    })
}

#[rpn_fn]
#[inline]
fn round_with_frac_int(arg0: &Option<Int>, arg1: &Option<Int>) -> Result<Option<Int>> {
    Ok(match (arg0, arg1) {
        (Some(number), Some(digits)) => {
            if *digits >= 0 {
                Some(*number)
            } else {
                let power = 10.0_f64.powi(-digits as i32);
                let frac = *number as f64 / power;
                Some((frac.round() * power) as i64)
            }
        }
        _ => None,
    })
}

#[rpn_fn]
#[inline]
fn round_with_frac_real(arg0: &Option<Real>, arg1: &Option<Int>) -> Result<Option<Real>> {
    Ok(match (arg0, arg1) {
        (Some(number), Some(digits)) => {
            let power = 10.0_f64.powi(-digits as i32);
            let frac = number.into_inner() / power;
            Some(Real::from(frac.round() * power))
        }
        _ => None,
    })
}

#[rpn_fn]
#[inline]
fn round_with_frac_dec(arg0: &Option<Decimal>, arg1: &Option<Int>) -> Result<Option<Decimal>> {
    Ok(match (arg0, arg1) {
        (Some(number), Some(digits)) => {
            let result: codec::Result<Decimal> = number
                .to_owned()
                .round(*digits as i8, RoundMode::HalfEven)
                .into();
            Some(result?)
        }
        _ => None,
    })
}

#[inline]
#[rpn_fn]
fn sign(arg: &Option<Real>) -> Result<Option<Int>> {
    Ok(arg.and_then(|n| {
        if *n > 0f64 {
            Some(1)
        } else if *n == 0f64 {
            Some(0)
        } else {
            Some(-1)
        }
    }))
}

#[inline]
#[rpn_fn]
fn sqrt(arg: &Option<Real>) -> Result<Option<Real>> {
    Ok(arg.and_then(|n| {
        if *n < 0f64 {
            None
        } else {
            let res = n.sqrt();
            if res.is_nan() {
                None
            } else {
                Some(Real::from(res))
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use tipb::ScalarFuncSig;

    use super::*;
    use crate::rpn_expr::types::test_util::RpnFnScalarEvaluator;

    #[test]
    fn test_abs_int() {
        let test_cases = vec![
            (ScalarFuncSig::AbsInt, -3, Some(3), false),
            (
                ScalarFuncSig::AbsInt,
                std::i64::MAX,
                Some(std::i64::MAX),
                false,
            ),
            (
                ScalarFuncSig::AbsUInt,
                std::u64::MAX as i64,
                Some(std::u64::MAX as i64),
                false,
            ),
            (ScalarFuncSig::AbsInt, std::i64::MIN, Some(0), true),
        ];

        for (sig, arg, expect_output, is_err) in test_cases {
            let output = RpnFnScalarEvaluator::new().push_param(arg).evaluate(sig);

            if is_err {
                assert!(output.is_err());
            } else {
                let output = output.unwrap();
                assert_eq!(output, expect_output, "{:?}", arg);
            }
        }
    }

    #[test]
    fn test_abs_real() {
        let test_cases: Vec<(Real, Option<Real>)> = vec![
            (Real::new(3.5).unwrap(), Real::new(3.5).ok()),
            (Real::new(-3.5).unwrap(), Real::new(3.5).ok()),
        ];

        for (arg, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::AbsReal)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", arg);
        }
    }

    #[test]
    fn test_abs_decimal() {
        let test_cases = vec![("1.1", "1.1"), ("-1.1", "1.1")];

        for (arg, expect_output) in test_cases {
            let arg = arg.parse::<Decimal>().ok();
            let expect_output = expect_output.parse::<Decimal>().ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg.clone())
                .evaluate(ScalarFuncSig::AbsDecimal)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", arg);
        }
    }

    #[test]
    fn test_ceil_real() {
        let cases = vec![
            (4.0, 3.5),
            (4.0, 3.45),
            (4.0, 3.1),
            (-3.0, -3.45),
            (0.0, -0.1),
            (std::f64::MAX, std::f64::MAX),
            (std::f64::MIN, std::f64::MIN),
        ];
        for (expected, input) in cases {
            let arg = Real::from(input);
            let expected = Real::new(expected).ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Real>(ScalarFuncSig::CeilReal)
                .unwrap();
            assert_eq!(expected, output);
        }
    }

    #[test]
    fn test_ceil_dec_to_dec() {
        let cases = vec![
            ("9223372036854775808", "9223372036854775808"),
            ("124", "123.456"),
            ("-123", "-123.456"),
        ];

        for (expected, input) in cases {
            let arg = input.parse::<Decimal>().ok();
            let expected = expected.parse::<Decimal>().ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Decimal>(ScalarFuncSig::CeilDecToDec)
                .unwrap();
            assert_eq!(expected, output);
        }
    }

    #[test]
    fn test_ceil_dec_to_int() {
        let cases = vec![
            (124, "123.456"),
            (2, "1.23"),
            (-1, "-1.23"),
            (std::i64::MIN, "-9223372036854775808"),
        ];
        for (expected, input) in cases {
            let arg = input.parse::<Decimal>().ok();
            let expected = Some(expected);
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Int>(ScalarFuncSig::CeilDecToInt)
                .unwrap();
            assert_eq!(expected, output);
        }
    }

    #[test]
    fn test_ceil_int_to_int() {
        let cases = vec![
            (1, 1),
            (2, 2),
            (666, 666),
            (-3, -3),
            (-233, -233),
            (std::i64::MAX, std::i64::MAX),
            (std::i64::MIN, std::i64::MIN),
        ];

        for (expected, input) in cases {
            let expected = Some(expected);
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Int>(ScalarFuncSig::CeilIntToInt)
                .unwrap();
            assert_eq!(expected, output);
        }
    }

    fn test_unary_func_ok_none<I: Evaluable, O: Evaluable>(sig: ScalarFuncSig)
    where
        O: PartialEq,
        Option<I>: Into<ScalarValue>,
        Option<O>: From<ScalarValue>,
    {
        assert_eq!(
            None,
            RpnFnScalarEvaluator::new()
                .push_param(Option::<I>::None)
                .evaluate::<O>(sig)
                .unwrap()
        );
    }

    #[test]
    fn test_floor_real() {
        let cases = vec![
            (3.5, 3.0),
            (3.7, 3.0),
            (3.45, 3.0),
            (3.1, 3.0),
            (-3.45, -4.0),
            (-0.1, -1.0),
            (16140901064495871255.0, 16140901064495871255.0),
            (std::f64::MAX, std::f64::MAX),
            (std::f64::MIN, std::f64::MIN),
        ];
        for (input, expected) in cases {
            let arg = Real::from(input);
            let expected = Real::new(expected).ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Real>(ScalarFuncSig::FloorReal)
                .unwrap();
            assert_eq!(expected, output);
        }

        test_unary_func_ok_none::<Real, Real>(ScalarFuncSig::FloorReal);
    }

    #[test]
    fn test_floor_dec_to_dec() {
        let cases = vec![
            ("9223372036854775808", "9223372036854775808"),
            ("123.456", "123"),
            ("-123.456", "-124"),
        ];

        for (input, expected) in cases {
            let arg = input.parse::<Decimal>().ok();
            let expected = expected.parse::<Decimal>().ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Decimal>(ScalarFuncSig::FloorDecToDec)
                .unwrap();
            assert_eq!(expected, output);
        }

        test_unary_func_ok_none::<Decimal, Decimal>(ScalarFuncSig::FloorDecToDec);
    }

    #[test]
    fn test_floor_dec_to_int() {
        let cases = vec![
            ("123.456", 123),
            ("1.23", 1),
            ("-1.23", -2),
            ("-9223372036854775808", std::i64::MIN),
        ];
        for (input, expected) in cases {
            let arg = input.parse::<Decimal>().ok();
            let expected = Some(expected);
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Int>(ScalarFuncSig::FloorDecToInt)
                .unwrap();
            assert_eq!(expected, output);
        }

        test_unary_func_ok_none::<Decimal, Int>(ScalarFuncSig::FloorDecToInt);
    }

    #[test]
    fn test_floor_int_to_int() {
        let cases = vec![
            (1, 1),
            (2, 2),
            (-3, -3),
            (std::i64::MAX, std::i64::MAX),
            (std::i64::MIN, std::i64::MIN),
        ];

        for (expected, input) in cases {
            let expected = Some(expected);
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Int>(ScalarFuncSig::FloorIntToInt)
                .unwrap();
            assert_eq!(expected, output);
        }

        test_unary_func_ok_none::<Int, Int>(ScalarFuncSig::FloorIntToInt);
    }

    #[test]
    fn test_round_int() {
        let test_cases: Vec<(Int, Option<Int>)> = vec![
            (1, Some(1)),
            (Int::max_value(), Some(Int::max_value())),
            (Int::min_value(), Some(Int::min_value())),
        ];

        for (arg, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::RoundInt)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_round_real() {
        let test_cases: Vec<(Real, Option<Real>)> = vec![
            (Real::from(3.45_f64), Some(Real::from(3_f64))),
            (Real::from(-3.45_f64), Some(Real::from(-3_f64))),
            (Real::from(std::f64::MAX), Some(Real::from(std::f64::MAX))),
            (Real::from(std::f64::MIN), Some(Real::from(std::f64::MIN))),
        ];

        for (arg, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::RoundReal)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_round_dec() {
        let test_cases: Vec<(Decimal, Option<Decimal>)> = vec![
            ("123.456".parse().unwrap(), Some("123.000".parse().unwrap())),
            ("123.656".parse().unwrap(), Some("124.000".parse().unwrap())),
            (
                "-123.456".parse().unwrap(),
                Some("-123.000".parse().unwrap()),
            ),
        ];

        for (arg, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::RoundDec)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_round_with_frac_int() {
        let test_cases: Vec<(Int, Int, Option<Int>)> = vec![
            (23, 2, Some(23)),
            (23, -1, Some(20)),
            (-27, -1, Some(-30)),
            (-27, -2, Some(0)),
        ];

        for (arg0, arg1, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0)
                .push_param(arg1)
                .evaluate(ScalarFuncSig::RoundWithFracInt)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_round_with_frac_real() {
        let test_cases: Vec<(Real, Int, Option<Real>)> = vec![
            (Real::from(-1.298_f64), 1, Some(Real::from(-1.3_f64))),
            (Real::from(-1.298_f64), 0, Some(Real::from(-1.0_f64))),
            (Real::from(23.298_f64), 2, Some(Real::from(23.30_f64))),
            (Real::from(23.298_f64), -1, Some(Real::from(20.0_f64))),
        ];

        for (arg0, arg1, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0)
                .push_param(arg1)
                .evaluate(ScalarFuncSig::RoundWithFracReal)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_round_with_frac_dec() {
        let test_cases: Vec<(Decimal, Int, Option<Decimal>)> = vec![
            (
                "150.000".parse().unwrap(),
                2,
                Some("150.00".parse().unwrap()),
            ),
            (
                "150.257".parse().unwrap(),
                1,
                Some("150.3".parse().unwrap()),
            ),
            ("153.257".parse().unwrap(), -1, Some("150".parse().unwrap())),
        ];

        for (arg0, arg1, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0)
                .push_param(arg1)
                .evaluate(ScalarFuncSig::RoundWithFracDec)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_sign() {
        let test_cases = vec![
            (None, None),
            (Some(42f64), Some(1)),
            (Some(0f64), Some(0)),
            (Some(-47f64), Some(-1)),
        ];
        for (input, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate(ScalarFuncSig::Sign)
                .unwrap();
            assert_eq!(expect, output, "{:?}", input);
        }
    }

    #[test]
    fn test_sqrt() {
        let test_cases = vec![
            (None, None),
            (Some(64f64), Some(Real::from(8f64))),
            (Some(2f64), Some(Real::from(std::f64::consts::SQRT_2))),
            (Some(-16f64), None),
            (Some(std::f64::NAN), None),
        ];
        for (input, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate(ScalarFuncSig::Sqrt)
                .unwrap();
            assert_eq!(expect, output, "{:?}", input);
        }
    }
}
