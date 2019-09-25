use tidb_query_codegen::rpn_fn;

use crate::codec::data_type::*;
use crate::codec::{self, Error};
use crate::expr::EvalContext;
use crate::Result;

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

#[rpn_fn]
#[inline]
fn abs_int(arg: &Option<Int>) -> Result<Option<Int>> {
    match arg {
        None => Ok(None),
        Some(arg) => match (*arg).checked_abs() {
            None => Err(Error::overflow("BIGINT", &format!("abs({})", *arg)).into()),
            Some(arg_abs) => Ok(Some(arg_abs)),
        },
    }
}

#[rpn_fn]
#[inline]
fn abs_uint(arg: &Option<Int>) -> Result<Option<Int>> {
    Ok(*arg)
}

#[rpn_fn]
#[inline]
fn abs_real(arg: &Option<Real>) -> Result<Option<Real>> {
    match arg {
        Some(arg) => Ok(Some(num_traits::Signed::abs(arg))),
        None => Ok(None),
    }
}

#[rpn_fn]
#[inline]
fn abs_decimal(arg: &Option<Decimal>) -> Result<Option<Decimal>> {
    match arg {
        Some(arg) => {
            let res: codec::Result<Decimal> = arg.to_owned().abs().into();
            Ok(Some(res?))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tipb::ScalarFuncSig;

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
}
