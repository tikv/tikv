use tidb_query_codegen::rpn_fn;

use crate::codec::data_type::*;
use crate::codec::{self, Error};
use crate::expr::EvalContext;
use crate::Result;

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

    fn ceil(_ctx: &mut EvalContext, _arg: &Self::Input) -> Result<Option<Self::Output>>;
}

pub struct CeilReal;

impl Ceil for CeilReal {
    type Input = Real;
    type Output = Real;

    fn ceil(_ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(Some(Real::from(arg.ceil())))
    }
}

pub struct CeilDecToDec;

impl Ceil for CeilDecToDec {
    type Input = Decimal;
    type Output = Decimal;

    fn ceil(ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        let result = arg.ceil();

        if result.is_truncated() {
            ctx.handle_truncate(true)?;
        } else if result.is_overflow() {
            ctx.handle_overflow(codec::Error::overflow("DECIMAL", ""))?;
        }

        let result: codec::Result<Decimal> = result.into();
        result.map_err(|err| err.into()).map(Some)
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

    use tipb::expression::ScalarFuncSig;

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
}
