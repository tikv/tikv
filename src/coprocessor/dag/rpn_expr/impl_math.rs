use cop_codegen::rpn_fn;

use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::codec::Error;
use crate::coprocessor::Result;

#[rpn_fn]
#[inline]
fn abs_int(arg: &Option<Int>) -> Result<Option<Int>> {
    match arg {
        None => Ok(None),
        Some(arg) => match (*arg).checked_abs() {
            None => Err(Error::overflow("BIGINT", &format!("abs({})", *arg)))?,
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
    // abs returns NAN if the number is NAN, so don't worry about it
    match arg {
        Some(arg) => {
            let f = arg.abs();
            Ok(Some(Real::new(f).unwrap()))
        }
        None => Ok(None),
    }
}

#[rpn_fn]
#[inline]
fn abs_decimal(arg: &Option<Decimal>) -> Result<Option<Decimal>> {
    use crate::coprocessor::codec::mysql::Res;

    match arg {
        Some(arg) => match arg.to_owned().abs() {
            Res::Ok(v) => Ok(Some(v)),
            Res::Truncated(_) => Err(Error::truncated())?,
            Res::Overflow(_) => Err(Error::overflow("DECIMAL", &format!("abs({})", arg)))?,
        },
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tipb::expression::ScalarFuncSig;

    use crate::coprocessor::dag::rpn_expr::types::test_util::RpnFnScalarEvaluator;

    #[test]
    fn test_abs_int() {
        let test_cases = vec![
            (ScalarFuncSig::AbsInt, -3, Some(3)),
            (ScalarFuncSig::AbsInt, std::i64::MAX, Some(std::i64::MAX)),
            (
                ScalarFuncSig::AbsUInt,
                std::u64::MAX as i64,
                Some(std::u64::MAX as i64),
            ),
        ];

        for (sig, arg, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(sig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}, {:?}", arg, sig);
        }
    }

    #[test]
    fn test_abs_real() {
        let test_cases: Vec<(ScalarFuncSig, Real, Option<Real>)> = vec![
            (
                ScalarFuncSig::AbsReal,
                Real::new(3.5).unwrap(),
                Real::new(3.5).ok(),
            ),
            (
                ScalarFuncSig::AbsReal,
                Real::new(-3.5).unwrap(),
                Real::new(3.5).ok(),
            ),
        ];

        for (sig, arg, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(sig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}, {:?}", arg, sig);
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
            assert_eq!(output, expect_output, "{:?} {:?}", arg, ScalarFuncSig::AbsDecimal);
        }
    }
}
