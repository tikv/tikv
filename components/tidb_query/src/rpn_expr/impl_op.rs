// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::rpn_fn;

use crate::codec::data_type::*;
use crate::Result;

#[rpn_fn]
#[inline]
pub fn logical_and(lhs: &Option<i64>, rhs: &Option<i64>) -> Result<Option<i64>> {
    Ok(match (lhs, rhs) {
        (Some(0), _) | (_, Some(0)) => Some(0),
        (None, _) | (_, None) => None,
        _ => Some(1),
    })
}

#[rpn_fn]
#[inline]
pub fn logical_or(arg0: &Option<i64>, arg1: &Option<i64>) -> Result<Option<i64>> {
    // This is a standard Kleene OR used in SQL where
    // `null OR false == null` and `null OR true == true`
    Ok(match (arg0, arg1) {
        (Some(0), Some(0)) => Some(0),
        (None, None) | (None, Some(0)) | (Some(0), None) => None,
        _ => Some(1),
    })
}

#[rpn_fn]
#[inline]
pub fn unary_not_int(arg: &Option<i64>) -> Result<Option<i64>> {
    Ok(arg.map(|v| (v == 0) as i64))
}

#[rpn_fn]
#[inline]
pub fn unary_not_real(arg: &Option<Real>) -> Result<Option<i64>> {
    Ok(arg.map(|v| (v.into_inner() == 0f64) as i64))
}

#[rpn_fn]
#[inline]
pub fn unary_not_decimal(arg: &Option<Decimal>) -> Result<Option<i64>> {
    Ok(arg.as_ref().map(|v| v.is_zero() as i64))
}

#[rpn_fn]
#[inline]
pub fn is_null<T: Evaluable>(arg: &Option<T>) -> Result<Option<i64>> {
    Ok(Some(arg.is_none() as i64))
}

#[rpn_fn]
#[inline]
pub fn int_is_true(arg: &Option<Int>) -> Result<Option<i64>> {
    Ok(Some(arg.map_or(0, |v| (v != 0) as i64)))
}

#[rpn_fn]
#[inline]
pub fn real_is_true(arg: &Option<Real>) -> Result<Option<i64>> {
    Ok(Some(arg.map_or(0, |v| (v.into_inner() != 0f64) as i64)))
}

#[rpn_fn]
#[inline]
pub fn decimal_is_true(arg: &Option<Decimal>) -> Result<Option<i64>> {
    Ok(Some(arg.as_ref().map_or(0, |v| !v.is_zero() as i64)))
}

#[rpn_fn]
#[inline]
pub fn int_is_false(arg: &Option<Int>) -> Result<Option<i64>> {
    Ok(Some(arg.map_or(0, |v| (v == 0) as i64)))
}

#[rpn_fn]
#[inline]
pub fn real_is_false(arg: &Option<Real>) -> Result<Option<i64>> {
    Ok(Some(arg.map_or(0, |v| (v.into_inner() == 0f64) as i64)))
}

#[rpn_fn]
#[inline]
fn decimal_is_false(arg: &Option<Decimal>) -> Result<Option<i64>> {
    Ok(Some(arg.as_ref().map_or(0, |v| v.is_zero() as i64)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tipb::ScalarFuncSig;

    use crate::codec::mysql::{time, Tz};
    use crate::rpn_expr::test_util::RpnFnScalarEvaluator;

    #[test]
    fn test_logical_and() {
        let test_cases = vec![
            (Some(1), Some(1), Some(1)),
            (Some(1), Some(0), Some(0)),
            (Some(0), Some(0), Some(0)),
            (Some(2), Some(-1), Some(1)),
            (Some(0), None, Some(0)),
            (None, Some(1), None),
        ];
        for (arg0, arg1, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0)
                .push_param(arg1)
                .evaluate(ScalarFuncSig::LogicalAnd)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_logical_or() {
        let test_cases = vec![
            (Some(1), Some(1), Some(1)),
            (Some(1), Some(0), Some(1)),
            (Some(0), Some(0), Some(0)),
            (Some(2), Some(-1), Some(1)),
            (Some(1), None, Some(1)),
            (None, Some(0), None),
        ];
        for (arg0, arg1, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0)
                .push_param(arg1)
                .evaluate(ScalarFuncSig::LogicalOr)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_unary_not_int() {
        let test_cases = vec![
            (None, None),
            (0.into(), Some(1)),
            (1.into(), Some(0)),
            (2.into(), Some(0)),
            ((-1).into(), Some(0)),
        ];
        for (arg, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::UnaryNotInt)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", arg);
        }
    }

    #[test]
    fn test_unary_not_real() {
        let test_cases = vec![
            (None, None),
            (0.0.into(), Some(1)),
            (1.0.into(), Some(0)),
            (0.3.into(), Some(0)),
        ];
        for (arg, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::UnaryNotReal)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", arg);
        }
    }

    #[test]
    fn test_unary_not_decimal() {
        let test_cases = vec![
            (None, None),
            (Decimal::zero().into(), Some(1)),
            (Decimal::from(1).into(), Some(0)),
        ];
        for (arg, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg.clone())
                .evaluate(ScalarFuncSig::UnaryNotDecimal)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", arg);
        }
    }

    #[test]
    fn test_is_null() {
        let test_cases = vec![
            (ScalarValue::Int(None), ScalarFuncSig::IntIsNull, Some(1)),
            (0.into(), ScalarFuncSig::IntIsNull, Some(0)),
            (ScalarValue::Real(None), ScalarFuncSig::RealIsNull, Some(1)),
            (0.0.into(), ScalarFuncSig::RealIsNull, Some(0)),
            (
                ScalarValue::Decimal(None),
                ScalarFuncSig::DecimalIsNull,
                Some(1),
            ),
            (
                Decimal::from(1).into(),
                ScalarFuncSig::DecimalIsNull,
                Some(0),
            ),
            (
                ScalarValue::Bytes(None),
                ScalarFuncSig::StringIsNull,
                Some(1),
            ),
            (vec![0u8].into(), ScalarFuncSig::StringIsNull, Some(0)),
            (
                ScalarValue::DateTime(None),
                ScalarFuncSig::TimeIsNull,
                Some(1),
            ),
            (
                time::zero_datetime(&Tz::utc()).into(),
                ScalarFuncSig::TimeIsNull,
                Some(0),
            ),
            (
                ScalarValue::Duration(None),
                ScalarFuncSig::DurationIsNull,
                Some(1),
            ),
            (
                Duration::from_nanos(1, 0).unwrap().into(),
                ScalarFuncSig::DurationIsNull,
                Some(0),
            ),
            (ScalarValue::Json(None), ScalarFuncSig::JsonIsNull, Some(1)),
            (
                Json::Array(vec![]).into(),
                ScalarFuncSig::JsonIsNull,
                Some(0),
            ),
        ];
        for (arg, sig, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg.clone())
                .evaluate(sig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}, {:?}", arg, sig);
        }
    }

    #[test]
    fn test_is_true() {
        let test_cases = vec![
            (ScalarValue::Int(None), ScalarFuncSig::IntIsTrue, Some(0)),
            (0.into(), ScalarFuncSig::IntIsTrue, Some(0)),
            (1.into(), ScalarFuncSig::IntIsTrue, Some(1)),
            (ScalarValue::Real(None), ScalarFuncSig::RealIsTrue, Some(0)),
            (0.0.into(), ScalarFuncSig::RealIsTrue, Some(0)),
            (1.0.into(), ScalarFuncSig::RealIsTrue, Some(1)),
            (
                ScalarValue::Decimal(None),
                ScalarFuncSig::DecimalIsTrue,
                Some(0),
            ),
            (
                Decimal::zero().into(),
                ScalarFuncSig::DecimalIsTrue,
                Some(0),
            ),
            (
                Decimal::from(1).into(),
                ScalarFuncSig::DecimalIsTrue,
                Some(1),
            ),
        ];
        for (arg, sig, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg.clone())
                .evaluate(sig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}, {:?}", arg, sig);
        }
    }

    #[test]
    fn test_is_false() {
        let test_cases = vec![
            (ScalarValue::Int(None), ScalarFuncSig::IntIsFalse, Some(0)),
            (0.into(), ScalarFuncSig::IntIsFalse, Some(1)),
            (1.into(), ScalarFuncSig::IntIsFalse, Some(0)),
            (ScalarValue::Real(None), ScalarFuncSig::RealIsFalse, Some(0)),
            (0.0.into(), ScalarFuncSig::RealIsFalse, Some(1)),
            (1.0.into(), ScalarFuncSig::RealIsFalse, Some(0)),
            (
                ScalarValue::Decimal(None),
                ScalarFuncSig::DecimalIsFalse,
                Some(0),
            ),
            (
                Decimal::zero().into(),
                ScalarFuncSig::DecimalIsFalse,
                Some(1),
            ),
            (
                Decimal::from(1).into(),
                ScalarFuncSig::DecimalIsFalse,
                Some(0),
            ),
        ];
        for (arg, sig, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg.clone())
                .evaluate(sig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}, {:?}", arg, sig);
        }
    }
}
