// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::rpn_fn;

use crate::codec::data_type::*;
use crate::codec::Error;
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
pub fn logical_xor(arg0: &Option<i64>, arg1: &Option<i64>) -> Result<Option<i64>> {
    // evaluates to 1 if an odd number of operands is nonzero, otherwise 0 is returned.
    Ok(match (arg0, arg1) {
        (Some(arg0), Some(arg1)) => Some(((*arg0 == 0) ^ (*arg1 == 0)) as i64),
        _ => None,
    })
}

#[rpn_fn]
#[inline]
pub fn unary_not_int(arg: &Option<Int>) -> Result<Option<i64>> {
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
pub fn unary_minus_uint(arg: &Option<Int>) -> Result<Option<Int>> {
    match *arg {
        Some(val) => {
            let uval = val as u64;
            if uval > std::i64::MAX as u64 + 1 {
                Err(Error::overflow("BIGINT", &format!("-{}", uval)).into())
            } else if uval == std::i64::MAX as u64 + 1 {
                Ok(Some(std::i64::MIN))
            } else {
                Ok(Some(-val))
            }
        }
        None => Ok(None),
    }
}

#[rpn_fn]
#[inline]
pub fn unary_minus_int(arg: &Option<Int>) -> Result<Option<Int>> {
    match *arg {
        Some(val) => {
            if val == std::i64::MIN {
                Err(Error::overflow("BIGINT", &format!("-{}", val)).into())
            } else {
                Ok(Some(-val))
            }
        }
        None => Ok(None),
    }
}

#[rpn_fn]
#[inline]
pub fn unary_minus_real(arg: &Option<Real>) -> Result<Option<Real>> {
    Ok(arg.map(|val| -val))
}

#[rpn_fn]
#[inline]
pub fn unary_minus_decimal(arg: &Option<Decimal>) -> Result<Option<Decimal>> {
    Ok(arg.as_ref().map(|val| -*val))
}

#[rpn_fn]
#[inline]
pub fn is_null<T: Evaluable>(arg: &Option<T>) -> Result<Option<i64>> {
    Ok(Some(arg.is_none() as i64))
}

#[rpn_fn]
#[inline]
pub fn bit_and(lhs: &Option<Int>, rhs: &Option<Int>) -> Result<Option<Int>> {
    Ok(match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => Some(lhs & rhs),
        _ => None,
    })
}

#[rpn_fn]
#[inline]
pub fn bit_or(lhs: &Option<Int>, rhs: &Option<Int>) -> Result<Option<Int>> {
    Ok(match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => Some(lhs | rhs),
        _ => None,
    })
}

#[rpn_fn]
#[inline]
pub fn bit_xor(lhs: &Option<Int>, rhs: &Option<Int>) -> Result<Option<Int>> {
    Ok(match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => Some(lhs ^ rhs),
        _ => None,
    })
}

#[rpn_fn]
#[inline]
pub fn bit_neg(arg: &Option<Int>) -> Result<Option<Int>> {
    Ok(arg.map(|arg| !arg))
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

#[rpn_fn]
#[inline]
fn left_shift(lhs: &Option<Int>, rhs: &Option<Int>) -> Result<Option<Int>> {
    Ok(match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => {
            if *rhs as u64 >= 64 {
                Some(0)
            } else {
                Some((*lhs as u64).wrapping_shl(*rhs as u32) as i64)
            }
        }
        _ => None,
    })
}

#[rpn_fn]
#[inline]
fn right_shift(lhs: &Option<Int>, rhs: &Option<Int>) -> Result<Option<Int>> {
    Ok(match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => {
            if *rhs as u64 >= 64 {
                Some(0)
            } else {
                Some((*lhs as u64).wrapping_shr(*rhs as u32) as i64)
            }
        }
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use tidb_query_datatype::{builder::FieldTypeBuilder, FieldTypeFlag, FieldTypeTp};
    use tipb::ScalarFuncSig;

    use super::*;
    use crate::codec::mysql::TimeType;
    use crate::expr::EvalContext;
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
    fn test_logical_xor() {
        let test_cases = vec![
            (Some(1), Some(1), Some(0)),
            (Some(1), Some(0), Some(1)),
            (Some(0), Some(0), Some(0)),
            (Some(2), Some(-1), Some(0)),
            (Some(-1), Some(0), Some(1)),
            (Some(0), None, None),
            (None, Some(1), None),
        ];
        for (arg0, arg1, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0)
                .push_param(arg1)
                .evaluate(ScalarFuncSig::LogicalXor)
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
    fn test_unary_minus_int() {
        let unsigned_test_cases = vec![
            (None, None),
            (Some((std::i64::MAX as u64 + 1) as i64), Some(std::i64::MIN)),
            (Some(12345), Some(-12345)),
            (Some(0), Some(0)),
        ];
        for (arg, expect_output) in unsigned_test_cases {
            let field_type = FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .flag(FieldTypeFlag::UNSIGNED)
                .build();
            let output = RpnFnScalarEvaluator::new()
                .push_param_with_field_type(arg, field_type)
                .evaluate::<Int>(ScalarFuncSig::UnaryMinusInt)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", arg);
        }
        assert!(RpnFnScalarEvaluator::new()
            .push_param_with_field_type(
                Some((std::i64::MAX as u64 + 2) as i64),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::LongLong)
                    .flag(FieldTypeFlag::UNSIGNED)
                    .build()
            )
            .evaluate::<Int>(ScalarFuncSig::UnaryMinusInt)
            .is_err());

        let signed_test_cases = vec![
            (None, None),
            (Some(std::i64::MAX), Some(-std::i64::MAX)),
            (Some(-std::i64::MAX), Some(std::i64::MAX)),
            (Some(std::i64::MIN + 1), Some(std::i64::MAX)),
            (Some(0), Some(0)),
        ];
        for (arg, expect_output) in signed_test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Int>(ScalarFuncSig::UnaryMinusInt)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", arg);
        }
        assert!(RpnFnScalarEvaluator::new()
            .push_param(std::i64::MIN)
            .evaluate::<Int>(ScalarFuncSig::UnaryMinusInt)
            .is_err());
    }

    #[test]
    fn test_unary_minus_real() {
        let test_cases = vec![
            (None, None),
            (Some(Real::from(0.123_f64)), Some(Real::from(-0.123_f64))),
            (Some(Real::from(-0.123_f64)), Some(Real::from(0.123_f64))),
            (Some(Real::from(0.0_f64)), Some(Real::from(0.0_f64))),
            (
                Some(Real::from(std::f64::INFINITY)),
                Some(Real::from(std::f64::NEG_INFINITY)),
            ),
        ];
        for (arg, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Real>(ScalarFuncSig::UnaryMinusReal)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", arg);
        }
    }

    #[test]
    fn test_unary_minus_decimal() {
        let test_cases = vec![
            (None, None),
            (Some(Decimal::zero()), Some(Decimal::zero())),
            (
                "0.123".parse::<Decimal>().ok(),
                "-0.123".parse::<Decimal>().ok(),
            ),
            (
                "-0.123".parse::<Decimal>().ok(),
                "0.123".parse::<Decimal>().ok(),
            ),
        ];
        for (arg, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg.clone())
                .evaluate::<Decimal>(ScalarFuncSig::UnaryMinusDecimal)
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
                DateTime::zero(&mut EvalContext::default(), 0, TimeType::DateTime)
                    .unwrap()
                    .into(),
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
    fn test_bit_and() {
        let cases = vec![
            (Some(123), Some(321), Some(65)),
            (Some(-123), Some(321), Some(257)),
            (None, Some(1), None),
            (Some(1), None, None),
            (None, None, None),
        ];
        for (lhs, rhs, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(lhs)
                .push_param(rhs)
                .evaluate(ScalarFuncSig::BitAndSig)
                .unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_bit_or() {
        let cases = vec![
            (Some(123), Some(321), Some(379)),
            (Some(-123), Some(321), Some(-59)),
            (None, Some(1), None),
            (Some(1), None, None),
            (None, None, None),
        ];
        for (lhs, rhs, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(lhs)
                .push_param(rhs)
                .evaluate(ScalarFuncSig::BitOrSig)
                .unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_bit_xor() {
        let cases = vec![
            (Some(123), Some(321), Some(314)),
            (Some(-123), Some(321), Some(-316)),
            (None, Some(1), None),
            (Some(1), None, None),
            (None, None, None),
        ];
        for (lhs, rhs, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(lhs)
                .push_param(rhs)
                .evaluate(ScalarFuncSig::BitXorSig)
                .unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_bit_neg() {
        let cases = vec![
            (Some(123), Some(-124)),
            (Some(-123), Some(122)),
            (Some(0), Some(-1)),
            (None, None),
        ];
        for (arg, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::BitNegSig)
                .unwrap();
            assert_eq!(output, expected);
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

    #[test]
    fn test_left_shift() {
        let cases = vec![
            (Some(123), Some(2), Some(492)),
            (Some(-123), Some(-1), Some(0)),
            (Some(123), Some(0), Some(123)),
            (None, Some(1), None),
            (Some(123), None, None),
            (Some(-123), Some(60), Some(5764607523034234880)),
            (None, None, None),
        ];
        for (lhs, rhs, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(lhs)
                .push_param(rhs)
                .evaluate(ScalarFuncSig::LeftShift)
                .unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_right_shift() {
        let cases = vec![
            (Some(123), Some(2), Some(30)),
            (Some(-123), Some(-1), Some(0)),
            (Some(123), Some(0), Some(123)),
            (None, Some(1), None),
            (Some(123), None, None),
            (Some(-123), Some(2), Some(4611686018427387873)),
            (None, None, None),
        ];
        for (lhs, rhs, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(lhs)
                .push_param(rhs)
                .evaluate(ScalarFuncSig::RightShift)
                .unwrap();
            assert_eq!(output, expected);
        }
    }
}
