// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use cop_codegen::RpnFunction;

use super::types::RpnFnCallPayload;
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::Result;

#[derive(Debug, Clone, Copy, RpnFunction)]
#[rpn_function(args = 2)]
pub struct RpnFnLogicalAnd;

impl RpnFnLogicalAnd {
    #[inline]
    fn call(
        _ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        arg0: &Option<i64>,
        arg1: &Option<i64>,
    ) -> Result<Option<i64>> {
        // The mapping from Rust to SQL logic is:
        //
        // * None => null
        // * Some(0) => false
        // * Some(x != 0) => true
        Ok(match (arg0, arg1) {
            (Some(0), _) | (_, Some(0)) => Some(0),
            (None, _) | (_, None) => None,
            _ => Some(1),
        })
    }
}

#[derive(Debug, Clone, Copy, RpnFunction)]
#[rpn_function(args = 2)]
pub struct RpnFnLogicalOr;

impl RpnFnLogicalOr {
    #[inline]
    fn call(
        _ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        arg0: &Option<i64>,
        arg1: &Option<i64>,
    ) -> Result<Option<i64>> {
        // This is a standard Kleene OR used in SQL where
        // `null OR false == null` and `null OR true == true`
        Ok(match (arg0, arg1) {
            (Some(0), Some(0)) => Some(0),
            (None, None) | (None, Some(0)) | (Some(0), None) => None,
            _ => Some(1),
        })
    }
}

#[derive(Clone, Debug, RpnFunction)]
#[rpn_function(args = 1)]
pub struct RpnFnIsNull<T: Evaluable> {
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Evaluable> Copy for RpnFnIsNull<T> {}

impl<T: Evaluable> RpnFnIsNull<T> {
    #[inline]
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    fn call(
        _ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        arg: &Option<T>,
    ) -> Result<Option<i64>> {
        Ok(Some(arg.is_none() as i64))
    }
}

#[derive(Clone, Copy, Debug, RpnFunction)]
#[rpn_function(args = 1)]
pub struct RpnFnIntIsTrue;

impl RpnFnIntIsTrue {
    #[inline]
    fn call(
        _ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        arg: &Option<Int>,
    ) -> Result<Option<i64>> {
        Ok(Some(arg.map_or(0, |v| (v != 0) as i64)))
    }
}

#[derive(Clone, Copy, Debug, RpnFunction)]
#[rpn_function(args = 1)]
pub struct RpnFnRealIsTrue;

impl RpnFnRealIsTrue {
    #[inline]
    fn call(
        _ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        arg: &Option<Real>,
    ) -> Result<Option<i64>> {
        Ok(Some(arg.map_or(0, |v| (v.into_inner() != 0f64) as i64)))
    }
}

#[derive(Clone, Copy, Debug, RpnFunction)]
#[rpn_function(args = 1)]
pub struct RpnFnDecimalIsTrue;

impl RpnFnDecimalIsTrue {
    #[inline]
    fn call(
        _ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        arg: &Option<Decimal>,
    ) -> Result<Option<i64>> {
        Ok(Some(arg.as_ref().map_or(0, |v| !v.is_zero() as i64)))
    }
}

#[derive(Clone, Copy, Debug, RpnFunction)]
#[rpn_function(args = 1)]
pub struct RpnFnIntIsFalse;

impl RpnFnIntIsFalse {
    #[inline]
    fn call(
        _ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        arg: &Option<Int>,
    ) -> Result<Option<i64>> {
        Ok(Some(arg.map_or(0, |v| (v == 0) as i64)))
    }
}

#[derive(Clone, Copy, Debug, RpnFunction)]
#[rpn_function(args = 1)]
pub struct RpnFnRealIsFalse;

impl RpnFnRealIsFalse {
    #[inline]
    fn call(
        _ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        arg: &Option<Real>,
    ) -> Result<Option<i64>> {
        Ok(Some(arg.map_or(0, |v| (v.into_inner() == 0f64) as i64)))
    }
}

#[derive(Clone, Copy, Debug, RpnFunction)]
#[rpn_function(args = 1)]
pub struct RpnFnDecimalIsFalse;

impl RpnFnDecimalIsFalse {
    #[inline]
    fn call(
        _ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        arg: &Option<Decimal>,
    ) -> Result<Option<i64>> {
        Ok(Some(arg.as_ref().map_or(0, |v| v.is_zero() as i64)))
    }
}

#[derive(Debug, Clone, Copy, RpnFunction)]
#[rpn_function(args = 1)]
pub struct RpnFnUnaryNot;

impl RpnFnUnaryNot {
    #[inline]
    fn call(
        _ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        arg: &Option<i64>,
    ) -> Result<Option<i64>> {
        Ok(arg.map(|v| (v == 0) as i64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tipb::expression::ScalarFuncSig;

    use crate::coprocessor::codec::mysql::{time, Tz};
    use crate::coprocessor::dag::rpn_expr::types::test_util::RpnFnScalarEvaluator;

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
