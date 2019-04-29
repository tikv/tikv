// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::types::RpnFnCallPayload;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::Result;

#[derive(Debug, Clone, Copy)]
pub struct RpnFnLogicalAnd;

impl_template_fn! { 2 arg @ RpnFnLogicalAnd }

impl RpnFnLogicalAnd {
    #[inline]
    fn call(
        _ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        arg0: &Option<i64>,
        arg1: &Option<i64>,
    ) -> Result<Option<i64>> {
        // Intentionally not merging `None` and `Some(0)` conditions to be clear.
        Ok(match (arg0, arg1) {
            (Some(0), _) | (_, Some(0)) => Some(0),
            (None, _) | (_, None) => None,
            _ => Some(1),
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RpnFnLogicalOr;

impl_template_fn! { 2 arg @ RpnFnLogicalOr }

impl RpnFnLogicalOr {
    #[inline]
    fn call(
        _ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        arg0: &Option<i64>,
        arg1: &Option<i64>,
    ) -> Result<Option<i64>> {
        // Rule 1. As long as there is `Some(x)` where x is not 0, the result is `1`.
        // Rule 2. As long as there is `None`, the result is `None`.
        // Rule 3. For the rest, the result is `0`.
        Ok(match arg0 {
            None => match arg1 {
                None => None,
                Some(0) => None,
                Some(_) => Some(1),
            },
            Some(0) => match arg1 {
                None => None,
                Some(0) => Some(0),
                Some(_) => Some(1),
            },
            Some(_) => Some(1),
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RpnFnIntIsNull;

impl_template_fn! { 1 arg @ RpnFnIntIsNull }

impl RpnFnIntIsNull {
    #[inline]
    fn call(
        _ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        arg0: &Option<i64>,
    ) -> Result<Option<i64>> {
        Ok(Some(arg0.is_none() as i64))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RpnFnUnaryNot;

impl_template_fn! { 1 arg @ RpnFnUnaryNot }

impl RpnFnUnaryNot {
    #[inline]
    fn call(
        _ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        arg0: &Option<i64>,
    ) -> Result<Option<i64>> {
        Ok(arg0.map(|arg| (arg == 0) as i64))
    }
}

#[cfg(test)]
mod tests {
    use tipb::expression::ScalarFuncSig;

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
}
