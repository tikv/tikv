// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::types::RpnFnCallPayload;
use crate::expr::EvalContext;
use crate::Result;

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

#[cfg(test)]
mod tests {
    use super::*;

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
                .evaluate(RpnFnLogicalAnd)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }
}
