// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use cop_codegen::RpnFunction;

use super::types::RpnFnCallPayload;
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::Result;

#[derive(RpnFunction)]
#[rpn_function(args = 2)]
pub struct RpnFnIfNull<T: Evaluable> {
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Evaluable> RpnFnIfNull<T> {
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
        lhs: &Option<T>,
        rhs: &Option<T>,
    ) -> Result<Option<T>> {
        if lhs.is_some() {
            return Ok(lhs.clone());
        }
        Ok(rhs.clone())
    }
}

impl<T: Evaluable> std::fmt::Debug for RpnFnIfNull<T> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RpnFnIfNull")
    }
}

impl<T: Evaluable> Copy for RpnFnIfNull<T> {}

impl<T: Evaluable> Clone for RpnFnIfNull<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use tipb::expression::ScalarFuncSig;

    use crate::coprocessor::dag::rpn_expr::types::test_util::RpnFnScalarEvaluator;

    #[test]
    fn test_if_null() {
        let cases = vec![
            (None, None, None),
            (None, Some(1), Some(1)),
            (Some(2), None, Some(2)),
            (Some(2), Some(1), Some(2)),
        ];
        for (lhs, rhs, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(lhs)
                .push_param(rhs)
                .evaluate(ScalarFuncSig::IfNullInt)
                .unwrap();
            assert_eq!(output, expected, "lhs={}, rhs={}", lhs, rhs);
        }
    }
}
