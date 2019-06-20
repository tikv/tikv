// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use cop_codegen::rpn_fn;

use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::Result;

#[rpn_fn]
#[inline]
fn if_null<T: Evaluable>(lhs: &Option<T>, rhs: &Option<T>) -> Result<Option<T>> {
    if lhs.is_some() {
        return Ok(lhs.clone());
    }
    Ok(rhs.clone())
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
            assert_eq!(output, expected, "lhs={:?}, rhs={:?}", lhs, rhs);
        }
    }
}
