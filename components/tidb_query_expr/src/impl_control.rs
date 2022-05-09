// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::rpn_fn;
use tidb_query_common::Result;
use tidb_query_datatype::codec::data_type::*;

#[rpn_fn(nullable)]
#[inline]
fn if_null<T: Evaluable + EvaluableRet>(lhs: Option<&T>, rhs: Option<&T>) -> Result<Option<T>> {
    if lhs.is_some() {
        return Ok(lhs.cloned());
    }
    Ok(rhs.cloned())
}

#[rpn_fn(nullable)]
#[inline]
fn if_null_json(lhs: Option<JsonRef>, rhs: Option<JsonRef>) -> Result<Option<Json>> {
    if lhs.is_some() {
        return Ok(lhs.map(|x| x.to_owned()));
    }
    Ok(rhs.map(|x| x.to_owned()))
}

#[rpn_fn(nullable)]
#[inline]
fn if_null_bytes(lhs: Option<BytesRef>, rhs: Option<BytesRef>) -> Result<Option<Bytes>> {
    if lhs.is_some() {
        return Ok(lhs.map(|x| x.to_vec()));
    }
    Ok(rhs.map(|x| x.to_vec()))
}

#[rpn_fn(nullable, raw_varg, extra_validator = case_when_validator::<T>)]
#[inline]
pub fn case_when<T: Evaluable + EvaluableRet>(args: &[ScalarValueRef<'_>]) -> Result<Option<T>> {
    for chunk in args.chunks(2) {
        if chunk.len() == 1 {
            // Else statement
            let ret: Option<&T> = Evaluable::borrow_scalar_value_ref(chunk[0]);
            return Ok(ret.cloned());
        }
        let cond: Option<&Int> = Evaluable::borrow_scalar_value_ref(chunk[0]);
        if cond.cloned().unwrap_or(0) != 0 {
            let ret: Option<&T> = Evaluable::borrow_scalar_value_ref(chunk[1]);
            return Ok(ret.cloned());
        }
    }
    Ok(None)
}

#[rpn_fn(nullable, raw_varg, extra_validator = case_when_validator::<Bytes>)]
#[inline]
pub fn case_when_bytes(args: &[ScalarValueRef<'_>]) -> Result<Option<Bytes>> {
    for chunk in args.chunks(2) {
        if chunk.len() == 1 {
            // Else statement
            let ret: Option<BytesRef> = EvaluableRef::borrow_scalar_value_ref(chunk[0]);
            return Ok(ret.map(|x| x.to_vec()));
        }
        let cond: Option<&Int> = Evaluable::borrow_scalar_value_ref(chunk[0]);
        if cond.cloned().unwrap_or(0) != 0 {
            let ret: Option<BytesRef> = EvaluableRef::borrow_scalar_value_ref(chunk[1]);
            return Ok(ret.map(|x| x.to_vec()));
        }
    }
    Ok(None)
}

#[rpn_fn(nullable, raw_varg, extra_validator = case_when_validator::<Json>)]
#[inline]
pub fn case_when_json(args: &[ScalarValueRef<'_>]) -> Result<Option<Json>> {
    for chunk in args.chunks(2) {
        if chunk.len() == 1 {
            // Else statement
            let ret: Option<JsonRef> = EvaluableRef::borrow_scalar_value_ref(chunk[0]);
            return Ok(ret.map(|x| x.to_owned()));
        }
        let cond: Option<&Int> = Evaluable::borrow_scalar_value_ref(chunk[0]);
        if cond.cloned().unwrap_or(0) != 0 {
            let ret: Option<JsonRef> = EvaluableRef::borrow_scalar_value_ref(chunk[1]);
            return Ok(ret.map(|x| x.to_owned()));
        }
    }
    Ok(None)
}

#[rpn_fn(nullable)]
#[inline]
fn if_condition<T: Evaluable + EvaluableRet>(
    condition: Option<&Int>,
    value_if_true: Option<&T>,
    value_if_false: Option<&T>,
) -> Result<Option<T>> {
    Ok(if condition.cloned().unwrap_or(0) != 0 {
        value_if_true.cloned()
    } else {
        value_if_false.cloned()
    })
}

#[rpn_fn(nullable)]
#[inline]
fn if_condition_json(
    condition: Option<&Int>,
    value_if_true: Option<JsonRef>,
    value_if_false: Option<JsonRef>,
) -> Result<Option<Json>> {
    Ok(if condition.cloned().unwrap_or(0) != 0 {
        value_if_true.map(|x| x.to_owned())
    } else {
        value_if_false.map(|x| x.to_owned())
    })
}

#[rpn_fn(nullable)]
#[inline]
fn if_condition_bytes(
    condition: Option<&Int>,
    value_if_true: Option<BytesRef>,
    value_if_false: Option<BytesRef>,
) -> Result<Option<Bytes>> {
    Ok(if condition.cloned().unwrap_or(0) != 0 {
        value_if_true.map(|x| x.to_vec())
    } else {
        value_if_false.map(|x| x.to_vec())
    })
}

fn case_when_validator<T: EvaluableRet>(expr: &tipb::Expr) -> Result<()> {
    for chunk in expr.get_children().chunks(2) {
        if chunk.len() == 1 {
            super::function::validate_expr_return_type(&chunk[0], T::EVAL_TYPE)?;
        } else {
            super::function::validate_expr_return_type(&chunk[0], <Int as Evaluable>::EVAL_TYPE)?;
            super::function::validate_expr_return_type(&chunk[1], T::EVAL_TYPE)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tipb::ScalarFuncSig;

    use super::*;
    use crate::test_util::RpnFnScalarEvaluator;

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

    #[test]
    fn test_case_when() {
        let cases: Vec<(Vec<ScalarValue>, Option<Real>)> = vec![
            (
                vec![1.into(), (3.0).into(), 1.into(), (5.0).into()],
                Real::new(3.0).ok(),
            ),
            (
                vec![0.into(), (3.0).into(), 1.into(), (5.0).into()],
                Real::new(5.0).ok(),
            ),
            (
                vec![ScalarValue::Int(None), (2.0).into(), 1.into(), (6.0).into()],
                Real::new(6.0).ok(),
            ),
            (vec![(7.0).into()], Real::new(7.0).ok()),
            (vec![0.into(), ScalarValue::Real(None)], None),
            (vec![1.into(), ScalarValue::Real(None)], None),
            (vec![1.into(), (3.5).into()], Real::new(3.5).ok()),
            (vec![2.into(), (3.5).into()], Real::new(3.5).ok()),
            (
                vec![
                    0.into(),
                    ScalarValue::Real(None),
                    ScalarValue::Int(None),
                    ScalarValue::Real(None),
                    (5.5).into(),
                ],
                Real::new(5.5).ok(),
            ),
        ];

        for (args, expected) in cases {
            let mut evaluator = RpnFnScalarEvaluator::new();
            for arg in args {
                evaluator = evaluator.push_param(arg);
            }
            let output = evaluator.evaluate(ScalarFuncSig::CaseWhenReal).unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_case_when_bytes() {
        let cases: Vec<(Vec<ScalarValue>, Option<Bytes>)> = vec![
            (
                vec![
                    1.into(),
                    vec![1, 2, 3].into(),
                    1.into(),
                    vec![4, 5, 6].into(),
                ],
                Some(vec![1, 2, 3]),
            ),
            (
                vec![
                    0.into(),
                    vec![1, 2, 3].into(),
                    1.into(),
                    vec![4, 5, 6].into(),
                ],
                Some(vec![4, 5, 6]),
            ),
        ];

        for (args, expected) in cases {
            let mut evaluator = RpnFnScalarEvaluator::new();
            for arg in args {
                evaluator = evaluator.push_param(arg);
            }
            let output = evaluator.evaluate(ScalarFuncSig::CaseWhenString).unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_if() {
        use std::f64::consts::{E, PI};

        let cases = vec![
            ((Some(0), E, PI), Real::new(PI).ok()),
            ((Some(1), E, PI), Real::new(E).ok()),
            ((None, E, PI), Real::new(PI).ok()),
        ];

        for ((condition, value1, value2), expected) in cases {
            assert_eq!(
                expected,
                RpnFnScalarEvaluator::new()
                    .push_param(condition)
                    .push_param(value1)
                    .push_param(value2)
                    .evaluate(ScalarFuncSig::IfReal)
                    .unwrap()
            );
        }
    }
}
