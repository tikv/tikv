// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::rpn_fn;
use tidb_query_common::Result;
use tidb_query_datatype::{
    codec::{Error, batch::LazyBatchColumnVec, data_type::*},
    expr::EvalContext,
};
use tipb::FieldType;

use crate::{RpnExpression, RpnStackNode, RpnStackNodeVectorValue};

#[rpn_fn(nullable)]
#[inline]
pub fn logical_and(lhs: Option<&i64>, rhs: Option<&i64>) -> Result<Option<i64>> {
    Ok(match (lhs, rhs) {
        (Some(0), _) | (_, Some(0)) => Some(0),
        (None, _) | (_, None) => None,
        _ => Some(1),
    })
}

#[rpn_fn(nullable)]
#[inline]
pub fn logical_or(arg0: Option<&i64>, arg1: Option<&i64>) -> Result<Option<i64>> {
    // This is a standard Kleene OR used in SQL where
    // `null OR false == null` and `null OR true == true`
    Ok(match (arg0, arg1) {
        (Some(0), Some(0)) => Some(0),
        (None, None) | (None, Some(0)) | (Some(0), None) => None,
        _ => Some(1),
    })
}

/// Evaluates logical AND from left to right and only evaluates each argument
/// for rows whose result has not been determined by previous arguments.
pub fn sc_logical_and(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input_physical_columns: &LazyBatchColumnVec,
    input_logical_rows: &[usize],
    output_rows: usize,
    args: &[RpnExpression],
) -> Result<VectorValue> {
    eval_logical_short_circuit::<ScLogicalAnd>(
        ctx,
        schema,
        input_physical_columns,
        input_logical_rows,
        output_rows,
        args,
    )
}

/// Evaluates logical OR from left to right and only evaluates each argument for
/// rows whose result has not been determined by previous arguments.
pub fn sc_logical_or(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input_physical_columns: &LazyBatchColumnVec,
    input_logical_rows: &[usize],
    output_rows: usize,
    args: &[RpnExpression],
) -> Result<VectorValue> {
    eval_logical_short_circuit::<ScLogicalOr>(
        ctx,
        schema,
        input_physical_columns,
        input_logical_rows,
        output_rows,
        args,
    )
}

trait ScLogicalOp {
    const IDENTITY: Int;

    #[inline]
    fn normalize_value(value: Option<Int>) -> Option<Int> {
        value.map(|v| if v != 0 { 1 } else { 0 })
    }

    #[inline]
    fn is_short_circuit(value: Option<Int>) -> bool {
        matches!(
            value,
            Some(value) if value != Self::IDENTITY
        )
    }

    #[inline]
    fn handle_res_value(
        result: &mut ChunkedVecSized<Int>,
        idx: usize,
        value: Option<Int>,
        resolved_count: &mut usize,
    ) {
        match Self::normalize_value(value) {
            Some(value) if value != Self::IDENTITY => {
                // An absorbing value determines the final result even if a
                // previous argument was NULL.
                result.set(idx, Some(value));
                (*resolved_count) += 1;
            }
            // An identity value does not change the accumulated result. In
            // particular, it must not overwrite a previous NULL.
            Some(_) => {}
            None => result.set(idx, None),
        }
    }
}

struct ScLogicalAnd;

impl ScLogicalOp for ScLogicalAnd {
    const IDENTITY: Int = 1;
}

struct ScLogicalOr;

impl ScLogicalOp for ScLogicalOr {
    const IDENTITY: Int = 0;
}

fn eval_logical_short_circuit<Op: ScLogicalOp>(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input_physical_columns: &LazyBatchColumnVec,
    input_logical_rows: &[usize],
    output_rows: usize,
    args: &[RpnExpression],
) -> Result<VectorValue> {
    assert!(args.len() >= 2);
    assert!(input_logical_rows.is_empty() || input_logical_rows.len() == output_rows);
    if output_rows == 0 {
        return Ok(VectorValue::from_scalar(&ScalarValue::Int(Some(0)), 0));
    }
    let mut result = ChunkedVecSized::<Int>::with_capacity(0);

    // `None` means all output rows are still pending. Keep this state implicit
    // until an argument resolves only part of the rows, so the common no-short-
    // circuit path does not allocate or copy row mappings.
    let mut pending_rows: Option<(Vec<usize>, Vec<usize>)> = None;

    for (arg_index, arg) in args.iter().enumerate() {
        let (pending_positions, pending_logical_rows, pending_len) = match pending_rows.as_ref() {
            Some((positions, logical_rows)) => (
                Some(positions.as_slice()),
                logical_rows.as_slice(),
                positions.len(),
            ),
            None => (None, input_logical_rows, output_rows),
        };

        let arg_result = arg.eval_decoded(
            ctx,
            schema,
            input_physical_columns,
            pending_logical_rows,
            pending_len,
        )?;

        let mut resolved_count = 0;
        let is_first = result.is_empty();
        match arg_result {
            RpnStackNode::Scalar { value, .. } => {
                let value = Op::normalize_value(value.as_int().copied());
                resolved_count = if Op::is_short_circuit(value) {
                    pending_len
                } else {
                    0
                };
                if is_first {
                    result = ChunkedVecSized::with_capacity(output_rows);
                    for _ in 0..pending_len {
                        result.push(value);
                    }
                } else if resolved_count == pending_len || value.is_none() {
                    for i in 0..pending_len {
                        let output_index = pending_positions.map_or(i, |positions| positions[i]);
                        result.set(output_index, value);
                    }
                }
            }
            RpnStackNode::Vector {
                value:
                    RpnStackNodeVectorValue::Generated {
                        physical_value: VectorValue::Int(vec_result),
                    },
                ..
            } => {
                if is_first {
                    result = vec_result;

                    for i in 0..pending_len {
                        let value = result.get_option_ref(i).copied();
                        let normalized = Op::normalize_value(value);

                        if normalized != value {
                            result.set(i, normalized);
                        }

                        if Op::is_short_circuit(normalized) {
                            resolved_count += 1;
                        }
                    }
                } else {
                    for i in 0..pending_len {
                        let output_index = pending_positions.map_or(i, |positions| positions[i]);

                        Op::handle_res_value(
                            &mut result,
                            output_index,
                            vec_result.get_option_ref(i).copied(),
                            &mut resolved_count,
                        );
                    }
                }
            }
            RpnStackNode::Vector {
                value:
                    RpnStackNodeVectorValue::Ref {
                        physical_value,
                        logical_rows,
                    },
                ..
            } => {
                let vec_result = match physical_value {
                    VectorValue::Int(vec_result) => vec_result,
                    VectorValue::Enum(vec_result) => vec_result.as_vec_int(),
                    _ => {
                        return Err(other_err!(
                            "logical expression must produce Int, got {}",
                            physical_value.eval_type()
                        ));
                    }
                };
                if is_first {
                    result = ChunkedVecSized::<Int>::with_capacity(output_rows);
                    for i in 0..pending_len {
                        let value = Op::normalize_value(
                            vec_result.get_option_ref(logical_rows[i]).copied(),
                        );
                        if Op::is_short_circuit(value) {
                            resolved_count += 1;
                        }
                        result.push(value);
                    }
                } else {
                    for i in 0..pending_len {
                        let output_index = pending_positions.map_or(i, |positions| positions[i]);
                        Op::handle_res_value(
                            &mut result,
                            output_index,
                            vec_result.get_option_ref(logical_rows[i]).copied(),
                            &mut resolved_count,
                        );
                    }
                }
            }
            RpnStackNode::Vector { value, .. } => {
                return Err(other_err!(
                    "logical expression must produce Int, got {}",
                    value.as_ref().eval_type()
                ));
            }
        }

        if resolved_count == 0 {
            continue;
        }
        if resolved_count == pending_len || arg_index + 1 == args.len() {
            return Ok(VectorValue::Int(result));
        }

        match &mut pending_rows {
            Some((positions, logical_rows)) => {
                let old_pending_len = positions.len();
                let has_logical_rows = !logical_rows.is_empty();
                let (mut read_index, mut write_index) = (0, 0);
                positions.retain(|&output_index| {
                    let keep = !matches!(
                        (&result).get_option_ref(output_index),
                        Some(value) if *value != Op::IDENTITY
                    );
                    if keep && has_logical_rows {
                        logical_rows[write_index] = logical_rows[read_index];
                        write_index += 1;
                    }

                    read_index += 1;
                    keep
                });

                debug_assert_eq!(read_index, old_pending_len);
                if has_logical_rows {
                    logical_rows.truncate(write_index);
                }
            }
            None => {
                let new_pending_len = pending_len - resolved_count;
                let mut positions = Vec::with_capacity(new_pending_len);
                let mut logical_rows = if input_logical_rows.is_empty() {
                    Vec::new()
                } else {
                    Vec::with_capacity(new_pending_len)
                };

                for output_index in 0..output_rows {
                    let keep = !matches!(
                        (&result).get_option_ref(output_index),
                        Some(value) if *value != Op::IDENTITY
                    );
                    if keep {
                        positions.push(output_index);
                        if !input_logical_rows.is_empty() {
                            logical_rows.push(input_logical_rows[output_index]);
                        }
                    }
                }

                debug_assert_eq!(positions.len(), new_pending_len);
                pending_rows = Some((positions, logical_rows));
            }
        }

        let (pending_positions, pending_logical_rows) = pending_rows.as_ref().unwrap();
        debug_assert_eq!(
            pending_logical_rows.len(),
            if input_logical_rows.is_empty() {
                0
            } else {
                pending_positions.len()
            }
        );
    }

    Ok(VectorValue::Int(result))
}

#[rpn_fn(nullable)]
#[inline]
pub fn logical_xor(arg0: Option<&i64>, arg1: Option<&i64>) -> Result<Option<i64>> {
    // evaluates to 1 if an odd number of operands is nonzero, otherwise 0 is
    // returned.
    Ok(match (arg0, arg1) {
        (Some(arg0), Some(arg1)) => Some(((*arg0 == 0) ^ (*arg1 == 0)) as i64),
        _ => None,
    })
}

#[rpn_fn(nullable)]
#[inline]
pub fn unary_not_int(arg: Option<&Int>) -> Result<Option<i64>> {
    Ok(arg.map(|v| (*v == 0) as i64))
}

#[rpn_fn(nullable)]
#[inline]
pub fn unary_not_real(arg: Option<&Real>) -> Result<Option<i64>> {
    Ok(arg.map(|v| (v.into_inner() == 0f64) as i64))
}

#[rpn_fn(nullable)]
#[inline]
pub fn unary_not_decimal(arg: Option<&Decimal>) -> Result<Option<i64>> {
    Ok(arg.as_ref().map(|v| v.is_zero() as i64))
}

#[rpn_fn(nullable)]
#[inline]
pub fn unary_not_json(arg: Option<JsonRef>) -> Result<Option<i64>> {
    let json_zero = Json::from_i64(0).unwrap();
    Ok(arg.as_ref().map(|v| {
        if v == &json_zero.as_ref() {
            return 1;
        }
        0
    }))
}

#[rpn_fn(nullable)]
#[inline]
pub fn unary_minus_uint(arg: Option<&Int>) -> Result<Option<Int>> {
    use std::cmp::Ordering::*;

    match arg {
        Some(val) => {
            let uval = *val as u64;
            match uval.cmp(&(i64::MAX as u64 + 1)) {
                Greater => Err(Error::overflow("BIGINT", format!("-{}", uval)).into()),
                Equal => Ok(Some(i64::MIN)),
                Less => Ok(Some(-*val)),
            }
        }
        None => Ok(None),
    }
}

#[rpn_fn(nullable)]
#[inline]
pub fn unary_minus_int(arg: Option<&Int>) -> Result<Option<Int>> {
    match arg {
        Some(val) => {
            if *val == i64::MIN {
                Err(Error::overflow("BIGINT", format!("-{}", *val)).into())
            } else {
                Ok(Some(-*val))
            }
        }
        None => Ok(None),
    }
}

#[rpn_fn(nullable)]
#[inline]
pub fn unary_minus_real(arg: Option<&Real>) -> Result<Option<Real>> {
    Ok(arg.map(|val| -*val))
}

#[rpn_fn(nullable)]
#[inline]
pub fn unary_minus_decimal(arg: Option<&Decimal>) -> Result<Option<Decimal>> {
    Ok(arg.map(|val| -*val))
}

#[inline]
pub fn is_null_ref<'a, T: EvaluableRef<'a>>(arg: Option<T>) -> Result<Option<i64>> {
    Ok(Some(arg.is_none() as i64))
}

#[rpn_fn(nullable)]
#[inline]
pub fn is_null<T: Evaluable + EvaluableRet>(arg: Option<&T>) -> Result<Option<i64>> {
    is_null_ref(arg)
}

#[rpn_fn(nullable)]
#[inline]
pub fn is_null_bytes(arg: Option<BytesRef>) -> Result<Option<i64>> {
    is_null_ref(arg)
}

#[rpn_fn(nullable)]
#[inline]
pub fn is_null_json(arg: Option<JsonRef>) -> Result<Option<i64>> {
    is_null_ref(arg)
}

#[rpn_fn(nullable)]
#[inline]
pub fn is_null_vector_float32(arg: Option<VectorFloat32Ref>) -> Result<Option<i64>> {
    is_null_ref(arg)
}

#[rpn_fn(nullable)]
#[inline]
pub fn bit_and(lhs: Option<&Int>, rhs: Option<&Int>) -> Result<Option<Int>> {
    Ok(match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => Some(lhs & rhs),
        _ => None,
    })
}

#[rpn_fn(nullable)]
#[inline]
pub fn bit_or(lhs: Option<&Int>, rhs: Option<&Int>) -> Result<Option<Int>> {
    Ok(match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => Some(lhs | rhs),
        _ => None,
    })
}

#[rpn_fn(nullable)]
#[inline]
pub fn bit_xor(lhs: Option<&Int>, rhs: Option<&Int>) -> Result<Option<Int>> {
    Ok(match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => Some(lhs ^ rhs),
        _ => None,
    })
}

#[rpn_fn(nullable)]
#[inline]
pub fn bit_neg(arg: Option<&Int>) -> Result<Option<Int>> {
    Ok(arg.map(|arg| !arg))
}

pub trait KeepNull {
    const VALUE: bool;
}

pub struct KeepNullOn;
impl KeepNull for KeepNullOn {
    const VALUE: bool = true;
}

pub struct KeepNullOff;
impl KeepNull for KeepNullOff {
    const VALUE: bool = false;
}

#[rpn_fn(nullable)]
#[inline]
pub fn int_is_true<K: KeepNull>(arg: Option<&Int>) -> Result<Option<i64>> {
    Ok(if K::VALUE {
        arg.map(|v| (*v != 0) as i64)
    } else {
        Some(arg.map_or(0, |v| (*v != 0) as i64))
    })
}

#[rpn_fn(nullable)]
#[inline]
pub fn real_is_true<K: KeepNull>(arg: Option<&Real>) -> Result<Option<i64>> {
    Ok(if K::VALUE {
        arg.map(|v| (v.into_inner() != 0f64) as i64)
    } else {
        Some(arg.map_or(0, |v| (v.into_inner() != 0f64) as i64))
    })
}

#[rpn_fn(nullable)]
#[inline]
pub fn decimal_is_true<K: KeepNull>(arg: Option<&Decimal>) -> Result<Option<i64>> {
    Ok(if K::VALUE {
        arg.map(|v| !v.is_zero() as i64)
    } else {
        Some(arg.map_or(0, |v| !v.is_zero() as i64))
    })
}

#[rpn_fn(nullable)]
#[inline]
pub fn int_is_false<K: KeepNull>(arg: Option<&Int>) -> Result<Option<i64>> {
    Ok(if K::VALUE {
        arg.map(|v| (*v == 0) as i64)
    } else {
        Some(arg.map_or(0, |v| (*v == 0) as i64))
    })
}

#[rpn_fn(nullable)]
#[inline]
pub fn real_is_false<K: KeepNull>(arg: Option<&Real>) -> Result<Option<i64>> {
    Ok(if K::VALUE {
        arg.map(|v| (v.into_inner() == 0f64) as i64)
    } else {
        Some(arg.map_or(0, |v| (v.into_inner() == 0f64) as i64))
    })
}

#[rpn_fn(nullable)]
#[inline]
fn decimal_is_false<K: KeepNull>(arg: Option<&Decimal>) -> Result<Option<i64>> {
    Ok(if K::VALUE {
        arg.map(|v| v.is_zero() as i64)
    } else {
        Some(arg.map_or(0, |v| v.is_zero() as i64))
    })
}

#[rpn_fn(nullable)]
#[inline]
fn left_shift(lhs: Option<&Int>, rhs: Option<&Int>) -> Result<Option<Int>> {
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

#[rpn_fn(nullable)]
#[inline]
fn right_shift(lhs: Option<&Int>, rhs: Option<&Int>) -> Result<Option<Int>> {
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
    use tidb_query_datatype::{
        FieldTypeFlag, FieldTypeTp, builder::FieldTypeBuilder, codec::mysql::TimeType,
        expr::EvalContext,
    };
    use tipb::ScalarFuncSig;

    use super::*;
    use crate::test_util::RpnFnScalarEvaluator;

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
    fn test_logical_short_circuit_rejects_non_int_column() {
        let args = vec![
            crate::RpnExpressionBuilder::new_for_test()
                .push_column_ref_for_test(0)
                .build_for_test(),
            crate::RpnExpressionBuilder::new_for_test()
                .push_column_ref_for_test(1)
                .build_for_test(),
        ];
        let columns = LazyBatchColumnVec::from(vec![
            VectorValue::Real(vec![Real::new(1.0).ok()].into()),
            VectorValue::Int(vec![Some(0)].into()),
        ]);
        let schema = &[FieldTypeTp::Double.into(), FieldTypeTp::LongLong.into()];

        let err = sc_logical_or(
            &mut EvalContext::default(),
            schema,
            &columns,
            &[0],
            1,
            &args,
        )
        .unwrap_err();

        assert!(err.to_string().contains("must produce Int, got Real"));
    }

    #[test]
    fn test_logical_short_circuit_rejects_non_int_generated_vector() {
        let args = vec![
            crate::RpnExpressionBuilder::new_for_test()
                .push_constant_for_test(1.0)
                .push_fn_call_for_test(unary_minus_real_fn_meta(), 1, FieldTypeTp::Double)
                .build_for_test(),
            crate::RpnExpressionBuilder::new_for_test()
                .push_constant_for_test(0_i64)
                .build_for_test(),
        ];

        let err = sc_logical_or(
            &mut EvalContext::default(),
            &[],
            &LazyBatchColumnVec::empty(),
            &[],
            1,
            &args,
        )
        .unwrap_err();

        assert!(err.to_string().contains("must produce Int, got Real"));
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
                .push_param(arg)
                .evaluate(ScalarFuncSig::UnaryNotDecimal)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", arg);
        }
    }

    #[test]
    fn test_unary_not_json() {
        let test_cases = vec![
            (None, None),
            (Some(Json::from_i64(0).unwrap()), Some(1)),
            (Some(Json::from_i64(1).unwrap()), Some(0)),
            (
                Some(Json::from_array(vec![Json::from_i64(0).unwrap()]).unwrap()),
                Some(0),
            ),
        ];
        for (arg, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg.clone())
                .evaluate(ScalarFuncSig::UnaryNotJson)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", arg.as_ref());
        }
    }

    #[test]
    fn test_unary_minus_int() {
        let unsigned_test_cases = vec![
            (None, None),
            (Some((i64::MAX as u64 + 1) as i64), Some(i64::MIN)),
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
        RpnFnScalarEvaluator::new()
            .push_param_with_field_type(
                Some((i64::MAX as u64 + 2) as i64),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::LongLong)
                    .flag(FieldTypeFlag::UNSIGNED)
                    .build(),
            )
            .evaluate::<Int>(ScalarFuncSig::UnaryMinusInt)
            .unwrap_err();

        let signed_test_cases = vec![
            (None, None),
            (Some(i64::MAX), Some(-i64::MAX)),
            (Some(-i64::MAX), Some(i64::MAX)),
            (Some(i64::MIN + 1), Some(i64::MAX)),
            (Some(0), Some(0)),
        ];
        for (arg, expect_output) in signed_test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Int>(ScalarFuncSig::UnaryMinusInt)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", arg);
        }
        RpnFnScalarEvaluator::new()
            .push_param(i64::MIN)
            .evaluate::<Int>(ScalarFuncSig::UnaryMinusInt)
            .unwrap_err();
    }

    #[test]
    fn test_unary_minus_real() {
        let test_cases = vec![
            (None, None),
            (
                Some(Real::new(0.123_f64).unwrap()),
                Some(Real::new(-0.123_f64).unwrap()),
            ),
            (
                Some(Real::new(-0.123_f64).unwrap()),
                Some(Real::new(0.123_f64).unwrap()),
            ),
            (
                Some(Real::new(0.0_f64).unwrap()),
                Some(Real::new(0.0_f64).unwrap()),
            ),
            (
                Some(Real::new(f64::INFINITY).unwrap()),
                Some(Real::new(f64::NEG_INFINITY).unwrap()),
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
                .push_param(arg)
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
                Json::from_array(vec![]).unwrap().into(),
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
            (
                ScalarValue::Int(None),
                ScalarFuncSig::IntIsTrueWithNull,
                None,
            ),
            (0.into(), ScalarFuncSig::IntIsTrue, Some(0)),
            (0.into(), ScalarFuncSig::IntIsTrueWithNull, Some(0)),
            (1.into(), ScalarFuncSig::IntIsTrue, Some(1)),
            (1.into(), ScalarFuncSig::IntIsTrueWithNull, Some(1)),
            (ScalarValue::Real(None), ScalarFuncSig::RealIsTrue, Some(0)),
            (
                ScalarValue::Real(None),
                ScalarFuncSig::RealIsTrueWithNull,
                None,
            ),
            (0.0.into(), ScalarFuncSig::RealIsTrue, Some(0)),
            (0.0.into(), ScalarFuncSig::RealIsTrueWithNull, Some(0)),
            (1.0.into(), ScalarFuncSig::RealIsTrue, Some(1)),
            (1.0.into(), ScalarFuncSig::RealIsTrueWithNull, Some(1)),
            (
                ScalarValue::Decimal(None),
                ScalarFuncSig::DecimalIsTrue,
                Some(0),
            ),
            (
                ScalarValue::Decimal(None),
                ScalarFuncSig::DecimalIsTrueWithNull,
                None,
            ),
            (
                Decimal::zero().into(),
                ScalarFuncSig::DecimalIsTrue,
                Some(0),
            ),
            (
                Decimal::zero().into(),
                ScalarFuncSig::DecimalIsTrueWithNull,
                Some(0),
            ),
            (
                Decimal::from(1).into(),
                ScalarFuncSig::DecimalIsTrue,
                Some(1),
            ),
            (
                Decimal::from(1).into(),
                ScalarFuncSig::DecimalIsTrueWithNull,
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
