// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryFrom;

use cop_codegen::rpn_fn;
use cop_datatype::{EvalType, FieldTypeAccessor, FieldTypeFlag};
use tipb::expression::FieldType;

use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::dag::rpn_expr::{RpnExpressionNode, RpnFnCallExtra};
use crate::coprocessor::Result;

/// Gets the cast function between specified data types.
///
/// TODO: This function supports some internal casts performed by TiKV. However it would be better
/// to be done in TiDB.
pub fn get_cast_fn_rpn_node(
    from_field_type: &FieldType,
    to_field_type: FieldType,
) -> Result<RpnExpressionNode> {
    let from = box_try!(EvalType::try_from(from_field_type.tp()));
    let to = box_try!(EvalType::try_from(to_field_type.tp()));
    let func_meta = match (from, to) {
        (EvalType::Int, EvalType::Decimal) => {
            if !is_unsigned(from_field_type) && !is_unsigned(&to_field_type) {
                cast_int_as_decimal_fn_meta()
            } else {
                cast_uint_as_decimal_fn_meta()
            }
        }
        (EvalType::Bytes, EvalType::Real) => cast_string_as_real_fn_meta(),
        (EvalType::DateTime, EvalType::Real) => cast_time_as_real_fn_meta(),
        (EvalType::Duration, EvalType::Real) => cast_duration_as_real_fn_meta(),
        (EvalType::Json, EvalType::Real) => cast_json_as_real_fn_meta(),
        _ => return Err(box_err!("Unsupported cast from {} to {}", from, to)),
    };
    // This cast function is inserted by `Coprocessor` automatically,
    // the `inUnion` flag always false in this situation. Ideally,
    // the cast function should be inserted by TiDB and pushed down
    // with all implicit arguments.
    Ok(RpnExpressionNode::FnCall {
        func_meta,
        args_len: 1,
        field_type: to_field_type,
        implicit_args: Vec::new(),
    })
}

#[inline]
fn is_unsigned(ft: &FieldType) -> bool {
    ft.as_accessor().flag().contains(FieldTypeFlag::UNSIGNED)
}

fn produce_dec_with_specified_tp(
    ctx: &mut EvalContext,
    dec: Decimal,
    ft: &FieldType,
) -> Result<Decimal> {
    // FIXME: The implementation is not exactly the same as TiDB's `ProduceDecWithSpecifiedTp`.
    let (flen, decimal) = (ft.flen(), ft.decimal());
    if flen == cop_datatype::UNSPECIFIED_LENGTH || decimal == cop_datatype::UNSPECIFIED_LENGTH {
        return Ok(dec);
    }
    Ok(dec.convert_to(ctx, flen as u8, decimal as u8)?)
}

// TODO: remove it after CAST function use `in_union` function
#[allow(dead_code)]

/// Indicates whether the current expression is evaluated in union statement
///
/// Note: The TiDB will push down the `inUnion` flag by implicit constant arguments,
/// but some CAST expressions inserted by TiKV coprocessor use an empty vector to represent
/// the `inUnion` flag is false.
/// See: https://github.com/pingcap/tidb/blob/1e403873d905b2d0ad3be06bd8cd261203d84638/expression/builtin.go#L260
fn in_union(implicit_args: &[ScalarValue]) -> bool {
    implicit_args.get(0) == Some(&ScalarValue::Int(Some(1)))
}

/// The unsigned int implementation for push down signature `CastIntAsDecimal`.
#[rpn_fn(capture = [ctx, extra])]
#[inline]
pub fn cast_uint_as_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<i64>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let dec = Decimal::from(*val as u64);
            Ok(Some(produce_dec_with_specified_tp(
                ctx,
                dec,
                extra.ret_field_type,
            )?))
        }
    }
}

/// The signed int implementation for push down signature `CastIntAsDecimal`.
#[rpn_fn(capture = [ctx, extra])]
#[inline]
pub fn cast_int_as_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<i64>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let dec = Decimal::from(*val);
            Ok(Some(produce_dec_with_specified_tp(
                ctx,
                dec,
                extra.ret_field_type,
            )?))
        }
    }
}

/// The implementation for push down signature `CastStringAsReal`.
#[rpn_fn(capture = [ctx])]
#[inline]
pub fn cast_string_as_real(ctx: &mut EvalContext, val: &Option<Bytes>) -> Result<Option<Real>> {
    use crate::coprocessor::codec::convert::bytes_to_f64;

    match val {
        None => Ok(None),
        Some(val) => {
            let val = bytes_to_f64(ctx, val.as_slice())?;
            // FIXME: There is an additional step `ProduceFloatWithSpecifiedTp` in TiDB.
            Ok(Real::new(val).ok())
        }
    }
}

/// The implementation for push down signature `CastTimeAsReal`.
#[rpn_fn]
#[inline]
pub fn cast_time_as_real(val: &Option<DateTime>) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.to_decimal()?.as_f64()?;
            Ok(Real::new(val).ok())
        }
    }
}

/// The implementation for push down signature `CastDurationAsReal`.
#[rpn_fn]
#[inline]
fn cast_duration_as_real(val: &Option<Duration>) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = Decimal::try_from(*val)?.as_f64()?;
            Ok(Real::new(val).ok())
        }
    }
}

/// The implementation for push down signature `CastJsonAsReal`.
#[rpn_fn(capture = [ctx])]
#[inline]
fn cast_json_as_real(ctx: &mut EvalContext, val: &Option<Json>) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.cast_to_real(ctx)?;
            Ok(Real::new(val).ok())
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_in_union() {
        use super::*;

        assert_eq!(in_union(&[]), false);
        assert_eq!(in_union(&[ScalarValue::Int(None)]), false);
        assert_eq!(in_union(&[ScalarValue::Int(Some(0))]), false);
        assert_eq!(
            in_union(&[ScalarValue::Int(Some(0)), ScalarValue::Int(Some(1))]),
            false
        );
        assert_eq!(in_union(&[ScalarValue::Int(Some(1))]), true);
        assert_eq!(
            in_union(&[ScalarValue::Int(Some(1)), ScalarValue::Int(Some(0))]),
            true
        );
    }
}
