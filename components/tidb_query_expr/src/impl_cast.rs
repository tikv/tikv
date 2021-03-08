// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::num::IntErrorKind;

use num_traits::identities::Zero;
use tidb_query_codegen::rpn_fn;
use tidb_query_datatype::*;
use tipb::{Expr, FieldType};

use crate::types::RpnExpressionBuilder;
use crate::{RpnExpressionNode, RpnFnCallExtra, RpnFnMeta};
use tidb_query_common::Result;
use tidb_query_datatype::codec::convert::*;
use tidb_query_datatype::codec::data_type::*;
use tidb_query_datatype::codec::error::{ERR_DATA_OUT_OF_RANGE, ERR_TRUNCATE_WRONG_VALUE};
use tidb_query_datatype::codec::mysql::time::{MAX_YEAR, MIN_YEAR};
use tidb_query_datatype::codec::mysql::{binary_literal, Time};
use tidb_query_datatype::codec::Error;
use tidb_query_datatype::expr::EvalContext;

fn get_cast_fn_rpn_meta(
    is_from_constant: bool,
    from_field_type: &FieldType,
    to_field_type: &FieldType,
) -> Result<RpnFnMeta> {
    let from = box_try!(EvalType::try_from(from_field_type.as_accessor().tp()));
    let to = box_try!(EvalType::try_from(to_field_type.as_accessor().tp()));
    let func_meta = match (from, to) {
        // any as int
        (EvalType::Int, EvalType::Int) => {
            if !from_field_type.is_unsigned() && to_field_type.is_unsigned() {
                cast_signed_int_as_unsigned_int_fn_meta()
            } else {
                cast_int_as_int_others_fn_meta()
            }
        }
        (EvalType::Real, EvalType::Int) => {
            if to_field_type.is_unsigned() {
                cast_real_as_uint_fn_meta()
            } else {
                cast_any_as_any_fn_meta::<Real, Int>()
            }
        }
        (EvalType::Bytes, EvalType::Int) => {
            if is_from_constant && from_field_type.is_binary_string_like() {
                cast_binary_string_as_int_fn_meta()
            } else {
                cast_string_as_int_fn_meta()
            }
        }
        (EvalType::Decimal, EvalType::Int) => {
            if to_field_type.is_unsigned() {
                cast_decimal_as_uint_fn_meta()
            } else {
                cast_any_as_any_fn_meta::<Decimal, Int>()
            }
        }
        (EvalType::DateTime, EvalType::Int) => cast_any_as_any_fn_meta::<DateTime, Int>(),
        (EvalType::Duration, EvalType::Int) => cast_any_as_any_fn_meta::<Duration, Int>(),
        (EvalType::Json, EvalType::Int) => {
            if to_field_type.is_unsigned() {
                cast_json_as_uint_fn_meta()
            } else {
                cast_json_as_any_fn_meta::<Int>()
            }
        }

        //  any as real
        (EvalType::Int, EvalType::Real) => {
            let fu = from_field_type.is_unsigned();
            let ru = to_field_type.is_unsigned();
            match (fu, ru) {
                (true, _) => cast_unsigned_int_as_signed_or_unsigned_real_fn_meta(),
                (false, false) => cast_signed_int_as_signed_real_fn_meta(),
                (false, true) => cast_signed_int_as_unsigned_real_fn_meta(),
            }
        }
        (EvalType::Real, EvalType::Real) => {
            if to_field_type.is_unsigned() {
                cast_real_as_signed_real_fn_meta()
            } else {
                cast_real_as_unsigned_real_fn_meta()
            }
        }
        (EvalType::Bytes, EvalType::Real) => {
            match (
                is_from_constant && from_field_type.is_binary_string_like(),
                to_field_type.is_unsigned(),
            ) {
                (true, true) => cast_binary_string_as_unsigned_real_fn_meta(),
                (true, false) => cast_binary_string_as_signed_real_fn_meta(),
                (false, true) => cast_string_as_unsigned_real_fn_meta(),
                (false, false) => cast_string_as_signed_real_fn_meta(),
            }
        }
        (EvalType::Decimal, EvalType::Real) => {
            if to_field_type.is_unsigned() {
                cast_decimal_as_unsigned_real_fn_meta()
            } else {
                cast_any_as_any_fn_meta::<Decimal, Real>()
            }
        }
        (EvalType::DateTime, EvalType::Real) => cast_any_as_any_fn_meta::<DateTime, Real>(),
        (EvalType::Duration, EvalType::Real) => cast_any_as_any_fn_meta::<Duration, Real>(),
        (EvalType::Json, EvalType::Real) => cast_json_as_any_fn_meta::<Real>(),

        // any as string
        (EvalType::Int, EvalType::Bytes) => {
            if FieldTypeAccessor::tp(from_field_type) == FieldTypeTp::Year {
                cast_year_as_string_fn_meta()
            } else if from_field_type.is_unsigned() {
                cast_uint_as_string_fn_meta()
            } else {
                cast_any_as_string_fn_meta::<Int>()
            }
        }
        (EvalType::Real, EvalType::Bytes) => {
            if FieldTypeAccessor::tp(from_field_type) == FieldTypeTp::Float {
                cast_float_real_as_string_fn_meta()
            } else {
                cast_any_as_string_fn_meta::<Real>()
            }
        }
        (EvalType::Bytes, EvalType::Bytes) => cast_string_as_string_fn_meta(),
        (EvalType::Decimal, EvalType::Bytes) => cast_any_as_string_fn_meta::<Decimal>(),
        (EvalType::DateTime, EvalType::Bytes) => cast_any_as_string_fn_meta::<DateTime>(),
        (EvalType::Duration, EvalType::Bytes) => cast_any_as_string_fn_meta::<Duration>(),
        (EvalType::Json, EvalType::Bytes) => cast_json_as_bytes_fn_meta(),

        // any as decimal
        (EvalType::Int, EvalType::Decimal) => {
            let fu = from_field_type.is_unsigned();
            let ru = to_field_type.is_unsigned();
            match (fu, ru) {
                (true, _) => cast_unsigned_int_as_signed_or_unsigned_decimal_fn_meta(),
                (false, true) => cast_signed_int_as_unsigned_decimal_fn_meta(),
                (false, false) => cast_any_as_decimal_fn_meta::<Int>(),
            }
        }
        (EvalType::Real, EvalType::Decimal) => cast_real_as_decimal_fn_meta(),
        (EvalType::Bytes, EvalType::Decimal) => {
            if to_field_type.is_unsigned() {
                cast_string_as_unsigned_decimal_fn_meta()
            } else {
                cast_bytes_as_decimal_fn_meta()
            }
        }
        (EvalType::Decimal, EvalType::Decimal) => {
            if to_field_type.is_unsigned() {
                cast_decimal_as_unsigned_decimal_fn_meta()
            } else {
                cast_decimal_as_signed_decimal_fn_meta()
            }
        }
        (EvalType::DateTime, EvalType::Decimal) => cast_any_as_decimal_fn_meta::<DateTime>(),
        (EvalType::Duration, EvalType::Decimal) => cast_any_as_decimal_fn_meta::<Duration>(),
        (EvalType::Json, EvalType::Decimal) => cast_json_as_decimal_fn_meta(),

        // any as duration
        (EvalType::Int, EvalType::Duration) => cast_int_as_duration_fn_meta(),
        (EvalType::Real, EvalType::Duration) => cast_real_as_duration_fn_meta(),
        (EvalType::Bytes, EvalType::Duration) => cast_bytes_as_duration_fn_meta(),
        (EvalType::Decimal, EvalType::Duration) => cast_decimal_as_duration_fn_meta(),
        (EvalType::DateTime, EvalType::Duration) => cast_time_as_duration_fn_meta(),
        (EvalType::Duration, EvalType::Duration) => cast_duration_as_duration_fn_meta(),
        (EvalType::Json, EvalType::Duration) => cast_json_as_duration_fn_meta(),

        (EvalType::Int, EvalType::DateTime) => {
            if FieldTypeAccessor::tp(from_field_type) == FieldTypeTp::Year {
                cast_year_as_time_fn_meta()
            } else {
                cast_int_as_time_fn_meta()
            }
        }
        (EvalType::Real, EvalType::DateTime) => cast_real_as_time_fn_meta(),
        (EvalType::Bytes, EvalType::DateTime) => cast_string_as_time_fn_meta(),
        (EvalType::Decimal, EvalType::DateTime) => cast_decimal_as_time_fn_meta(),
        (EvalType::DateTime, EvalType::DateTime) => cast_time_as_time_fn_meta(),
        (EvalType::Duration, EvalType::DateTime) => cast_duration_as_time_fn_meta(),

        // any as json
        (EvalType::Int, EvalType::Json) => {
            if from_field_type.is_bool() {
                cast_bool_as_json_fn_meta()
            } else if from_field_type.is_unsigned() {
                cast_uint_as_json_fn_meta()
            } else {
                cast_any_as_json_fn_meta::<Int>()
            }
        }
        (EvalType::Real, EvalType::Json) => cast_any_as_json_fn_meta::<Real>(),
        (EvalType::Bytes, EvalType::Json) => cast_string_as_json_fn_meta(),
        (EvalType::Decimal, EvalType::Json) => cast_any_as_json_fn_meta::<Decimal>(),
        (EvalType::DateTime, EvalType::Json) => cast_any_as_json_fn_meta::<DateTime>(),
        (EvalType::Duration, EvalType::Json) => cast_any_as_json_fn_meta::<Duration>(),
        (EvalType::Json, EvalType::Json) => cast_json_as_json_fn_meta(),

        _ => return Err(other_err!("Unsupported cast from {} to {}", from, to)),
    };
    Ok(func_meta)
}

/// Gets the cast function between specified data types.
///
/// TODO: This function supports some internal casts performed by TiKV. However it would be better
/// to be done in TiDB.
pub fn get_cast_fn_rpn_node(
    is_from_constant: bool,
    from_field_type: &FieldType,
    to_field_type: FieldType,
) -> Result<RpnExpressionNode> {
    let func_meta = get_cast_fn_rpn_meta(is_from_constant, from_field_type, &to_field_type)?;
    // This cast function is inserted by `Coprocessor` automatically,
    // the `inUnion` flag always false in this situation. Ideally,
    // the cast function should be inserted by TiDB and pushed down
    // with all implicit arguments.
    Ok(RpnExpressionNode::FnCall {
        func_meta,
        args_len: 1,
        field_type: to_field_type,
        metadata: Box::new(tipb::InUnionMetadata::default()),
    })
}

/// Gets the RPN function meta
pub fn map_cast_func(expr: &Expr) -> Result<RpnFnMeta> {
    let children = expr.get_children();
    if children.len() != 1 {
        return Err(other_err!(
            "Unexpected arguments: sig {:?} with {} args",
            expr.get_sig(),
            children.len()
        ));
    }
    get_cast_fn_rpn_meta(
        RpnExpressionBuilder::is_expr_eval_to_scalar(&children[0])?,
        children[0].get_field_type(),
        expr.get_field_type(),
    )
}

// cast any as int/uint, some cast functions reuse `cast_any_as_any`
//
// - cast_real_as_int -> cast_any_as_any<Real, Int>
// - cast_decimal_as_int -> cast_any_as_any<Decimal, Int>
// - cast_time_as_int_or_uint -> cast_any_as_any<Time, Int>
// - cast_duration_as_int_or_uint -> cast_any_as_any<Duration, Int>
// - cast_json_as_int -> cast_any_as_any<Json, Int>

#[rpn_fn(nullable, capture = [metadata], metadata_type = tipb::InUnionMetadata)]
#[inline]
fn cast_signed_int_as_unsigned_int(
    metadata: &tipb::InUnionMetadata,
    val: Option<&Int>,
) -> Result<Option<Int>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = *val;
            if metadata.get_in_union() && val < 0i64 {
                Ok(Some(0))
            } else {
                Ok(Some(val))
            }
        }
    }
}

#[rpn_fn(nullable)]
#[inline]
fn cast_int_as_int_others(val: Option<&Int>) -> Result<Option<Int>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(*val)),
    }
}

#[rpn_fn(nullable, capture = [ctx, metadata], metadata_type = tipb::InUnionMetadata)]
#[inline]
fn cast_real_as_uint(
    ctx: &mut EvalContext,
    metadata: &tipb::InUnionMetadata,
    val: Option<&Real>,
) -> Result<Option<Int>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.into_inner();
            if metadata.get_in_union() && val < 0f64 {
                Ok(Some(0))
            } else {
                // FIXME: mysql's double to unsigned is very special,
                //  it **seems** that if the float num bigger than i64::MAX,
                //  then return i64::MAX always.
                //  This may be the bug of mysql.
                //  So I don't change ours' behavior here.
                let val: u64 = val.convert(ctx)?;
                Ok(Some(val as i64))
            }
        }
    }
}

#[rpn_fn(nullable, capture = [ctx, extra, metadata], metadata_type = tipb::InUnionMetadata)]
#[inline]
fn cast_string_as_int(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    metadata: &tipb::InUnionMetadata,
    val: Option<BytesRef>,
) -> Result<Option<Int>> {
    match val {
        None => Ok(None),
        Some(val) => {
            // TODO: in TiDB, if `b.args[0].GetType().Hybrid()` || `IsBinaryLiteral(b.args[0])`,
            //  then it will return res from EvalInt() directly.
            let is_unsigned = extra.ret_field_type.is_unsigned();
            let val = get_valid_utf8_prefix(ctx, val)?;
            let val = val.trim();
            let is_str_neg = val.starts_with('-');
            if metadata.get_in_union() && is_unsigned && is_str_neg {
                Ok(Some(0))
            } else {
                // FIXME: if the err get_valid_int_prefix returned is overflow err,
                //  it should be ERR_TRUNCATE_WRONG_VALUE but not others.
                let valid_int_prefix = get_valid_int_prefix(ctx, val)?;
                let parse_res = if !is_str_neg {
                    valid_int_prefix.parse::<u64>().map(|x| x as i64)
                } else {
                    valid_int_prefix.parse::<i64>()
                };
                // The `OverflowAsWarning` is true just if in `SELECT` statement context, e.g:
                // 1. SELECT * FROM t  => OverflowAsWarning = true
                // 2. INSERT INTO t VALUE (...) => OverflowAsWarning = false
                // 3. INSERT INTO t SELECT * FROM t2 => OverflowAsWarning = false
                // (according to https://github.com/pingcap/tidb/blob/e173c7f5c1041b3c7e67507889d50a7bdbcdfc01/executor/executor.go#L1452)
                //
                // NOTE: if this flag(OverflowAsWarning)'s setting had changed,
                // then here's behavior should be changed to keep consistent with TiDB.
                match parse_res {
                    Ok(x) => {
                        if !is_str_neg {
                            if !is_unsigned && x as u64 > std::i64::MAX as u64 {
                                ctx.warnings
                                    .append_warning(Error::cast_as_signed_overflow())
                            }
                        } else if is_unsigned {
                            ctx.warnings
                                .append_warning(Error::cast_neg_int_as_unsigned());
                        }
                        Ok(Some(x as i64))
                    }
                    Err(err) => match *err.kind() {
                        IntErrorKind::PosOverflow | IntErrorKind::NegOverflow => {
                            let err = if is_str_neg {
                                Error::overflow("BIGINT UNSIGNED", valid_int_prefix)
                            } else {
                                Error::overflow("BIGINT", valid_int_prefix)
                            };
                            let warn_err = Error::truncated_wrong_val("INTEGER", val);
                            ctx.handle_overflow_err(warn_err).map_err(|_| err)?;
                            let val = if is_str_neg {
                                std::i64::MIN
                            } else {
                                std::u64::MAX as i64
                            };
                            Ok(Some(val))
                        }
                        _ => Err(other_err!("parse string to int failed: {}", err)),
                    },
                }
            }
        }
    }
}

#[rpn_fn(nullable, capture = [ctx])]
fn cast_binary_string_as_int(ctx: &mut EvalContext, val: Option<BytesRef>) -> Result<Option<Int>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let r = binary_literal::to_uint(ctx, val)? as i64;
            Ok(Some(r))
        }
    }
}

/// # TODO
///
/// This function is added to prove `rpn_fn` supports `enum`/`set` correctly. We will add enum/set
/// related copr functions into `get_cast_fn_rpn_meta` after Enum/Set decode implemented.
#[rpn_fn]
#[inline]
fn cast_enum_as_int(val: EnumRef) -> Result<Option<Int>> {
    Ok(Some(val.value() as Int))
}

#[rpn_fn]
#[inline]
fn cast_set_as_int(val: SetRef) -> Result<Option<Int>> {
    Ok(Some(val.value() as Int))
}

#[rpn_fn(nullable, capture = [ctx, metadata], metadata_type = tipb::InUnionMetadata)]
#[inline]
fn cast_decimal_as_uint(
    ctx: &mut EvalContext,
    metadata: &tipb::InUnionMetadata,
    val: Option<&Decimal>,
) -> Result<Option<Int>> {
    match val {
        None => Ok(None),
        Some(val) => {
            // TODO: here TiDB round before call `val.is_negative()`
            if metadata.get_in_union() && val.is_negative() {
                Ok(Some(0))
            } else {
                let r: u64 = val.convert(ctx)?;
                Ok(Some(r as i64))
            }
        }
    }
}

#[rpn_fn(nullable, capture = [ctx])]
#[inline]
fn cast_json_as_uint(ctx: &mut EvalContext, val: Option<JsonRef>) -> Result<Option<Int>> {
    match val {
        None => Ok(None),
        Some(j) => {
            let r: u64 = j.convert(ctx)?;
            Ok(Some(r as i64))
        }
    }
}

// cast any as real, some cast functions reuse `cast_any_as_any`
//
// cast_decimal_as_signed_real -> cast_any_as_any<Decimal, Real>
// cast_time_as_real -> cast_any_as_any<Time, Real>
// cast_duration_as_real -> cast_any_as_any<Duration, Real>
// cast_json_as_real -> by cast_any_as_any<Json, Real>

#[rpn_fn(nullable)]
#[inline]
fn cast_signed_int_as_signed_real(val: Option<&Int>) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Real::new(*val as f64).ok()),
    }
}

#[rpn_fn(nullable, capture = [metadata], metadata_type = tipb::InUnionMetadata)]
#[inline]
fn cast_signed_int_as_unsigned_real(
    metadata: &tipb::InUnionMetadata,
    val: Option<&Int>,
) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            if metadata.get_in_union() && *val < 0 {
                Ok(Some(Real::zero()))
            } else {
                // FIXME: negative number to unsigned real's logic may be wrong here.
                Ok(Real::new(*val as u64 as f64).ok())
            }
        }
    }
}

// because we needn't to consider if uint overflow upper boundary of signed real,
// so we can merge uint to signed/unsigned real in one function
#[rpn_fn(nullable)]
#[inline]
fn cast_unsigned_int_as_signed_or_unsigned_real(val: Option<&Int>) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Real::new(*val as u64 as f64).ok()),
    }
}

#[rpn_fn(nullable)]
#[inline]
fn cast_real_as_signed_real(val: Option<&Real>) -> Result<Option<Real>> {
    Ok(val.cloned())
}

#[rpn_fn(nullable, capture = [metadata], metadata_type = tipb::InUnionMetadata)]
#[inline]
fn cast_real_as_unsigned_real(
    metadata: &tipb::InUnionMetadata,
    val: Option<&Real>,
) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            if metadata.get_in_union() && val.into_inner() < 0f64 {
                Ok(Some(Real::zero()))
            } else {
                // FIXME: negative number to unsigned real's logic may be wrong here.
                Ok(Some(*val))
            }
        }
    }
}

#[rpn_fn(nullable, capture = [ctx, extra])]
#[inline]
fn cast_string_as_signed_real(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Option<BytesRef>,
) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let r: f64;
            if val.is_empty() {
                r = 0.0;
            } else {
                r = val.convert(ctx)?;
            }
            let r = produce_float_with_specified_tp(ctx, extra.ret_field_type, r)?;
            Ok(Real::new(r).ok())
        }
    }
}

#[rpn_fn(nullable, capture = [ctx, extra])]
#[inline]
fn cast_binary_string_as_signed_real(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Option<BytesRef>,
) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let r = binary_literal::to_uint(ctx, val)? as i64 as f64;
            let r = produce_float_with_specified_tp(ctx, extra.ret_field_type, r)?;
            Ok(Real::new(r).ok())
        }
    }
}

#[rpn_fn(nullable, capture = [ctx, extra, metadata], metadata_type = tipb::InUnionMetadata)]
#[inline]
fn cast_string_as_unsigned_real(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    metadata: &tipb::InUnionMetadata,
    val: Option<BytesRef>,
) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let mut r: f64 = val.convert(ctx)?;
            if metadata.get_in_union() && r < 0f64 {
                r = 0f64;
            }
            let r = produce_float_with_specified_tp(ctx, extra.ret_field_type, r)?;
            Ok(Real::new(r).ok())
        }
    }
}

#[rpn_fn(nullable, capture = [ctx, extra])]
#[inline]
fn cast_binary_string_as_unsigned_real(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Option<BytesRef>,
) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let r = binary_literal::to_uint(ctx, val)? as f64;
            let r = produce_float_with_specified_tp(ctx, extra.ret_field_type, r)?;
            Ok(Real::new(r).ok())
        }
    }
}

#[rpn_fn(nullable, capture = [ctx, metadata], metadata_type = tipb::InUnionMetadata)]
#[inline]
fn cast_decimal_as_unsigned_real(
    ctx: &mut EvalContext,
    metadata: &tipb::InUnionMetadata,
    val: Option<&Decimal>,
) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            if metadata.get_in_union() && val.is_negative() {
                Ok(Some(Real::zero()))
            } else {
                // FIXME: negative number to unsigned real's logic may be wrong here.
                Ok(Some(val.convert(ctx)?))
            }
        }
    }
}

// cast any as string, some cast functions reuse `cast_any_as_any`
//
// cast_int_as_string -> cast_any_as_string_fn_meta::<Int>
// cast_real_as_string -> cast_any_as_string_fn_meta::<Real>
// cast_decimal_as_string -> cast_any_as_string_fn_meta::<Decimal>
// cast_datetime_as_string -> cast_any_as_string_fn_meta::<DateTime>
// cast_duration_as_string -> cast_any_as_string_fn_meta::<Duration>
// cast_json_as_string -> by cast_any_as_any<Json, String>

#[rpn_fn(nullable, capture = [ctx, extra])]
#[inline]
fn cast_any_as_string<T: ConvertTo<Bytes> + Evaluable + EvaluableRet>(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Option<&T>,
) -> Result<Option<Bytes>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val: Bytes = val.convert(ctx)?;
            cast_as_string_helper(ctx, extra, val)
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_year_as_string(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Int,
) -> Result<Option<Bytes>> {
    let cast = if *val == 0 {
        b"0000".to_vec()
    } else {
        val.to_string().into_bytes()
    };
    cast_as_string_helper(ctx, extra, cast)
}

#[rpn_fn(nullable, capture = [ctx, extra])]
#[inline]
fn cast_uint_as_string(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Option<&Int>,
) -> Result<Option<Bytes>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = (*val as u64).to_string().into_bytes();
            cast_as_string_helper(ctx, extra, val)
        }
    }
}

#[rpn_fn(nullable, capture = [ctx, extra])]
#[inline]
fn cast_float_real_as_string(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Option<&Real>,
) -> Result<Option<Bytes>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.into_inner() as f32;
            let val = val.to_string().into_bytes();
            cast_as_string_helper(ctx, extra, val)
        }
    }
}

// FIXME: We cannot use specialization in current Rust version, so impl ConvertTo<Bytes> for Bytes cannot
//  pass compile because of we have impl Convert<Bytes> for T where T: ToString + Evaluable
//  Refactor this part after https://github.com/rust-lang/rust/issues/31844 closed
#[rpn_fn(nullable, capture = [ctx, extra])]
#[inline]
fn cast_string_as_string(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Option<BytesRef>,
) -> Result<Option<Bytes>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.to_vec();
            cast_as_string_helper(ctx, extra, val)
        }
    }
}

#[inline]
fn cast_as_string_helper(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Vec<u8>,
) -> Result<Option<Bytes>> {
    let res = produce_str_with_specified_tp(
        ctx,
        Cow::Borrowed(val.as_slice()),
        extra.ret_field_type,
        false,
    )?;
    let mut res = match res {
        Cow::Borrowed(_) => val,
        Cow::Owned(x) => x.to_vec(),
    };
    pad_zero_for_binary_type(&mut res, extra.ret_field_type);
    Ok(Some(res))
}

// cast any as decimal, some cast functions reuse `cast_any_as_decimal`
//
// - cast_signed_int_as_signed_decimal -> cast_any_as_decimal<Int>
// - cast_string_as_signed_decimal -> cast_any_as_decimal<Bytes>
// - cast_time_as_decimal -> cast_any_as_decimal<Time>
// - cast_duration_as_decimal -> cast_any_as_decimal<Duration>
// - cast_json_as_decimal -> cast_any_as_decimal<Json>

#[rpn_fn(nullable, capture = [ctx, extra])]
#[inline]
fn cast_unsigned_int_as_signed_or_unsigned_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Option<&i64>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            // because uint's upper bound is smaller than signed decimal's upper bound
            // so we can merge cast uint as signed/unsigned decimal in this function
            let dec = Decimal::from(*val as u64);
            Ok(Some(produce_dec_with_specified_tp(
                ctx,
                dec,
                extra.ret_field_type,
            )?))
        }
    }
}

#[rpn_fn(nullable, capture = [ctx, extra, metadata], metadata_type = tipb::InUnionMetadata)]
#[inline]
fn cast_signed_int_as_unsigned_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    metadata: &tipb::InUnionMetadata,
    val: Option<&i64>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let dec = if metadata.get_in_union() && *val < 0 {
                Decimal::zero()
            } else {
                Decimal::from(*val as u64)
            };
            Ok(Some(produce_dec_with_specified_tp(
                ctx,
                dec,
                extra.ret_field_type,
            )?))
        }
    }
}

#[rpn_fn(nullable, capture = [ctx, extra, metadata], metadata_type = tipb::InUnionMetadata)]
#[inline]
fn cast_real_as_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    metadata: &tipb::InUnionMetadata,
    val: Option<&Real>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.into_inner();
            let res = if metadata.get_in_union() && val < 0f64 {
                Decimal::zero()
            } else {
                Decimal::from_f64(val)?
            };
            Ok(Some(produce_dec_with_specified_tp(
                ctx,
                res,
                extra.ret_field_type,
            )?))
        }
    }
}

#[rpn_fn(nullable, capture = [ctx, extra, metadata], metadata_type = tipb::InUnionMetadata)]
#[inline]
fn cast_string_as_unsigned_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    metadata: &tipb::InUnionMetadata,
    val: Option<BytesRef>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            // FIXME: in TiDB, if the param IsBinaryLiteral, then return the result of `evalDecimal` directly
            let d: Decimal = val.convert(ctx)?;
            let d = if metadata.get_in_union() && d.is_negative() {
                Decimal::zero()
            } else {
                d
            };
            Ok(Some(produce_dec_with_specified_tp(
                ctx,
                d,
                extra.ret_field_type,
            )?))
        }
    }
}

#[rpn_fn(nullable, capture = [ctx, extra])]
#[inline]
fn cast_decimal_as_signed_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Option<&Decimal>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(produce_dec_with_specified_tp(
            ctx,
            *val,
            extra.ret_field_type,
        )?)),
    }
}

#[rpn_fn(nullable, capture = [ctx, extra, metadata], metadata_type = tipb::InUnionMetadata)]
#[inline]
fn cast_decimal_as_unsigned_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    metadata: &tipb::InUnionMetadata,
    val: Option<&Decimal>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let res = if metadata.get_in_union() && val.is_negative() {
                Decimal::zero()
            } else {
                *val
            };
            Ok(Some(produce_dec_with_specified_tp(
                ctx,
                res,
                extra.ret_field_type,
            )?))
        }
    }
}

#[rpn_fn(nullable, capture = [ctx, extra])]
#[inline]
fn cast_any_as_decimal<From: Evaluable + EvaluableRet + ConvertTo<Decimal>>(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Option<&From>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let dec: Decimal = val.convert(ctx)?;
            Ok(Some(produce_dec_with_specified_tp(
                ctx,
                dec,
                extra.ret_field_type,
            )?))
        }
    }
}

#[rpn_fn(nullable, capture = [ctx, extra])]
#[inline]
fn cast_json_as_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Option<JsonRef>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let dec: Decimal = val.convert(ctx)?;
            Ok(Some(produce_dec_with_specified_tp(
                ctx,
                dec,
                extra.ret_field_type,
            )?))
        }
    }
}

#[rpn_fn(nullable, capture = [ctx, extra])]
#[inline]
fn cast_bytes_as_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Option<BytesRef>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let dec: Decimal = val.convert(ctx)?;
            Ok(Some(produce_dec_with_specified_tp(
                ctx,
                dec,
                extra.ret_field_type,
            )?))
        }
    }
}

// cast any as duration, no cast functions reuse `cast_any_as_any`

#[rpn_fn(nullable, capture = [ctx, extra])]
#[inline]
fn cast_int_as_duration(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Option<&Int>,
) -> Result<Option<Duration>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let fsp = extra.ret_field_type.get_decimal() as i8;
            Duration::from_i64(ctx, *val, fsp).map(Some).or_else(|err| {
                if err.is_overflow() {
                    ctx.handle_overflow_err(err)?;
                    Ok(None)
                } else if err.is_truncated() {
                    ctx.handle_truncate_err(err)?;
                    Ok(None)
                } else {
                    Err(err.into())
                }
            })
        }
    }
}

#[rpn_fn(nullable, capture = [ctx, extra])]
#[inline]
fn cast_time_as_duration(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Option<&DateTime>,
) -> Result<Option<Duration>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let dur: Duration = val.convert(ctx)?;
            Ok(Some(dur.round_frac(extra.ret_field_type.decimal() as i8)?))
        }
    }
}

#[rpn_fn(nullable, capture = [extra])]
#[inline]
fn cast_duration_as_duration(
    extra: &RpnFnCallExtra,
    val: Option<&Duration>,
) -> Result<Option<Duration>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(val.round_frac(extra.ret_field_type.decimal() as i8)?)),
    }
}

// TODO: use this macro to simplify all other place
macro_rules! skip_none {
    ($val:expr) => {
        match $val {
            None => return Ok(None),
            Some(v) => v,
        }
    };
}

#[inline]
fn cast_bytes_like_as_duration(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &[u8],
) -> Result<Option<Duration>> {
    let val = std::str::from_utf8(val).map_err(Error::Encoding)?;
    let result = Duration::parse(ctx, val, extra.ret_field_type.get_decimal() as i8);
    match result {
        Ok(dur) => Ok(Some(dur)),
        Err(e) => match e.code() {
            ERR_DATA_OUT_OF_RANGE => {
                ctx.handle_overflow_err(e)?;
                Ok(None)
            }
            ERR_TRUNCATE_WRONG_VALUE => {
                ctx.handle_truncate_err(e)?;
                Ok(None)
            }
            _ => Err(e.into()),
        },
    }
}

#[rpn_fn(nullable, capture = [ctx, extra])]
#[inline]
pub fn cast_real_as_duration(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Option<&Real>,
) -> Result<Option<Duration>> {
    let v = skip_none!(val).into_inner().to_string();
    cast_bytes_like_as_duration(ctx, extra, v.as_bytes())
}

#[rpn_fn(nullable, capture = [ctx, extra])]
#[inline]
pub fn cast_bytes_as_duration(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Option<BytesRef>,
) -> Result<Option<Duration>> {
    let v = skip_none!(val);
    cast_bytes_like_as_duration(ctx, extra, v)
}

#[rpn_fn(nullable, capture = [ctx, extra])]
#[inline]
pub fn cast_decimal_as_duration(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Option<&Decimal>,
) -> Result<Option<Duration>> {
    let v = skip_none!(val).to_string();
    cast_bytes_like_as_duration(ctx, extra, v.as_bytes())
}

#[rpn_fn(nullable, capture = [ctx, extra])]
#[inline]
pub fn cast_json_as_duration(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Option<JsonRef>,
) -> Result<Option<Duration>> {
    let v = skip_none!(val).unquote()?;
    cast_bytes_like_as_duration(ctx, extra, v.as_bytes())
}

#[rpn_fn(nullable, capture = [ctx, extra])]
fn cast_int_as_time(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Option<&Int>,
) -> Result<Option<Time>> {
    if let Some(val) = val {
        // Parse `val` as a `u64`
        Time::parse_from_i64(
            ctx,
            *val,
            extra.ret_field_type.as_accessor().tp().try_into()?,
            extra.ret_field_type.get_decimal() as i8,
        )
        .map(Some)
        .or_else(|_| {
            Ok(ctx
                .handle_invalid_time_error(Error::incorrect_datetime_value(val))
                .map(|_| None)?)
        })
    } else {
        Ok(None)
    }
}

#[rpn_fn(capture = [ctx, extra])]
fn cast_year_as_time(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    year: &Int,
) -> Result<Option<Time>> {
    let year = *year;
    if year != 0 && (year < MIN_YEAR.into() || year > MAX_YEAR.into()) {
        ctx.handle_truncate_err(Error::truncated_wrong_val("YEAR", year))?;
        return Ok(None);
    }
    let time_type = FieldTypeAccessor::tp(extra.ret_field_type).try_into()?;
    let fsp = extra.ret_field_type.decimal() as i8;
    let time = Time::from_year(ctx, year as u32, fsp, time_type)?;

    Ok(Some(time))
}

// NOTE: in MySQL, casting `Real` to `Time` should cast `Real` to `Int` first,
// However, TiDB cast `Real` to `String` and then parse it into a `Time`
#[rpn_fn(nullable, capture = [ctx, extra])]
fn cast_real_as_time(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Option<&Real>,
) -> Result<Option<Time>> {
    if let Some(val) = val {
        if val.is_zero() {
            Time::zero(
                ctx,
                extra.ret_field_type.get_decimal() as i8,
                extra.ret_field_type.as_accessor().tp().try_into()?,
            )
        } else {
            // Convert `val` to a string first and then parse it as a float string.
            Time::parse(
                ctx,
                &val.to_string(),
                extra.ret_field_type.as_accessor().tp().try_into()?,
                extra.ret_field_type.get_decimal() as i8,
                // Enable round
                true,
            )
        }
        .map(Some)
        .or_else(|e| Ok(ctx.handle_invalid_time_error(e).map(|_| None)?))
    } else {
        Ok(None)
    }
}

#[rpn_fn(nullable, capture = [ctx, extra])]
fn cast_string_as_time(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Option<BytesRef>,
) -> Result<Option<Time>> {
    if let Some(val) = val {
        // Convert `val` to a string first and then parse it as a float string.
        Time::parse(
            ctx,
            unsafe { std::str::from_utf8_unchecked(val) },
            extra.ret_field_type.as_accessor().tp().try_into()?,
            extra.ret_field_type.get_decimal() as i8,
            // Enable round
            true,
        )
        .map(Some)
        .or_else(|e| Ok(ctx.handle_invalid_time_error(e).map(|_| None)?))
    } else {
        Ok(None)
    }
}

#[rpn_fn(nullable, capture = [ctx, extra])]
fn cast_decimal_as_time(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Option<&Decimal>,
) -> Result<Option<Time>> {
    if let Some(val) = val {
        // Convert `val` to a string first and then parse it as a string.
        Time::parse_from_decimal(
            ctx,
            val,
            extra.ret_field_type.as_accessor().tp().try_into()?,
            extra.ret_field_type.get_decimal() as i8,
            // Enable round
            true,
        )
        .map(Some)
        .or_else(|e| Ok(ctx.handle_invalid_time_error(e).map(|_| None)?))
    } else {
        Ok(None)
    }
}

#[rpn_fn(nullable, capture = [ctx, extra])]
fn cast_time_as_time(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Option<&Time>,
) -> Result<Option<Time>> {
    if let Some(val) = val {
        let mut val = *val;
        val.set_time_type(extra.ret_field_type.as_accessor().tp().try_into()?)?;
        val.round_frac(ctx, extra.ret_field_type.get_decimal() as i8)
            .map(Some)
            .or_else(|e| Ok(ctx.handle_invalid_time_error(e).map(|_| None)?))
    } else {
        Ok(None)
    }
}

#[rpn_fn(nullable, capture = [ctx, extra])]
fn cast_duration_as_time(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Option<&Duration>,
) -> Result<Option<Time>> {
    if let Some(val) = val {
        Time::from_duration(
            ctx,
            *val,
            extra.ret_field_type.as_accessor().tp().try_into()?,
        )
        .and_then(|now| now.round_frac(ctx, extra.ret_field_type.get_decimal() as i8))
        .map(Some)
        .or_else(|e| Ok(ctx.handle_invalid_time_error(e).map(|_| None)?))
    } else {
        Ok(None)
    }
}

// cast any as json, some cast functions reuse `cast_any_as_any`
//
// - cast_int_as_json -> cast_any_as_any<Int, Json>
// - cast_real_as_json -> cast_any_as_any<Real, Json>
// - cast_decimal_as_json -> cast_any_as_any<Decimal, Json>
// - cast_time_as_json -> cast_any_as_any<Time, Json>
// - cast_duration_as_json -> cast_any_as_any<Duration, Json>

#[rpn_fn(nullable)]
#[inline]
fn cast_bool_as_json(val: Option<&Int>) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(Json::from_bool(*val != 0)?)),
    }
}

#[rpn_fn(nullable)]
#[inline]
fn cast_uint_as_json(val: Option<&Int>) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(Json::from_u64(*val as u64)?)),
    }
}

#[rpn_fn(nullable, capture = [extra])]
#[inline]
fn cast_string_as_json(extra: &RpnFnCallExtra<'_>, val: Option<BytesRef>) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => {
            if extra
                .ret_field_type
                .as_accessor()
                .flag()
                .contains(FieldTypeFlag::PARSE_TO_JSON)
            {
                // if failed, is it because of bug?
                let s: String = box_try!(String::from_utf8(val.to_owned()));
                let val: Json = s.parse()?;
                Ok(Some(val))
            } else {
                // FIXME: port `JSONBinary` from TiDB to adapt if the bytes is not a valid utf8 string
                let val = unsafe { String::from_utf8_unchecked(val.to_owned()) };
                Ok(Some(Json::from_string(val)?))
            }
        }
    }
}

#[rpn_fn(nullable)]
#[inline]
fn cast_json_as_json(val: Option<JsonRef>) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(val.to_owned())),
    }
}

#[rpn_fn(nullable, capture = [ctx])]
#[inline]
fn cast_any_as_any<From: ConvertTo<To> + Evaluable + EvaluableRet, To: Evaluable + EvaluableRet>(
    ctx: &mut EvalContext,
    val: Option<&From>,
) -> Result<Option<To>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.convert(ctx)?;
            Ok(Some(val))
        }
    }
}

#[rpn_fn(nullable, capture = [ctx])]
#[inline]
fn cast_json_as_any<To: Evaluable + EvaluableRet + ConvertFrom<Json>>(
    ctx: &mut EvalContext,
    val: Option<JsonRef>,
) -> Result<Option<To>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = To::convert_from(ctx, val.to_owned())?;
            Ok(Some(val))
        }
    }
}

#[rpn_fn(nullable, capture = [ctx])]
#[inline]
fn cast_any_as_json<From: ConvertTo<Json> + Evaluable + EvaluableRet>(
    ctx: &mut EvalContext,
    val: Option<&From>,
) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.convert(ctx)?;
            Ok(Some(val))
        }
    }
}

#[rpn_fn(nullable, capture = [ctx])]
#[inline]
fn cast_any_as_bytes<From: ConvertTo<Bytes> + Evaluable + EvaluableRet>(
    ctx: &mut EvalContext,
    val: Option<&From>,
) -> Result<Option<Bytes>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.convert(ctx)?;
            Ok(Some(val))
        }
    }
}

#[rpn_fn(nullable, capture = [ctx])]
#[inline]
fn cast_json_as_bytes(ctx: &mut EvalContext, val: Option<JsonRef>) -> Result<Option<Bytes>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.convert(ctx)?;
            Ok(Some(val))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Result;
    use crate::impl_cast::*;
    use crate::types::test_util::RpnFnScalarEvaluator;
    use crate::RpnFnCallExtra;
    use std::collections::BTreeMap;
    use std::fmt::{Debug, Display};
    use std::sync::Arc;
    use std::{f32, f64, i64, u64};
    use tidb_query_datatype::builder::FieldTypeBuilder;
    use tidb_query_datatype::codec::convert::produce_dec_with_specified_tp;
    use tidb_query_datatype::codec::data_type::{Bytes, Int, Real};
    use tidb_query_datatype::codec::error::{
        ERR_DATA_OUT_OF_RANGE, ERR_DATA_TOO_LONG, ERR_TRUNCATE_WRONG_VALUE, ERR_UNKNOWN,
        WARN_DATA_TRUNCATED,
    };
    use tidb_query_datatype::codec::mysql::charset::*;
    use tidb_query_datatype::codec::mysql::decimal::{max_decimal, max_or_min_dec};
    use tidb_query_datatype::codec::mysql::{
        Decimal, Duration, Json, RoundMode, Time, TimeType, MAX_FSP, MIN_FSP,
    };
    use tidb_query_datatype::expr::Flag;
    use tidb_query_datatype::expr::{EvalConfig, EvalContext};
    use tidb_query_datatype::{Collation, FieldTypeFlag, FieldTypeTp, UNSPECIFIED_LENGTH};
    use tikv_util::buffer_vec::BufferVec;
    use tipb::ScalarFuncSig;

    fn test_none_with_ctx_and_extra<F, Input, Ret>(func: F)
    where
        F: Fn(&mut EvalContext, &RpnFnCallExtra, Option<Input>) -> Result<Option<Ret>>,
    {
        let mut ctx = EvalContext::default();
        let ret_field_type: FieldType = FieldType::default();
        let extra = RpnFnCallExtra {
            ret_field_type: &ret_field_type,
        };
        let r = func(&mut ctx, &extra, None).unwrap();
        assert!(r.is_none());
    }

    fn test_none_with_ctx<F, Input, Ret>(func: F)
    where
        F: Fn(&mut EvalContext, Option<Input>) -> Result<Option<Ret>>,
    {
        let mut ctx = EvalContext::default();
        let r = func(&mut ctx, None).unwrap();
        assert!(r.is_none());
    }

    fn test_none_with_extra<F, Input, Ret>(func: F)
    where
        F: Fn(&RpnFnCallExtra, Option<Input>) -> Result<Option<Ret>>,
    {
        let ret_field_type: FieldType = FieldType::default();
        let extra = RpnFnCallExtra {
            ret_field_type: &ret_field_type,
        };
        let r = func(&extra, None).unwrap();
        assert!(r.is_none());
    }

    fn test_none_with_metadata<F, Input, Ret>(func: F)
    where
        F: Fn(&tipb::InUnionMetadata, Option<Input>) -> Result<Option<Ret>>,
    {
        let metadata = make_metadata(true);
        let r = func(&metadata, None).unwrap();
        assert!(r.is_none());
    }

    fn test_none_with_ctx_and_metadata<F, Input, Ret>(func: F)
    where
        F: Fn(&mut EvalContext, &tipb::InUnionMetadata, Option<Input>) -> Result<Option<Ret>>,
    {
        let mut ctx = EvalContext::default();
        let metadata = make_metadata(true);
        let r = func(&mut ctx, &metadata, None).unwrap();
        assert!(r.is_none());
    }

    fn test_none_with_ctx_and_extra_and_metadata<F, Input, Ret>(func: F)
    where
        F: Fn(
            &mut EvalContext,
            &RpnFnCallExtra,
            &tipb::InUnionMetadata,
            Option<Input>,
        ) -> Result<Option<Ret>>,
    {
        let mut ctx = EvalContext::default();
        let ret_field_type: FieldType = FieldType::default();
        let extra = RpnFnCallExtra {
            ret_field_type: &ret_field_type,
        };
        let metadata = make_metadata(true);
        let r = func(&mut ctx, &extra, &metadata, None).unwrap();
        assert!(r.is_none());
    }

    fn test_none_with_nothing<F, Input, Ret>(func: F)
    where
        F: Fn(Option<Input>) -> Result<Option<Ret>>,
    {
        let r = func(None).unwrap();
        assert!(r.is_none());
    }

    struct CtxConfig {
        overflow_as_warning: bool,
        truncate_as_warning: bool,
        should_clip_to_zero: bool,
        in_insert_stmt: bool,
        in_update_or_delete_stmt: bool,
    }

    impl Default for CtxConfig {
        fn default() -> Self {
            CtxConfig {
                overflow_as_warning: false,
                truncate_as_warning: false,
                should_clip_to_zero: false,
                in_insert_stmt: false,
                in_update_or_delete_stmt: false,
            }
        }
    }

    impl From<CtxConfig> for EvalContext {
        fn from(config: CtxConfig) -> Self {
            let mut flag: Flag = Flag::empty();
            if config.overflow_as_warning {
                flag |= Flag::OVERFLOW_AS_WARNING;
            }
            if config.truncate_as_warning {
                flag |= Flag::TRUNCATE_AS_WARNING;
            }
            if config.should_clip_to_zero {
                flag |= Flag::IN_INSERT_STMT;
            }
            if config.in_insert_stmt {
                flag |= Flag::IN_INSERT_STMT;
            }
            if config.in_update_or_delete_stmt {
                flag |= Flag::IN_UPDATE_OR_DELETE_STMT;
            }
            let cfg = Arc::new(EvalConfig::from_flag(flag));
            EvalContext::new(cfg)
        }
    }

    fn make_metadata(in_union: bool) -> tipb::InUnionMetadata {
        let mut metadata = tipb::InUnionMetadata::default();
        metadata.set_in_union(in_union);
        metadata
    }

    struct FieldTypeConfig {
        unsigned: bool,
        flen: isize,
        decimal: isize,
        charset: Option<&'static str>,
        tp: Option<FieldTypeTp>,
        collation: Option<Collation>,
    }

    impl Default for FieldTypeConfig {
        fn default() -> Self {
            FieldTypeConfig {
                unsigned: false,
                flen: UNSPECIFIED_LENGTH,
                decimal: UNSPECIFIED_LENGTH,
                charset: None,
                tp: None,
                collation: None,
            }
        }
    }

    impl From<FieldTypeConfig> for FieldType {
        fn from(config: FieldTypeConfig) -> Self {
            let mut ft = FieldType::default();
            if let Some(c) = config.charset {
                ft.set_charset(String::from(c));
            }
            let fta = ft.as_mut_accessor();
            if config.unsigned {
                fta.set_flag(FieldTypeFlag::UNSIGNED);
            }
            fta.set_flen(config.flen);
            fta.set_decimal(config.decimal);
            if let Some(tp) = config.tp {
                fta.set_tp(tp);
            }
            if let Some(c) = config.collation {
                fta.set_collation(c);
            }
            ft
        }
    }

    fn make_extra(ret_field_type: &FieldType) -> RpnFnCallExtra {
        RpnFnCallExtra { ret_field_type }
    }

    fn make_log<P: Display, R: Display + Debug>(
        input: &P,
        expect: &R,
        result: &Result<Option<R>>,
    ) -> String {
        format!(
            "input: {}, expect: {:?}, output: {:?}",
            input, expect, result
        )
    }

    fn check_overflow(ctx: &EvalContext, overflow: bool, log: &str) {
        if overflow {
            check_warning(ctx, Some(ERR_DATA_OUT_OF_RANGE), log)
        }
    }

    fn check_truncate(ctx: &EvalContext, truncate: bool, log: &str) {
        if truncate {
            check_warning(ctx, Some(ERR_TRUNCATE_WRONG_VALUE), log)
        }
    }

    fn check_warning(ctx: &EvalContext, err_code: Option<i32>, log: &str) {
        if let Some(x) = err_code {
            assert_eq!(
                ctx.warnings.warning_cnt, 1,
                "log: {}, warnings: {:?}",
                log, ctx.warnings.warnings
            );
            assert_eq!(ctx.warnings.warnings[0].get_code(), x, "{}", log);
        }
    }

    fn check_result<R: Debug + PartialEq>(expect: Option<&R>, res: &Result<Option<R>>, log: &str) {
        assert!(res.is_ok(), "{}", log);
        let res = res.as_ref().unwrap();
        if res.is_none() {
            assert!(expect.is_none(), "{}", log);
        } else {
            let res = res.as_ref().unwrap();
            assert_eq!(res, expect.unwrap(), "{}", log);
        }
    }

    // comment for all test below:
    // if there should not be any overflow/truncate,
    // then should not set ctx with overflow_as_warning/truncated_as_warning flag,
    // and then if there is unexpected overflow/truncate,
    // then we will find them in `unwrap`
    #[test]
    fn test_int_as_int_others() {
        test_none_with_nothing(cast_int_as_int_others);
        let cs = vec![
            (i64::MAX, i64::MAX),
            (i64::MIN, i64::MIN),
            (u64::MAX as i64, u64::MAX as i64),
        ];
        for (input, expect) in cs {
            let r = cast_int_as_int_others(Some(&input));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_signed_int_as_unsigned_int() {
        test_none_with_metadata(cast_signed_int_as_unsigned_int);

        let cs = vec![
            // (origin, result, in_union)
            // in union
            (-10, 0u64, true),
            (10, 10u64, true),
            (i64::MIN, 0u64, true),
            (i64::MAX, i64::MAX as u64, true),
            // not in union
            (-10, (-10i64) as u64, false),
            (10, 10u64, false),
            (i64::MIN, i64::MIN as u64, false),
            (i64::MAX, i64::MAX as u64, false),
        ];
        for (input, expect, in_union) in cs {
            let metadata = make_metadata(in_union);
            let r = cast_signed_int_as_unsigned_int(&metadata, Some(&input));
            let r = r.map(|x| x.map(|x| x as u64));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_real_as_int() {
        test_none_with_ctx(cast_any_as_any::<Real, Int>);

        let cs = vec![
            // (origin, result, overflow)
            (-10.4, -10i64, false),
            (-10.5, -11, false),
            (10.4, 10, false),
            (10.5, 11, false),
            (i64::MAX as f64, i64::MAX, false),
            ((1u64 << 63) as f64, i64::MAX, false),
            (i64::MIN as f64, i64::MIN, false),
            ((1u64 << 63) as f64 + (1u64 << 62) as f64, i64::MAX, true),
            ((i64::MIN as f64) * 2f64, i64::MIN, true),
        ];

        for (input, result, overflow) in cs {
            let mut ctx = CtxConfig {
                overflow_as_warning: true,
                ..CtxConfig::default()
            }
            .into();
            let r = cast_any_as_any::<Real, Int>(&mut ctx, Real::new(input).as_ref().ok());
            let log = make_log(&input, &result, &r);
            check_result(Some(&result), &r, log.as_str());
            check_overflow(&ctx, overflow, log.as_str());
        }
    }

    #[test]
    fn test_enum_as_int() {
        // TODO: we need to test None case here.

        let mut buf = BufferVec::new();
        buf.push("我好强啊");
        buf.push("我太强啦");

        let cs = vec![
            // (input, expect)
            (EnumRef::new(&buf, 0), 0),
            (EnumRef::new(&buf, 1), 1),
        ];

        for (input, expect) in cs {
            let r = cast_enum_as_int(input);
            let r = r.map(|x| x.map(|x| x as u64));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_set_as_int() {
        // TODO: we need to test None case here.

        let mut buf = BufferVec::new();
        buf.push("我好强啊");
        buf.push("我太强啦");

        let cs = vec![
            // (input, expect)
            (SetRef::new(&buf, 0b01), 1),
            (SetRef::new(&buf, 0b11), 3),
        ];

        for (input, expect) in cs {
            let r = cast_set_as_int(input);
            let r = r.map(|x| x.map(|x| x as u64));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_real_as_uint() {
        test_none_with_ctx_and_metadata(cast_real_as_uint);

        // in_union
        let cs = vec![
            // (input, expect)
            (-10.0, 0u64),
            (i64::MIN as f64, 0),
            (10.0, 10u64),
            (i64::MAX as f64, (1u64 << 63)),
        ];

        for (input, expect) in cs {
            let mut ctx = EvalContext::default();
            let metadata = make_metadata(true);
            let r = cast_real_as_uint(
                &mut ctx,
                &metadata,
                Some(Real::new(input).as_ref().unwrap()),
            );
            let r = r.map(|x| x.map(|x| x as u64));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }

        // no clip to zero
        let cs = vec![
            // (origin, expect, overflow)
            (10.5, 11u64, false),
            (10.4, 10u64, false),
            (
                ((1u64 << 63) + (1u64 << 62)) as f64,
                ((1u64 << 63) + (1u64 << 62)),
                false,
            ),
            (u64::MAX as f64, u64::MAX, false),
            ((u64::MAX as f64) * 2f64, u64::MAX, true),
            (-1f64, -1f64 as i64 as u64, true),
        ];

        for (input, expect, overflow) in cs {
            let mut ctx = CtxConfig {
                overflow_as_warning: true,
                ..CtxConfig::default()
            }
            .into();
            let metadata = make_metadata(false);
            let r = cast_real_as_uint(&mut ctx, &metadata, Real::new(input).as_ref().ok());
            let r = r.map(|x| x.map(|x| x as u64));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
            check_overflow(&ctx, overflow, log.as_str())
        }

        // should clip to zero
        let cs: Vec<(f64, u64, bool)> = vec![
            // (origin, expect, overflow)
            (-1f64, 0, true),
            (i64::MIN as f64, 0, true),
        ];

        for (input, expect, overflow) in cs {
            let mut ctx = CtxConfig {
                overflow_as_warning: true,
                should_clip_to_zero: true,
                ..CtxConfig::default()
            }
            .into();
            let metadata = make_metadata(false);
            let r = cast_real_as_uint(
                &mut ctx,
                &metadata,
                Some(Real::new(input).as_ref().unwrap()),
            );
            let r = r.map(|x| x.map(|x| x as u64));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
            check_overflow(&ctx, overflow, log.as_str())
        }
    }

    #[test]
    fn test_cast_string_as_int() {
        // None
        {
            let output: Option<Int> = RpnFnScalarEvaluator::new()
                .push_param(ScalarValue::Bytes(None))
                .evaluate(ScalarFuncSig::CastStringAsInt)
                .unwrap();
            assert_eq!(output, None);
        }

        #[derive(Debug)]
        enum Cond {
            None,
            Unsigned,
            InUnionAndUnsigned,
        }
        impl Cond {
            fn in_union(&self) -> bool {
                matches!(self, Cond::InUnionAndUnsigned)
            }

            fn is_unsigned(&self) -> bool {
                matches!(self, Cond::InUnionAndUnsigned | Cond::Unsigned)
            }
        }

        let cs: Vec<(&str, i64, Vec<i32>, Cond)> = vec![
            // (origin, expect, err_code, condition)

            // has no prefix `-`
            (
                " 9223372036854775807  ",
                9223372036854775807i64,
                vec![],
                Cond::None,
            ),
            (
                "9223372036854775807",
                9223372036854775807i64,
                vec![],
                Cond::None,
            ),
            (
                "9223372036854775808",
                9223372036854775808u64 as i64,
                vec![ERR_UNKNOWN],
                Cond::None,
            ),
            (
                "9223372036854775808",
                9223372036854775808u64 as i64,
                vec![],
                Cond::Unsigned,
            ),
            (
                " 9223372036854775807abc  ",
                9223372036854775807i64,
                vec![ERR_TRUNCATE_WRONG_VALUE],
                Cond::None,
            ),
            (
                "9223372036854775807abc",
                9223372036854775807i64,
                vec![ERR_TRUNCATE_WRONG_VALUE],
                Cond::None,
            ),
            (
                "9223372036854775808abc",
                9223372036854775808u64 as i64,
                vec![ERR_TRUNCATE_WRONG_VALUE, ERR_UNKNOWN],
                Cond::None,
            ),
            (
                "9223372036854775808abc",
                9223372036854775808u64 as i64,
                vec![ERR_TRUNCATE_WRONG_VALUE],
                Cond::Unsigned,
            ),
            // TODO: there are some cases that has not be covered.

            // FIXME: in mysql, this case will return 18446744073709551615
            //  and `show warnings` will show
            //  `| Warning | 1292 | Truncated incorrect INTEGER value: '18446744073709551616'`
            //  fix this cast_string_as_int after fix TiDB's
            // ("18446744073709551616", 18446744073709551615 as i64, Some(ERR_TRUNCATE_WRONG_VALUE) , Cond::Unsigned)
            // FIXME: our cast_string_as_int's err handle is not exactly same as TiDB's
            // ("18446744073709551616", 18446744073709551615u64 as i64, Some(ERR_TRUNCATE_WRONG_VALUE), Cond::InSelectStmt),

            // has prefix `-` and in_union and unsigned
            ("-10", 0, vec![], Cond::InUnionAndUnsigned),
            ("-9223372036854775808", 0, vec![], Cond::InUnionAndUnsigned),
            // has prefix `-` and not in_union or not unsigned
            ("-10", -10i64, vec![], Cond::None),
            (
                "-9223372036854775808",
                -9223372036854775808i64,
                vec![],
                Cond::None,
            ),
            // FIXME: our cast_string_as_int's err handle is not exactly same as TiDB's
            (
                "-9223372036854775809",
                -9223372036854775808i64,
                vec![ERR_TRUNCATE_WRONG_VALUE],
                Cond::None,
            ),
            ("-10", -10i64, vec![ERR_UNKNOWN], Cond::Unsigned),
            (
                "-9223372036854775808",
                -9223372036854775808i64,
                vec![ERR_UNKNOWN],
                Cond::Unsigned,
            ),
            (
                "-9223372036854775809",
                -9223372036854775808i64,
                vec![ERR_TRUNCATE_WRONG_VALUE],
                Cond::Unsigned,
            ),
        ];

        for (input, expected, mut err_code, cond) in cs {
            let (result, ctx) = RpnFnScalarEvaluator::new()
                .context(CtxConfig {
                    overflow_as_warning: true,
                    truncate_as_warning: true,
                    ..CtxConfig::default()
                })
                .metadata(Box::new(make_metadata(cond.in_union())))
                .push_param(ScalarValue::Bytes(Some(input.as_bytes().to_owned())))
                .evaluate_raw(
                    FieldTypeConfig {
                        tp: Some(FieldTypeTp::LongLong),
                        unsigned: cond.is_unsigned(),
                        ..FieldTypeConfig::default()
                    },
                    ScalarFuncSig::CastStringAsInt,
                );
            let output: Option<Int> = result.unwrap().into();
            assert_eq!(
                output.unwrap(),
                expected,
                "input:{:?}, expected:{:?}, cond:{:?}",
                input,
                expected,
                cond,
            );
            let mut got_warnings = ctx
                .warnings
                .warnings
                .iter()
                .map(|w| w.get_code())
                .collect::<Vec<i32>>();
            got_warnings.sort_unstable();
            err_code.sort_unstable();
            assert_eq!(
                ctx.warnings.warning_cnt,
                err_code.len(),
                "input:{:?}, expected:{:?}, warnings:{:?}",
                input,
                expected,
                got_warnings,
            );
            assert_eq!(got_warnings, err_code);
        }

        // binary literal
        let cases = vec![
            (vec![0x01, 0x02, 0x03], Some(0x010203_i64)),
            (vec![0x01, 0x02, 0x03, 0x4], Some(0x01020304_i64)),
            (
                vec![0x01, 0x02, 0x03, 0x4, 0x05, 0x06, 0x06, 0x06, 0x06],
                None,
            ),
        ];
        for (input, expected) in cases {
            let output: Result<Option<Int>> = RpnFnScalarEvaluator::new()
                .return_field_type(FieldTypeConfig {
                    tp: Some(FieldTypeTp::LongLong),
                    ..FieldTypeConfig::default()
                })
                .push_param_with_field_type(
                    input.clone(),
                    FieldTypeConfig {
                        tp: Some(FieldTypeTp::VarString),
                        collation: Some(Collation::Binary),
                        ..FieldTypeConfig::default()
                    },
                )
                .evaluate(ScalarFuncSig::CastStringAsInt);

            if let Some(exp) = expected {
                assert!(output.is_ok(), "input: {:?}", input);
                assert_eq!(output.unwrap().unwrap(), exp, "input={:?}", input);
            } else {
                assert!(output.is_err());
            }
        }
    }

    #[test]
    fn test_decimal_as_int() {
        test_none_with_ctx(cast_any_as_any::<Decimal, Int>);

        let cs: Vec<(Decimal, i64, Option<i32>)> = vec![
            // (origin, expect, overflow)
            (
                Decimal::from_bytes(b"9223372036854775807")
                    .unwrap()
                    .unwrap(),
                9223372036854775807,
                None,
            ),
            (
                Decimal::from_bytes(b"-9223372036854775808")
                    .unwrap()
                    .unwrap(),
                -9223372036854775808,
                None,
            ),
            (
                Decimal::from_bytes(b"9223372036854775808")
                    .unwrap()
                    .unwrap(),
                9223372036854775807,
                Some(ERR_TRUNCATE_WRONG_VALUE),
            ),
            (
                Decimal::from_bytes(b"-9223372036854775809")
                    .unwrap()
                    .unwrap(),
                -9223372036854775808,
                Some(ERR_TRUNCATE_WRONG_VALUE),
            ),
        ];

        for (input, expect, err_code) in cs {
            let mut ctx = CtxConfig {
                overflow_as_warning: true,
                ..CtxConfig::default()
            }
            .into();
            let r = cast_any_as_any::<Decimal, Int>(&mut ctx, Some(&input));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
            check_warning(&ctx, err_code, log.as_str());
        }
    }

    #[test]
    fn test_decimal_as_uint() {
        test_none_with_ctx_and_metadata(cast_decimal_as_uint);
        // in_union
        let cs: Vec<(Decimal, u64)> = vec![
            (
                Decimal::from_bytes(b"-9223372036854775808")
                    .unwrap()
                    .unwrap(),
                0,
            ),
            (
                Decimal::from_bytes(b"-9223372036854775809")
                    .unwrap()
                    .unwrap(),
                0,
            ),
            (
                Decimal::from_bytes(b"9223372036854775808")
                    .unwrap()
                    .unwrap(),
                9223372036854775808,
            ),
            (
                Decimal::from_bytes(b"18446744073709551615")
                    .unwrap()
                    .unwrap(),
                18446744073709551615,
            ),
        ];

        for (input, expect) in cs {
            let mut ctx = EvalContext::default();
            let metadata = make_metadata(true);

            let r = cast_decimal_as_uint(&mut ctx, &metadata, Some(&input));
            let r = r.map(|x| x.map(|x| x as u64));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }

        let cs: Vec<(Decimal, u64, Option<i32>)> = vec![
            // (input, expect, err_code)
            (Decimal::from_bytes(b"10").unwrap().unwrap(), 10, None),
            (
                Decimal::from_bytes(b"1844674407370955161")
                    .unwrap()
                    .unwrap(),
                1844674407370955161,
                None,
            ),
            (
                Decimal::from_bytes(b"-10").unwrap().unwrap(),
                0,
                Some(ERR_TRUNCATE_WRONG_VALUE),
            ),
            (
                Decimal::from_bytes(b"18446744073709551616")
                    .unwrap()
                    .unwrap(),
                u64::MAX,
                Some(ERR_TRUNCATE_WRONG_VALUE),
            ),
        ];

        for (input, expect, err_code) in cs {
            let mut ctx = CtxConfig {
                overflow_as_warning: true,
                ..CtxConfig::default()
            }
            .into();
            let metadata = make_metadata(false);

            let r = cast_decimal_as_uint(&mut ctx, &metadata, Some(&input));
            let r = r.map(|x| x.map(|x| x as u64));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
            check_warning(&ctx, err_code, log.as_str());
        }
    }

    #[test]
    fn test_time_as_int_and_uint() {
        let mut ctx = EvalContext::default();
        // TODO: add more test case
        // TODO: add test that make cast_any_as_any::<Time, Int> returning truncated error
        let cs: Vec<(Time, i64)> = vec![
            (
                Time::parse_datetime(&mut ctx, "2000-01-01T12:13:14", 0, true).unwrap(),
                20000101121314,
            ),
            (
                Time::parse_datetime(&mut ctx, "2000-01-01T12:13:14.6666", 0, true).unwrap(),
                20000101121315,
            ),
            // FiXME
            //  Time::parse_utc_datetime("2000-01-01T12:13:14.6666", 4).unwrap().round_frac(DEFAULT_FSP)
            //  will get 2000-01-01T12:13:14, this is a bug
            // (
            //     Time::parse_utc_datetime("2000-01-01T12:13:14.6666", 4).unwrap(),
            //     20000101121315,
            // ),
        ];

        for (input, expect) in cs {
            let r = cast_any_as_any::<Time, Int>(&mut ctx, Some(&input));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_cast_int_as_time() {
        let should_pass = vec![
            ("0000-00-00 00:00:00", 0),
            ("2000-01-01 00:00:00", 101),
            ("2045-00-00 00:00:00", 450_000),
            ("2059-12-31 00:00:00", 591_231),
            ("1970-01-01 00:00:00", 700_101),
            ("1999-12-31 00:00:00", 991_231),
            ("1000-01-00 00:00:00", 10_000_100),
            ("2000-01-01 00:00:00", 101_000_000),
            ("2069-12-31 23:59:59", 691_231_235_959),
            ("1970-01-01 00:00:00", 700_101_000_000),
            ("1999-12-31 23:59:59", 991_231_235_959),
            ("0100-00-00 00:00:00", 1_000_000_000_000),
            ("1000-01-01 00:00:00", 10_000_101_000_000),
            ("1999-01-01 00:00:00", 19_990_101_000_000),
        ];

        for (expected, input) in should_pass {
            let actual: Time = RpnFnScalarEvaluator::new()
                .push_param(input)
                .return_field_type(FieldTypeBuilder::new().tp(FieldTypeTp::DateTime).build())
                .evaluate(ScalarFuncSig::CastIntAsTime)
                // `Result<Option<_>>`
                .unwrap()
                .unwrap();
            assert_eq!(actual.to_string(), expected);
        }

        let should_fail = vec![
            -11111,
            1,
            100,
            700_100,
            100_000_000,
            100_000_101_000_000,
            73,
        ];

        for case in should_fail {
            let actual = RpnFnScalarEvaluator::new()
                .push_param(case)
                .return_field_type(FieldTypeBuilder::new().tp(FieldTypeTp::Date).build())
                .evaluate::<Time>(ScalarFuncSig::CastIntAsTime)
                .unwrap();
            assert!(actual.is_none());
        }
    }

    #[test]
    fn test_cast_year_as_time() {
        let normal_cases = vec![
            ("2020-00-00 00:00:00", 2020),
            ("2000-00-00 00:00:00", 2000),
            ("1999-00-00 00:00:00", 1999),
            ("2077-00-00 00:00:00", 2077),
            ("1901-00-00 00:00:00", 1901),
            ("2155-00-00 00:00:00", 2155),
            ("0000-00-00 00:00:00", 0),
        ];

        for (expected, input) in normal_cases {
            let actual = RpnFnScalarEvaluator::new()
                .push_param_with_field_type(input, FieldTypeTp::Year)
                .return_field_type(FieldTypeBuilder::new().tp(FieldTypeTp::DateTime).build())
                .evaluate::<Time>(ScalarFuncSig::CastIntAsTime)
                .unwrap()
                .unwrap();
            assert_eq!(actual.to_string(), expected);
        }

        let null_cases = vec![
            None,
            Some(10086),
            Some(1900),
            Some(2156),
            Some(i64::MAX),
            Some(i64::MIN),
        ];

        for input in null_cases {
            let actual = RpnFnScalarEvaluator::new()
                .push_param_with_field_type(input, FieldTypeTp::Year)
                .return_field_type(FieldTypeBuilder::new().tp(FieldTypeTp::DateTime).build())
                .context(EvalContext::new(Arc::new(EvalConfig::from_flag(
                    Flag::TRUNCATE_AS_WARNING,
                ))))
                .evaluate::<Time>(ScalarFuncSig::CastIntAsTime)
                .unwrap();
            assert!(actual.is_none());
        }
    }

    #[test]
    #[allow(clippy::excessive_precision)]
    fn test_cast_real_time() {
        let cases = vec![
            ("2019-09-16 10:11:12", 190916101112.111, 0),
            ("2019-09-16 10:11:12", 20190916101112.111, 0),
            ("2019-09-16 10:11:12", 20190916101112.123, 0),
            ("2019-09-16 10:11:13", 20190916101112.999, 0),
            ("0000-00-00 00:00:00", 0.0, 0),
        ];

        for (expected, input, fsp) in cases {
            let actual: Time = RpnFnScalarEvaluator::new()
                .push_param(input)
                .return_field_type(
                    FieldTypeBuilder::new()
                        .tp(FieldTypeTp::DateTime)
                        .decimal(fsp)
                        .build(),
                )
                .evaluate::<Time>(ScalarFuncSig::CastRealAsTime)
                // `Result<Option<_>>`
                .unwrap()
                .unwrap();
            assert_eq!(actual.to_string(), expected);
        }
    }

    #[test]
    fn test_cast_string_as_time() {
        let cases = vec![
            ("2019-09-16 10:11:12", "20190916101112", 0),
            ("2019-09-16 10:11:12", "190916101112", 0),
            ("2019-09-16 10:11:01", "19091610111", 0),
            ("2019-09-16 10:11:00", "1909161011", 0),
            ("2019-09-16 10:01:00", "190916101", 0),
            ("1909-12-10 00:00:00", "19091210", 0),
            ("2020-02-29 10:00:00", "20200229100000", 0),
            ("2019-09-16 01:00:00", "1909161", 0),
            ("2019-09-16 00:00:00", "190916", 0),
            ("2019-09-01 00:00:00", "19091", 0),
            ("2019-09-16 10:11:12.111", "190916101112.111", 3),
            ("2019-09-16 10:11:12.111", "20190916101112.111", 3),
            ("2019-09-16 10:11:12.67", "20190916101112.666", 2),
            ("2019-09-16 10:11:13.0", "20190916101112.999", 1),
            ("2019-09-16 00:00:00", "2019-09-16", 0),
            ("2019-09-16 10:11:12", "2019-09-16 10:11:12", 0),
            ("2019-09-16 10:11:12", "2019-09-16T10:11:12", 0),
            ("2019-09-16 10:11:12.7", "2019-09-16T10:11:12.66", 1),
            ("2019-09-16 10:11:13.0", "2019-09-16T10:11:12.99", 1),
            ("2020-01-01 00:00:00.0", "2019-12-31 23:59:59.99", 1),
        ];

        for (expected, input, fsp) in cases {
            let actual: Time = RpnFnScalarEvaluator::new()
                .push_param(input.as_bytes().to_vec())
                .return_field_type(
                    FieldTypeBuilder::new()
                        .tp(FieldTypeTp::DateTime)
                        .decimal(fsp)
                        .build(),
                )
                .evaluate::<Time>(ScalarFuncSig::CastStringAsTime)
                // `Result<Option<_>>`
                .unwrap()
                .unwrap();
            assert_eq!(actual.to_string(), expected);
        }
    }

    #[test]
    fn test_time_as_time() {
        let cases = vec![
            // (Timestamp, DateTime)
            ("2020-02-29 10:00:00.999", "2020-02-29 10:00:01.0", 1),
            ("2019-09-16 01:00:00.999", "2019-09-16 01:00:01.00", 2),
            ("2019-09-16 00:00:00.9999", "2019-09-16 00:00:01.0", 1),
        ];

        for (input, expected, fsp) in cases {
            let mut ctx = EvalContext::default();
            let time =
                Time::parse_timestamp(&mut ctx, input, MAX_FSP, /* Enable round*/ true).unwrap();

            let actual: Time = RpnFnScalarEvaluator::new()
                .push_param(time)
                .return_field_type(
                    FieldTypeBuilder::new()
                        .tp(FieldTypeTp::DateTime)
                        .decimal(fsp)
                        .build(),
                )
                .evaluate::<Time>(ScalarFuncSig::CastTimeAsTime)
                // `Result<Option<_>>`
                .unwrap()
                .unwrap();
            assert_eq!(actual.to_string(), expected);
        }
    }

    #[test]
    fn test_cast_duration_as_time() {
        use chrono::Datelike;

        let cases = vec!["11:30:45.123456", "-35:30:46"];

        for case in cases {
            let mut ctx = EvalContext::default();

            let duration = Duration::parse(&mut ctx, case, MAX_FSP).unwrap();
            let now = RpnFnScalarEvaluator::new()
                .push_param(duration)
                .return_field_type(
                    FieldTypeBuilder::new()
                        .tp(FieldTypeTp::DateTime)
                        .decimal(MAX_FSP as isize)
                        .build(),
                )
                .evaluate::<Time>(ScalarFuncSig::CastDurationAsTime)
                .unwrap()
                .unwrap();
            let chrono_today = chrono::Utc::now();
            let today = now.checked_sub(&mut ctx, duration).unwrap();

            assert_eq!(today.year(), chrono_today.year() as u32);
            assert_eq!(today.month(), chrono_today.month());
            assert_eq!(today.day(), chrono_today.day());
            assert_eq!(today.hour(), 0);
            assert_eq!(today.minute(), 0);
            assert_eq!(today.second(), 0);
            assert_eq!(today.micro(), 0);
        }
    }

    #[test]
    fn test_cast_decimal_as_time() {
        let cases = vec![
            ("2019-09-16 10:11:12", "20190916101112", 0),
            ("2019-09-16 10:11:12", "190916101112", 0),
            ("1909-12-10 00:00:00", "19091210", 0),
            ("2020-02-29 10:00:00", "20200229100000", 0),
            ("2019-09-16 00:00:00", "190916", 0),
            ("2019-09-16 10:11:12.111", "190916101112.111", 3),
            ("2019-09-16 10:11:12.111", "20190916101112.111", 3),
            ("2019-09-16 10:11:12.67", "20190916101112.666", 2),
            ("2019-09-16 10:11:13.0", "20190916101112.999", 1),
            ("2001-11-11 00:00:00.0000", "11111.1111", 4),
            ("0102-11-21 14:11:05.4324", "1021121141105.4324", 4),
            ("2002-11-21 14:11:05.101", "21121141105.101", 3),
            ("2000-11-21 14:11:05.799055", "1121141105.799055", 6),
            ("2000-01-21 14:11:05.123", "121141105.123", 3),
            ("0114-11-05 00:00:00", "1141105", 0),
            ("2004-11-05 00:00:00.00", "41105.11", 2),
            ("2000-11-05 00:00:00.0", "1105.3", 1),
            ("2000-01-05 00:00:00", "105", 0),
        ];

        for (expected, decimal, fsp) in cases {
            let decimal: Decimal = decimal.parse().unwrap();
            let actual: Time = RpnFnScalarEvaluator::new()
                .push_param(decimal)
                .return_field_type(
                    FieldTypeBuilder::new()
                        .tp(FieldTypeTp::DateTime)
                        .decimal(fsp)
                        .build(),
                )
                .evaluate(ScalarFuncSig::CastDecimalAsTime)
                // `Result<Option<_>>`
                .unwrap()
                .unwrap();
            assert_eq!(actual.to_string(), expected);
        }

        let should_fail = vec![
            "19091610111",
            "1909161011",
            "190916101",
            "1909161",
            "19091",
            "201705051315111.22",
            "2011110859.1111",
            "2011110859.1111",
            "191203081.1111",
            "43128.121105",
        ];

        for case in should_fail {
            let case: Decimal = case.parse().unwrap();
            let actual = RpnFnScalarEvaluator::new()
                .push_param(case)
                .return_field_type(FieldTypeBuilder::new().tp(FieldTypeTp::DateTime).build())
                .evaluate::<Time>(ScalarFuncSig::CastDecimalAsTime)
                .unwrap();
            assert!(actual.is_none());
        }
    }

    #[test]
    fn test_duration_as_int() {
        let mut ctx = EvalContext::default();
        // TODO: add more test case
        let cs: Vec<(Duration, i64)> = vec![
            (Duration::parse(&mut ctx, "17:51:04.78", 2).unwrap(), 175105),
            (
                Duration::parse(&mut ctx, "-17:51:04.78", 2).unwrap(),
                -175105,
            ),
            (Duration::parse(&mut ctx, "17:51:04.78", 0).unwrap(), 175105),
            (
                Duration::parse(&mut ctx, "-17:51:04.78", 0).unwrap(),
                -175105,
            ),
        ];

        for (input, expect) in cs {
            let mut ctx = CtxConfig {
                overflow_as_warning: true,
                ..CtxConfig::default()
            }
            .into();
            let r = cast_any_as_any::<Duration, Int>(&mut ctx, Some(&input));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_json_as_int() {
        test_none_with_ctx(cast_json_as_any::<Int>);

        // no overflow
        let cs = vec![
            // (origin, expect, overflow, truncate)
            (
                Json::from_object(BTreeMap::default()).unwrap(),
                0,
                false,
                true,
            ),
            (Json::from_array(vec![]).unwrap(), 0, false, true),
            (Json::from_i64(10).unwrap(), 10i64, false, false),
            (Json::from_i64(i64::MAX).unwrap(), i64::MAX, false, false),
            (Json::from_i64(i64::MIN).unwrap(), i64::MIN, false, false),
            (Json::from_u64(0).unwrap(), 0, false, false),
            (
                Json::from_u64(u64::MAX).unwrap(),
                u64::MAX as i64,
                false,
                false,
            ),
            (
                Json::from_f64(i64::MIN as u64 as f64).unwrap(),
                i64::MAX,
                false,
                false,
            ),
            (
                Json::from_f64(i64::MAX as u64 as f64).unwrap(),
                i64::MAX,
                false,
                false,
            ),
            (
                Json::from_f64(i64::MIN as u64 as f64).unwrap(),
                i64::MAX,
                false,
                false,
            ),
            (
                Json::from_f64(i64::MIN as f64).unwrap(),
                i64::MIN,
                false,
                false,
            ),
            (Json::from_f64(10.5).unwrap(), 11, false, false),
            (Json::from_f64(10.4).unwrap(), 10, false, false),
            (Json::from_f64(-10.4).unwrap(), -10, false, false),
            (Json::from_f64(-10.5).unwrap(), -11, false, false),
            (
                Json::from_string(String::from("10.0")).unwrap(),
                10,
                false,
                false,
            ),
            (Json::from_bool(true).unwrap(), 1, false, false),
            (Json::from_bool(false).unwrap(), 0, false, false),
            (Json::none().unwrap(), 0, false, false),
            (
                Json::from_f64(((1u64 << 63) + (1u64 << 62)) as u64 as f64).unwrap(),
                i64::MAX,
                true,
                false,
            ),
            (
                Json::from_f64(-((1u64 << 63) as f64 + (1u64 << 62) as f64)).unwrap(),
                i64::MIN,
                true,
                false,
            ),
        ];

        for (input, expect, overflow, truncate) in cs {
            let mut ctx = CtxConfig {
                overflow_as_warning: true,
                truncate_as_warning: true,
                ..CtxConfig::default()
            }
            .into();
            let r = cast_json_as_any::<Int>(&mut ctx, Some(input.as_ref()));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
            check_overflow(&ctx, overflow, log.as_str());
            check_truncate(&ctx, truncate, log.as_str())
        }
    }

    #[test]
    fn test_json_as_uint() {
        test_none_with_ctx(cast_json_as_uint);

        // no clip to zero
        let cs: Vec<(Json, u64, Option<i32>)> = vec![
            // (origin, expect, error_code)
            (Json::from_f64(-1.0).unwrap(), -1.0f64 as i64 as u64, None),
            (Json::from_string(String::from("10")).unwrap(), 10, None),
            (
                Json::from_string(String::from("+10abc")).unwrap(),
                10,
                Some(ERR_TRUNCATE_WRONG_VALUE),
            ),
            (
                Json::from_string(String::from("9999999999999999999999999")).unwrap(),
                u64::MAX,
                Some(ERR_DATA_OUT_OF_RANGE),
            ),
            (
                Json::from_f64(2f64 * (u64::MAX as f64)).unwrap(),
                u64::MAX,
                Some(ERR_DATA_OUT_OF_RANGE),
            ),
        ];

        for (input, expect, error_code) in cs {
            let mut ctx = CtxConfig {
                overflow_as_warning: true,
                truncate_as_warning: true,
                ..CtxConfig::default()
            }
            .into();
            let r = cast_json_as_uint(&mut ctx, Some(input.as_ref()));
            let r = r.map(|x| x.map(|x| x as u64));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
            check_warning(&ctx, error_code, log.as_str());
        }

        // should clip to zero
        let cs: Vec<(Json, u64, Option<i32>)> = vec![
            // (origin, expect, err_code)
            (Json::from_f64(-1.0).unwrap(), 0, None),
            (
                Json::from_string(String::from("-10")).unwrap(),
                0,
                Some(ERR_DATA_OUT_OF_RANGE),
            ),
            (Json::from_string(String::from("10")).unwrap(), 10, None),
            (
                Json::from_string(String::from("+10abc")).unwrap(),
                10,
                Some(ERR_TRUNCATE_WRONG_VALUE),
            ),
            (
                Json::from_string(String::from("9999999999999999999999999")).unwrap(),
                u64::MAX,
                Some(ERR_DATA_OUT_OF_RANGE),
            ),
            (
                Json::from_f64(2f64 * (u64::MAX as f64)).unwrap(),
                u64::MAX,
                Some(ERR_DATA_OUT_OF_RANGE),
            ),
        ];

        for (input, expect, err_code) in cs {
            let mut ctx = CtxConfig {
                overflow_as_warning: true,
                truncate_as_warning: true,
                should_clip_to_zero: true,
                ..CtxConfig::default()
            }
            .into();
            let r = cast_json_as_uint(&mut ctx, Some(input.as_ref()));
            let r = r.map(|x| x.map(|x| x as u64));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
            check_warning(&ctx, err_code, log.as_str());
        }
    }

    #[test]
    fn test_signed_int_as_signed_real() {
        test_none_with_nothing(cast_signed_int_as_signed_real);

        let cs: Vec<(i64, f64)> = vec![
            // (input, expect)
            (i64::MIN, i64::MIN as f64),
            (0, 0f64),
            (i64::MAX, i64::MAX as f64),
        ];

        for (input, expect) in cs {
            let r = cast_signed_int_as_signed_real(Some(&input));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_signed_int_as_unsigned_real() {
        test_none_with_metadata(cast_signed_int_as_unsigned_real);

        let cs: Vec<(i64, f64, bool)> = vec![
            // (input, expect, in_union)

            // TODO: add test case of negative int to unsigned real without in_union
            // (i64::MIN, i64::MIN as u64 as f64, false),

            // not in union
            (i64::MAX, i64::MAX as f64, false),
            (0, 0f64, false),
            // in union
            (i64::MIN, 0f64, true),
            (-1, 0f64, true),
            (i64::MAX, i64::MAX as f64, true),
            (0, 0f64, true),
        ];
        for (input, expect, in_union) in cs {
            let metadata = make_metadata(in_union);
            let r = cast_signed_int_as_unsigned_real(&metadata, Some(&input));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            let log = format!(
                "input: {}, expect: {}, in_union: {}",
                input, expect, in_union
            );
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_unsigned_int_as_signed_or_unsigned_real() {
        test_none_with_nothing(cast_unsigned_int_as_signed_or_unsigned_real);

        let cs = vec![
            // (input, expect)
            (0, 0f64),
            (u64::MAX, u64::MAX as f64),
            (i64::MAX as u64, i64::MAX as u64 as f64),
        ];
        for (input, expect) in cs {
            let r = cast_unsigned_int_as_signed_or_unsigned_real(Some(&(input as i64)));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_real_as_signed_real() {
        test_none_with_nothing(cast_real_as_signed_real);

        let cs = vec![
            // (input, expect)
            (f64::from(f32::MIN), f64::from(f32::MIN)),
            (f64::from(f32::MAX), f64::from(f32::MAX)),
            (f64::MIN, f64::MIN),
            (0f64, 0f64),
            (f64::MAX, f64::MAX),
            (i64::MIN as f64, i64::MIN as f64),
            (i64::MAX as f64, i64::MAX as f64),
            (u64::MAX as f64, u64::MAX as f64),
        ];
        for (input, expect) in cs {
            let r = cast_real_as_signed_real(Some(Real::new(input).as_ref().unwrap()));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_real_as_unsigned_real() {
        let cs = vec![
            // (input, expect, in_union)
            // not in union
            // TODO: add test case of negative real to unsigned real
            // (-1.0, -1.0, false),
            // (i64::MIN as f64, i64::MIN as f64, false),
            // (f64::MIN, f64::MIN, false),
            (u64::MIN as f64, u64::MIN as f64, false),
            (1.0, 1.0, false),
            (i64::MAX as f64, i64::MAX as f64, false),
            (u64::MAX as f64, u64::MAX as f64, false),
            (f64::MAX, f64::MAX, false),
            // in union
            (-1.0, 0.0, true),
            (i64::MIN as f64, 0.0, true),
            (u64::MIN as f64, 0.0, true),
            (f64::MIN, 0.0, true),
            (1.0, 1.0, true),
            (i64::MAX as f64, i64::MAX as f64, true),
            (u64::MAX as f64, u64::MAX as f64, true),
            (f64::MAX, f64::MAX, true),
        ];

        for (input, expect, in_union) in cs {
            let metadata = make_metadata(in_union);
            let r = cast_real_as_unsigned_real(&metadata, Some(Real::new(input).as_ref().unwrap()));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            let log = format!(
                "input: {}, expect: {}, in_union: {}",
                input, expect, in_union
            );
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_cast_string_as_real() {
        // None
        {
            let output: Option<Real> = RpnFnScalarEvaluator::new()
                .push_param(ScalarValue::Bytes(None))
                .evaluate(ScalarFuncSig::CastStringAsReal)
                .unwrap();
            assert_eq!(output, None);
        }

        // signed
        let ul = UNSPECIFIED_LENGTH;
        let cs: Vec<(String, f64, isize, isize, bool, bool)> = vec![
            // (input, expect, flen, decimal, truncated, overflow)
            // no special flen and decimal
            (String::from("99999999"), 99999999f64, ul, ul, false, false),
            (String::from("1234abc"), 1234f64, ul, ul, true, false),
            (String::from("-1234abc"), -1234f64, ul, ul, true, false),
            (
                (0..400).map(|_| '9').collect::<String>(),
                f64::MAX,
                ul,
                ul,
                true,
                false,
            ),
            (
                (0..401)
                    .map(|x| if x == 0 { '-' } else { '9' })
                    .collect::<String>(),
                f64::MIN,
                ul,
                ul,
                true,
                false,
            ),
            // with special flen and decimal
            (String::from("99999999"), 99999999f64, 8, 0, false, false),
            (String::from("99999999"), 99999999f64, 9, 0, false, false),
            (String::from("99999999"), 9999999f64, 7, 0, false, true),
            (String::from("99999999"), 999999.99, 8, 2, false, true),
            (String::from("1234abc"), 0.9f64, 1, 1, true, true),
            (String::from("-1234abc"), -0.9f64, 1, 1, true, true),
            (String::from(""), 0f64, 1, 0, false, false),
        ];

        for (input, expected, flen, decimal, truncated, overflow) in cs {
            let (result, ctx) = RpnFnScalarEvaluator::new()
                .context(CtxConfig {
                    overflow_as_warning: true,
                    truncate_as_warning: true,
                    ..CtxConfig::default()
                })
                .push_param(input.clone().into_bytes())
                .evaluate_raw(
                    FieldTypeConfig {
                        unsigned: false,
                        flen,
                        decimal,
                        tp: Some(FieldTypeTp::Double),
                        ..FieldTypeConfig::default()
                    },
                    ScalarFuncSig::CastStringAsReal,
                );
            let output: Option<Real> = result.unwrap().into();
            assert!(
                (output.unwrap().into_inner() - expected).abs() < std::f64::EPSILON,
                "input={:?}",
                input
            );
            let (warning_cnt, warnings) = match (truncated, overflow) {
                (true, true) => (2, vec![ERR_TRUNCATE_WRONG_VALUE, ERR_DATA_OUT_OF_RANGE]),
                (true, false) => (1, vec![ERR_TRUNCATE_WRONG_VALUE]),
                (false, true) => (1, vec![ERR_DATA_OUT_OF_RANGE]),
                _ => (0, vec![]),
            };
            assert_eq!(ctx.warnings.warning_cnt, warning_cnt);
            let mut got_warnings = ctx
                .warnings
                .warnings
                .iter()
                .map(|w| w.get_code())
                .collect::<Vec<i32>>();
            got_warnings.sort_unstable();
            assert_eq!(got_warnings, warnings);
        }

        // unsigned
        let cs: Vec<(String, f64, isize, isize, bool, bool, bool)> = vec![
            // (input, expect, flen, decimal, truncated, overflow, in_union)

            // not in union
            (
                String::from("99999999"),
                99999999f64,
                ul,
                ul,
                false,
                false,
                false,
            ),
            (String::from("1234abc"), 1234f64, ul, ul, true, false, false),
            (
                (0..400).map(|_| '9').collect::<String>(),
                f64::MAX,
                ul,
                ul,
                true,
                false,
                false,
            ),
            (
                String::from("99999999"),
                99999999f64,
                8,
                0,
                false,
                false,
                false,
            ),
            (
                String::from("99999999"),
                9999999.9,
                8,
                1,
                false,
                true,
                false,
            ),
            (
                String::from("99999999"),
                999999.99,
                8,
                2,
                false,
                true,
                false,
            ),
            (String::from("99999999"), 999999.9, 7, 1, false, true, false),
            (String::from("1234abc"), 1234.0, 4, 0, true, false, false),
            (String::from("1234abc"), 999.9, 4, 1, true, true, false),
            (String::from("1234abc"), 99.99, 4, 2, true, true, false),
            (String::from("1234abc"), 99.9, 3, 1, true, true, false),
            (String::from("1234abc"), 9.999, 4, 3, true, true, false),
            (
                String::from("99999999"),
                99999999f64,
                8,
                0,
                false,
                false,
                false,
            ),
            (
                String::from("99999999"),
                9999999.9,
                8,
                1,
                false,
                true,
                false,
            ),
            (
                String::from("99999999"),
                999999.99,
                8,
                2,
                false,
                true,
                false,
            ),
            (String::from("99999999"), 999999.9, 7, 1, false, true, false),
            (String::from("1234abc"), 1234.0, 4, 0, true, false, false),
            (String::from("1234abc"), 999.9, 4, 1, true, true, false),
            (String::from("1234abc"), 99.99, 4, 2, true, true, false),
            (String::from("1234abc"), 99.9, 3, 1, true, true, false),
            (String::from("1234abc"), 9.999, 4, 3, true, true, false),
            (
                (0..400).map(|_| '9').collect::<String>(),
                f64::MAX,
                ul,
                ul,
                true,
                false,
                false,
            ),
            (
                (0..400).map(|_| '9').collect::<String>(),
                9999999999.0,
                10,
                0,
                true,
                true,
                false,
            ),
            (
                (0..400).map(|_| '9').collect::<String>(),
                999999999.9,
                10,
                1,
                true,
                true,
                false,
            ),
            // TODO
            // (
            //     (0..401)
            //         .map(|x| if x == 0 { '-' } else { '9' })
            //         .collect::<String>(),
            //     0f64, ul, ul, true, true, false,
            // ),
            // (
            //     String::from("-1234abc"), 0f64, ul, ul,
            //     true, true, false,
            // ),
            // (String::from("-1234abc"), 0.0, 4, 0, true, true, false),
            // (String::from("-1234abc"), 0.0, 4, 1, true, true, false),
            // (String::from("-1234abc"), 0.0, 4, 2, true, true, false),
            // (String::from("-1234abc"), 0.0, 3, 1, true, true, false),
            // (String::from("-1234abc"), 0.0, 4, 3, true, true, false),

            // in union
            // in union and neg
            (String::from("-190"), 0f64, ul, ul, false, false, true),
            (String::from("-10abc"), 0f64, ul, ul, true, false, true),
            (String::from("-1234abc"), 0.0, ul, ul, true, false, true),
        ];

        for (input, expected, flen, decimal, truncated, overflow, in_union) in cs {
            let (result, ctx) = RpnFnScalarEvaluator::new()
                .context(CtxConfig {
                    overflow_as_warning: true,
                    truncate_as_warning: true,
                    ..CtxConfig::default()
                })
                .metadata(Box::new(make_metadata(in_union)))
                .push_param(input.clone().into_bytes())
                .evaluate_raw(
                    FieldTypeConfig {
                        unsigned: true,
                        flen,
                        decimal,
                        tp: Some(FieldTypeTp::Double),
                        ..FieldTypeConfig::default()
                    },
                    ScalarFuncSig::CastStringAsReal,
                );
            let output: Option<Real> = result.unwrap().into();
            assert!(
                (output.unwrap().into_inner() - expected).abs() < std::f64::EPSILON,
                "input:{:?}, expected:{:?}, flen:{:?}, decimal:{:?}, truncated:{:?}, overflow:{:?}, in_union:{:?}",
                input, expected, flen, decimal, truncated, overflow, in_union
            );
            let (warning_cnt, warnings) = match (truncated, overflow) {
                (true, true) => (2, vec![ERR_TRUNCATE_WRONG_VALUE, ERR_DATA_OUT_OF_RANGE]),
                (true, false) => (1, vec![ERR_TRUNCATE_WRONG_VALUE]),
                (false, true) => (1, vec![ERR_DATA_OUT_OF_RANGE]),
                _ => (0, vec![]),
            };
            let mut got_warnings = ctx
                .warnings
                .warnings
                .iter()
                .map(|w| w.get_code())
                .collect::<Vec<i32>>();
            got_warnings.sort_unstable();
            assert_eq!(
                ctx.warnings.warning_cnt, warning_cnt,
                "input:{:?}, expected:{:?}, flen:{:?}, decimal:{:?}, truncated:{:?}, overflow:{:?}, in_union:{:?}, warnings:{:?}",
                input, expected, flen, decimal, truncated, overflow, in_union,got_warnings,
            );
            assert_eq!(got_warnings, warnings);
        }

        // not in union, neg
        let cs: Vec<(String, f64, isize, isize, Vec<i32>)> = vec![
            (
                (0..401)
                    .map(|x| if x == 0 { '-' } else { '9' })
                    .collect::<String>(),
                0f64,
                ul,
                ul,
                vec![ERR_TRUNCATE_WRONG_VALUE, ERR_DATA_OUT_OF_RANGE],
            ),
            (
                String::from("-1234abc"),
                0f64,
                ul,
                ul,
                vec![ERR_TRUNCATE_WRONG_VALUE, ERR_DATA_OUT_OF_RANGE],
            ),
            (
                String::from("-1234abc"),
                0.0,
                4,
                0,
                vec![ERR_TRUNCATE_WRONG_VALUE, ERR_DATA_OUT_OF_RANGE],
            ),
            // the case below has 3 warning
            // 1. from getValidFloatPrefix, because of `-1234abc`'s `abc`, (ERR_TRUNCATE_WRONG_VALUE)
            // 2. from ProduceFloatWithSpecifiedTp, because of TruncateFloat (ERR_DATA_OUT_OF_RANGE)
            // 3. from ProduceFloatWithSpecifiedTp, because of unsigned but negative (ERR_DATA_OUT_OF_RANGE)
            (
                String::from("-1234abc"),
                0.0,
                4,
                1,
                vec![
                    ERR_TRUNCATE_WRONG_VALUE,
                    ERR_DATA_OUT_OF_RANGE,
                    ERR_DATA_OUT_OF_RANGE,
                ],
            ),
            (
                String::from("-1234abc"),
                0.0,
                4,
                2,
                vec![
                    ERR_TRUNCATE_WRONG_VALUE,
                    ERR_DATA_OUT_OF_RANGE,
                    ERR_DATA_OUT_OF_RANGE,
                ],
            ),
            (
                String::from("-1234abc"),
                0.0,
                3,
                1,
                vec![
                    ERR_TRUNCATE_WRONG_VALUE,
                    ERR_DATA_OUT_OF_RANGE,
                    ERR_DATA_OUT_OF_RANGE,
                ],
            ),
            (
                String::from("-1234abc"),
                0.0,
                4,
                3,
                vec![
                    ERR_TRUNCATE_WRONG_VALUE,
                    ERR_DATA_OUT_OF_RANGE,
                    ERR_DATA_OUT_OF_RANGE,
                ],
            ),
        ];
        for (input, expected, flen, decimal, err_codes) in cs {
            let (result, ctx) = RpnFnScalarEvaluator::new()
                .context(CtxConfig {
                    overflow_as_warning: true,
                    truncate_as_warning: true,
                    ..CtxConfig::default()
                })
                .metadata(Box::new(make_metadata(false)))
                .push_param(input.clone().into_bytes())
                .evaluate_raw(
                    FieldTypeConfig {
                        unsigned: true,
                        flen,
                        decimal,
                        tp: Some(FieldTypeTp::Double),
                        ..FieldTypeConfig::default()
                    },
                    ScalarFuncSig::CastStringAsReal,
                );
            let output: Option<Real> = result.unwrap().into();
            assert!(
                (output.unwrap().into_inner() - expected).abs() < std::f64::EPSILON,
                "input={:?}",
                input
            );

            assert_eq!(ctx.warnings.warning_cnt, err_codes.len());
            for (idx, err) in err_codes.iter().enumerate() {
                assert_eq!(
                    ctx.warnings.warnings[idx].get_code(),
                    *err,
                    "input: {:?}",
                    input
                );
            }
        }

        // binary literal
        let cases = vec![
            (vec![0x01, 0x02, 0x03], Some(f64::from(0x010203))),
            (vec![0x01, 0x02, 0x03, 0x4], Some(f64::from(0x01020304))),
            (
                vec![0x01, 0x02, 0x03, 0x4, 0x05, 0x06, 0x06, 0x06, 0x06],
                None,
            ),
        ];
        for (input, expected) in cases {
            let output: Result<Option<Real>> = RpnFnScalarEvaluator::new()
                .metadata(Box::new(make_metadata(false)))
                .return_field_type(FieldTypeConfig {
                    flen: tidb_query_datatype::UNSPECIFIED_LENGTH,
                    decimal: tidb_query_datatype::UNSPECIFIED_LENGTH,
                    tp: Some(FieldTypeTp::Double),
                    ..FieldTypeConfig::default()
                })
                .push_param_with_field_type(
                    input.clone(),
                    FieldTypeConfig {
                        tp: Some(FieldTypeTp::VarString),
                        collation: Some(Collation::Binary),
                        ..FieldTypeConfig::default()
                    },
                )
                .evaluate(ScalarFuncSig::CastStringAsReal);

            if let Some(exp) = expected {
                assert!(output.is_ok(), "input: {:?}", input);
                assert!(
                    (output.unwrap().unwrap().into_inner() - exp).abs() < std::f64::EPSILON,
                    "input={:?}",
                    input
                );
            } else {
                assert!(output.is_err());
            }
        }
    }

    #[test]
    fn test_decimal_as_signed_real() {
        test_none_with_ctx(cast_any_as_any::<Decimal, Int>);

        // because decimal can always be represent by signed real,
        // so we needn't to check whether get truncated err.
        let cs = vec![
            // (input, expect)
            (Decimal::from_f64(-10.0).unwrap(), -10.0),
            (Decimal::from_f64(i64::MIN as f64).unwrap(), i64::MIN as f64),
            (Decimal::from_f64(i64::MAX as f64).unwrap(), i64::MAX as f64),
            (Decimal::from_f64(u64::MAX as f64).unwrap(), u64::MAX as f64),
        ];
        for (input, expect) in cs {
            let mut ctx = EvalContext::default();
            let r = cast_any_as_any::<Decimal, Real>(&mut ctx, Some(&input));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_decimal_as_unsigned_real() {
        test_none_with_ctx_and_metadata(cast_decimal_as_unsigned_real);

        let cs: Vec<(Decimal, f64, bool, bool)> = vec![
            // (origin, expect, in_union, overflow)
            // not in union
            (Decimal::from(0), 0.0, false, false),
            (
                Decimal::from(9223372036854775807u64),
                9223372036854775807.0,
                false,
                false,
            ),
            (
                Decimal::from_bytes(b"9223372036854775809")
                    .unwrap()
                    .unwrap(),
                9223372036854775809.0,
                false,
                false,
            ),
            // TODO: add test case for negative decimal to unsigned real

            // in union
            (Decimal::from(-1023), 0f64, true, false),
            (Decimal::from(-10), 0f64, true, false),
            (Decimal::from(i64::MIN), 0f64, true, false),
            (Decimal::from(1023), 1023.0, true, false),
            (Decimal::from(10), 10.0, true, false),
            (Decimal::from(i64::MAX), i64::MAX as f64, true, false),
            (Decimal::from(u64::MAX), u64::MAX as f64, true, false),
            (
                Decimal::from(1844674407370955161u64),
                1844674407370955161u64 as f64,
                true,
                false,
            ),
            (
                Decimal::from_bytes(b"18446744073709551616")
                    .unwrap()
                    .unwrap(),
                // 18446744073709551616 - u64::MAX==1,
                // but u64::MAX as f64 == 18446744073709551616
                u64::MAX as f64,
                true,
                false,
            ),
        ];

        for (input, expect, in_union, overflow) in cs {
            let mut ctx = CtxConfig {
                overflow_as_warning: true,
                ..CtxConfig::default()
            }
            .into();
            let metadata = make_metadata(in_union);
            let r = cast_decimal_as_unsigned_real(&mut ctx, &metadata, Some(&input));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            let log = format!(
                "input: {}, expect: {}, in_union: {}, expect_overflow: {}, result: {:?}",
                input, expect, in_union, overflow, r
            );
            check_result(Some(&expect), &r, log.as_str());
            check_overflow(&ctx, overflow, log.as_str());
        }
    }

    #[test]
    #[allow(clippy::excessive_precision)]
    fn test_time_as_real() {
        let mut ctx = EvalContext::default();
        test_none_with_ctx(cast_any_as_any::<Time, Real>);

        // TODO: add more test case
        let cs = vec![
            (
                Time::parse_datetime(&mut ctx, "2000-01-01T12:13:14.6666", 6, true).unwrap(),
                20000101121314.666600,
            ),
            (
                Time::parse_datetime(&mut ctx, "2000-01-01T12:13:14.6666", 0, true).unwrap(),
                20000101121315.0,
            ),
            (
                Time::parse_datetime(&mut ctx, "2000-01-01T12:13:14.6666", 3, true).unwrap(),
                20000101121314.667,
            ),
            (
                Time::parse_datetime(&mut ctx, "2000-01-01T12:13:14.6666", 4, true).unwrap(),
                20000101121314.6666,
            ),
        ];

        for (input, expect) in cs {
            let mut ctx = EvalContext::default();
            let r = cast_any_as_any::<Time, Real>(&mut ctx, Some(&input));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_duration_as_real() {
        let mut ctx = EvalContext::default();
        // TODO: add more test case
        let cs = vec![
            // (input, expect)
            (
                Duration::parse(&mut ctx, "17:51:04.78", 2).unwrap(),
                175104.78,
            ),
            (
                Duration::parse(&mut ctx, "-17:51:04.78", 2).unwrap(),
                -175104.78,
            ),
            (
                Duration::parse(&mut ctx, "17:51:04.78", 0).unwrap(),
                175105.0,
            ),
            (
                Duration::parse(&mut ctx, "-17:51:04.78", 0).unwrap(),
                -175105.0,
            ),
        ];
        for (input, expect) in cs {
            let mut ctx = EvalContext::default();
            let r = cast_any_as_any::<Duration, Real>(&mut ctx, Some(&input));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_json_as_real() {
        let cs: Vec<(Json, f64, Option<i32>)> = vec![
            // (input, expect, err_code)
            (Json::from_object(BTreeMap::default()).unwrap(), 0f64, None),
            (Json::from_array(vec![]).unwrap(), 0f64, None),
            (Json::from_i64(10).unwrap(), 10f64, None),
            (Json::from_i64(i64::MAX).unwrap(), i64::MAX as f64, None),
            (Json::from_i64(i64::MIN).unwrap(), i64::MIN as f64, None),
            (Json::from_u64(0).unwrap(), 0f64, None),
            (Json::from_u64(u64::MAX).unwrap(), u64::MAX as f64, None),
            (Json::from_f64(f64::MAX).unwrap(), f64::MAX, None),
            (Json::from_f64(f64::MIN).unwrap(), f64::MIN, None),
            (Json::from_string(String::from("10.0")).unwrap(), 10.0, None),
            (
                Json::from_string(String::from("-10.0")).unwrap(),
                -10.0,
                None,
            ),
            (Json::from_bool(true).unwrap(), 1f64, None),
            (Json::from_bool(false).unwrap(), 0f64, None),
            (Json::none().unwrap(), 0f64, None),
            (
                Json::from_string((0..500).map(|_| '9').collect::<String>()).unwrap(),
                f64::MAX,
                Some(ERR_TRUNCATE_WRONG_VALUE),
            ),
            (
                Json::from_string(
                    (0..500)
                        .map(|x| if x == 0 { '-' } else { '9' })
                        .collect::<String>(),
                )
                .unwrap(),
                f64::MIN,
                Some(ERR_TRUNCATE_WRONG_VALUE),
            ),
        ];

        for (input, expect, err_code) in cs {
            let mut ctx = CtxConfig {
                truncate_as_warning: true,
                ..CtxConfig::default()
            }
            .into();
            let r = cast_json_as_any::<Real>(&mut ctx, Some(input.as_ref()));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
            check_warning(&ctx, err_code, log.as_str());
        }
    }

    /// base_cs:
    /// vector of (T, T to bytes(without any other handle do by cast_as_string_helper),
    /// T to string for debug output),
    /// the object should not be zero len.
    #[allow(clippy::type_complexity)]
    fn test_as_string_helper<T: Clone, FnCast>(
        base_cs: Vec<(T, Vec<u8>, String)>,
        cast_func: FnCast,
        func_name: &str,
    ) where
        FnCast: Fn(&mut EvalContext, &RpnFnCallExtra, Option<T>) -> Result<Option<Bytes>>,
    {
        #[derive(Clone, Copy)]
        enum FlenType {
            Eq,
            LessOne,
            ExtraOne,
            Unspecified,
        }
        let cs: Vec<(FlenType, bool, &str, FieldTypeTp, Collation, Option<i32>)> = vec![
            // (flen_type, pad_zero, charset, tp, collation, err_code)

            // normal, flen==str.len
            (
                FlenType::Eq,
                false,
                CHARSET_BIN,
                FieldTypeTp::String,
                Collation::Binary,
                None,
            ),
            (
                FlenType::Eq,
                false,
                CHARSET_UTF8,
                FieldTypeTp::String,
                Collation::Binary,
                None,
            ),
            (
                FlenType::Eq,
                false,
                CHARSET_UTF8MB4,
                FieldTypeTp::String,
                Collation::Binary,
                None,
            ),
            (
                FlenType::Eq,
                false,
                CHARSET_ASCII,
                FieldTypeTp::String,
                Collation::Binary,
                None,
            ),
            (
                FlenType::Eq,
                false,
                CHARSET_LATIN1,
                FieldTypeTp::String,
                Collation::Binary,
                None,
            ),
            // normal, flen==UNSPECIFIED_LENGTH
            (
                FlenType::Unspecified,
                false,
                CHARSET_BIN,
                FieldTypeTp::String,
                Collation::Binary,
                None,
            ),
            (
                FlenType::Unspecified,
                false,
                CHARSET_UTF8,
                FieldTypeTp::String,
                Collation::Binary,
                None,
            ),
            (
                FlenType::Unspecified,
                false,
                CHARSET_UTF8MB4,
                FieldTypeTp::String,
                Collation::Binary,
                None,
            ),
            (
                FlenType::Unspecified,
                false,
                CHARSET_ASCII,
                FieldTypeTp::String,
                Collation::Binary,
                None,
            ),
            (
                FlenType::Unspecified,
                false,
                CHARSET_LATIN1,
                FieldTypeTp::String,
                Collation::Binary,
                None,
            ),
            // branch 1 of ProduceStrWithSpecifiedTp
            // not bin_str, so no pad_zero
            (
                FlenType::LessOne,
                false,
                CHARSET_UTF8,
                FieldTypeTp::String,
                Collation::Utf8Mb4BinNoPadding,
                Some(ERR_DATA_TOO_LONG),
            ),
            (
                FlenType::LessOne,
                false,
                CHARSET_UTF8MB4,
                FieldTypeTp::String,
                Collation::Utf8Mb4BinNoPadding,
                Some(ERR_DATA_TOO_LONG),
            ),
            (
                FlenType::Eq,
                false,
                CHARSET_UTF8,
                FieldTypeTp::String,
                Collation::Utf8Mb4BinNoPadding,
                None,
            ),
            (
                FlenType::Eq,
                false,
                CHARSET_UTF8MB4,
                FieldTypeTp::String,
                Collation::Utf8Mb4BinNoPadding,
                None,
            ),
            (
                FlenType::ExtraOne,
                false,
                CHARSET_UTF8,
                FieldTypeTp::String,
                Collation::Utf8Mb4BinNoPadding,
                None,
            ),
            (
                FlenType::ExtraOne,
                false,
                CHARSET_UTF8MB4,
                FieldTypeTp::String,
                Collation::Utf8Mb4BinNoPadding,
                None,
            ),
            (
                FlenType::ExtraOne,
                false,
                CHARSET_UTF8,
                FieldTypeTp::String,
                Collation::Utf8Mb4BinNoPadding,
                None,
            ),
            (
                FlenType::ExtraOne,
                false,
                CHARSET_UTF8MB4,
                FieldTypeTp::String,
                Collation::Utf8Mb4BinNoPadding,
                None,
            ),
            // bin_str, so need pad_zero
            (
                FlenType::ExtraOne,
                true,
                CHARSET_UTF8,
                FieldTypeTp::String,
                Collation::Binary,
                None,
            ),
            (
                FlenType::ExtraOne,
                true,
                CHARSET_UTF8MB4,
                FieldTypeTp::String,
                Collation::Binary,
                None,
            ),
            // branch 2 of ProduceStrWithSpecifiedTp
            // branch 2 need s.len>flen, so never need pad_zero
            (
                FlenType::LessOne,
                false,
                CHARSET_ASCII,
                FieldTypeTp::String,
                Collation::Utf8Mb4BinNoPadding,
                Some(ERR_DATA_TOO_LONG),
            ),
            (
                FlenType::LessOne,
                false,
                CHARSET_LATIN1,
                FieldTypeTp::String,
                Collation::Utf8Mb4BinNoPadding,
                Some(ERR_DATA_TOO_LONG),
            ),
            (
                FlenType::LessOne,
                false,
                CHARSET_BIN,
                FieldTypeTp::String,
                Collation::Utf8Mb4BinNoPadding,
                Some(ERR_DATA_TOO_LONG),
            ),
            // branch 3 of ProduceStrWithSpecifiedTp ,
            // will never be reached,
            // because padZero param is always false
        ];
        for (input, bytes, debug_str) in base_cs {
            for (flen_type, pad_zero, charset, tp, collation, err_code) in cs.iter() {
                let mut ctx = CtxConfig {
                    truncate_as_warning: true,
                    ..CtxConfig::default()
                }
                .into();
                let res_len = bytes.len();
                let flen = match flen_type {
                    FlenType::Eq => res_len as isize,
                    FlenType::LessOne => {
                        if res_len == 0 {
                            continue;
                        } else {
                            (res_len - 1) as isize
                        }
                    }
                    FlenType::ExtraOne => (res_len + 1) as isize,
                    FlenType::Unspecified => UNSPECIFIED_LENGTH,
                };
                let rft = FieldTypeConfig {
                    flen,
                    charset: Some(charset),
                    tp: Some(*tp),
                    collation: Some(*collation),
                    ..FieldTypeConfig::default()
                }
                .into();
                let extra = make_extra(&rft);

                let r = cast_func(&mut ctx, &extra, Some(input.clone()));

                let mut expect = bytes.clone();
                if *pad_zero && flen > expect.len() as isize {
                    expect.extend((expect.len()..flen as usize).map(|_| 0u8));
                } else if flen != UNSPECIFIED_LENGTH {
                    expect.truncate(flen as usize);
                }

                let log = format!(
                    "func: {:?}, input: {}, expect: {:?}, flen: {}, \
                     charset: {}, field_type: {}, collation: {}, output: {:?}",
                    func_name, debug_str, &expect, flen, charset, tp, collation, &r
                );
                check_result(Some(&expect), &r, log.as_str());
                check_warning(&ctx, *err_code, log.as_str());
            }
        }
    }

    #[test]
    fn test_int_as_string() {
        test_none_with_ctx_and_extra(cast_any_as_string::<Int>);

        let cs: Vec<(&i64, Vec<u8>, String)> = vec![
            (
                &i64::MAX,
                i64::MAX.to_string().into_bytes(),
                i64::MAX.to_string(),
            ),
            (
                &i64::MIN,
                i64::MIN.to_string().into_bytes(),
                i64::MIN.to_string(),
            ),
        ];
        test_as_string_helper(cs, cast_any_as_string::<Int>, "cast_any_as_string::<Int>");
    }

    fn helper_get_cs_ref<U, V: Clone, W: Clone>(cs: &[(U, V, W)]) -> Vec<(&U, V, W)> {
        cs.iter()
            .map(|(u, v, w)| (u, v.clone(), w.clone()))
            .collect()
    }

    #[test]
    fn test_uint_as_string() {
        test_none_with_ctx_and_extra(cast_uint_as_string);

        let cs: Vec<(u64, Vec<u8>, String)> = vec![
            (
                i64::MAX as u64,
                (i64::MAX as u64).to_string().into_bytes(),
                (i64::MAX as u64).to_string(),
            ),
            (
                i64::MIN as u64,
                (i64::MIN as u64).to_string().into_bytes(),
                (i64::MIN as u64).to_string(),
            ),
            (
                u64::MAX,
                u64::MAX.to_string().into_bytes(),
                u64::MAX.to_string(),
            ),
            (0u64, 0u64.to_string().into_bytes(), 0u64.to_string()),
        ];

        let ref_cs = helper_get_cs_ref(&cs);

        test_as_string_helper(
            ref_cs,
            |ctx, extra, val| {
                let val = val.map(|x| *x as i64);
                cast_uint_as_string(ctx, extra, val.as_ref())
            },
            "cast_uint_as_string",
        );
    }

    #[test]
    fn test_year_as_string() {
        let cs: Vec<(i64, Vec<u8>, String)> = vec![
            (0, b"0000".to_vec(), "0000".to_string()),
            (2000, b"2000".to_vec(), "2000".to_string()),
        ];

        let ref_cs = helper_get_cs_ref(&cs);

        test_as_string_helper(
            ref_cs,
            |ctx, extra, val| {
                let val = val.map(|x| *x as i64);
                cast_year_as_string(ctx, extra, &val.unwrap())
            },
            "cast_year_as_string",
        );
    }

    #[test]
    fn test_float_real_as_string() {
        test_none_with_ctx_and_extra(cast_float_real_as_string);

        let cs: Vec<(f32, Vec<u8>, String)> = vec![
            (
                f32::MAX,
                f32::MAX.to_string().into_bytes(),
                f32::MAX.to_string(),
            ),
            (1.0f32, 1.0f32.to_string().into_bytes(), 1.0f32.to_string()),
            (
                1.1113f32,
                1.1113f32.to_string().into_bytes(),
                1.1113f32.to_string(),
            ),
            (0.1f32, 0.1f32.to_string().into_bytes(), 0.1f32.to_string()),
        ];

        let ref_cs = helper_get_cs_ref(&cs);

        test_as_string_helper(
            ref_cs,
            |ctx, extra, val| {
                cast_float_real_as_string(
                    ctx,
                    extra,
                    val.map(|x| Real::new(f64::from(*x)).unwrap()).as_ref(),
                )
            },
            "cast_float_real_as_string",
        );
    }

    #[test]
    fn test_double_real_as_string() {
        test_none_with_ctx_and_extra(cast_any_as_string::<Real>);

        let cs: Vec<(f64, Vec<u8>, String)> = vec![
            (
                f64::from(f32::MAX),
                (f64::from(f32::MAX)).to_string().into_bytes(),
                f64::from(f32::MAX).to_string(),
            ),
            (
                f64::from(f32::MIN),
                (f64::from(f32::MIN)).to_string().into_bytes(),
                f64::from(f32::MIN).to_string(),
            ),
            (
                f64::MIN,
                f64::MIN.to_string().into_bytes(),
                f64::MIN.to_string(),
            ),
            (
                f64::MAX,
                f64::MAX.to_string().into_bytes(),
                f64::MAX.to_string(),
            ),
            (1.0f64, 1.0f64.to_string().into_bytes(), 1.0f64.to_string()),
            (
                1.1113f64,
                1.1113f64.to_string().into_bytes(),
                1.1113f64.to_string(),
            ),
            (0.1f64, 0.1f64.to_string().into_bytes(), 0.1f64.to_string()),
        ];

        let ref_cs = helper_get_cs_ref(&cs);

        test_as_string_helper(
            ref_cs,
            |ctx, extra, val| {
                cast_any_as_string::<Real>(ctx, extra, val.map(|x| Real::new(*x).unwrap()).as_ref())
            },
            "cast_any_as_string::<Real>",
        );
    }

    #[test]
    fn test_string_as_string() {
        test_none_with_ctx_and_extra(cast_string_as_string);

        let test_vec_1 = Vec::from(b"".as_ref());
        let test_vec_2 = (0..1024).map(|_| b'0').collect::<Vec<u8>>();

        let cs: Vec<(BytesRef, Vec<u8>, String)> = vec![
            (
                test_vec_1.as_slice(),
                Vec::from(b"".as_ref()),
                String::from("<empty-str>"),
            ),
            (
                test_vec_2.as_slice(),
                (0..1024).map(|_| b'0').collect::<Vec<u8>>(),
                String::from("1024 zeros('0')"),
            ),
        ];

        test_as_string_helper(cs, cast_string_as_string, "cast_string_as_string");
    }

    #[test]
    fn test_decimal_as_string() {
        test_none_with_ctx_and_extra(cast_any_as_string::<Decimal>);

        let cs: Vec<(Decimal, Vec<u8>, String)> = vec![
            (
                Decimal::from(i64::MAX),
                i64::MAX.to_string().into_bytes(),
                i64::MAX.to_string(),
            ),
            (
                Decimal::from(i64::MIN),
                i64::MIN.to_string().into_bytes(),
                i64::MIN.to_string(),
            ),
            (
                Decimal::from(u64::MAX),
                u64::MAX.to_string().into_bytes(),
                u64::MAX.to_string(),
            ),
            (
                Decimal::from_f64(0.0).unwrap(),
                0.0.to_string().into_bytes(),
                0.0.to_string(),
            ),
            (
                Decimal::from_f64(i64::MAX as f64).unwrap(),
                (i64::MAX as f64).to_string().into_bytes(),
                (i64::MAX as f64).to_string(),
            ),
            (
                Decimal::from_f64(i64::MIN as f64).unwrap(),
                (i64::MIN as f64).to_string().into_bytes(),
                (i64::MIN as f64).to_string(),
            ),
            (
                Decimal::from_f64(u64::MAX as f64).unwrap(),
                (u64::MAX as f64).to_string().into_bytes(),
                (u64::MAX as f64).to_string(),
            ),
            (
                Decimal::from_bytes(b"999999999999999999999999")
                    .unwrap()
                    .unwrap(),
                Vec::from(b"999999999999999999999999".as_ref()),
                String::from("999999999999999999999999"),
            ),
        ];

        let ref_cs = helper_get_cs_ref(&cs);

        test_as_string_helper(
            ref_cs,
            cast_any_as_string::<Decimal>,
            "cast_any_as_string::<Decimal>",
        );
    }

    #[test]
    fn test_time_as_string() {
        test_none_with_ctx_and_extra(cast_any_as_string::<Time>);

        let mut ctx = EvalContext::default();
        // TODO: add more test case
        let cs: Vec<(Time, Vec<u8>, String)> = vec![
            (
                Time::parse_datetime(&mut ctx, "2000-01-01T12:13:14", 0, true).unwrap(),
                "2000-01-01 12:13:14".to_string().into_bytes(),
                "2000-01-01 12:13:14".to_string(),
            ),
            (
                Time::parse_datetime(&mut ctx, "2000-01-01T12:13:14.6666", 0, true).unwrap(),
                "2000-01-01 12:13:15".to_string().into_bytes(),
                "2000-01-01 12:13:15".to_string(),
            ),
            (
                Time::parse_datetime(&mut ctx, "2000-01-01T12:13:14.6666", 3, true).unwrap(),
                "2000-01-01 12:13:14.667".to_string().into_bytes(),
                "2000-01-01 12:13:14.667".to_string(),
            ),
            (
                Time::parse_datetime(&mut ctx, "2000-01-01T12:13:14.6666", 4, true).unwrap(),
                "2000-01-01 12:13:14.6666".to_string().into_bytes(),
                "2000-01-01 12:13:14.6666".to_string(),
            ),
            (
                Time::parse_datetime(&mut ctx, "2000-01-01T12:13:14.6666", 6, true).unwrap(),
                "2000-01-01 12:13:14.666600".to_string().into_bytes(),
                "2000-01-01 12:13:14.666600".to_string(),
            ),
        ];

        let ref_cs = helper_get_cs_ref(&cs);

        test_as_string_helper(
            ref_cs,
            cast_any_as_string::<Time>,
            "cast_any_as_string::<Time>",
        );
    }

    #[test]
    fn test_duration_as_string() {
        test_none_with_ctx_and_extra(cast_any_as_string::<Duration>);
        let mut ctx = EvalContext::default();
        let cs = vec![
            (
                Duration::parse(&mut ctx, "17:51:04.78", 2).unwrap(),
                "17:51:04.78".to_string().into_bytes(),
                "17:51:04.78".to_string(),
            ),
            (
                Duration::parse(&mut ctx, "-17:51:04.78", 2).unwrap(),
                "-17:51:04.78".to_string().into_bytes(),
                "-17:51:04.78".to_string(),
            ),
            (
                Duration::parse(&mut ctx, "17:51:04.78", 0).unwrap(),
                "17:51:05".to_string().into_bytes(),
                "17:51:05".to_string(),
            ),
            (
                Duration::parse(&mut ctx, "-17:51:04.78", 0).unwrap(),
                "-17:51:05".to_string().into_bytes(),
                "-17:51:05".to_string(),
            ),
        ];

        let ref_cs = helper_get_cs_ref(&cs);

        test_as_string_helper(
            ref_cs,
            cast_any_as_string::<Duration>,
            "cast_any_as_string::<Duration>",
        );
    }

    #[test]
    fn test_json_as_string() {
        test_none_with_ctx(cast_json_as_bytes);

        // FIXME: this case is not exactly same as TiDB's,
        //  such as(left is TiKV, right is TiDB)
        //  f64::MIN =>        "1.7976931348623157e308",  "1.7976931348623157e+308",
        //  f64::MAX =>        "-1.7976931348623157e308", "-1.7976931348623157e+308",
        //  f32::MIN as f64 => "3.4028234663852886e38",   "3.4028234663852886e+38",
        //  f32::MAX as f64 => "-3.4028234663852886e38",  "-3.4028234663852886e+38",
        //  i64::MIN as f64 => "-9.223372036854776e18", "-9223372036854776000",
        //  i64::MAX as f64 => "9.223372036854776e18",  "9223372036854776000",
        //  u64::MAX as f64 => "1.8446744073709552e19", "18446744073709552000",
        let cs = vec![
            (
                Json::from_object(BTreeMap::default()).unwrap(),
                "{}".to_string(),
            ),
            (Json::from_array(vec![]).unwrap(), "[]".to_string()),
            (Json::from_i64(10).unwrap(), "10".to_string()),
            (Json::from_i64(i64::MAX).unwrap(), i64::MAX.to_string()),
            (Json::from_i64(i64::MIN).unwrap(), i64::MIN.to_string()),
            (Json::from_u64(0).unwrap(), "0".to_string()),
            (Json::from_u64(u64::MAX).unwrap(), u64::MAX.to_string()),
            (Json::from_f64(f64::MIN).unwrap(), format!("{:e}", f64::MIN)),
            (Json::from_f64(f64::MAX).unwrap(), format!("{:e}", f64::MAX)),
            (
                Json::from_f64(f64::from(f32::MIN)).unwrap(),
                format!("{:e}", f64::from(f32::MIN)),
            ),
            (
                Json::from_f64(f64::from(f32::MAX)).unwrap(),
                format!("{:e}", f64::from(f32::MAX)),
            ),
            (
                Json::from_f64(i64::MIN as f64).unwrap(),
                format!("{:e}", i64::MIN as f64),
            ),
            (
                Json::from_f64(i64::MAX as f64).unwrap(),
                format!("{:e}", i64::MAX as f64),
            ),
            (
                Json::from_f64(u64::MAX as f64).unwrap(),
                format!("{:e}", u64::MAX as f64),
            ),
            (Json::from_f64(10.5).unwrap(), "10.5".to_string()),
            (Json::from_f64(10.4).unwrap(), "10.4".to_string()),
            (Json::from_f64(-10.4).unwrap(), "-10.4".to_string()),
            (Json::from_f64(-10.5).unwrap(), "-10.5".to_string()),
            (
                Json::from_string(String::from("10.0")).unwrap(),
                r#""10.0""#.to_string(),
            ),
            (Json::from_bool(true).unwrap(), "true".to_string()),
            (Json::from_bool(false).unwrap(), "false".to_string()),
            (Json::none().unwrap(), "null".to_string()),
        ];

        for (input, expect) in cs {
            let mut ctx = EvalContext::default();
            let r = cast_json_as_bytes(&mut ctx, Some(input.as_ref()));
            let r = r.map(|x| x.map(|x| unsafe { String::from_utf8_unchecked(x) }));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    macro_rules! cast_closure_with_metadata {
        ($cast_fn:expr) => {
            |ctx, extra, _, val| $cast_fn(ctx, extra, val)
        };
    }

    /// base_cs
    ///   - (cast_func_input, in_union, is_res_unsigned, base_result)
    ///   - the base_result is the result **should** produce by
    /// the logic of cast func above `produce_dec_with_specified_tp`
    fn test_as_decimal_helper<T: Clone, FnCast, FnToStr>(
        base_cs: Vec<(T, bool, bool, Decimal)>,
        cast_func: FnCast,
        input_as_debug_str_func: FnToStr,
        func_name: &str,
    ) where
        FnCast: Fn(
            &mut EvalContext,
            &RpnFnCallExtra,
            &tipb::InUnionMetadata,
            Option<&T>,
        ) -> Result<Option<Decimal>>,
        FnToStr: Fn(&T) -> String,
    {
        #[derive(Clone, Copy, Debug)]
        #[allow(clippy::enum_variant_names)]
        enum Cond {
            TargetIntPartLenLessThanOriginIntPartLen,
            TargetDecimalBiggerThanOriginDecimal,
            TargetDecimalLessThanOriginDecimal,
        }

        #[derive(Clone, Copy, Debug)]
        enum Sign {
            Positive,
            Negative,
        }

        #[derive(Clone, Copy, Debug)]
        enum ResType {
            Zero,
            Same,
            TruncateToMax,
            TruncateToMin,
            Round,
        }

        let cs = vec![
            // (
            // origin, origin_flen, origin_decimal, res_flen, res_decimal, is_unsigned,
            // expect, warning_err_code,
            // (InInsertStmt || InUpdateStmt || InDeleteStmt), overflow_as_warning, truncate_as_warning
            // )
            //
            // The origin_flen, origin_decimal here is
            // to let the programmer clearly know what the flen and decimal of the decimal is.

            // res_flen and res_decimal isn't UNSPECIFIED_LENGTH
            // origin not zero, but res's int part len < origin's int part
            (
                Cond::TargetIntPartLenLessThanOriginIntPartLen,
                Sign::Positive,
                false,
                ResType::TruncateToMax,
                Some(ERR_DATA_OUT_OF_RANGE),
                false,
                true,
                false,
            ),
            (
                Cond::TargetIntPartLenLessThanOriginIntPartLen,
                Sign::Negative,
                false,
                ResType::TruncateToMin,
                Some(ERR_DATA_OUT_OF_RANGE),
                false,
                true,
                false,
            ),
            // origin_decimal < res_decimal
            (
                Cond::TargetDecimalBiggerThanOriginDecimal,
                Sign::Positive,
                false,
                ResType::Same,
                None,
                false,
                false,
                false,
            ),
            (
                Cond::TargetDecimalBiggerThanOriginDecimal,
                Sign::Positive,
                false,
                ResType::Same,
                None,
                true,
                false,
                false,
            ),
            (
                Cond::TargetDecimalBiggerThanOriginDecimal,
                Sign::Negative,
                false,
                ResType::Same,
                None,
                false,
                false,
                false,
            ),
            (
                Cond::TargetDecimalBiggerThanOriginDecimal,
                Sign::Positive,
                false,
                ResType::Same,
                None,
                true,
                false,
                false,
            ),
            (
                Cond::TargetDecimalBiggerThanOriginDecimal,
                Sign::Positive,
                true,
                ResType::Same,
                None,
                false,
                false,
                false,
            ),
            (
                Cond::TargetDecimalBiggerThanOriginDecimal,
                Sign::Positive,
                true,
                ResType::Same,
                None,
                true,
                false,
                false,
            ),
            (
                Cond::TargetDecimalBiggerThanOriginDecimal,
                Sign::Negative,
                true,
                ResType::Zero,
                None,
                false,
                false,
                false,
            ),
            (
                Cond::TargetDecimalBiggerThanOriginDecimal,
                Sign::Negative,
                true,
                ResType::Zero,
                None,
                true,
                false,
                false,
            ),
            // origin_decimal > res_decimal
            (
                Cond::TargetDecimalLessThanOriginDecimal,
                Sign::Positive,
                false,
                ResType::Round,
                Some(WARN_DATA_TRUNCATED),
                false,
                false,
                true,
            ),
            (
                Cond::TargetDecimalLessThanOriginDecimal,
                Sign::Positive,
                false,
                ResType::Round,
                Some(WARN_DATA_TRUNCATED),
                true,
                false,
                false,
            ),
            (
                Cond::TargetDecimalLessThanOriginDecimal,
                Sign::Negative,
                false,
                ResType::Round,
                Some(WARN_DATA_TRUNCATED),
                false,
                false,
                true,
            ),
            (
                Cond::TargetDecimalLessThanOriginDecimal,
                Sign::Negative,
                false,
                ResType::Round,
                Some(WARN_DATA_TRUNCATED),
                true,
                false,
                true,
            ),
            (
                Cond::TargetDecimalLessThanOriginDecimal,
                Sign::Positive,
                true,
                ResType::Round,
                Some(WARN_DATA_TRUNCATED),
                false,
                false,
                true,
            ),
            (
                Cond::TargetDecimalLessThanOriginDecimal,
                Sign::Positive,
                true,
                ResType::Round,
                Some(WARN_DATA_TRUNCATED),
                true,
                false,
                false,
            ),
            (
                Cond::TargetDecimalLessThanOriginDecimal,
                Sign::Negative,
                true,
                ResType::Zero,
                Some(WARN_DATA_TRUNCATED),
                false,
                false,
                true,
            ),
            (
                Cond::TargetDecimalLessThanOriginDecimal,
                Sign::Negative,
                true,
                ResType::Zero,
                Some(WARN_DATA_TRUNCATED),
                true,
                false,
                false,
            ),
            // TODO: add test case for Decimal::round failure
        ];

        for (input, in_union, is_res_unsigned, base_res) in base_cs {
            for (
                cond,
                sign,
                is_unsigned,
                res_type,
                mut warning_err_code,
                in_dml,
                mut overflow_as_warning,
                mut truncate_as_warning,
            ) in cs.clone()
            {
                let (origin_flen, origin_decimal) = base_res.prec_and_frac();

                // some test case in `cs` is just for unsigned result or signed result,
                // some is just for negative/positive base_res
                //
                // in the test case above, we have negative and positive for every test case,
                // so if the sign is different from base_res's sign, we can skip it.
                if is_res_unsigned != is_unsigned {
                    continue;
                }
                let base_res = match sign {
                    Sign::Positive => {
                        if base_res.is_negative() {
                            continue;
                        } else {
                            base_res
                        }
                    }
                    Sign::Negative => {
                        if base_res.is_negative() {
                            base_res
                        } else {
                            continue;
                        }
                    }
                };

                let (res_flen, res_decimal) = match cond {
                    Cond::TargetIntPartLenLessThanOriginIntPartLen => {
                        if origin_flen - origin_decimal == 0 || origin_flen <= 1 {
                            continue;
                        }
                        (origin_flen - 1, origin_decimal)
                    }
                    Cond::TargetDecimalBiggerThanOriginDecimal => {
                        (origin_flen + 1, origin_decimal + 1)
                    }
                    Cond::TargetDecimalLessThanOriginDecimal => {
                        if origin_decimal == 0 || origin_flen <= 1 {
                            continue;
                        }
                        // TODO: if add test case for Decimal::round failure,
                        //  then should check whether this setting is right.
                        let res = base_res
                            .clone()
                            .round((origin_decimal - 1) as i8, RoundMode::HalfEven);
                        if res.is_zero() {
                            truncate_as_warning = false;
                            overflow_as_warning = false;
                            warning_err_code = None;
                        }

                        (origin_flen - 1, origin_decimal - 1)
                    }
                };
                let expect = match res_type {
                    ResType::Zero => Decimal::zero(),
                    ResType::Same => base_res,
                    ResType::TruncateToMax => max_decimal(res_flen as u8, res_decimal as u8),
                    ResType::TruncateToMin => {
                        max_or_min_dec(true, res_flen as u8, res_decimal as u8)
                    }
                    ResType::Round => {
                        let r = base_res
                            .clone()
                            .round(res_decimal as i8, RoundMode::HalfEven)
                            .unwrap();
                        if r == base_res {
                            overflow_as_warning = false;
                            truncate_as_warning = false;
                            warning_err_code = None;
                        }
                        r
                    }
                };

                let ctx_in_dml_flag = vec![Flag::IN_INSERT_STMT, Flag::IN_UPDATE_OR_DELETE_STMT];
                for in_dml_flag in ctx_in_dml_flag {
                    let (res_flen, res_decimal) = (res_flen as isize, res_decimal as isize);
                    let rft = FieldTypeConfig {
                        unsigned: is_unsigned,
                        flen: res_flen,
                        decimal: res_decimal,
                        ..FieldTypeConfig::default()
                    }
                    .into();
                    let metadata = make_metadata(in_union);
                    let extra = make_extra(&rft);

                    let mut ctx = CtxConfig {
                        overflow_as_warning,
                        truncate_as_warning,
                        in_insert_stmt: in_dml_flag == Flag::IN_INSERT_STMT,
                        in_update_or_delete_stmt: in_dml_flag == Flag::IN_UPDATE_OR_DELETE_STMT,
                        ..CtxConfig::default()
                    }
                    .into();
                    let cast_func_res =
                        cast_func(&mut ctx, &extra, &metadata, Some(&input.clone()));

                    let mut ctx = CtxConfig {
                        overflow_as_warning,
                        truncate_as_warning,
                        in_insert_stmt: in_dml_flag == Flag::IN_INSERT_STMT,
                        in_update_or_delete_stmt: in_dml_flag == Flag::IN_UPDATE_OR_DELETE_STMT,
                        ..CtxConfig::default()
                    }
                    .into();
                    let pd_res = produce_dec_with_specified_tp(&mut ctx, base_res, &rft);

                    // make log
                    let cast_func_res_log = cast_func_res
                        .as_ref()
                        .map(|x| x.as_ref().map(|x| x.to_string()));
                    let pd_res_log = pd_res.as_ref().map(|x| x.to_string());
                    let log = format!(
                            "test_func_name: {}, \
                         input: {}, base_res: {}, \
                         origin_flen: {}, origin_decimal: {}, \
                         res_flen: {}, res_decimal: {}, \
                         in_union: {}, is_unsigned: {}, in_dml: {}, in_dml_flag: {:?}, \
                         cond: {:?}, sign: {:?}, res_type: {:?}, \
                         overflow_as_warning: {}, truncate_as_warning: {}, expect_warning_err_code: {:?} \
                         expect: {}, expect_from_produce_dec_with_specified_tp(this is just for debug): {:?}, result: {:?}",
                            func_name, input_as_debug_str_func(&input), base_res,
                            origin_flen, origin_decimal,
                            res_flen, res_decimal,
                            in_union, is_unsigned, in_dml, in_dml_flag,
                            cond, sign, res_type,
                            overflow_as_warning, truncate_as_warning, warning_err_code,
                            expect.to_string(), pd_res_log, cast_func_res_log
                        );

                    check_result(Some(&expect), &cast_func_res, log.as_str());
                    check_warning(&ctx, warning_err_code, log.as_str());
                }
            }
        }
    }

    // These test depend on the correctness of
    // Decimal::from(u64), Decimal::from(i64), Decimal::from_f64(), Decimal::from_bytes()
    // Decimal::zero(), Decimal::round, max_or_min_dec, max_decimal
    #[test]
    fn test_unsigned_int_as_signed_or_unsigned_decimal() {
        test_none_with_ctx_and_extra(cast_unsigned_int_as_signed_or_unsigned_decimal);

        let cs = vec![
            (10u64 as i64, false, true, Decimal::from(10)),
            (u64::MAX as i64, false, true, Decimal::from(u64::MAX)),
            (i64::MAX as u64 as i64, false, true, Decimal::from(i64::MAX)),
        ];
        test_as_decimal_helper(
            cs,
            cast_closure_with_metadata!(cast_unsigned_int_as_signed_or_unsigned_decimal),
            |x| x.to_string(),
            "cast_unsigned_int_as_signed_or_unsigned_decimal",
        );
    }

    #[test]
    fn test_signed_int_as_unsigned_decimal() {
        test_none_with_ctx_and_extra_and_metadata(cast_signed_int_as_unsigned_decimal);

        let cs = vec![
            // (input, in_union, is_res_unsigned, base_result)

            // negative, in_union
            (-1, true, true, Decimal::zero()),
            (-10, true, true, Decimal::zero()),
            (i64::MIN, true, true, Decimal::zero()),
            // not negative, in_union
            (1, true, true, Decimal::from(1)),
            (10, true, true, Decimal::from(10)),
            (i64::MAX, true, true, Decimal::from(i64::MAX)),
            // negative, not in_union
            // FIXME: fix these case(negative to unsigned decimal, without in_union)
            //  after fix the bug of this situation(negative to unsigned decimal, without in_union)
            (-1, false, true, Decimal::from(-1i64 as u64)),
            (-10, false, true, Decimal::from(-10i64 as u64)),
            (
                i64::MIN + 1,
                false,
                true,
                Decimal::from((i64::MIN + 1) as u64),
            ),
            // not negative, not in_union
            (1, false, true, Decimal::from(1)),
            (10, false, true, Decimal::from(10)),
            (i64::MAX, false, true, Decimal::from(i64::MAX)),
        ];
        test_as_decimal_helper(
            cs,
            cast_signed_int_as_unsigned_decimal,
            |x| x.to_string(),
            "cast_signed_int_as_unsigned_decimal",
        );
    }

    #[test]
    fn test_signed_int_as_signed_decimal() {
        test_none_with_ctx_and_extra(cast_any_as_decimal::<Int>);

        let cs: Vec<(i64, bool, bool, Decimal)> = vec![
            // (input, in_union, is_res_unsigned, base_result)
            (-1, false, false, Decimal::from(-1)),
            (-10, false, false, Decimal::from(-10)),
            (i64::MIN, false, false, Decimal::from(i64::MIN)),
            (1, false, false, Decimal::from(1)),
            (10, false, false, Decimal::from(10)),
            (i64::MAX, false, false, Decimal::from(i64::MAX)),
        ];
        test_as_decimal_helper(
            cs,
            cast_closure_with_metadata!(cast_any_as_decimal::<Int>),
            |x| x.to_string(),
            "cast_signed_int_as_signed_decimal",
        );
    }

    #[test]
    fn test_real_as_decimal() {
        test_none_with_ctx_and_extra_and_metadata(cast_real_as_decimal);

        // TODO: add test case that make Decimal::from_f64 return err
        let cs = vec![
            // (input, in_union, is_res_unsigned, base_result)
            // neg and in_union
            (-10.0, true, false, Decimal::zero()),
            (i64::MIN as f64, true, false, Decimal::zero()),
            (-1.0, true, false, Decimal::zero()),
            (-0.0001, true, false, Decimal::zero()),
            // not neg and in_union
            (10.0, true, false, Decimal::from_f64(10.0).unwrap()),
            (
                i64::MAX as f64,
                true,
                false,
                Decimal::from_f64(i64::MAX as f64).unwrap(),
            ),
            (1.0, true, false, Decimal::from_f64(1.0).unwrap()),
            (0.0001, true, false, Decimal::from_f64(0.0001).unwrap()),
            // neg and not in_union
            (-10.0, false, false, Decimal::from_f64(-10.0).unwrap()),
            (
                i64::MIN as f64,
                false,
                false,
                Decimal::from_f64(i64::MIN as f64).unwrap(),
            ),
            (-1.0, false, false, Decimal::from_f64(-1.0).unwrap()),
            (-0.0001, false, false, Decimal::from_f64(-0.0001).unwrap()),
            // not neg and not in_union
            (10.0, false, false, Decimal::from_f64(10.0).unwrap()),
            (
                i64::MAX as f64,
                false,
                false,
                Decimal::from_f64(i64::MAX as f64).unwrap(),
            ),
            (1.0, false, false, Decimal::from_f64(1.0).unwrap()),
            (0.0001, false, false, Decimal::from_f64(0.0001).unwrap()),
        ];
        test_as_decimal_helper(
            cs,
            |ctx, extra, metadata, val| {
                let val = val.map(|x| Real::new(*x).unwrap());
                cast_real_as_decimal(ctx, extra, metadata, val.as_ref())
            },
            |x| x.to_string(),
            "cast_real_as_decimal",
        );
    }

    #[test]
    fn test_string_as_signed_decimal() {
        test_none_with_ctx_and_extra(cast_bytes_as_decimal);

        // TODO: add test case that make Decimal::from_bytes return err.
        let cs = vec![
            // (input, in_union, is_res_unsigned, base_result)
            // neg and in_union
            ("-10", true, false, Decimal::from(-10)),
            ("-1", true, false, Decimal::from(-1)),
            (
                "-0.001",
                true,
                false,
                Decimal::from_bytes(b"-0.001").unwrap().unwrap(),
            ),
            (
                "-9223372036854775807",
                true,
                false,
                Decimal::from(-9223372036854775807i64),
            ),
            (
                "-9223372036854775808",
                true,
                false,
                Decimal::from(-9223372036854775808i64),
            ),
            (
                "-9223372036854775808.001",
                true,
                false,
                Decimal::from_bytes(b"-9223372036854775808.001")
                    .unwrap()
                    .unwrap(),
            ),
            (
                "-9223372036854775808.002",
                true,
                false,
                Decimal::from_bytes(b"-9223372036854775808.002")
                    .unwrap()
                    .unwrap(),
            ),
            (
                "-18446744073709551615",
                true,
                false,
                Decimal::from_bytes(b"-18446744073709551615")
                    .unwrap()
                    .unwrap(),
            ),
            (
                "-18446744073709551615.001",
                true,
                false,
                Decimal::from_bytes(b"-18446744073709551615.001")
                    .unwrap()
                    .unwrap(),
            ),
            (
                "-18446744073709551615.11",
                true,
                false,
                Decimal::from_bytes(b"-18446744073709551615.11")
                    .unwrap()
                    .unwrap(),
            ),
            // not neg and in_union
            ("10", true, false, Decimal::from(10)),
            ("1", true, false, Decimal::from(1)),
            ("0.001", true, false, Decimal::from_f64(0.001).unwrap()),
            (
                "9223372036854775807",
                true,
                false,
                Decimal::from(9223372036854775807u64),
            ),
            (
                "9223372036854775808",
                true,
                false,
                Decimal::from(9223372036854775808u64),
            ),
            (
                "9223372036854775808.001",
                true,
                false,
                Decimal::from_bytes(b"9223372036854775808.001")
                    .unwrap()
                    .unwrap(),
            ),
            (
                "9223372036854775808.002",
                true,
                false,
                Decimal::from_bytes(b"9223372036854775808.002")
                    .unwrap()
                    .unwrap(),
            ),
            (
                "18446744073709551615",
                true,
                false,
                Decimal::from(18446744073709551615u64),
            ),
            (
                "18446744073709551615.001",
                true,
                false,
                Decimal::from_bytes(b"18446744073709551615.001")
                    .unwrap()
                    .unwrap(),
            ),
            (
                "18446744073709551615.11",
                true,
                false,
                Decimal::from_bytes(b"18446744073709551615.11")
                    .unwrap()
                    .unwrap(),
            ),
            // neg and not in_union
            ("-10", false, false, Decimal::from(-10)),
            ("-1", false, false, Decimal::from(-1)),
            ("-0.001", false, false, Decimal::from_f64(-0.001).unwrap()),
            (
                "-9223372036854775807",
                false,
                true,
                Decimal::from(-9223372036854775807i64),
            ),
            (
                "-9223372036854775808",
                false,
                true,
                Decimal::from(-9223372036854775808i64),
            ),
            (
                "-9223372036854775808.001",
                false,
                true,
                Decimal::from_bytes(b"-9223372036854775808.001")
                    .unwrap()
                    .unwrap(),
            ),
            (
                "-9223372036854775808.002",
                false,
                true,
                Decimal::from_bytes(b"-9223372036854775808.002")
                    .unwrap()
                    .unwrap(),
            ),
            (
                "-18446744073709551615",
                false,
                true,
                Decimal::from_bytes(b"-18446744073709551615")
                    .unwrap()
                    .unwrap(),
            ),
            (
                "-18446744073709551615.001",
                false,
                true,
                Decimal::from_bytes(b"-18446744073709551615.001")
                    .unwrap()
                    .unwrap(),
            ),
            (
                "-18446744073709551615.11",
                false,
                true,
                Decimal::from_bytes(b"-18446744073709551615.11")
                    .unwrap()
                    .unwrap(),
            ),
            // not neg and not in_union
            ("10", false, false, Decimal::from(10)),
            ("1", false, false, Decimal::from(1)),
            ("0.001", false, false, Decimal::from_f64(0.001).unwrap()),
            (
                "9223372036854775807",
                false,
                true,
                Decimal::from(9223372036854775807u64),
            ),
            (
                "9223372036854775808",
                false,
                true,
                Decimal::from(9223372036854775808u64),
            ),
            (
                "9223372036854775808.001",
                false,
                true,
                Decimal::from_bytes(b"9223372036854775808.001")
                    .unwrap()
                    .unwrap(),
            ),
            (
                "9223372036854775808.002",
                false,
                true,
                Decimal::from_bytes(b"9223372036854775808.002")
                    .unwrap()
                    .unwrap(),
            ),
            (
                "18446744073709551615",
                false,
                true,
                Decimal::from(18446744073709551615u64),
            ),
            (
                "18446744073709551615.001",
                false,
                true,
                Decimal::from_bytes(b"18446744073709551615.001")
                    .unwrap()
                    .unwrap(),
            ),
            (
                "18446744073709551615.11",
                false,
                true,
                Decimal::from_bytes(b"18446744073709551615.11")
                    .unwrap()
                    .unwrap(),
            ),
            // can not convert to decimal
            ("abcde", false, false, Decimal::zero()),
            ("", false, false, Decimal::zero()),
            ("s", false, false, Decimal::zero()),
            ("abcde", true, false, Decimal::zero()),
            ("", true, false, Decimal::zero()),
            ("s", true, false, Decimal::zero()),
            ("abcde", false, true, Decimal::zero()),
            ("", false, true, Decimal::zero()),
            ("s", false, true, Decimal::zero()),
            ("abcde", true, true, Decimal::zero()),
            ("", true, true, Decimal::zero()),
            ("s", true, true, Decimal::zero()),
        ];

        test_as_decimal_helper(
            cs,
            |ctx, extra, _, val| {
                let val = val.map(|x| x.as_bytes());
                cast_bytes_as_decimal(ctx, extra, val)
            },
            |x| (*x).to_string(),
            "cast_string_as_signed_decimal",
        )
    }

    #[test]
    fn test_string_as_unsigned_decimal() {
        test_none_with_ctx_and_extra_and_metadata(cast_string_as_unsigned_decimal);

        let cs = vec![
            // (input, in_union, is_res_unsigned, base_result)
            // neg and in_union
            ("-10", true, true, Decimal::zero()),
            ("-1", true, true, Decimal::zero()),
            ("-0.001", true, true, Decimal::zero()),
            ("-9223372036854775807", true, true, Decimal::zero()),
            ("-9223372036854775808", true, true, Decimal::zero()),
            ("-9223372036854775808.001", true, true, Decimal::zero()),
            ("-9223372036854775808.002", true, true, Decimal::zero()),
            ("-18446744073709551615", true, true, Decimal::zero()),
            ("-18446744073709551615.001", true, true, Decimal::zero()),
            ("-18446744073709551615.11", true, true, Decimal::zero()),
            // not neg and in_union
            ("10", true, true, Decimal::from(10)),
            ("1", true, true, Decimal::from(1)),
            ("0.001", true, true, Decimal::from_f64(0.001).unwrap()),
            (
                "9223372036854775807",
                true,
                true,
                Decimal::from(9223372036854775807u64),
            ),
            (
                "9223372036854775808",
                true,
                true,
                Decimal::from(9223372036854775808u64),
            ),
            (
                "9223372036854775808.001",
                true,
                true,
                Decimal::from_bytes(b"9223372036854775808.001")
                    .unwrap()
                    .unwrap(),
            ),
            (
                "9223372036854775808.002",
                true,
                true,
                Decimal::from_bytes(b"9223372036854775808.002")
                    .unwrap()
                    .unwrap(),
            ),
            (
                "18446744073709551615",
                true,
                true,
                Decimal::from(18446744073709551615u64),
            ),
            (
                "18446744073709551615.001",
                true,
                true,
                Decimal::from_bytes(b"18446744073709551615.001")
                    .unwrap()
                    .unwrap(),
            ),
            (
                "18446744073709551615.11",
                true,
                true,
                Decimal::from_bytes(b"18446744073709551615.11")
                    .unwrap()
                    .unwrap(),
            ),
            // neg and not in_union
            ("-10", false, true, Decimal::from(-10)),
            ("-1", false, true, Decimal::from(-1)),
            ("-0.001", false, true, Decimal::from_f64(-0.001).unwrap()),
            (
                "-9223372036854775807",
                false,
                true,
                Decimal::from(-9223372036854775807i64),
            ),
            (
                "-9223372036854775808",
                false,
                true,
                Decimal::from(-9223372036854775808i64),
            ),
            (
                "-9223372036854775808.001",
                false,
                true,
                Decimal::from_bytes(b"-9223372036854775808.001")
                    .unwrap()
                    .unwrap(),
            ),
            (
                "-9223372036854775808.002",
                false,
                true,
                Decimal::from_bytes(b"-9223372036854775808.002")
                    .unwrap()
                    .unwrap(),
            ),
            (
                "-18446744073709551615",
                false,
                true,
                Decimal::from_bytes(b"-18446744073709551615")
                    .unwrap()
                    .unwrap(),
            ),
            (
                "-18446744073709551615.001",
                false,
                true,
                Decimal::from_bytes(b"-18446744073709551615.001")
                    .unwrap()
                    .unwrap(),
            ),
            (
                "-18446744073709551615.11",
                false,
                true,
                Decimal::from_bytes(b"-18446744073709551615.11")
                    .unwrap()
                    .unwrap(),
            ),
            // not neg and not in_union
            ("10", false, true, Decimal::from(10)),
            ("1", false, true, Decimal::from(1)),
            ("0.001", false, true, Decimal::from_f64(0.001).unwrap()),
            (
                "9223372036854775807",
                false,
                true,
                Decimal::from(9223372036854775807u64),
            ),
            (
                "9223372036854775808",
                false,
                true,
                Decimal::from(9223372036854775808u64),
            ),
            (
                "9223372036854775808.001",
                false,
                true,
                Decimal::from_bytes(b"9223372036854775808.001")
                    .unwrap()
                    .unwrap(),
            ),
            (
                "9223372036854775808.002",
                false,
                true,
                Decimal::from_bytes(b"9223372036854775808.002")
                    .unwrap()
                    .unwrap(),
            ),
            (
                "18446744073709551615",
                false,
                true,
                Decimal::from(18446744073709551615u64),
            ),
            (
                "18446744073709551615.001",
                false,
                true,
                Decimal::from_bytes(b"18446744073709551615.001")
                    .unwrap()
                    .unwrap(),
            ),
            (
                "18446744073709551615.11",
                false,
                true,
                Decimal::from_bytes(b"18446744073709551615.11")
                    .unwrap()
                    .unwrap(),
            ),
            // can not convert to decimal
            ("abcde", false, false, Decimal::zero()),
            ("", false, false, Decimal::zero()),
            ("s", false, false, Decimal::zero()),
            ("abcde", true, false, Decimal::zero()),
            ("", true, false, Decimal::zero()),
            ("s", true, false, Decimal::zero()),
            ("abcde", false, true, Decimal::zero()),
            ("", false, true, Decimal::zero()),
            ("s", false, true, Decimal::zero()),
            ("abcde", true, true, Decimal::zero()),
            ("", true, true, Decimal::zero()),
            ("s", true, true, Decimal::zero()),
        ];

        test_as_decimal_helper(
            cs,
            |ctx, extra, metadata, val| {
                let val = val.map(|x| x.as_bytes());
                cast_string_as_unsigned_decimal(ctx, extra, metadata, val)
            },
            |x| (*x).to_string(),
            "cast_string_as_unsigned_decimal",
        );
    }

    #[test]
    fn test_decimal_as_signed_decimal() {
        test_none_with_ctx_and_extra(cast_decimal_as_signed_decimal);

        // in_union and result is unsigned
        let cs = vec![
            // (input, in_union, is_res_unsigned, base_result)

            // in_union
            (Decimal::zero(), true, false, Decimal::zero()),
            (
                Decimal::from_f64(-10f64).unwrap(),
                true,
                false,
                Decimal::from_f64(-10f64).unwrap(),
            ),
            (
                Decimal::from(i64::MIN),
                true,
                false,
                Decimal::from(i64::MIN),
            ),
            (
                Decimal::from(i64::MAX),
                true,
                false,
                Decimal::from(i64::MAX),
            ),
            (
                Decimal::from(u64::MAX),
                true,
                false,
                Decimal::from(u64::MAX),
            ),
            // not in_union
            (Decimal::zero(), false, false, Decimal::zero()),
            (
                Decimal::from_f64(-10f64).unwrap(),
                false,
                false,
                Decimal::from_f64(-10f64).unwrap(),
            ),
            (
                Decimal::from(i64::MIN),
                false,
                false,
                Decimal::from(i64::MIN),
            ),
            (
                Decimal::from(i64::MAX),
                false,
                false,
                Decimal::from(i64::MAX),
            ),
            (
                Decimal::from(u64::MAX),
                false,
                false,
                Decimal::from(u64::MAX),
            ),
        ];

        test_as_decimal_helper(
            cs,
            cast_closure_with_metadata!(cast_decimal_as_signed_decimal),
            |x| x.to_string(),
            "cast_decimal_as_signed_decimal",
        );
    }

    #[test]
    fn test_decimal_as_unsigned_decimal() {
        test_none_with_ctx_and_extra_and_metadata(cast_decimal_as_unsigned_decimal);

        // in_union and result is unsigned
        let cs = vec![
            // (input, in_union, is_res_unsigned, base_result)

            // neg and in_union
            (
                Decimal::from_f64(-10f64).unwrap(),
                true,
                true,
                Decimal::zero(),
            ),
            (Decimal::from(i64::MIN), true, true, Decimal::zero()),
            // not neg and in_union
            (Decimal::zero(), true, true, Decimal::zero()),
            (
                Decimal::from_f64(10f64).unwrap(),
                true,
                true,
                Decimal::from_f64(10f64).unwrap(),
            ),
            (Decimal::from(i64::MAX), true, true, Decimal::from(i64::MAX)),
            (Decimal::from(u64::MAX), true, true, Decimal::from(u64::MAX)),
            // neg and not in_union
            (
                Decimal::from_f64(-10f64).unwrap(),
                false,
                true,
                Decimal::from_f64(-10f64).unwrap(),
            ),
            (
                Decimal::from(i64::MIN),
                false,
                true,
                Decimal::from(i64::MIN),
            ),
            // not neg and not in_union
            (Decimal::zero(), true, true, Decimal::zero()),
            (
                Decimal::from_f64(10f64).unwrap(),
                true,
                true,
                Decimal::from_f64(10f64).unwrap(),
            ),
            (Decimal::from(i64::MAX), true, true, Decimal::from(i64::MAX)),
            (Decimal::from(u64::MAX), true, true, Decimal::from(u64::MAX)),
        ];

        test_as_decimal_helper(
            cs,
            cast_decimal_as_unsigned_decimal,
            |x| x.to_string(),
            "cast_decimal_as_unsigned_decimal",
        );
    }

    #[test]
    fn test_time_as_decimal() {
        test_none_with_ctx_and_extra(cast_any_as_decimal::<Time>);
        let mut ctx = EvalContext::default();

        // TODO: add more test case
        let cs: Vec<(Time, bool, bool, Decimal)> = vec![
            // (cast_func_input, in_union, is_res_unsigned, base_result)
            (
                Time::parse_datetime(&mut ctx, "2000-01-01T12:13:14", 0, false).unwrap(),
                false,
                false,
                Decimal::from_bytes(b"20000101121314").unwrap().unwrap(),
            ),
            (
                Time::parse_datetime(&mut ctx, "2000-01-01T12:13:14.6666", 0, true).unwrap(),
                false,
                false,
                Decimal::from_bytes(b"20000101121315").unwrap().unwrap(),
            ),
        ];
        test_as_decimal_helper(
            cs,
            cast_closure_with_metadata!(cast_any_as_decimal::<Time>),
            |x| x.to_string(),
            "cast_time_as_decimal",
        )
    }

    #[test]
    fn test_duration_as_decimal() {
        test_none_with_ctx_and_extra(cast_any_as_decimal::<Duration>);
        let mut ctx = EvalContext::default();
        // TODO: add more test case
        let cs: Vec<(Duration, bool, bool, Decimal)> = vec![
            // (input, in_union, is_res_unsigned, base_result)
            (
                Duration::parse(&mut ctx, "17:51:04.78", 2).unwrap(),
                false,
                false,
                Decimal::from_f64(175104.78).unwrap(),
            ),
            (
                Duration::parse(&mut ctx, "-17:51:04.78", 2).unwrap(),
                false,
                false,
                Decimal::from_f64(-175104.78).unwrap(),
            ),
            (
                Duration::parse(&mut ctx, "17:51:04.78", 0).unwrap(),
                false,
                false,
                Decimal::from(175105),
            ),
            (
                Duration::parse(&mut ctx, "-17:51:04.78", 0).unwrap(),
                false,
                false,
                Decimal::from(-175105),
            ),
        ];
        test_as_decimal_helper(
            cs,
            cast_closure_with_metadata!(cast_any_as_decimal::<Duration>),
            |x| x.to_string(),
            "cast_duration_as_int",
        )
    }

    #[test]
    fn test_json_as_decimal() {
        test_none_with_ctx_and_extra(cast_json_as_decimal);

        // TODO: add test case that make Decimal::from_str failed
        let cs: Vec<(Json, bool, bool, Decimal)> = vec![
            (
                Json::from_i64(10).unwrap(),
                false,
                false,
                Decimal::from_f64(10f64).unwrap(),
            ),
            (
                Json::from_i64(i64::MAX).unwrap(),
                false,
                false,
                Decimal::from_f64(i64::MAX as f64).unwrap(),
            ),
            (
                Json::from_i64(i64::MIN).unwrap(),
                false,
                false,
                Decimal::from_f64(i64::MIN as f64).unwrap(),
            ),
            (Json::from_u64(0).unwrap(), false, false, Decimal::zero()),
            (
                Json::from_u64(i64::MAX as u64).unwrap(),
                false,
                false,
                Decimal::from_f64(i64::MAX as f64).unwrap(),
            ),
            (
                Json::from_u64(u64::MAX).unwrap(),
                false,
                false,
                Decimal::from_f64(u64::MAX as f64).unwrap(),
            ),
            (
                Json::from_f64(i64::MAX as f64).unwrap(),
                false,
                false,
                Decimal::from_f64(i64::MAX as f64).unwrap(),
            ),
            (
                Json::from_f64(i64::MIN as f64).unwrap(),
                false,
                false,
                Decimal::from_f64(i64::MIN as f64).unwrap(),
            ),
            (
                Json::from_f64(u64::MAX as f64).unwrap(),
                false,
                false,
                Decimal::from_f64(u64::MAX as f64).unwrap(),
            ),
            (
                Json::from_string("10.0".to_string()).unwrap(),
                false,
                false,
                Decimal::from_bytes(b"10.0").unwrap().unwrap(),
            ),
            (
                Json::from_string("-10.0".to_string()).unwrap(),
                false,
                false,
                Decimal::from_bytes(b"-10.0").unwrap().unwrap(),
            ),
            (
                Json::from_string("9999999999999999999".to_string()).unwrap(),
                false,
                false,
                Decimal::from_bytes(b"9999999999999999999")
                    .unwrap()
                    .unwrap(),
            ),
            (
                Json::from_string("-9999999999999999999".to_string()).unwrap(),
                false,
                false,
                Decimal::from_bytes(b"-9999999999999999999")
                    .unwrap()
                    .unwrap(),
            ),
            (
                Json::from_bool(true).unwrap(),
                false,
                false,
                Decimal::from_f64(1f64).unwrap(),
            ),
            (
                Json::from_bool(false).unwrap(),
                false,
                false,
                Decimal::zero(),
            ),
            (Json::none().unwrap(), false, false, Decimal::zero()),
        ];

        test_as_decimal_helper(
            cs,
            |ctx, extra, _, val| cast_json_as_decimal(ctx, extra, val.map(|x| x.as_ref())),
            |x| x.to_string(),
            "cast_json_as_decimal",
        );
    }

    #[test]
    fn test_truncate_when_cast_json_object_or_array_as_decimal() {
        test_none_with_ctx(cast_any_as_any::<Real, Int>);

        let cs = vec![
            // (origin, result, errcode)
            (
                Json::from_object(BTreeMap::default()).unwrap(),
                Decimal::zero(),
                ERR_TRUNCATE_WRONG_VALUE,
            ),
            (
                Json::from_array(vec![]).unwrap(),
                Decimal::zero(),
                ERR_TRUNCATE_WRONG_VALUE,
            ),
        ];

        for (input, result, errcode) in cs {
            let mut ctx = CtxConfig {
                truncate_as_warning: true,
                ..CtxConfig::default()
            }
            .into();

            let rft = FieldTypeConfig::default().into();
            let extra = make_extra(&rft);
            let r = cast_json_as_decimal(&mut ctx, &extra, Some(input.as_ref()));

            let log = make_log(&input, &result, &r);
            check_result(Some(&result), &r, log.as_str());
            check_warning(&ctx, Some(errcode), log.as_str());
        }
    }

    #[test]
    fn test_int_as_duration() {
        // None
        {
            let output: Option<Real> = RpnFnScalarEvaluator::new()
                .push_param(ScalarValue::Bytes(None))
                .evaluate(ScalarFuncSig::CastIntAsDuration)
                .unwrap();
            assert_eq!(output, None);
        }

        let mut ctx = EvalContext::default();

        struct TestCase(
            i64,
            isize,
            tidb_query_datatype::codec::Result<Option<Duration>>,
            bool,
            bool,
        );
        // This case copy from Duration.rs::tests::test_from_i64
        let cs: Vec<TestCase> = vec![
            // (input, fsp, expect, overflow, truncated)
            TestCase(
                101010,
                0,
                Ok(Some(Duration::parse(&mut ctx, "10:10:10", 0).unwrap())),
                false,
                false,
            ),
            TestCase(
                101010,
                5,
                Ok(Some(Duration::parse(&mut ctx, "10:10:10", 5).unwrap())),
                false,
                false,
            ),
            TestCase(
                8385959,
                0,
                Ok(Some(Duration::parse(&mut ctx, "838:59:59", 0).unwrap())),
                false,
                false,
            ),
            TestCase(
                8385959,
                6,
                Ok(Some(Duration::parse(&mut ctx, "838:59:59", 6).unwrap())),
                false,
                false,
            ),
            TestCase(
                -101010,
                0,
                Ok(Some(Duration::parse(&mut ctx, "-10:10:10", 0).unwrap())),
                false,
                false,
            ),
            TestCase(
                -101010,
                5,
                Ok(Some(Duration::parse(&mut ctx, "-10:10:10", 5).unwrap())),
                false,
                false,
            ),
            TestCase(
                -8385959,
                0,
                Ok(Some(Duration::parse(&mut ctx, "-838:59:59", 0).unwrap())),
                false,
                false,
            ),
            TestCase(
                -8385959,
                6,
                Ok(Some(Duration::parse(&mut ctx, "-838:59:59", 6).unwrap())),
                false,
                false,
            ),
            // overflow as warning
            TestCase(8385960, 0, Ok(None), true, false),
            TestCase(-8385960, 0, Ok(None), true, false),
            // will truncated
            TestCase(8376049, 0, Ok(None), false, true),
            TestCase(8375960, 0, Ok(None), false, true),
            TestCase(-8376049, 0, Ok(None), false, true),
            TestCase(2002073, 0, Ok(None), false, true),
            TestCase(2007320, 0, Ok(None), false, true),
            TestCase(-2002073, 0, Ok(None), false, true),
            TestCase(-2007320, 0, Ok(None), false, true),
            TestCase(
                10000000000,
                0,
                Ok(Some(Duration::parse(&mut ctx, "0:0:0", 0).unwrap())),
                false,
                false,
            ),
            TestCase(
                10000235959,
                0,
                Ok(Some(Duration::parse(&mut ctx, "23:59:59", 0).unwrap())),
                false,
                false,
            ),
            TestCase(-10000235959, 0, Ok(None), true, false),
        ];

        for TestCase(input, fsp, expected, overflow, truncated) in cs {
            let (result, ctx) = RpnFnScalarEvaluator::new()
                .context(CtxConfig {
                    overflow_as_warning: true,
                    truncate_as_warning: true,
                    ..CtxConfig::default()
                })
                .push_param(input)
                .evaluate_raw(
                    FieldTypeConfig {
                        tp: Some(FieldTypeTp::Duration),
                        decimal: fsp,
                        ..FieldTypeConfig::default()
                    },
                    ScalarFuncSig::CastIntAsDuration,
                );
            match expected {
                Ok(expected) => {
                    let result: Option<Duration> = result.unwrap().into();
                    assert_eq!(
                        result, expected,
                        "input:{:?}, expected:{:?}, got:{:?}",
                        input, expected, result,
                    );
                }
                Err(_) => {
                    assert!(
                        result.is_err(),
                        "input:{:?}, expected err:{:?}, got:{:?}",
                        input,
                        expected,
                        result
                    );
                }
            }
            if overflow {
                assert_eq!(ctx.warnings.warning_cnt, 1);
                assert_eq!(ctx.warnings.warnings[0].get_code(), ERR_DATA_OUT_OF_RANGE);
            }
            if truncated {
                assert_eq!(ctx.warnings.warning_cnt, 1);
                assert_eq!(
                    ctx.warnings.warnings[0].get_code(),
                    ERR_TRUNCATE_WRONG_VALUE
                );
            }
        }
    }

    fn test_as_duration_helper<T: Clone, FnCast>(
        base_cs: Vec<T>,
        func_to_cast_str: impl Fn(T) -> String,
        func_to_debug_str: impl Fn(T) -> String,
        func_cast: FnCast,
        func_name: &str,
    ) where
        FnCast: Fn(&mut EvalContext, &RpnFnCallExtra, Option<T>) -> Result<Option<Duration>>,
    {
        // cast_real_as_duration call `Duration::parse`, directly,
        // and `Duration::parse`, is test in duration.rs.
        // Our test here is to make sure that the result is same as calling `Duration::parse`,
        // no matter whether call_real_as_duration call `Duration::parse`, directly.
        for val in base_cs {
            for fsp in MIN_FSP..=MAX_FSP {
                let mut ctx = CtxConfig {
                    overflow_as_warning: true,
                    truncate_as_warning: true,
                    ..CtxConfig::default()
                }
                .into();
                let rft = FieldTypeConfig {
                    decimal: fsp as isize,
                    ..FieldTypeConfig::default()
                }
                .into();
                let extra = make_extra(&rft);

                let result = func_cast(&mut ctx, &extra, Some(val.clone()));

                let val_str = func_to_cast_str(val.clone());
                let base_expect = Duration::parse(&mut ctx, &val_str, fsp);

                // make log
                let result_str = result.as_ref().map(|x| x.map(|x| x.to_string()));

                match base_expect {
                    Err(e) => match e.code() {
                        ERR_DATA_OUT_OF_RANGE => {
                            let log = format!(
                                "func_name:{}, input: {}, fsp: {}, output: {:?}, expect: {}, expect_warn: {}",
                                func_name, func_to_debug_str(val.clone()), fsp, result_str, Duration::zero(), ERR_DATA_OUT_OF_RANGE
                            );
                            check_overflow(&ctx, true, log.as_str());
                            check_result(None, &result, log.as_str());
                        }
                        ERR_TRUNCATE_WRONG_VALUE => {
                            let log = format!(
                                "func_name:{}, input: {}, fsp: {}, output: {:?}, output_warn: {:?}, expect: {}, expect_warn: {}",
                                func_name, func_to_debug_str(val.clone()), fsp, result_str, ctx.warnings.warnings, Duration::zero(), WARN_DATA_TRUNCATED
                            );
                            check_warning(&ctx, Some(ERR_TRUNCATE_WRONG_VALUE), log.as_str());
                            check_result(None, &result, log.as_str());
                        }
                        _ => {
                            let expect_err: tidb_query_common::error::Error = e.into();
                            let log = format!(
                                "func_name:{}, input: {}, fsp: {}, output: {:?}, output_warn: {:?}, expect: {:?}",
                                func_name, func_to_debug_str(val.clone()), fsp, result_str, ctx.warnings.warnings, expect_err
                            );
                            assert!(result.is_err(), "log: {}", log)
                        }
                    },
                    Ok(v) => {
                        let log = format!(
                            "func_name:{}, input: {}, fsp: {}, output: {:?}, output_warn: {:?}, expect: {:?}",
                            func_name, func_to_debug_str(val.clone()), fsp, result_str, ctx.warnings.warnings, v
                        );
                        check_result(Some(&v), &result, log.as_str())
                    }
                }
            }
        }
    }

    #[test]
    fn test_real_as_duration() {
        test_none_with_ctx_and_extra(cast_real_as_duration);

        let cs: Vec<f64> = vec![
            101112.0,
            101112.123456,
            1112.0,
            12.0,
            -0.123,
            12345.0,
            -123.0,
            -23.0,
        ];

        test_as_duration_helper(
            cs,
            |x| x.to_string(),
            |x| x.to_string(),
            |ctx, extra, val| {
                let val = val.map(|x| Real::new(x).unwrap());
                cast_real_as_duration(ctx, extra, val.as_ref())
            },
            "cast_real_as_duration",
        )
    }

    #[test]
    fn test_bytes_as_duration() {
        test_none_with_ctx_and_extra(cast_bytes_as_duration);

        let cs: Vec<BytesRef> = vec![
            b"17:51:04.78",
            b"-17:51:04.78",
            b"17:51:04.78",
            b"-17:51:04.78",
        ];

        test_as_duration_helper(
            cs,
            |x| String::from_utf8_lossy(x).to_string(),
            |x| String::from_utf8_lossy(x).to_string(),
            cast_bytes_as_duration,
            "cast_bytes_as_duration",
        );
    }

    #[test]
    fn test_decimal_as_duration() {
        test_none_with_ctx_and_extra(cast_decimal_as_duration);

        let cs = vec![
            Decimal::from(i64::MIN),
            Decimal::from(i64::MAX),
            Decimal::from(u64::MAX),
            Decimal::zero(),
            Decimal::from_bytes(b"-9223372036854775808")
                .unwrap()
                .unwrap(),
            Decimal::from_bytes(b"9223372036854775808")
                .unwrap()
                .unwrap(),
            Decimal::from_bytes(b"-9223372036854775809")
                .unwrap()
                .unwrap(),
            Decimal::from_bytes(b"9223372036854775809")
                .unwrap()
                .unwrap(),
            Decimal::from_bytes(b"-18446744073709551615")
                .unwrap()
                .unwrap(),
            Decimal::from_bytes(b"18446744073709551615")
                .unwrap()
                .unwrap(),
            Decimal::from_bytes(b"-18446744073709551616")
                .unwrap()
                .unwrap(),
            Decimal::from_bytes(b"18446744073709551616")
                .unwrap()
                .unwrap(),
            Decimal::from_bytes(b"-184467440737095516160")
                .unwrap()
                .unwrap(),
            Decimal::from_bytes(b"184467440737095516160")
                .unwrap()
                .unwrap(),
            Decimal::from_bytes(b"-99999999999999999999999999999999")
                .unwrap()
                .unwrap(),
            Decimal::from_bytes(b"99999999999999999999999999999999")
                .unwrap()
                .unwrap(),
        ];

        let cs_ref: Vec<&Decimal> = cs.iter().collect();

        test_as_duration_helper(
            cs_ref,
            |x| x.to_string(),
            |x| x.to_string(),
            cast_decimal_as_duration,
            "cast_decimal_as_duration",
        );
    }

    #[test]
    fn test_time_as_duration() {
        test_none_with_ctx_and_extra(cast_time_as_duration);

        // copy from test_convert_to_duration
        let cs = vec![
            // (input, input's fsp, output's fsp, output)
            ("2012-12-31 11:30:45.123456", 4, 0, "11:30:45"),
            ("2012-12-31 11:30:45.123456", 4, 1, "11:30:45.1"),
            ("2012-12-31 11:30:45.123456", 4, 2, "11:30:45.12"),
            ("2012-12-31 11:30:45.123456", 4, 3, "11:30:45.124"),
            ("2012-12-31 11:30:45.123456", 4, 4, "11:30:45.1235"),
            ("2012-12-31 11:30:45.123456", 4, 5, "11:30:45.12350"),
            ("2012-12-31 11:30:45.123456", 4, 6, "11:30:45.123500"),
            ("2012-12-31 11:30:45.123456", 6, 0, "11:30:45"),
            ("2012-12-31 11:30:45.123456", 6, 1, "11:30:45.1"),
            ("2012-12-31 11:30:45.123456", 6, 2, "11:30:45.12"),
            ("2012-12-31 11:30:45.123456", 6, 3, "11:30:45.123"),
            ("2012-12-31 11:30:45.123456", 6, 4, "11:30:45.1235"),
            ("2012-12-31 11:30:45.123456", 6, 5, "11:30:45.12346"),
            ("2012-12-31 11:30:45.123456", 6, 6, "11:30:45.123456"),
            ("2012-12-31 11:30:45.123456", 0, 0, "11:30:45"),
            ("2012-12-31 11:30:45.123456", 0, 1, "11:30:45.0"),
            ("2012-12-31 11:30:45.123456", 0, 2, "11:30:45.00"),
            ("2012-12-31 11:30:45.123456", 0, 3, "11:30:45.000"),
            ("2012-12-31 11:30:45.123456", 0, 4, "11:30:45.0000"),
            ("2012-12-31 11:30:45.123456", 0, 5, "11:30:45.00000"),
            ("2012-12-31 11:30:45.123456", 0, 6, "11:30:45.000000"),
            ("0000-00-00 00:00:00", 6, 0, "00:00:00"),
            ("0000-00-00 00:00:00", 6, 1, "00:00:00.0"),
            ("0000-00-00 00:00:00", 6, 2, "00:00:00.00"),
            ("0000-00-00 00:00:00", 6, 3, "00:00:00.000"),
            ("0000-00-00 00:00:00", 6, 4, "00:00:00.0000"),
            ("0000-00-00 00:00:00", 6, 5, "00:00:00.00000"),
            ("0000-00-00 00:00:00", 6, 6, "00:00:00.000000"),
        ];
        for (s, fsp, expect_fsp, expect) in cs {
            let mut ctx = EvalContext::default();

            let rft = FieldTypeConfig {
                decimal: expect_fsp,
                ..FieldTypeConfig::default()
            }
            .into();
            let extra = make_extra(&rft);

            let input_time = Time::parse_datetime(&mut ctx, s, fsp, true).unwrap();
            let expect_time = Duration::parse(&mut ctx, expect, expect_fsp as i8).unwrap();
            let result = cast_time_as_duration(&mut ctx, &extra, Some(&input_time));
            let result_str = result.as_ref().map(|x| x.as_ref().map(|x| x.to_string()));
            let log = format!(
                "input: {}, fsp: {}, expect_fsp: {}, expect: {}, output: {:?}",
                s, fsp, expect_fsp, expect, result_str,
            );
            check_result(Some(&expect_time), &result, log.as_str());
        }
    }

    #[test]
    fn test_duration_as_duration() {
        test_none_with_extra(cast_duration_as_duration);

        let cs = vec![
            ("11:30:45.123456", 6, 0, "11:30:45"),
            ("11:30:45.123456", 6, 1, "11:30:45.1"),
            ("11:30:45.123456", 6, 2, "11:30:45.12"),
            ("11:30:45.123456", 6, 3, "11:30:45.123"),
            ("11:30:45.123456", 6, 4, "11:30:45.1235"),
            ("11:30:45.123456", 6, 5, "11:30:45.12346"),
            ("11:30:45.123456", 6, 6, "11:30:45.123456"),
        ];

        for (input, input_fsp, output_fsp, expect) in cs {
            let rft = FieldTypeConfig {
                decimal: output_fsp as isize,
                ..FieldTypeConfig::default()
            }
            .into();
            let extra = make_extra(&rft);

            let mut ctx = EvalContext::default();
            let dur = Duration::parse(&mut ctx, input, input_fsp).unwrap();
            let expect = Duration::parse(&mut ctx, expect, output_fsp).unwrap();
            let r = cast_duration_as_duration(&extra, Some(&dur));

            let result_str = r.as_ref().map(|x| x.map(|x| x.to_string()));
            let log = format!(
                "input: {}, input_fsp: {}, output_fsp: {}, expect: {}, output: {:?}",
                input, input_fsp, output_fsp, expect, result_str
            );
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_json_as_duration() {
        test_none_with_ctx_and_extra(cast_json_as_duration);

        // the case that Json::unquote failed had be tested by test_json_unquote

        let cs = vec![
            Json::from_object(BTreeMap::default()).unwrap(),
            Json::from_array(vec![]).unwrap(),
            Json::from_i64(10).unwrap(),
            Json::from_i64(i64::MAX).unwrap(),
            Json::from_i64(i64::MIN).unwrap(),
            Json::from_u64(0).unwrap(),
            Json::from_u64(u64::MAX).unwrap(),
            Json::from_f64(10.5).unwrap(),
            Json::from_f64(10.4).unwrap(),
            Json::from_f64(-10.4).unwrap(),
            Json::from_f64(-10.5).unwrap(),
            Json::from_f64(i64::MIN as u64 as f64).unwrap(),
            Json::from_f64(i64::MAX as u64 as f64).unwrap(),
            Json::from_f64(i64::MIN as u64 as f64).unwrap(),
            Json::from_f64(i64::MIN as f64).unwrap(),
            Json::from_f64(((1u64 << 63) + (1u64 << 62)) as u64 as f64).unwrap(),
            Json::from_f64(-((1u64 << 63) as f64 + (1u64 << 62) as f64)).unwrap(),
            Json::from_f64(f64::from(f32::MIN)).unwrap(),
            Json::from_f64(f64::from(f32::MAX)).unwrap(),
            Json::from_f64(f64::MAX).unwrap(),
            Json::from_f64(f64::MAX).unwrap(),
            Json::from_string(String::from("10.0")).unwrap(),
            Json::from_string(String::from(
                "999999999999999999999999999999999999999999999999",
            ))
            .unwrap(),
            Json::from_string(String::from(
                "-999999999999999999999999999999999999999999999999",
            ))
            .unwrap(),
            Json::from_string(String::from(
                "99999999999999999999999999999999999999999999999aabcde9",
            ))
            .unwrap(),
            Json::from_string(String::from(
                "-99999999999999999999999999999999999999999999999aabcde9",
            ))
            .unwrap(),
            Json::from_bool(true).unwrap(),
            Json::from_bool(false).unwrap(),
            Json::none().unwrap(),
        ];

        let cs_ref: Vec<JsonRef> = cs.iter().map(|x| x.as_ref()).collect();
        test_as_duration_helper(
            cs_ref,
            |x| x.unquote().unwrap(),
            |x| format!("{:?}", x),
            cast_json_as_duration,
            "cast_json_as_duration",
        );
    }

    #[test]
    fn test_int_as_json() {
        test_none_with_ctx(cast_any_as_json::<Int>);

        let cs = vec![
            (i64::MIN, Json::from_i64(i64::MIN).unwrap()),
            (0, Json::from_i64(0).unwrap()),
            (i64::MAX, Json::from_i64(i64::MAX).unwrap()),
        ];
        for (input, expect) in cs {
            let mut ctx = EvalContext::default();
            let r = cast_any_as_json::<Int>(&mut ctx, Some(&input));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_uint_as_json() {
        test_none_with_nothing(cast_uint_as_json);

        let cs = vec![
            (u64::MAX, Json::from_u64(u64::MAX).unwrap()),
            (0, Json::from_u64(0).unwrap()),
            (i64::MAX as u64, Json::from_u64(i64::MAX as u64).unwrap()),
        ];
        for (input, expect) in cs {
            let r = cast_uint_as_json(Some(&(input as i64)));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_bool_as_json() {
        test_none_with_nothing(cast_bool_as_json);

        let cs = vec![
            (0, Json::from_bool(false).unwrap()),
            (i64::MIN, Json::from_bool(true).unwrap()),
            (i64::MAX, Json::from_bool(true).unwrap()),
        ];
        for (input, expect) in cs {
            let result = cast_bool_as_json(Some(&input));
            let log = make_log(&input, &expect, &result);
            check_result(Some(&expect), &result, log.as_str());
        }
    }

    #[test]
    fn test_real_as_json() {
        test_none_with_ctx(cast_any_as_json::<Real>);

        let cs = vec![
            (
                f64::from(f32::MAX),
                Json::from_f64(f64::from(f32::MAX)).unwrap(),
            ),
            (
                f64::from(f32::MIN),
                Json::from_f64(f64::from(f32::MIN)).unwrap(),
            ),
            (f64::MAX, Json::from_f64(f64::MAX).unwrap()),
            (f64::MIN, Json::from_f64(f64::MIN).unwrap()),
        ];
        for (input, expect) in cs {
            let mut ctx = EvalContext::default();
            let r = cast_any_as_json::<Real>(&mut ctx, Real::new(input).as_ref().ok());
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_string_as_json() {
        test_none_with_extra(cast_string_as_json);

        let mut jo1: BTreeMap<String, Json> = BTreeMap::new();
        jo1.insert(
            String::from("a"),
            Json::from_string(String::from("b")).unwrap(),
        );
        // HasParseToJSONFlag
        let cs = vec![
            (
                "{\"a\": \"b\"}".to_string(),
                Json::from_object(jo1).unwrap(),
                true,
            ),
            (
                "{}".to_string(),
                Json::from_object(BTreeMap::new()).unwrap(),
                true,
            ),
            (
                "[1, 2, 3]".to_string(),
                Json::from_array(vec![
                    Json::from_i64(1).unwrap(),
                    Json::from_i64(2).unwrap(),
                    Json::from_i64(3).unwrap(),
                ])
                .unwrap(),
                true,
            ),
            (
                "[]".to_string(),
                Json::from_array(Vec::new()).unwrap(),
                true,
            ),
            (
                "9223372036854775807".to_string(),
                Json::from_i64(9223372036854775807).unwrap(),
                true,
            ),
            (
                "-9223372036854775808".to_string(),
                Json::from_i64(-9223372036854775808).unwrap(),
                true,
            ),
            (
                "18446744073709551615".to_string(),
                Json::from_f64(18446744073709552000.0).unwrap(),
                true,
            ),
            // FIXME: f64::MAX.to_string() to json should success
            // (f64::MAX.to_string(), Json::from_f64(f64::MAX), true),
            ("0.0".to_string(), Json::from_f64(0.0).unwrap(), true),
            (
                "\"abcde\"".to_string(),
                Json::from_string("abcde".to_string()).unwrap(),
                true,
            ),
            (
                "\"\"".to_string(),
                Json::from_string("".to_string()).unwrap(),
                true,
            ),
            ("true".to_string(), Json::from_bool(true).unwrap(), true),
            ("false".to_string(), Json::from_bool(false).unwrap(), true),
        ];
        for (input, expect, parse_to_json) in cs {
            let mut rft = FieldType::default();
            if parse_to_json {
                let fta = rft.as_mut_accessor();
                fta.set_flag(FieldTypeFlag::PARSE_TO_JSON);
            }
            let extra = make_extra(&rft);
            let result = cast_string_as_json(&extra, Some(&input.clone().into_bytes()));
            let result_str = result.as_ref().map(|x| x.as_ref().map(|x| x.to_string()));
            let log = format!(
                "input: {}, parse_to_json: {}, expect: {:?}, result: {:?}",
                input, parse_to_json, expect, result_str
            );
            check_result(Some(&expect), &result, log.as_str());
        }
    }

    #[test]
    fn test_decimal_as_json() {
        test_none_with_ctx(cast_any_as_json::<Decimal>);
        let cs = vec![
            (
                Decimal::from_f64(i64::MIN as f64).unwrap(),
                Json::from_f64(i64::MIN as f64).unwrap(),
            ),
            (
                Decimal::from_f64(i64::MAX as f64).unwrap(),
                Json::from_f64(i64::MAX as f64).unwrap(),
            ),
            (
                Decimal::from_bytes(b"184467440737095516160")
                    .unwrap()
                    .unwrap(),
                Json::from_f64(184467440737095516160.0).unwrap(),
            ),
            (
                Decimal::from_bytes(b"-184467440737095516160")
                    .unwrap()
                    .unwrap(),
                Json::from_f64(-184467440737095516160.0).unwrap(),
            ),
        ];

        for (input, expect) in cs {
            let mut ctx = EvalContext::default();
            let r = cast_any_as_json::<Decimal>(&mut ctx, Some(&input));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_time_as_json() {
        test_none_with_ctx(cast_any_as_json::<Time>);
        let mut ctx = EvalContext::default();

        // TODO: add more case for other TimeType
        let cs = vec![
            // Add time_type filed here is to make maintainer know clearly that what is the type of the time.
            (
                Time::parse_datetime(&mut ctx, "2000-01-01T12:13:14", 0, true).unwrap(),
                TimeType::DateTime,
                Json::from_string("2000-01-01 12:13:14.000000".to_string()).unwrap(),
            ),
            (
                Time::parse_datetime(&mut ctx, "2000-01-01T12:13:14.6666", 0, true).unwrap(),
                TimeType::DateTime,
                Json::from_string("2000-01-01 12:13:15.000000".to_string()).unwrap(),
            ),
            (
                Time::parse_datetime(&mut ctx, "2000-01-01T12:13:14", 6, true).unwrap(),
                TimeType::DateTime,
                Json::from_string("2000-01-01 12:13:14.000000".to_string()).unwrap(),
            ),
            (
                Time::parse_datetime(&mut ctx, "2000-01-01T12:13:14.6666", 6, true).unwrap(),
                TimeType::DateTime,
                Json::from_string("2000-01-01 12:13:14.666600".to_string()).unwrap(),
            ),
            (
                Time::parse_datetime(&mut ctx, "2019-09-01", 0, true).unwrap(),
                TimeType::DateTime,
                Json::from_string("2019-09-01 00:00:00.000000".to_string()).unwrap(),
            ),
            (
                Time::parse_datetime(&mut ctx, "2019-09-01", 6, true).unwrap(),
                TimeType::DateTime,
                Json::from_string("2019-09-01 00:00:00.000000".to_string()).unwrap(),
            ),
        ];
        for (input, time_type, expect) in cs {
            let mut ctx = EvalContext::default();
            let result = cast_any_as_json::<Time>(&mut ctx, Some(&input));
            let result_str = result.as_ref().map(|x| x.as_ref().map(|x| x.to_string()));
            let log = format!(
                "input: {}, expect_time_type: {:?}, real_time_type: {:?}, expect: {}, result: {:?}",
                &input,
                time_type,
                input.get_time_type(),
                &expect,
                result_str
            );
            assert_eq!(input.get_time_type(), time_type, "{}", log);
            check_result(Some(&expect), &result, log.as_str());
        }
    }

    #[test]
    fn test_duration_as_json() {
        test_none_with_ctx(cast_any_as_json::<Duration>);

        // TODO: add more case
        let cs = vec![
            (
                Duration::zero(),
                Json::from_string("00:00:00.000000".to_string()).unwrap(),
            ),
            (
                Duration::parse(&mut EvalContext::default(), "10:10:10", 0).unwrap(),
                Json::from_string("10:10:10.000000".to_string()).unwrap(),
            ),
        ];

        for (input, expect) in cs {
            let mut ctx = EvalContext::default();
            let result = cast_any_as_json::<Duration>(&mut ctx, Some(&input));
            let log = make_log(&input, &expect, &result);
            check_result(Some(&expect), &result, log.as_str());
        }
    }

    #[test]
    fn test_json_as_json() {
        test_none_with_nothing(cast_json_as_json);

        let mut jo1: BTreeMap<String, Json> = BTreeMap::new();
        jo1.insert("a".to_string(), Json::from_string("b".to_string()).unwrap());
        let cs = vec![
            Json::from_object(jo1).unwrap(),
            Json::from_array(vec![
                Json::from_i64(1).unwrap(),
                Json::from_i64(3).unwrap(),
                Json::from_i64(4).unwrap(),
            ])
            .unwrap(),
            Json::from_i64(i64::MIN).unwrap(),
            Json::from_i64(i64::MAX).unwrap(),
            Json::from_u64(0u64).unwrap(),
            Json::from_u64(u64::MAX).unwrap(),
            Json::from_f64(f64::MIN).unwrap(),
            Json::from_f64(f64::MAX).unwrap(),
            Json::from_string("abcde".to_string()).unwrap(),
            Json::from_bool(true).unwrap(),
            Json::from_bool(false).unwrap(),
            Json::none().unwrap(),
        ];

        for input in cs {
            let expect = input.clone();
            let result = cast_json_as_json(Some(input.as_ref()));
            let log = make_log(&input, &expect, &result);
            check_result(Some(&expect), &result, log.as_str());
        }
    }
}
