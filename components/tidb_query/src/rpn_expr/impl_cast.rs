// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;
use std::convert::TryFrom;

use num_traits::identities::Zero;
use tidb_query_codegen::rpn_fn;
use tidb_query_datatype::*;
use tipb::{Expr, FieldType};

use crate::codec::convert::*;
use crate::codec::data_type::*;
use crate::codec::error::{ERR_DATA_OUT_OF_RANGE, WARN_DATA_TRUNCATED};
use crate::codec::mysql::RoundMode;
use crate::codec::Error;
use crate::expr::{EvalContext, Flag};
use crate::rpn_expr::{RpnExpressionNode, RpnFnCallExtra, RpnFnMeta};
use crate::Result;

fn get_cast_fn_rpn_meta(
    from_field_type: &FieldType,
    to_field_type: &FieldType,
) -> Result<RpnFnMeta> {
    let from = box_try!(EvalType::try_from(from_field_type.as_accessor().tp()));
    let to = box_try!(EvalType::try_from(to_field_type.as_accessor().tp()));
    let func_meta = match (from, to) {
        // ---------------------* as real-----------------------
        // TODO
        (EvalType::Int, EvalType::Real) => {
            if !from_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Int, Real>()
            } else {
                cast_uint_as_real_fn_meta()
            }
        }
        (EvalType::Real, EvalType::Real) => {
            if !from_field_type.is_unsigned() {
                cast_real_as_signed_real_fn_meta()
            } else {
                cast_real_as_unsigned_real_fn_meta()
            }
        }
        (EvalType::Bytes, EvalType::Real) => {
            if !from_field_type.is_unsigned() {
                cast_string_as_signed_real_fn_meta()
            } else {
                cast_string_as_unsigned_real_fn_meta()
            }
        }
        (EvalType::Decimal, EvalType::Real) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Decimal, Real>()
            } else {
                cast_decimal_as_unsigned_real_fn_meta()
            }
        }
        (EvalType::DateTime, EvalType::Real) => cast_any_as_any_fn_meta::<DateTime, Real>(),
        (EvalType::Duration, EvalType::Real) => cast_any_as_any_fn_meta::<Duration, Real>(),
        (EvalType::Json, EvalType::Real) => cast_any_as_any_fn_meta::<Json, Real>(),

        // ---------------------* as decimal-----------------------
        (EvalType::Int, EvalType::Decimal) => {
            if !from_field_type.is_unsigned() && !to_field_type.is_unsigned() {
                cast_any_as_decimal_fn_meta::<Int>()
            } else {
                cast_uint_as_decimal_fn_meta()
            }
        }
        (EvalType::Real, EvalType::Decimal) => cast_real_as_decimal_fn_meta(),
        (EvalType::Bytes, EvalType::Decimal) => cast_string_as_decimal_fn_meta(),
        (EvalType::Decimal, EvalType::Decimal) => cast_decimal_as_decimal_fn_meta(),
        (EvalType::DateTime, EvalType::Decimal) => cast_any_as_decimal_fn_meta::<DateTime>(),
        (EvalType::Duration, EvalType::Decimal) => cast_any_as_decimal_fn_meta::<Duration>(),
        (EvalType::Json, EvalType::Decimal) => cast_any_as_decimal_fn_meta::<Json>(),

        // ---------------------* as int-----------------------
        (EvalType::Int, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_int_as_int_fn_meta()
            } else {
                cast_int_as_uint_fn_meta()
            }
        }
        (EvalType::Real, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Real, Int>()
            } else {
                cast_real_as_uint_fn_meta()
            }
        }
        (EvalType::Bytes, EvalType::Int) => cast_string_as_int_or_uint_fn_meta(),
        (EvalType::Decimal, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_decimal_as_int_fn_meta()
            } else {
                cast_decimal_as_uint_fn_meta()
            }
        }
        (EvalType::DateTime, EvalType::Int) => cast_any_as_any_fn_meta::<DateTime, Int>(),
        (EvalType::Duration, EvalType::Int) => cast_any_as_any_fn_meta::<Duration, Int>(),
        (EvalType::Json, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Json, Int>()
            } else {
                cast_json_as_uint_fn_meta()
            }
        }

        // --------------* as string--------------
        (EvalType::Int, EvalType::Bytes) => {
            if !is_field_type_unsigned(from_field_type) {
                cast_int_as_string_fn_meta()
            } else {
                cast_uint_as_string_fn_meta()
            }
        }
        (EvalType::Real, EvalType::Bytes) => {
            if from_field_type.tp() == FieldTypeTp::Float {
                cast_float_real_as_string_fn_meta()
            } else {
                cast_double_real_as_string_fn_meta()
            }
        }
        (EvalType::Bytes, EvalType::Bytes) => cast_string_as_string_fn_meta(),
        (EvalType::Decimal, EvalType::Bytes) => cast_decimal_as_string_fn_meta(),
        (EvalType::Duration, EvalType::Bytes) => cast_duration_as_string_fn_meta(),
        (EvalType::Json, EvalType::Bytes) => cast_any_as_any_fn_meta::<Json, Bytes>(),

        // ---------------------* as json-----------------------
        (EvalType::Int, EvalType::Json) => {
            if from_field_type
                .as_accessor()
                .flag()
                .contains(FieldTypeFlag::IS_BOOLEAN)
            {
                cast_bool_as_json_fn_meta()
            } else if !from_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Int, Json>()
            } else {
                cast_uint_as_json_fn_meta()
            }
        }
        (EvalType::Real, EvalType::Json) => cast_any_as_any_fn_meta::<Real, Json>(),
        (EvalType::Bytes, EvalType::Json) => cast_string_as_json_fn_meta(),
        (EvalType::Decimal, EvalType::Json) => cast_any_as_any_fn_meta::<Decimal, Json>(),
        (EvalType::DateTime, EvalType::Json) => cast_any_as_any_fn_meta::<DateTime, Json>(),
        (EvalType::Duration, EvalType::Json) => cast_any_as_any_fn_meta::<Duration, Json>(),
        (EvalType::Json, EvalType::Json) => cast_json_as_json_fn_meta(),

        // ---------------------* as duration-----------------------
        (EvalType::Int, EvalType::Duration) => cast_int_as_duration_fn_meta(),
        (EvalType::Real, EvalType::Duration) => cast_real_as_duration_fn_meta(),
        (EvalType::Bytes, EvalType::Duration) => cast_bytes_as_duration_fn_meta(),
        (EvalType::Decimal, EvalType::Duration) => cast_decimal_as_duration_fn_meta(),
        (EvalType::DateTime, EvalType::Duration) => cast_time_as_duration_fn_meta(),
        (EvalType::Duration, EvalType::Duration) => cast_duration_as_duration_fn_meta(),
        (EvalType::Json, EvalType::Duration) => cast_json_as_duration_fn_meta(),

        // ---------------------others-----------------------
        _ => return Err(other_err!("Unsupported cast from {} to {}", from, to)),
    };
    Ok(func_meta)
}

/// Gets the cast function between specified data types.
///
/// TODO: This function supports some internal casts performed by TiKV. However it would be better
/// to be done in TiDB.
pub fn get_cast_fn_rpn_node(
    from_field_type: &FieldType,
    to_field_type: FieldType,
) -> Result<RpnExpressionNode> {
    let func_meta = get_cast_fn_rpn_meta(from_field_type, &to_field_type)?;
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
    get_cast_fn_rpn_meta(children[0].get_field_type(), expr.get_field_type())
}

/// Indicates whether the current expression is evaluated in union statement
///
/// Note: The TiDB will push down the `inUnion` flag by implicit constant arguments,
/// but some CAST expressions inserted by TiKV coprocessor use an empty vector to represent
/// the `inUnion` flag is false.
/// See: https://github.com/pingcap/tidb/blob/1e403873d905b2d0ad3be06bd8cd261203d84638/expression/builtin.go#L260
fn in_union(implicit_args: &[ScalarValue]) -> bool {
    implicit_args.get(0) == Some(&ScalarValue::Int(Some(1)))
}

fn is_field_type_unsigned(ft: &FieldType) -> bool {
    ft.flag().contains(FieldTypeFlag::UNSIGNED)
}

// -------------------* as decimal----------------------------------
// TODO, TiDB's impl has bug, fix this after fixed TiDB's
#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_uint_as_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<i64>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            // TODO, TiDB's uint to decimal seems has bug, fix this after fix TiDB's
            let dec = if in_union(extra.implicit_args) && *val < 0 {
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

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_real_as_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Real>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.into_inner();
            let res = if in_union(extra.implicit_args) && val < 0f64 {
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

// TODO, TiDB's builtinCastStringAsDecimalSig has bug
//  fix this after fixed TiDB's
#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_string_as_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Bytes>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            // TODO, in TiDB, if the param IsBinaryLiteral, then return the result of `evalDecimal` directly
            let d: Decimal = val.convert(ctx)?;
            let in_union = in_union(extra.implicit_args);
            let is_unsigned = is_field_type_unsigned(extra.ret_field_type);
            let d = if in_union && is_unsigned && d.is_negative() {
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

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_decimal_as_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Decimal>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let res = if in_union(extra.implicit_args)
                && is_field_type_unsigned(extra.ret_field_type)
                && val.is_negative()
            {
                Decimal::zero()
            } else {
                // TODO, avoid this clone
                val.clone()
            };
            Ok(Some(produce_dec_with_specified_tp(
                ctx,
                res,
                extra.ret_field_type,
            )?))
        }
    }
}

/// The signed int implementation for push down signature `CastIntAsDecimal`.
///
/// It include `cast_int_as_decimal`, `cast_time_as_decimal`, `cast_duration_as_decimal`, `cast_json_as_decimal`
// TODO, for cast_int_as_decimal, TiDB's impl has bug, fix this after fixed TiDB's
#[rpn_fn(capture = [ctx, extra])]
#[inline]
pub fn cast_any_as_decimal<From: Evaluable + ConvertTo<Decimal>>(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<From>,
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

// -------------------* as int/uint----------------------------------
#[rpn_fn]
#[inline]
fn cast_int_as_int(val: &Option<Int>) -> Result<Option<i64>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(*val)),
    }
}

#[rpn_fn(capture = [extra])]
#[inline]
fn cast_int_as_uint(extra: &RpnFnCallExtra<'_>, val: &Option<Int>) -> Result<Option<i64>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = *val;
            if in_union(extra.implicit_args) && val < 0i64 {
                Ok(Some(0))
            } else {
                Ok(Some(val))
            }
        }
    }
}

// cast_real_as_int is represent by cast_any_as_any<Real, int>

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_real_as_uint(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Real>,
) -> Result<Option<i64>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.into_inner();
            if in_union(&extra.implicit_args) && val < 0f64 {
                Ok(Some(0))
            } else {
                let val: u64 = val.convert(ctx)?;
                Ok(Some(val as i64))
            }
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_string_as_int_or_uint(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Bytes>,
) -> Result<Option<i64>> {
    match val {
        None => Ok(None),
        Some(val) => {
            // TODO, in TiDB',s if `b.args[0].GetType().Hybrid()`,
            //  then it will return res from EvalInt() directly.
            let is_unsigned = is_field_type_unsigned(extra.ret_field_type);
            let val = get_valid_utf8_prefix(ctx, val.as_slice())?;
            let val = val.trim();
            let neg = val.starts_with('-');
            if !neg {
                let r: crate::codec::error::Result<u64> = val.as_bytes().convert(ctx);
                match r {
                    Ok(x) => {
                        if !is_unsigned && x > std::i64::MAX as u64 {
                            ctx.warnings
                                .append_warning(Error::cast_as_signed_overflow())
                        }
                        Ok(Some(x as i64))
                    }
                    Err(e) => handle_overflow_for_cast_string_as_int(ctx, e, false, val).map(Some),
                }
            } else if in_union(extra.implicit_args) && is_unsigned {
                Ok(Some(0))
            } else {
                let r: crate::codec::error::Result<i64> = val.as_bytes().convert(ctx);
                match r {
                    Ok(x) => {
                        if is_unsigned {
                            ctx.warnings
                                .append_warning(Error::cast_neg_int_as_unsigned());
                        }
                        Ok(Some(x))
                    }
                    Err(e) => handle_overflow_for_cast_string_as_int(ctx, e, true, val).map(Some),
                }
            }
        }
    }
}

// TODO, TiDB's this func can return err and res at the same time, however, we can't,
//  so it may be some inconsistency between this func and TiDB's
fn handle_overflow_for_cast_string_as_int(
    ctx: &mut EvalContext,
    err: Error,
    is_neg: bool,
    origin_str: &str,
) -> Result<i64> {
    if ctx.cfg.flag.contains(Flag::IN_INSERT_STMT) && err.is_overflow() {
        // TODO, here our `handle_overflow_err` is not same as TiDB's HandleOverflow,
        //  the latter will return `err` if OverflowAsWarning, but we will return `truncated_wrong_val`.
        ctx.handle_overflow_err(Error::truncated_wrong_val("INTEGER", origin_str))?;
        if is_neg {
            Ok(std::i64::MIN)
        } else {
            Ok(std::u64::MAX as i64)
        }
    } else {
        Err(err.into())
    }
}

#[rpn_fn(capture = [ctx])]
#[inline]
fn cast_decimal_as_int(ctx: &mut EvalContext, val: &Option<Decimal>) -> Result<Option<i64>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.clone();
            let val = val.round(0, RoundMode::HalfEven);
            // in TiDB, if err is not nil, then it return err and set `isNull` true,
            // so when it is not ok, I return Ok(None) if no err is returned.
            // FIXME, here may not same as TiDB's
            let val = if val.is_ok() {
                val.unwrap()
            } else {
                val.into_result(ctx)?;
                // according to https://github.com/pingcap/tidb/pull/10498#discussion_r313420336
                return Ok(None);
            };

            // TODO, TiDB's HandleOverflow is not same as ours,
            //  so here is a little difference between TiDB's
            let r = val.as_i64();
            let err = Error::truncated_wrong_val("DECIMAL", &val);
            Ok(Some(r.into_result_with_overflow_err(ctx, err)?))
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_decimal_as_uint(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Decimal>,
) -> Result<Option<i64>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.clone();
            let val = val.round(0, RoundMode::HalfEven);
            // in TiDB, if err is not nil, then it return err and set `isNull` true,
            // so when it is not ok, I return Ok(None) if no err is returned.
            // FIXME, here may not same as TiDB's
            let val = if val.is_ok() {
                val.unwrap()
            } else {
                val.into_result(ctx)?;
                // according to https://github.com/pingcap/tidb/pull/10498#discussion_r313420336
                return Ok(None);
            };

            if in_union(extra.implicit_args) && val.is_negative() {
                Ok(Some(0))
            } else {
                let r = val.as_u64();
                // TODO, TiDB's HandleOverflow is not same as ours,
                //  so here is a little difference between TiDB's
                let r = r.into_result_with_overflow_err(
                    ctx,
                    Error::truncated_wrong_val("DECIMAL", &val),
                )?;
                Ok(Some(r as i64))
            }
        }
    }
}

// cast_time_as_int_or_uint is represent by cast_any_as_any<Time, Int>

// cast_duration_as_int_or_uint is represent by cast_any_as_any<Duration, Int>

// cast_json_as_int is represent by cast_any_as_any<Json, Int>

#[rpn_fn(capture = [ctx])]
#[inline]
fn cast_json_as_uint(ctx: &mut EvalContext, val: &Option<Json>) -> Result<Option<i64>> {
    match val {
        None => Ok(None),
        Some(j) => {
            let r: u64 = j.convert(ctx)?;
            Ok(Some(r as i64))
        }
    }
}

// -------------------* as real----------------------------------
// TODO, TiDB has bug here
#[rpn_fn]
#[inline]
fn cast_int_as_real(val: &Option<Int>) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Real::new(*val as f64).ok()),
    }
}

// TODO, TiDB has bug here
#[rpn_fn]
#[inline]
fn cast_uint_as_real(val: &Option<Int>) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Real::new(*val as u64 as f64).ok()),
    }
}

#[rpn_fn]
#[inline]
fn cast_real_as_signed_real(val: &Option<Real>) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(*val)),
    }
}

#[rpn_fn(capture = [extra])]
#[inline]
fn cast_real_as_unsigned_real(
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Real>,
) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            if in_union(extra.implicit_args) && val.into_inner() < 0f64 {
                Ok(Some(Real::zero()))
            } else {
                Ok(Some(*val))
            }
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_string_as_signed_real(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Bytes>,
) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            // TODO, in TiDB's builtinCastStringAsRealSig, if val is IsBinaryLiteral,
            //  then return evalReal directly
            let r: f64 = val.convert(ctx)?;
            let r = produce_float_with_specified_tp(ctx, extra.ret_field_type, r)?;
            Ok(Real::new(r).ok())
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_string_as_unsigned_real(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Bytes>,
) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            // TODO, in TiDB's builtinCastStringAsRealSig, if val is IsBinaryLiteral,
            //  then return evalReal directly
            let mut r: f64 = val.convert(ctx)?;
            if in_union(extra.implicit_args) && r < 0f64 {
                r = 0f64;
            }
            let r = produce_float_with_specified_tp(ctx, extra.ret_field_type, r)?;
            Ok(Real::new(r).ok())
        }
    }
}

// cast_decimal_as_signed_real is represented by cast_any_as_any<Decimal, Real>

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_decimal_as_unsigned_real(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Decimal>,
) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            if in_union(extra.implicit_args) && val.is_negative() {
                Ok(Some(Real::zero()))
            } else {
                Ok(Some(val.convert(ctx)?))
            }
        }
    }
}

// cast_time_as_real is represented by cast_any_as_any<Time, Real>

// cast_duration_as_real is represented by cast_any_as_any<Duration, Real>

// cast_json_as_real is represented by cast_any_as_any<Json, Real>

// -------------------* as string----------------------------------
#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_int_as_string(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Int>,
) -> Result<Option<Bytes>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let r = (*val).to_string().into_bytes();
            cast_as_string_helper(ctx, extra, r)
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_uint_as_string(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Int>,
) -> Result<Option<Bytes>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let p = (*val as u64).to_string().into_bytes();
            cast_as_string_helper(ctx, extra, p)
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_float_real_as_string(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Real>,
) -> Result<Option<Bytes>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.into_inner() as f32;
            let val = val.to_string();
            cast_as_string_helper(ctx, extra, Vec::from(val.as_bytes()))
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_double_real_as_string(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Real>,
) -> Result<Option<Bytes>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.into_inner();
            let val = val.to_string();
            cast_as_string_helper(ctx, extra, Vec::from(val.as_bytes()))
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_string_as_string(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Bytes>,
) -> Result<Option<Bytes>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.clone();
            cast_as_string_helper(ctx, extra, val)
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_decimal_as_string(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Decimal>,
) -> Result<Option<Bytes>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.to_string();
            cast_as_string_helper(ctx, extra, Vec::from(val.as_bytes()))
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_duration_as_string(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Duration>,
) -> Result<Option<Bytes>> {
    match val {
        None => Ok(None),
        Some(dur) => {
            let s = dur.to_numeric_string();
            cast_as_string_helper(ctx, extra, Vec::from(s.as_bytes()))
        }
    }
}

// TODO, make val Cow, then we can avoid some copy
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

// cast_json_as_string is represented by cast_any_as_any<Json, String>

// -------------------* as json----------------------------------
// cast_int_as_json is represented by cast_any_as_any<Int, Json>

#[rpn_fn]
#[inline]
fn cast_uint_as_json(val: &Option<Int>) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(Json::U64(*val as u64))),
    }
}

#[rpn_fn]
#[inline]
fn cast_bool_as_json(val: &Option<Int>) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(Json::Boolean(*val != 0))),
    }
}

// cast_real_as_json is represented by cast_any_as_any<Real, Json>

#[rpn_fn(capture = [extra])]
#[inline]
fn cast_string_as_json(extra: &RpnFnCallExtra<'_>, val: &Option<Bytes>) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => {
            if extra
                .ret_field_type
                .flag()
                .contains(FieldTypeFlag::PARSE_TO_JSON)
            {
                let s = box_try!(String::from_utf8(val.to_owned()));
                let val: Json = s.parse()?;
                Ok(Some(val))
            } else {
                // FIXME: port `JSONBinary` from TiDB to adapt if the bytes is not a valid utf8 string
                let val = unsafe { String::from_utf8_unchecked(val.to_owned()) };
                Ok(Some(Json::String(val)))
            }
        }
    }
}

// TODO, TiDB's builtinCastDecimalAsJSONSig::evalJSON return null when
//  MyDecimal::ToFloat64 return err, but this is a bug,
//  because decimal to float will never overflow
// cast_decimal_as_json is represented by cast_any_as_any<Decimal, Json>

// cast_time_as_json is represented by cast_any_as_any<Time, Json>

// cast_duration_as_json is represented by cast_any_as_any<Duration, Json>

#[rpn_fn]
#[inline]
fn cast_json_as_json(val: &Option<Json>) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(val.clone())),
    }
}

// -------------------* as duration----------------------------------
#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_int_as_duration(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Int>,
) -> Result<Option<Duration>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let dur = Duration::from_i64(ctx, *val, extra.ret_field_type.get_decimal() as u8)?;
            Ok(Some(dur))
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_time_as_duration(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<DateTime>,
) -> Result<Option<Duration>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let dur: Duration = val.convert(ctx)?;
            Ok(Some(dur.round_frac(extra.ret_field_type.decimal() as i8)?))
        }
    }
}

#[rpn_fn(capture = [extra])]
#[inline]
fn cast_duration_as_duration(
    extra: &RpnFnCallExtra,
    val: &Option<Duration>,
) -> Result<Option<Duration>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(val.round_frac(extra.ret_field_type.decimal() as i8)?)),
    }
}

macro_rules! cast_as_duration {
    ($ty:ty, $as_uint_fn:ident, $extra:expr) => {
        #[rpn_fn(capture = [ctx, extra])]
        #[inline]
        pub fn $as_uint_fn(
            ctx: &mut EvalContext,
            extra: &RpnFnCallExtra<'_>,
            val: &Option<$ty>,
        ) -> Result<Option<Duration>> {
            match val {
                None => Ok(None),
                Some(val) => {
                    let result = Duration::parse($extra, extra.ret_field_type.get_decimal() as i8);
                    match result {
                        Ok(dur) => Ok(Some(dur)),
                        // TODO, here is not same as TiDB's because
                        //  1. Duration::parse is not same as TiDB's impl
                        //  2. if parse return err, tidb can get res, too, however, we can't.
                        Err(e) => match e.code() {
                            ERR_DATA_OUT_OF_RANGE => {
                                ctx.handle_overflow_err(e)?;
                                Ok(Some(Duration::zero()))
                            }
                            WARN_DATA_TRUNCATED => {
                                ctx.handle_truncate_err(e)?;
                                Ok(Some(Duration::zero()))
                            }
                            _ => Err(e.into()),
                        },
                    }
                }
            }
        }
    };
}

cast_as_duration!(
    Real,
    cast_real_as_duration,
    val.into_inner().to_string().as_bytes()
);
cast_as_duration!(Bytes, cast_bytes_as_duration, val);
cast_as_duration!(Json, cast_json_as_duration, val.unquote()?.as_bytes());
cast_as_duration!(
    Decimal,
    cast_decimal_as_duration,
    val.to_string().as_bytes()
);

// -------------------any as any(others)----------------------------------
#[rpn_fn(capture = [ctx])]
#[inline]
fn cast_any_as_any<From: ConvertTo<To> + Evaluable, To: Evaluable>(
    ctx: &mut EvalContext,
    val: &Option<From>,
) -> Result<Option<To>> {
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
