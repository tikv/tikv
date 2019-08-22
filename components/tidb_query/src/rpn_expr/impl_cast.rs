// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;
use std::convert::TryFrom;

use tidb_query_codegen::rpn_fn;
use tidb_query_datatype::FieldTypeAccessor;
use tidb_query_datatype::*;
use tipb::FieldType;

use crate::codec::convert::*;
use crate::codec::data_type::*;
use crate::codec::mysql::RoundMode;
use crate::codec::Error;
use crate::expr::EvalContext;
use crate::expr::Flag;
use crate::rpn_expr::{RpnExpressionNode, RpnFnCallExtra};
use crate::Result;

// TODO: This function supports some internal casts performed by TiKV. However it would be better
//  to be done in TiDB.
//
// If these is no cast_x_as_y in this file, then call these cast_x_as_y,
// otherwise, call cast_any_as_any::<xxx>.convert instead.
//
/// Gets the cast function between specified data types.
///

pub fn get_cast_fn_rpn_node(
    from_field_type: &FieldType,
    to_field_type: FieldType,
) -> Result<RpnExpressionNode> {
    let from: EvalType = box_try!(EvalType::try_from(from_field_type.as_accessor().tp()));
    let to: EvalType = box_try!(EvalType::try_from(to_field_type.as_accessor().tp()));
    let func_meta = match (from, to) {
        // --------------* as int/uint--------------
        (EvalType::Int, EvalType::Int) => cast_int_as_int_fn_meta(),
        (EvalType::Real, EvalType::Int) => cast_real_as_int_fn_meta(),
        (EvalType::Bytes, EvalType::Int) => cast_string_as_int_fn_meta(),
        (EvalType::Decimal, EvalType::Int) => cast_decimal_as_int_fn_meta(),
        (EvalType::Duration, EvalType::Int) => cast_duration_as_int_fn_meta(),
        (EvalType::Json, EvalType::Int) => cast_json_as_int_fn_meta(),

        // --------------* as real--------------
        (EvalType::Int, EvalType::Real) => cast_int_as_real_fn_meta(),
        (EvalType::Real, EvalType::Real) => cast_real_as_real_fn_meta(),
        (EvalType::Bytes, EvalType::Real) => cast_string_as_real_fn_meta(),
        (EvalType::Decimal, EvalType::Real) => cast_decimal_as_real_fn_meta(),
        (EvalType::Duration, EvalType::Real) => cast_duration_as_real_fn_meta(),
        (EvalType::Json, EvalType::Real) => cast_json_as_real_fn_meta(),

        // --------------* as string--------------
        (EvalType::Int, EvalType::Bytes) => {
            if is_field_type_unsigned(from_field_type) {
                cast_uint_as_string_fn_meta()
            } else {
                cast_int_as_string_fn_meta()
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
        (EvalType::Json, EvalType::Bytes) => cast_json_as_string_fn_meta(),

        // --------------* as decimal--------------
        (EvalType::Int, EvalType::Decimal) => {
            if is_field_type_unsigned(from_field_type) {
                cast_uint_as_decimal_fn_meta()
            } else {
                cast_int_as_decimal_fn_meta()
            }
        }
        (EvalType::Real, EvalType::Decimal) => cast_real_as_decimal_fn_meta(),
        (EvalType::Bytes, EvalType::Decimal) => cast_string_as_decimal_fn_meta(),
        (EvalType::Decimal, EvalType::Decimal) => cast_decimal_as_decimal_fn_meta(),
        (EvalType::Duration, EvalType::Decimal) => cast_duration_as_decimal_fn_meta(),
        (EvalType::Json, EvalType::Decimal) => cast_json_as_decimal_fn_meta(),

        // --------------* as duration--------------
        (EvalType::Int, EvalType::Duration) => cast_int_as_duration_fn_meta(),
        (EvalType::Real, EvalType::Duration) => cast_real_as_duration_fn_meta(),
        (EvalType::Bytes, EvalType::Duration) => cast_string_as_duration_fn_meta(),
        (EvalType::Decimal, EvalType::Duration) => cast_decimal_as_duration_fn_meta(),
        (EvalType::Duration, EvalType::Duration) => cast_duration_as_duration_fn_meta(),
        (EvalType::Json, EvalType::Duration) => cast_json_as_duration_fn_meta(),

        // --------------* as json--------------
        (EvalType::Int, EvalType::Json) => {
            let flags = from_field_type.as_accessor().flag();
            if flags.contains(FieldTypeFlag::IS_BOOLEAN) {
                cast_bool_as_json_fn_meta()
            } else if !is_field_type_unsigned(from_field_type) {
                cast_int_as_json_fn_meta()
            } else {
                cast_uint_as_json_fn_meta()
            }
        }
        (EvalType::Real, EvalType::Json) => cast_real_as_json_fn_meta(),
        (EvalType::Bytes, EvalType::Json) => cast_string_as_json_fn_meta(),
        (EvalType::Decimal, EvalType::Json) => cast_decimal_as_json_fn_meta(),
        (EvalType::Duration, EvalType::Json) => cast_duration_as_json_fn_meta(),
        (EvalType::Json, EvalType::Json) => cast_json_as_json_fn_meta(),

        // --------------bugs--------------
        _ => return Err(other_err!("Unsupported cast from {} to {}", from, to)),
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

// -------------------* as int/uint----------------------------------
// ok
#[rpn_fn(capture = [extra])]
#[inline]
fn cast_int_as_int(extra: &RpnFnCallExtra<'_>, val: &Option<Int>) -> Result<Option<i64>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = *val;
            if is_field_type_unsigned(extra.ret_field_type)
                && in_union(&extra.implicit_args)
                && val < 0i64
            {
                Ok(Some(0))
            } else {
                Ok(Some(val))
            }
        }
    }
}

// ok
#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_real_as_int(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Real>,
) -> Result<Option<i64>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.into_inner();
            if !is_field_type_unsigned(extra.ret_field_type) {
                let val: i64 = <f64 as ConvertTo<i64>>::convert(&val, ctx)?;
                Ok(Some(val))
            } else if in_union(&extra.implicit_args) && val < 0f64 {
                Ok(Some(0))
            } else {
                let val: u64 = <f64 as ConvertTo<u64>>::convert(&val, ctx)?;
                Ok(Some(val as i64))
            }
        }
    }
}

// ok
#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_string_as_int(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Bytes>,
) -> Result<Option<i64>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let is_unsigned = is_field_type_unsigned(extra.ret_field_type);
            let val = get_valid_utf8_prefix(ctx, val.as_slice())?;
            let val = val.trim();
            let neg = val.starts_with('-');
            if !neg {
                let r: crate::codec::error::Result<u64> =
                    <&str as ConvertTo<u64>>::convert(&val, ctx);
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
                let r: crate::codec::error::Result<i64> =
                    <&str as ConvertTo<i64>>::convert(&val, ctx);
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

// ok
#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_decimal_as_int(
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
            if is_field_type_unsigned(extra.ret_field_type)
                && in_union(extra.implicit_args)
                && val.is_negative()
            {
                return Ok(Some(0));
            }
            let r = if is_field_type_unsigned(extra.ret_field_type) {
                let r = val.as_u64();
                // TODO, TiDB's HandleOverflow is not same as ours,
                //  so here is a little difference between TiDB's
                r.into_result_with_overflow_err(ctx, Error::truncated_wrong_val("DECIMAL", &val))?
                    as i64
            } else {
                let r = val.as_i64();
                // TODO, TiDB's HandleOverflow is not same as ours,
                //  so here is a little difference between TiDB's
                r.into_result_with_overflow_err(ctx, Error::truncated_wrong_val("DECIMAL", &val))?
            };
            Ok(Some(r))
        }
    }
}

// ok
#[rpn_fn(capture = [ctx])]
#[inline]
fn cast_duration_as_int(ctx: &mut EvalContext, val: &Option<Duration>) -> Result<Option<i64>> {
    match val {
        None => Ok(None),
        // TODO, in the convert, we handle err using ctx, however, TiDB doesn't
        Some(val) => Ok(Some(<Duration as ConvertTo<i64>>::convert(val, ctx)?)),
    }
}

// ok
#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_json_as_int(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Json>,
) -> Result<Option<i64>> {
    match val {
        None => Ok(None),
        Some(j) => {
            if is_field_type_unsigned(extra.ret_field_type) {
                Ok(Some(<Json as ConvertTo<u64>>::convert(&j, ctx)? as i64))
            } else {
                Ok(Some(<Json as ConvertTo<i64>>::convert(&j, ctx)?))
            }
        }
    }
}

// -------------------* as real----------------------------------
// TODO, TiDB's seems has bug
#[rpn_fn(capture = [extra])]
#[inline]
fn cast_int_as_real(extra: &RpnFnCallExtra<'_>, val: &Option<Int>) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = *val;
            if is_field_type_unsigned(extra.ret_field_type)
                && in_union(extra.implicit_args)
                && val < 0
            {
                Ok(Real::new(0f64).ok())
            } else {
                Ok(Real::new(val as f64).ok())
            }
        }
    }
}

// TODO, TiDB's seems has bug
#[rpn_fn]
#[inline]
fn cast_uint_as_real(val: &Option<Int>) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            // recall that, int to float is different from uint to float
            let val = *val as u64;
            Ok(Real::new(val as f64).ok())
        }
    }
}

// ok
#[rpn_fn(capture = [extra])]
#[inline]
fn cast_real_as_real(extra: &RpnFnCallExtra<'_>, val: &Option<Real>) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            if is_field_type_unsigned(extra.ret_field_type)
                && in_union(extra.implicit_args)
                && val.into_inner() < 0f64
            {
                Ok(Real::new(0f64).ok())
            } else {
                Ok(Some(*val))
            }
        }
    }
}

// ok
#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_string_as_real(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Bytes>,
) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            // TODO, in TiDB's builtinCastStringAsRealSig, if val is IsBinaryLiteral,
            //  then return evalReal directly
            let mut r: f64 = <Bytes as ConvertTo<f64>>::convert(val, ctx)?;
            if is_field_type_unsigned(extra.ret_field_type)
                && in_union(extra.implicit_args)
                && r < 0f64
            {
                r = 0f64;
            }
            let r = produce_float_with_specified_tp(ctx, extra.ret_field_type, r)?;
            Ok(Real::new(r).ok())
        }
    }
}

// ok
#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_decimal_as_real(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Decimal>,
) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            if is_field_type_unsigned(extra.ret_field_type)
                && in_union(extra.implicit_args)
                && val.is_negative()
            {
                Ok(Real::new(0f64).ok())
            } else {
                Ok(Real::new(<Decimal as ConvertTo<f64>>::convert(val, ctx)?).ok())
            }
        }
    }
}

// ok
#[rpn_fn(capture = [ctx])]
#[inline]
fn cast_duration_as_real(ctx: &mut EvalContext, val: &Option<Duration>) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val: f64 = <Duration as ConvertTo<f64>>::convert(val, ctx)?;
            Ok(Real::new(val).ok())
        }
    }
}

// ok
#[rpn_fn(capture = [ctx])]
#[inline]
fn cast_json_as_real(ctx: &mut EvalContext, val: &Option<Json>) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(j) => Ok(Real::new(<Json as ConvertTo<f64>>::convert(&j, ctx)?).ok()),
    }
}

// -------------------* as string----------------------------------
// ok
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

// ok
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

// ok
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

// ok
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

// ok
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

// ok
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

// ok
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

// ok
#[rpn_fn]
#[inline]
fn cast_json_as_string(val: &Option<Json>) -> Result<Option<Bytes>> {
    match val {
        None => Ok(None),
        Some(j) => Ok(Some(j.to_string().into_bytes())),
    }
}

// -------------------* as decimal----------------------------------
// TODO, TiDB's has bug
#[rpn_fn]
#[inline]
fn cast_int_as_decimal(val: &Option<Int>) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(Decimal::from(*val))),
    }
}

// TODO, TiDB's has bug
#[rpn_fn]
#[inline]
fn cast_uint_as_decimal(val: &Option<Int>) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(Decimal::from(*val as u64))),
    }
}

// ok
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
                <f64 as ConvertTo<Decimal>>::convert(&val, ctx)?
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
#[rpn_fn]
#[inline]
fn cast_string_as_decimal(val: &Option<Bytes>) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(_val) => {
            // TODO, in TiDB, if the param IsBinaryLiteral, then return the result of `evalDecimal` directly
            Ok(Some(Decimal::zero()))
        }
    }
}

// ok
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

// ok
#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_duration_as_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Duration>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let dec: Decimal = <Duration as ConvertTo<Decimal>>::convert(val, ctx)?;
            Ok(Some(produce_dec_with_specified_tp(
                ctx,
                dec,
                extra.ret_field_type,
            )?))
        }
    }
}

// ok
#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_json_as_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Json>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(j) => {
            let d: Decimal = <Json as ConvertTo<Decimal>>::convert(j, ctx)?;
            Ok(Some(produce_dec_with_specified_tp(
                ctx,
                d,
                extra.ret_field_type,
            )?))
        }
    }
}

// -------------------* as duration----------------------------------
// ok
#[rpn_fn(capture = [ctx, extra])]
#[inline]
pub fn cast_int_as_duration(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Int>,
) -> Result<Option<Duration>> {
    match val {
        None => Ok(None),
        Some(val) => {
            // the from_i64 had handle overflow, so needn't to handle the err like TiDB does
            let dur = Duration::from_i64(ctx, *val, extra.ret_field_type.get_decimal() as u8)?;
            Ok(Some(dur))
        }
    }
}

// ok
#[rpn_fn(capture = [extra])]
#[inline]
pub fn cast_real_as_duration(
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Real>,
) -> Result<Option<Duration>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.into_inner();
            let val = val.to_string();
            Ok(Some(Duration::parse(
                val.as_bytes(),
                extra.ret_field_type.decimal() as i8,
            )?))
        }
    }
}

// ok
#[rpn_fn(capture = [ctx, extra])]
#[inline]
pub fn cast_string_as_duration(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Bytes>,
) -> Result<Option<Duration>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let s = get_valid_utf8_prefix(ctx, val.as_slice())?;
            cast_as_duration_helper(ctx, extra, s)
        }
    }
}

// ok
#[rpn_fn(capture = [ctx, extra])]
#[inline]
pub fn cast_decimal_as_duration(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Decimal>,
) -> Result<Option<Duration>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let s = val.to_string();
            cast_as_duration_helper(ctx, extra, s.as_str())
        }
    }
}

// ok
#[rpn_fn(capture = [extra])]
#[inline]
fn cast_duration_as_duration(
    extra: &RpnFnCallExtra,
    val: &Option<Duration>,
) -> Result<Option<Duration>> {
    match val {
        None => Ok(None),
        Some(d) => Ok(Some(d.round_frac(extra.ret_field_type.decimal() as i8)?)),
    }
}

// ok
#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_json_as_duration(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Json>,
) -> Result<Option<Duration>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.unquote()?;
            cast_as_duration_helper(ctx, extra, val.as_str())
        }
    }
}

// TODO, there are some difference between TiKV's and TiDB's,
//  read the doc for more info
#[inline]
fn cast_as_duration_helper(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &str,
) -> Result<Option<Duration>> {
    let r = Duration::parse(val.as_bytes(), extra.ret_field_type.decimal() as i8);
    match r {
        // TODO, here is not same as TiDB's because
        //  1. Duration::parse is not same as TiDB's impl
        //  2. if parse return err, tidb can get res, too.
        //  however, we can't
        Err(e) => {
            ctx.handle_truncate_err(e)?;
            Ok(Some(Duration::zero()))
        }
        Ok(d) => Ok(Some(d)),
    }
}

// -------------------* as json----------------------------------
// ok
#[rpn_fn]
#[inline]
fn cast_int_as_json(val: &Option<Int>) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(Json::I64(*val))),
    }
}

// ok
#[rpn_fn]
#[inline]
fn cast_uint_as_json(val: &Option<Int>) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(Json::U64(*val as u64))),
    }
}

// ok
#[rpn_fn]
#[inline]
fn cast_bool_as_json(val: &Option<Int>) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(Json::Boolean(*val != 0))),
    }
}

// ok
#[rpn_fn]
#[inline]
fn cast_real_as_json(val: &Option<Real>) -> Result<Option<Json>> {
    // FIXME: `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `DOUBLE` now.
    Ok(val.map(|x| Json::Double(x.into_inner())))
}

// ok
#[rpn_fn(capture = [extra])]
#[inline]
fn cast_string_as_json(extra: &RpnFnCallExtra<'_>, val: &Option<Bytes>) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let ft = extra.ret_field_type;
            if ft.flag().contains(FieldTypeFlag::PARSE_TO_JSON) {
                let s: Cow<str> = String::from_utf8_lossy(val.as_slice());
                Ok(Some(s.parse::<Json>()?))
            } else {
                // FIXME: port `JSONBinary` from TiDB to adapt if the bytes is not a valid utf8 string
                let val = unsafe { String::from_utf8_unchecked(val.to_owned()) };
                Ok(Some(Json::String(val)))
            }
        }
    }
}

// ok
#[rpn_fn(capture = [ctx])]
#[inline]
fn cast_decimal_as_json(ctx: &mut EvalContext, val: &Option<Decimal>) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => {
            // FIXME: `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `DOUBLE` now.
            let r: crate::codec::Result<f64> = <Decimal as ConvertTo<f64>>::convert(val, ctx);
            match r {
                // TODO, in TiDB, they return isNull(true) and err at the same time,
                //  however, we can't
                Ok(n) => Ok(Some(Json::Double(n))),
                Err(_) => Ok(None),
            }
        }
    }
}

// ok
#[rpn_fn]
#[inline]
fn cast_duration_as_json(val: &Option<Duration>) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val: Duration = (*val).maximize_fsp();
            Ok(Some(Json::String(val.to_numeric_string())))
        }
    }
}

// ok
#[rpn_fn]
#[inline]
fn cast_json_as_json(val: &Option<Json>) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(val.clone())),
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
