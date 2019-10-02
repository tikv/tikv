// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![allow(dead_code)]

use std::borrow::Cow;
use std::convert::TryFrom;

use num_traits::identities::Zero;
use tidb_query_codegen::rpn_fn;
use tidb_query_datatype::*;
use tipb::{Expr, FieldType};

use crate::codec::convert::*;
use crate::codec::data_type::*;
use crate::codec::error::{ERR_DATA_OUT_OF_RANGE, WARN_DATA_TRUNCATED};
use crate::codec::Error;
use crate::expr::EvalContext;
use crate::rpn_expr::{RpnExpressionNode, RpnFnCallExtra, RpnFnMeta};
use crate::Result;
use std::num::IntErrorKind;

fn get_cast_fn_rpn_meta(
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
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Real, Int>()
            } else {
                cast_real_as_uint_fn_meta()
            }
        }
        (EvalType::Bytes, EvalType::Int) => cast_string_as_int_or_uint_fn_meta(),
        (EvalType::Decimal, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Decimal, Int>()
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
            if !to_field_type.is_unsigned() {
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

        (EvalType::Int, EvalType::Decimal) => {
            if !from_field_type.is_unsigned() && !to_field_type.is_unsigned() {
                cast_any_as_decimal_fn_meta::<Int>()
            } else {
                cast_uint_as_decimal_fn_meta()
            }
        }
        (EvalType::Bytes, EvalType::Decimal) => cast_any_as_decimal_fn_meta::<Bytes>(),
        (EvalType::Real, EvalType::Decimal) => cast_any_as_decimal_fn_meta::<Real>(),
        (EvalType::DateTime, EvalType::Decimal) => cast_any_as_decimal_fn_meta::<DateTime>(),
        (EvalType::Duration, EvalType::Decimal) => cast_any_as_decimal_fn_meta::<Duration>(),
        (EvalType::Json, EvalType::Decimal) => cast_any_as_decimal_fn_meta::<Json>(),
        (EvalType::Int, EvalType::Bytes) => {
            if !from_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Int, Bytes>()
            } else {
                cast_uint_as_string_fn_meta()
            }
        }
        (EvalType::Real, EvalType::Bytes) => cast_any_as_any_fn_meta::<Real, Bytes>(),
        (EvalType::Decimal, EvalType::Bytes) => cast_any_as_any_fn_meta::<Decimal, Bytes>(),
        (EvalType::DateTime, EvalType::Bytes) => cast_any_as_any_fn_meta::<DateTime, Bytes>(),
        (EvalType::Duration, EvalType::Bytes) => cast_any_as_any_fn_meta::<Duration, Bytes>(),
        (EvalType::Json, EvalType::Bytes) => cast_any_as_any_fn_meta::<Json, Bytes>(),
        (EvalType::Int, EvalType::Json) => {
            if from_field_type
                .as_accessor()
                .flag()
                .contains(FieldTypeFlag::IS_BOOLEAN)
            {
                cast_int_as_json_boolean_fn_meta()
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
        (EvalType::Int, EvalType::Duration) => cast_int_as_duration_fn_meta(),
        (EvalType::Real, EvalType::Duration) => cast_real_as_duration_fn_meta(),
        (EvalType::Bytes, EvalType::Duration) => cast_bytes_as_duration_fn_meta(),
        (EvalType::Decimal, EvalType::Duration) => cast_decimal_as_duration_fn_meta(),
        (EvalType::DateTime, EvalType::Duration) => cast_any_as_any_fn_meta::<DateTime, Duration>(),
        (EvalType::Json, EvalType::Duration) => cast_json_as_duration_fn_meta(),
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

// cast any as int/uint, some cast functions reuse `cast_any_as_any`
//
// - cast_real_as_int -> cast_any_as_any<Real, Int>
// - cast_decimal_as_int -> cast_any_as_any<Decimal, Int>
// - cast_time_as_int_or_uint -> cast_any_as_any<Time, Int>
// - cast_duration_as_int_or_uint -> cast_any_as_any<Duration, Int>
// - cast_json_as_int -> cast_any_as_any<Json, Int>

#[rpn_fn(capture = [extra])]
#[inline]
fn cast_signed_int_as_unsigned_int(
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Int>,
) -> Result<Option<Int>> {
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

#[rpn_fn]
#[inline]
fn cast_int_as_int_others(val: &Option<Int>) -> Result<Option<Int>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(*val)),
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_real_as_uint(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Real>,
) -> Result<Option<Int>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.into_inner();
            if in_union(&extra.implicit_args) && val < 0f64 {
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

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_string_as_int_or_uint(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Bytes>,
) -> Result<Option<Int>> {
    match val {
        None => Ok(None),
        Some(val) => {
            // TODO: in TiDB, if `b.args[0].GetType().Hybrid()` || `IsBinaryLiteral(b.args[0])`,
            //  then it will return res from EvalInt() directly.
            let is_unsigned = extra.ret_field_type.is_unsigned();
            let val = get_valid_utf8_prefix(ctx, val.as_slice())?;
            let val = val.trim();
            let is_str_neg = val.starts_with('-');
            if in_union(extra.implicit_args) && is_unsigned && is_str_neg {
                Ok(Some(0))
            } else {
                let valid_int_prefix = get_valid_int_prefix(ctx, val)?;
                let parse_res = if !is_str_neg {
                    valid_int_prefix.parse::<u64>().map(|x| x as i64)
                } else {
                    valid_int_prefix.parse::<i64>()
                };
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
                    Err(err) => {
                        if err.kind() == &IntErrorKind::Overflow {
                            let err = if is_str_neg {
                                Error::overflow("BIGINT UNSIGNED", valid_int_prefix)
                            } else {
                                Error::overflow("BIGINT", valid_int_prefix)
                            };
                            ctx.handle_overflow_err(err)?;
                            let val = if is_str_neg {
                                std::i64::MIN
                            } else {
                                std::u64::MAX as i64
                            };
                            Ok(Some(val))
                        } else {
                            Err(other_err!("parse string to int failed: {}", err))
                        }
                    }
                }
            }
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_decimal_as_uint(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Decimal>,
) -> Result<Option<Int>> {
    match val {
        None => Ok(None),
        Some(val) => {
            // TODO: here TiDB round before call `val.is_negative()`
            if in_union(extra.implicit_args) && val.is_negative() {
                Ok(Some(0))
            } else {
                let r: u64 = val.convert(ctx)?;
                Ok(Some(r as i64))
            }
        }
    }
}

#[rpn_fn(capture = [ctx])]
#[inline]
fn cast_json_as_uint(ctx: &mut EvalContext, val: &Option<Json>) -> Result<Option<Int>> {
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

#[rpn_fn]
#[inline]
fn cast_signed_int_as_signed_real(val: &Option<Int>) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Real::new(*val as f64).ok()),
    }
}

#[rpn_fn(capture = [extra])]
#[inline]
fn cast_signed_int_as_unsigned_real(
    extra: &RpnFnCallExtra,
    val: &Option<Int>,
) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            if in_union(extra.implicit_args) && *val < 0 {
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
#[rpn_fn]
#[inline]
fn cast_unsigned_int_as_signed_or_unsigned_real(val: &Option<Int>) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Real::new(*val as u64 as f64).ok()),
    }
}

#[rpn_fn]
#[inline]
fn cast_real_as_signed_real(val: &Option<Real>) -> Result<Option<Real>> {
    Ok(*val)
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
                // FIXME: negative number to unsigned real's logic may be wrong here.
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
            // FIXME: in TiDB's builtinCastStringAsRealSig, if val is IsBinaryLiteral,
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
            // FIXME: in TiDB's builtinCastStringAsRealSig, if val is IsBinaryLiteral,
            //  then return evalReal directly
            let mut r: f64 = val.convert(ctx)?;
            if in_union(extra.implicit_args) && r < 0f64 {
                r = 0f64;
            }
            let r = produce_float_with_specified_tp(ctx, extra.ret_field_type, r)?;
            // FIXME: negative number to unsigned real's logic may be wrong here.
            Ok(Real::new(r).ok())
        }
    }
}

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
                // FIXME: negative number to unsigned real's logic may be wrong here.
                Ok(Some(val.convert(ctx)?))
            }
        }
    }
}

/// The unsigned int implementation for push down signature `CastIntAsDecimal`.
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
            // TODO: TiDB's uint to decimal seems has bug, fix this after fix TiDB's
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

/// The signed int implementation for push down signature `CastIntAsDecimal`.
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

/// The implementation for push down signature `CastIntAsString` from unsigned integer.
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
            let res = produce_str_with_specified_tp(
                ctx,
                Cow::Borrowed(p.as_slice()),
                &extra.ret_field_type,
                false,
            )?;
            let mut res = match res {
                Cow::Borrowed(_) => p,
                Cow::Owned(x) => x.to_vec(),
            };
            pad_zero_for_binary_type(&mut res, &extra.ret_field_type);
            Ok(Some(res))
        }
    }
}

/// The implementation for push down signature `CastIntAsJson` from unsigned integer.
#[rpn_fn]
#[inline]
pub fn cast_uint_as_json(val: &Option<Int>) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(Json::U64(*val as u64))),
    }
}

#[rpn_fn]
#[inline]
pub fn cast_int_as_json_boolean(val: &Option<Int>) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(Json::Boolean(*val != 0))),
    }
}

#[rpn_fn(capture = [extra])]
#[inline]
pub fn cast_string_as_json(
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Bytes>,
) -> Result<Option<Json>> {
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

/// The implementation for push down signature `CastIntAsDuration`
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
            let dur = Duration::from_i64(ctx, *val, extra.ret_field_type.get_decimal() as u8)?;
            Ok(Some(dur))
        }
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

#[cfg(test)]
mod tests {
    use super::Result;
    use crate::codec::data_type::{Decimal, Int, Real, ScalarValue};
    use crate::codec::error::*;
    use crate::codec::mysql::{Duration, Json, Time};
    use crate::expr::Flag;
    use crate::expr::{EvalConfig, EvalContext};
    use crate::rpn_expr::impl_cast::*;
    use crate::rpn_expr::RpnFnCallExtra;
    use bitfield::fmt::Display;
    use std::collections::BTreeMap;
    use std::fmt::{Debug, Formatter};
    use std::sync::Arc;
    use std::{f32, f64, i64, u64};
    use tidb_query_datatype::{FieldTypeFlag, UNSPECIFIED_LENGTH};

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

    fn test_none_with_ctx_and_extra<F, Input, Ret>(func: F)
    where
        F: Fn(&mut EvalContext, &RpnFnCallExtra, &Option<Input>) -> Result<Option<Ret>>,
    {
        let mut ctx = EvalContext::default();
        let implicit_args = [ScalarValue::Int(Some(1))];
        let ret_field_type: FieldType = FieldType::default();
        let extra = RpnFnCallExtra {
            ret_field_type: &ret_field_type,
            implicit_args: &implicit_args,
        };
        let r = func(&mut ctx, &extra, &None).unwrap();
        assert!(r.is_none());
    }

    fn test_none_with_ctx<F, Input, Ret>(func: F)
    where
        F: Fn(&mut EvalContext, &Option<Input>) -> Result<Option<Ret>>,
    {
        let mut ctx = EvalContext::default();
        let r = func(&mut ctx, &None).unwrap();
        assert!(r.is_none());
    }

    fn test_none_with_extra<F, Input, Ret>(func: F)
    where
        F: Fn(&RpnFnCallExtra, &Option<Input>) -> Result<Option<Ret>>,
    {
        let implicit_args = [ScalarValue::Int(Some(1))];
        let ret_field_type: FieldType = FieldType::default();
        let extra = RpnFnCallExtra {
            ret_field_type: &ret_field_type,
            implicit_args: &implicit_args,
        };
        let r = func(&extra, &None).unwrap();
        assert!(r.is_none());
    }

    fn test_none_with_nothing<F, Input, Ret>(func: F)
    where
        F: Fn(&Option<Input>) -> Result<Option<Ret>>,
    {
        let r = func(&None).unwrap();
        assert!(r.is_none());
    }

    fn make_ctx(
        overflow_as_warning: bool,
        truncate_as_warning: bool,
        should_clip_to_zero: bool,
    ) -> EvalContext {
        let mut flag: Flag = Flag::empty();
        if overflow_as_warning {
            flag |= Flag::OVERFLOW_AS_WARNING;
        }
        if truncate_as_warning {
            flag |= Flag::TRUNCATE_AS_WARNING;
        }
        if should_clip_to_zero {
            flag |= Flag::IN_INSERT_STMT;
        }
        let cfg = Arc::new(EvalConfig::from_flag(flag));
        EvalContext::new(cfg)
    }

    fn make_implicit_args(in_union: bool) -> [ScalarValue; 1] {
        if in_union {
            [ScalarValue::Int(Some(1))]
        } else {
            [ScalarValue::Int(Some(0))]
        }
    }

    fn make_ret_field_type(unsigned: bool) -> FieldType {
        let mut ft = if unsigned {
            let mut ft = FieldType::default();
            ft.as_mut_accessor().set_flag(FieldTypeFlag::UNSIGNED);
            ft
        } else {
            FieldType::default()
        };
        let fta = ft.as_mut_accessor();
        fta.set_flen(UNSPECIFIED_LENGTH);
        fta.set_decimal(UNSPECIFIED_LENGTH);
        ft
    }

    fn make_ret_field_type_2(unsigned: bool, flen: isize, decimal: isize) -> FieldType {
        let mut ft = make_ret_field_type(unsigned);
        let fta = ft.as_mut_accessor();
        fta.set_flen(flen);
        fta.set_decimal(decimal);
        ft
    }

    fn make_extra<'a>(
        ret_field_type: &'a FieldType,
        implicit_args: &'a [ScalarValue],
    ) -> RpnFnCallExtra<'a> {
        RpnFnCallExtra {
            ret_field_type,
            implicit_args,
        }
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
            assert_eq!(
                ctx.warnings.warning_cnt, 1,
                "{}, {:?}",
                log, ctx.warnings.warnings
            );
            assert_eq!(
                ctx.warnings.warnings[0].get_code(),
                ERR_DATA_OUT_OF_RANGE,
                "{}",
                log
            );
        } else {
            assert_eq!(ctx.warnings.warning_cnt, 0, "{}", log);
        }
    }

    fn check_warning(ctx: &EvalContext, err_code: Option<i32>, log: &str) {
        if let Some(x) = err_code {
            assert_eq!(
                ctx.warnings.warning_cnt, 1,
                "{}, warnings: {:?}",
                log, ctx.warnings.warnings
            );
            assert_eq!(ctx.warnings.warnings[0].get_code(), x, "{}", log);
        }
    }

    fn check_warning_2(ctx: &EvalContext, err_code: Vec<i32>, log: &str) {
        assert_eq!(
            ctx.warnings.warning_cnt,
            err_code.len(),
            "{}, warnings: {:?}",
            log,
            ctx.warnings.warnings
        );
        for i in 0..err_code.len() {
            let e1 = err_code[i];
            let e2 = ctx.warnings.warnings[i].get_code();
            assert_eq!(
                e1, e2,
                "log: {}, ctx_warnings: {:?}, expect_warning: {:?}",
                log, ctx.warnings.warnings, err_code
            );
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

    impl Display for Json {
        fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "{}", self.to_string())
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
            let r = cast_int_as_int_others(&Some(input));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_signed_int_as_unsigned_int() {
        test_none_with_extra(cast_signed_int_as_unsigned_int);

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
            let rtf = make_ret_field_type(true);
            let ia = make_implicit_args(in_union);
            let extra = make_extra(&rtf, &ia);
            let r = cast_signed_int_as_unsigned_int(&extra, &Some(input));
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
            let mut ctx = make_ctx(true, false, false);
            let r = cast_any_as_any::<Real, Int>(&mut ctx, &Real::new(input).ok());
            let log = make_log(&input, &result, &r);
            check_result(Some(&result), &r, log.as_str());
            check_overflow(&ctx, overflow, log.as_str());
        }
    }

    #[test]
    fn test_real_as_uint() {
        test_none_with_ctx_and_extra(cast_real_as_uint);

        // in_union
        let cs = vec![
            // (input, expect)
            (-10.0, 0u64),
            (i64::MIN as f64, 0),
            (10.0, 10u64),
            (i64::MAX as f64, (1u64 << 63)),
        ];

        for (input, expect) in cs {
            let mut ctx = make_ctx(false, false, false);
            let rtf = make_ret_field_type(true);
            let ia = make_implicit_args(true);
            let extra = make_extra(&rtf, &ia);
            let r = cast_real_as_uint(&mut ctx, &extra, &Some(Real::new(input).unwrap()));
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
            let mut ctx = make_ctx(true, false, false);
            let ia = make_implicit_args(false);
            let rtf = make_ret_field_type(true);
            let extra = make_extra(&rtf, &ia);
            let r = cast_real_as_uint(&mut ctx, &extra, &Real::new(input).ok());
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
            let mut ctx = make_ctx(true, false, true);
            let ia = make_implicit_args(false);
            let rft = make_ret_field_type(true);
            let extra = make_extra(&rft, &ia);
            let r = cast_real_as_uint(&mut ctx, &extra, &Some(Real::new(input).unwrap()));
            let r = r.map(|x| x.map(|x| x as u64));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
            check_overflow(&ctx, overflow, log.as_str())
        }
    }

    #[test]
    fn test_string_as_int_or_uint() {
        test_none_with_ctx_and_extra(cast_string_as_int_or_uint);

        #[derive(Debug)]
        enum Cond {
            None,
            Unsigned,
            InSelectStmt,
            InUnionAndUnsigned,
        }
        impl Cond {
            fn in_union(&self) -> bool {
                if let Cond::InUnionAndUnsigned = self {
                    true
                } else {
                    false
                }
            }
            fn is_unsigned(&self) -> bool {
                match self {
                    Cond::InUnionAndUnsigned | Cond::Unsigned => true,
                    _ => false,
                }
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
            //  fix this cast_string_as_int_or_uint after fix TiDB's
            // ("18446744073709551616", 18446744073709551615 as i64, Some(ERR_TRUNCATE_WRONG_VALUE) , Cond::Unsigned)
            // FIXME: our cast_string_as_int_or_uint's err handle is not exactly same as TiDB's
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
            // FIXME: if not is select stmt, should has this err code ERR_DATA_OUT_OF_RANGE, too.
            // ("-9223372036854775809", -9223372036854775808i64, Some(ERR_DATA_OUT_OF_RANGE), Cond::None),
            // FIXME: our cast_string_as_int_or_uint's err handle is not exactly same as TiDB's
            // ("-9223372036854775809", -9223372036854775808i64, Some(ERR_DATA_OUT_OF_RANGE), Cond::InSelectStmt),
            ("-10", -10i64, vec![ERR_UNKNOWN], Cond::Unsigned),
            // FIXME: our cast_string_as_int_or_uint's err handle is not exactly same as TiDB's
            // ("-9223372036854775808", -9223372036854775808i64, Some(ERR_UNKNOWN), Cond::Unsigned),
            // FIXME: if not is select stmt, should has this err code ERR_DATA_OUT_OF_RANGE, too.
            // ("-9223372036854775809", -9223372036854775808i64, Some(ERR_DATA_OUT_OF_RANGE), Cond::Unsigned),
            // FIXME: our cast_string_as_int_or_uint's err handle is not exactly same as TiDB's
            // ("-9223372036854775809", -9223372036854775808i64, Some(ERR_DATA_OUT_OF_RANGE), Cond::Unsigned),
        ];

        for (input, expect, err_code, cond) in cs {
            let mut ctx = if let Cond::InSelectStmt = cond {
                let mut flag: Flag = Flag::empty();
                flag |= Flag::OVERFLOW_AS_WARNING;
                flag |= Flag::TRUNCATE_AS_WARNING;
                flag |= Flag::IN_SELECT_STMT;
                let cfg = Arc::new(EvalConfig::from_flag(flag));
                EvalContext::new(cfg)
            } else {
                make_ctx(true, true, false)
            };
            let ia = make_implicit_args(cond.in_union());
            let rft = make_ret_field_type(cond.is_unsigned());
            let extra = make_extra(&rft, &ia);

            let val = Some(Vec::from(input.as_bytes()));
            let r = cast_string_as_int_or_uint(&mut ctx, &extra, &val);

            let log = format!(
                "input: {}, expect: {}, expect_err_code: {:?}, cond: {:?}, output: {:?}",
                input, expect, err_code, cond, r
            );
            check_result(Some(&expect), &r, log.as_str());
            check_warning_2(&ctx, err_code, log.as_str());
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
            let mut ctx = make_ctx(true, false, false);
            let r = cast_any_as_any::<Decimal, Int>(&mut ctx, &Some(input.clone()));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
            check_warning(&ctx, err_code, log.as_str());
        }
    }

    #[test]
    fn test_decimal_as_uint() {
        test_none_with_ctx_and_extra(cast_decimal_as_uint);
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
            let mut ctx = make_ctx(false, false, false);
            let ia = make_implicit_args(true);
            let rft = make_ret_field_type(true);
            let extra = make_extra(&rft, &ia);

            let r = cast_decimal_as_uint(&mut ctx, &extra, &Some(input.clone()));
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
            let mut ctx = make_ctx(true, false, false);
            let ia = make_implicit_args(false);
            let rft = make_ret_field_type(true);
            let extra = make_extra(&rft, &ia);

            let r = cast_decimal_as_uint(&mut ctx, &extra, &Some(input.clone()));
            let r = r.map(|x| x.map(|x| x as u64));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
            check_warning(&ctx, err_code, log.as_str());
        }
    }

    #[test]
    fn test_time_as_int_and_uint() {
        // TODO: add more test case
        // TODO: add test that make cast_any_as_any::<Time, Int> returning truncated error
        let cs: Vec<(Time, i64)> = vec![
            (
                Time::parse_utc_datetime("2000-01-01T12:13:14", 0).unwrap(),
                20000101121314,
            ),
            (
                Time::parse_utc_datetime("2000-01-01T12:13:14.6666", 0).unwrap(),
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
            let mut ctx = EvalContext::default();
            let r = cast_any_as_any::<Time, Int>(&mut ctx, &Some(input.clone()));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_duration_as_int() {
        // TODO: add more test case
        let cs: Vec<(Duration, i64)> = vec![
            (Duration::parse(b"17:51:04.78", 2).unwrap(), 175105),
            (Duration::parse(b"-17:51:04.78", 2).unwrap(), -175105),
            (Duration::parse(b"17:51:04.78", 0).unwrap(), 175105),
            (Duration::parse(b"-17:51:04.78", 0).unwrap(), -175105),
        ];

        for (input, expect) in cs {
            let mut ctx = make_ctx(true, false, false);
            let r = cast_any_as_any::<Duration, Int>(&mut ctx, &Some(input));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_json_as_int() {
        test_none_with_ctx(cast_any_as_any::<Json, Int>);

        // no overflow
        let cs = vec![
            // (origin, expect, overflow)
            (Json::Object(BTreeMap::default()), 0, false),
            (Json::Array(vec![]), 0, false),
            (Json::I64(10), 10i64, false),
            (Json::I64(i64::MAX), i64::MAX, false),
            (Json::I64(i64::MIN), i64::MIN, false),
            (Json::U64(0), 0, false),
            (Json::U64(u64::MAX), u64::MAX as i64, false),
            (Json::Double(i64::MIN as u64 as f64), i64::MAX, false),
            (Json::Double(i64::MAX as u64 as f64), i64::MAX, false),
            (Json::Double(i64::MIN as u64 as f64), i64::MAX, false),
            (Json::Double(i64::MIN as f64), i64::MIN, false),
            (Json::Double(10.5), 11, false),
            (Json::Double(10.4), 10, false),
            (Json::Double(-10.4), -10, false),
            (Json::Double(-10.5), -11, false),
            (Json::String(String::from("10.0")), 10, false),
            (Json::Boolean(true), 1, false),
            (Json::Boolean(false), 0, false),
            (Json::None, 0, false),
            (
                Json::Double(((1u64 << 63) + (1u64 << 62)) as u64 as f64),
                i64::MAX,
                true,
            ),
            (
                Json::Double(-((1u64 << 63) as f64 + (1u64 << 62) as f64)),
                i64::MIN,
                true,
            ),
        ];

        for (input, expect, overflow) in cs {
            let mut ctx = make_ctx(true, false, false);
            let r = cast_any_as_any::<Json, Int>(&mut ctx, &Some(input.clone()));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
            check_overflow(&ctx, overflow, log.as_str());
        }
    }

    #[test]
    fn test_json_as_uint() {
        // no clip to zero
        let cs: Vec<(Json, u64, Option<i32>)> = vec![
            // (origin, expect, error_code)
            (Json::Double(-1.0), -1.0f64 as i64 as u64, None),
            (Json::String(String::from("10")), 10, None),
            (
                Json::String(String::from("+10abc")),
                10,
                Some(ERR_TRUNCATE_WRONG_VALUE),
            ),
            (
                Json::String(String::from("9999999999999999999999999")),
                u64::MAX,
                Some(ERR_DATA_OUT_OF_RANGE),
            ),
            (
                Json::Double(2f64 * (u64::MAX as f64)),
                u64::MAX,
                Some(ERR_DATA_OUT_OF_RANGE),
            ),
        ];

        for (input, expect, error_code) in cs {
            let mut ctx = make_ctx(true, true, false);
            let r = cast_json_as_uint(&mut ctx, &Some(input.clone()));
            let r = r.map(|x| x.map(|x| x as u64));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
            check_warning(&ctx, error_code, log.as_str());
        }

        // should clip to zero
        let cs: Vec<(Json, u64, Option<i32>)> = vec![
            // (origin, expect, err_code)
            (Json::Double(-1.0), 0, None),
            (
                Json::String(String::from("-10")),
                0,
                Some(ERR_DATA_OUT_OF_RANGE),
            ),
            (Json::String(String::from("10")), 10, None),
            (
                Json::String(String::from("+10abc")),
                10,
                Some(ERR_TRUNCATE_WRONG_VALUE),
            ),
            (
                Json::String(String::from("9999999999999999999999999")),
                u64::MAX,
                Some(ERR_DATA_OUT_OF_RANGE),
            ),
            (
                Json::Double(2f64 * (u64::MAX as f64)),
                u64::MAX,
                Some(ERR_DATA_OUT_OF_RANGE),
            ),
        ];

        for (input, expect, err_code) in cs {
            let mut ctx = make_ctx(true, true, true);
            let r = cast_json_as_uint(&mut ctx, &Some(input.clone()));
            let r = r.map(|x| x.map(|x| x as u64));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
            check_warning(&ctx, err_code, log.as_str());
        }
    }

    #[test]
    fn tes_signed_int_as_signed_real() {
        test_none_with_nothing(cast_signed_int_as_signed_real);

        let cs: Vec<(i64, f64)> = vec![
            // (input, expect)
            (i64::MIN, i64::MIN as f64),
            (0, 0f64),
            (i64::MAX, i64::MAX as f64),
        ];

        for (input, expect) in cs {
            let r = cast_signed_int_as_signed_real(&Some(input));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_signed_int_as_unsigned_real() {
        test_none_with_extra(cast_signed_int_as_unsigned_real);

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
            let ia = make_implicit_args(in_union);
            let rft = make_ret_field_type(true);
            let extra = make_extra(&rft, &ia);
            let r = cast_signed_int_as_unsigned_real(&extra, &Some(input));
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
            let r = cast_unsigned_int_as_signed_or_unsigned_real(&Some(input as i64));
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
            let r = cast_real_as_signed_real(&Some(Real::new(input).unwrap()));
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
            let ia = make_implicit_args(in_union);
            let rft = make_ret_field_type(true);
            let extra = make_extra(&rft, &ia);
            let r = cast_real_as_unsigned_real(&extra, &Some(Real::new(input).unwrap()));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            let log = format!(
                "input: {}, expect: {}, in_union: {}",
                input, expect, in_union
            );
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_string_as_signed_real() {
        test_none_with_ctx_and_extra(cast_string_as_signed_real);

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
        ];

        for (input, expect, flen, decimal, truncated, overflow) in cs {
            let mut ctx = make_ctx(true, true, false);
            let ia = make_implicit_args(false);
            let rft = make_ret_field_type_2(false, flen, decimal);
            let extra = make_extra(&rft, &ia);
            let r = cast_string_as_signed_real(&mut ctx, &extra, &Some(input.clone().into_bytes()));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            let log = format!(
                "input: {}, expect: {}, flen: {}, decimal: {}, expect_truncated: {}, expect_overflow: {}",
                input.as_str(), expect, flen, decimal, truncated, overflow
            );
            check_result(Some(&expect), &r, log.as_str());
            match (truncated, overflow) {
                (true, true) => {
                    assert_eq!(ctx.warnings.warning_cnt, 2, "{}", log.as_str());
                    let a = ctx.warnings.warnings[0].get_code();
                    let b = ctx.warnings.warnings[1].get_code();
                    let (a, b) = if a > b { (b, a) } else { (a, b) };
                    assert_eq!(a, ERR_TRUNCATE_WRONG_VALUE, "{}", log.as_str());
                    assert_eq!(b, ERR_DATA_OUT_OF_RANGE, "{}", log.as_str());
                }
                (true, false) => check_warning(&ctx, Some(ERR_TRUNCATE_WRONG_VALUE), log.as_str()),
                (false, true) => check_overflow(&ctx, true, log.as_str()),
                _ => (),
            }
        }
    }

    #[test]
    fn test_string_as_unsigned_real() {
        test_none_with_ctx_and_extra(cast_string_as_unsigned_real);

        let ul = UNSPECIFIED_LENGTH;
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

        for (input, expect, flen, decimal, truncated, overflow, in_union) in cs {
            let mut ctx = make_ctx(true, true, false);
            let ia = make_implicit_args(in_union);
            let rft = make_ret_field_type_2(true, flen, decimal);
            let extra = make_extra(&rft, &ia);

            let p = Some(input.clone().into_bytes());
            let r = cast_string_as_unsigned_real(&mut ctx, &extra, &p);
            let r = r.map(|x| x.map(|x| x.into_inner()));

            let log = format!(
                "input: {}, expect: {}, flen: {}, decimal: {}, expect_truncated: {}, expect_overflow: {}, in_union: {}",
                input.as_str(), expect, flen, decimal, truncated, overflow, in_union
            );

            check_result(Some(&expect), &r, log.as_str());
            match (truncated, overflow) {
                (true, true) => {
                    assert_eq!(ctx.warnings.warning_cnt, 2, "{}", log.as_str());
                    let a = ctx.warnings.warnings[0].get_code();
                    let b = ctx.warnings.warnings[1].get_code();
                    let (a, b) = if a > b { (b, a) } else { (a, b) };
                    assert_eq!(a, ERR_TRUNCATE_WRONG_VALUE, "{}", log.as_str());
                    assert_eq!(b, ERR_DATA_OUT_OF_RANGE, "{}", log.as_str());
                }
                (true, false) => check_warning(&ctx, Some(ERR_TRUNCATE_WRONG_VALUE), log.as_str()),
                (false, true) => check_overflow(&ctx, true, log.as_str()),
                _ => (),
            }
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
        for (input, expect, flen, decimal, err_codes) in cs {
            let mut ctx = make_ctx(true, true, false);
            let ia = make_implicit_args(false);
            let rft = make_ret_field_type_2(true, flen, decimal);
            let extra = make_extra(&rft, &ia);

            let p = Some(input.clone().into_bytes());
            let r = cast_string_as_unsigned_real(&mut ctx, &extra, &p);
            let r = r.map(|x| x.map(|x| x.into_inner()));
            let log = format!(
                "input: {}, expect: {}, flen: {}, decimal: {}, err_code: {:?}",
                input.as_str(),
                expect,
                flen,
                decimal,
                err_codes
            );
            check_result(Some(&expect), &r, log.as_str());
            assert_eq!(
                ctx.warnings.warning_cnt,
                err_codes.len(),
                "{}",
                log.as_str()
            );
            for (idx, err) in err_codes.iter().enumerate() {
                assert_eq!(
                    ctx.warnings.warnings[idx].get_code(),
                    *err,
                    "{}",
                    log.as_str()
                )
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
            let mut ctx = make_ctx(false, false, false);
            let r = cast_any_as_any::<Decimal, Real>(&mut ctx, &Some(input.clone()));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_decimal_as_unsigned_real() {
        test_none_with_ctx_and_extra(cast_decimal_as_unsigned_real);

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
            let mut ctx = make_ctx(true, false, false);
            let ia = make_implicit_args(in_union);
            let rft = make_ret_field_type(true);
            let extra = make_extra(&rft, &ia);
            let r = cast_decimal_as_unsigned_real(&mut ctx, &extra, &Some(input.clone()));
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
    fn test_time_as_real() {
        test_none_with_ctx(cast_any_as_any::<Time, Real>);

        // TODO: add more test case
        let cs = vec![
            (
                Time::parse_utc_datetime("2000-01-01T12:13:14.6666", 6).unwrap(),
                20000101121314.666600,
            ),
            (
                Time::parse_utc_datetime("2000-01-01T12:13:14.6666", 0).unwrap(),
                20000101121315.0,
            ),
            (
                Time::parse_utc_datetime("2000-01-01T12:13:14.6666", 3).unwrap(),
                20000101121314.667,
            ),
            (
                Time::parse_utc_datetime("2000-01-01T12:13:14.6666", 4).unwrap(),
                20000101121314.6666,
            ),
        ];

        for (input, expect) in cs {
            let mut ctx = make_ctx(false, false, false);
            let r = cast_any_as_any::<Time, Real>(&mut ctx, &Some(input.clone()));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_duration_as_real() {
        // TODO: add more test case
        let cs = vec![
            // (input, expect)
            (Duration::parse(b"17:51:04.78", 2).unwrap(), 175104.78),
            (Duration::parse(b"-17:51:04.78", 2).unwrap(), -175104.78),
            (Duration::parse(b"17:51:04.78", 0).unwrap(), 175105.0),
            (Duration::parse(b"-17:51:04.78", 0).unwrap(), -175105.0),
        ];
        for (input, expect) in cs {
            let mut ctx = make_ctx(false, false, false);
            let r = cast_any_as_any::<Duration, Real>(&mut ctx, &Some(input));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_json_as_real() {
        let cs: Vec<(Json, f64, Option<i32>)> = vec![
            // (input, expect, err_code)
            (Json::Object(BTreeMap::default()), 0f64, None),
            (Json::Array(vec![]), 0f64, None),
            (Json::I64(10), 10f64, None),
            (Json::I64(i64::MAX), i64::MAX as f64, None),
            (Json::I64(i64::MIN), i64::MIN as f64, None),
            (Json::U64(0), 0f64, None),
            (Json::U64(u64::MAX), u64::MAX as f64, None),
            (Json::Double(f64::MAX), f64::MAX, None),
            (Json::Double(f64::MIN), f64::MIN, None),
            (Json::String(String::from("10.0")), 10.0, None),
            (Json::String(String::from("-10.0")), -10.0, None),
            (Json::Boolean(true), 1f64, None),
            (Json::Boolean(false), 0f64, None),
            (Json::None, 0f64, None),
            (
                Json::String((0..500).map(|_| '9').collect::<String>()),
                f64::MAX,
                Some(ERR_TRUNCATE_WRONG_VALUE),
            ),
            (
                Json::String(
                    (0..500)
                        .map(|x| if x == 0 { '-' } else { '9' })
                        .collect::<String>(),
                ),
                f64::MIN,
                Some(ERR_TRUNCATE_WRONG_VALUE),
            ),
        ];

        for (input, expect, err_code) in cs {
            let mut ctx = make_ctx(false, true, false);
            let r = cast_any_as_any::<Json, Real>(&mut ctx, &Some(input.clone()));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
            check_warning(&ctx, err_code, log.as_str());
        }
    }
}
