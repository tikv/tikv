// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;
use std::convert::TryFrom;

use tidb_query_codegen::rpn_fn;
use tidb_query_datatype::*;
use tipb::{Expr, FieldType};

use crate::codec::convert::*;
use crate::codec::data_type::*;
use crate::codec::error::{ERR_DATA_OUT_OF_RANGE, WARN_DATA_TRUNCATED};
use crate::codec::mysql::Time;
use crate::expr::EvalContext;
use crate::rpn_expr::{RpnExpressionNode, RpnFnCallExtra, RpnFnMeta};
use crate::Result;

fn get_cast_fn_rpn_meta(
    from_field_type: &FieldType,
    to_field_type: &FieldType,
) -> Result<RpnFnMeta> {
    let from = box_try!(EvalType::try_from(from_field_type.as_accessor().tp()));
    let to = box_try!(EvalType::try_from(to_field_type.as_accessor().tp()));
    let func_meta = match (from, to) {
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
            if !to_field_type.is_unsigned() {
                cast_any_as_decimal_fn_meta::<Bytes>()
            } else {
                cast_string_as_unsigned_decimal_fn_meta()
            }
        }
        (EvalType::Decimal, EvalType::Decimal) => {
            if !to_field_type.is_unsigned() {
                cast_decimal_as_signed_decimal_fn_meta()
            } else {
                cast_decimal_as_unsigned_decimal_fn_meta()
            }
        }
        (EvalType::DateTime, EvalType::Decimal) => cast_any_as_decimal_fn_meta::<DateTime>(),
        (EvalType::Duration, EvalType::Decimal) => cast_any_as_decimal_fn_meta::<Duration>(),
        (EvalType::Json, EvalType::Decimal) => cast_any_as_decimal_fn_meta::<Json>(),

        (EvalType::Int, EvalType::Int) => {
            match (from_field_type.is_unsigned(), to_field_type.is_unsigned()) {
                (false, false) => cast_any_as_any_fn_meta::<Int, Int>(),
                (false, true) => cast_int_as_uint_fn_meta(),
                (true, false) => cast_uint_as_int_fn_meta(),
                (true, true) => cast_uint_as_uint_fn_meta(),
            }
        }
        (EvalType::Real, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Real, Int>()
            } else {
                cast_float_as_uint_fn_meta()
            }
        }
        (EvalType::Bytes, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Bytes, Int>()
            } else {
                cast_bytes_as_uint_fn_meta()
            }
        }
        (EvalType::Decimal, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Decimal, Int>()
            } else {
                cast_decimal_as_uint_fn_meta()
            }
        }
        (EvalType::DateTime, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<DateTime, Int>()
            } else {
                cast_datetime_as_uint_fn_meta()
            }
        }
        (EvalType::Duration, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Duration, Int>()
            } else {
                cast_duration_as_uint_fn_meta()
            }
        }
        (EvalType::Json, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Json, Int>()
            } else {
                cast_json_as_uint_fn_meta()
            }
        }
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

// cast any as decimal, some cast functions reuse `cast_any_as_decimal`
//
// - cast_signed_int_as_signed_decimal -> cast_any_as_decimal<Int>
// - cast_string_as_signed_decimal -> cast_any_as_decimal<Bytes>
// - cast_time_as_decimal -> cast_any_as_decimal<Time>
// - cast_duration_as_decimal -> cast_any_as_decimal<Duration>
// - cast_json_as_decimal -> cast_any_as_decimal<Json>

// because uint's upper bound is smaller than signed decimal's upper bound
// so we can merge cast uint as signed/unsigned decimal in this function
#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_unsigned_int_as_signed_or_unsigned_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
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

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_signed_int_as_unsigned_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<i64>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let dec = if in_union(extra.implicit_args) && *val < 0 {
                Decimal::zero()
            } else {
                // FIXME, here TiDB has bug, fix this after fix TiDB's
                // if val is >=0, then val as u64 is ok,
                // if val <0, there may be bug here.
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

// FIXME, here TiDB may has bug, fix this after fix TiDB's
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

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_string_as_unsigned_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Bytes>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            // FIXME, in TiDB, if the param IsBinaryLiteral, then return the result of `evalDecimal` directly
            let d: Decimal = val.convert(ctx)?;
            let d = if in_union(extra.implicit_args) && d.is_negative() {
                Decimal::zero()
            } else {
                d
            };
            // FIXME, how to make a negative decimal value to unsigned decimal value
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
fn cast_decimal_as_signed_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Decimal>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.clone();
            Ok(Some(produce_dec_with_specified_tp(
                ctx,
                val,
                extra.ret_field_type,
            )?))
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_decimal_as_unsigned_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Decimal>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let res = if in_union(extra.implicit_args) && val.is_negative() {
                Decimal::zero()
            } else {
                // FIXME, here TiDB may has bug, fix this after fix TiDB's
                // FIXME, how to make a unsigned decimal from negative decimal
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

// FIXME, for cast_int_as_decimal, TiDB's impl has bug, fix this after fixed TiDB's
#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_any_as_decimal<From: Evaluable + ConvertTo<Decimal>>(
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

#[rpn_fn]
#[inline]
fn cast_uint_as_int(val: &Option<Int>) -> Result<Option<i64>> {
    match val {
        None => Ok(None),
        Some(val) => {
            // the val is uint, so it will never < 0,
            // then we needn't to check whether in_union.
            //
            // needn't to call convert_uint_as_int
            Ok(Some(*val as i64))
        }
    }
}

macro_rules! cast_as_unsigned_integer {
    ($ty:ty, $as_uint_fn:ident) => {
        cast_as_unsigned_integer!(_inner, $ty, $as_uint_fn, val,);
    };
    ($ty:ty, $as_uint_fn:ident, $extra:expr) => {
        cast_as_unsigned_integer!(_inner, $ty, $as_uint_fn, $extra,);
    };
    ($ty:ty, $as_uint_fn:ident, $extra:expr, $($hook:tt)*) => {
        cast_as_unsigned_integer!(_inner, $ty, $as_uint_fn, $extra, $($hook)*);
    };
    (_inner, $ty:ty, $as_uint_fn:ident, $extra:expr, $($hook:tt)*) => {
        #[rpn_fn(capture = [ctx, extra])]
        #[inline]
        #[allow(unused)]
        pub fn $as_uint_fn(
            ctx: &mut EvalContext,
            extra: &RpnFnCallExtra<'_>,
            val: &Option<$ty>,
        ) -> Result<Option<i64>> {
            match val {
                None => Ok(None),
                Some(val) => {
                    $($hook)*;
                    let val = ($extra).to_uint(ctx, FieldTypeTp::LongLong)?;
                    Ok(Some(val as i64))
                }
            }
        }
    };
}

cast_as_unsigned_integer!(
    Int,
    cast_int_as_uint,
    *val,
    if *val < 0 && in_union(extra.implicit_args) {
        return Ok(Some(0));
    }
);
cast_as_unsigned_integer!(Int, cast_uint_as_uint, *val as u64);
cast_as_unsigned_integer!(
    Real,
    cast_float_as_uint,
    val.into_inner(),
    if val.into_inner() < 0f64 && in_union(extra.implicit_args) {
        return Ok(Some(0));
    }
);
cast_as_unsigned_integer!(Bytes, cast_bytes_as_uint);
cast_as_unsigned_integer!(
    Decimal,
    cast_decimal_as_uint,
    val,
    if val.is_negative() && in_union(extra.implicit_args) {
        return Ok(Some(0));
    }
);
cast_as_unsigned_integer!(DateTime, cast_datetime_as_uint);
cast_as_unsigned_integer!(Duration, cast_duration_as_uint);
cast_as_unsigned_integer!(Json, cast_json_as_uint);

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
    use crate::codec::convert::produce_dec_with_specified_tp;
    use crate::codec::data_type::{Bytes, Decimal, Int, Real, ScalarValue};
    use crate::codec::error::*;
    use crate::codec::mysql::charset::*;
    use crate::codec::mysql::decimal::{max_decimal, max_or_min_dec};
    use crate::codec::mysql::{Duration, Json, RoundMode, Time};
    use crate::codec::Error;
    use crate::expr::Flag;
    use crate::expr::{EvalConfig, EvalContext};
    use crate::rpn_expr::impl_cast::*;
    use crate::rpn_expr::{RpnExpression, RpnFnCallExtra};
    use nom::AsBytes;
    use std::collections::BTreeMap;
    use std::fmt::{Debug, Display, Formatter};
    use std::sync::Arc;
    use std::{f32, f64, i64, u64};
    use tidb_query_datatype::{Collation, FieldTypeAccessor, FieldTypeTp, UNSPECIFIED_LENGTH};

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

    fn make_ctx(
        overflow_as_warning: bool,
        truncate_as_warning: bool,
        should_clip_to_zero: bool,
    ) -> EvalContext {
        let mut flag: Flag = Flag::default();
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

    fn make_ctx_4(
        overflow_as_warning: bool,
        truncate_as_warning: bool,
        flags: Vec<Flag>,
    ) -> EvalContext {
        let mut flag: Flag = Flag::default();
        if overflow_as_warning {
            flag |= Flag::OVERFLOW_AS_WARNING;
        }
        if truncate_as_warning {
            flag |= Flag::TRUNCATE_AS_WARNING;
        }
        for i in flags {
            flag |= i;
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

    fn make_ret_field_type_3(
        flen: isize,
        charset: &str,
        tp: FieldTypeTp,
        collation: Collation,
    ) -> FieldType {
        let mut ft = FieldType::default();
        let fta = ft.as_mut_accessor();
        fta.set_flen(flen);
        fta.set_tp(tp);
        fta.set_collation(collation);
        ft.set_charset(String::from(charset));
        ft
    }

    fn make_ret_field_type_4(flen: isize, decimal: isize, unsigned: bool) -> FieldType {
        let mut ft = FieldType::default();
        let fta = ft.as_mut_accessor();
        fta.set_flen(flen);
        fta.set_decimal(decimal);
        if unsigned {
            fta.set_flag(FieldTypeFlag::UNSIGNED);
        }
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

    fn make_log<P: Display, R: Debug>(input: &P, expect: &R, result: &Result<Option<R>>) -> String {
        format!(
            "input: {}, expect: {:?}, output: {:?}",
            input, expect, result
        )
    }

    fn check_warning(ctx: &EvalContext, err_code: Option<i32>, log: &str) {
        if let Some(x) = err_code {
            // if ctx.warnings.warning_cnt != 1 {
            //     println!(
            //         "ctx.warnings.cnt!=1, it is: {}, log: {}, warnings: {:?}",
            //         ctx.warnings.warning_cnt, log, ctx.warnings.warnings
            //     );
            // }
            // if ctx.warnings.warning_cnt == 0 {
            //     return;
            // }
            // if ctx.warnings.warnings[0].get_code() != x {
            //     println!(
            //         "warn[0].code()!=expect, it is: {}, expect: {}, log: {}, warnings: {:?}",
            //         ctx.warnings.warnings[0].get_code(),
            //         x,
            //         log,
            //         ctx.warnings.warnings
            //     );
            // }
            assert_eq!(
                ctx.warnings.warning_cnt, 1,
                "log: {}, warnings: {:?}",
                log, ctx.warnings.warnings
            );
            assert_eq!(
                ctx.warnings.warnings[0].get_code(),
                x,
                "log: {}, warnings: {:?}",
                log,
                ctx.warnings.warnings
            );
        } else {
            // if ctx.warnings.warning_cnt != 0 {
            //     println!(
            //         "expect warn_cnt==0, it is: {}, log: {}, warnings: {:?}",
            //         ctx.warnings.warning_cnt, log, ctx.warnings.warnings
            //     );
            // }
            assert_eq!(
                ctx.warnings.warning_cnt, 0,
                "log: {}, warnings: {:?}",
                log, ctx.warnings.warnings
            );
        }
    }

    fn check_result<R: Debug + PartialEq>(expect: Option<&R>, res: &Result<Option<R>>, log: &str) {
        assert!(res.is_ok(), "{}", log);
        let res = res.as_ref().unwrap();
        if res.is_none() {
            // if expect.is_some() {
            //     println!("expect should be none, but it is some, log: {}", log);
            // }
            assert!(expect.is_none(), "{}", log);
        } else {
            let res = res.as_ref().unwrap();
            // if expect.unwrap() != res {
            //     println!(
            //         "expect.unwrap()==res: {}, log: {}",
            //         expect.unwrap() == res,
            //         log
            //     );
            // }
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
        FnCast: Fn(&mut EvalContext, &RpnFnCallExtra, &Option<T>) -> Result<Option<Decimal>>,
        FnToStr: Fn(&T) -> String,
    {
        #[derive(Clone, Copy, Debug)]
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
            // why there is origin_flen, origin_decimal here?
            // to make let the programmer clearly know what the flen and decimal of the decimal is.

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
            // TODO, add test case for Decimal::round failure
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
                            base_res.clone()
                        }
                    }
                    Sign::Negative => {
                        if base_res.is_negative() {
                            base_res.clone()
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
                        // TODO, if add test case for Decimal::round failure,
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
                    ResType::Same => base_res.clone(),
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
                    let rft = make_ret_field_type_4(res_flen, res_decimal, is_unsigned);
                    let ia = make_implicit_args(in_union);
                    let extra = make_extra(&rft, &ia);

                    let mut ctx =
                        make_ctx_4(overflow_as_warning, truncate_as_warning, vec![in_dml_flag]);
                    let cast_func_res = cast_func(&mut ctx, &extra, &Some(input.clone()));

                    let mut ctx =
                        make_ctx_4(overflow_as_warning, truncate_as_warning, vec![in_dml_flag]);
                    let pd_res = produce_dec_with_specified_tp(&mut ctx, base_res.clone(), &rft);

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
                         expect: {}, expect_from_produce_dec_with_specified_tp: {:?}, result: {:?}",
                        func_name, input_as_debug_str_func(&input), base_res,
                        origin_flen, origin_decimal,
                        res_flen, res_decimal,
                        in_union, is_unsigned, in_dml, in_dml_flag,
                        cond, sign, res_type,
                        overflow_as_warning, truncate_as_warning, warning_err_code,
                        expect.to_string(), pd_res_log, cast_func_res_log
                    );

                    if pd_res.is_err() {
                        println!("{}", log);
                    }
                    if &expect != pd_res.as_ref().unwrap() {
                        println!(
                            "expect: {:?}, pd_res: {:?}",
                            expect,
                            pd_res.as_ref().unwrap()
                        );
                    }
                    assert!(pd_res.is_ok(), "{}", log);
                    assert_eq!(
                        expect, pd_res.unwrap(),
                        "the expect decimal is not same as decimal constructed \
                         by base_dec with produce_dec_with_specified_tp, this is the bug of this test function, \
                         the log of this case is: {}",
                        log
                    );

                    check_result(Some(&expect), &cast_func_res, log.as_str());
                    check_warning(&ctx, warning_err_code, log.as_str());
                }
            }
        }
    }

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
            cast_unsigned_int_as_signed_or_unsigned_decimal,
            |x| x.to_string(),
            "cast_unsigned_int_as_signed_or_unsigned_decimal",
        );
    }

    #[test]
    fn test_signed_int_as_unsigned_decimal() {
        test_none_with_ctx_and_extra(cast_signed_int_as_unsigned_decimal);

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
            // FIXME, fix these case(negative to unsigned decimal, without in_union)
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
            cast_any_as_decimal::<Int>,
            |x| x.to_string(),
            "cast_signed_int_as_signed_decimal",
        );
    }

    #[test]
    fn test_real_as_decimal() {
        test_none_with_ctx_and_extra(cast_real_as_decimal);

        // TODO, add test case that make Decimal::from_f64 return err
        let cs = vec![
            /// (input, in_union, is_res_unsigned, base_result)
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
            |ctx, extra, val| {
                let val = val.map(|x| Real::new(x).unwrap());
                cast_real_as_decimal(ctx, extra, &val)
            },
            |x| x.to_string(),
            "cast_real_as_decimal",
        );
    }

    #[test]
    fn test_string_as_signed_decimal() {
        test_none_with_ctx_and_extra(cast_any_as_decimal::<Bytes>);

        // TODO, add test case that make Decimal::from_bytes return err.
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
        ];

        test_as_decimal_helper(
            cs,
            |ctx, extra, val| {
                let val = val.map(|x| x.as_bytes().to_vec());
                cast_any_as_decimal::<Bytes>(ctx, extra, &val)
            },
            |x| x.to_string(),
            "cast_string_as_signed_decimal",
        )
    }

    #[test]
    fn test_string_as_unsigned_decimal() {
        test_none_with_ctx_and_extra(cast_string_as_unsigned_decimal);

        // TODO, add test case that make Decimal::from_bytes return err.
        let cs = vec![("-10abc", Decimal::from(-10))];

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
        ];

        test_as_decimal_helper(
            cs,
            |ctx, extra, val| {
                let val = val.map(|x| x.as_bytes().to_vec());
                cast_string_as_unsigned_decimal(ctx, extra, &val)
            },
            |x| x.to_string(),
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
            cast_decimal_as_signed_decimal,
            |x| x.to_string(),
            "cast_decimal_as_signed_decimal",
        );
    }

    #[test]
    fn test_decimal_as_unsigned_decimal() {
        test_none_with_ctx_and_extra(cast_decimal_as_unsigned_decimal);

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

        // TODO, add more test case
        let cs: Vec<(Time, bool, bool, Decimal)> = vec![
            // (cast_func_input, in_union, is_res_unsigned, base_result)
            (
                Time::parse_utc_datetime("2000-01-01T12:13:14", 0).unwrap(),
                false,
                false,
                Decimal::from_bytes(b"20000101121314").unwrap().unwrap(),
            ),
            (
                Time::parse_utc_datetime("2000-01-01T12:13:14.6666", 0).unwrap(),
                false,
                false,
                Decimal::from_bytes(b"20000101121315").unwrap().unwrap(),
            ),
        ];
        test_as_decimal_helper(
            cs,
            cast_any_as_decimal::<Time>,
            |x| x.to_string(),
            "cast_time_as_decimal",
        )
    }

    #[test]
    fn test_duration_as_decimal() {
        test_none_with_ctx_and_extra(cast_any_as_decimal::<Duration>);

        // TODO, add more test case
        let cs: Vec<(Duration, bool, bool, Decimal)> = vec![
            // (input, in_union, is_res_unsigned, base_result)
            (
                Duration::parse(b"17:51:04.78", 2).unwrap(),
                false,
                false,
                Decimal::from_f64(175104.78).unwrap(),
            ),
            (
                Duration::parse(b"-17:51:04.78", 2).unwrap(),
                false,
                false,
                Decimal::from_f64(-175104.78).unwrap(),
            ),
            (
                Duration::parse(b"17:51:04.78", 0).unwrap(),
                false,
                false,
                Decimal::from(175105),
            ),
            (
                Duration::parse(b"-17:51:04.78", 0).unwrap(),
                false,
                false,
                Decimal::from(-175105),
            ),
        ];
        test_as_decimal_helper(
            cs,
            cast_any_as_decimal::<Duration>,
            |x| x.to_string(),
            "cast_duration_as_int",
        )
    }

    #[test]
    fn test_json_as_decimal() {
        test_none_with_ctx_and_extra(cast_any_as_decimal::<Json>);

        // TODO, add test case that make Decimal::from_str failed
        let cs: Vec<(Json, bool, bool, Decimal)> = vec![
            // (cast_func_input, in_union, is_res_unsigned, base_result)
            (
                Json::Object(BTreeMap::default()),
                false,
                false,
                Decimal::zero(),
            ),
            (Json::Array(vec![]), false, false, Decimal::zero()),
            (
                Json::I64(10),
                false,
                false,
                Decimal::from_f64(10f64 as f64).unwrap(),
            ),
            (
                Json::I64(i64::MAX),
                false,
                false,
                Decimal::from_f64(i64::MAX as f64).unwrap(),
            ),
            (
                Json::I64(i64::MIN),
                false,
                false,
                Decimal::from_f64(i64::MIN as f64).unwrap(),
            ),
            (Json::U64(0), false, false, Decimal::zero()),
            (
                Json::U64(i64::MAX as u64),
                false,
                false,
                Decimal::from_f64(i64::MAX as f64).unwrap(),
            ),
            (
                Json::U64(u64::MAX),
                false,
                false,
                Decimal::from_f64(u64::MAX as f64).unwrap(),
            ),
            (
                Json::Double(i64::MAX as f64),
                false,
                false,
                Decimal::from_f64(i64::MAX as f64).unwrap(),
            ),
            (
                Json::Double(i64::MIN as f64),
                false,
                false,
                Decimal::from_f64(i64::MIN as f64).unwrap(),
            ),
            (
                Json::Double(u64::MAX as f64),
                false,
                false,
                Decimal::from_f64(u64::MAX as f64).unwrap(),
            ),
            (
                Json::String("10.0".to_string()),
                false,
                false,
                Decimal::from_bytes(b"10.0").unwrap().unwrap(),
            ),
            (
                Json::String("-10.0".to_string()),
                false,
                false,
                Decimal::from_bytes(b"-10.0").unwrap().unwrap(),
            ),
            (
                Json::String("9999999999999999999".to_string()),
                false,
                false,
                Decimal::from_bytes(b"9999999999999999999")
                    .unwrap()
                    .unwrap(),
            ),
            (
                Json::String("-9999999999999999999".to_string()),
                false,
                false,
                Decimal::from_bytes(b"-9999999999999999999")
                    .unwrap()
                    .unwrap(),
            ),
            (
                Json::Boolean(true),
                false,
                false,
                Decimal::from_f64(1f64).unwrap(),
            ),
            (Json::Boolean(false), false, false, Decimal::zero()),
            (Json::None, false, false, Decimal::zero()),
        ];

        test_as_decimal_helper(
            cs,
            cast_any_as_decimal::<Json>,
            |x| x.to_string(),
            "cast_json_as_decimal",
        );
    }
}
