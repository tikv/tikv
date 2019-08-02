// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryFrom;
use std::borrow::Cow;

use tidb_query_codegen::rpn_fn;
use tidb_query_datatype::{Collation, EvalType, FieldTypeAccessor, FieldTypeTp};
use tidb_query_datatype::UNSPECIFIED_LENGTH;
use tipb::expression::FieldType;

use crate::codec::convert::*;
use crate::codec::data_type::*;
use crate::codec::mysql::charset;
use crate::rpn_expr::{RpnExpressionNode, RpnFnCallExtra};
use crate::Result;
use crate::expr::{Error, EvalContext};

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
        (EvalType::Int, EvalType::Real) => {
            if !from_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Int, Real>()
            } else {
                cast_uint_as_real_fn_meta()
            }
        }
        (EvalType::Bytes, EvalType::Real) => cast_any_as_any_fn_meta::<Bytes, Real>(),
        (EvalType::Decimal, EvalType::Real) => cast_any_as_any_fn_meta::<Decimal, Real>(),
        (EvalType::DateTime, EvalType::Real) => cast_any_as_any_fn_meta::<DateTime, Real>(),
        (EvalType::Duration, EvalType::Real) => cast_any_as_any_fn_meta::<Duration, Real>(),
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

fn produce_dec_with_specified_tp(
    ctx: &mut EvalContext,
    mut dec: Decimal,
    ft: &FieldType,
) -> Result<Decimal> {
    let (flen, decimal) = (ft.flen(), ft.decimal());
    if flen != UNSPECIFIED_LENGTH && decimal != UNSPECIFIED_LENGTH {
        dec = dec.convert_to(ctx, flen as u8, decimal as u8)?;
    }
    if ft.is_unsigned() && dec.is_negative() {
        Ok(Decimal::zero())
    } else {
        Ok(dec)
    }
}

/// `produce_str_with_specified_tp`(`ProduceStrWithSpecifiedTp` in tidb) produces
/// a new string according to `flen` and `chs`.
/// # panic
/// The s must represent a valid str, otherwise, panic!
fn produce_str_with_specified_tp<'a>(
    ctx: &mut EvalContext,
    s: Cow<'a, [u8]>,
    ft: &FieldType,
    pad_zero: bool,
) -> Result<Cow<'a, [u8]>> {
    let (flen, chs) = (ft.flen(), ft.get_charset());
    if flen < 0 {
        return Ok(s);
    }
    let flen = flen as usize;
    // flen is the char length, not byte length, for UTF8 charset, we need to calculate the
    // char count and truncate to flen chars if it is too long.
    if chs == charset::CHARSET_UTF8 || chs == charset::CHARSET_UTF8MB4 {
        let truncate_info = {
            let s: &str = std::str::from_utf8(s.as_ref()).unwrap();
            let mut indices = s.char_indices().skip(flen);
            if let Some((truncate_pos, _)) = indices.next() {
                let char_count = flen + 1 + indices.count();
                Some((char_count, truncate_pos))
            } else {
                None
            }
        };
        if truncate_info.is_none() {
            return Ok(s);
        }
        let (char_count, truncate_pos) = truncate_info.unwrap();
        ctx.handle_truncate_err(Error::data_too_long(format!(
            "Data Too Long, field len {}, data len {}",
            flen, char_count
        )))?;

        let mut res = s.into_owned();
        truncate_binary(&mut res, truncate_pos as isize);
        Ok(Cow::Owned(res))
    } else if s.len() > flen {
        ctx.handle_truncate_err(Error::data_too_long(format!(
            "Data Too Long, field len {}, data len {}",
            flen,
            s.len()
        )))?;
        let mut res = s.into_owned();
        truncate_binary(&mut res, flen as isize);
        Ok(Cow::Owned(res))
    } else if ft.tp() == FieldTypeTp::String && s.len() < flen && is_binary_str(ft) && pad_zero {
        let mut s = s.into_owned();
        s.resize(flen, 0);
        Ok(Cow::Owned(s))
    } else {
        Ok(s)
    }
}

#[inline]
fn is_binary_str(ft: &FieldType) -> bool {
    ft.collation() == Collation::Binary && ft.is_string_like()
}

fn pad_zero_for_binary_type(s: &mut Vec<u8>, ft: &FieldType) {
    let flen = ft.flen();
    if flen < 0 {
        return;
    }
    let flen = flen as usize;
    if ft.tp() == FieldTypeTp::String && is_binary_str(ft) && s.len() < flen {
        // it seems MaxAllowedPacket has not push down to tikv, so we needn't to handle it
        s.resize(flen, 0);
    }
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
            if in_union(extra.implicit_args) && *val < 0 {
                Ok(Some(Decimal::zero()))
            } else {
                // TODO, TiDB use ConvertIntToUint, but I think it is a bug
                let dec = Decimal::from(*val as u64);
                Ok(Some(produce_dec_with_specified_tp(
                    ctx,
                    dec,
                    extra.ret_field_type,
                )?))
            }
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

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_uint_as_int(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Int>,
) -> Result<Option<i64>> {
    match val {
        None => Ok(None),
        Some(val) => {
            if in_union(extra.implicit_args) && *val < 0 {
                Ok(Some(0))
            } else {
                let val = *val as u64;
                Ok(Some(<u64 as ConvertTo<i64>>::convert(&val, ctx)?))
            }
        }
    }
}

/// The implementation for push down signature `CastIntAsReal` from unsigned integer.
#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_uint_as_real(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Int>,
) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            if in_union(extra.implicit_args) && *val < 0 {
                return Ok(Some(Real::new(0f64).unwrap()));
            }
            // TODO, TiDB use ConvertIntToUint here, but I think it is a bug
            let val = *val as u64;
            let val: u64 = <u64 as ConvertTo<u64>>::convert(&val, ctx)?;
            Ok(Real::new(val as f64).ok())
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
            // TODO, tidb use ConvertIntToUint here, I think it is a bug.
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
