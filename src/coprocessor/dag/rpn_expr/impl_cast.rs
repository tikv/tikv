// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryFrom;

use cop_codegen::RpnFunction;
use cop_datatype::{EvalType, FieldTypeAccessor, FieldTypeFlag};
use tipb::expression::FieldType;

use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::dag::rpn_expr::function::RpnFunction;
use crate::coprocessor::dag::rpn_expr::types::RpnFnCallPayload;
use crate::coprocessor::Result;

/// Gets the cast function between specified data types.
///
/// TODO: This function supports some internal casts performed by TiKV. However it would be better
/// to be done in TiDB.
pub fn get_cast_fn(
    from_field_type: &FieldType,
    to_field_type: &FieldType,
) -> Result<Box<dyn RpnFunction>> {
    let from = box_try!(EvalType::try_from(from_field_type.tp()));
    let to = box_try!(EvalType::try_from(to_field_type.tp()));
    Ok(match (from, to) {
        (EvalType::Int, EvalType::Decimal) => {
            if !from_field_type
                .as_accessor()
                .flag()
                .contains(FieldTypeFlag::UNSIGNED)
                && !to_field_type
                    .as_accessor()
                    .flag()
                    .contains(FieldTypeFlag::UNSIGNED)
            {
                Box::new(RpnFnCastIntAsDecimal)
            } else {
                Box::new(RpnFnCastUintAsDecimal)
            }
        }
        (EvalType::Bytes, EvalType::Real) => Box::new(RpnFnCastStringAsReal),
        (EvalType::DateTime, EvalType::Real) => Box::new(RpnFnCastTimeAsReal),
        (EvalType::Duration, EvalType::Real) => Box::new(RpnFnCastDurationAsReal),
        (EvalType::Json, EvalType::Real) => Box::new(RpnFnCastJsonAsReal),
        _ => return Err(box_err!("Unsupported cast from {} to {}", from, to)),
    })
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

/// The unsigned int implementation for push down signature `CastIntAsDecimal`.
#[derive(Debug, Clone, Copy, RpnFunction)]
#[rpn_function(args = 1)]
pub struct RpnFnCastUintAsDecimal;

impl RpnFnCastUintAsDecimal {
    #[inline]
    fn call(
        ctx: &mut EvalContext,
        payload: RpnFnCallPayload<'_>,
        val: &Option<i64>,
    ) -> Result<Option<Decimal>> {
        match val {
            None => Ok(None),
            Some(val) => {
                let dec = Decimal::from(*val as u64);
                Ok(Some(produce_dec_with_specified_tp(
                    ctx,
                    dec,
                    payload.return_field_type(),
                )?))
            }
        }
    }
}

/// The signed int implementation for push down signature `CastIntAsDecimal`.
#[derive(Debug, Clone, Copy, RpnFunction)]
#[rpn_function(args = 1)]
pub struct RpnFnCastIntAsDecimal;

impl RpnFnCastIntAsDecimal {
    #[inline]
    fn call(
        ctx: &mut EvalContext,
        payload: RpnFnCallPayload<'_>,
        val: &Option<i64>,
    ) -> Result<Option<Decimal>> {
        match val {
            None => Ok(None),
            Some(val) => {
                let dec = Decimal::from(*val);
                Ok(Some(produce_dec_with_specified_tp(
                    ctx,
                    dec,
                    payload.return_field_type(),
                )?))
            }
        }
    }
}

/// The implementation for push down signature `CastStringAsReal`.
#[derive(Debug, Clone, Copy, RpnFunction)]
#[rpn_function(args = 1)]
pub struct RpnFnCastStringAsReal;

impl RpnFnCastStringAsReal {
    #[inline]
    fn call(
        ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        val: &Option<Bytes>,
    ) -> Result<Option<Real>> {
        use crate::coprocessor::codec::convert::bytes_to_f64;

        match val {
            None => Ok(None),
            Some(val) => {
                let val = bytes_to_f64(ctx, val.as_slice())?;
                // FIXME: There is an additional step `ProduceFloatWithSpecifiedTp` in TiDB.
                Ok(Some(val))
            }
        }
    }
}

/// The implementation for push down signature `CastTimeAsReal`.
#[derive(Debug, Clone, Copy, RpnFunction)]
#[rpn_function(args = 1)]
pub struct RpnFnCastTimeAsReal;

impl RpnFnCastTimeAsReal {
    #[inline]
    fn call(
        _ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        val: &Option<DateTime>,
    ) -> Result<Option<Real>> {
        match val {
            None => Ok(None),
            Some(val) => {
                let val = val.to_decimal()?.as_f64()?;
                Ok(Some(val))
            }
        }
    }
}

/// The implementation for push down signature `CastDurationAsReal`.
#[derive(Debug, Clone, Copy, RpnFunction)]
#[rpn_function(args = 1)]
pub struct RpnFnCastDurationAsReal;

impl RpnFnCastDurationAsReal {
    #[inline]
    fn call(
        _ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        val: &Option<Duration>,
    ) -> Result<Option<Real>> {
        match val {
            None => Ok(None),
            Some(val) => {
                let val = val.to_decimal()?.as_f64()?;
                Ok(Some(val))
            }
        }
    }
}

/// The implementation for push down signature `CastJsonAsReal`.
#[derive(Debug, Clone, Copy, RpnFunction)]
#[rpn_function(args = 1)]
pub struct RpnFnCastJsonAsReal;

impl RpnFnCastJsonAsReal {
    #[inline]
    fn call(
        ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        val: &Option<Json>,
    ) -> Result<Option<Real>> {
        match val {
            None => Ok(None),
            Some(val) => {
                let val = val.cast_to_real(ctx)?;
                Ok(Some(val))
            }
        }
    }
}
