// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
pub mod function;
pub mod types;

pub mod impl_arithmetic;
pub mod impl_cast;
pub mod impl_compare;
pub mod impl_like;
pub mod impl_op;

pub use self::function::RpnFn;
pub use self::types::{RpnExpression, RpnExpressionBuilder};

use cop_datatype::{FieldTypeAccessor, FieldTypeFlag};
use tipb::expression::{Expr, ScalarFuncSig};

use self::impl_arithmetic::*;
use self::impl_compare::*;
use self::impl_like::*;
use self::impl_op::*;
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::Result;

fn map_int_sig<F>(value: ScalarFuncSig, children: &[Expr], mapper: F) -> Result<RpnFn>
where
    F: Fn(bool, bool) -> RpnFn,
{
    // FIXME: The signature for different signed / unsigned int should be inferred at TiDB side.
    if children.len() != 2 {
        return Err(box_err!(
            "ScalarFunction {:?} (params = {}) is not supported in batch mode",
            value,
            children.len()
        ));
    }
    let lhs_is_unsigned = children[0]
        .get_field_type()
        .flag()
        .contains(FieldTypeFlag::UNSIGNED);
    let rhs_is_unsigned = children[1]
        .get_field_type()
        .flag()
        .contains(FieldTypeFlag::UNSIGNED);
    Ok(mapper(lhs_is_unsigned, rhs_is_unsigned))
}

fn compare_mapper<F: CmpOp>(lhs_is_unsigned: bool, rhs_is_unsigned: bool) -> RpnFn {
    match (lhs_is_unsigned, rhs_is_unsigned) {
        (false, false) => compare_fn::<BasicComparer<Int, F>>(),
        (false, true) => compare_fn::<IntUintComparer<F>>(),
        (true, false) => compare_fn::<UintIntComparer<F>>(),
        (true, true) => compare_fn::<UintUintComparer<F>>(),
    }
}

fn plus_mapper(lhs_is_unsigned: bool, rhs_is_unsigned: bool) -> RpnFn {
    match (lhs_is_unsigned, rhs_is_unsigned) {
        (false, false) => arithmetic_fn::<IntIntPlus>(),
        (false, true) => arithmetic_fn::<IntUintPlus>(),
        (true, false) => arithmetic_fn::<UintIntPlus>(),
        (true, true) => arithmetic_fn::<UintUintPlus>(),
    }
}

fn minus_mapper(lhs_is_unsigned: bool, rhs_is_unsigned: bool) -> RpnFn {
    match (lhs_is_unsigned, rhs_is_unsigned) {
        (false, false) => arithmetic_fn::<IntIntMinus>(),
        (false, true) => arithmetic_fn::<IntUintMinus>(),
        (true, false) => arithmetic_fn::<UintIntMinus>(),
        (true, true) => arithmetic_fn::<UintUintMinus>(),
    }
}

fn mod_mapper(lhs_is_unsigned: bool, rhs_is_unsigned: bool) -> RpnFn {
    match (lhs_is_unsigned, rhs_is_unsigned) {
        (false, false) => arithmetic_fn::<IntIntMod>(),
        (false, true) => arithmetic_fn::<IntUintMod>(),
        (true, false) => arithmetic_fn::<UintIntMod>(),
        (true, true) => arithmetic_fn::<UintUintMod>(),
    }
}

#[rustfmt::skip]
fn map_pb_sig_to_rpn_func(value: ScalarFuncSig, children: &[Expr]) -> Result<RpnFn> {
    Ok(match value {
        ScalarFuncSig::LTInt => map_int_sig(value, children, compare_mapper::<CmpOpLT>)?,
        ScalarFuncSig::LTReal => compare_fn::<BasicComparer<Real, CmpOpLT>>(),
        ScalarFuncSig::LTDecimal => compare_fn::<BasicComparer<Decimal, CmpOpLT>>(),
        ScalarFuncSig::LTString => compare_fn::<BasicComparer<Bytes, CmpOpLT>>(),
        ScalarFuncSig::LTTime => compare_fn::<BasicComparer<DateTime, CmpOpLT>>(),
        ScalarFuncSig::LTDuration => compare_fn::<BasicComparer<Duration, CmpOpLT>>(),
        ScalarFuncSig::LTJson => compare_fn::<BasicComparer<Json, CmpOpLT>>(),
        ScalarFuncSig::LEInt => map_int_sig(value, children, compare_mapper::<CmpOpLE>)?,
        ScalarFuncSig::LEReal => compare_fn::<BasicComparer<Real, CmpOpLE>>(),
        ScalarFuncSig::LEDecimal => compare_fn::<BasicComparer<Decimal, CmpOpLE>>(),
        ScalarFuncSig::LEString => compare_fn::<BasicComparer<Bytes, CmpOpLE>>(),
        ScalarFuncSig::LETime => compare_fn::<BasicComparer<DateTime, CmpOpLE>>(),
        ScalarFuncSig::LEDuration => compare_fn::<BasicComparer<Duration, CmpOpLE>>(),
        ScalarFuncSig::LEJson => compare_fn::<BasicComparer<Json, CmpOpLE>>(),
        ScalarFuncSig::GTInt => map_int_sig(value, children, compare_mapper::<CmpOpGT>)?,
        ScalarFuncSig::GTReal => compare_fn::<BasicComparer<Real, CmpOpGT>>(),
        ScalarFuncSig::GTDecimal => compare_fn::<BasicComparer<Decimal, CmpOpGT>>(),
        ScalarFuncSig::GTString => compare_fn::<BasicComparer<Bytes, CmpOpGT>>(),
        ScalarFuncSig::GTTime => compare_fn::<BasicComparer<DateTime, CmpOpGT>>(),
        ScalarFuncSig::GTDuration => compare_fn::<BasicComparer<Duration, CmpOpGT>>(),
        ScalarFuncSig::GTJson => compare_fn::<BasicComparer<Json, CmpOpGT>>(),
        ScalarFuncSig::GEInt => map_int_sig(value, children, compare_mapper::<CmpOpGE>)?,
        ScalarFuncSig::GEReal => compare_fn::<BasicComparer<Real, CmpOpGE>>(),
        ScalarFuncSig::GEDecimal => compare_fn::<BasicComparer<Decimal, CmpOpGE>>(),
        ScalarFuncSig::GEString => compare_fn::<BasicComparer<Bytes, CmpOpGE>>(),
        ScalarFuncSig::GETime => compare_fn::<BasicComparer<DateTime, CmpOpGE>>(),
        ScalarFuncSig::GEDuration => compare_fn::<BasicComparer<Duration, CmpOpGE>>(),
        ScalarFuncSig::GEJson => compare_fn::<BasicComparer<Json, CmpOpGE>>(),
        ScalarFuncSig::NEInt => map_int_sig(value, children, compare_mapper::<CmpOpNE>)?,
        ScalarFuncSig::NEReal => compare_fn::<BasicComparer<Real, CmpOpNE>>(),
        ScalarFuncSig::NEDecimal => compare_fn::<BasicComparer<Decimal, CmpOpNE>>(),
        ScalarFuncSig::NEString => compare_fn::<BasicComparer<Bytes, CmpOpNE>>(),
        ScalarFuncSig::NETime => compare_fn::<BasicComparer<DateTime, CmpOpNE>>(),
        ScalarFuncSig::NEDuration => compare_fn::<BasicComparer<Duration, CmpOpNE>>(),
        ScalarFuncSig::NEJson => compare_fn::<BasicComparer<Json, CmpOpNE>>(),
        ScalarFuncSig::EQInt => map_int_sig(value, children, compare_mapper::<CmpOpEQ>)?,
        ScalarFuncSig::EQReal => compare_fn::<BasicComparer<Real, CmpOpEQ>>(),
        ScalarFuncSig::EQDecimal => compare_fn::<BasicComparer<Decimal, CmpOpEQ>>(),
        ScalarFuncSig::EQString => compare_fn::<BasicComparer<Bytes, CmpOpEQ>>(),
        ScalarFuncSig::EQTime => compare_fn::<BasicComparer<DateTime, CmpOpEQ>>(),
        ScalarFuncSig::EQDuration => compare_fn::<BasicComparer<Duration, CmpOpEQ>>(),
        ScalarFuncSig::EQJson => compare_fn::<BasicComparer<Json, CmpOpEQ>>(),
        ScalarFuncSig::NullEQInt => map_int_sig(value, children, compare_mapper::<CmpOpNullEQ>)?,
        ScalarFuncSig::NullEQReal => compare_fn::<BasicComparer<Real, CmpOpNullEQ>>(),
        ScalarFuncSig::NullEQDecimal => compare_fn::<BasicComparer<Decimal, CmpOpNullEQ>>(),
        ScalarFuncSig::NullEQString => compare_fn::<BasicComparer<Bytes, CmpOpNullEQ>>(),
        ScalarFuncSig::NullEQTime => compare_fn::<BasicComparer<DateTime, CmpOpNullEQ>>(),
        ScalarFuncSig::NullEQDuration => compare_fn::<BasicComparer<Duration, CmpOpNullEQ>>(),
        ScalarFuncSig::NullEQJson => compare_fn::<BasicComparer<Json, CmpOpNullEQ>>(),
        ScalarFuncSig::IntIsNull => is_null_fn::<Int>(),
        ScalarFuncSig::RealIsNull => is_null_fn::<Real>(),
        ScalarFuncSig::DecimalIsNull => is_null_fn::<Decimal>(),
        ScalarFuncSig::StringIsNull => is_null_fn::<Bytes>(),
        ScalarFuncSig::TimeIsNull => is_null_fn::<DateTime>(),
        ScalarFuncSig::DurationIsNull => is_null_fn::<Duration>(),
        ScalarFuncSig::JsonIsNull => is_null_fn::<Json>(),
        ScalarFuncSig::IntIsTrue => int_is_true_fn(),
        ScalarFuncSig::RealIsTrue => real_is_true_fn(),
        ScalarFuncSig::DecimalIsTrue => decimal_is_true_fn(),
        ScalarFuncSig::IntIsFalse => int_is_false_fn(),
        ScalarFuncSig::RealIsFalse => real_is_false_fn(),
        ScalarFuncSig::DecimalIsFalse => decimal_is_false_fn(),
        ScalarFuncSig::LogicalAnd => logical_and_fn(),
        ScalarFuncSig::LogicalOr => logical_or_fn(),
        ScalarFuncSig::UnaryNot => unary_not_fn(),
        ScalarFuncSig::PlusInt => map_int_sig(value, children, plus_mapper)?,
        ScalarFuncSig::PlusReal => arithmetic_fn::<RealPlus>(),
        ScalarFuncSig::PlusDecimal => arithmetic_fn::<DecimalPlus>(),
        ScalarFuncSig::MinusInt => map_int_sig(value, children, minus_mapper)?,
        ScalarFuncSig::MinusReal => arithmetic_fn::<RealMinus>(),
        ScalarFuncSig::MinusDecimal => arithmetic_fn::<DecimalMinus>(),
        ScalarFuncSig::MultiplyDecimal => arithmetic_fn::<DecimalMultiply>(),
        ScalarFuncSig::ModReal => arithmetic_fn::<RealMod>(),
        ScalarFuncSig::ModDecimal => arithmetic_fn::<DecimalMod>(),
        ScalarFuncSig::ModInt => map_int_sig(value, children, mod_mapper)?,
        ScalarFuncSig::LikeSig => like_fn(),
        _ => return Err(box_err!(
            "ScalarFunction {:?} is not supported in batch mode",
            value
        )),
    })
}
