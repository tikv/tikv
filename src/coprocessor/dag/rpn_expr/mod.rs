// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
pub mod function;
pub mod types;

pub mod impl_arithmetic;
pub mod impl_cast;
pub mod impl_compare;
pub mod impl_op;

pub use self::function::RpnFunction;
pub use self::types::{RpnExpression, RpnExpressionBuilder};

use cop_datatype::{FieldTypeAccessor, FieldTypeFlag};
use tipb::expression::{Expr, ScalarFuncSig};

use self::impl_arithmetic::*;
use self::impl_compare::*;
use self::impl_op::*;
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::Result;

fn map_int_sig<F>(
    value: ScalarFuncSig,
    children: &[Expr],
    mapper: F,
) -> Result<Box<dyn RpnFunction>>
where
    F: Fn(bool, bool) -> Box<dyn RpnFunction>,
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

fn compare_mapper<F: CmpOp>(lhs_is_unsigned: bool, rhs_is_unsigned: bool) -> Box<dyn RpnFunction> {
    match (lhs_is_unsigned, rhs_is_unsigned) {
        (false, false) => Box::new(RpnFnCompare::<BasicComparer<Int, F>>::new()),
        (false, true) => Box::new(RpnFnCompare::<IntUintComparer<F>>::new()),
        (true, false) => Box::new(RpnFnCompare::<UintIntComparer<F>>::new()),
        (true, true) => Box::new(RpnFnCompare::<UintUintComparer<F>>::new()),
    }
}

fn plus_mapper(lhs_is_unsigned: bool, rhs_is_unsigned: bool) -> Box<dyn RpnFunction> {
    match (lhs_is_unsigned, rhs_is_unsigned) {
        (false, false) => Box::new(RpnFnArithmetic::<IntIntPlus>::new()),
        (false, true) => Box::new(RpnFnArithmetic::<IntUintPlus>::new()),
        (true, false) => Box::new(RpnFnArithmetic::<UintIntPlus>::new()),
        (true, true) => Box::new(RpnFnArithmetic::<UintUintPlus>::new()),
    }
}

fn minus_mapper(lhs_is_unsigned: bool, rhs_is_unsigned: bool) -> Box<dyn RpnFunction> {
    match (lhs_is_unsigned, rhs_is_unsigned) {
        (false, false) => Box::new(RpnFnArithmetic::<IntIntMinus>::new()),
        (false, true) => Box::new(RpnFnArithmetic::<IntUintMinus>::new()),
        (true, false) => Box::new(RpnFnArithmetic::<UintIntMinus>::new()),
        (true, true) => Box::new(RpnFnArithmetic::<UintUintMinus>::new()),
    }
}

fn mod_mapper(lhs_is_unsigned: bool, rhs_is_unsigned: bool) -> Box<dyn RpnFunction> {
    match (lhs_is_unsigned, rhs_is_unsigned) {
        (false, false) => Box::new(RpnFnArithmetic::<IntIntMod>::new()),
        (false, true) => Box::new(RpnFnArithmetic::<IntUintMod>::new()),
        (true, false) => Box::new(RpnFnArithmetic::<UintIntMod>::new()),
        (true, true) => Box::new(RpnFnArithmetic::<UintUintMod>::new()),
    }
}

#[rustfmt::skip]
fn map_pb_sig_to_rpn_func(value: ScalarFuncSig, children: &[Expr]) -> Result<Box<dyn RpnFunction>> {
    Ok(match value {
        ScalarFuncSig::LTInt => map_int_sig(value, children, compare_mapper::<CmpOpLT>)?,
        ScalarFuncSig::LTReal => Box::new(RpnFnCompare::<BasicComparer<Real, CmpOpLT>>::new()),
        ScalarFuncSig::LTDecimal => Box::new(RpnFnCompare::<BasicComparer<Decimal, CmpOpLT>>::new()),
        ScalarFuncSig::LTString => Box::new(RpnFnCompare::<BasicComparer<Bytes, CmpOpLT>>::new()),
        ScalarFuncSig::LTTime => Box::new(RpnFnCompare::<BasicComparer<DateTime, CmpOpLT>>::new()),
        ScalarFuncSig::LTDuration => Box::new(RpnFnCompare::<BasicComparer<Duration, CmpOpLT>>::new()),
        ScalarFuncSig::LTJson => Box::new(RpnFnCompare::<BasicComparer<Json, CmpOpLT>>::new()),
        ScalarFuncSig::LEInt => map_int_sig(value, children, compare_mapper::<CmpOpLE>)?,
        ScalarFuncSig::LEReal => Box::new(RpnFnCompare::<BasicComparer<Real, CmpOpLE>>::new()),
        ScalarFuncSig::LEDecimal => Box::new(RpnFnCompare::<BasicComparer<Decimal, CmpOpLE>>::new()),
        ScalarFuncSig::LEString => Box::new(RpnFnCompare::<BasicComparer<Bytes, CmpOpLE>>::new()),
        ScalarFuncSig::LETime => Box::new(RpnFnCompare::<BasicComparer<DateTime, CmpOpLE>>::new()),
        ScalarFuncSig::LEDuration => Box::new(RpnFnCompare::<BasicComparer<Duration, CmpOpLE>>::new()),
        ScalarFuncSig::LEJson => Box::new(RpnFnCompare::<BasicComparer<Json, CmpOpLE>>::new()),
        ScalarFuncSig::GTInt => map_int_sig(value, children, compare_mapper::<CmpOpGT>)?,
        ScalarFuncSig::GTReal => Box::new(RpnFnCompare::<BasicComparer<Real, CmpOpGT>>::new()),
        ScalarFuncSig::GTDecimal => Box::new(RpnFnCompare::<BasicComparer<Decimal, CmpOpGT>>::new()),
        ScalarFuncSig::GTString => Box::new(RpnFnCompare::<BasicComparer<Bytes, CmpOpGT>>::new()),
        ScalarFuncSig::GTTime => Box::new(RpnFnCompare::<BasicComparer<DateTime, CmpOpGT>>::new()),
        ScalarFuncSig::GTDuration => Box::new(RpnFnCompare::<BasicComparer<Duration, CmpOpGT>>::new()),
        ScalarFuncSig::GTJson => Box::new(RpnFnCompare::<BasicComparer<Json, CmpOpGT>>::new()),
        ScalarFuncSig::GEInt => map_int_sig(value, children, compare_mapper::<CmpOpGE>)?,
        ScalarFuncSig::GEReal => Box::new(RpnFnCompare::<BasicComparer<Real, CmpOpGE>>::new()),
        ScalarFuncSig::GEDecimal => Box::new(RpnFnCompare::<BasicComparer<Decimal, CmpOpGE>>::new()),
        ScalarFuncSig::GEString => Box::new(RpnFnCompare::<BasicComparer<Bytes, CmpOpGE>>::new()),
        ScalarFuncSig::GETime => Box::new(RpnFnCompare::<BasicComparer<DateTime, CmpOpGE>>::new()),
        ScalarFuncSig::GEDuration => Box::new(RpnFnCompare::<BasicComparer<Duration, CmpOpGE>>::new()),
        ScalarFuncSig::GEJson => Box::new(RpnFnCompare::<BasicComparer<Json, CmpOpGE>>::new()),
        ScalarFuncSig::NEInt => map_int_sig(value, children, compare_mapper::<CmpOpNE>)?,
        ScalarFuncSig::NEReal => Box::new(RpnFnCompare::<BasicComparer<Real, CmpOpNE>>::new()),
        ScalarFuncSig::NEDecimal => Box::new(RpnFnCompare::<BasicComparer<Decimal, CmpOpNE>>::new()),
        ScalarFuncSig::NEString => Box::new(RpnFnCompare::<BasicComparer<Bytes, CmpOpNE>>::new()),
        ScalarFuncSig::NETime => Box::new(RpnFnCompare::<BasicComparer<DateTime, CmpOpNE>>::new()),
        ScalarFuncSig::NEDuration => Box::new(RpnFnCompare::<BasicComparer<Duration, CmpOpNE>>::new()),
        ScalarFuncSig::NEJson => Box::new(RpnFnCompare::<BasicComparer<Json, CmpOpNE>>::new()),
        ScalarFuncSig::EQInt => map_int_sig(value, children, compare_mapper::<CmpOpEQ>)?,
        ScalarFuncSig::EQReal => Box::new(RpnFnCompare::<BasicComparer<Real, CmpOpEQ>>::new()),
        ScalarFuncSig::EQDecimal => Box::new(RpnFnCompare::<BasicComparer<Decimal, CmpOpEQ>>::new()),
        ScalarFuncSig::EQString => Box::new(RpnFnCompare::<BasicComparer<Bytes, CmpOpEQ>>::new()),
        ScalarFuncSig::EQTime => Box::new(RpnFnCompare::<BasicComparer<DateTime, CmpOpEQ>>::new()),
        ScalarFuncSig::EQDuration => Box::new(RpnFnCompare::<BasicComparer<Duration, CmpOpEQ>>::new()),
        ScalarFuncSig::EQJson => Box::new(RpnFnCompare::<BasicComparer<Json, CmpOpEQ>>::new()),
        ScalarFuncSig::NullEQInt => map_int_sig(value, children, compare_mapper::<CmpOpNullEQ>)?,
        ScalarFuncSig::NullEQReal => Box::new(RpnFnCompare::<BasicComparer<Real, CmpOpNullEQ>>::new()),
        ScalarFuncSig::NullEQDecimal => Box::new(RpnFnCompare::<BasicComparer<Decimal, CmpOpNullEQ>>::new()),
        ScalarFuncSig::NullEQString => Box::new(RpnFnCompare::<BasicComparer<Bytes, CmpOpNullEQ>>::new()),
        ScalarFuncSig::NullEQTime => Box::new(RpnFnCompare::<BasicComparer<DateTime, CmpOpNullEQ>>::new()),
        ScalarFuncSig::NullEQDuration => Box::new(RpnFnCompare::<BasicComparer<Duration, CmpOpNullEQ>>::new()),
        ScalarFuncSig::NullEQJson => Box::new(RpnFnCompare::<BasicComparer<Json, CmpOpNullEQ>>::new()),
        ScalarFuncSig::IntIsNull => Box::new(RpnFnIsNull::<Int>::new()),
        ScalarFuncSig::RealIsNull => Box::new(RpnFnIsNull::<Real>::new()),
        ScalarFuncSig::DecimalIsNull => Box::new(RpnFnIsNull::<Decimal>::new()),
        ScalarFuncSig::StringIsNull => Box::new(RpnFnIsNull::<Bytes>::new()),
        ScalarFuncSig::TimeIsNull => Box::new(RpnFnIsNull::<DateTime>::new()),
        ScalarFuncSig::DurationIsNull => Box::new(RpnFnIsNull::<Duration>::new()),
        ScalarFuncSig::JsonIsNull => Box::new(RpnFnIsNull::<Json>::new()),
        ScalarFuncSig::IntIsTrue => Box::new(RpnFnIntIsTrue),
        ScalarFuncSig::RealIsTrue => Box::new(RpnFnRealIsTrue),
        ScalarFuncSig::DecimalIsTrue => Box::new(RpnFnDecimalIsTrue),
        ScalarFuncSig::IntIsFalse => Box::new(RpnFnIntIsFalse),
        ScalarFuncSig::RealIsFalse => Box::new(RpnFnRealIsFalse),
        ScalarFuncSig::DecimalIsFalse => Box::new(RpnFnDecimalIsFalse),
        ScalarFuncSig::LogicalAnd => Box::new(RpnFnLogicalAnd),
        ScalarFuncSig::LogicalOr => Box::new(RpnFnLogicalOr),
        ScalarFuncSig::UnaryNot => Box::new(RpnFnUnaryNot),
        ScalarFuncSig::PlusInt => map_int_sig(value, children, plus_mapper)?,
        ScalarFuncSig::PlusReal => Box::new(RpnFnArithmetic::<RealPlus>::new()),
        ScalarFuncSig::PlusDecimal => Box::new(RpnFnArithmetic::<DecimalPlus>::new()),
        ScalarFuncSig::MinusInt => map_int_sig(value, children, minus_mapper)?,
        ScalarFuncSig::MinusReal => Box::new(RpnFnArithmetic::<RealMinus>::new()),
        ScalarFuncSig::MinusDecimal => Box::new(RpnFnArithmetic::<DecimalMinus>::new()),
        ScalarFuncSig::MultiplyDecimal => Box::new(RpnFnArithmetic::<DecimalMultiply>::new()),
        ScalarFuncSig::ModReal => Box::new(RpnFnArithmetic::<RealMod>::new()),
        ScalarFuncSig::ModDecimal => Box::new(RpnFnArithmetic::<DecimalMod>::new()),
        ScalarFuncSig::ModInt => map_int_sig(value, children, mod_mapper)?,
        _ => return Err(box_err!(
            "ScalarFunction {:?} is not supported in batch mode",
            value
        )),
    })
}
