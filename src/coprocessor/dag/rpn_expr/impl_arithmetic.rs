// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use cop_codegen::RpnFunction;

use super::types::RpnFnCallPayload;
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::codec::{self, Error};
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::Result;
use std::fmt::Debug;

#[derive(Debug, RpnFunction)]
#[rpn_function(args = 2)]
pub struct RpnFnArithmetic<A: ArithmeticOp> {
    _phantom: std::marker::PhantomData<A>,
}

impl<A: ArithmeticOp> RpnFnArithmetic<A> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    fn call(
        _ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        arg0: &Option<A::T>,
        arg1: &Option<A::T>,
    ) -> Result<Option<A::T>> {
        if let (Some(lhs), Some(rhs)) = (arg0, arg1) {
            A::calc(lhs, rhs)
        } else {
            // All arithmetical functions with a NULL argument return NULL
            Ok(None)
        }
    }
}

impl<A: ArithmeticOp> Clone for RpnFnArithmetic<A> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<A: ArithmeticOp> Copy for RpnFnArithmetic<A> {}

pub trait ArithmeticOp: Send + Sync + Debug + 'static {
    type T: Evaluable;

    fn calc(lhs: &Self::T, rhs: &Self::T) -> Result<Option<Self::T>>;
}

#[derive(Debug)]
pub struct IntIntPlus;

impl ArithmeticOp for IntIntPlus {
    type T = Int;

    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        lhs.checked_add(*rhs)
            .ok_or_else(|| Error::overflow("BIGINT", &format!("({} + {})", lhs, rhs)).into())
            .map(Some)
    }
}

#[derive(Debug)]
pub struct IntUintPlus;

impl ArithmeticOp for IntUintPlus {
    type T = Int;

    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        let res = if *lhs >= 0 {
            (*lhs as u64).checked_add(*rhs as u64)
        } else {
            (*rhs as u64).checked_sub(lhs.overflowing_neg().0 as u64)
        };
        res.ok_or_else(|| {
            Error::overflow("BIGINT UNSIGNED", &format!("({} + {})", lhs, rhs)).into()
        })
        .map(|v| Some(v as i64))
    }
}

#[derive(Debug)]
pub struct UintIntPlus;

impl ArithmeticOp for UintIntPlus {
    type T = Int;

    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        IntUintPlus::calc(rhs, lhs)
    }
}
#[derive(Debug)]
pub struct UintUintPlus;

impl ArithmeticOp for UintUintPlus {
    type T = Int;

    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        (*lhs as u64)
            .checked_add(*rhs as u64)
            .ok_or_else(|| {
                Error::overflow("BIGINT UNSIGNED", &format!("({} + {})", lhs, rhs)).into()
            })
            .map(|v| Some(v as i64))
    }
}

#[derive(Debug)]
pub struct RealPlus;

impl ArithmeticOp for RealPlus {
    type T = Real;

    fn calc(lhs: &Real, rhs: &Real) -> Result<Option<Real>> {
        let res = lhs + rhs;
        if res.is_infinite() {
            Err(Error::overflow("DOUBLE", &format!("({} + {})", lhs, rhs)))?;
        }
        Ok(Some(res))
    }
}

#[derive(Debug)]
pub struct DecimalPlus;

impl ArithmeticOp for DecimalPlus {
    type T = Decimal;

    fn calc(lhs: &Decimal, rhs: &Decimal) -> Result<Option<Decimal>> {
        let res: codec::Result<Decimal> = (lhs + rhs).into();
        Ok(Some(res?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coprocessor::dag::rpn_expr::types::test_util::RpnFnScalarEvaluator;
    use cop_datatype::builder::FieldTypeBuilder;
    use cop_datatype::{FieldTypeFlag, FieldTypeTp};
    use std::i64;
    use tipb::expression::ScalarFuncSig::*;

    #[test]
    fn test_arithmetic_int() {
        let test_cases = vec![
            (PlusInt, None, false, Some(1), false, None),
            (PlusInt, Some(1), false, None, false, None),
            (PlusInt, Some(17), false, Some(25), false, Some(42)),
            (
                PlusInt,
                Some(i64::MIN),
                false,
                Some((i64::MAX as u64 + 1) as i64),
                true,
                Some(0),
            ),
        ];
        for (sig, lhs, lhs_is_unsigned, rhs, rhs_is_unsigned, expected) in test_cases {
            let lhs_field_type = FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .flag(if lhs_is_unsigned {
                    FieldTypeFlag::UNSIGNED
                } else {
                    FieldTypeFlag::empty()
                })
                .build();
            let rhs_field_type = FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .flag(if rhs_is_unsigned {
                    FieldTypeFlag::UNSIGNED
                } else {
                    FieldTypeFlag::empty()
                })
                .build();
            let output = RpnFnScalarEvaluator::new()
                .push_param_with_field_type(lhs, lhs_field_type)
                .push_param_with_field_type(rhs, rhs_field_type)
                .evaluate::<Int>(sig)
                .unwrap();
            assert_eq!(output, expected, "{:?}, {:?}", output, expected);
        }
    }

    #[test]
    fn test_arithmetic_real() {
        let test_cases = vec![(PlusReal, Some(1.01001), Some(-0.01), Some(1.00001))];
        for (sig, lhs, rhs, expected) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(lhs)
                .push_param(rhs)
                .evaluate::<Real>(sig)
                .unwrap();
            assert_eq!(output, expected, "{:?}, {:?}", output, expected);
        }
    }

    #[test]
    fn test_arithmetic_decimal() {
        let test_cases = vec![(PlusDecimal, "1.1", "2.2", "3.3")];
        for (sig, lhs, rhs, expected) in test_cases {
            let expected = expected.parse().ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(lhs.parse::<Decimal>().ok())
                .push_param(rhs.parse::<Decimal>().ok())
                .evaluate::<Decimal>(sig)
                .unwrap();
            assert_eq!(output, expected, "{:?}, {:?}", output, expected);
        }
    }
}
