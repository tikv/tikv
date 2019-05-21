// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use cop_codegen::RpnFunction;

use super::types::RpnFnCallPayload;
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::codec::mysql::{Decimal, Res};
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

#[derive(Debug)]
pub struct IntIntMod;

impl ArithmeticOp for IntIntMod {
    type T = Int;

    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        if *rhs == 0i64 {
            return Ok(None);
        }
        Ok(Some(lhs % rhs))
    }
}

#[derive(Debug)]
pub struct IntUintMod;

impl ArithmeticOp for IntUintMod {
    type T = Int;

    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        if *rhs == 0i64 {
            return Ok(None);
        }
        Ok(Some(
            ((lhs.overflowing_abs().0 as u64) % (*rhs as u64)) as i64,
        ))
    }
}

#[derive(Debug)]
pub struct UintIntMod;

impl ArithmeticOp for UintIntMod {
    type T = Int;

    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        if *rhs == 0i64 {
            return Ok(None);
        }
        Ok(Some(
            ((*lhs as u64) % (rhs.overflowing_abs().0 as u64)) as i64,
        ))
    }
}

#[derive(Debug)]
pub struct UintUintMod;
impl ArithmeticOp for UintUintMod {
    type T = Int;

    fn calc(lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        if *rhs == 0i64 {
            return Ok(None);
        }
        Ok(Some(((*lhs as u64) % (*rhs as u64)) as i64))
    }
}

#[derive(Debug)]
pub struct RealMod;

impl ArithmeticOp for RealMod {
    type T = Real;

    fn calc(lhs: &Real, rhs: &Real) -> Result<Option<Real>> {
        if *rhs == 0f64 {
            return Ok(None);
        }
        Ok(Some(lhs % rhs))
    }
}

#[derive(Debug)]
pub struct DecimalMod;

impl ArithmeticOp for DecimalMod {
    type T = Decimal;

    fn calc(lhs: &Decimal, rhs: &Decimal) -> Result<Option<Decimal>> {
        if rhs.is_zero() {
            return Ok(None);
        }
        match lhs % rhs {
            Some(v) => match v {
                Res::Ok(v) => Ok(Some(v)),
                Res::Truncated(_) => Err(Error::truncated().into()),
                Res::Overflow(_) => {
                    let overflow = Error::overflow("DECIMAL", &format!("({} % {})", lhs, rhs));
                    Err(overflow.into())
                }
            },
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{i64, u64};

    use super::*;
    use crate::coprocessor::codec::data_type::{Decimal, Int};
    use crate::coprocessor::dag::rpn_expr::types::test_util::RpnFnScalarEvaluator;
    use cop_datatype::builder::FieldTypeBuilder;
    use cop_datatype::{FieldTypeAccessor, FieldTypeFlag, FieldTypeTp};
    use tipb::expression::FieldType;
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
        let test_cases = vec![
            (PlusReal, Some(1.01001), Some(-0.01), Some(1.00001), false),
            (PlusReal, Some(1e308), Some(1e308), None, true),
        ];
        for (sig, lhs, rhs, expected, is_err) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(lhs)
                .push_param(rhs)
                .evaluate::<Real>(sig);
            if is_err {
                assert!(output.is_err())
            } else {
                let output = output.unwrap();
                assert_eq!(output, expected, "{:?}, {:?}", output, expected);
            }
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

    #[test]
    fn test_mod_int() {
        let tests = vec![
            (ModInt, Some(13), Some(11), Some(2)),
            (ModInt, Some(-13), Some(11), Some(-2)),
            (ModInt, Some(13), Some(-11), Some(2)),
            (ModInt, Some(-13), Some(-11), Some(-2)),
            (ModInt, Some(33), Some(11), Some(0)),
            (ModInt, Some(33), Some(-11), Some(0)),
            (ModInt, Some(-33), Some(-11), Some(0)),
            (ModInt, Some(-11), None, None),
            (ModInt, None, Some(-11), None),
            (ModInt, Some(11), Some(0), None),
            (ModInt, Some(-11), Some(0), None),
            (ModInt, Some(i64::MAX), Some(i64::MIN), Some(i64::MAX)),
            (ModInt, Some(i64::MIN), Some(i64::MAX), Some(-1)),
        ];

        for (sig, arg0, arg1, expect) in tests {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0)
                .push_param(arg1)
                .evaluate(sig)
                .unwrap();
            assert_eq!(output, expect, "{:?}, {:?}, {:?}", arg0, arg1, sig);
        }
    }
    #[test]
    fn test_mod_int_unsigned() {
        let tests = vec![
            (
                ModInt,
                Some(u64::MAX as i64),
                true,
                Some(i64::MIN),
                false,
                i64::MAX as u64,
            ),
            (
                ModInt,
                Some(i64::MIN),
                false,
                Some(u64::MAX as i64),
                true,
                i64::MIN as u64,
            ),
        ];

        for (sig, arg0, arg0_unsigned, arg1, arg1_unsigned, expect) in tests {
            let mut evaluator = RpnFnScalarEvaluator::new();
            if arg0_unsigned {
                let mut field_type: FieldType = FieldTypeTp::LongLong.into();
                field_type
                    .as_mut_accessor()
                    .set_flag(FieldTypeFlag::UNSIGNED);
                evaluator = evaluator.push_param_with_field_type(arg0, field_type);
            } else {
                evaluator = evaluator.push_param(arg0);
            }
            if arg1_unsigned {
                let mut field_type: FieldType = FieldTypeTp::LongLong.into();
                field_type
                    .as_mut_accessor()
                    .set_flag(FieldTypeFlag::UNSIGNED);
                evaluator = evaluator.push_param_with_field_type(arg1, field_type);
            } else {
                evaluator = evaluator.push_param(arg1);
            }

            let output: Option<Int> = evaluator.evaluate(sig).unwrap();
            assert_eq!(
                output.unwrap() as u64,
                expect,
                "{:?}, {:?}, {:?}, {:?}, {:?}",
                arg0,
                arg0_unsigned,
                arg1,
                arg1_unsigned,
                sig
            );
        }
    }

    #[test]
    fn test_mod_real() {
        let tests = vec![
            (ModReal, Some(1.0), None, None),
            (ModReal, None, Some(1.0), None),
            (ModReal, Some(1.0), Some(1.1), Some(1.0)),
            (ModReal, Some(-1.0), Some(1.1), Some(-1.0)),
            (ModReal, Some(1.0), Some(-1.1), Some(1.0)),
            (ModReal, Some(-1.0), Some(-1.1), Some(-1.0)),
            (ModReal, Some(1.0), Some(0.0), None),
        ];

        for (sig, arg0, arg1, expect) in tests {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0)
                .push_param(arg1)
                .evaluate(sig)
                .unwrap();
            assert_eq!(output, expect, "{:?}, {:?}, {:?}", arg0, arg1, sig);
        }
    }

    #[test]
    fn test_mod_decimal() {
        fn str2dec(str: &'static str) -> Option<Decimal> {
            Some(str.parse().unwrap())
        }

        let tests = vec![
            (ModDecimal, str2dec("13"), str2dec("11"), str2dec("2")),
            (ModDecimal, str2dec("-13"), str2dec("11"), str2dec("-2")),
            (ModDecimal, str2dec("13"), str2dec("-11"), str2dec("2")),
            (ModDecimal, str2dec("-13"), str2dec("-11"), str2dec("-2")),
            (ModDecimal, str2dec("33"), str2dec("11"), str2dec("0")),
            (ModDecimal, str2dec("-33"), str2dec("11"), str2dec("0")),
            (ModDecimal, str2dec("33"), str2dec("-11"), str2dec("0")),
            (ModDecimal, str2dec("-33"), str2dec("-11"), str2dec("0")),
            (
                ModDecimal,
                str2dec("0.0000000001"),
                str2dec("1.0"),
                str2dec("0.0000000001"),
            ),
            (ModDecimal, str2dec("1"), str2dec("1.1"), str2dec("1")),
            (ModDecimal, str2dec("-1"), str2dec("1.1"), str2dec("-1")),
            (ModDecimal, str2dec("1"), str2dec("-1.1"), str2dec("1")),
            (ModDecimal, str2dec("-1"), str2dec("-1.1"), str2dec("-1")),
            (ModDecimal, str2dec("3"), str2dec("0"), None),
            (ModDecimal, str2dec("-3"), str2dec("0"), None),
            (ModDecimal, str2dec("0"), str2dec("0"), None),
            (ModDecimal, str2dec("-3"), None, None),
            (ModDecimal, None, str2dec("-3"), None),
            (ModDecimal, None, None, None),
        ];

        for (sig, arg0, arg1, expect) in tests {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0.clone())
                .push_param(arg1.clone())
                .evaluate(sig)
                .unwrap();
            assert_eq!(output, expect, "{:?}, {:?}, {:?}", arg0, arg1, sig);
        }
    }
}
