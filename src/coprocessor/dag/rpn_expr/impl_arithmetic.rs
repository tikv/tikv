// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use cop_codegen::RpnFunction;

use super::types::RpnFnCallPayload;
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::codec::mysql::Res;
use crate::coprocessor::codec::{self, Error};
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::dag::rpn_expr::Uint;
use crate::coprocessor::Result;
use std::fmt::Debug;

#[derive(Debug, RpnFunction)]
#[rpn_function(args = 2)]
pub struct RpnFnArithmetic<Arg0, Arg1, Ret, Op>
where
    Arg0: Evaluable,
    Arg1: Evaluable,
    Ret: Evaluable,
    Op: ArithmeticOp<Arg0, Arg1, Ret>,
{
    _phantom: std::marker::PhantomData<(Arg0, Arg1, Ret, Op)>,
}

impl<Arg0, Arg1, Ret, Op> RpnFnArithmetic<Arg0, Arg1, Ret, Op>
where
    Arg0: Evaluable,
    Arg1: Evaluable,
    Ret: Evaluable,
    Op: ArithmeticOp<Arg0, Arg1, Ret>,
{
    fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    fn call(
        ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        arg0: &Option<Arg0>,
        arg1: &Option<Arg1>,
    ) -> Result<Option<Ret>> {
        if let (Some(lhs), Some(rhs)) = (arg0, arg1) {
            Op::calc(ctx, lhs, rhs)
        } else {
            // All arithmetical functions with a NULL argument return NULL
            Ok(None)
        }
    }
}

impl<Arg0, Arg1, Ret, Op> Clone for RpnFnArithmetic<Arg0, Arg1, Ret, Op>
where
    Arg0: Evaluable,
    Arg1: Evaluable,
    Ret: Evaluable,
    Op: ArithmeticOp<Arg0, Arg1, Ret>,
{
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<Arg0, Arg1, Ret, Op> Copy for RpnFnArithmetic<Arg0, Arg1, Ret, Op>
where
    Arg0: Evaluable,
    Arg1: Evaluable,
    Ret: Evaluable,
    Op: ArithmeticOp<Arg0, Arg1, Ret>,
{
}

pub trait ArithmeticOp<Arg0, Arg1 = Arg0, Ret = Arg0>: Send + Sync + Debug + 'static
where
    Arg0: Evaluable,
    Arg1: Evaluable,
    Ret: Evaluable,
{
    fn calc(ctx: &mut EvalContext, lhs: &Arg0, rhs: &Arg1) -> Result<Option<Ret>>;

    fn func() -> RpnFnArithmetic<Arg0, Arg1, Ret, Self>
    where
        Self: Sized,
    {
        RpnFnArithmetic::new()
    }
}

macro_rules! arith_op {
    ($($Op:ident),* $(,)*) => {$(
    #[derive(Debug)]
    pub struct $Op<LHS, RHS=LHS>(std::marker::PhantomData<(LHS, RHS)>);
    )*};
}

arith_op![Plus, Minus, Mod];

impl ArithmeticOp<Int> for Plus<Int> {
    fn calc(_ctx: &mut EvalContext, lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        lhs.checked_add(*rhs)
            .ok_or_else(|| Error::overflow("BIGINT", &format!("({} + {})", lhs, rhs)).into())
            .map(Some)
    }
}

impl ArithmeticOp<Int> for Plus<Int, Uint> {
    fn calc(_ctx: &mut EvalContext, lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
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

impl ArithmeticOp<Int> for Plus<Uint, Int> {
    fn calc(_ctx: &mut EvalContext, lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        Plus::<Int, Uint>::calc(_ctx, rhs, lhs)
    }
}

impl ArithmeticOp<Int> for Plus<Uint, Uint> {
    fn calc(_ctx: &mut EvalContext, lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        (*lhs as u64)
            .checked_add(*rhs as u64)
            .ok_or_else(|| {
                Error::overflow("BIGINT UNSIGNED", &format!("({} + {})", lhs, rhs)).into()
            })
            .map(|v| Some(v as i64))
    }
}

impl ArithmeticOp<Real> for Plus<Real> {
    fn calc(_ctx: &mut EvalContext, lhs: &Real, rhs: &Real) -> Result<Option<Real>> {
        let res = *lhs + *rhs;
        if res.is_infinite() {
            Err(Error::overflow("DOUBLE", &format!("({} + {})", lhs, rhs)))?;
        }
        Ok(Some(res))
    }
}

impl ArithmeticOp<Decimal> for Plus<Decimal> {
    fn calc(_ctx: &mut EvalContext, lhs: &Decimal, rhs: &Decimal) -> Result<Option<Decimal>> {
        let res: codec::Result<Decimal> = (lhs + rhs).into();
        Ok(Some(res?))
    }
}

impl ArithmeticOp<Int> for Minus<Int> {
    fn calc(_ctx: &mut EvalContext, lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        lhs.checked_sub(*rhs)
            .ok_or_else(|| Error::overflow("BIGINT", &format!("({} - {})", lhs, rhs)).into())
            .map(Some)
    }
}

impl ArithmeticOp<Int> for Minus<Int, Uint> {
    fn calc(_ctx: &mut EvalContext, lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        if *lhs >= 0 {
            (*lhs as u64)
                .checked_sub(*rhs as u64)
                .ok_or_else(|| Error::overflow("BIGINT", &format!("({} - {})", lhs, rhs)).into())
                .map(|v| Some(v as i64))
        } else {
            Err(Error::overflow("BIGINT", &format!("({} - {})", lhs, rhs)))?
        }
    }
}

impl ArithmeticOp<Int> for Minus<Uint, Int> {
    fn calc(_ctx: &mut EvalContext, lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        let res = if *rhs >= 0 {
            (*lhs as u64).checked_sub(*rhs as u64)
        } else {
            (*lhs as u64).checked_add(rhs.overflowing_neg().0 as u64)
        };
        res.ok_or_else(|| Error::overflow("BIGINT", &format!("({} - {})", lhs, rhs)).into())
            .map(|v| Some(v as i64))
    }
}

impl ArithmeticOp<Int> for Minus<Uint, Uint> {
    fn calc(_ctx: &mut EvalContext, lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        (*lhs as u64)
            .checked_sub(*rhs as u64)
            .ok_or_else(|| {
                Error::overflow("BIGINT UNSIGNED", &format!("({} - {})", lhs, rhs)).into()
            })
            .map(|v| Some(v as i64))
    }
}

impl ArithmeticOp<Real> for Minus<Real> {
    fn calc(_ctx: &mut EvalContext, lhs: &Real, rhs: &Real) -> Result<Option<Real>> {
        let res = *lhs - *rhs;
        if res.is_infinite() {
            Err(Error::overflow("DOUBLE", &format!("({} - {})", lhs, rhs)))?;
        }
        Ok(Some(res))
    }
}

impl ArithmeticOp<Decimal> for Minus<Decimal> {
    fn calc(_ctx: &mut EvalContext, lhs: &Decimal, rhs: &Decimal) -> Result<Option<Decimal>> {
        let res: codec::Result<Decimal> = (lhs - rhs).into();
        Ok(Some(res?))
    }
}

impl ArithmeticOp<Int> for Mod<Int> {
    fn calc(_ctx: &mut EvalContext, lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        if *rhs == 0i64 {
            return Ok(None);
        }
        Ok(Some(lhs % rhs))
    }
}

impl ArithmeticOp<Int> for Mod<Int, Uint> {
    fn calc(_ctx: &mut EvalContext, lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        if *rhs == 0i64 {
            return Ok(None);
        }
        Ok(Some(
            ((lhs.overflowing_abs().0 as u64) % (*rhs as u64)) as i64,
        ))
    }
}

impl ArithmeticOp<Int> for Mod<Uint, Int> {
    fn calc(_ctx: &mut EvalContext, lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        if *rhs == 0i64 {
            return Ok(None);
        }
        Ok(Some(
            ((*lhs as u64) % (rhs.overflowing_abs().0 as u64)) as i64,
        ))
    }
}

impl ArithmeticOp<Int> for Mod<Uint, Uint> {
    fn calc(_ctx: &mut EvalContext, lhs: &Int, rhs: &Int) -> Result<Option<Int>> {
        if *rhs == 0i64 {
            return Ok(None);
        }
        Ok(Some(((*lhs as u64) % (*rhs as u64)) as i64))
    }
}

impl ArithmeticOp<Real> for Mod<Real> {
    fn calc(_ctx: &mut EvalContext, lhs: &Real, rhs: &Real) -> Result<Option<Real>> {
        if (*rhs).into_inner() == 0f64 {
            return Ok(None);
        }
        Ok(Some(*lhs % *rhs))
    }
}

impl ArithmeticOp<Decimal> for Mod<Decimal> {
    fn calc(_ctx: &mut EvalContext, lhs: &Decimal, rhs: &Decimal) -> Result<Option<Decimal>> {
        if rhs.is_zero() {
            return Ok(None);
        }
        match lhs % rhs {
            Some(v) => match v {
                Res::Ok(v) => Ok(Some(v)),
                Res::Truncated(_) => Err(Error::truncated())?,
                Res::Overflow(_) => {
                    Err(Error::overflow("DECIMAL", &format!("({} % {})", lhs, rhs)))?
                }
            },
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use cop_datatype::builder::FieldTypeBuilder;
    use cop_datatype::{FieldTypeFlag, FieldTypeTp};
    use tipb::expression::ScalarFuncSig;

    use crate::coprocessor::dag::rpn_expr::types::test_util::RpnFnScalarEvaluator;

    #[test]
    fn test_plus_int() {
        let test_cases = vec![
            (None, false, Some(1), false, None),
            (Some(1), false, None, false, None),
            (Some(17), false, Some(25), false, Some(42)),
            (
                Some(std::i64::MIN),
                false,
                Some((std::i64::MAX as u64 + 1) as i64),
                true,
                Some(0),
            ),
        ];
        for (lhs, lhs_is_unsigned, rhs, rhs_is_unsigned, expected) in test_cases {
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
                .evaluate(ScalarFuncSig::PlusInt)
                .unwrap();
            assert_eq!(output, expected, "lhs={:?}, rhs={:?}", lhs, rhs);
        }
    }

    #[test]
    fn test_plus_real() {
        let test_cases = vec![
            (1.01001, -0.01, Some(1.00001), false),
            (1e308, 1e308, None, true),
        ];
        for (lhs, rhs, expected, is_err) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(Real::new(lhs).ok())
                .push_param(Real::new(rhs).ok())
                .evaluate(ScalarFuncSig::PlusReal);
            if is_err {
                assert!(output.is_err())
            } else {
                let output = output.unwrap().map(Real::into_inner);
                assert_eq!(output, expected, "lhs={:?}, rhs={:?}", lhs, rhs);
            }
        }
    }

    #[test]
    fn test_plus_decimal() {
        let test_cases = vec![("1.1", "2.2", "3.3")];
        for (lhs, rhs, expected) in test_cases {
            let expected: Option<Decimal> = expected.parse().ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(lhs.parse::<Decimal>().ok())
                .push_param(rhs.parse::<Decimal>().ok())
                .evaluate(ScalarFuncSig::PlusDecimal)
                .unwrap();
            assert_eq!(output, expected, "lhs={:?}, rhs={:?}", lhs, rhs);
        }
    }

    #[test]
    fn test_minus_int() {
        let test_cases = vec![
            (None, false, Some(1), false, None, false),
            (Some(1), false, None, false, None, false),
            (Some(12), false, Some(1), false, Some(11), false),
            (
                Some(0),
                true,
                Some(std::i64::MIN),
                false,
                Some((std::i64::MAX as u64 + 1) as i64),
                false,
            ),
            (
                Some(std::i64::MIN),
                false,
                Some(std::i64::MAX),
                false,
                None,
                true,
            ),
            (
                Some(std::i64::MAX),
                false,
                Some(std::i64::MIN),
                false,
                None,
                true,
            ),
            (Some(-1), false, Some(2), true, None, true),
            (Some(1), true, Some(2), false, None, true),
        ];
        for (lhs, lhs_is_unsigned, rhs, rhs_is_unsigned, expected, is_err) in test_cases {
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
                .evaluate(ScalarFuncSig::MinusInt);
            if is_err {
                assert!(output.is_err())
            } else {
                let output = output.unwrap();
                assert_eq!(output, expected, "lhs={:?}, rhs={:?}", lhs, rhs);
            }
        }
    }

    #[test]
    fn test_minus_real() {
        let test_cases = vec![
            (
                Real::new(1.01001).ok(),
                Real::new(-0.01).ok(),
                Real::new(1.02001).ok(),
                false,
            ),
            (
                Real::new(std::f64::MIN).ok(),
                Real::new(std::f64::MAX).ok(),
                None,
                true,
            ),
        ];
        for (lhs, rhs, expected, is_err) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(lhs)
                .push_param(rhs)
                .evaluate(ScalarFuncSig::MinusReal);
            if is_err {
                assert!(output.is_err())
            } else {
                let output = output.unwrap();
                assert_eq!(output, expected, "lhs={:?}, rhs={:?}", lhs, rhs);
            }
        }
    }

    #[test]
    fn test_minus_decimal() {
        let test_cases = vec![("1.1", "2.2", "-1.1")];
        for (lhs, rhs, expected) in test_cases {
            let expected: Option<Decimal> = expected.parse().ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(lhs.parse::<Decimal>().ok())
                .push_param(rhs.parse::<Decimal>().ok())
                .evaluate(ScalarFuncSig::MinusDecimal)
                .unwrap();
            assert_eq!(output, expected, "lhs={:?}, rhs={:?}", lhs, rhs);
        }
    }

    #[test]
    fn test_mod_int() {
        let tests = vec![
            (Some(13), Some(11), Some(2)),
            (Some(-13), Some(11), Some(-2)),
            (Some(13), Some(-11), Some(2)),
            (Some(-13), Some(-11), Some(-2)),
            (Some(33), Some(11), Some(0)),
            (Some(33), Some(-11), Some(0)),
            (Some(-33), Some(-11), Some(0)),
            (Some(-11), None, None),
            (None, Some(-11), None),
            (Some(11), Some(0), None),
            (Some(-11), Some(0), None),
            (
                Some(std::i64::MAX),
                Some(std::i64::MIN),
                Some(std::i64::MAX),
            ),
            (Some(std::i64::MIN), Some(std::i64::MAX), Some(-1)),
        ];

        for (lhs, rhs, expected) in tests {
            let output = RpnFnScalarEvaluator::new()
                .push_param(lhs)
                .push_param(rhs)
                .evaluate(ScalarFuncSig::ModInt)
                .unwrap();
            assert_eq!(output, expected, "lhs={:?}, rhs={:?}", lhs, rhs);
        }
    }
    #[test]
    fn test_mod_int_unsigned() {
        let tests = vec![
            (
                Some(std::u64::MAX as i64),
                true,
                Some(std::i64::MIN),
                false,
                Some(std::i64::MAX),
            ),
            (
                Some(std::i64::MIN),
                false,
                Some(std::u64::MAX as i64),
                true,
                Some(std::i64::MIN),
            ),
        ];

        for (lhs, lhs_is_unsigned, rhs, rhs_is_unsigned, expected) in tests {
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
                .evaluate(ScalarFuncSig::ModInt)
                .unwrap();
            assert_eq!(output, expected, "lhs={:?}, rhs={:?}", lhs, rhs);
        }
    }

    #[test]
    fn test_mod_real() {
        let tests = vec![
            (Real::new(1.0).ok(), None, None),
            (None, Real::new(1.0).ok(), None),
            (
                Real::new(1.0).ok(),
                Real::new(1.1).ok(),
                Real::new(1.0).ok(),
            ),
            (
                Real::new(-1.0).ok(),
                Real::new(1.1).ok(),
                Real::new(-1.0).ok(),
            ),
            (
                Real::new(1.0).ok(),
                Real::new(-1.1).ok(),
                Real::new(1.0).ok(),
            ),
            (
                Real::new(-1.0).ok(),
                Real::new(-1.1).ok(),
                Real::new(-1.0).ok(),
            ),
            (Real::new(1.0).ok(), Real::new(0.0).ok(), None),
        ];

        for (lhs, rhs, expected) in tests {
            let output = RpnFnScalarEvaluator::new()
                .push_param(lhs)
                .push_param(rhs)
                .evaluate(ScalarFuncSig::ModReal)
                .unwrap();
            assert_eq!(output, expected, "lhs={:?}, rhs={:?}", lhs, rhs);
        }
    }

    #[test]
    fn test_mod_decimal() {
        let tests = vec![
            ("13", "11", "2"),
            ("-13", "11", "-2"),
            ("13", "-11", "2"),
            ("-13", "-11", "-2"),
            ("33", "11", "0"),
            ("-33", "11", "0"),
            ("33", "-11", "0"),
            ("-33", "-11", "0"),
            ("0.0000000001", "1.0", "0.0000000001"),
            ("1", "1.1", "1"),
            ("-1", "1.1", "-1"),
            ("1", "-1.1", "1"),
            ("-1", "-1.1", "-1"),
            ("3", "0", ""),
            ("-3", "0", ""),
            ("0", "0", ""),
            ("-3", "", ""),
            ("", ("-3"), ""),
            ("", "", ""),
        ];

        for (lhs, rhs, expected) in tests {
            let expected = expected.parse::<Decimal>().ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(lhs.parse::<Decimal>().ok())
                .push_param(rhs.parse::<Decimal>().ok())
                .evaluate(ScalarFuncSig::ModDecimal)
                .unwrap();
            assert_eq!(output, expected, "lhs={:?}, rhs={:?}", lhs, rhs);
        }
    }
}
