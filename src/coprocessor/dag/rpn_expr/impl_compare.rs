// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;

use cop_codegen::RpnFunction;

use super::types::RpnFnCallPayload;
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::{codec, Error, Result};

#[derive(RpnFunction)]
#[rpn_function(args = 2)]
pub struct RpnFnCompare<C: Comparer> {
    _phantom: std::marker::PhantomData<C>,
}

impl<C: Comparer> RpnFnCompare<C> {
    #[inline]
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    fn call(
        _ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        lhs: &Option<C::T>,
        rhs: &Option<C::T>,
    ) -> Result<Option<i64>> {
        C::compare(lhs, rhs)
    }
}

impl<C: Comparer> std::fmt::Debug for RpnFnCompare<C> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RpnFnCompare")
    }
}

// See rust-lang/rust#26925 for why the followings are implemented manually. =====

impl<C: Comparer> Copy for RpnFnCompare<C> {}

impl<C: Comparer> Clone for RpnFnCompare<C> {
    #[inline]
    fn clone(&self) -> Self {
        Self::new()
    }
}

// ======

pub trait Comparer: 'static + Send + Sync {
    type T: Evaluable;

    fn compare(lhs: &Option<Self::T>, rhs: &Option<Self::T>) -> Result<Option<i64>>;
}

pub struct RealComparer<F: CmpOp> {
    _phantom_f: std::marker::PhantomData<F>,
}

impl<F: CmpOp> Comparer for RealComparer<F> {
    type T = Real;

    #[inline]
    fn compare(lhs: &Option<Real>, rhs: &Option<Real>) -> Result<Option<i64>> {
        match (lhs, rhs) {
            (None, None) => Ok(F::compare_null()),
            (None, _) | (_, None) => Ok(F::compare_partial_null()),
            (Some(lhs), Some(rhs)) => lhs
                .partial_cmp(rhs)
                // FIXME: It is weird to be a codec error.
                // FIXME: This should never happen because special numbers like NaN and Inf are not
                // allowed at all.
                .ok_or_else(|| {
                    Error::from(codec::Error::InvalidDataType(format!(
                        "{} and {} can't be compared",
                        lhs, rhs
                    )))
                })
                .map(|v| Some(F::compare_order(v) as i64)),
        }
    }
}

pub trait CmpOp: 'static + Send + Sync {
    #[inline]
    fn compare_null() -> Option<i64> {
        None
    }

    #[inline]
    fn compare_partial_null() -> Option<i64> {
        None
    }

    fn compare_order(ordering: std::cmp::Ordering) -> bool;
}

pub struct CmpOpLT;

impl CmpOp for CmpOpLT {
    #[inline]
    fn compare_order(ordering: Ordering) -> bool {
        ordering == Ordering::Less
    }
}

pub struct CmpOpLE;

impl CmpOp for CmpOpLE {
    #[inline]
    fn compare_order(ordering: Ordering) -> bool {
        ordering != Ordering::Greater
    }
}

pub struct CmpOpGT;

impl CmpOp for CmpOpGT {
    #[inline]
    fn compare_order(ordering: Ordering) -> bool {
        ordering == Ordering::Greater
    }
}

pub struct CmpOpGE;

impl CmpOp for CmpOpGE {
    #[inline]
    fn compare_order(ordering: Ordering) -> bool {
        ordering != Ordering::Less
    }
}

pub struct CmpOpNE;

impl CmpOp for CmpOpNE {
    #[inline]
    fn compare_order(ordering: Ordering) -> bool {
        ordering != Ordering::Equal
    }
}

pub struct CmpOpEQ;

impl CmpOp for CmpOpEQ {
    #[inline]
    fn compare_order(ordering: Ordering) -> bool {
        ordering == Ordering::Equal
    }
}

pub struct CmpOpNullEQ;

impl CmpOp for CmpOpNullEQ {
    #[inline]
    fn compare_null() -> Option<i64> {
        Some(1)
    }

    #[inline]
    fn compare_partial_null() -> Option<i64> {
        Some(0)
    }

    #[inline]
    fn compare_order(ordering: Ordering) -> bool {
        ordering == Ordering::Equal
    }
}

#[cfg(test)]
mod tests {
    use tipb::expression::ScalarFuncSig;

    use crate::coprocessor::dag::rpn_expr::types::test_util::RpnFnScalarEvaluator;

    #[test]
    fn test_compare_real() {
        let test_cases = vec![
            (None, None, ScalarFuncSig::GTReal, None),
            (Some(3.5), None, ScalarFuncSig::GTReal, None),
            (Some(-2.1), None, ScalarFuncSig::GTReal, None),
            (None, Some(3.5), ScalarFuncSig::GTReal, None),
            (None, Some(-2.1), ScalarFuncSig::GTReal, None),
            (Some(3.5), Some(-2.1), ScalarFuncSig::GTReal, Some(1)),
            (Some(-2.1), Some(3.5), ScalarFuncSig::GTReal, Some(0)),
            (Some(3.5), Some(3.5), ScalarFuncSig::GTReal, Some(0)),
            (Some(-2.1), Some(-2.1), ScalarFuncSig::GTReal, Some(0)),
            (None, None, ScalarFuncSig::GEReal, None),
            (Some(3.5), None, ScalarFuncSig::GEReal, None),
            (Some(-2.1), None, ScalarFuncSig::GEReal, None),
            (None, Some(3.5), ScalarFuncSig::GEReal, None),
            (None, Some(-2.1), ScalarFuncSig::GEReal, None),
            (Some(3.5), Some(-2.1), ScalarFuncSig::GEReal, Some(1)),
            (Some(-2.1), Some(3.5), ScalarFuncSig::GEReal, Some(0)),
            (Some(3.5), Some(3.5), ScalarFuncSig::GEReal, Some(1)),
            (Some(-2.1), Some(-2.1), ScalarFuncSig::GEReal, Some(1)),
            (None, None, ScalarFuncSig::LTReal, None),
            (Some(3.5), None, ScalarFuncSig::LTReal, None),
            (Some(-2.1), None, ScalarFuncSig::LTReal, None),
            (None, Some(3.5), ScalarFuncSig::LTReal, None),
            (None, Some(-2.1), ScalarFuncSig::LTReal, None),
            (Some(3.5), Some(-2.1), ScalarFuncSig::LTReal, Some(0)),
            (Some(-2.1), Some(3.5), ScalarFuncSig::LTReal, Some(1)),
            (Some(3.5), Some(3.5), ScalarFuncSig::LTReal, Some(0)),
            (Some(-2.1), Some(-2.1), ScalarFuncSig::LTReal, Some(0)),
            (None, None, ScalarFuncSig::LEReal, None),
            (Some(3.5), None, ScalarFuncSig::LEReal, None),
            (Some(-2.1), None, ScalarFuncSig::LEReal, None),
            (None, Some(3.5), ScalarFuncSig::LEReal, None),
            (None, Some(-2.1), ScalarFuncSig::LEReal, None),
            (Some(3.5), Some(-2.1), ScalarFuncSig::LEReal, Some(0)),
            (Some(-2.1), Some(3.5), ScalarFuncSig::LEReal, Some(1)),
            (Some(3.5), Some(3.5), ScalarFuncSig::LEReal, Some(1)),
            (Some(-2.1), Some(-2.1), ScalarFuncSig::LEReal, Some(1)),
            (None, None, ScalarFuncSig::EQReal, None),
            (Some(3.5), None, ScalarFuncSig::EQReal, None),
            (Some(-2.1), None, ScalarFuncSig::EQReal, None),
            (None, Some(3.5), ScalarFuncSig::EQReal, None),
            (None, Some(-2.1), ScalarFuncSig::EQReal, None),
            (Some(3.5), Some(-2.1), ScalarFuncSig::EQReal, Some(0)),
            (Some(-2.1), Some(3.5), ScalarFuncSig::EQReal, Some(0)),
            (Some(3.5), Some(3.5), ScalarFuncSig::EQReal, Some(1)),
            (Some(-2.1), Some(-2.1), ScalarFuncSig::EQReal, Some(1)),
            (None, None, ScalarFuncSig::NEReal, None),
            (Some(3.5), None, ScalarFuncSig::NEReal, None),
            (Some(-2.1), None, ScalarFuncSig::NEReal, None),
            (None, Some(3.5), ScalarFuncSig::NEReal, None),
            (None, Some(-2.1), ScalarFuncSig::NEReal, None),
            (Some(3.5), Some(-2.1), ScalarFuncSig::NEReal, Some(1)),
            (Some(-2.1), Some(3.5), ScalarFuncSig::NEReal, Some(1)),
            (Some(3.5), Some(3.5), ScalarFuncSig::NEReal, Some(0)),
            (Some(-2.1), Some(-2.1), ScalarFuncSig::NEReal, Some(0)),
            (None, None, ScalarFuncSig::NullEQReal, Some(1)),
            (Some(3.5), None, ScalarFuncSig::NullEQReal, Some(0)),
            (Some(-2.1), None, ScalarFuncSig::NullEQReal, Some(0)),
            (None, Some(3.5), ScalarFuncSig::NullEQReal, Some(0)),
            (None, Some(-2.1), ScalarFuncSig::NullEQReal, Some(0)),
            (Some(3.5), Some(-2.1), ScalarFuncSig::NullEQReal, Some(0)),
            (Some(-2.1), Some(3.5), ScalarFuncSig::NullEQReal, Some(0)),
            (Some(3.5), Some(3.5), ScalarFuncSig::NullEQReal, Some(1)),
            (Some(-2.1), Some(-2.1), ScalarFuncSig::NullEQReal, Some(1)),
        ];
        for (arg0, arg1, sig, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0)
                .push_param(arg1)
                .evaluate(sig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}, {:?}, {:?}", arg0, arg1, sig);
        }
    }
}
