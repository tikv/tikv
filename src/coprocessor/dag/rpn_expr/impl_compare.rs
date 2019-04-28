// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;

use super::types::RpnFnCallPayload;
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::{codec, Error, Result};

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

unsafe impl<C: Comparer> Send for RpnFnCompare<C> {}

unsafe impl<C: Comparer> Sync for RpnFnCompare<C> {}

// ======

impl_template_fn! { 2 arg @ RpnFnCompare<C>, C: Comparer }

pub trait Comparer: 'static {
    type T: Evaluable;

    fn compare(lhs: &Option<Self::T>, rhs: &Option<Self::T>) -> Result<Option<i64>>;
}

pub struct BasicComparer<T: Evaluable + Ord, F: CmpOp> {
    _phantom_t: std::marker::PhantomData<T>,
    _phantom_f: std::marker::PhantomData<F>,
}

impl<T: Evaluable + Ord, F: CmpOp> Comparer for BasicComparer<T, F> {
    type T = T;

    #[inline]
    fn compare(lhs: &Option<T>, rhs: &Option<T>) -> Result<Option<i64>> {
        Ok(match (lhs, rhs) {
            (None, None) => F::compare_null(),
            (None, _) | (_, None) => F::compare_partial_null(),
            (Some(lhs), Some(rhs)) => Some(F::compare_order(lhs.cmp(rhs)) as i64),
        })
    }
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
                // FIXME: It is wired to be a codec error.
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

pub struct UintUintComparer<F: CmpOp> {
    _phantom_f: std::marker::PhantomData<F>,
}

impl<F: CmpOp> Comparer for UintUintComparer<F> {
    type T = Int;

    #[inline]
    fn compare(lhs: &Option<Int>, rhs: &Option<Int>) -> Result<Option<i64>> {
        Ok(match (lhs, rhs) {
            (None, None) => F::compare_null(),
            (None, _) | (_, None) => F::compare_partial_null(),
            (Some(lhs), Some(rhs)) => {
                let lhs = *lhs as u64;
                let rhs = *rhs as u64;
                Some(F::compare_order(lhs.cmp(&rhs)) as i64)
            }
        })
    }
}

pub struct UintIntComparer<F: CmpOp> {
    _phantom_f: std::marker::PhantomData<F>,
}

impl<F: CmpOp> Comparer for UintIntComparer<F> {
    type T = Int;

    #[inline]
    fn compare(lhs: &Option<Int>, rhs: &Option<Int>) -> Result<Option<i64>> {
        Ok(match (lhs, rhs) {
            (None, None) => F::compare_null(),
            (None, _) | (_, None) => F::compare_partial_null(),
            (Some(lhs), Some(rhs)) => {
                let ordering = if *rhs < 0 || *lhs as u64 > std::i64::MAX as u64 {
                    Ordering::Greater
                } else {
                    lhs.cmp(&rhs)
                };
                Some(F::compare_order(ordering) as i64)
            }
        })
    }
}

pub struct IntUintComparer<F: CmpOp> {
    _phantom_f: std::marker::PhantomData<F>,
}

impl<F: CmpOp> Comparer for IntUintComparer<F> {
    type T = Int;

    #[inline]
    fn compare(lhs: &Option<Int>, rhs: &Option<Int>) -> Result<Option<i64>> {
        Ok(match (lhs, rhs) {
            (None, None) => F::compare_null(),
            (None, _) | (_, None) => F::compare_partial_null(),
            (Some(lhs), Some(rhs)) => {
                let ordering = if *lhs < 0 || *rhs as u64 > std::i64::MAX as u64 {
                    Ordering::Less
                } else {
                    lhs.cmp(&rhs)
                };
                Some(F::compare_order(ordering) as i64)
            }
        })
    }
}

pub trait CmpOp: 'static {
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
