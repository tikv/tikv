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

#[cfg(test)]
mod tests {
    use super::*;

    use cop_datatype::builder::FieldTypeBuilder;
    use cop_datatype::{FieldTypeFlag, FieldTypeTp};
    use tipb::expression::ScalarFuncSig;

    use crate::coprocessor::dag::rpn_expr::types::test_util::RpnFnScalarEvaluator;

    #[test]
    fn test_compare_signed_int() {
        let test_cases = vec![
            (None, None, ScalarFuncSig::GTInt, None),
            (Some(1), None, ScalarFuncSig::GTInt, None),
            (Some(-1), None, ScalarFuncSig::GTInt, None),
            (None, Some(1), ScalarFuncSig::GTInt, None),
            (None, Some(-1), ScalarFuncSig::GTInt, None),
            (Some(1), Some(-1), ScalarFuncSig::GTInt, Some(1)),
            (Some(-1), Some(1), ScalarFuncSig::GTInt, Some(0)),
            (Some(1), Some(1), ScalarFuncSig::GTInt, Some(0)),
            (Some(-1), Some(-1), ScalarFuncSig::GTInt, Some(0)),
            (None, None, ScalarFuncSig::GEInt, None),
            (Some(1), None, ScalarFuncSig::GEInt, None),
            (Some(-1), None, ScalarFuncSig::GEInt, None),
            (None, Some(1), ScalarFuncSig::GEInt, None),
            (None, Some(-1), ScalarFuncSig::GEInt, None),
            (Some(1), Some(-1), ScalarFuncSig::GEInt, Some(1)),
            (Some(-1), Some(1), ScalarFuncSig::GEInt, Some(0)),
            (Some(1), Some(1), ScalarFuncSig::GEInt, Some(1)),
            (Some(-1), Some(-1), ScalarFuncSig::GEInt, Some(1)),
            (None, None, ScalarFuncSig::LTInt, None),
            (Some(1), None, ScalarFuncSig::LTInt, None),
            (Some(-1), None, ScalarFuncSig::LTInt, None),
            (None, Some(1), ScalarFuncSig::LTInt, None),
            (None, Some(-1), ScalarFuncSig::LTInt, None),
            (Some(1), Some(-1), ScalarFuncSig::LTInt, Some(0)),
            (Some(-1), Some(1), ScalarFuncSig::LTInt, Some(1)),
            (Some(1), Some(1), ScalarFuncSig::LTInt, Some(0)),
            (Some(-1), Some(-1), ScalarFuncSig::LTInt, Some(0)),
            (None, None, ScalarFuncSig::LEInt, None),
            (Some(1), None, ScalarFuncSig::LEInt, None),
            (Some(-1), None, ScalarFuncSig::LEInt, None),
            (None, Some(1), ScalarFuncSig::LEInt, None),
            (None, Some(-1), ScalarFuncSig::LEInt, None),
            (Some(1), Some(-1), ScalarFuncSig::LEInt, Some(0)),
            (Some(-1), Some(1), ScalarFuncSig::LEInt, Some(1)),
            (Some(1), Some(1), ScalarFuncSig::LEInt, Some(1)),
            (Some(-1), Some(-1), ScalarFuncSig::LEInt, Some(1)),
            (None, None, ScalarFuncSig::EQInt, None),
            (Some(1), None, ScalarFuncSig::EQInt, None),
            (Some(-1), None, ScalarFuncSig::EQInt, None),
            (None, Some(1), ScalarFuncSig::EQInt, None),
            (None, Some(-1), ScalarFuncSig::EQInt, None),
            (Some(1), Some(-1), ScalarFuncSig::EQInt, Some(0)),
            (Some(-1), Some(1), ScalarFuncSig::EQInt, Some(0)),
            (Some(1), Some(1), ScalarFuncSig::EQInt, Some(1)),
            (Some(-1), Some(-1), ScalarFuncSig::EQInt, Some(1)),
            (None, None, ScalarFuncSig::NEInt, None),
            (Some(1), None, ScalarFuncSig::NEInt, None),
            (Some(-1), None, ScalarFuncSig::NEInt, None),
            (None, Some(1), ScalarFuncSig::NEInt, None),
            (None, Some(-1), ScalarFuncSig::NEInt, None),
            (Some(1), Some(-1), ScalarFuncSig::NEInt, Some(1)),
            (Some(-1), Some(1), ScalarFuncSig::NEInt, Some(1)),
            (Some(1), Some(1), ScalarFuncSig::NEInt, Some(0)),
            (Some(-1), Some(-1), ScalarFuncSig::NEInt, Some(0)),
            (None, None, ScalarFuncSig::NullEQInt, Some(1)),
            (Some(1), None, ScalarFuncSig::NullEQInt, Some(0)),
            (Some(-1), None, ScalarFuncSig::NullEQInt, Some(0)),
            (None, Some(1), ScalarFuncSig::NullEQInt, Some(0)),
            (None, Some(-1), ScalarFuncSig::NullEQInt, Some(0)),
            (Some(1), Some(-1), ScalarFuncSig::NullEQInt, Some(0)),
            (Some(-1), Some(1), ScalarFuncSig::NullEQInt, Some(0)),
            (Some(1), Some(1), ScalarFuncSig::NullEQInt, Some(1)),
            (Some(-1), Some(-1), ScalarFuncSig::NullEQInt, Some(1)),
        ];
        for (arg0, arg1, sig, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0)
                .push_param(arg1)
                .evaluate(sig)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_compare_int_2() {
        let test_cases = vec![
            (Some(5), false, Some(3), false, Ordering::Greater),
            (
                Some(std::u64::MAX as i64),
                false,
                Some(5),
                false,
                Ordering::Less,
            ),
            (
                Some(std::u64::MAX as i64),
                true,
                Some((std::u64::MAX - 1) as i64),
                true,
                Ordering::Greater,
            ),
            (
                Some(std::u64::MAX as i64),
                true,
                Some(5),
                true,
                Ordering::Greater,
            ),
            (Some(5), true, Some(std::i64::MIN), false, Ordering::Greater),
            (
                Some(std::u64::MAX as i64),
                true,
                Some(std::i64::MIN),
                false,
                Ordering::Greater,
            ),
            (Some(5), true, Some(3), false, Ordering::Greater),
            (Some(std::i64::MIN), false, Some(3), true, Ordering::Less),
            (
                Some(5),
                false,
                Some(std::u64::MAX as i64),
                true,
                Ordering::Less,
            ),
            (Some(5), false, Some(3), true, Ordering::Greater),
        ];
        for (lhs, lhs_is_unsigned, rhs, rhs_is_unsigned, ordering) in test_cases {
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

            for (sig, accept_orderings) in &[
                (ScalarFuncSig::EQInt, vec![Ordering::Equal]),
                (
                    ScalarFuncSig::NEInt,
                    vec![Ordering::Greater, Ordering::Less],
                ),
                (ScalarFuncSig::GTInt, vec![Ordering::Greater]),
                (
                    ScalarFuncSig::GEInt,
                    vec![Ordering::Greater, Ordering::Equal],
                ),
                (ScalarFuncSig::LTInt, vec![Ordering::Less]),
                (ScalarFuncSig::LEInt, vec![Ordering::Less, Ordering::Equal]),
            ] {
                let output = RpnFnScalarEvaluator::new()
                    .push_param_with_field_type(lhs, lhs_field_type.clone())
                    .push_param_with_field_type(rhs, rhs_field_type.clone())
                    .evaluate(sig.clone())
                    .unwrap();
                if accept_orderings.iter().any(|&x| x == ordering) {
                    assert_eq!(output, Some(1));
                } else {
                    assert_eq!(output, Some(0));
                }
            }
        }
    }
}
