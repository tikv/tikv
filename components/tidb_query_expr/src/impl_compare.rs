// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::{max, min, Ordering},
    str,
};

use tidb_query_codegen::rpn_fn;
use tidb_query_common::Result;
use tidb_query_datatype::{
    codec::{collation::Collator, data_type::*, mysql::Time, Error},
    expr::EvalContext,
};

#[rpn_fn(nullable)]
#[inline]
pub fn compare<C: Comparer>(lhs: Option<&C::T>, rhs: Option<&C::T>) -> Result<Option<i64>>
where
    C: Comparer,
{
    C::compare(lhs, rhs)
}

#[rpn_fn(nullable)]
#[inline]
pub fn compare_json<F: CmpOp>(lhs: Option<JsonRef>, rhs: Option<JsonRef>) -> Result<Option<i64>> {
    Ok(match (lhs, rhs) {
        (None, None) => F::compare_null(),
        (None, _) | (_, None) => F::compare_partial_null(),
        (Some(lhs), Some(rhs)) => Some(F::compare_order(lhs.cmp(&rhs)) as i64),
    })
}

#[rpn_fn(nullable)]
#[inline]
pub fn compare_bytes<C: Collator, F: CmpOp>(
    lhs: Option<BytesRef>,
    rhs: Option<BytesRef>,
) -> Result<Option<i64>> {
    Ok(match (lhs, rhs) {
        (None, None) => F::compare_null(),
        (None, _) | (_, None) => F::compare_partial_null(),
        (Some(lhs), Some(rhs)) => {
            let ord = C::sort_compare(lhs, rhs)?;
            Some(F::compare_order(ord) as i64)
        }
    })
}

pub trait Comparer {
    type T: Evaluable + EvaluableRet;

    fn compare(lhs: Option<&Self::T>, rhs: Option<&Self::T>) -> Result<Option<i64>>;
}

pub struct BasicComparer<T: Evaluable + Ord, F: CmpOp> {
    _phantom: std::marker::PhantomData<(T, F)>,
}

impl<T: Evaluable + EvaluableRet + Ord, F: CmpOp> Comparer for BasicComparer<T, F> {
    type T = T;

    #[inline]
    fn compare(lhs: Option<&T>, rhs: Option<&T>) -> Result<Option<i64>> {
        Ok(match (lhs, rhs) {
            (None, None) => F::compare_null(),
            (None, _) | (_, None) => F::compare_partial_null(),
            (Some(lhs), Some(rhs)) => Some(F::compare_order(lhs.cmp(rhs)) as i64),
        })
    }
}

pub struct UintUintComparer<F: CmpOp> {
    _phantom: std::marker::PhantomData<F>,
}

impl<F: CmpOp> Comparer for UintUintComparer<F> {
    type T = Int;

    #[inline]
    fn compare(lhs: Option<&Int>, rhs: Option<&Int>) -> Result<Option<i64>> {
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
    _phantom: std::marker::PhantomData<F>,
}

impl<F: CmpOp> Comparer for UintIntComparer<F> {
    type T = Int;

    #[inline]
    fn compare(lhs: Option<&Int>, rhs: Option<&Int>) -> Result<Option<i64>> {
        Ok(match (lhs, rhs) {
            (None, None) => F::compare_null(),
            (None, _) | (_, None) => F::compare_partial_null(),
            (Some(lhs), Some(rhs)) => {
                let ordering = if *rhs < 0 || *lhs as u64 > i64::MAX as u64 {
                    Ordering::Greater
                } else {
                    lhs.cmp(rhs)
                };
                Some(F::compare_order(ordering) as i64)
            }
        })
    }
}

pub struct IntUintComparer<F: CmpOp> {
    _phantom: std::marker::PhantomData<F>,
}

impl<F: CmpOp> Comparer for IntUintComparer<F> {
    type T = Int;

    #[inline]
    fn compare(lhs: Option<&Int>, rhs: Option<&Int>) -> Result<Option<i64>> {
        Ok(match (lhs, rhs) {
            (None, None) => F::compare_null(),
            (None, _) | (_, None) => F::compare_partial_null(),
            (Some(lhs), Some(rhs)) => {
                let ordering = if *lhs < 0 || *rhs as u64 > i64::MAX as u64 {
                    Ordering::Less
                } else {
                    lhs.cmp(rhs)
                };
                Some(F::compare_order(ordering) as i64)
            }
        })
    }
}

pub trait CmpOp {
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

#[rpn_fn(nullable, varg)]
#[inline]
pub fn coalesce<T: Evaluable + EvaluableRet>(args: &[Option<&T>]) -> Result<Option<T>> {
    for arg in args {
        if arg.is_some() {
            return Ok(arg.cloned());
        }
    }
    Ok(None)
}

#[rpn_fn(nullable, varg)]
#[inline]
pub fn coalesce_bytes(args: &[Option<BytesRef>]) -> Result<Option<Bytes>> {
    for arg in args {
        if arg.is_some() {
            return Ok(arg.map(|x| x.to_vec()));
        }
    }
    Ok(None)
}

#[rpn_fn(nullable, varg)]
#[inline]
pub fn coalesce_json(args: &[Option<JsonRef>]) -> Result<Option<Json>> {
    for arg in args {
        if arg.is_some() {
            return Ok(arg.map(|x| x.to_owned()));
        }
    }
    Ok(None)
}

#[rpn_fn(nullable, varg, min_args = 2)]
#[inline]
pub fn greatest_int(args: &[Option<&Int>]) -> Result<Option<Int>> {
    do_get_extremum(args, max)
}

#[rpn_fn(nullable, varg, min_args = 2)]
#[inline]
pub fn least_int(args: &[Option<&Int>]) -> Result<Option<Int>> {
    do_get_extremum(args, min)
}

#[rpn_fn(nullable, varg, min_args = 2)]
#[inline]
pub fn interval_int(args: &[Option<&Int>]) -> Result<Option<Int>> {
    let target = match args[0] {
        None => return Ok(Some(-1)),
        Some(v) => Some(v),
    };

    let arr = &args[1..];

    match arr.binary_search(&target) {
        Ok(pos) => Ok(Some(pos as Int + 1)),
        Err(pos) => Ok(Some(pos as Int)),
    }
}

#[rpn_fn(nullable, varg, min_args = 2)]
#[inline]
pub fn greatest_decimal(args: &[Option<&Decimal>]) -> Result<Option<Decimal>> {
    do_get_extremum(args, max)
}

#[rpn_fn(nullable, varg, min_args = 2)]
#[inline]
pub fn least_decimal(args: &[Option<&Decimal>]) -> Result<Option<Decimal>> {
    do_get_extremum(args, min)
}

#[rpn_fn(nullable, varg, min_args = 2)]
#[inline]
pub fn greatest_string(args: &[Option<BytesRef>]) -> Result<Option<Bytes>> {
    do_get_extremum(args, max)
}

#[rpn_fn(nullable, varg, min_args = 2)]
#[inline]
pub fn least_string(args: &[Option<BytesRef>]) -> Result<Option<Bytes>> {
    do_get_extremum(args, min)
}

#[rpn_fn(nullable, varg, min_args = 2)]
#[inline]
pub fn greatest_real(args: &[Option<&Real>]) -> Result<Option<Real>> {
    do_get_extremum(args, max)
}

#[rpn_fn(nullable, varg, min_args = 2)]
#[inline]
pub fn least_real(args: &[Option<&Real>]) -> Result<Option<Real>> {
    do_get_extremum(args, min)
}

#[rpn_fn(nullable, varg, min_args = 2)]
#[inline]
pub fn interval_real(args: &[Option<&Real>]) -> Result<Option<Int>> {
    let target = match args[0] {
        None => return Ok(Some(-1)),
        Some(v) => Some(v),
    };

    let arr = &args[1..];

    match arr.binary_search(&target) {
        Ok(pos) => Ok(Some(pos as Int + 1)),
        Err(pos) => Ok(Some(pos as Int)),
    }
}

#[rpn_fn(nullable, varg, min_args = 2, capture = [ctx])]
#[inline]
pub fn greatest_cmp_string_as_time(
    ctx: &mut EvalContext,
    args: &[Option<BytesRef>],
) -> Result<Option<Bytes>> {
    let mut greatest = None;
    for arg in args {
        match arg {
            Some(arg_val) => {
                let s = match str::from_utf8(arg_val) {
                    Ok(s) => s,
                    Err(err) => {
                        return ctx
                            .handle_invalid_time_error(Error::Encoding(err))
                            .map(|_| Ok(None))?;
                    }
                };
                match Time::parse_datetime(ctx, s, Time::parse_fsp(s), true) {
                    Ok(t) => greatest = max(greatest, Some(t)),
                    Err(_) => {
                        return ctx
                            .handle_invalid_time_error(Error::invalid_time_format(&s))
                            .map(|_| Ok(None))?;
                    }
                }
            }
            None => {
                return Ok(None);
            }
        }
    }

    Ok(greatest.map(|time| time.to_string().into_bytes()))
}

#[rpn_fn(nullable, varg, min_args = 2, capture = [ctx])]
#[inline]
pub fn least_cmp_string_as_time(
    ctx: &mut EvalContext,
    args: &[Option<BytesRef>],
) -> Result<Option<Bytes>> {
    // Max datetime range defined at https://dev.mysql.com/doc/refman/8.0/en/datetime.html
    let mut least = Some(Time::parse_datetime(ctx, "9999-12-31 23:59:59", 0, true)?);
    for arg in args {
        match arg {
            Some(arg_val) => {
                let s = match str::from_utf8(arg_val) {
                    Ok(s) => s,
                    Err(err) => {
                        return ctx
                            .handle_invalid_time_error(Error::Encoding(err))
                            .map(|_| Ok(None))?;
                    }
                };
                match Time::parse_datetime(ctx, s, Time::parse_fsp(s), true) {
                    Ok(t) => least = min(least, Some(t)),
                    Err(_) => {
                        return ctx
                            .handle_invalid_time_error(Error::invalid_time_format(&s))
                            .map(|_| Ok(None))?;
                    }
                }
            }
            None => {
                return Ok(None);
            }
        }
    }

    Ok(least.map(|time| time.to_string().into_bytes()))
}

#[rpn_fn(nullable, varg, min_args = 2, capture = [ctx])]
#[inline]
pub fn greatest_cmp_string_as_date(
    ctx: &mut EvalContext,
    args: &[Option<BytesRef>],
) -> Result<Option<Bytes>> {
    let mut greatest = None;
    for arg in args {
        match arg {
            Some(arg_val) => {
                let s = match str::from_utf8(arg_val) {
                    Ok(s) => s,
                    Err(err) => {
                        return ctx
                            .handle_invalid_time_error(Error::Encoding(err))
                            .map(|_| Ok(None))?;
                    }
                };
                match Time::parse_date(ctx, s) {
                    Ok(t) => greatest = max(greatest, Some(t)),
                    Err(_) => {
                        return ctx
                            .handle_invalid_time_error(Error::invalid_time_format(&s))
                            .map(|_| Ok(None))?;
                    }
                }
            }
            None => {
                return Ok(None);
            }
        }
    }

    Ok(greatest.map(|time| time.to_string().into_bytes()))
}

#[rpn_fn(nullable, varg, min_args = 2, capture = [ctx])]
#[inline]
pub fn least_cmp_string_as_date(
    ctx: &mut EvalContext,
    args: &[Option<BytesRef>],
) -> Result<Option<Bytes>> {
    // Max date range defined at https://dev.mysql.com/doc/refman/8.0/en/datetime.html
    let mut least = Some(Time::parse_date(ctx, "9999-12-31")?);
    for arg in args {
        match arg {
            Some(arg_val) => {
                let s = match str::from_utf8(arg_val) {
                    Ok(s) => s,
                    Err(err) => {
                        return ctx
                            .handle_invalid_time_error(Error::Encoding(err))
                            .map(|_| Ok(None))?;
                    }
                };
                match Time::parse_date(ctx, s) {
                    Ok(t) => least = min(least, Some(t)),
                    Err(_) => {
                        return ctx
                            .handle_invalid_time_error(Error::invalid_time_format(&s))
                            .map(|_| Ok(None))?;
                    }
                }
            }
            None => {
                return Ok(None);
            }
        }
    }

    Ok(least.map(|time| time.to_string().into_bytes()))
}

#[rpn_fn(nullable, varg, min_args = 2)]
#[inline]
pub fn greatest_datetime(args: &[Option<&DateTime>]) -> Result<Option<DateTime>> {
    do_get_extremum(args, max)
}

#[rpn_fn(nullable, varg, min_args = 2)]
#[inline]
pub fn least_datetime(args: &[Option<&DateTime>]) -> Result<Option<DateTime>> {
    do_get_extremum(args, min)
}

#[rpn_fn(nullable, varg, min_args = 2)]
#[inline]
pub fn greatest_duration(args: &[Option<&Duration>]) -> Result<Option<Duration>> {
    do_get_extremum(args, max)
}

#[rpn_fn(nullable, varg, min_args = 2)]
#[inline]
pub fn least_duration(args: &[Option<&Duration>]) -> Result<Option<Duration>> {
    do_get_extremum(args, min)
}

#[inline]
fn do_get_extremum<'a, T>(
    args: &[Option<&'a T>],
    chooser: fn(&'a T, &'a T) -> &'a T,
) -> Result<Option<T::Owned>>
where
    T: Ord + ToOwned + ?Sized,
{
    let first = args[0];
    match first {
        None => Ok(None),
        Some(first_val) => {
            let mut res = first_val;
            for arg in &args[1..] {
                match arg {
                    None => {
                        return Ok(None);
                    }
                    Some(v) => {
                        res = chooser(res, *v);
                    }
                }
            }
            Ok(Some(res.to_owned()))
        }
    }
}

#[cfg(test)]
mod tests {
    use tidb_query_datatype::{builder::FieldTypeBuilder, Collation, FieldTypeFlag, FieldTypeTp};
    use tipb::ScalarFuncSig;

    use super::*;
    use crate::test_util::RpnFnScalarEvaluator;

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum TestCaseCmpOp {
        GT,
        GE,
        LT,
        LE,
        EQ,
        NE,
        NullEQ,
    }

    #[allow(clippy::type_complexity)]
    fn generate_numeric_compare_cases()
    -> Vec<(Option<Real>, Option<Real>, TestCaseCmpOp, Option<i64>)> {
        vec![
            (None, None, TestCaseCmpOp::GT, None),
            (Real::new(3.5).ok(), None, TestCaseCmpOp::GT, None),
            (Real::new(-2.1).ok(), None, TestCaseCmpOp::GT, None),
            (None, Real::new(3.5).ok(), TestCaseCmpOp::GT, None),
            (None, Real::new(-2.1).ok(), TestCaseCmpOp::GT, None),
            (
                Real::new(3.5).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::GT,
                Some(1),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::GT,
                Some(0),
            ),
            (
                Real::new(3.5).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::GT,
                Some(0),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::GT,
                Some(0),
            ),
            (None, None, TestCaseCmpOp::GE, None),
            (Real::new(3.5).ok(), None, TestCaseCmpOp::GE, None),
            (Real::new(-2.1).ok(), None, TestCaseCmpOp::GE, None),
            (None, Real::new(3.5).ok(), TestCaseCmpOp::GE, None),
            (None, Real::new(-2.1).ok(), TestCaseCmpOp::GE, None),
            (
                Real::new(3.5).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::GE,
                Some(1),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::GE,
                Some(0),
            ),
            (
                Real::new(3.5).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::GE,
                Some(1),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::GE,
                Some(1),
            ),
            (None, None, TestCaseCmpOp::LT, None),
            (Real::new(3.5).ok(), None, TestCaseCmpOp::LT, None),
            (Real::new(-2.1).ok(), None, TestCaseCmpOp::LT, None),
            (None, Real::new(3.5).ok(), TestCaseCmpOp::LT, None),
            (None, Real::new(-2.1).ok(), TestCaseCmpOp::LT, None),
            (
                Real::new(3.5).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::LT,
                Some(0),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::LT,
                Some(1),
            ),
            (
                Real::new(3.5).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::LT,
                Some(0),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::LT,
                Some(0),
            ),
            (None, None, TestCaseCmpOp::LE, None),
            (Real::new(3.5).ok(), None, TestCaseCmpOp::LE, None),
            (Real::new(-2.1).ok(), None, TestCaseCmpOp::LE, None),
            (None, Real::new(3.5).ok(), TestCaseCmpOp::LE, None),
            (None, Real::new(-2.1).ok(), TestCaseCmpOp::LE, None),
            (
                Real::new(3.5).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::LE,
                Some(0),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::LE,
                Some(1),
            ),
            (
                Real::new(3.5).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::LE,
                Some(1),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::LE,
                Some(1),
            ),
            (None, None, TestCaseCmpOp::EQ, None),
            (Real::new(3.5).ok(), None, TestCaseCmpOp::EQ, None),
            (Real::new(-2.1).ok(), None, TestCaseCmpOp::EQ, None),
            (None, Real::new(3.5).ok(), TestCaseCmpOp::EQ, None),
            (None, Real::new(-2.1).ok(), TestCaseCmpOp::EQ, None),
            (
                Real::new(3.5).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::EQ,
                Some(0),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::EQ,
                Some(0),
            ),
            (
                Real::new(3.5).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::EQ,
                Some(1),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::EQ,
                Some(1),
            ),
            (None, None, TestCaseCmpOp::NE, None),
            (Real::new(3.5).ok(), None, TestCaseCmpOp::NE, None),
            (Real::new(-2.1).ok(), None, TestCaseCmpOp::NE, None),
            (None, Real::new(3.5).ok(), TestCaseCmpOp::NE, None),
            (None, Real::new(-2.1).ok(), TestCaseCmpOp::NE, None),
            (
                Real::new(3.5).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::NE,
                Some(1),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::NE,
                Some(1),
            ),
            (
                Real::new(3.5).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::NE,
                Some(0),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::NE,
                Some(0),
            ),
            (None, None, TestCaseCmpOp::NullEQ, Some(1)),
            (Real::new(3.5).ok(), None, TestCaseCmpOp::NullEQ, Some(0)),
            (Real::new(-2.1).ok(), None, TestCaseCmpOp::NullEQ, Some(0)),
            (None, Real::new(3.5).ok(), TestCaseCmpOp::NullEQ, Some(0)),
            (None, Real::new(-2.1).ok(), TestCaseCmpOp::NullEQ, Some(0)),
            (
                Real::new(3.5).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::NullEQ,
                Some(0),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::NullEQ,
                Some(0),
            ),
            (
                Real::new(3.5).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::NullEQ,
                Some(1),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::NullEQ,
                Some(1),
            ),
        ]
    }

    #[test]
    fn test_compare_real() {
        for (arg0, arg1, cmp_op, expect_output) in generate_numeric_compare_cases() {
            let sig = match cmp_op {
                TestCaseCmpOp::GT => ScalarFuncSig::GtReal,
                TestCaseCmpOp::GE => ScalarFuncSig::GeReal,
                TestCaseCmpOp::LT => ScalarFuncSig::LtReal,
                TestCaseCmpOp::LE => ScalarFuncSig::LeReal,
                TestCaseCmpOp::EQ => ScalarFuncSig::EqReal,
                TestCaseCmpOp::NE => ScalarFuncSig::NeReal,
                TestCaseCmpOp::NullEQ => ScalarFuncSig::NullEqReal,
            };
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0)
                .push_param(arg1)
                .evaluate(sig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}, {:?}, {:?}", arg0, arg1, sig);
        }
    }

    #[test]
    fn test_compare_duration() {
        fn map_double_to_duration(v: Real) -> Duration {
            Duration::from_millis((v.into_inner() * 1000.0) as i64, 4).unwrap()
        }

        for (arg0, arg1, cmp_op, expect_output) in generate_numeric_compare_cases() {
            let sig = match cmp_op {
                TestCaseCmpOp::GT => ScalarFuncSig::GtDuration,
                TestCaseCmpOp::GE => ScalarFuncSig::GeDuration,
                TestCaseCmpOp::LT => ScalarFuncSig::LtDuration,
                TestCaseCmpOp::LE => ScalarFuncSig::LeDuration,
                TestCaseCmpOp::EQ => ScalarFuncSig::EqDuration,
                TestCaseCmpOp::NE => ScalarFuncSig::NeDuration,
                TestCaseCmpOp::NullEQ => ScalarFuncSig::NullEqDuration,
            };
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0.map(map_double_to_duration))
                .push_param(arg1.map(map_double_to_duration))
                .evaluate(sig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}, {:?}, {:?}", arg0, arg1, sig);
        }
    }

    #[test]
    fn test_compare_decimal() {
        use tidb_query_datatype::{codec::convert::ConvertTo, expr::EvalContext};
        fn f64_to_decimal(ctx: &mut EvalContext, f: f64) -> Result<Decimal> {
            let val = f.convert(ctx)?;
            Ok(val)
        }
        let mut ctx = EvalContext::default();
        for (arg0, arg1, cmp_op, expect_output) in generate_numeric_compare_cases() {
            let sig = match cmp_op {
                TestCaseCmpOp::GT => ScalarFuncSig::GtDecimal,
                TestCaseCmpOp::GE => ScalarFuncSig::GeDecimal,
                TestCaseCmpOp::LT => ScalarFuncSig::LtDecimal,
                TestCaseCmpOp::LE => ScalarFuncSig::LeDecimal,
                TestCaseCmpOp::EQ => ScalarFuncSig::EqDecimal,
                TestCaseCmpOp::NE => ScalarFuncSig::NeDecimal,
                TestCaseCmpOp::NullEQ => ScalarFuncSig::NullEqDecimal,
            };
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0.map(|v| f64_to_decimal(&mut ctx, v.into_inner()).unwrap()))
                .push_param(arg1.map(|v| f64_to_decimal(&mut ctx, v.into_inner()).unwrap()))
                .evaluate(sig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}, {:?}, {:?}", arg0, arg1, sig);
        }
    }

    #[test]
    fn test_compare_signed_int() {
        for (arg0, arg1, cmp_op, expect_output) in generate_numeric_compare_cases() {
            let sig = match cmp_op {
                TestCaseCmpOp::GT => ScalarFuncSig::GtInt,
                TestCaseCmpOp::GE => ScalarFuncSig::GeInt,
                TestCaseCmpOp::LT => ScalarFuncSig::LtInt,
                TestCaseCmpOp::LE => ScalarFuncSig::LeInt,
                TestCaseCmpOp::EQ => ScalarFuncSig::EqInt,
                TestCaseCmpOp::NE => ScalarFuncSig::NeInt,
                TestCaseCmpOp::NullEQ => ScalarFuncSig::NullEqInt,
            };
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0.map(|v| v.into_inner() as i64))
                .push_param(arg1.map(|v| v.into_inner() as i64))
                .evaluate(sig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}, {:?}, {:?}", arg0, arg1, sig);
        }
    }

    #[test]
    fn test_compare_int_2() {
        let test_cases = vec![
            (Some(5), false, Some(3), false, Ordering::Greater),
            (Some(u64::MAX as i64), false, Some(5), false, Ordering::Less),
            (
                Some(u64::MAX as i64),
                true,
                Some((u64::MAX - 1) as i64),
                true,
                Ordering::Greater,
            ),
            (
                Some(u64::MAX as i64),
                true,
                Some(5),
                true,
                Ordering::Greater,
            ),
            (Some(5), true, Some(i64::MIN), false, Ordering::Greater),
            (
                Some(u64::MAX as i64),
                true,
                Some(i64::MIN),
                false,
                Ordering::Greater,
            ),
            (Some(5), true, Some(3), false, Ordering::Greater),
            (Some(i64::MIN), false, Some(3), true, Ordering::Less),
            (Some(5), false, Some(u64::MAX as i64), true, Ordering::Less),
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
                (ScalarFuncSig::EqInt, vec![Ordering::Equal]),
                (
                    ScalarFuncSig::NeInt,
                    vec![Ordering::Greater, Ordering::Less],
                ),
                (ScalarFuncSig::GtInt, vec![Ordering::Greater]),
                (
                    ScalarFuncSig::GeInt,
                    vec![Ordering::Greater, Ordering::Equal],
                ),
                (ScalarFuncSig::LtInt, vec![Ordering::Less]),
                (ScalarFuncSig::LeInt, vec![Ordering::Less, Ordering::Equal]),
            ] {
                let output = RpnFnScalarEvaluator::new()
                    .push_param_with_field_type(lhs, lhs_field_type.clone())
                    .push_param_with_field_type(rhs, rhs_field_type.clone())
                    .evaluate(*sig)
                    .unwrap();
                if accept_orderings.iter().any(|&x| x == ordering) {
                    assert_eq!(output, Some(1));
                } else {
                    assert_eq!(output, Some(0));
                }
            }
        }
    }

    #[test]
    fn test_compare_string() {
        fn should_match(ord: Ordering, sig: ScalarFuncSig) -> bool {
            match ord {
                Ordering::Less => {
                    sig == ScalarFuncSig::LtString
                        || sig == ScalarFuncSig::LeString
                        || sig == ScalarFuncSig::NeString
                }
                Ordering::Equal => {
                    sig == ScalarFuncSig::EqString
                        || sig == ScalarFuncSig::LeString
                        || sig == ScalarFuncSig::GeString
                }
                Ordering::Greater => {
                    sig == ScalarFuncSig::GtString
                        || sig == ScalarFuncSig::GeString
                        || sig == ScalarFuncSig::NeString
                }
            }
        }

        let signatures = vec![
            ScalarFuncSig::LtString,
            ScalarFuncSig::LeString,
            ScalarFuncSig::GtString,
            ScalarFuncSig::GeString,
            ScalarFuncSig::EqString,
            ScalarFuncSig::NeString,
        ];
        let cases = vec![
            // strA, strB, [binOrd, utfbin_no_padding, utf8bin, ciOrd]
            (
                "",
                " ",
                [
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Equal,
                    Ordering::Equal,
                    Ordering::Equal,
                ],
            ),
            (
                "a",
                "b",
                [
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                ],
            ),
            (
                "a",
                "A",
                [
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Equal,
                    Ordering::Equal,
                ],
            ),
            (
                "a",
                "A ",
                [
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Equal,
                    Ordering::Equal,
                ],
            ),
            (
                "a",
                "a ",
                [
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Equal,
                    Ordering::Equal,
                    Ordering::Equal,
                ],
            ),
            (
                "À",
                "A",
                [
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Equal,
                    Ordering::Equal,
                ],
            ),
            (
                "À\t",
                "A",
                [
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                ],
            ),
            (
                "abc",
                "ab",
                [
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                ],
            ),
            (
                "a bc",
                "ab ",
                [
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                ],
            ),
            (
                "Abc",
                "abC",
                [
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Equal,
                    Ordering::Equal,
                ],
            ),
            (
                "filé-110",
                "file-12",
                [
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Less,
                    Ordering::Less,
                ],
            ),
            (
                "😜",
                "😃",
                [
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Equal,
                    Ordering::Equal,
                ],
            ),
            (
                "aa",
                "AA۝۝۝۝۝۝۝۝۝",
                [
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Less,
                    Ordering::Equal,
                ],
            ),
        ];
        let collations = [
            (Collation::Binary, 0),
            (Collation::Utf8Mb4BinNoPadding, 1),
            (Collation::Utf8Mb4Bin, 2),
            (Collation::Utf8Mb4GeneralCi, 3),
            (Collation::Utf8Mb4UnicodeCi, 4),
        ];

        for (str_a, str_b, ordering_in_collations) in cases {
            for &sig in &signatures {
                for &(collation, index) in &collations {
                    let result: i64 = RpnFnScalarEvaluator::new()
                        .push_param(str_a.as_bytes().to_vec())
                        .push_param(str_b.as_bytes().to_vec())
                        .return_field_type(
                            FieldTypeBuilder::new()
                                .tp(FieldTypeTp::Long)
                                .collation(collation),
                        )
                        .evaluate(sig)
                        .unwrap()
                        .unwrap();
                    assert_eq!(
                        should_match(ordering_in_collations[index], sig) as i64,
                        result,
                        "Unexpected {:?}({}, {}) == {} in {}",
                        sig,
                        str_a,
                        str_b,
                        result,
                        collation
                    );
                }
            }
        }
    }

    #[test]
    fn test_coalesce() {
        let cases = vec![
            (vec![], None),
            (vec![None], None),
            (vec![None, None], None),
            (vec![None, None, None], None),
            (vec![None, Some(0), None], Some(0)),
        ];
        for (args, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(args)
                .evaluate(ScalarFuncSig::CoalesceInt)
                .unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_greatest_int() {
        let cases = vec![
            (vec![None, None], None),
            (vec![Some(1), Some(1)], Some(1)),
            (vec![Some(1), Some(-1), None], None),
            (vec![Some(-2), Some(-1), Some(1), Some(2)], Some(2)),
            (
                vec![Some(i64::MIN), Some(0), Some(-1), Some(i64::MAX)],
                Some(i64::MAX),
            ),
            (vec![Some(0), Some(4), Some(8), Some(8)], Some(8)),
        ];

        for (row, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(row)
                .evaluate(ScalarFuncSig::GreatestInt)
                .unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_least_int() {
        let cases = vec![
            (vec![None, None], None),
            (vec![Some(1), Some(1)], Some(1)),
            (vec![Some(1), Some(-1), None], None),
            (vec![Some(-2), Some(-1), Some(1), Some(2)], Some(-2)),
            (
                vec![Some(i64::MIN), Some(0), Some(-1), Some(i64::MAX)],
                Some(i64::MIN),
            ),
            (vec![Some(0), Some(4), Some(8), Some(8)], Some(0)),
        ];

        for (row, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(row)
                .evaluate(ScalarFuncSig::LeastInt)
                .unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_interval_int() {
        let cases = vec![
            (vec![Some(1), None], Some(1)),
            (vec![Some(1), Some(-10)], Some(1)),
            (vec![Some(1), Some(2)], Some(0)),
            (vec![Some(1), Some(1), Some(2)], Some(1)),
            (vec![Some(1), Some(0), Some(1), Some(2)], Some(2)),
            (vec![Some(1), Some(0), Some(1), Some(2), Some(5)], Some(2)),
        ];

        for (row, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(row)
                .evaluate(ScalarFuncSig::IntervalInt)
                .unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_interval_real() {
        let cases = vec![
            (vec![Some(1f64), None], Some(1)),
            (vec![Some(1f64), Some(-10f64)], Some(1)),
            (vec![Some(1f64), Some(2f64)], Some(0)),
            (vec![Some(1f64), Some(1f64), Some(2f64)], Some(1)),
            (
                vec![Some(1f64), Some(0f64), Some(1f64), Some(2f64)],
                Some(2),
            ),
            (
                vec![Some(1f64), Some(0f64), Some(1f64), Some(2f64), Some(5f64)],
                Some(2),
            ),
        ];

        for (row, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(row)
                .evaluate(ScalarFuncSig::IntervalReal)
                .unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_greatest_real() {
        let cases = vec![
            (vec![None, None], None),
            (vec![Real::new(1.0).ok(), Real::new(-1.0).ok(), None], None),
            (
                vec![
                    Real::new(1.0).ok(),
                    Real::new(-1.0).ok(),
                    Real::new(-2.0).ok(),
                    Real::new(0f64).ok(),
                ],
                Real::new(1.0).ok(),
            ),
            (
                vec![
                    Real::new(f64::MAX).ok(),
                    Real::new(f64::MIN).ok(),
                    Real::new(0f64).ok(),
                ],
                Real::new(f64::MAX).ok(),
            ),
            (vec![Real::new(f64::NAN).ok(), Real::new(0f64).ok()], None),
            (
                vec![
                    Real::new(f64::INFINITY).ok(),
                    Real::new(f64::NEG_INFINITY).ok(),
                    Real::new(f64::MAX).ok(),
                    Real::new(f64::MIN).ok(),
                ],
                Real::new(f64::INFINITY).ok(),
            ),
        ];

        for (row, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(row)
                .evaluate(ScalarFuncSig::GreatestReal)
                .unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_greatest_string() {
        let cases = vec![
            (vec![None, None], None),
            (
                vec![
                    Some(b"aaa".to_owned().to_vec()),
                    Some(b"bbb".to_owned().to_vec()),
                ],
                Some(b"bbb".to_owned().to_vec()),
            ),
            (vec![Some(b"aaa".to_owned().to_vec()), None], None),
        ];

        for (row, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(row)
                .evaluate(ScalarFuncSig::GreatestString)
                .unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_least_string() {
        let cases = vec![
            (vec![None, None], None),
            (
                vec![
                    Some(b"aaa".to_owned().to_vec()),
                    Some(b"bbb".to_owned().to_vec()),
                ],
                Some(b"aaa".to_owned().to_vec()),
            ),
            (vec![Some(b"aaa".to_owned().to_vec()), None], None),
        ];

        for (row, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(row)
                .evaluate(ScalarFuncSig::LeastString)
                .unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_least_real() {
        let cases = vec![
            (vec![None, None], None),
            (vec![Real::new(1.0).ok(), Real::new(-1.0).ok(), None], None),
            (
                vec![
                    Real::new(1.0).ok(),
                    Real::new(-1.0).ok(),
                    Real::new(-2.0).ok(),
                    Real::new(0f64).ok(),
                ],
                Real::new(-2.0).ok(),
            ),
            (
                vec![
                    Real::new(f64::MAX).ok(),
                    Real::new(f64::MIN).ok(),
                    Real::new(0f64).ok(),
                ],
                Real::new(f64::MIN).ok(),
            ),
            (vec![Real::new(f64::NAN).ok(), Real::new(0f64).ok()], None),
            (
                vec![
                    Real::new(f64::INFINITY).ok(),
                    Real::new(f64::NEG_INFINITY).ok(),
                    Real::new(f64::MAX).ok(),
                    Real::new(f64::MIN).ok(),
                ],
                Real::new(f64::NEG_INFINITY).ok(),
            ),
        ];

        for (row, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(row)
                .evaluate(ScalarFuncSig::LeastReal)
                .unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_greatest_time() {
        let mut ctx = EvalContext::default();
        let cases = vec![
            (vec![None, None], None),
            (
                vec![
                    DateTime::parse_date(&mut ctx, "2012-12-12 12:00:39").ok(),
                    DateTime::parse_date(&mut ctx, "2012-12-24 12:00:39").ok(),
                    None,
                ],
                None,
            ),
            (
                vec![
                    DateTime::parse_date(&mut ctx, "2012-12-12 12:00:39").ok(),
                    DateTime::parse_date(&mut ctx, "2012-12-24 12:00:39").ok(),
                    DateTime::parse_date(&mut ctx, "2012-12-31 12:00:39").ok(),
                ],
                DateTime::parse_date(&mut ctx, "2012-12-31 12:00:39").ok(),
            ),
            (
                vec![
                    DateTime::parse_date(&mut ctx, "2012-12-12 12:00:39").ok(),
                    DateTime::parse_date(&mut ctx, "2013-12-24 12:00:39").ok(),
                    DateTime::parse_date(&mut ctx, "2014-12-31 12:00:39").ok(),
                ],
                DateTime::parse_date(&mut ctx, "2014-12-31 12:00:39").ok(),
            ),
        ];

        for (row, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(row)
                .evaluate(ScalarFuncSig::GreatestTime)
                .unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_least_time() {
        let mut ctx = EvalContext::default();
        let cases = vec![
            (vec![None, None], None),
            (
                vec![
                    DateTime::parse_date(&mut ctx, "2012-12-12 12:00:39").ok(),
                    DateTime::parse_date(&mut ctx, "2012-12-24 12:00:39").ok(),
                    None,
                ],
                None,
            ),
            (
                vec![
                    DateTime::parse_date(&mut ctx, "2012-12-12 12:00:39").ok(),
                    DateTime::parse_date(&mut ctx, "2012-12-24 12:00:39").ok(),
                    DateTime::parse_date(&mut ctx, "2012-12-31 12:00:39").ok(),
                ],
                DateTime::parse_date(&mut ctx, "2012-12-12 12:00:39").ok(),
            ),
            (
                vec![
                    DateTime::parse_date(&mut ctx, "2012-12-12 12:00:39").ok(),
                    DateTime::parse_date(&mut ctx, "2013-12-24 12:00:39").ok(),
                    DateTime::parse_date(&mut ctx, "2014-12-31 12:00:39").ok(),
                ],
                DateTime::parse_date(&mut ctx, "2012-12-12 12:00:39").ok(),
            ),
        ];

        for (row, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(row)
                .evaluate(ScalarFuncSig::LeastTime)
                .unwrap();

            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_greatest_date() {
        let mut ctx = EvalContext::default();
        let cases = vec![
            (vec![None, None], None),
            (
                vec![
                    DateTime::parse_date(&mut ctx, "2012-12-12").ok(),
                    DateTime::parse_date(&mut ctx, "2012-12-24").ok(),
                    None,
                ],
                None,
            ),
            (
                vec![
                    DateTime::parse_date(&mut ctx, "2012-12-12").ok(),
                    DateTime::parse_date(&mut ctx, "2012-12-24").ok(),
                    DateTime::parse_date(&mut ctx, "2012-12-31").ok(),
                ],
                DateTime::parse_date(&mut ctx, "2012-12-31").ok(),
            ),
            (
                vec![
                    DateTime::parse_date(&mut ctx, "2012-12-12").ok(),
                    DateTime::parse_date(&mut ctx, "2013-12-24").ok(),
                    DateTime::parse_date(&mut ctx, "2014-12-31").ok(),
                ],
                DateTime::parse_date(&mut ctx, "2014-12-31").ok(),
            ),
        ];

        for (row, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(row)
                .evaluate(ScalarFuncSig::GreatestDate)
                .unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_least_date() {
        let mut ctx = EvalContext::default();
        let cases = vec![
            (vec![None, None], None),
            (
                vec![
                    DateTime::parse_date(&mut ctx, "2012-12-12").ok(),
                    DateTime::parse_date(&mut ctx, "2012-12-24").ok(),
                    None,
                ],
                None,
            ),
            (
                vec![
                    DateTime::parse_date(&mut ctx, "2012-12-12").ok(),
                    DateTime::parse_date(&mut ctx, "2012-12-24").ok(),
                    DateTime::parse_date(&mut ctx, "2012-12-31").ok(),
                ],
                DateTime::parse_date(&mut ctx, "2012-12-12").ok(),
            ),
            (
                vec![
                    DateTime::parse_date(&mut ctx, "2012-12-12").ok(),
                    DateTime::parse_date(&mut ctx, "2013-12-24").ok(),
                    DateTime::parse_date(&mut ctx, "2014-12-31").ok(),
                ],
                DateTime::parse_date(&mut ctx, "2012-12-12").ok(),
            ),
        ];

        for (row, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(row)
                .evaluate(ScalarFuncSig::LeastDate)
                .unwrap();

            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_greatest_duration() {
        let mut ctx = EvalContext::default();

        let cases = vec![
            (vec![None, None], None),
            (
                vec![
                    Duration::parse(&mut ctx, "123:12:12", 0).ok(),
                    Duration::parse(&mut ctx, "123:22:12", 0).ok(),
                    None,
                ],
                None,
            ),
            (
                vec![
                    Duration::parse(&mut ctx, "123:12:12", 0).ok(),
                    Duration::parse(&mut ctx, "123:22:12", 0).ok(),
                    Duration::parse(&mut ctx, "123:32:12", 0).ok(),
                ],
                Duration::parse(&mut ctx, "123:32:12", 0).ok(),
            ),
        ];

        for (row, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(row)
                .evaluate(ScalarFuncSig::GreatestDuration)
                .unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_least_duration() {
        let mut ctx = EvalContext::default();

        let cases = vec![
            (vec![None, None], None),
            (
                vec![
                    Duration::parse(&mut ctx, "123:12:12", 0).ok(),
                    Duration::parse(&mut ctx, "123:22:12", 0).ok(),
                    None,
                ],
                None,
            ),
            (
                vec![
                    Duration::parse(&mut ctx, "123:12:12", 0).ok(),
                    Duration::parse(&mut ctx, "123:22:12", 0).ok(),
                    Duration::parse(&mut ctx, "123:32:12", 0).ok(),
                ],
                Duration::parse(&mut ctx, "123:12:12", 0).ok(),
            ),
        ];

        for (row, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(row)
                .evaluate(ScalarFuncSig::LeastDuration)
                .unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_greatest_cmp_string_as_time() {
        let cases = vec![
            (vec![None, None], None),
            (
                vec![
                    Some(b"2012-12-12 12:00:39".to_owned().to_vec()),
                    Some(b"2012-12-24 12:00:39".to_owned().to_vec()),
                    None,
                ],
                None,
            ),
            (
                vec![
                    Some(b"2012-12-12 12:00:39".to_owned().to_vec()),
                    Some(b"2012-12-24 12:00:39".to_owned().to_vec()),
                    Some(b"2012-12-31 12:00:39".to_owned().to_vec()),
                ],
                Some(b"2012-12-31 12:00:39".to_owned().to_vec()),
            ),
            (
                vec![
                    Some(b"2012-12-12 12:00:39".to_owned().to_vec()),
                    Some(b"2012-12-24 12:00:39".to_owned().to_vec()),
                    Some(b"2012-12-31 12:00:39".to_owned().to_vec()),
                    Some(b"invalid_time".to_owned().to_vec()),
                ],
                None,
            ),
            (
                vec![
                    Some(b"2012-12-12 12:00:39".to_owned().to_vec()),
                    Some(b"2012-12-24 12:00:39".to_owned().to_vec()),
                    Some(b"2012-12-31 12:00:39".to_owned().to_vec()),
                    Some(b"2012-12-12 12:00:38.12003800000".to_owned().to_vec()),
                    Some(b"2012-12-31 12:00:39.120050".to_owned().to_vec()),
                    Some(b"2018-04-03 00:00:00.000000".to_owned().to_vec()),
                ],
                Some(b"2018-04-03 00:00:00.000000".to_owned().to_vec()),
            ),
            (
                vec![
                    Some(b"2012-12-12 12:00:39".to_owned().to_vec()),
                    Some(vec![0, 159, 146, 150]), // Invalid utf-8 bytes
                ],
                None,
            ),
        ];

        for (row, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(row)
                .evaluate(ScalarFuncSig::GreatestCmpStringAsTime)
                .unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_least_cmp_string_as_time() {
        let cases = vec![
            (vec![None, None], None),
            (
                vec![
                    Some(b"2012-12-12 12:00:39".to_owned().to_vec()),
                    Some(b"2012-12-24 12:00:39".to_owned().to_vec()),
                    None,
                ],
                None,
            ),
            (
                vec![
                    Some(b"2012-12-12 12:00:39".to_owned().to_vec()),
                    Some(b"2012-12-24 12:00:39".to_owned().to_vec()),
                    Some(b"2012-12-31 12:00:39".to_owned().to_vec()),
                ],
                Some(b"2012-12-12 12:00:39".to_owned().to_vec()),
            ),
            (
                vec![
                    Some(b"2012-12-12 12:00:39".to_owned().to_vec()),
                    Some(b"2012-12-24 12:00:39".to_owned().to_vec()),
                    Some(b"2012-12-31 12:00:39".to_owned().to_vec()),
                    Some(b"invalid_time".to_owned().to_vec()),
                ],
                None,
            ),
            (
                vec![
                    Some(b"2012-12-12 12:00:39".to_owned().to_vec()),
                    Some(b"2012-12-24 12:00:39".to_owned().to_vec()),
                    Some(b"2012-12-31 12:00:39".to_owned().to_vec()),
                    Some(b"2012-12-12 12:00:38.12003800000".to_owned().to_vec()),
                    Some(b"2012-12-31 12:00:39.120050".to_owned().to_vec()),
                    Some(b"2018-04-03 00:00:00.000000".to_owned().to_vec()),
                ],
                Some(b"2012-12-12 12:00:38.120038".to_owned().to_vec()),
            ),
            (
                vec![
                    Some(b"2012-12-12 12:00:39".to_owned().to_vec()),
                    Some(vec![0, 159, 146, 150]), // Invalid utf-8 bytes
                ],
                None,
            ),
        ];

        for (row, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(row)
                .evaluate(ScalarFuncSig::LeastCmpStringAsTime)
                .unwrap();

            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_greatest_cmp_string_as_date() {
        let cases = vec![
            (vec![None, None], None),
            (
                vec![
                    Some(b"2012-12-12".to_owned().to_vec()),
                    Some(b"2012-12-24".to_owned().to_vec()),
                    None,
                ],
                None,
            ),
            (
                vec![
                    Some(b"2012-12-12".to_owned().to_vec()),
                    Some(b"2012-12-24".to_owned().to_vec()),
                    Some(b"2012-12-31".to_owned().to_vec()),
                ],
                Some(b"2012-12-31".to_owned().to_vec()),
            ),
            (
                vec![
                    Some(b"2012-12-12".to_owned().to_vec()),
                    Some(b"2012-12-24".to_owned().to_vec()),
                    Some(b"2012-12-31".to_owned().to_vec()),
                    Some(b"invalid_time".to_owned().to_vec()),
                ],
                None,
            ),
            (
                vec![
                    Some(b"2012-12-12".to_owned().to_vec()),
                    Some(b"2012-12-24".to_owned().to_vec()),
                    Some(b"2012-12-31".to_owned().to_vec()),
                    Some(b"2012-12-12".to_owned().to_vec()),
                    Some(b"2012-12-31".to_owned().to_vec()),
                    Some(b"2018-04-03".to_owned().to_vec()),
                ],
                Some(b"2018-04-03".to_owned().to_vec()),
            ),
            (
                vec![
                    Some(b"2012-12-12".to_owned().to_vec()),
                    Some(vec![0, 159, 146, 150]), // Invalid utf-8 bytes
                ],
                None,
            ),
        ];

        for (row, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(row)
                .evaluate(ScalarFuncSig::GreatestCmpStringAsDate)
                .unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_least_cmp_string_as_date() {
        let cases = vec![
            (vec![None, None], None),
            (
                vec![
                    Some(b"2012-12-12".to_owned().to_vec()),
                    Some(b"2012-12-24".to_owned().to_vec()),
                    None,
                ],
                None,
            ),
            (
                vec![
                    Some(b"2012-12-12".to_owned().to_vec()),
                    Some(b"2012-12-24".to_owned().to_vec()),
                    Some(b"2012-12-31".to_owned().to_vec()),
                ],
                Some(b"2012-12-12".to_owned().to_vec()),
            ),
            (
                vec![
                    Some(b"2012-12-12".to_owned().to_vec()),
                    Some(b"2012-12-24".to_owned().to_vec()),
                    Some(b"2012-12-31".to_owned().to_vec()),
                    Some(b"invalid_time".to_owned().to_vec()),
                ],
                None,
            ),
            (
                vec![
                    Some(b"2012-12-12".to_owned().to_vec()),
                    Some(b"2012-12-24".to_owned().to_vec()),
                    Some(b"2012-12-31".to_owned().to_vec()),
                    Some(b"2012-12-12".to_owned().to_vec()),
                    Some(b"2012-12-31".to_owned().to_vec()),
                    Some(b"2018-04-03".to_owned().to_vec()),
                ],
                Some(b"2012-12-12".to_owned().to_vec()),
            ),
            (
                vec![
                    Some(b"2012-12-12".to_owned().to_vec()),
                    Some(vec![0, 159, 146, 150]), // Invalid utf-8 bytes
                ],
                None,
            ),
        ];

        for (row, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(row)
                .evaluate(ScalarFuncSig::LeastCmpStringAsDate)
                .unwrap();

            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_do_get_extrenum() {
        let ints = [Some(1), Some(2), Some(3)];
        let ints_ref = ints.iter().map(|it| it.as_ref()).collect::<Vec<_>>();

        let ints_max = do_get_extremum(&ints_ref, max);
        let ints_min = do_get_extremum(&ints_ref, min);
        assert_eq!(ints_max.unwrap(), Some(3));
        assert_eq!(ints_min.unwrap(), Some(1));

        // If any item in the array is None, result should be none
        let ints_with_none = [Some(1), None, Some(3)];
        let ints_ref = ints_with_none
            .iter()
            .map(|it| it.as_ref())
            .collect::<Vec<_>>();

        let ints_max = do_get_extremum(&ints_ref, max);
        let ints_min = do_get_extremum(&ints_ref, min);
        assert_eq!(ints_max.unwrap(), None);
        assert_eq!(ints_min.unwrap(), None);
    }
}
