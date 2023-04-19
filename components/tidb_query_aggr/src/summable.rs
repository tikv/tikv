// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_common::Result;
use tidb_query_datatype::{codec::data_type::*, expr::EvalContext};

/// A trait for all summable types.
///
/// This trait is used to implement `AVG()` and `SUM()` by using generics.
pub trait Summable: Evaluable + EvaluableRet {
    /// Returns the zero value.
    fn zero() -> Self;

    /// Adds assign another value.
    fn add_assign(&mut self, ctx: &mut EvalContext, other: &Self) -> Result<()>;

    fn add(&self, other: &Self) -> Result<Self>;
    fn sub(&self, other: &Self) -> Result<Self>;
    fn mul(&self, other: &Self) -> Result<Self>;
    fn div(&self, other: &Self) -> Result<Self>;
    fn from_usize(val: usize) -> Result<Self>;
}

impl Summable for Decimal {
    #[inline]
    fn zero() -> Self {
        Decimal::zero()
    }

    #[inline]
    fn add_assign(&mut self, _ctx: &mut EvalContext, other: &Self) -> Result<()> {
        // TODO: If there is truncate error, should it be a warning instead?
        let r: tidb_query_datatype::codec::Result<Decimal> = (self as &Self + other).into();
        *self = r?;
        Ok(())
    }

    fn add(&self, other: &Self) -> Result<Self> {
        let r: tidb_query_datatype::codec::Result<Decimal> = (self as &Self + other).into();
        Ok(r?)
    }
    fn sub(&self, other: &Self) -> Result<Self> {
        let r: tidb_query_datatype::codec::Result<Decimal> = (self as &Self - other).into();
        Ok(r?)
    }
    fn mul(&self, other: &Self) -> Result<Self> {
        let r: tidb_query_datatype::codec::Result<Decimal> = (self as &Self * other).into();
        Ok(r?)
    }
    fn div(&self, other: &Self) -> Result<Self> {
        let r: tidb_query_datatype::codec::Result<Decimal> =
            (self as &Self / other).unwrap().into();
        Ok(r?)
    }
    fn from_usize(val: usize) -> Result<Self> {
        Ok(Decimal::from(val))
    }
}

impl Summable for Real {
    #[inline]
    fn zero() -> Self {
        Real::new(0.0).unwrap()
    }

    #[inline]
    fn add_assign(&mut self, _ctx: &mut EvalContext, other: &Self) -> Result<()> {
        *self += *other;
        Ok(())
    }

    fn add(&self, other: &Self) -> Result<Self> {
        Ok(*self + *other)
    }
    fn sub(&self, other: &Self) -> Result<Self> {
        Ok(*self - *other)
    }
    fn mul(&self, other: &Self) -> Result<Self> {
        Ok(*self * *other)
    }
    fn div(&self, other: &Self) -> Result<Self> {
        Ok(*self / *other)
    }
    fn from_usize(val: usize) -> Result<Self> {
        Ok(Real::new(val as f64).unwrap())
    }
}
