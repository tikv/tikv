use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::Result;

pub trait Summable: Evaluable {
    fn zero() -> Self;

    fn add_assign(&mut self, ctx: &mut EvalContext, other: &Self) -> Result<()>;
}

impl Summable for Decimal {
    #[inline]
    fn zero() -> Self {
        Decimal::zero()
    }

    #[inline]
    fn add_assign(&mut self, _ctx: &mut EvalContext, other: &Self) -> Result<()> {
        let r: crate::coprocessor::codec::Result<Decimal> = (self as &Self + other).into();
        *self = r?;
        Ok(())
        // TODO: If there is truncate error, should it be a warning instead?
    }
}

impl Summable for Real {
    #[inline]
    fn zero() -> Self {
        0.0
    }

    #[inline]
    fn add_assign(&mut self, _ctx: &mut EvalContext, other: &Self) -> Result<()> {
        *self += other;
        Ok(())
    }
}
