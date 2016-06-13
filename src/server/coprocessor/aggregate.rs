use tipb::expression::{Expr, ExprType};

use util::codec::Datum;

use super::Result;


pub fn build_aggr_func(expr: &Expr) -> Result<Box<AggrFunc>> {
    match expr.get_tp() {
        ExprType::Count => Ok(box 0),
        et => Err(box_err!("unsupport AggrExprType: {:?}", et)),
    }
}

/// `AggrFunc` is used to execute aggregate operations.
pub trait AggrFunc {
    /// `update` is used for update aggregate context.
    fn update(&mut self, args: &[Datum]) -> Result<()>;
    /// `calc` calculates the aggregated result and push it to collector.
    fn calc(&mut self, collector: &mut Vec<Datum>) -> Result<()>;
}

type Count = u64;

impl AggrFunc for Count {
    fn update(&mut self, args: &[Datum]) -> Result<()> {
        for arg in args {
            if *arg == Datum::Null {
                return Ok(());
            }
        }
        *self += 1;
        Ok(())
    }

    fn calc(&mut self, collector: &mut Vec<Datum>) -> Result<()> {
        collector.push(Datum::U64(*self));
        Ok(())
    }
}
