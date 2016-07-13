use tipb::expression::{Expr, ExprType};

use util::codec::Datum;
use util::xeval::evaluator;

use super::Result;


pub fn build_aggr_func(expr: &Expr) -> Result<Box<AggrFunc>> {
    match expr.get_tp() {
        ExprType::Count => Ok(box 0),
        ExprType::First => Ok(box None),
        ExprType::Sum => Ok(box Sum { res: None }),
        et => Err(box_err!("unsupport AggrExprType: {:?}", et)),
    }
}

/// `AggrFunc` is used to execute aggregate operations.
pub trait AggrFunc {
    /// `update` is used for update aggregate context.
    fn update(&mut self, args: Vec<Datum>) -> Result<()>;
    /// `calc` calculates the aggregated result and push it to collector.
    fn calc(&mut self, collector: &mut Vec<Datum>) -> Result<()>;
}

type Count = u64;

impl AggrFunc for Count {
    fn update(&mut self, args: Vec<Datum>) -> Result<()> {
        for arg in args {
            if arg == Datum::Null {
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

type First = Option<Datum>;

impl AggrFunc for First {
    fn update(&mut self, mut args: Vec<Datum>) -> Result<()> {
        if self.is_some() {
            return Ok(());
        }
        if args.len() != 1 {
            return Err(box_err!("Wrong number of args for AggFuncFirstRow: {}", args.len()));
        }
        *self = args.pop();
        Ok(())
    }

    fn calc(&mut self, collector: &mut Vec<Datum>) -> Result<()> {
        collector.push(self.take().unwrap());
        Ok(())
    }
}

struct Sum {
    res: Option<Datum>,
}

impl AggrFunc for Sum {
    fn update(&mut self, mut args: Vec<Datum>) -> Result<()> {
        if args.len() != 1 {
            return Err(box_err!("sum only support one column, but got {}", args.len()));
        }
        let a = args.pop().unwrap();
        if a == Datum::Null {
            return Ok(());
        }
        let res = match self.res.take() {
            Some(b) => box_try!(evaluator::eval_arith(a, b, Datum::checked_add)),
            None => a,
        };
        self.res = Some(res);
        Ok(())
    }

    fn calc(&mut self, collector: &mut Vec<Datum>) -> Result<()> {
        let res = self.res.take().unwrap();
        if res == Datum::Null {
            collector.push(res);
            return Ok(());
        }
        let d = box_try!(res.into_dec());
        collector.push(Datum::Dec(d));
        Ok(())
    }
}
