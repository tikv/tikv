use std::cmp::Ordering;
use tipb::expression::{Expr, ExprType};

use util::codec::Datum;
use util::xeval::{evaluator, EvalContext};

use super::Result;


pub fn build_aggr_func(expr: &Expr) -> Result<Box<AggrFunc>> {
    match expr.get_tp() {
        ExprType::Count => Ok(box Count { c: 0 }),
        ExprType::First => Ok(box First { e: None }),
        ExprType::Sum => Ok(box Sum { res: None }),
        ExprType::Avg => {
            Ok(box Avg {
                sum: Sum { res: None },
                cnt: 0,
            })
        }
        ExprType::Max => Ok(box Extremum::new(Ordering::Less)),
        ExprType::Min => Ok(box Extremum::new(Ordering::Greater)),
        et => Err(box_err!("unsupport AggrExprType: {:?}", et)),
    }
}

/// `AggrFunc` is used to execute aggregate operations.
pub trait AggrFunc {
    /// `update` is used for update aggregate context.
    fn update(&mut self, ctx: &EvalContext, args: Vec<Datum>) -> Result<()>;
    /// `calc` calculates the aggregated result and push it to collector.
    fn calc(&mut self, collector: &mut Vec<Datum>) -> Result<()>;
}

struct Count {
    c: u64,
}

impl AggrFunc for Count {
    fn update(&mut self, _: &EvalContext, args: Vec<Datum>) -> Result<()> {
        for arg in args {
            if arg == Datum::Null {
                return Ok(());
            }
        }
        self.c += 1;
        Ok(())
    }

    fn calc(&mut self, collector: &mut Vec<Datum>) -> Result<()> {
        collector.push(Datum::U64(self.c));
        Ok(())
    }
}

struct First {
    e: Option<Datum>,
}

impl AggrFunc for First {
    fn update(&mut self, _: &EvalContext, mut args: Vec<Datum>) -> Result<()> {
        if self.e.is_some() {
            return Ok(());
        }
        if args.len() != 1 {
            return Err(box_err!("Wrong number of args for AggFuncFirstRow: {}", args.len()));
        }
        self.e = args.pop();
        Ok(())
    }

    fn calc(&mut self, collector: &mut Vec<Datum>) -> Result<()> {
        collector.push(self.e.take().unwrap_or(Datum::Null));
        Ok(())
    }
}

struct Sum {
    res: Option<Datum>,
}

impl Sum {
    /// add others to res.
    ///
    /// return false means the others is skipped.
    fn add_asssign(&mut self, ctx: &EvalContext, mut args: Vec<Datum>) -> Result<bool> {
        if args.len() != 1 {
            return Err(box_err!("sum only support one column, but got {}", args.len()));
        }
        let a = args.pop().unwrap();
        if a == Datum::Null {
            return Ok(false);
        }
        let res = match self.res.take() {
            Some(b) => box_try!(evaluator::eval_arith(ctx, a, b, Datum::checked_add)),
            None => a,
        };
        self.res = Some(res);
        Ok(true)
    }
}

impl AggrFunc for Sum {
    fn update(&mut self, ctx: &EvalContext, args: Vec<Datum>) -> Result<()> {
        try!(self.add_asssign(ctx, args));
        Ok(())
    }

    fn calc(&mut self, collector: &mut Vec<Datum>) -> Result<()> {
        let res = self.res.take().unwrap_or(Datum::Null);
        if res == Datum::Null {
            collector.push(res);
            return Ok(());
        }
        let d = box_try!(res.into_dec());
        collector.push(Datum::Dec(d));
        Ok(())
    }
}

struct Avg {
    sum: Sum,
    cnt: u64,
}

impl AggrFunc for Avg {
    fn update(&mut self, ctx: &EvalContext, args: Vec<Datum>) -> Result<()> {
        if try!(self.sum.add_asssign(ctx, args)) {
            self.cnt += 1;
        }
        Ok(())
    }

    fn calc(&mut self, collector: &mut Vec<Datum>) -> Result<()> {
        collector.push(Datum::U64(self.cnt));
        self.sum.calc(collector)
    }
}

struct Extremum {
    datum: Option<Datum>,
    ord: Ordering,
}

impl Extremum {
    fn new(ord: Ordering) -> Extremum {
        Extremum {
            datum: None,
            ord: ord,
        }
    }
}

impl AggrFunc for Extremum {
    fn update(&mut self, ctx: &EvalContext, mut args: Vec<Datum>) -> Result<()> {
        if args.len() != 1 {
            return Err(box_err!("max/min only support one column, but got {}", args.len()));
        }
        if args[0] == Datum::Null {
            return Ok(());
        }
        if let Some(ref d) = self.datum {
            if box_try!(d.cmp(ctx, &args[0])) != self.ord {
                return Ok(());
            }
        }
        self.datum = args.pop();
        Ok(())
    }

    fn calc(&mut self, collector: &mut Vec<Datum>) -> Result<()> {
        collector.push(self.datum.take().unwrap_or(Datum::Null));
        Ok(())
    }
}
