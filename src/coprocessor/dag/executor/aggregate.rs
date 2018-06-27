// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::Ordering;
use tipb::expression::ExprType;

use coprocessor::codec::mysql::Decimal;
use coprocessor::codec::Datum;
use coprocessor::Result;

use super::super::expr::{eval_arith, EvalContext};

pub fn build_aggr_func(tp: ExprType) -> Result<Box<AggrFunc>> {
    match tp {
        ExprType::Agg_BitAnd => Ok(box AggBitAnd {
            c: 0xffffffffffffffff,
        }),
        ExprType::Agg_BitOr => Ok(box AggBitOr { c: 0 }),
        ExprType::Agg_BitXor => Ok(box AggBitXor { c: 0 }),
        ExprType::Count => Ok(box Count { c: 0 }),
        ExprType::First => Ok(box First { e: None }),
        ExprType::Sum => Ok(box Sum { res: None }),
        ExprType::Avg => Ok(box Avg {
            sum: Sum { res: None },
            cnt: 0,
        }),
        ExprType::Max => Ok(box Extremum::new(Ordering::Less)),
        ExprType::Min => Ok(box Extremum::new(Ordering::Greater)),
        et => Err(box_err!("unsupport AggrExprType: {:?}", et)),
    }
}

/// `AggrFunc` is used to execute aggregate operations.
pub trait AggrFunc: Send {
    /// `update` is used for update aggregate context.
    fn update(&mut self, ctx: &mut EvalContext, args: Vec<Datum>) -> Result<()>;
    /// `calc` calculates the aggregated result and push it to collector.
    fn calc(&mut self, collector: &mut Vec<Datum>) -> Result<()>;
}

struct AggBitAnd {
    c: u64,
}

impl AggrFunc for AggBitAnd {
    fn update(&mut self, _: &mut EvalContext, args: Vec<Datum>) -> Result<()> {
        if args.len() != 1 {
            return Err(box_err!(
                "bit_and only support one column, but got {}",
                args.len()
            ));
        }
        if args[0] == Datum::Null {
            return Ok(());
        }
        self.c &= args[0].u64();
        Ok(())
    }

    fn calc(&mut self, collector: &mut Vec<Datum>) -> Result<()> {
        collector.push(Datum::U64(self.c));
        Ok(())
    }
}

struct AggBitOr {
    c: u64,
}

impl AggrFunc for AggBitOr {
    fn update(&mut self, _: &mut EvalContext, args: Vec<Datum>) -> Result<()> {
        if args.len() != 1 {
            return Err(box_err!(
                "bit_or only support one column, but got {}",
                args.len()
            ));
        }
        if args[0] == Datum::Null {
            return Ok(());
        }
        self.c |= args[0].u64();
        Ok(())
    }

    fn calc(&mut self, collector: &mut Vec<Datum>) -> Result<()> {
        collector.push(Datum::U64(self.c));
        Ok(())
    }
}

struct AggBitXor {
    c: u64,
}

impl AggrFunc for AggBitXor {
    fn update(&mut self, _: &mut EvalContext, args: Vec<Datum>) -> Result<()> {
        if args.len() != 1 {
            return Err(box_err!(
                "bit_xor only support one column, but got {}",
                args.len()
            ));
        }
        if args[0] == Datum::Null {
            return Ok(());
        }
        self.c ^= args[0].u64();
        Ok(())
    }

    fn calc(&mut self, collector: &mut Vec<Datum>) -> Result<()> {
        collector.push(Datum::U64(self.c));
        Ok(())
    }
}

struct Count {
    c: u64,
}

impl AggrFunc for Count {
    fn update(&mut self, _: &mut EvalContext, args: Vec<Datum>) -> Result<()> {
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
    fn update(&mut self, _: &mut EvalContext, mut args: Vec<Datum>) -> Result<()> {
        if self.e.is_some() {
            return Ok(());
        }
        if args.len() != 1 {
            return Err(box_err!(
                "Wrong number of args for AggFuncFirstRow: {}",
                args.len()
            ));
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
    fn add_asssign(&mut self, ctx: &mut EvalContext, mut args: Vec<Datum>) -> Result<bool> {
        if args.len() != 1 {
            return Err(box_err!(
                "sum only support one column, but got {}",
                args.len()
            ));
        }
        let a = args.pop().unwrap();
        if a == Datum::Null {
            return Ok(false);
        }

        let v = match a {
            Datum::I64(v) => Datum::Dec(Decimal::from(v)),
            Datum::U64(v) => Datum::Dec(Decimal::from(v)),
            v => v,
        };
        let res = match self.res.take() {
            Some(b) => eval_arith(ctx, v, b, Datum::checked_add)?,
            None => v,
        };
        self.res = Some(res);
        Ok(true)
    }
}

impl AggrFunc for Sum {
    fn update(&mut self, ctx: &mut EvalContext, args: Vec<Datum>) -> Result<()> {
        self.add_asssign(ctx, args)?;
        Ok(())
    }

    fn calc(&mut self, collector: &mut Vec<Datum>) -> Result<()> {
        let res = self.res.take().unwrap_or(Datum::Null);
        match res {
            Datum::Null | Datum::F64(_) => collector.push(res),
            _ => {
                let d = res.into_dec()?;
                collector.push(Datum::Dec(d));
            }
        }
        Ok(())
    }
}

struct Avg {
    sum: Sum,
    cnt: u64,
}

impl AggrFunc for Avg {
    fn update(&mut self, ctx: &mut EvalContext, args: Vec<Datum>) -> Result<()> {
        if self.sum.add_asssign(ctx, args)? {
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
        Extremum { datum: None, ord }
    }
}

impl AggrFunc for Extremum {
    fn update(&mut self, ctx: &mut EvalContext, mut args: Vec<Datum>) -> Result<()> {
        if args.len() != 1 {
            return Err(box_err!(
                "max/min only support one column, but got {}",
                args.len()
            ));
        }
        if args[0] == Datum::Null {
            return Ok(());
        }
        if let Some(ref d) = self.datum {
            if d.cmp(ctx, &args[0])? != self.ord {
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

#[cfg(test)]
mod test {
    use coprocessor::dag::expr::EvalContext;
    use std::ops::Add;
    use std::{i64, u64};

    use super::*;

    #[test]
    fn test_sum_int() {
        let mut sum = Sum { res: None };
        let mut ctx = EvalContext::default();
        let v1 = Datum::I64(i64::MAX);
        let v2 = Datum::I64(12);
        let res = Decimal::from(i64::MAX).add(&Decimal::from(12)).unwrap();
        sum.update(&mut ctx, vec![v1]).unwrap();
        sum.update(&mut ctx, vec![v2]).unwrap();
        let v = sum.res.take().unwrap();
        assert_eq!(v, Datum::Dec(res));
    }

    #[test]
    fn test_sum_uint() {
        let mut sum = Sum { res: None };
        let mut ctx = EvalContext::default();
        let v1 = Datum::U64(u64::MAX);
        let v2 = Datum::U64(12);
        let res = Decimal::from(u64::MAX).add(&Decimal::from(12)).unwrap();
        sum.update(&mut ctx, vec![v1]).unwrap();
        sum.update(&mut ctx, vec![v2]).unwrap();
        let v = sum.res.take().unwrap();
        assert_eq!(v, Datum::Dec(res));
    }
}
