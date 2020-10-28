// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;
use tipb::ExprType;

use tidb_query_common::Result;
use tidb_query_datatype::codec::mysql::Decimal;
use tidb_query_datatype::codec::Datum;

use tidb_query_datatype::expr::EvalContext;
use tidb_query_normal_expr::eval_arith;

pub fn build_aggr_func(tp: ExprType) -> Result<Box<dyn AggrFunc>> {
    match tp {
        ExprType::AggBitAnd => Ok(Box::new(AggBitAnd {
            c: 0xffffffffffffffff,
        })),
        ExprType::AggBitOr => Ok(Box::new(AggBitOr { c: 0 })),
        ExprType::AggBitXor => Ok(Box::new(AggBitXor { c: 0 })),
        ExprType::Count => Ok(Box::new(Count { c: 0 })),
        ExprType::First => Ok(Box::new(First { e: None })),
        ExprType::Sum => Ok(Box::new(Sum { res: None })),
        ExprType::Avg => Ok(Box::new(Avg {
            sum: Sum { res: None },
            cnt: 0,
        })),
        ExprType::Max => Ok(Box::new(Extremum::new(Ordering::Less))),
        ExprType::Min => Ok(Box::new(Extremum::new(Ordering::Greater))),
        et => Err(other_err!("unsupport AggrExprType: {:?}", et)),
    }
}

/// `AggrFunc` is used to execute aggregate operations.
pub trait AggrFunc: Send {
    /// `update` is used for update aggregate context.
    fn update(&mut self, ctx: &mut EvalContext, args: &mut Vec<Datum>) -> Result<()>;
    /// `calc` calculates the aggregated result and push it to collector.
    fn calc(&mut self, collector: &mut Vec<Datum>) -> Result<()>;
}

struct AggBitAnd {
    c: u64,
}

impl AggrFunc for AggBitAnd {
    fn update(&mut self, ctx: &mut EvalContext, args: &mut Vec<Datum>) -> Result<()> {
        if args.len() != 1 {
            return Err(other_err!(
                "bit_and only support one column, but got {}",
                args.len()
            ));
        }
        if args[0] == Datum::Null {
            return Ok(());
        }
        let val = if let Datum::U64(v) = args[0] {
            v
        } else {
            args.pop().unwrap().into_i64(ctx)? as u64
        };
        self.c &= val;
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
    fn update(&mut self, ctx: &mut EvalContext, args: &mut Vec<Datum>) -> Result<()> {
        if args.len() != 1 {
            return Err(other_err!(
                "bit_or only support one column, but got {}",
                args.len()
            ));
        }
        if args[0] == Datum::Null {
            return Ok(());
        }
        let val = if let Datum::U64(v) = args[0] {
            v
        } else {
            args.pop().unwrap().into_i64(ctx)? as u64
        };
        self.c |= val;
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
    fn update(&mut self, ctx: &mut EvalContext, args: &mut Vec<Datum>) -> Result<()> {
        if args.len() != 1 {
            return Err(other_err!(
                "bit_xor only support one column, but got {}",
                args.len()
            ));
        }
        if args[0] == Datum::Null {
            return Ok(());
        }
        let val = if let Datum::U64(v) = args[0] {
            v
        } else {
            args.pop().unwrap().into_i64(ctx)? as u64
        };
        self.c ^= val;
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
    fn update(&mut self, _: &mut EvalContext, args: &mut Vec<Datum>) -> Result<()> {
        for arg in args {
            if *arg == Datum::Null {
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
    fn update(&mut self, _: &mut EvalContext, args: &mut Vec<Datum>) -> Result<()> {
        if self.e.is_some() {
            return Ok(());
        }
        if args.len() != 1 {
            return Err(other_err!(
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
    /// Keep compatible with TiDB's `calculateSum` function.
    fn add_asssign(&mut self, ctx: &mut EvalContext, args: &mut Vec<Datum>) -> Result<bool> {
        if args.len() != 1 {
            return Err(other_err!(
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
            Datum::Dec(d) => Datum::Dec(d),
            v => {
                let f = v.into_f64(ctx)?;
                Datum::F64(f)
            }
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
    fn update(&mut self, ctx: &mut EvalContext, args: &mut Vec<Datum>) -> Result<()> {
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
    fn update(&mut self, ctx: &mut EvalContext, args: &mut Vec<Datum>) -> Result<()> {
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
    fn update(&mut self, ctx: &mut EvalContext, args: &mut Vec<Datum>) -> Result<()> {
        if args.len() != 1 {
            return Err(other_err!(
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
mod tests {
    use std::ops::Add;
    use std::sync::Arc;
    use std::{i64, u64};
    use tidb_query_datatype::expr::{EvalConfig, EvalContext};

    use super::*;

    #[test]
    fn test_sum_int() {
        let mut sum = Sum { res: None };
        let mut ctx = EvalContext::default();
        let v1 = Datum::I64(i64::MAX);
        let v2 = Datum::I64(12);
        let res = Decimal::from(i64::MAX).add(&Decimal::from(12)).unwrap();
        sum.update(&mut ctx, &mut vec![v1]).unwrap();
        sum.update(&mut ctx, &mut vec![v2]).unwrap();
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
        sum.update(&mut ctx, &mut vec![v1]).unwrap();
        sum.update(&mut ctx, &mut vec![v2]).unwrap();
        let v = sum.res.take().unwrap();
        assert_eq!(v, Datum::Dec(res));
    }

    #[test]
    fn test_sum_as_f64() {
        let mut sum = Sum { res: None };
        let cfg = EvalConfig::default_for_test();
        let mut ctx = EvalContext::new(Arc::new(cfg));
        let data = vec![
            Datum::Bytes(b"123.09xxx".to_vec()),
            Datum::Bytes(b"aaa".to_vec()),
            Datum::Null,
            Datum::F64(12.1),
        ];
        let res = 123.09 + 12.1;
        for v in data {
            sum.update(&mut ctx, &mut vec![v]).unwrap();
        }
        let v = sum.res.take().unwrap();
        assert_eq!(v, Datum::F64(res));
    }

    fn f64_to_decimal(ctx: &mut EvalContext, f: f64) -> Result<Decimal> {
        use tidb_query_datatype::codec::convert::ConvertTo;
        let val = f.convert(ctx)?;
        Ok(val)
    }

    #[test]
    fn test_bit_and() {
        let mut aggr = AggBitAnd {
            c: 0xffffffffffffffff,
        };
        let cfg = EvalConfig::default_for_test();
        let mut ctx = EvalContext::new(Arc::new(cfg));
        assert_eq!(aggr.c, u64::MAX);

        let data = vec![
            Datum::U64(1),
            Datum::Null,
            Datum::U64(1),
            Datum::I64(3),
            Datum::I64(2),
            Datum::Dec(f64_to_decimal(&mut ctx, 1.234).unwrap()),
            Datum::Dec(f64_to_decimal(&mut ctx, 3.012).unwrap()),
            Datum::Dec(f64_to_decimal(&mut ctx, 2.12345678).unwrap()),
        ];

        for v in data {
            aggr.update(&mut ctx, &mut vec![v]).unwrap();
        }
        assert_eq!(aggr.c, 0);
    }

    #[test]
    fn test_bit_or() {
        let mut aggr = AggBitOr { c: 0 };
        let cfg = EvalConfig::default_for_test();
        let mut ctx = EvalContext::new(Arc::new(cfg));
        let data = vec![
            Datum::U64(1),
            Datum::Null,
            Datum::U64(1),
            Datum::I64(3),
            Datum::I64(2),
            Datum::Dec(f64_to_decimal(&mut ctx, 12.34).unwrap()),
            Datum::Dec(f64_to_decimal(&mut ctx, 1.012).unwrap()),
            Datum::Dec(f64_to_decimal(&mut ctx, 15.12345678).unwrap()),
            Datum::Dec(f64_to_decimal(&mut ctx, 16.000).unwrap()),
        ];

        for v in data {
            aggr.update(&mut ctx, &mut vec![v]).unwrap();
        }
        assert_eq!(aggr.c, 31);
    }

    #[test]
    fn test_bit_xor() {
        let mut aggr = AggBitXor { c: 0 };
        let cfg = EvalConfig::default_for_test();
        let mut ctx = EvalContext::new(Arc::new(cfg));

        let data = vec![
            Datum::U64(1),
            Datum::Null,
            Datum::U64(1),
            Datum::I64(3),
            Datum::I64(2),
            Datum::Dec(f64_to_decimal(&mut ctx, 1.234).unwrap()),
            Datum::Dec(f64_to_decimal(&mut ctx, 1.012).unwrap()),
            Datum::Dec(f64_to_decimal(&mut ctx, 2.12345678).unwrap()),
        ];

        for v in data {
            aggr.update(&mut ctx, &mut vec![v]).unwrap();
        }
        assert_eq!(aggr.c, 3);
    }
}
