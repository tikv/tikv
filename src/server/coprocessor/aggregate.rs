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
use std::rc::Rc;
use std::collections::HashMap;
use std::collections::hash_map::Entry;

use tipb::expression::{Expr, ExprType};
use tipb::select::{SelectRequest, Chunk, ByItem, RowMeta};
use tipb::schema::ColumnInfo;

use util::codec::{datum, Datum};
use util::xeval::{evaluator, Evaluator, EvalContext};

use super::{Result, util, Collector};

pub const SINGLE_GROUP: &'static [u8] = b"SingleGroup";

/// A collector that handles all the aggregation.
pub struct AggrCollector {
    aggr_cols: Vec<ColumnInfo>,
    aggregates: Vec<Expr>,
    group_by: Vec<ByItem>,
    gks: Vec<Rc<Vec<u8>>>,
    gk_aggrs: HashMap<Rc<Vec<u8>>, Vec<Box<AggrFunc>>>,
}

impl AggrCollector {
    fn get_group_key(&mut self, eval: &mut Evaluator) -> Result<Vec<u8>> {
        if self.group_by.is_empty() {
            return Ok(SINGLE_GROUP.to_vec());
        }
        let mut vals = Vec::with_capacity(self.group_by.len());
        for item in &self.group_by {
            let v = box_try!(eval.eval(item.get_expr()));
            vals.push(v);
        }
        let res = box_try!(datum::encode_value(&vals));
        Ok(res)
    }
}

impl Collector for AggrCollector {
    fn create(sel: &SelectRequest) -> Result<AggrCollector> {
        let aggr_cols;

        {
            let select_cols = if sel.has_table_info() {
                sel.get_table_info().get_columns()
            } else {
                sel.get_index_info().get_columns()
            };
            let mut cond_col_map = HashMap::new();
            try!(util::collect_col_in_expr(&mut cond_col_map, select_cols, sel.get_field_where()));
            let mut aggr_cols_map = HashMap::new();
            for aggr in sel.get_aggregates() {
                try!(util::collect_col_in_expr(&mut aggr_cols_map, select_cols, aggr));
            }
            for item in sel.get_group_by() {
                try!(util::collect_col_in_expr(&mut aggr_cols_map, select_cols, item.get_expr()));
            }
            if !aggr_cols_map.is_empty() {
                for cond_col in cond_col_map.keys() {
                    aggr_cols_map.remove(cond_col);
                }
            }
            aggr_cols = aggr_cols_map.drain().map(|(_, v)| v).collect();
        }

        Ok(AggrCollector {
            aggr_cols: aggr_cols,
            aggregates: sel.get_aggregates().to_vec(),
            group_by: sel.get_group_by().to_vec(),
            gks: vec![],
            gk_aggrs: map![],
        })
    }

    fn collect(&mut self,
               eval: &mut Evaluator,
               handle: i64,
               values: &HashMap<i64, &[u8]>)
               -> Result<usize> {
        try!(util::inflate_with_col(eval, values, &self.aggr_cols, handle));
        let gk = Rc::new(try!(self.get_group_key(eval)));
        match self.gk_aggrs.entry(gk.clone()) {
            Entry::Occupied(e) => {
                let funcs = e.into_mut();
                for (expr, func) in self.aggregates.iter().zip(funcs) {
                    // TODO: cache args
                    let args = box_try!(eval.batch_eval(expr.get_children()));
                    try!(func.update(&eval.ctx, args));
                }
            }
            Entry::Vacant(e) => {
                let mut aggrs = Vec::with_capacity(self.aggregates.len());
                for expr in &self.aggregates {
                    let mut aggr = try!(build_aggr_func(expr));
                    let args = box_try!(eval.batch_eval(expr.get_children()));
                    try!(aggr.update(&eval.ctx, args));
                    aggrs.push(aggr);
                }
                self.gks.push(gk);
                e.insert(aggrs);
            }
        }
        Ok(0)
    }

    /// Convert aggregate partial result to rows.
    /// Data layout example:
    /// SQL: select count(c1), sum(c2), avg(c3) from t;
    /// Aggs: count(c1), sum(c2), avg(c3)
    /// Rows: groupKey1, count1, value2, count3, value3
    ///       groupKey2, count1, value2, count3, value3
    fn take_collection(&mut self) -> Result<Vec<Chunk>> {
        let mut chunks = Vec::with_capacity((self.gk_aggrs.len() + util::BATCH_ROW_COUNT - 1) /
                                            util::BATCH_ROW_COUNT);
        // Each aggregate partial result will be converted to two datum.
        let mut row_data = Vec::with_capacity(1 + 2 * self.aggregates.len());
        for gk in self.gks.drain(..) {
            let aggrs = self.gk_aggrs.remove(&gk).unwrap();

            let chunk = util::get_chunk(&mut chunks);
            // The first column is group key.
            row_data.push(Datum::Bytes(Rc::try_unwrap(gk).unwrap()));
            for mut aggr in aggrs {
                try!(aggr.calc(&mut row_data));
            }
            let last_len = chunk.get_rows_data().len();
            box_try!(datum::encode_to(chunk.mut_rows_data(), &row_data, false));
            let mut meta = RowMeta::new();
            meta.set_length((chunk.get_rows_data().len() - last_len) as i64);
            chunk.mut_rows_meta().push(meta);
            row_data.clear();
        }
        Ok(chunks)
    }
}

pub fn build_aggr_func(expr: &Expr) -> Result<Box<AggrFunc>> {
    match expr.get_tp() {
        ExprType::Count => Ok(box 0),
        ExprType::First => Ok(box None),
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

type Count = u64;

impl AggrFunc for Count {
    fn update(&mut self, _: &EvalContext, args: Vec<Datum>) -> Result<()> {
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
    fn update(&mut self, _: &EvalContext, mut args: Vec<Datum>) -> Result<()> {
        if self.is_some() {
            return Ok(());
        }
        if args.len() != 1 {
            return Err(box_err!("Wrong number of args for AggFuncFirstRow: {}", args.len()));
        }
        if args[0] == Datum::Null {
            return Ok(());
        }
        *self = args.pop();
        Ok(())
    }

    fn calc(&mut self, collector: &mut Vec<Datum>) -> Result<()> {
        collector.push(self.take().unwrap_or(Datum::Null));
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
    fn add_asssign(&mut self, mut args: Vec<Datum>) -> Result<bool> {
        if args.len() != 1 {
            return Err(box_err!("sum only support one column, but got {}", args.len()));
        }
        let a = args.pop().unwrap();
        if a == Datum::Null {
            return Ok(false);
        }
        let res = match self.res.take() {
            Some(b) => box_try!(evaluator::eval_arith(a, b, Datum::checked_add)),
            None => a,
        };
        self.res = Some(res);
        Ok(true)
    }
}

impl AggrFunc for Sum {
    fn update(&mut self, _: &EvalContext, args: Vec<Datum>) -> Result<()> {
        try!(self.add_asssign(args));
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
    fn update(&mut self, _: &EvalContext, args: Vec<Datum>) -> Result<()> {
        if try!(self.sum.add_asssign(args)) {
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
