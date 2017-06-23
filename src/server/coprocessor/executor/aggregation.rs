// Copyright 2017 PingCAP, Inc.
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

// FIXME: remove later
#![allow(dead_code)]

use util::collections::{HashMap, HashMapEntry as Entry};

use super::{Executor, Row};
use super::topn::ExprColumnRefVisitor;
use super::super::Result;
use super::super::endpoint::{inflate_with_col, SINGLE_GROUP};
use super::super::aggregate::{self, AggrFunc};

use tipb::schema::ColumnInfo;
use tipb::executor::Aggregation;
use tipb::expression::Expr;
use util::codec::datum::{self, DatumEncoder, approximate_size};
use util::codec::table::{RowColMeta, RowColsDict};
use util::xeval::{Evaluator, EvalContext};


struct AggregationExecutor<'a> {
    group_by: Vec<Expr>,
    aggr_func: Vec<Expr>,
    gks: Vec<Vec<u8>>,
    gk_aggrs: HashMap<Vec<u8>, Vec<Box<AggrFunc>>>,
    cursor: usize,
    executed: bool,
    ctx: EvalContext,
    cols: Vec<ColumnInfo>,
    src: &'a mut Executor,
}

impl<'a> AggregationExecutor<'a> {
    fn new(mut meta: Aggregation,
           ctx: EvalContext,
           columns: &[ColumnInfo],
           src: &'a mut Executor)
           -> Result<AggregationExecutor<'a>> {
        // collect all cols used in aggregation
        let mut visitor = ExprColumnRefVisitor::new();
        let group_by = meta.take_group_by().into_vec();
        try!(visitor.batch_visit(&group_by));
        let aggr_func = meta.take_agg_func().into_vec();
        try!(visitor.batch_visit(&aggr_func));
        // filter from all cols
        let cols = columns.iter()
            .filter(|col| visitor.col_ids.contains(&col.get_column_id()))
            .cloned()
            .collect();

        Ok(AggregationExecutor {
            group_by: group_by,
            aggr_func: aggr_func,
            gks: vec![],
            gk_aggrs: map![],
            cursor: 0,
            executed: false,
            ctx: ctx,
            cols: cols,
            src: src,
        })
    }

    fn get_group_key(&mut self, eval: &mut Evaluator) -> Result<Vec<u8>> {
        if self.group_by.is_empty() {
            return Ok(SINGLE_GROUP.to_vec());
        }
        let mut vals = Vec::with_capacity(self.group_by.len());
        for expr in &self.group_by {
            let v = box_try!(eval.eval(&self.ctx, expr));
            vals.push(v);
        }
        let res = box_try!(datum::encode_value(&vals));
        Ok(res)
    }

    fn aggregate(&mut self) -> Result<()> {
        while let Some(row) = try!(self.src.next()) {
            let mut eval = Evaluator::default();
            try!(inflate_with_col(&mut eval, &self.ctx, &row.data, &self.cols, row.handle));
            let gk = try!(self.get_group_key(&mut eval));
            match self.gk_aggrs.entry(gk.clone()) {
                Entry::Vacant(e) => {
                    let mut aggrs = Vec::with_capacity(self.aggr_func.len());
                    for expr in &self.aggr_func {
                        let mut aggr = try!(aggregate::build_aggr_func(expr));
                        let vals = box_try!(eval.batch_eval(&self.ctx, expr.get_children()));
                        try!(aggr.update(&self.ctx, vals));
                        aggrs.push(aggr);
                    }
                    self.gks.push(gk);
                    e.insert(aggrs);
                }
                Entry::Occupied(e) => {
                    let aggrs = e.into_mut();
                    for (expr, aggr) in self.aggr_func.iter().zip(aggrs) {
                        let vals = box_try!(eval.batch_eval(&self.ctx, expr.get_children()));
                        box_try!(aggr.update(&self.ctx, vals));
                    }
                }
            }
        }
        Ok(())
    }
}


impl<'a> Executor for AggregationExecutor<'a> {
    fn next(&mut self) -> Result<Option<Row>> {
        if !self.executed {
            try!(self.aggregate());
            self.executed = true;
            assert_eq!(self.gks.len(), self.gk_aggrs.len());
        }

        if self.cursor >= self.gks.len() {
            return Ok(None);
        }
        // calc all aggr func
        let mut aggr_cols = Vec::with_capacity(2 * self.aggr_func.len());
        let gk = &self.gks[self.cursor];
        let mut aggrs = self.gk_aggrs.remove(gk).unwrap();
        for aggr in &mut aggrs {
            try!(aggr.calc(&mut aggr_cols));
        }
        // construct row data
        let value_size = gk.len() + approximate_size(&aggr_cols, false);
        let mut value = Vec::with_capacity(value_size);
        let mut meta = HashMap::with_capacity(1 + 2 * aggr_cols.len());
        let (mut id, mut offset) = (0, 0);
        value.extend_from_slice(gk);
        meta.insert(id, RowColMeta::new(offset, (value.len() - offset)));
        id = id + 1;
        offset = value.len();
        for i in 0..aggr_cols.len() {
            box_try!(value.encode(&aggr_cols[i..i + 1], false));
            meta.insert(id, RowColMeta::new(offset, (value.len() - offset)));
            id = id + 1;
            offset = value.len();
        }
        self.cursor += 1;
        Ok(Some(Row {
            handle: 0,
            data: RowColsDict::new(meta, value),
        }))
    }
}
