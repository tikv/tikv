// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;
use std::mem;
use std::sync::Arc;

use tipb::executor::Aggregation;
use tipb::expression::{Expr, ExprType};

use tikv_util::collections::{OrderMap, OrderMapEntry};

use crate::codec::datum::{self, Datum};
use crate::expr::{EvalConfig, EvalContext, EvalWarnings, Expression};
use crate::*;

use super::aggregate::{self, AggrFunc};
use super::ExecutorMetrics;
use super::{Executor, ExprColumnRefVisitor, Row};

struct AggFuncExpr {
    args: Vec<Expression>,
    tp: ExprType,
    eval_buffer: Vec<Datum>,
}

impl AggFuncExpr {
    fn batch_build(ctx: &EvalContext, expr: Vec<Expr>) -> Result<Vec<AggFuncExpr>> {
        expr.into_iter()
            .map(|v| AggFuncExpr::build(ctx, v))
            .collect()
    }

    fn build(ctx: &EvalContext, mut expr: Expr) -> Result<AggFuncExpr> {
        let args = Expression::batch_build(ctx, expr.take_children().into_vec())?;
        let tp = expr.get_tp();
        let eval_buffer = Vec::with_capacity(args.len());
        Ok(AggFuncExpr {
            args,
            tp,
            eval_buffer,
        })
    }

    fn eval_args(&mut self, ctx: &mut EvalContext, row: &[Datum]) -> Result<()> {
        self.eval_buffer.clear();
        for arg in &self.args {
            self.eval_buffer.push(arg.eval(ctx, row)?);
        }
        Ok(())
    }
}

impl dyn AggrFunc {
    fn update_with_expr(
        &mut self,
        ctx: &mut EvalContext,
        expr: &mut AggFuncExpr,
        row: &[Datum],
    ) -> Result<()> {
        expr.eval_args(ctx, row)?;
        self.update(ctx, &mut expr.eval_buffer)?;
        Ok(())
    }
}

struct AggExecutor {
    group_by: Vec<Expression>,
    aggr_func: Vec<AggFuncExpr>,
    executed: bool,
    ctx: EvalContext,
    related_cols_offset: Vec<usize>, // offset of related columns
    src: Box<dyn Executor + Send>,
    first_collect: bool,
}

impl AggExecutor {
    fn new(
        group_by: Vec<Expr>,
        aggr_func: Vec<Expr>,
        eval_config: Arc<EvalConfig>,
        src: Box<dyn Executor + Send>,
    ) -> Result<AggExecutor> {
        // collect all cols used in aggregation
        let mut visitor = ExprColumnRefVisitor::new(src.get_len_of_columns());
        visitor.batch_visit(&group_by)?;
        visitor.batch_visit(&aggr_func)?;
        let ctx = EvalContext::new(eval_config);
        Ok(AggExecutor {
            group_by: Expression::batch_build(&ctx, group_by)?,
            aggr_func: AggFuncExpr::batch_build(&ctx, aggr_func)?,
            executed: false,
            ctx,
            related_cols_offset: visitor.column_offsets(),
            src,
            first_collect: true,
        })
    }

    fn next(&mut self) -> Result<Option<Vec<Datum>>> {
        if let Some(row) = self.src.next()? {
            let row = row.take_origin();
            row.inflate_cols_with_offsets(&self.ctx, &self.related_cols_offset)
                .map(Some)
        } else {
            Ok(None)
        }
    }

    fn get_group_by_cols(&mut self, row: &[Datum]) -> Result<Vec<Datum>> {
        if self.group_by.is_empty() {
            return Ok(Vec::default());
        }
        let mut vals = Vec::with_capacity(self.group_by.len());
        for expr in &self.group_by {
            let v = expr.eval(&mut self.ctx, row)?;
            vals.push(v);
        }
        Ok(vals)
    }

    fn collect_output_counts(&mut self, counts: &mut Vec<i64>) {
        self.src.collect_output_counts(counts);
    }

    fn collect_metrics_into(&mut self, metrics: &mut ExecutorMetrics) {
        self.src.collect_metrics_into(metrics);
        if self.first_collect {
            metrics.executor_count.aggregation += 1;
            self.first_collect = false;
        }
    }

    fn take_eval_warnings(&mut self) -> Option<EvalWarnings> {
        if let Some(mut warnings) = self.src.take_eval_warnings() {
            warnings.merge(&mut self.ctx.take_warnings());
            Some(warnings)
        } else {
            Some(self.ctx.take_warnings())
        }
    }

    fn get_len_of_columns(&self) -> usize {
        self.src.get_len_of_columns()
    }
}
// HashAggExecutor deals with the aggregate functions.
// When Next() is called, it reads all the data from src
// and updates all the values in group_key_aggrs, then returns a result.
pub struct HashAggExecutor {
    inner: AggExecutor,
    group_key_aggrs: OrderMap<Vec<u8>, Vec<Box<dyn AggrFunc>>>,
    cursor: usize,
}

impl HashAggExecutor {
    pub fn new(
        mut meta: Aggregation,
        eval_config: Arc<EvalConfig>,
        src: Box<dyn Executor + Send>,
    ) -> Result<HashAggExecutor> {
        let group_bys = meta.take_group_by().into_vec();
        let aggs = meta.take_agg_func().into_vec();
        let inner = AggExecutor::new(group_bys, aggs, eval_config, src)?;
        Ok(HashAggExecutor {
            inner,
            group_key_aggrs: OrderMap::new(),
            cursor: 0,
        })
    }

    fn get_group_key(&mut self, row: &[Datum]) -> Result<Vec<u8>> {
        let group_by_cols = self.inner.get_group_by_cols(row)?;
        if group_by_cols.is_empty() {
            let single_group = Datum::Bytes(SINGLE_GROUP.to_vec());
            return Ok(box_try!(datum::encode_value(&[single_group])));
        }
        let res = box_try!(datum::encode_value(&group_by_cols));
        Ok(res)
    }

    fn aggregate(&mut self) -> Result<()> {
        while let Some(cols) = self.inner.next()? {
            let group_key = self.get_group_key(&cols)?;
            match self.group_key_aggrs.entry(group_key) {
                OrderMapEntry::Vacant(e) => {
                    let mut aggrs = Vec::with_capacity(self.inner.aggr_func.len());
                    for expr in &mut self.inner.aggr_func {
                        let mut aggr = aggregate::build_aggr_func(expr.tp)?;
                        aggr.update_with_expr(&mut self.inner.ctx, expr, &cols)?;
                        aggrs.push(aggr);
                    }
                    e.insert(aggrs);
                }
                OrderMapEntry::Occupied(e) => {
                    let aggrs = e.into_mut();
                    for (expr, aggr) in self.inner.aggr_func.iter_mut().zip(aggrs) {
                        aggr.update_with_expr(&mut self.inner.ctx, expr, &cols)?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl Executor for HashAggExecutor {
    fn next(&mut self) -> Result<Option<Row>> {
        if !self.inner.executed {
            self.aggregate()?;
            self.inner.executed = true;
        }

        match self.group_key_aggrs.get_index_mut(self.cursor) {
            Some((mut group_key, aggrs)) => {
                self.cursor += 1;
                let mut aggr_cols = Vec::with_capacity(2 * self.inner.aggr_func.len());

                // calc all aggr func
                for aggr in aggrs {
                    aggr.calc(&mut aggr_cols)?;
                }

                if !self.inner.group_by.is_empty() {
                    Ok(Some(Row::agg(
                        aggr_cols,
                        mem::replace(&mut group_key, Vec::new()),
                    )))
                } else {
                    Ok(Some(Row::agg(aggr_cols, Vec::default())))
                }
            }
            None => Ok(None),
        }
    }

    fn collect_output_counts(&mut self, counts: &mut Vec<i64>) {
        self.inner.collect_output_counts(counts);
    }

    fn collect_metrics_into(&mut self, metrics: &mut ExecutorMetrics) {
        self.inner.collect_metrics_into(metrics)
    }

    fn take_eval_warnings(&mut self) -> Option<EvalWarnings> {
        self.inner.take_eval_warnings()
    }

    fn get_len_of_columns(&self) -> usize {
        self.inner.get_len_of_columns()
    }
}

impl Executor for StreamAggExecutor {
    fn next(&mut self) -> Result<Option<Row>> {
        if self.inner.executed {
            return Ok(None);
        }

        while let Some(cols) = self.inner.next()? {
            self.has_data = true;
            let new_group = self.meet_new_group(&cols)?;
            let ret = if new_group {
                Some(self.get_partial_result()?)
            } else {
                None
            };
            for (expr, func) in self.inner.aggr_func.iter_mut().zip(&mut self.agg_funcs) {
                func.update_with_expr(&mut self.inner.ctx, expr, &cols)?;
            }
            if new_group {
                return Ok(ret);
            }
        }
        self.inner.executed = true;
        // If there is no data in the t, then whether there is 'group by' that can affect the result.
        // e.g. select count(*) from t. Result is 0.
        // e.g. select count(*) from t group by c. Result is empty.
        if !self.has_data && !self.inner.group_by.is_empty() {
            return Ok(None);
        }
        Ok(Some(self.get_partial_result()?))
    }

    fn collect_output_counts(&mut self, counts: &mut Vec<i64>) {
        self.inner.collect_output_counts(counts);
    }

    fn collect_metrics_into(&mut self, metrics: &mut ExecutorMetrics) {
        self.inner.collect_metrics_into(metrics)
    }

    fn take_eval_warnings(&mut self) -> Option<EvalWarnings> {
        self.inner.take_eval_warnings()
    }

    fn get_len_of_columns(&self) -> usize {
        self.inner.get_len_of_columns()
    }
}

// StreamAggExecutor deals with the aggregation functions.
// It assumes all the input data is sorted by group by key.
// When next() is called, it finds a group and returns a result for the same group.
pub struct StreamAggExecutor {
    inner: AggExecutor,
    // save partial agg result
    agg_funcs: Vec<Box<dyn AggrFunc>>,
    cur_group_row: Vec<Datum>,
    next_group_row: Vec<Datum>,
    count: i64,
    has_data: bool,
}

impl StreamAggExecutor {
    pub fn new(
        eval_config: Arc<EvalConfig>,
        src: Box<dyn Executor + Send>,
        mut meta: Aggregation,
    ) -> Result<StreamAggExecutor> {
        let group_bys = meta.take_group_by().into_vec();
        let aggs = meta.take_agg_func().into_vec();
        let group_len = group_bys.len();
        let inner = AggExecutor::new(group_bys, aggs, eval_config, src)?;
        // Get aggregation functions.
        let mut funcs = Vec::with_capacity(inner.aggr_func.len());
        for expr in &inner.aggr_func {
            let agg = aggregate::build_aggr_func(expr.tp)?;
            funcs.push(agg);
        }

        Ok(StreamAggExecutor {
            inner,
            agg_funcs: funcs,
            cur_group_row: Vec::with_capacity(group_len),
            next_group_row: Vec::with_capacity(group_len),
            count: 0,
            has_data: false,
        })
    }

    fn meet_new_group(&mut self, row: &[Datum]) -> Result<bool> {
        let mut cur_group_by_cols = self.inner.get_group_by_cols(row)?;
        if cur_group_by_cols.is_empty() {
            return Ok(false);
        }

        // first group
        if self.cur_group_row.is_empty() {
            mem::swap(&mut self.cur_group_row, &mut cur_group_by_cols);
            return Ok(false);
        }
        let mut meet_new_group = false;
        for (prev, cur) in self.cur_group_row.iter().zip(cur_group_by_cols.iter()) {
            if prev.cmp(&mut self.inner.ctx, cur)? != Ordering::Equal {
                meet_new_group = true;
                break;
            }
        }
        if meet_new_group {
            mem::swap(&mut self.next_group_row, &mut cur_group_by_cols);
        }
        Ok(meet_new_group)
    }

    // get_partial_result gets a result for the same group.
    fn get_partial_result(&mut self) -> Result<Row> {
        let mut cols = Vec::with_capacity(2 * self.agg_funcs.len() + self.cur_group_row.len());
        // Calculate all aggregation funcutions.
        for (i, agg_func) in self.agg_funcs.iter_mut().enumerate() {
            agg_func.calc(&mut cols)?;
            let agg = aggregate::build_aggr_func(self.inner.aggr_func[i].tp)?;
            *agg_func = agg;
        }

        // Get the values of 'group by'.
        if !self.inner.group_by.is_empty() {
            cols.extend_from_slice(self.cur_group_row.as_slice());
            mem::swap(&mut self.cur_group_row, &mut self.next_group_row);
        }

        self.count += 1;
        Ok(Row::agg(cols, Vec::default()))
    }
}
