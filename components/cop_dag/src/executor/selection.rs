// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use tipb::executor::Selection;

use crate::expr::{EvalConfig, EvalContext, EvalWarnings, Expression};
use crate::Result;

use super::{Executor, ExecutorMetrics, ExprColumnRefVisitor, Row};

/// Retrieves rows from the source executor and filter rows by expressions.
pub struct SelectionExecutor {
    conditions: Vec<Expression>,
    related_cols_offset: Vec<usize>, // offset of related columns
    ctx: EvalContext,
    src: Box<dyn Executor + Send>,
    first_collect: bool,
}

impl SelectionExecutor {
    pub fn new(
        mut meta: Selection,
        eval_cfg: Arc<EvalConfig>,
        src: Box<dyn Executor + Send>,
    ) -> Result<SelectionExecutor> {
        let conditions = meta.take_conditions().into_vec();
        let mut visitor = ExprColumnRefVisitor::new(src.get_len_of_columns());
        visitor.batch_visit(&conditions)?;
        let ctx = EvalContext::new(eval_cfg);
        Ok(SelectionExecutor {
            conditions: Expression::batch_build(&ctx, conditions)?,
            related_cols_offset: visitor.column_offsets(),
            ctx,
            src,
            first_collect: true,
        })
    }
}

impl Executor for SelectionExecutor {
    fn next(&mut self) -> Result<Option<Row>> {
        'next: while let Some(row) = self.src.next()? {
            let row = row.take_origin();
            let cols = row.inflate_cols_with_offsets(&self.ctx, &self.related_cols_offset)?;
            for filter in &self.conditions {
                let val = filter.eval(&mut self.ctx, &cols)?;
                if !val.into_bool(&mut self.ctx)?.unwrap_or(false) {
                    continue 'next;
                }
            }
            return Ok(Some(Row::Origin(row)));
        }
        Ok(None)
    }

    fn collect_output_counts(&mut self, counts: &mut Vec<i64>) {
        self.src.collect_output_counts(counts);
    }

    fn collect_metrics_into(&mut self, metrics: &mut ExecutorMetrics) {
        self.src.collect_metrics_into(metrics);
        if self.first_collect {
            metrics.executor_count.selection += 1;
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
