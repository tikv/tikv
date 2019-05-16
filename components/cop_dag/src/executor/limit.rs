// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use tipb::executor::Limit;

use super::ExecutorMetrics;
use crate::executor::{Executor, Row};
use crate::expr::EvalWarnings;
use crate::Result;

/// Retrieves rows from the source executor and only produces part of the rows.
pub struct LimitExecutor<'a> {
    limit: u64,
    cursor: u64,
    src: Box<dyn Executor + Send + 'a>,
    first_collect: bool,
}

impl<'a> LimitExecutor<'a> {
    pub fn new(limit: Limit, src: Box<dyn Executor + Send + 'a>) -> LimitExecutor<'_> {
        LimitExecutor {
            limit: limit.get_limit(),
            cursor: 0,
            src,
            first_collect: true,
        }
    }
}

impl<'a> Executor for LimitExecutor<'a> {
    fn next(&mut self) -> Result<Option<Row>> {
        if self.cursor >= self.limit {
            return Ok(None);
        }
        if let Some(row) = self.src.next()? {
            self.cursor += 1;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }

    fn collect_output_counts(&mut self, _: &mut Vec<i64>) {
        // We do not know whether `limit` has consumed all of it's source, so just ignore it.
    }

    fn collect_metrics_into(&mut self, metrics: &mut ExecutorMetrics) {
        self.src.collect_metrics_into(metrics);
        if self.first_collect {
            metrics.executor_count.limit += 1;
            self.first_collect = false;
        }
    }

    fn take_eval_warnings(&mut self) -> Option<EvalWarnings> {
        self.src.take_eval_warnings()
    }

    fn get_len_of_columns(&self) -> usize {
        self.src.get_len_of_columns()
    }
}
