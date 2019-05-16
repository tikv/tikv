// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use tipb::executor::Limit;

use super::ExecutorMetrics;
use crate::coprocessor::dag::exec_summary::{ExecSummary, ExecSummaryCollector};
use crate::coprocessor::dag::executor::{Executor, Row};
use crate::coprocessor::dag::expr::EvalWarnings;
use crate::coprocessor::Result;

/// Retrieves rows from the source executor and only produces part of the rows.
pub struct LimitExecutor<C: ExecSummaryCollector> {
    summary_collector: C,
    limit: u64,
    cursor: u64,
    src: Box<dyn Executor + Send>,
    first_collect: bool,
}

impl<C: ExecSummaryCollector> LimitExecutor<C> {
    pub fn new(summary_collector: C, limit: Limit, src: Box<dyn Executor + Send>) -> Self {
        LimitExecutor {
            summary_collector,
            limit: limit.get_limit(),
            cursor: 0,
            src,
            first_collect: true,
        }
    }

    fn next_impl(&mut self) -> Result<Option<Row>> {
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
}

impl<C: ExecSummaryCollector> Executor for LimitExecutor<C> {
    fn next(&mut self) -> Result<Option<Row>> {
        let timer = self.summary_collector.on_start_iterate();
        let ret = self.next_impl();
        if let Ok(Some(_)) = ret {
            self.summary_collector.on_finish_iterate(timer, 1);
        } else {
            self.summary_collector.on_finish_iterate(timer, 0);
        }
        ret
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

    fn collect_execution_summaries(&mut self, target: &mut [ExecSummary]) {
        self.src.collect_execution_summaries(target);
        self.summary_collector.collect_into(target);
    }

    fn get_len_of_columns(&self) -> usize {
        self.src.get_len_of_columns()
    }

    fn take_eval_warnings(&mut self) -> Option<EvalWarnings> {
        self.src.take_eval_warnings()
    }
}

#[cfg(test)]
mod tests {
    use crate::coprocessor::codec::datum::Datum;
    use cop_datatype::FieldTypeTp;

    use super::super::tests::*;
    use super::*;
    use crate::coprocessor::dag::exec_summary::ExecSummaryCollectorDisabled;

    #[test]
    fn test_limit_executor() {
        // prepare data and store
        let tid = 1;
        let cis = vec![
            new_col_info(1, FieldTypeTp::LongLong),
            new_col_info(2, FieldTypeTp::VarChar),
        ];
        let raw_data = vec![
            vec![Datum::I64(1), Datum::Bytes(b"a".to_vec())],
            vec![Datum::I64(2), Datum::Bytes(b"b".to_vec())],
            vec![Datum::I64(3), Datum::Bytes(b"c".to_vec())],
            vec![Datum::I64(4), Datum::Bytes(b"d".to_vec())],
            vec![Datum::I64(5), Datum::Bytes(b"e".to_vec())],
            vec![Datum::I64(6), Datum::Bytes(b"f".to_vec())],
            vec![Datum::I64(7), Datum::Bytes(b"g".to_vec())],
        ];
        // prepare range
        let range1 = get_range(tid, 0, 4);
        let range2 = get_range(tid, 5, 10);
        let key_ranges = vec![range1, range2];
        let ts_ect = gen_table_scan_executor(tid, cis, &raw_data, Some(key_ranges));

        // init Limit meta
        let mut limit_meta = Limit::default();
        let limit = 5;
        limit_meta.set_limit(limit);
        // init topn executor
        let mut limit_ect = LimitExecutor::new(ExecSummaryCollectorDisabled, limit_meta, ts_ect);
        let mut limit_rows = Vec::with_capacity(limit as usize);
        while let Some(row) = limit_ect.next().unwrap() {
            limit_rows.push(row.take_origin());
        }
        assert_eq!(limit_rows.len(), limit as usize);
        let expect_row_handles = vec![1, 2, 3, 5, 6];
        for (row, handle) in limit_rows.iter().zip(expect_row_handles) {
            assert_eq!(row.handle, handle);
        }
    }
}
