// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tipb::expression::FieldType;

use crate::coprocessor::dag::batch::interface::*;
use crate::coprocessor::Result;

/// Executor that retrieves rows from the source executor
/// and only produces part of the rows.
pub struct BatchLimitExecutor<C: ExecSummaryCollector, Src: BatchExecutor> {
    summary_collector: C,
    src: Src,
    remaining_rows: usize,
}

impl<C: ExecSummaryCollector, Src: BatchExecutor> BatchLimitExecutor<C, Src> {
    pub fn new(summary_collector: C, src: Src, limit: usize) -> Result<Self> {
        if limit == 0 {
            return Err(box_err!("limit should not be zero"));
        }
        Ok(Self {
            summary_collector,
            src,
            remaining_rows: limit,
        })
    }
}

impl<C: ExecSummaryCollector, Src: BatchExecutor> BatchExecutor for BatchLimitExecutor<C, Src> {
    fn schema(&self) -> &[FieldType] {
        self.src.schema()
    }

    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        let timer = self.summary_collector.on_start_iterate();

        let mut result = self.src.next_batch(scan_rows);
        if result.data.rows_len() < self.remaining_rows {
            self.remaining_rows -= result.data.rows_len();
        } else {
            result.data.truncate(self.remaining_rows);
            self.remaining_rows = 0;
            result.is_drained = Ok(true);
        }

        self.summary_collector
            .on_finish_iterate(timer, result.data.rows_len());

        result
    }

    fn collect_statistics(&mut self, destination: &mut BatchExecuteStatistics) {
        self.src.collect_statistics(destination);
        self.summary_collector
            .collect_into(&mut destination.summary_per_executor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coprocessor::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
    use crate::coprocessor::codec::data_type::VectorValue;
    use crate::coprocessor::dag::exec_summary::*;
    use crate::coprocessor::dag::expr::EvalConfig;
    use cop_datatype::{EvalType, FieldTypeTp};
    use tipb::expression::FieldType;

    struct MockExecutor {
        // ID(INT,PK), Foo(INT), Bar(Float,Default 4.5)
        data: Vec<(i64, Option<i64>, Option<f64>)>,
        field_types: Vec<FieldType>,
        offset: usize,
        cfg: EvalConfig,
    }

    impl MockExecutor {
        /// create the MockExecutor with fixed schema and data.
        fn new() -> MockExecutor {
            let expect_rows = vec![
                (1, Some(10), Some(5.2)),
                (3, Some(-5), None),
                (4, None, Some(4.5)),
                (5, None, Some(0.1)),
                (6, None, Some(4.5)),
            ];
            let field_types = vec![
                FieldTypeTp::LongLong.into(),
                FieldTypeTp::LongLong.into(),
                FieldTypeTp::Double.into(),
            ];

            MockExecutor {
                data: expect_rows,
                field_types,
                offset: 0,
                cfg: EvalConfig::default(),
            }
        }
    }

    impl BatchExecutor for MockExecutor {
        fn schema(&self) -> &[FieldType] {
            self.field_types.as_ref()
        }

        fn next_batch(&mut self, expect_rows: usize) -> BatchExecuteResult {
            let upper = if expect_rows + self.offset > self.data.len() {
                self.data.len()
            } else {
                expect_rows + self.offset
            };
            let mut pks = VectorValue::with_capacity(self.data.len(), EvalType::Int);
            let mut foos = VectorValue::with_capacity(self.data.len(), EvalType::Int);
            let mut bars = VectorValue::with_capacity(self.data.len(), EvalType::Real);
            for id in self.offset..upper {
                let (handle, foo, bar) = self.data[id];
                pks.push_int(Some(handle));
                foos.push_int(foo);
                bars.push_real(bar);
            }
            self.offset = upper;
            BatchExecuteResult {
                data: LazyBatchColumnVec::from(vec![
                    LazyBatchColumn::Decoded(pks),
                    LazyBatchColumn::Decoded(foos),
                    LazyBatchColumn::Decoded(bars),
                ]),
                warnings: self.cfg.new_eval_warnings(),
                is_drained: Ok(self.offset == self.data.len()),
            }
        }

        fn collect_statistics(&mut self, _: &mut BatchExecuteStatistics) {}
    }

    #[test]
    fn test_limit_normal() {
        let data = vec![
            //limit, expect_rows(real get rows)
            (4, 3), // get less than limit
            (3, 4), // get more than limit
            (3, 3), // get equals to limit
        ];
        for (limit, get_rows) in data {
            let src = MockExecutor::new();
            let mut executor =
                BatchLimitExecutor::new(ExecSummaryCollectorDisabled, src, limit).unwrap();
            let res = executor.next_batch(get_rows);
            if limit <= get_rows {
                // is drained
                assert!(res.is_drained.as_ref().unwrap());
                assert_eq!(res.data.rows_len(), limit);
            } else {
                assert!(!res.is_drained.as_ref().unwrap());
                assert_eq!(res.data.rows_len(), get_rows);
            }
        }
    }

    #[test]
    fn test_limit_remaining() {
        let src = MockExecutor::new();
        let mut executor = BatchLimitExecutor::new(ExecSummaryCollectorDisabled, src, 5).unwrap();
        let expect_rows = 3;
        let mut remaining_rows = 5;
        while remaining_rows > 0 {
            let res = executor.next_batch(expect_rows);
            remaining_rows -= res.data.rows_len();
            assert_eq!(remaining_rows, executor.remaining_rows);
        }
    }

    #[test]
    fn test_execution_summary() {
        let src = MockExecutor::new();
        let mut executor =
            BatchLimitExecutor::new(ExecSummaryCollectorEnabled::new(1), src, 4).unwrap();
        executor.next_batch(1);
        executor.next_batch(2);
        let mut s = BatchExecuteStatistics::new(2, 1);
        // Collected statistics remain unchanged until `next_batch` generated delta statistics.
        for _ in 0..2 {
            executor.collect_statistics(&mut s);
            let exec_summary = s.summary_per_executor[1];
            assert_eq!(3, exec_summary.num_produced_rows);
            assert_eq!(2, exec_summary.num_iterations);
        }

        // we get 1 row since the limit is 4
        executor.next_batch(10);
        executor.collect_statistics(&mut s);
        let exec_summary = s.summary_per_executor[1];
        assert_eq!(4, exec_summary.num_produced_rows);
        assert_eq!(3, exec_summary.num_iterations);
    }

    #[test]
    fn test_invalid_limit() {
        let src = MockExecutor::new();
        assert!(BatchLimitExecutor::new(ExecSummaryCollectorDisabled, src, 0).is_err());
    }

    /// MockErrExecutor is based on MockExecutor, the only difference is
    /// that when call the function next_batch, it always returns is_drained error.
    struct MockErrExecutor(MockExecutor);
    impl MockErrExecutor {
        fn new() -> Self {
            MockErrExecutor(MockExecutor::new())
        }
    }
    impl BatchExecutor for MockErrExecutor {
        fn next_batch(&mut self, expect_rows: usize) -> BatchExecuteResult {
            let mut result = self.0.next_batch(expect_rows);
            result.is_drained = Err(box_err!("next batch mock error"));
            result
        }

        fn collect_statistics(&mut self, _: &mut BatchExecuteStatistics) {}

        fn schema(&self) -> &[FieldType] {
            self.0.schema()
        }
    }

    #[test]
    fn test_src_next_batch_err() {
        let data = vec![
            //limit, expect_rows(real get rows)
            (4, 3), // error happens before limit rows
            (3, 4), // error happens after limit rows
            (3, 3), // error happens when get the limit + 1 row.
        ];
        for (limit, get_rows) in data {
            let src = MockErrExecutor::new();
            let mut executor =
                BatchLimitExecutor::new(ExecSummaryCollectorDisabled, src, limit).unwrap();
            let res = executor.next_batch(get_rows);
            if limit <= get_rows {
                // error happens after limit rows
                assert!(res.is_drained.as_ref().unwrap());
                assert_eq!(res.data.rows_len(), limit);
            } else {
                // error happens before limit rows
                assert!(res.is_drained.as_ref().is_err());
                assert_eq!(res.data.rows_len(), get_rows);
            }
        }
    }
}
