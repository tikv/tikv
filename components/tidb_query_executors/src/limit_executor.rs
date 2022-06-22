// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_common::{storage::IntervalRange, Result};
use tipb::FieldType;

use crate::interface::*;

/// Executor that retrieves rows from the source executor
/// and only produces part of the rows.
pub struct BatchLimitExecutor<Src: BatchExecutor> {
    src: Src,
    remaining_rows: usize,
    is_src_scan_executor: bool,
}

impl<Src: BatchExecutor> BatchLimitExecutor<Src> {
    pub fn new(src: Src, limit: usize, is_src_scan_executor: bool) -> Result<Self> {
        Ok(Self {
            src,
            remaining_rows: limit,
            is_src_scan_executor,
        })
    }
}

impl<Src: BatchExecutor> BatchExecutor for BatchLimitExecutor<Src> {
    type StorageStats = Src::StorageStats;

    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.src.schema()
    }

    #[inline]
    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        let real_scan_rows = if self.is_src_scan_executor {
            std::cmp::min(scan_rows, self.remaining_rows)
        } else {
            scan_rows
        };
        let mut result = self.src.next_batch(real_scan_rows);
        if result.logical_rows.len() < self.remaining_rows {
            self.remaining_rows -= result.logical_rows.len();
        } else {
            // We don't need to touch the physical data.
            result.logical_rows.truncate(self.remaining_rows);
            result.is_drained = Ok(true);
            self.remaining_rows = 0;
        }

        result
    }

    #[inline]
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        self.src.collect_exec_stats(dest);
    }

    #[inline]
    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        self.src.collect_storage_stats(dest);
    }

    #[inline]
    fn take_scanned_range(&mut self) -> IntervalRange {
        self.src.take_scanned_range()
    }

    #[inline]
    fn can_be_cached(&self) -> bool {
        self.src.can_be_cached()
    }
}

#[cfg(test)]
mod tests {
    use tidb_query_datatype::{
        codec::{batch::LazyBatchColumnVec, data_type::VectorValue},
        expr::EvalWarnings,
        FieldTypeTp,
    };

    use super::*;
    use crate::util::mock_executor::{MockExecutor, MockScanExecutor};

    #[test]
    fn test_limit_0() {
        let src_exec = MockExecutor::new(
            vec![FieldTypeTp::LongLong.into()],
            vec![BatchExecuteResult {
                physical_columns: LazyBatchColumnVec::from(vec![VectorValue::Int(
                    vec![None, Some(50), None].into(),
                )]),
                logical_rows: vec![1, 2],
                warnings: EvalWarnings::default(),
                is_drained: Ok(true),
            }],
        );

        let mut exec = BatchLimitExecutor::new(src_exec, 0, false).unwrap();

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 3);
        assert!(r.is_drained.unwrap());
    }

    #[test]
    fn test_error_before_limit() {
        let src_exec = MockExecutor::new(
            vec![FieldTypeTp::LongLong.into()],
            vec![BatchExecuteResult {
                physical_columns: LazyBatchColumnVec::from(vec![VectorValue::Int(
                    vec![None, Some(50), None].into(),
                )]),
                logical_rows: vec![1, 2],
                warnings: EvalWarnings::default(),
                is_drained: Err(other_err!("foo")),
            }],
        );

        let mut exec = BatchLimitExecutor::new(src_exec, 10, false).unwrap();

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[1, 2]);
        assert_eq!(r.physical_columns.rows_len(), 3);
        assert!(r.is_drained.is_err());
    }

    #[test]
    fn test_drain_before_limit() {
        let src_exec = MockExecutor::new(
            vec![FieldTypeTp::LongLong.into()],
            vec![
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![VectorValue::Int(
                        vec![Some(-5), None, None].into(),
                    )]),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![VectorValue::Int(
                        vec![None, Some(50), None].into(),
                    )]),
                    logical_rows: vec![1, 2],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        );

        let mut exec = BatchLimitExecutor::new(src_exec, 10, false).unwrap();

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 3);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[1, 2]);
        assert_eq!(r.physical_columns.rows_len(), 3);
        assert!(r.is_drained.unwrap());
    }

    #[test]
    fn test_error_when_limit() {
        let src_exec = MockExecutor::new(
            vec![FieldTypeTp::LongLong.into()],
            vec![
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![VectorValue::Int(
                        vec![Some(-5), Some(-1), None].into(),
                    )]),
                    logical_rows: vec![1, 2],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![VectorValue::Int(
                        vec![None, Some(50), None].into(),
                    )]),
                    logical_rows: vec![0, 2],
                    warnings: EvalWarnings::default(),
                    is_drained: Err(other_err!("foo")),
                },
            ],
        );

        let mut exec = BatchLimitExecutor::new(src_exec, 4, false).unwrap();

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[1, 2]);
        assert_eq!(r.physical_columns.rows_len(), 3);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[0, 2]);
        assert_eq!(r.physical_columns.rows_len(), 3);
        assert!(r.is_drained.unwrap()); // No errors
    }

    #[test]
    fn test_drain_after_limit() {
        let src_exec = MockExecutor::new(
            vec![FieldTypeTp::LongLong.into()],
            vec![
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![VectorValue::Int(
                        vec![Some(-5), Some(-1), None].into(),
                    )]),
                    logical_rows: vec![1, 2],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::empty(),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![VectorValue::Int(
                        vec![None, Some(50), None, None, Some(1)].into(),
                    )]),
                    logical_rows: vec![0, 4, 1, 3],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        );

        let mut exec = BatchLimitExecutor::new(src_exec, 4, false).unwrap();

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[1, 2]);
        assert_eq!(r.physical_columns.rows_len(), 3);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[0, 4]);
        assert_eq!(r.physical_columns.rows_len(), 5);
        assert!(r.is_drained.unwrap());
    }

    #[test]
    fn test_src_exec_is_scan() {
        let schema = vec![FieldTypeTp::LongLong.into()];
        let rows = (0..1024).collect();
        let src_exec = MockScanExecutor::new(rows, schema);

        let mut exec = BatchLimitExecutor::new(src_exec, 5, true).unwrap();
        let r = exec.next_batch(100);
        assert_eq!(r.logical_rows, &[0, 1, 2, 3, 4]);
        let r = exec.next_batch(2);
        assert_eq!(r.is_drained.unwrap(), true);

        let schema = vec![FieldTypeTp::LongLong.into()];
        let rows = (0..1024).collect();
        let src_exec = MockScanExecutor::new(rows, schema);
        let mut exec = BatchLimitExecutor::new(src_exec, 1024, true).unwrap();
        for _i in 0..1023 {
            let r = exec.next_batch(1);
            assert_eq!(r.is_drained.unwrap(), false);
        }
        let r = exec.next_batch(1);
        assert_eq!(r.is_drained.unwrap(), true);
    }
}
