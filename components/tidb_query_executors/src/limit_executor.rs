// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp::Ordering, sync::Arc};

use async_trait::async_trait;
use tidb_query_common::{Result, storage::IntervalRange};
use tipb::FieldType;
use tidb_query_datatype::{
    codec::{batch::LazyBatchColumnVec, data_type::*},
    expr::{EvalConfig, EvalContext, EvalWarnings},
};
use tidb_query_expr::{RpnStackNode, RpnExpression};

use crate::{interface::*, util::{ensure_columns_decoded, eval_exprs_decoded_no_lifetime}};

/// Executor that retrieves rows from the source executor
/// and only produces part of the rows.
pub struct BatchLimitExecutor<Src: BatchExecutor> {
    src: Src,
    remaining_rows: usize,
    is_src_scan_executor: bool,

    context: EvalContext,
    
    prefix_keys_exps: Vec<RpnExpression>,
    prefix_keys_field_type: Vec<FieldType>,
    prefix_key_num: usize,

    /// Stores previous prefix keys to compare with current prefix keys
    prev_prefix_keys: Vec<ScalarValue>,

    /// Stores current prefix keys
    /// It is just used to reduce allocations. The lifetime is not really
    /// 'static. The elements are only valid in the same batch where they
    /// are added.
    current_prefix_keys_unsafe: Vec<RpnStackNode<'static>>,
    executed_in_limit_for_test: bool,
    executed_in_rank_limit_for_test: bool,
}

impl<Src: BatchExecutor> BatchLimitExecutor<Src> {
    pub fn new(src: Src, limit: usize, is_src_scan_executor: bool) -> Result<Self> {
        Ok(Self {
            src,
            remaining_rows: limit,
            is_src_scan_executor,
            context: EvalContext::new(Arc::new(EvalConfig::default())),
            prefix_keys_exps: Vec::with_capacity(0),
            prefix_keys_field_type: Vec::with_capacity(0),
            prefix_key_num: 0,
            prev_prefix_keys: Vec::with_capacity(0),
            current_prefix_keys_unsafe: Vec::with_capacity(0),
            executed_in_limit_for_test: false,
            executed_in_rank_limit_for_test: false,
        })
    }

    pub fn new_rank_limit(
        src: Src,
        limit: usize,
        is_src_scan_executor: bool,
        config: Arc<EvalConfig>,
        prefix_key_exp_defs: Vec<RpnExpression>) -> Result<Self> {
            let prefix_key_num = prefix_key_exp_defs.len();
            let prefix_keys_field_type: Vec<FieldType> = prefix_key_exp_defs
                .iter()
                .map(|exp| exp.ret_field_type(src.schema()).clone())
                .collect();
            Ok(Self {
                src,
                remaining_rows: limit,
                is_src_scan_executor,
                context: EvalContext::new(config),
                prefix_keys_exps: prefix_key_exp_defs,
                prefix_keys_field_type: prefix_keys_field_type,
                prefix_key_num: prefix_key_num,
                prev_prefix_keys: Vec::with_capacity(prefix_key_num),
                current_prefix_keys_unsafe: Vec::with_capacity(prefix_key_num),
                executed_in_limit_for_test: false,
                executed_in_rank_limit_for_test: false,
            })
    }

    #[cfg(test)]
    pub fn into_child(self) -> Src {
        self.src
    }
}

#[async_trait]
impl<Src: BatchExecutor> BatchExecutor for BatchLimitExecutor<Src> {
    type StorageStats = Src::StorageStats;

    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.src.schema()
    }

    #[inline]
    fn intermediate_schema(&self, index: usize) -> Result<&[FieldType]> {
        self.src.intermediate_schema(index)
    }

    #[inline]
    fn consume_and_fill_intermediate_results(
        &mut self,
        results: &mut [Vec<BatchExecuteResult>],
    ) -> Result<()> {
        self.src.consume_and_fill_intermediate_results(results)
    }

    #[inline]
    async fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        if self.prefix_keys_exps.len() > 0 {  
            #[cfg(debug_assertions)] { self.executed_in_rank_limit_for_test = true; }

            if self.remaining_rows == 0 {
                return BatchExecuteResult {
                        physical_columns: LazyBatchColumnVec::empty(),
                        logical_rows: Vec::new(),
                        warnings: EvalWarnings::default(),
                        is_drained: Ok(BatchExecIsDrain::Drain)
                    };
            }

            let mut result = self.src.next_batch(scan_rows).await;

            let src_schema = self.src.schema();
            // Decode columns with mutable input first, so subsequent access to input can be
            // immutable (and the borrow checker will be happy)
            let mut res = ensure_columns_decoded(
                &mut self.context,
                &self.prefix_keys_exps,
                src_schema,
                &mut result.physical_columns,
                &result.logical_rows,
            );
            match res {
                Ok(_) => {},
                Err(err) => {return BatchExecuteResult{is_drained:Err(err), physical_columns: result.physical_columns, logical_rows: result.logical_rows, warnings: EvalWarnings::default()};}
            }

            assert!(self.current_prefix_keys_unsafe.is_empty());
            unsafe {
                res = eval_exprs_decoded_no_lifetime(
                    &mut self.context,
                    &self.prefix_keys_exps,
                    src_schema,
                    &mut result.physical_columns,
                    &result.logical_rows,
                    &mut self.current_prefix_keys_unsafe,
                );

                match res {
                    Ok(_) => {},
                    Err(err) => {return BatchExecuteResult{is_drained:Err(err), physical_columns: result.physical_columns, logical_rows: result.logical_rows, warnings: EvalWarnings::default()};}
                }
            }

            let mut cur_prefix_keys_ref = Vec::with_capacity(self.prefix_keys_exps.len());
            let logical_rows_len = result.logical_rows.len();
            for logical_row_idx in 0..logical_rows_len {
                for cur_prefix_key_result in &self.current_prefix_keys_unsafe {
                    cur_prefix_keys_ref.push(cur_prefix_key_result.get_logical_scalar_ref(logical_row_idx));
                }

                if self.prev_prefix_keys.len() == 0 {
                    self.prev_prefix_keys.extend(cur_prefix_keys_ref.drain(..).map(ScalarValueRef::to_owned));
                    cur_prefix_keys_ref.clear();
                    continue;
                }

                let prefix_key_match = || -> Result<bool> {
                    match self.prev_prefix_keys.chunks_exact(self.prefix_key_num).next() {
                        Some(current_key) => {
                            for preifx_key_col_index in 0..self.prefix_key_num {
                                if current_key[preifx_key_col_index]
                                    .as_scalar_value_ref()
                                    .cmp_sort_key(
                                        &cur_prefix_keys_ref[preifx_key_col_index],
                                        &self.prefix_keys_field_type[preifx_key_col_index],
                                    )?
                                    != Ordering::Equal
                                {
                                    return Ok(false);
                                }
                            }
                            Ok(true)
                        }
                        _ => Ok(false),
                    }
                };

                let res = prefix_key_match();
                match res {
                    Ok(v) => {
                        if v {
                            cur_prefix_keys_ref.clear();
                        } else {
                            self.remaining_rows -= 1;
                            if self.remaining_rows == 0 {
                                result.logical_rows.truncate(logical_row_idx);
                                result.is_drained = Ok(BatchExecIsDrain::Drain);
                                return result;
                            } else {
                                self.prev_prefix_keys.clear();
                                self.prev_prefix_keys.extend(cur_prefix_keys_ref.drain(..).map(ScalarValueRef::to_owned));
                            }
                        }
                    },
                    Err(err) => {return BatchExecuteResult{is_drained:Err(err), physical_columns: result.physical_columns, logical_rows: result.logical_rows, warnings: EvalWarnings::default()};}
                }
            }
            return result;
        } else {
            #[cfg(debug_assertions)] { self.executed_in_limit_for_test = true; }

            let real_scan_rows = if self.is_src_scan_executor {
                std::cmp::min(scan_rows, self.remaining_rows)
            } else {
                scan_rows
            };
            let mut result = self.src.next_batch(real_scan_rows).await;
            if result.logical_rows.len() < self.remaining_rows {
                self.remaining_rows -= result.logical_rows.len();
            } else {
                // We don't need to touch the physical data.
                result.logical_rows.truncate(self.remaining_rows);
                result.is_drained = Ok(BatchExecIsDrain::Drain);
                self.remaining_rows = 0;
            }

            result
        }
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
    use futures::executor::block_on;
    use tidb_query_datatype::{
        FieldTypeTp,
        codec::{batch::LazyBatchColumnVec, data_type::VectorValue},
        expr::EvalWarnings,
    };

    use tidb_query_expr::RpnExpressionBuilder;

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
                is_drained: Ok(BatchExecIsDrain::Drain),
            }],
        );

        let mut exec = BatchLimitExecutor::new(src_exec, 0, false).unwrap();

        let r = block_on(exec.next_batch(1));
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 3);
        assert!(r.is_drained.unwrap().stop());
        assert!(exec.executed_in_limit_for_test);
        assert!(!exec.executed_in_rank_limit_for_test);
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

        let r = block_on(exec.next_batch(1));
        assert_eq!(&r.logical_rows, &[1, 2]);
        assert_eq!(r.physical_columns.rows_len(), 3);
        r.is_drained.unwrap_err();
        assert!(exec.executed_in_limit_for_test);
        assert!(!exec.executed_in_rank_limit_for_test);
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
                    is_drained: Ok(BatchExecIsDrain::Remain),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![VectorValue::Int(
                        vec![None, Some(50), None].into(),
                    )]),
                    logical_rows: vec![1, 2],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(BatchExecIsDrain::Drain),
                },
            ],
        );

        let mut exec = BatchLimitExecutor::new(src_exec, 10, false).unwrap();

        let r = block_on(exec.next_batch(1));
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 3);
        assert!(r.is_drained.unwrap().is_remain());

        let r = block_on(exec.next_batch(1));
        assert_eq!(&r.logical_rows, &[1, 2]);
        assert_eq!(r.physical_columns.rows_len(), 3);
        assert!(r.is_drained.unwrap().stop());
        assert!(exec.executed_in_limit_for_test);
        assert!(!exec.executed_in_rank_limit_for_test);
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
                    is_drained: Ok(BatchExecIsDrain::Remain),
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

        let r = block_on(exec.next_batch(1));
        assert_eq!(&r.logical_rows, &[1, 2]);
        assert_eq!(r.physical_columns.rows_len(), 3);
        assert!(r.is_drained.unwrap().is_remain());

        let r = block_on(exec.next_batch(1));
        assert_eq!(&r.logical_rows, &[0, 2]);
        assert_eq!(r.physical_columns.rows_len(), 3);
        assert!(r.is_drained.unwrap().stop()); // No errors
        assert!(exec.executed_in_limit_for_test);
        assert!(!exec.executed_in_rank_limit_for_test);
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
                    is_drained: Ok(BatchExecIsDrain::Remain),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::empty(),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(BatchExecIsDrain::Remain),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![VectorValue::Int(
                        vec![None, Some(50), None, None, Some(1)].into(),
                    )]),
                    logical_rows: vec![0, 4, 1, 3],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(BatchExecIsDrain::Drain),
                },
            ],
        );

        let mut exec = BatchLimitExecutor::new(src_exec, 4, false).unwrap();

        let r = block_on(exec.next_batch(1));
        assert_eq!(&r.logical_rows, &[1, 2]);
        assert_eq!(r.physical_columns.rows_len(), 3);
        assert!(r.is_drained.unwrap().is_remain());

        let r = block_on(exec.next_batch(1));
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(r.is_drained.unwrap().is_remain());

        let r = block_on(exec.next_batch(1));
        assert_eq!(&r.logical_rows, &[0, 4]);
        assert_eq!(r.physical_columns.rows_len(), 5);
        assert!(r.is_drained.unwrap().stop());
        assert!(exec.executed_in_limit_for_test);
        assert!(!exec.executed_in_rank_limit_for_test);
    }

    #[test]
    fn test_src_exec_is_scan() {
        let schema = vec![FieldTypeTp::LongLong.into()];
        let rows = (0..1024).collect();
        let src_exec = MockScanExecutor::new(rows, schema);

        let mut exec = BatchLimitExecutor::new(src_exec, 5, true).unwrap();
        let r = block_on(exec.next_batch(100));
        assert_eq!(r.logical_rows, &[0, 1, 2, 3, 4]);
        let r = block_on(exec.next_batch(2));
        assert!(r.is_drained.unwrap().stop());

        let schema = vec![FieldTypeTp::LongLong.into()];
        let rows = (0..1024).collect();
        let src_exec = MockScanExecutor::new(rows, schema);
        let mut exec = BatchLimitExecutor::new(src_exec, 1024, true).unwrap();
        for _i in 0..1023 {
            let r = block_on(exec.next_batch(1));
            assert!(r.is_drained.unwrap().is_remain());
        }
        let r = block_on(exec.next_batch(1));
        assert!(r.is_drained.unwrap().stop());
        assert!(exec.executed_in_limit_for_test);
        assert!(!exec.executed_in_rank_limit_for_test);
    }

    #[test]
    fn test_extra_common_handle_keys() {
        let src = MockExecutor::new(
            vec![FieldTypeTp::LongLong.into(), FieldTypeTp::LongLong.into()],
            vec![BatchExecuteResult {
                physical_columns: LazyBatchColumnVec::with_columns_and_extra_common_handle_keys(
                    vec![
                        VectorValue::Int(
                            vec![Some(10), Some(11), Some(12), Some(13), Some(14)].into(),
                        )
                        .into(),
                        VectorValue::Int(
                            vec![Some(20), Some(21), Some(22), Some(23), Some(24)].into(),
                        )
                        .into(),
                    ],
                    Some(vec![
                        b"h00".to_vec(),
                        b"h01".to_vec(),
                        b"h02".to_vec(),
                        b"h03".to_vec(),
                        b"h04".to_vec(),
                    ]),
                ),
                logical_rows: vec![4, 0, 2, 1, 3],
                warnings: EvalWarnings::default(),
                is_drained: Ok(BatchExecIsDrain::Drain),
            }],
        );

        let mut exec = BatchLimitExecutor::new(src, 3, true).unwrap();
        let mut r = block_on(exec.next_batch(100));
        assert_eq!(r.logical_rows, &[4, 0, 2]);
        assert_eq!(
            r.physical_columns.take_extra_common_handle_keys(),
            Some(vec![
                b"h00".to_vec(),
                b"h01".to_vec(),
                b"h02".to_vec(),
                b"h03".to_vec(),
                b"h04".to_vec(),
            ]),
        );
        assert!(r.is_drained.unwrap().stop());
        assert!(exec.executed_in_limit_for_test);
        assert!(!exec.executed_in_rank_limit_for_test);
    }

    #[test]
    fn test_rank_limit_0() {
        let src_exec = MockExecutor::new(
            vec![FieldTypeTp::LongLong.into()],
            vec![BatchExecuteResult {
                physical_columns: LazyBatchColumnVec::from(vec![VectorValue::Int(
                    vec![None, Some(50), None].into(),
                )]),
                logical_rows: vec![1, 2],
                warnings: EvalWarnings::default(),
                is_drained: Ok(BatchExecIsDrain::Drain),
            }],
        );

        let config = Arc::new(EvalConfig::default());
        let group_by_exp = || {
            RpnExpressionBuilder::new_for_test()
                .push_column_ref_for_test(1)
                .build_for_test()
        };
        let mut exec = BatchLimitExecutor::new_rank_limit(src_exec, 0, false, config, vec![group_by_exp()]).unwrap();

        let r = block_on(exec.next_batch(1));
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 3);
        assert!(r.is_drained.unwrap().stop());
    }
}
