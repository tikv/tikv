// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp::Ordering, sync::Arc};

use async_trait::async_trait;
use tidb_query_common::{Result, storage::IntervalRange};
use tipb::{FieldType, Expr};
use tidb_query_datatype::{
    codec::{batch::LazyBatchColumnVec, data_type::*},
    expr::{EvalConfig, EvalContext, EvalWarnings},
};
use tidb_query_expr::{RpnStackNode, RpnExpression, RpnExpressionBuilder};

use crate::{interface::*, util::{ensure_columns_decoded, eval_exprs_decoded_no_lifetime}};

/// Executor that retrieves rows from the source executor
/// and only produces part of the rows.
pub struct BatchLimitExecutor<Src: BatchExecutor> {
    src: Src,
    remaining_rows: usize,
    is_src_scan_executor: bool,

    context: EvalContext,
    
    truncate_keys_exps: Vec<RpnExpression>,
    truncate_keys_field_type: Vec<FieldType>,
    truncate_key_num: usize,

    /// Stores previous truncate keys to compare with current truncate keys
    prev_truncate_keys: Vec<ScalarValue>,

    /// Stores current truncate keys
    /// It is just used to reduce allocations. The lifetime is not really
    /// 'static. The elements are only valid in the same batch where they
    /// are added.
    current_truncate_keys_unsafe: Vec<RpnStackNode<'static>>,
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
            truncate_keys_exps: Vec::with_capacity(0),
            truncate_keys_field_type: Vec::with_capacity(0),
            truncate_key_num: 0,
            prev_truncate_keys: Vec::with_capacity(0),
            current_truncate_keys_unsafe: Vec::with_capacity(0),
            executed_in_limit_for_test: false,
            executed_in_rank_limit_for_test: false,
        })
    }

    pub fn new_rank_limit_for_test(
        src: Src,
        limit: usize,
        is_src_scan_executor: bool,
        config: Arc<EvalConfig>,
        truncate_key_exp: Vec<RpnExpression>
    ) -> Result<Self> {
        return Self::new_rank_limit_impl(src, limit, is_src_scan_executor, config, truncate_key_exp)
    }

    pub fn new_rank_limit_impl(
        src: Src,
        limit: usize,
        is_src_scan_executor: bool,
        config: Arc<EvalConfig>,
        truncate_key_exp: Vec<RpnExpression>
    ) -> Result<Self> {
        let truncate_key_num = truncate_key_exp.len();
        let truncate_keys_field_type: Vec<FieldType> = truncate_key_exp
            .iter()
            .map(|exp| exp.ret_field_type(src.schema()).clone())
            .collect();
        Ok(Self {
            src,
            remaining_rows: limit,
            is_src_scan_executor,
            context: EvalContext::new(config),
            truncate_keys_exps: truncate_key_exp,
            truncate_keys_field_type,
            truncate_key_num,
            prev_truncate_keys: Vec::with_capacity(truncate_key_num),
            current_truncate_keys_unsafe: Vec::with_capacity(truncate_key_num),
            executed_in_limit_for_test: false,
            executed_in_rank_limit_for_test: false,
        })
    }

    // truncate_key_exp_defs: Vec<RpnExpression>
    pub fn new_rank_limit(
        src: Src,
        limit: usize,
        is_src_scan_executor: bool,
        config: Arc<EvalConfig>,
        truncate_key_exp_defs: Vec<Expr>) -> Result<Self> {
        
        let schema_len = src.schema().len();
        let mut truncate_key_exp = Vec::with_capacity(truncate_key_exp_defs.len());
        let mut ctx = EvalContext::new(config.clone());
        for def in truncate_key_exp_defs {
            truncate_key_exp.push(RpnExpressionBuilder::build_from_expr_tree(
                def, &mut ctx, schema_len,
            )?);
        }
        Self::new_rank_limit_impl(
            src,
            limit,
            is_src_scan_executor,
            config,
            truncate_key_exp
        )
    }

    #[cfg(test)]
    pub fn into_child(self) -> Src {
        self.src
    }

    // Record the truncate key values for the last row
    #[inline]
    fn record_truncate_key_values(&mut self, result: &mut BatchExecuteResult, idx: usize) -> Result<()> {
        let src_schema = self.src.schema();
        let mut res = ensure_columns_decoded(
            &mut self.context,
            &self.truncate_keys_exps,
            src_schema,
            &mut result.physical_columns,
            &result.logical_rows,
        );
        match res {
            Ok(_) => {},
            Err(err) => {return Err(err);}
        }

        self.current_truncate_keys_unsafe.clear();
        unsafe {
            res = eval_exprs_decoded_no_lifetime(
                &mut self.context,
                &self.truncate_keys_exps,
                src_schema,
                &mut result.physical_columns,
                &result.logical_rows,
                &mut self.current_truncate_keys_unsafe,
            );

            match res {
                Ok(_) => {},
                Err(err) => {return Err(err);}
            }
        }

        let mut cur_truncate_keys_ref = Vec::with_capacity(self.truncate_keys_exps.len());
        for cur_truncate_key_result in &self.current_truncate_keys_unsafe {
            cur_truncate_keys_ref.push(cur_truncate_key_result.get_logical_scalar_ref(idx));
        }

        self.prev_truncate_keys.clear();
        self.prev_truncate_keys.extend(cur_truncate_keys_ref.drain(..).map(ScalarValueRef::to_owned));
        cur_truncate_keys_ref.clear();
        Ok(())
    }

    #[inline]
    fn find_different_truncate_key_row(&mut self, result: &mut BatchExecuteResult, start_idx: usize) -> Result<usize> {
        let src_schema = self.src.schema();
        // Decode columns with mutable input first, so subsequent access to input can be
        // immutable (and the borrow checker will be happy)
        let mut res = ensure_columns_decoded(
            &mut self.context,
            &self.truncate_keys_exps,
            src_schema,
            &mut result.physical_columns,
            &result.logical_rows,
        );
        match res {
            Ok(_) => {},
            Err(err) => {return Err(err);}
        }

        self.current_truncate_keys_unsafe.clear();
        unsafe {
            res = eval_exprs_decoded_no_lifetime(
                &mut self.context,
                &self.truncate_keys_exps,
                src_schema,
                &mut result.physical_columns,
                &result.logical_rows,
                &mut self.current_truncate_keys_unsafe,
            );

            match res {
                Ok(_) => {},
                Err(err) => {return Err(err);}
            }
        }

        let total_row_num = result.logical_rows.len();
        let mut i = start_idx;
        let mut cur_truncate_keys_ref = Vec::with_capacity(self.truncate_keys_exps.len());
        while i < total_row_num {
            for cur_truncate_key_result in &self.current_truncate_keys_unsafe {
                cur_truncate_keys_ref.push(cur_truncate_key_result.get_logical_scalar_ref(i));
            }

            if self.prev_truncate_keys.len() == 0 {
                self.prev_truncate_keys.extend(cur_truncate_keys_ref.drain(..).map(ScalarValueRef::to_owned));
                cur_truncate_keys_ref.clear();
                i += 1;
                continue;
            }

            let truncate_key_match = || -> Result<bool> {
                match self.prev_truncate_keys.chunks_exact(self.truncate_key_num).next() {
                    Some(current_key) => {
                        for preifx_key_col_index in 0..self.truncate_key_num {
                            if current_key[preifx_key_col_index]
                                .as_scalar_value_ref()
                                .cmp_sort_key(
                                    &cur_truncate_keys_ref[preifx_key_col_index],
                                    &self.truncate_keys_field_type[preifx_key_col_index],
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

            let res = truncate_key_match();
            match res {
                Ok(v) => {
                    if v {
                        cur_truncate_keys_ref.clear();
                    } else {
                        return Ok(i);
                    }
                },
                Err(err) => {return Err(err);}
            }

            i += 1;
        }
        return Ok(i);
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
        let mut real_scan_rows = if self.is_src_scan_executor {
            std::cmp::min(scan_rows, self.remaining_rows)
        } else {
            scan_rows
        };
        
        if self.truncate_keys_exps.len() > 0 {  
            #[cfg(debug_assertions)] { self.executed_in_rank_limit_for_test = true; }

            if self.remaining_rows == 0 && self.prev_truncate_keys.len() == 0 {
                return BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::empty(),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(BatchExecIsDrain::Drain)
                };
            }

            if real_scan_rows == 0 {
                real_scan_rows = crate::runner::BATCH_MAX_SIZE;
            }

            let mut result = self.src.next_batch(real_scan_rows).await;
            let total_row_num = result.logical_rows.len();
            if total_row_num == 0 {
                return result;
            }

            let output_row_num;
            if total_row_num < self.remaining_rows {
                output_row_num = total_row_num;
                self.remaining_rows -= output_row_num;

                // Record truncate key values for further search
                let res = self.record_truncate_key_values(&mut result, output_row_num-1);
                match res {
                    Ok(_) => {},
                    Err(err) => {return BatchExecuteResult{is_drained:Err(err), physical_columns: result.physical_columns, logical_rows: result.logical_rows, warnings: EvalWarnings::default()}}
                }
            } else {
                // When self.remaining_rows == 0, it means that previous truncate key values have been recorded before.
                if self.remaining_rows != 0 {
                    // Record truncate key values for further search
                    let res = self.record_truncate_key_values(&mut result, self.remaining_rows-1);
                    match res {
                        Ok(_) => {},
                        Err(err) => {return BatchExecuteResult{is_drained:Err(err), physical_columns: result.physical_columns, logical_rows: result.logical_rows, warnings: EvalWarnings::default()}}
                    }
                }

                // Traverse remaining rows, so that we can find the row whose truncate key values are different the prev.
                let end_idx = self.find_different_truncate_key_row(&mut result, self.remaining_rows);
                match end_idx {
                    Ok(v) => {output_row_num = v;}
                    Err(err) => {return BatchExecuteResult{is_drained:Err(err), physical_columns: result.physical_columns, logical_rows: result.logical_rows, warnings: EvalWarnings::default()}}
                }
                self.remaining_rows = 0;
            }

            if output_row_num < result.logical_rows.len() {
                result.is_drained = Ok(BatchExecIsDrain::Drain);
            }

            // We don't need to touch the physical data.
            result.logical_rows.truncate(output_row_num);
            return result;
        } else {
            #[cfg(debug_assertions)] { self.executed_in_limit_for_test = true; }

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
        builder::FieldTypeBuilder, codec::{batch::LazyBatchColumnVec, data_type::VectorValue}, expr::EvalWarnings, Collation, FieldTypeTp
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
            vec![FieldTypeTp::LongLong.into(), FieldTypeTp::LongLong.into()],
            vec![BatchExecuteResult {
                physical_columns: LazyBatchColumnVec::from(vec![
                    VectorValue::Int(vec![None, Some(50), None].into()),
                    VectorValue::Int(vec![None, Some(50), None].into()),
                ]),
                logical_rows: vec![1, 2],
                warnings: EvalWarnings::default(),
                is_drained: Ok(BatchExecIsDrain::Drain),
            }],
        );

        let config = Arc::new(EvalConfig::default());
        let truncate_key_exp = 
            RpnExpressionBuilder::new_for_test()
                .push_column_ref_for_test(1)
                .build_for_test();
        let mut exec = BatchLimitExecutor::new_rank_limit_for_test(src_exec, 0, true, config, vec![truncate_key_exp]).unwrap();

        let r = block_on(exec.next_batch(1));
        assert!(r.logical_rows.is_empty());
        assert!(r.is_drained.unwrap().stop());
    }

    #[test]
    fn test_rank_limit_one_chunk() {
        let limits = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100];
        let results = vec![
            vec![],
            vec![0, 1, 2],
            vec![0, 1, 2],
            vec![0, 1, 2],
            vec![0, 1, 2, 3],
            vec![0, 1, 2, 3, 4, 5, 6, 7],
            vec![0, 1, 2, 3, 4, 5, 6, 7],
            vec![0, 1, 2, 3, 4, 5, 6, 7],
            vec![0, 1, 2, 3, 4, 5, 6, 7],
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8],
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        ];

        for i in 0..limits.len() {
            let src_exec = MockExecutor::new(
                vec![
                    FieldTypeBuilder::new().tp(FieldTypeTp::VarString).collation(Collation::Utf8Mb4Bin).build(),
                    FieldTypeBuilder::new().tp(FieldTypeTp::VarString).collation(Collation::Utf8Mb4Bin).build(),
                ],
                vec![BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Bytes(vec![
                            Some(b"val1".to_vec()),
                            Some(b"val2".to_vec()),
                            Some(b"val3".to_vec()),
                            Some(b"val4".to_vec()),
                            Some(b"val5".to_vec()),
                            Some(b"val6".to_vec()),
                            Some(b"val7".to_vec()),
                            Some(b"val8".to_vec()),
                            Some(b"val9".to_vec()),
                            Some(b"val10".to_vec()),
                        ].into()),
                        VectorValue::Bytes(vec![
                            Some(b"group1".to_vec()),
                            Some(b"group1".to_vec()),
                            Some(b"group1".to_vec()),
                            Some(b"group2".to_vec()),
                            Some(b"group3".to_vec()),
                            Some(b"group3".to_vec()),
                            Some(b"group3".to_vec()),
                            Some(b"group3".to_vec()),
                            Some(b"group4".to_vec()),
                            Some(b"group5".to_vec()),
                        ].into()),
                    ]),
                    logical_rows: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(BatchExecIsDrain::Drain),
                }],
            );

            let config = Arc::new(EvalConfig::default());
            let truncate_key_exp = 
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(1)
                    .build_for_test();

            let mut exec = BatchLimitExecutor::new_rank_limit_for_test(src_exec, limits[i], true, config, vec![truncate_key_exp]).unwrap();

            let r = block_on(exec.next_batch(10));
            assert_eq!(r.logical_rows, results[i]);
            if limits[i] == 0 {
                assert_eq!(r.physical_columns.rows_len(), 0);
            } else {
                assert_eq!(r.physical_columns.rows_len(), 10);
            }
            assert!(r.is_drained.unwrap().stop());
            if limits[i] <= 10 {
                assert_eq!(exec.remaining_rows, 0);
            }
            assert!(exec.executed_in_rank_limit_for_test);
        }
    }

    #[test]
    fn test_rank_limit_several_chunks() {
        let limits = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 100];
        let results = vec![
            vec![],
            vec![0],
            vec![0, 1],
            vec![0, 1, 0],
            vec![0, 1, 0, 1, 2, 3],
            vec![0, 1, 0, 1, 2, 3], // 5
            vec![0, 1, 0, 1, 2, 3],
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0],
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0],
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0],
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0], // 10
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0],
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0],
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0, 1, 2],
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0, 1, 2],
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0, 1, 2, 0], // 15
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0, 1, 2, 0, 0, 1],
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0, 1, 2, 0, 0, 1],
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0, 1, 2, 0, 0, 1, 2],
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0, 1, 2, 0, 0, 1, 2, 0, 1],
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0, 1, 2, 0, 0, 1, 2, 0, 1], // 20
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0, 1, 2, 0, 0, 1, 2, 0, 1],
        ];

        for i in 0..limits.len() {
            let src_exec = MockExecutor::new(
                vec![
                    FieldTypeBuilder::new().tp(FieldTypeTp::VarString).collation(Collation::Binary).build(),
                    FieldTypeBuilder::new().tp(FieldTypeTp::VarString).collation(Collation::Binary).build(),
                ],
                vec![
                    BatchExecuteResult {
                        physical_columns: LazyBatchColumnVec::from(vec![
                            VectorValue::Bytes(vec![
                                Some(b"val1".to_vec()),
                                Some(b"val2".to_vec()),
                            ].into()),
                            VectorValue::Bytes(vec![
                                Some(b"group1".to_vec()),
                                Some(b"group2".to_vec()),
                            ].into()),
                        ]),
                        logical_rows: vec![0, 1],
                        warnings: EvalWarnings::default(),
                        is_drained: Ok(BatchExecIsDrain::Remain),
                    },
                    BatchExecuteResult {
                        physical_columns: LazyBatchColumnVec::from(vec![
                            VectorValue::Bytes(vec![
                                Some(b"val3".to_vec()),
                                Some(b"val4".to_vec()),
                                Some(b"val5".to_vec()),
                                Some(b"val6".to_vec()),
                                Some(b"val7".to_vec()),
                            ].into()),
                            VectorValue::Bytes(vec![
                                Some(b"group3".to_vec()),
                                Some(b"group4".to_vec()),
                                Some(b"group4".to_vec()),
                                Some(b"group4".to_vec()),
                                Some(b"group5".to_vec()),
                            ].into()),
                        ]),
                        logical_rows: vec![0, 1, 2, 3, 4],
                        warnings: EvalWarnings::default(),
                        is_drained: Ok(BatchExecIsDrain::Remain),
                    },
                    BatchExecuteResult {
                        physical_columns: LazyBatchColumnVec::from(vec![
                            VectorValue::Bytes(vec![
                                Some(b"val8".to_vec()),
                                Some(b"val9".to_vec()),
                                Some(b"val10".to_vec()),
                            ].into()),
                            VectorValue::Bytes(vec![
                                Some(b"group5".to_vec()),
                                Some(b"group5".to_vec()),
                                Some(b"group5".to_vec()),
                            ].into()),
                        ]),
                        logical_rows: vec![0, 1, 2],
                        warnings: EvalWarnings::default(),
                        is_drained: Ok(BatchExecIsDrain::Remain),
                    },
                    BatchExecuteResult {
                        physical_columns: LazyBatchColumnVec::from(vec![
                            VectorValue::Bytes(vec![
                                Some(b"val11".to_vec()),
                            ].into()),
                            VectorValue::Bytes(vec![
                                Some(b"group5".to_vec()),
                            ].into()),
                        ]),
                        logical_rows: vec![0],
                        warnings: EvalWarnings::default(),
                        is_drained: Ok(BatchExecIsDrain::Remain),
                    },
                    BatchExecuteResult {
                        physical_columns: LazyBatchColumnVec::from(vec![
                            VectorValue::Bytes(vec![
                                Some(b"val12".to_vec()),
                                Some(b"val13".to_vec()),
                                Some(b"val14".to_vec()),
                            ].into()),
                            VectorValue::Bytes(vec![
                                Some(b"group5".to_vec()),
                                Some(b"group6".to_vec()),
                                Some(b"group6".to_vec()),
                            ].into()),
                        ]),
                        logical_rows: vec![0, 1, 2],
                        warnings: EvalWarnings::default(),
                        is_drained: Ok(BatchExecIsDrain::Remain),
                    },
                    BatchExecuteResult {
                        physical_columns: LazyBatchColumnVec::from(vec![
                            VectorValue::Bytes(vec![
                                Some(b"val15".to_vec()),
                            ].into()),
                            VectorValue::Bytes(vec![
                                Some(b"group7".to_vec()),
                            ].into()),
                        ]),
                        logical_rows: vec![0],
                        warnings: EvalWarnings::default(),
                        is_drained: Ok(BatchExecIsDrain::Remain),
                    },
                    BatchExecuteResult {
                        physical_columns: LazyBatchColumnVec::from(vec![
                            VectorValue::Bytes(vec![
                                Some(b"val16".to_vec()),
                                Some(b"val17".to_vec()),
                                Some(b"val18".to_vec()),
                            ].into()),
                            VectorValue::Bytes(vec![
                                Some(b"group8".to_vec()),
                                Some(b"group8".to_vec()),
                                Some(b"group9".to_vec()),
                            ].into()),
                        ]),
                        logical_rows: vec![0, 1, 2],
                        warnings: EvalWarnings::default(),
                        is_drained: Ok(BatchExecIsDrain::Remain),
                    },
                    BatchExecuteResult {
                        physical_columns: LazyBatchColumnVec::from(vec![
                            VectorValue::Bytes(vec![
                                Some(b"val19".to_vec()),
                                Some(b"val20".to_vec()),
                            ].into()),
                            VectorValue::Bytes(vec![
                                Some(b"group10".to_vec()),
                                Some(b"group10".to_vec()),
                            ].into()),
                        ]),
                        logical_rows: vec![0, 1],
                        warnings: EvalWarnings::default(),
                        is_drained: Ok(BatchExecIsDrain::Drain),
                    },
                ],
            );

            let config = Arc::new(EvalConfig::default());
            let truncate_key_exp = 
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(1)
                    .build_for_test();
            let mut exec = BatchLimitExecutor::new_rank_limit_for_test(src_exec, limits[i], true, config, vec![truncate_key_exp]).unwrap();

            let mut actual_logical_rows: Vec<usize> = vec![];

            loop {
                let r = block_on(exec.next_batch(10));
                actual_logical_rows.extend(r.logical_rows);
                if r.is_drained.unwrap().stop() {
                    break;
                }
            }

            assert_eq!(results[i], actual_logical_rows);
            assert!(exec.executed_in_rank_limit_for_test);
        }
    }

    #[test]
    fn test_rank_limit_with_ci_collation() {
        let limits = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 100];
        let results = vec![
            vec![],
            vec![0],
            vec![0, 1],
            vec![0, 1, 0],
            vec![0, 1, 0, 1, 2, 3],
            vec![0, 1, 0, 1, 2, 3], // 5
            vec![0, 1, 0, 1, 2, 3],
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0],
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0],
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0],
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0], // 10
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0],
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0],
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0, 1, 2],
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0, 1, 2],
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0, 1, 2, 0], // 15
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0, 1, 2, 0, 0, 1],
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0, 1, 2, 0, 0, 1],
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0, 1, 2, 0, 0, 1, 2],
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0, 1, 2, 0, 0, 1, 2, 0, 1],
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0, 1, 2, 0, 0, 1, 2, 0, 1], // 20
            vec![0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 0, 0, 1, 2, 0, 0, 1, 2, 0, 1],
        ];

        for i in 0..limits.len() {
            let src_exec = MockExecutor::new(
                vec![
                    FieldTypeBuilder::new().tp(FieldTypeTp::VarString).collation(Collation::Utf8Mb4GeneralCi).build(),
                    FieldTypeBuilder::new().tp(FieldTypeTp::VarString).collation(Collation::Utf8Mb4GeneralCi).build(),
                ],
                    vec![
                        BatchExecuteResult {
                            physical_columns: LazyBatchColumnVec::from(vec![
                                VectorValue::Bytes(vec![
                                    Some(b"val1".to_vec()),
                                    Some(b"val2".to_vec()),
                                ].into()),
                                VectorValue::Bytes(vec![
                                    Some(b"group1".to_vec()),
                                    Some(b"group2".to_vec()),
                                ].into()),
                            ]),
                            logical_rows: vec![0, 1],
                            warnings: EvalWarnings::default(),
                            is_drained: Ok(BatchExecIsDrain::Remain),
                        },
                        BatchExecuteResult {
                            physical_columns: LazyBatchColumnVec::from(vec![
                                VectorValue::Bytes(vec![
                                    Some(b"val3".to_vec()),
                                    Some(b"val4".to_vec()),
                                    Some(b"val5".to_vec()),
                                    Some(b"val6".to_vec()),
                                    Some(b"val7".to_vec()),
                                ].into()),
                                VectorValue::Bytes(vec![
                                    Some(b"group3".to_vec()),
                                    Some(b"group4".to_vec()),
                                    Some(b"groUp4".to_vec()),
                                    Some(b"gRoup4".to_vec()),
                                    Some(b"group5".to_vec()),
                                ].into()),
                            ]),
                            logical_rows: vec![0, 1, 2, 3, 4],
                            warnings: EvalWarnings::default(),
                            is_drained: Ok(BatchExecIsDrain::Remain),
                        },
                        BatchExecuteResult {
                            physical_columns: LazyBatchColumnVec::from(vec![
                                VectorValue::Bytes(vec![
                                    Some(b"val8".to_vec()),
                                    Some(b"val9".to_vec()),
                                    Some(b"val10".to_vec()),
                                ].into()),
                                VectorValue::Bytes(vec![
                                    Some(b"group5".to_vec()),
                                    Some(b"groUP5".to_vec()),
                                    Some(b"group5".to_vec()),
                                ].into()),
                            ]),
                            logical_rows: vec![0, 1, 2],
                            warnings: EvalWarnings::default(),
                            is_drained: Ok(BatchExecIsDrain::Remain),
                        },
                        BatchExecuteResult {
                            physical_columns: LazyBatchColumnVec::from(vec![
                                VectorValue::Bytes(vec![
                                    Some(b"val11".to_vec()),
                                ].into()),
                                VectorValue::Bytes(vec![
                                    Some(b"gRoup5".to_vec()),
                                ].into()),
                            ]),
                            logical_rows: vec![0],
                            warnings: EvalWarnings::default(),
                            is_drained: Ok(BatchExecIsDrain::Remain),
                        },
                        BatchExecuteResult {
                            physical_columns: LazyBatchColumnVec::from(vec![
                                VectorValue::Bytes(vec![
                                    Some(b"val12".to_vec()),
                                    Some(b"val13".to_vec()),
                                    Some(b"val14".to_vec()),
                                ].into()),
                                VectorValue::Bytes(vec![
                                    Some(b"group5".to_vec()),
                                    Some(b"group6".to_vec()),
                                    Some(b"GROUP6".to_vec()),
                                ].into()),
                            ]),
                            logical_rows: vec![0, 1, 2],
                            warnings: EvalWarnings::default(),
                            is_drained: Ok(BatchExecIsDrain::Remain),
                        },
                        BatchExecuteResult {
                            physical_columns: LazyBatchColumnVec::from(vec![
                                VectorValue::Bytes(vec![
                                    Some(b"val15".to_vec()),
                                ].into()),
                                VectorValue::Bytes(vec![
                                    Some(b"grOup7".to_vec()),
                                ].into()),
                            ]),
                            logical_rows: vec![0],
                            warnings: EvalWarnings::default(),
                            is_drained: Ok(BatchExecIsDrain::Remain),
                        },
                        BatchExecuteResult {
                            physical_columns: LazyBatchColumnVec::from(vec![
                                VectorValue::Bytes(vec![
                                    Some(b"val16".to_vec()),
                                    Some(b"val17".to_vec()),
                                    Some(b"val18".to_vec()),
                                ].into()),
                                VectorValue::Bytes(vec![
                                    Some(b"GROUP8".to_vec()),
                                    Some(b"group8".to_vec()),
                                    Some(b"group9".to_vec()),
                                ].into()),
                            ]),
                            logical_rows: vec![0, 1, 2],
                            warnings: EvalWarnings::default(),
                            is_drained: Ok(BatchExecIsDrain::Remain),
                        },
                        BatchExecuteResult {
                            physical_columns: LazyBatchColumnVec::from(vec![
                                VectorValue::Bytes(vec![
                                    Some(b"val19".to_vec()),
                                    Some(b"val20".to_vec()),
                                ].into()),
                                VectorValue::Bytes(vec![
                                    Some(b"gRoup10".to_vec()),
                                    Some(b"grouP10".to_vec()),
                                ].into()),
                            ]),
                            logical_rows: vec![0, 1],
                            warnings: EvalWarnings::default(),
                            is_drained: Ok(BatchExecIsDrain::Drain),
                        },
                    ],
            );
            
            let config = Arc::new(EvalConfig::default());
            let truncate_key_exp = 
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(1)
                    .build_for_test();
            let mut exec = BatchLimitExecutor::new_rank_limit_for_test(src_exec, limits[i], true, config, vec![truncate_key_exp]).unwrap();

            let mut actual_logical_rows: Vec<usize> = vec![];

            loop {
                let r = block_on(exec.next_batch(10));
                actual_logical_rows.extend(r.logical_rows);
                if r.is_drained.unwrap().stop() {
                    break;
                }
            }

            assert_eq!(results[i], actual_logical_rows);
            assert!(exec.executed_in_rank_limit_for_test);
        }
    }
}
