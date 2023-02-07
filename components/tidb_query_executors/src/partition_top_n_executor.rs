// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;
use tidb_query_common::{storage::IntervalRange, Result};
use tidb_query_datatype::{
    codec::{batch::LazyBatchColumnVec, data_type::BATCH_MAX_SIZE},
    expr::{EvalConfig, EvalContext, EvalWarnings},
};
use tidb_query_expr::{RpnExpression, RpnExpressionBuilder, RpnStackNode};
use tipb::{Expr, FieldType};

use crate::{
    interface::*,
    util::{
        ensure_columns_decoded, eval_exprs_decoded_no_lifetime,
        top_n_heap::{HeapItemSourceData, HeapItemUnsafe, TopNHeap},
    },
};

pub struct BatchPartitionTopNExecutor<Src: BatchExecutor> {
    heap: TopNHeap,

    #[allow(clippy::box_collection)]
    eval_columns_buffer_unsafe: Box<Vec<RpnStackNode<'static>>>,

    /// The data should be sorted by the partition expression.
    /// But if not, the result is still correct after the second-stage topn.
    partition_exprs: Box<[RpnExpression]>,
    partition_exprs_field_type: Box<[FieldType]>,
    /// dummy value, just for convenience.
    partition_is_desc: Box<[bool]>,

    last_partition_key: Option<HeapItemUnsafe>,

    order_exprs: Box<[RpnExpression]>,
    /// This field stores the field type of the results evaluated by the exprs
    /// in `order_exprs`.
    order_exprs_field_type: Box<[FieldType]>,

    /// Whether or not it is descending order for each order by column.
    order_is_desc: Box<[bool]>,

    n: usize,

    context: EvalContext,
    src: Src,
}

impl<Src: BatchExecutor> BatchPartitionTopNExecutor<Src> {
    #[cfg(test)]
    pub fn new_for_test(
        src: Src,
        order_exprs: Vec<RpnExpression>,
        order_is_desc: Vec<bool>,
        partition_exprs: Vec<RpnExpression>,
        n: usize,
    ) -> Self {
        assert_eq!(order_exprs.len(), order_is_desc.len());

        let order_exprs_field_type: Vec<FieldType> = order_exprs
            .iter()
            .map(|expr| expr.ret_field_type(src.schema()).clone())
            .collect();

        let partition_exprs_field_type: Vec<FieldType> = partition_exprs
            .iter()
            .map(|expr| expr.ret_field_type(src.schema()).clone())
            .collect();

        Self {
            heap: TopNHeap::new(n),
            eval_columns_buffer_unsafe: Box::<Vec<_>>::default(),
            partition_is_desc: vec![false; partition_exprs.len()].into_boxed_slice(),
            partition_exprs: partition_exprs.into_boxed_slice(),
            partition_exprs_field_type: partition_exprs_field_type.into_boxed_slice(),
            last_partition_key: None,
            order_exprs: order_exprs.into_boxed_slice(),
            order_exprs_field_type: order_exprs_field_type.into_boxed_slice(),
            order_is_desc: order_is_desc.into_boxed_slice(),
            n,

            context: EvalContext::default(),
            src,
        }
    }

    #[cfg(test)]
    pub fn new_for_test_with_config(
        config: Arc<EvalConfig>,
        src: Src,
        order_exprs: Vec<RpnExpression>,
        order_is_desc: Vec<bool>,
        partition_exprs: Vec<RpnExpression>,
        n: usize,
    ) -> Self {
        assert_eq!(order_exprs.len(), order_is_desc.len());

        let order_exprs_field_type: Vec<FieldType> = order_exprs
            .iter()
            .map(|expr| expr.ret_field_type(src.schema()).clone())
            .collect();

        let partition_exprs_field_type: Vec<FieldType> = partition_exprs
            .iter()
            .map(|expr| expr.ret_field_type(src.schema()).clone())
            .collect();

        Self {
            heap: TopNHeap::new(n),
            eval_columns_buffer_unsafe: Box::<Vec<_>>::default(),
            partition_is_desc: vec![false; partition_exprs.len()].into_boxed_slice(),
            partition_exprs: partition_exprs.into_boxed_slice(),
            partition_exprs_field_type: partition_exprs_field_type.into_boxed_slice(),
            last_partition_key: None,
            order_exprs: order_exprs.into_boxed_slice(),
            order_exprs_field_type: order_exprs_field_type.into_boxed_slice(),
            order_is_desc: order_is_desc.into_boxed_slice(),
            n,

            context: EvalContext::new(config),
            src,
        }
    }

    pub fn new(
        config: Arc<EvalConfig>,
        src: Src,
        partition_exprs_def: Vec<Expr>,
        order_exprs_def: Vec<Expr>,
        order_is_desc: Vec<bool>,
        n: usize,
    ) -> Result<Self> {
        assert_eq!(order_exprs_def.len(), order_is_desc.len());

        let mut ctx = EvalContext::new(config.clone());

        let mut order_exprs: Vec<RpnExpression> = Vec::with_capacity(order_exprs_def.len());
        for def in order_exprs_def {
            order_exprs.push(RpnExpressionBuilder::build_from_expr_tree(
                def,
                &mut ctx,
                src.schema().len(),
            )?);
        }
        let order_exprs_field_type: Vec<FieldType> = order_exprs
            .iter()
            .map(|expr| expr.ret_field_type(src.schema()).clone())
            .collect();

        let mut partition_exprs: Vec<RpnExpression> = Vec::with_capacity(partition_exprs_def.len());
        for def in partition_exprs_def {
            partition_exprs.push(RpnExpressionBuilder::build_from_expr_tree(
                def,
                &mut ctx,
                src.schema().len(),
            )?);
        }
        let partition_exprs_field_type: Vec<FieldType> = partition_exprs
            .iter()
            .map(|expr| expr.ret_field_type(src.schema()).clone())
            .collect();

        Ok(Self {
            // Simply large enough to avoid repeated allocations
            heap: TopNHeap::new(n),
            eval_columns_buffer_unsafe: Box::new(Vec::with_capacity(512)),
            partition_is_desc: vec![false; partition_exprs.len()].into_boxed_slice(),
            partition_exprs: partition_exprs.into_boxed_slice(),
            partition_exprs_field_type: partition_exprs_field_type.into_boxed_slice(),
            order_exprs: order_exprs.into_boxed_slice(),
            order_exprs_field_type: order_exprs_field_type.into_boxed_slice(),
            order_is_desc: order_is_desc.into_boxed_slice(),
            n,
            context: EvalContext::new(config),
            src,
            last_partition_key: None,
        })
    }

    // Check whether the partition key of the this row is equal to the saved
    // partition key. If yes, return true. Else, update saved partition key,
    // and return false.
    fn check_partition_equal_or_update(&mut self, current: HeapItemUnsafe) -> Result<bool> {
        if let Some(last_partition_key) = &self.last_partition_key {
            if last_partition_key == &current {
                return Ok(true);
            }
        }
        self.last_partition_key = Some(current);
        Ok(false)
    }

    #[inline]
    async fn handle_next_batch(&mut self) -> Result<(LazyBatchColumnVec, bool)> {
        let mut result = LazyBatchColumnVec::empty();
        let src_result = self.src.next_batch(BATCH_MAX_SIZE).await;
        self.context.warnings = src_result.warnings;
        let src_is_drained = src_result.is_drained?;

        let (mut physical_columns, logical_rows) =
            (src_result.physical_columns, src_result.logical_rows);

        if !logical_rows.is_empty() {
            ensure_columns_decoded(
                &mut self.context,
                &self.order_exprs,
                self.src.schema(),
                &mut physical_columns,
                &logical_rows,
            )?;

            let pinned_source_data = Arc::new(HeapItemSourceData {
                physical_columns,
                logical_rows,
            });

            let order_eval_offset = self.eval_columns_buffer_unsafe.len();
            unsafe {
                eval_exprs_decoded_no_lifetime(
                    &mut self.context,
                    &self.order_exprs,
                    self.src.schema(),
                    &pinned_source_data.physical_columns,
                    &pinned_source_data.logical_rows,
                    &mut self.eval_columns_buffer_unsafe,
                )?;
            }
            // todo: optimize memory use of this.
            let partition_eval_offset = self.eval_columns_buffer_unsafe.len();
            unsafe {
                eval_exprs_decoded_no_lifetime(
                    &mut self.context,
                    &self.partition_exprs,
                    self.src.schema(),
                    &pinned_source_data.physical_columns,
                    &pinned_source_data.logical_rows,
                    &mut self.eval_columns_buffer_unsafe,
                )?;
            }
            // dbg!(&self.eval_columns_buffer_unsafe);
            // todo: optimize the memory usage of this, don't need so many same information
            // in items. Maybe we can import a Heap with customer comparator.
            for logical_row_index in 0..pinned_source_data.logical_rows.len() {
                let partition_key = HeapItemUnsafe {
                    order_is_desc_ptr: (*self.partition_is_desc).into(), // just a dummy value
                    order_exprs_field_type_ptr: (*self.partition_exprs_field_type).into(),
                    source_data: pinned_source_data.clone(),
                    eval_columns_buffer_ptr: self.eval_columns_buffer_unsafe.as_ref().into(),
                    eval_columns_offset: partition_eval_offset,
                    logical_row_index,
                };

                if !self.check_partition_equal_or_update(partition_key)? {
                    self.heap.take_all_append_to(&mut result);
                    self.heap = TopNHeap::new(self.n);
                }

                let row = HeapItemUnsafe {
                    order_is_desc_ptr: (*self.order_is_desc).into(),
                    order_exprs_field_type_ptr: (*self.order_exprs_field_type).into(),
                    source_data: pinned_source_data.clone(),
                    eval_columns_buffer_ptr: self.eval_columns_buffer_unsafe.as_ref().into(),
                    eval_columns_offset: order_eval_offset,
                    logical_row_index,
                };
                self.heap.add_row(row)?;
            }
        }
        if src_is_drained {
            self.heap.take_all_append_to(&mut result);
        }

        Ok((result, src_is_drained))
    }
}

/// todo: review this.
/// All `NonNull` pointers in `BatchPartitionTopNExecutor` cannot be accessed
/// out of the struct and `BatchPartitionTopNExecutor` doesn't leak the pointers
/// to other threads. Therefore, with those `NonNull` pointers,
/// BatchPartitionTopNExecutor still remains `Send`.
unsafe impl<Src: BatchExecutor> Send for BatchPartitionTopNExecutor<Src> {}

#[async_trait]
impl<Src: BatchExecutor> BatchExecutor for BatchPartitionTopNExecutor<Src> {
    type StorageStats = Src::StorageStats;

    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.src.schema()
    }

    #[inline]
    /// Implementation of BatchExecutor::next_batch
    /// Memory Control Analysis:
    /// 1. if n > paging_size(1024), this operator won't do anything and just
    /// return data to upstream. So we can think n is less than or equal to
    /// paging_size.
    /// 2. The worst case is that there is already n rows in heap, and first
    /// row of src_result has different partition with rows in heap. So heap
    /// will be flushed. And the last row of src_result has another different
    /// partition with the first two. So heap will be flushed again.
    /// In this case, there can be 2*n-1 rows in the result, which may be larger
    /// than paging_size.
    /// todo: find a good solution to limit it up to paging_size.
    /// baseline: limit n up to paging_size/2
    async fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        if self.n == 0 {
            return BatchExecuteResult {
                physical_columns: LazyBatchColumnVec::empty(),
                logical_rows: Vec::new(),
                warnings: EvalWarnings::default(),
                is_drained: Ok(true),
            };
        }

        // limit middle memory by paging_size.
        if let Some(paging_size) = self.context.cfg.paging_size {
            if self.n * 2 > paging_size as usize {
                return self.src.next_batch(scan_rows).await;
            }
        }

        let result = self.handle_next_batch().await;

        match result {
            Err(e) => BatchExecuteResult {
                physical_columns: LazyBatchColumnVec::empty(),
                logical_rows: Vec::new(),
                warnings: self.context.take_warnings(),
                is_drained: Err(e),
            },
            Ok((logical_columns, is_drained)) => {
                let logical_rows = (0..logical_columns.rows_len()).collect();
                BatchExecuteResult {
                    physical_columns: logical_columns,
                    logical_rows,
                    warnings: self.context.take_warnings(),
                    is_drained: Ok(is_drained),
                }
            }
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
        builder::FieldTypeBuilder,
        codec::{batch::LazyBatchColumnVec, data_type::*},
        expr::EvalWarnings,
        Collation, FieldTypeFlag, FieldTypeTp,
    };
    use tidb_query_expr::RpnExpressionBuilder;

    use super::*;
    use crate::util::mock_executor::MockExecutor;

    #[test]
    fn test_top_0() {
        let src_exec = MockExecutor::new(
            vec![FieldTypeTp::Double.into(), FieldTypeTp::Double.into()],
            vec![BatchExecuteResult {
                physical_columns: LazyBatchColumnVec::from(vec![
                    VectorValue::Real(vec![None, Real::new(7.0).ok(), None, None].into()),
                    VectorValue::Real(vec![None, Real::new(7.0).ok(), None, None].into()),
                ]),
                logical_rows: (0..1).collect(),
                warnings: EvalWarnings::default(),
                is_drained: Ok(true),
            }],
        );

        let mut exec = BatchPartitionTopNExecutor::new_for_test(
            src_exec,
            vec![
                RpnExpressionBuilder::new_for_test()
                    .push_constant_for_test(1)
                    .build_for_test(),
            ],
            vec![false],
            vec![
                RpnExpressionBuilder::new_for_test()
                    .push_constant_for_test(1)
                    .build_for_test(),
            ],
            0,
        );

        let r = block_on(exec.next_batch(1));
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(r.is_drained.unwrap());
    }

    #[test]
    fn test_constant_partition() {
        let src_exec = MockExecutor::new(
            vec![FieldTypeTp::Double.into(), FieldTypeTp::Double.into()],
            vec![BatchExecuteResult {
                physical_columns: LazyBatchColumnVec::from(vec![
                    VectorValue::Real(
                        vec![
                            Real::new(1.0).ok(),
                            Real::new(2.0).ok(),
                            Real::new(3.0).ok(),
                            Real::new(4.0).ok(),
                        ]
                        .into(),
                    ),
                    VectorValue::Real(
                        vec![
                            Real::new(5.0).ok(),
                            Real::new(6.0).ok(),
                            Real::new(7.0).ok(),
                            Real::new(8.0).ok(),
                        ]
                        .into(),
                    ),
                ]),
                logical_rows: (0..4).collect(),
                warnings: EvalWarnings::default(),
                is_drained: Ok(true),
            }],
        );

        let mut exec = BatchPartitionTopNExecutor::new_for_test(
            src_exec,
            vec![
                RpnExpressionBuilder::new_for_test()
                    .push_constant_for_test(1)
                    .build_for_test(),
            ],
            vec![false],
            vec![
                RpnExpressionBuilder::new_for_test()
                    .push_constant_for_test(1)
                    .build_for_test(),
            ],
            2,
        );

        let r = block_on(exec.next_batch(1));
        assert_eq!(&r.logical_rows, &[0, 1]);
        assert_eq!(r.physical_columns.rows_len(), 2);
        assert_eq!(r.physical_columns.columns_len(), 2);
        assert_eq!(
            r.physical_columns[0].decoded().to_real_vec(),
            &[Real::new(2.0).ok(), Real::new(1.0).ok(),]
        );
        assert_eq!(
            r.physical_columns[1].decoded().to_real_vec(),
            &[Real::new(6.0).ok(), Real::new(5.0).ok(),]
        );
        assert!(r.is_drained.unwrap());
    }

    #[test]
    fn test_multiple_and_null_part_key() {
        let src_exec = MockExecutor::new(
            vec![FieldTypeTp::Long.into(), FieldTypeTp::Long.into()],
            vec![BatchExecuteResult {
                physical_columns: LazyBatchColumnVec::from(vec![
                    VectorValue::Int(
                        vec![
                            Some(1),
                            Some(1),
                            Some(1),
                            None,
                            None,
                            None,
                            Some(2),
                            Some(2),
                            Some(2),
                        ]
                        .into(),
                    ),
                    VectorValue::Int(
                        vec![
                            Some(1),
                            Some(1),
                            None,
                            None,
                            None,
                            Some(2),
                            Some(1),
                            Some(1),
                            None,
                        ]
                        .into(),
                    ),
                ]),
                logical_rows: (0..9).collect(),
                warnings: EvalWarnings::default(),
                is_drained: Ok(true),
            }],
        );

        let mut exec = BatchPartitionTopNExecutor::new_for_test(
            src_exec,
            vec![],
            vec![],
            vec![
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(0)
                    .build_for_test(),
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(1)
                    .build_for_test(),
            ],
            1,
        );

        let r = block_on(exec.next_batch(1));
        // dbg!(r.physical_columns);
        assert_eq!(&r.logical_rows, &[0, 1, 2, 3, 4, 5]);
        assert_eq!(r.physical_columns.rows_len(), 6);
        assert_eq!(r.physical_columns.columns_len(), 2);
        assert_eq!(
            r.physical_columns[0].decoded().to_int_vec(),
            &[Some(1), Some(1), None, None, Some(2), Some(2)]
        );
        assert_eq!(
            r.physical_columns[1].decoded().to_int_vec(),
            &[Some(1), None, None, Some(2), Some(1), None]
        );
        assert!(r.is_drained.unwrap());
    }

    #[test]
    fn test_unordered_key() {
        let src_exec = MockExecutor::new(
            vec![FieldTypeTp::Long.into(), FieldTypeTp::Double.into()],
            vec![BatchExecuteResult {
                physical_columns: LazyBatchColumnVec::from(vec![
                    VectorValue::Int(vec![Some(1), Some(1), Some(2), Some(1)].into()),
                    VectorValue::Real(
                        vec![
                            Real::new(5.0).ok(),
                            None,
                            Real::new(7.0).ok(),
                            Real::new(4.0).ok(),
                        ]
                        .into(),
                    ),
                ]),
                logical_rows: (0..4).collect(),
                warnings: EvalWarnings::default(),
                is_drained: Ok(true),
            }],
        );

        let mut exec = BatchPartitionTopNExecutor::new_for_test(
            src_exec,
            vec![
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(1)
                    .build_for_test(),
            ],
            vec![false],
            vec![
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(0)
                    .build_for_test(),
            ],
            1,
        );

        let r = block_on(exec.next_batch(1));
        // dbg!(r.physical_columns);
        assert_eq!(&r.logical_rows, &[0, 1, 2]);
        assert_eq!(r.physical_columns.rows_len(), 3);
        assert_eq!(r.physical_columns.columns_len(), 2);
        assert_eq!(
            r.physical_columns[0].decoded().to_int_vec(),
            &[Some(1), Some(2), Some(1)]
        );
        assert_eq!(
            r.physical_columns[1].decoded().to_real_vec(),
            &[None, Real::new(7.0).ok(), Real::new(4.0).ok()]
        );
        assert!(r.is_drained.unwrap());
    }

    /// Builds an executor that will return these data:
    ///
    /// ```text
    /// == Schema ==
    /// Col0 (LongLong(Unsigned))      Col1(LongLong[UnSigned])       Col2(LongLong[Signed])
    /// == Call #1 ==
    /// 1                              18,446,744,073,709,551,615     -3
    /// 1                              NULL                           NULL
    /// 1                              18,446,744,073,709,551,613     -1
    /// 1                              2023                           2024
    /// 1                              2000                           2000
    /// == Call #2 ==
    /// == Call #3 ==
    /// 2                              9,223,372,036,854,775,807      9,223,372,036,854,775,807
    /// 2                              300                            300
    /// 2                              9,223,372,036,854,775,808      -9,223,372,036,854,775,808
    /// 2                              NULL                           NULL                    
    /// 3                              NULL                           NULL    
    /// == Call #4 ==
    /// (drained)                      (drained)                    (drained)
    fn make_full_batch() -> MockExecutor {
        MockExecutor::new(
            vec![
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::LongLong)
                    .flag(FieldTypeFlag::UNSIGNED)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::LongLong)
                    .flag(FieldTypeFlag::UNSIGNED)
                    .into(),
                FieldTypeTp::LongLong.into(),
            ],
            vec![
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(vec![Some(1), Some(1), Some(1), Some(1), Some(1)].into()),
                        VectorValue::Int(
                            vec![
                                Some(18_446_744_073_709_551_615_u64 as i64),
                                None,
                                Some(18_446_744_073_709_551_613_u64 as i64),
                                Some(2023),
                                Some(2000),
                            ]
                            .into(),
                        ),
                        VectorValue::Int(
                            vec![Some(-3), None, Some(-1), Some(2024), Some(2000)].into(),
                        ),
                    ]),
                    logical_rows: vec![0, 1, 2, 3, 4],
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
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(vec![Some(2), Some(2), Some(2), Some(2), Some(3)].into()),
                        VectorValue::Int(
                            vec![
                                Some(9_223_372_036_854_775_807_u64 as i64),
                                Some(300),
                                Some(9_223_372_036_854_775_808_u64 as i64),
                                None,
                                None,
                            ]
                            .into(),
                        ),
                        VectorValue::Int(
                            vec![
                                Some(9_223_372_036_854_775_807_u64 as i64),
                                Some(300),
                                Some(-9_223_372_036_854_775_808),
                                None,
                                None,
                            ]
                            .into(),
                        ),
                    ]),
                    logical_rows: vec![0, 1, 2, 3, 4],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::empty(),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        )
    }

    #[test]
    fn test_small_n() {
        let mut config = EvalConfig::default();
        config.paging_size = Some(10);
        let config = Arc::new(config);
        let src_exec = make_full_batch();
        let mut exec = BatchPartitionTopNExecutor::new_for_test_with_config(
            config,
            src_exec,
            vec![
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(1)
                    .build_for_test(),
            ],
            vec![false],
            vec![
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(0)
                    .build_for_test(),
            ],
            2,
        );

        let r = block_on(exec.next_batch(1));
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = block_on(exec.next_batch(1));
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = block_on(exec.next_batch(1));
        assert_eq!(&r.logical_rows, &[0, 1, 2, 3]);
        assert_eq!(r.physical_columns.rows_len(), 4);
        assert_eq!(r.physical_columns.columns_len(), 3);
        assert!(!r.is_drained.unwrap());
        assert_eq!(
            r.physical_columns[0].decoded().to_int_vec(),
            &[Some(1), Some(1), Some(2), Some(2)]
        );

        let r = block_on(exec.next_batch(1));
        assert_eq!(&r.logical_rows, &[0]);
        assert_eq!(r.physical_columns.rows_len(), 1);
        assert!(r.is_drained.unwrap());
        assert_eq!(
            r.physical_columns[0].decoded().to_int_vec(),
            &[Some(3 as i64)]
        );
    }

    #[test]
    fn test_no_order_key() {
        let mut config = EvalConfig::default();
        config.paging_size = Some(10);
        let config = Arc::new(config);
        let src_exec = make_full_batch();
        let mut exec = BatchPartitionTopNExecutor::new_for_test_with_config(
            config,
            src_exec,
            vec![],
            vec![],
            vec![
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(0)
                    .build_for_test(),
            ],
            2,
        );

        let r = block_on(exec.next_batch(1));
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = block_on(exec.next_batch(1));
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = block_on(exec.next_batch(1));
        assert_eq!(&r.logical_rows, &[0, 1, 2, 3]);
        assert_eq!(r.physical_columns.rows_len(), 4);
        assert_eq!(r.physical_columns.columns_len(), 3);
        assert!(!r.is_drained.unwrap());
        assert_eq!(
            r.physical_columns[0].decoded().to_int_vec(),
            &[Some(1), Some(1), Some(2), Some(2)]
        );

        let r = block_on(exec.next_batch(1));
        assert_eq!(&r.logical_rows, &[0]);
        assert_eq!(r.physical_columns.rows_len(), 1);
        assert!(r.is_drained.unwrap());
        assert_eq!(r.physical_columns[0].decoded().to_int_vec(), &[Some(3)]);
    }

    #[test]
    fn test_paging_limit_normal_n() {
        let mut config = EvalConfig::default();
        config.paging_size = Some(10);
        let config = Arc::new(config);
        let src_exec = make_full_batch();
        let mut exec = BatchPartitionTopNExecutor::new_for_test_with_config(
            config,
            src_exec,
            vec![
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(1)
                    .build_for_test(),
            ],
            vec![false],
            vec![
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(0)
                    .build_for_test(),
            ],
            5,
        );

        let r = block_on(exec.next_batch(1));
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = block_on(exec.next_batch(1));
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = block_on(exec.next_batch(1));
        assert_eq!(&r.logical_rows, &[0, 1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(r.physical_columns.rows_len(), 9);
        assert_eq!(r.physical_columns.columns_len(), 3);
        assert!(!r.is_drained.unwrap());

        let r = block_on(exec.next_batch(1));
        assert_eq!(&r.logical_rows, &[0]);
        assert_eq!(r.physical_columns.rows_len(), 1);
        assert!(r.is_drained.unwrap());
    }

    #[test]
    fn test_paging_limit_oversize_n() {
        let mut config = EvalConfig::default();
        config.paging_size = Some(9);
        let config = Arc::new(config);
        let src_exec = make_full_batch();
        let mut exec = BatchPartitionTopNExecutor::new_for_test_with_config(
            config,
            src_exec,
            vec![
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(1)
                    .build_for_test(),
            ],
            vec![false],
            vec![
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(0)
                    .build_for_test(),
            ],
            5,
        );

        let r = block_on(exec.next_batch(1));
        assert_eq!(&r.logical_rows, &[0, 1, 2, 3, 4]);
        assert_eq!(r.physical_columns.rows_len(), 5);
        assert_eq!(r.physical_columns.columns_len(), 3);
        assert!(!r.is_drained.unwrap());

        let r = block_on(exec.next_batch(1));
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = block_on(exec.next_batch(1));
        assert_eq!(&r.logical_rows, &[0, 1, 2, 3, 4]);
        assert_eq!(r.physical_columns.rows_len(), 5);
        assert_eq!(r.physical_columns.columns_len(), 3);
        assert!(!r.is_drained.unwrap());

        let r = block_on(exec.next_batch(1));
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(r.is_drained.unwrap());
    }

    // #[test]
    // fn test_null_pk() {
    //     todo!()
    // }

    /// The following tests are copied from `batch_top_n_executor.rs` to
    /// verify when partition_item is null.
    #[test]
    fn test_no_partition_top_0() {
        let src_exec = MockExecutor::new(
            vec![FieldTypeTp::Double.into()],
            vec![BatchExecuteResult {
                physical_columns: LazyBatchColumnVec::from(vec![VectorValue::Real(
                    vec![None, Real::new(7.0).ok(), None, None].into(),
                )]),
                logical_rows: (0..1).collect(),
                warnings: EvalWarnings::default(),
                is_drained: Ok(true),
            }],
        );

        let mut exec = BatchPartitionTopNExecutor::new_for_test(
            src_exec,
            vec![
                RpnExpressionBuilder::new_for_test()
                    .push_constant_for_test(1)
                    .build_for_test(),
            ],
            vec![false],
            vec![],
            0,
        );

        let r = block_on(exec.next_batch(1));
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(r.is_drained.unwrap());
    }

    #[test]
    fn test_no_partition_no_row() {
        let src_exec = MockExecutor::new(
            vec![FieldTypeTp::LongLong.into()],
            vec![
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![VectorValue::Int(
                        vec![Some(5)].into(),
                    )]),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::empty(),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        );

        let mut exec = BatchPartitionTopNExecutor::new_for_test(
            src_exec,
            vec![
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(0)
                    .build_for_test(),
            ],
            vec![false],
            vec![],
            10,
        );

        let r = block_on(exec.next_batch(1));
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = block_on(exec.next_batch(1));
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(r.is_drained.unwrap());
    }

    /// Builds an executor that will return these data:
    ///
    /// == Schema ==
    /// Col0 (Int)      Col1(Int)       Col2(Real)
    /// == Call #1 ==
    /// NULL            -1              -1.0
    /// NULL            NULL            2.0
    /// NULL            1               4.0
    /// == Call #2 ==
    /// == Call #3 ==
    /// -1              NULL            NULL
    /// -10             10              3.0
    /// -10             NULL            -5.0
    /// -10             -10             0.0
    /// (drained)
    fn make_src_executor() -> MockExecutor {
        MockExecutor::new(
            vec![
                FieldTypeTp::LongLong.into(),
                FieldTypeTp::LongLong.into(),
                FieldTypeTp::Double.into(),
            ],
            vec![
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(vec![None, None, Some(5), None].into()),
                        VectorValue::Int(vec![None, Some(1), None, Some(-1)].into()),
                        VectorValue::Real(
                            vec![
                                Real::new(2.0).ok(),
                                Real::new(4.0).ok(),
                                None,
                                Real::new(-1.0).ok(),
                            ]
                            .into(),
                        ),
                    ]),
                    logical_rows: vec![3, 0, 1],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(vec![Some(0)].into()),
                        VectorValue::Int(vec![Some(10)].into()),
                        VectorValue::Real(vec![Real::new(10.0).ok()].into()),
                    ]),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(
                            vec![Some(-10), Some(-1), Some(-10), None, Some(-10), None].into(),
                        ),
                        VectorValue::Int(
                            vec![None, None, Some(10), Some(-9), Some(-10), None].into(),
                        ),
                        VectorValue::Real(
                            vec![
                                Real::new(-5.0).ok(),
                                None,
                                Real::new(3.0).ok(),
                                None,
                                Real::new(0.0).ok(),
                                Real::new(9.9).ok(),
                            ]
                            .into(),
                        ),
                    ]),
                    logical_rows: vec![1, 2, 0, 4],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        )
    }

    #[test]
    fn test_no_partition_integration_1() {
        // Order by single column, data len < n.
        //
        // mysql> select * from t order by col2 limit 100;
        // +------+------+------+
        // | col0 | col1 | col2 |
        // +------+------+------+
        // |   -1 | NULL | NULL |
        // |  -10 | NULL |   -5 |
        // | NULL |   -1 |   -1 |
        // |  -10 |  -10 |    0 |
        // | NULL | NULL |    2 |
        // |  -10 |   10 |    3 |
        // | NULL |    1 |    4 |
        // +------+------+------+
        //
        // Note: ORDER BY does not use stable sort, so let's order by col2 to avoid
        // duplicate records.

        let src_exec = make_src_executor();

        let mut exec = BatchPartitionTopNExecutor::new_for_test(
            src_exec,
            vec![
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(2)
                    .build_for_test(),
            ],
            vec![false],
            vec![],
            100,
        );

        let r = block_on(exec.next_batch(1));
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = block_on(exec.next_batch(1));
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = block_on(exec.next_batch(1));
        assert_eq!(&r.logical_rows, &[0, 1, 2, 3, 4, 5, 6]);
        assert_eq!(r.physical_columns.rows_len(), 7);
        assert_eq!(r.physical_columns.columns_len(), 3);
        assert_eq!(
            r.physical_columns[0].decoded().to_int_vec(),
            &[Some(-1), Some(-10), None, Some(-10), None, Some(-10), None]
        );
        assert_eq!(
            r.physical_columns[1].decoded().to_int_vec(),
            &[None, None, Some(-1), Some(-10), None, Some(10), Some(1)]
        );
        assert_eq!(
            r.physical_columns[2].decoded().to_real_vec(),
            &[
                None,
                Real::new(-5.0).ok(),
                Real::new(-1.0).ok(),
                Real::new(0.0).ok(),
                Real::new(2.0).ok(),
                Real::new(3.0).ok(),
                Real::new(4.0).ok()
            ]
        );
        assert!(r.is_drained.unwrap());
    }

    #[test]
    fn test_no_partition_integration_2() {
        // Order by multiple columns, data len == n.
        //
        // mysql> select * from t order by col0 desc, col1 limit 7;
        // +------+------+------+
        // | col0 | col1 | col2 |
        // +------+------+------+
        // |   -1 | NULL | NULL |
        // |  -10 | NULL |   -5 |
        // |  -10 |  -10 |    0 |
        // |  -10 |   10 |    3 |
        // | NULL | NULL |    2 |
        // | NULL |   -1 |   -1 |
        // | NULL |    1 |    4 |
        // +------+------+------+

        let src_exec = make_src_executor();

        let mut exec = BatchPartitionTopNExecutor::new_for_test(
            src_exec,
            vec![
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(0)
                    .build_for_test(),
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(1)
                    .build_for_test(),
            ],
            vec![true, false],
            vec![],
            7,
        );

        let r = block_on(exec.next_batch(1));
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = block_on(exec.next_batch(1));
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = block_on(exec.next_batch(1));
        assert_eq!(&r.logical_rows, &[0, 1, 2, 3, 4, 5, 6]);
        assert_eq!(r.physical_columns.rows_len(), 7);
        assert_eq!(r.physical_columns.columns_len(), 3);
        assert_eq!(
            r.physical_columns[0].decoded().to_int_vec(),
            &[Some(-1), Some(-10), Some(-10), Some(-10), None, None, None]
        );
        assert_eq!(
            r.physical_columns[1].decoded().to_int_vec(),
            &[None, None, Some(-10), Some(10), None, Some(-1), Some(1)]
        );
        assert_eq!(
            r.physical_columns[2].decoded().to_real_vec(),
            &[
                None,
                Real::new(-5.0).ok(),
                Real::new(0.0).ok(),
                Real::new(3.0).ok(),
                Real::new(2.0).ok(),
                Real::new(-1.0).ok(),
                Real::new(4.0).ok()
            ]
        );
        assert!(r.is_drained.unwrap());
    }

    #[test]
    fn test_no_partition_integration_3() {
        use tidb_query_expr::{
            impl_arithmetic::{arithmetic_fn_meta, IntIntPlus},
            impl_op::is_null_fn_meta,
        };

        // Order by multiple expressions, data len > n.
        //
        // mysql> select * from t order by isnull(col0), col0, col1 + 1 desc limit 5;
        // +------+------+------+
        // | col0 | col1 | col2 |
        // +------+------+------+
        // |  -10 |   10 |    3 |
        // |  -10 |  -10 |    0 |
        // |  -10 | NULL |   -5 |
        // |   -1 | NULL | NULL |
        // | NULL |    1 |    4 |
        // +------+------+------+

        let src_exec = make_src_executor();

        let mut exec = BatchPartitionTopNExecutor::new_for_test(
            src_exec,
            vec![
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(0)
                    .push_fn_call_for_test(is_null_fn_meta::<Int>(), 1, FieldTypeTp::LongLong)
                    .build_for_test(),
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(0)
                    .build_for_test(),
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(1)
                    .push_constant_for_test(1)
                    .push_fn_call_for_test(
                        arithmetic_fn_meta::<IntIntPlus>(),
                        2,
                        FieldTypeTp::LongLong,
                    )
                    .build_for_test(),
            ],
            vec![false, false, true],
            vec![],
            5,
        );

        let r = block_on(exec.next_batch(1));
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = block_on(exec.next_batch(1));
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = block_on(exec.next_batch(1));
        assert_eq!(&r.logical_rows, &[0, 1, 2, 3, 4]);
        assert_eq!(r.physical_columns.rows_len(), 5);
        assert_eq!(r.physical_columns.columns_len(), 3);
        assert_eq!(
            r.physical_columns[0].decoded().to_int_vec(),
            &[Some(-10), Some(-10), Some(-10), Some(-1), None]
        );
        assert_eq!(
            r.physical_columns[1].decoded().to_int_vec(),
            &[Some(10), Some(-10), None, None, Some(1)]
        );
        assert_eq!(
            r.physical_columns[2].decoded().to_real_vec(),
            &[
                Real::new(3.0).ok(),
                Real::new(0.0).ok(),
                Real::new(-5.0).ok(),
                None,
                Real::new(4.0).ok()
            ]
        );
        assert!(r.is_drained.unwrap());
    }

    /// Builds an executor that will return these data:
    ///
    /// ```text
    /// == Schema ==
    /// Col0 (Bytes[Utf8Mb4GeneralCi])      Col1(Bytes[Utf8Mb4Bin])     Col2(Bytes[Binary])
    /// == Call #1 ==
    /// "aa"                                "aaa"                       "áaA"
    /// NULL                                NULL                        "Aa"
    /// "aa"                                "aa"                        NULL
    /// == Call #2 ==
    /// == Call #3 ==
    /// "áaA"                               "áa"                        NULL
    /// "áa"                                "áaA"                       "aa"
    /// "Aa"                                NULL                        "aaa"
    /// "aaa"                               "Aa"                        "áa"
    /// (drained)
    /// ```
    fn make_bytes_src_executor() -> MockExecutor {
        MockExecutor::new(
            vec![
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::VarChar)
                    .collation(Collation::Utf8Mb4GeneralCi)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::VarChar)
                    .collation(Collation::Utf8Mb4Bin)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::VarChar)
                    .collation(Collation::Binary)
                    .into(),
            ],
            vec![
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Bytes(
                            vec![Some(b"aa".to_vec()), None, Some(b"aa".to_vec())].into(),
                        ),
                        VectorValue::Bytes(
                            vec![Some(b"aa".to_vec()), None, Some(b"aaa".to_vec())].into(),
                        ),
                        VectorValue::Bytes(
                            vec![None, Some(b"Aa".to_vec()), Some("áaA".as_bytes().to_vec())]
                                .into(),
                        ),
                    ]),
                    logical_rows: vec![2, 1, 0],
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
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Bytes(
                            vec![
                                Some("áaA".as_bytes().to_vec()),
                                Some("áa".as_bytes().to_vec()),
                                Some(b"Aa".to_vec()),
                                Some(b"aaa".to_vec()),
                            ]
                            .into(),
                        ),
                        VectorValue::Bytes(
                            vec![
                                Some("áa".as_bytes().to_vec()),
                                Some("áaA".as_bytes().to_vec()),
                                None,
                                Some(b"Aa".to_vec()),
                            ]
                            .into(),
                        ),
                        VectorValue::Bytes(
                            vec![
                                None,
                                Some(b"aa".to_vec()),
                                Some(b"aaa".to_vec()),
                                Some("áa".as_bytes().to_vec()),
                            ]
                            .into(),
                        ),
                    ]),
                    logical_rows: vec![0, 1, 2, 3],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        )
    }

    #[test]
    fn test_no_partition_bytes_1() {
        // Order by multiple expressions with collation, data len > n.
        //
        // mysql> select * from t order by col1 desc, col3 desc, col2 limit 5;
        // +------+--------+--------+
        // | col1 | col2   | col3   |
        // +------+--------+--------+
        // | aaa  | Aa     | áa     |
        // | áaA  | áa     | <null> |
        // | aa   | aaa    | áaA    |
        // | Aa   | <null> | aaa    |
        // | áa   | áaA    | aa     |
        // +------+--------+--------+

        let src_exec = make_bytes_src_executor();

        let mut exec = BatchPartitionTopNExecutor::new_for_test(
            src_exec,
            vec![
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(0)
                    .build_for_test(),
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(2)
                    .build_for_test(),
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(1)
                    .build_for_test(),
            ],
            vec![true, true, false],
            vec![],
            5,
        );

        let r = block_on(exec.next_batch(1));
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = block_on(exec.next_batch(1));
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = block_on(exec.next_batch(1));
        assert_eq!(&r.logical_rows, &[0, 1, 2, 3, 4]);
        assert_eq!(r.physical_columns.rows_len(), 5);
        assert_eq!(r.physical_columns.columns_len(), 3);
        assert_eq!(
            r.physical_columns[0].decoded().to_bytes_vec(),
            &[
                Some(b"aaa".to_vec()),
                Some("áaA".as_bytes().to_vec()),
                Some(b"aa".to_vec()),
                Some(b"Aa".to_vec()),
                Some("áa".as_bytes().to_vec()),
            ]
        );
        assert_eq!(
            r.physical_columns[1].decoded().to_bytes_vec(),
            &[
                Some(b"Aa".to_vec()),
                Some("áa".as_bytes().to_vec()),
                Some(b"aaa".to_vec()),
                None,
                Some("áaA".as_bytes().to_vec()),
            ]
        );
        assert_eq!(
            r.physical_columns[2].decoded().to_bytes_vec(),
            &[
                Some("áa".as_bytes().to_vec()),
                None,
                Some("áaA".as_bytes().to_vec()),
                Some(b"aaa".to_vec()),
                Some(b"aa".to_vec()),
            ]
        );
        assert!(r.is_drained.unwrap());
    }

    #[test]
    fn test_no_partition_bytes_2() {
        // Order by multiple expressions with collation, data len > n.
        //
        // mysql> select * from test order by col1, col2, col3 limit 5;
        // +--------+--------+--------+
        // | col1   | col2   | col3   |
        // +--------+--------+--------+
        // | <null> | <null> | Aa     |
        // | Aa     | <null> | aaa    |
        // | aa     | aa     | <null> |
        // | aa     | aaa    | áaA    |
        // | áa     | áaA    | aa     |
        // +--------+--------+--------+

        let src_exec = make_bytes_src_executor();

        let mut exec = BatchPartitionTopNExecutor::new_for_test(
            src_exec,
            vec![
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(0)
                    .build_for_test(),
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(1)
                    .build_for_test(),
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(2)
                    .build_for_test(),
            ],
            vec![false, false, false],
            vec![],
            5,
        );

        let r = block_on(exec.next_batch(1));
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = block_on(exec.next_batch(1));
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = block_on(exec.next_batch(1));
        assert_eq!(&r.logical_rows, &[0, 1, 2, 3, 4]);
        assert_eq!(r.physical_columns.rows_len(), 5);
        assert_eq!(r.physical_columns.columns_len(), 3);
        assert_eq!(
            r.physical_columns[0].decoded().to_bytes_vec(),
            &[
                None,
                Some(b"Aa".to_vec()),
                Some(b"aa".to_vec()),
                Some(b"aa".to_vec()),
                Some("áa".as_bytes().to_vec()),
            ]
        );
        assert_eq!(
            r.physical_columns[1].decoded().to_bytes_vec(),
            &[
                None,
                None,
                Some(b"aa".to_vec()),
                Some(b"aaa".to_vec()),
                Some("áaA".as_bytes().to_vec()),
            ]
        );
        assert_eq!(
            r.physical_columns[2].decoded().to_bytes_vec(),
            &[
                Some(b"Aa".to_vec()),
                Some(b"aaa".to_vec()),
                None,
                Some("áaA".as_bytes().to_vec()),
                Some(b"aa".to_vec()),
            ]
        );
        assert!(r.is_drained.unwrap());
    }

    /// Builds an executor that will return these data:
    ///
    /// ```text
    /// == Schema ==
    /// Col0 (LongLong(Unsigned))      Col1(LongLong[Signed])       Col2(Long[Unsigned])
    /// == Call #1 ==
    /// 18,446,744,073,709,551,615     -3                           4,294,967,293
    /// NULL                           NULL                         NULL
    /// 18,446,744,073,709,551,613     -1                           4,294,967,295
    /// == Call #2 ==
    /// == Call #3 ==
    /// 2000                           2000                         2000
    /// 9,223,372,036,854,775,807      9,223,372,036,854,775,807    2,147,483,647
    /// 300                            300                          300
    /// 9,223,372,036,854,775,808      -9,223,372,036,854,775,808   2,147,483,648
    /// (drained)                      (drained)                    (drained)
    /// ```
    fn make_src_executor_unsigned() -> MockExecutor {
        MockExecutor::new(
            vec![
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::LongLong)
                    .flag(FieldTypeFlag::UNSIGNED)
                    .into(),
                FieldTypeTp::LongLong.into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::Long)
                    .flag(FieldTypeFlag::UNSIGNED)
                    .into(),
            ],
            vec![
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(
                            vec![
                                Some(18_446_744_073_709_551_613_u64 as i64),
                                None,
                                Some(18_446_744_073_709_551_615_u64 as i64),
                            ]
                            .into(),
                        ),
                        VectorValue::Int(vec![Some(-1), None, Some(-3)].into()),
                        VectorValue::Int(
                            vec![
                                Some(4_294_967_295_u32 as i64),
                                None,
                                Some(4_294_967_295_u32 as i64),
                            ]
                            .into(),
                        ),
                    ]),
                    logical_rows: vec![2, 1, 0],
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
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(
                            vec![
                                Some(300_u64 as i64),
                                Some(9_223_372_036_854_775_807_u64 as i64),
                                Some(2000_u64 as i64),
                                Some(9_223_372_036_854_775_808_u64 as i64),
                            ]
                            .into(),
                        ),
                        VectorValue::Int(
                            vec![
                                Some(300),
                                Some(9_223_372_036_854_775_807),
                                Some(2000),
                                Some(-9_223_372_036_854_775_808),
                            ]
                            .into(),
                        ),
                        VectorValue::Int(
                            vec![
                                Some(300_u32 as i64),
                                Some(2_147_483_647_u32 as i64),
                                Some(2000_u32 as i64),
                                Some(2_147_483_648_u32 as i64),
                            ]
                            .into(),
                        ),
                    ]),
                    logical_rows: vec![2, 1, 0, 3],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        )
    }

    #[test]
    fn test_no_partition_top_unsigned() {
        let test_top5 = |col_index: usize, is_desc: bool, expected: &[Option<i64>]| {
            let src_exec = make_src_executor_unsigned();
            let mut exec = BatchPartitionTopNExecutor::new_for_test(
                src_exec,
                vec![
                    RpnExpressionBuilder::new_for_test()
                        .push_column_ref_for_test(col_index)
                        .build_for_test(),
                ],
                vec![is_desc],
                vec![],
                5,
            );

            let r = block_on(exec.next_batch(1));
            assert!(r.logical_rows.is_empty());
            assert_eq!(r.physical_columns.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let r = block_on(exec.next_batch(1));
            assert!(r.logical_rows.is_empty());
            assert_eq!(r.physical_columns.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let r = block_on(exec.next_batch(1));
            assert_eq!(&r.logical_rows, &[0, 1, 2, 3, 4]);
            assert_eq!(r.physical_columns.rows_len(), 5);
            assert_eq!(r.physical_columns.columns_len(), 3);
            assert_eq!(
                r.physical_columns[col_index].decoded().to_int_vec(),
                expected
            );
            assert!(r.is_drained.unwrap());
        };

        test_top5(
            0,
            false,
            &[
                None,
                Some(300_u64 as i64),
                Some(2000_u64 as i64),
                Some(9_223_372_036_854_775_807_u64 as i64),
                Some(9_223_372_036_854_775_808_u64 as i64),
            ],
        );

        test_top5(
            0,
            true,
            &[
                Some(18_446_744_073_709_551_615_u64 as i64),
                Some(18_446_744_073_709_551_613_u64 as i64),
                Some(9_223_372_036_854_775_808_u64 as i64),
                Some(9_223_372_036_854_775_807_u64 as i64),
                Some(2000_u64 as i64),
            ],
        );

        test_top5(
            1,
            false,
            &[
                None,
                Some(-9_223_372_036_854_775_808),
                Some(-3),
                Some(-1),
                Some(300),
            ],
        );

        test_top5(
            1,
            true,
            &[
                Some(9_223_372_036_854_775_807),
                Some(2000),
                Some(300),
                Some(-1),
                Some(-3),
            ],
        );

        test_top5(
            2,
            false,
            &[
                None,
                Some(300_u32 as i64),
                Some(2000_u32 as i64),
                Some(2_147_483_647_u32 as i64),
                Some(2_147_483_648_u32 as i64),
            ],
        );

        test_top5(
            2,
            true,
            &[
                Some(4_294_967_295_u32 as i64),
                Some(4_294_967_295_u32 as i64),
                Some(2_147_483_648_u32 as i64),
                Some(2_147_483_647_u32 as i64),
                Some(2000_u32 as i64),
            ],
        );
    }

    #[test]
    fn test_no_partition_top_paging() {
        // Top N = 5 and PagingSize = 10, same with no-paging.
        let test_top5_paging6 = |col_index: usize, is_desc: bool, expected: &[Option<i64>]| {
            let mut config = EvalConfig::default();
            config.paging_size = Some(10);
            let config = Arc::new(config);
            let src_exec = make_src_executor_unsigned();
            let mut exec = BatchPartitionTopNExecutor::new_for_test_with_config(
                config,
                src_exec,
                vec![
                    RpnExpressionBuilder::new_for_test()
                        .push_column_ref_for_test(col_index)
                        .build_for_test(),
                ],
                vec![is_desc],
                vec![],
                5,
            );

            let r = block_on(exec.next_batch(1));
            assert!(r.logical_rows.is_empty());
            assert_eq!(r.physical_columns.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let r = block_on(exec.next_batch(1));
            assert!(r.logical_rows.is_empty());
            assert_eq!(r.physical_columns.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let r = block_on(exec.next_batch(1));
            assert_eq!(&r.logical_rows, &[0, 1, 2, 3, 4]);
            assert_eq!(r.physical_columns.rows_len(), 5);
            assert_eq!(r.physical_columns.columns_len(), 3);
            assert_eq!(
                r.physical_columns[col_index].decoded().to_int_vec(),
                expected
            );
            assert!(r.is_drained.unwrap());
        };

        test_top5_paging6(
            0,
            false,
            &[
                None,
                Some(300_u64 as i64),
                Some(2000_u64 as i64),
                Some(9_223_372_036_854_775_807_u64 as i64),
                Some(9_223_372_036_854_775_808_u64 as i64),
            ],
        );

        test_top5_paging6(
            0,
            true,
            &[
                Some(18_446_744_073_709_551_615_u64 as i64),
                Some(18_446_744_073_709_551_613_u64 as i64),
                Some(9_223_372_036_854_775_808_u64 as i64),
                Some(9_223_372_036_854_775_807_u64 as i64),
                Some(2000_u64 as i64),
            ],
        );

        test_top5_paging6(
            1,
            false,
            &[
                None,
                Some(-9_223_372_036_854_775_808),
                Some(-3),
                Some(-1),
                Some(300),
            ],
        );

        test_top5_paging6(
            1,
            true,
            &[
                Some(9_223_372_036_854_775_807),
                Some(2000),
                Some(300),
                Some(-1),
                Some(-3),
            ],
        );

        test_top5_paging6(
            2,
            false,
            &[
                None,
                Some(300_u32 as i64),
                Some(2000_u32 as i64),
                Some(2_147_483_647_u32 as i64),
                Some(2_147_483_648_u32 as i64),
            ],
        );

        test_top5_paging6(
            2,
            true,
            &[
                Some(4_294_967_295_u32 as i64),
                Some(4_294_967_295_u32 as i64),
                Some(2_147_483_648_u32 as i64),
                Some(2_147_483_647_u32 as i64),
                Some(2000_u32 as i64),
            ],
        );

        // Top N = 5 and PagingSize = 8, return all data and do nothing.
        let test_top5_paging4 = |build_src_executor: fn() -> MockExecutor| {
            let mut config = EvalConfig::default();
            config.paging_size = Some(8);
            let config = Arc::new(config);
            let src_exec = build_src_executor();
            let mut exec = BatchPartitionTopNExecutor::new_for_test_with_config(
                config,
                src_exec,
                vec![
                    RpnExpressionBuilder::new_for_test()
                        .push_column_ref_for_test(0)
                        .build_for_test(),
                ],
                vec![false],
                vec![],
                5,
            );
            let mut exec2 = build_src_executor();

            loop {
                let r1 = block_on(exec.next_batch(1));
                let r2 = block_on(exec2.next_batch(1));
                assert_eq!(r1.logical_rows, r2.logical_rows);
                assert_eq!(
                    r1.physical_columns.rows_len(),
                    r2.physical_columns.rows_len()
                );
                assert_eq!(
                    r1.physical_columns.columns_len(),
                    r2.physical_columns.columns_len()
                );
                let r1_is_drained = r1.is_drained.unwrap();
                assert_eq!(r1_is_drained, r2.is_drained.unwrap());
                if r1_is_drained {
                    break;
                }
            }
        };

        test_top5_paging4(make_src_executor_unsigned);
        test_top5_paging4(make_src_executor);
        test_top5_paging4(make_bytes_src_executor);
    }
}
