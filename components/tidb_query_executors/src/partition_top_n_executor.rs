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

    partition_exprs: Box<[RpnExpression]>,
    partition_exprs_field_type: Box<[FieldType]>,

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
    pub fn new(
        config: Arc<EvalConfig>,
        src: Src,
        partition_exprs_def: Vec<Expr>,
        order_exprs_def: Vec<Expr>,
        order_is_desc: Vec<bool>,
        n: usize,
    ) -> Result<Self> {
        assert_eq!(order_exprs_def.len(), order_is_desc.len());

        let mut order_exprs: Vec<RpnExpression> = Vec::with_capacity(order_exprs_def.len());
        let mut ctx = EvalContext::new(config.clone());
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
        let mut ctx = EvalContext::new(config.clone());
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

            for logical_row_index in 0..pinned_source_data.logical_rows.len() {
                let partition_key = HeapItemUnsafe {
                    order_is_desc_ptr: (*vec![false; self.partition_exprs.len()]
                        .into_boxed_slice())
                    .into(),
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
    /// Memory Control Analysis:
    /// 1. if n > paging_size(1024), this operator won't do anything and just
    /// return data to upstream. So we can think n is less than or equal to
    /// paging_size.
    /// 2. The worst case is that there is already n rows in heap, and first
    /// row of src_result has different partition with rows in heap. So heap
    /// will be flushed. And the last row of src_result has another different
    /// partition with the first two. So heap will be flushed again.
    /// In this case, there can be 2*n-2 rows in the result, which may be larger
    /// than paging_size.
    /// todo: find a solution to limit it up to paging_size. baseline: limit n
    /// up to paging_size/2
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
            if self.n > paging_size as usize {
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
