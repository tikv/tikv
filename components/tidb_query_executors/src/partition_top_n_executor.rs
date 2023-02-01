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
    util::top_n_heap::{HeapItemSourceData, HeapItemUnsafe, TopNHeap},
};

pub struct BatchPartitionTopNExecutor<Src: BatchExecutor> {
    heap: TopNHeap,

    #[allow(clippy::box_collection)]
    eval_columns_buffer_unsafe: Box<Vec<RpnStackNode<'static>>>,

    /// must be prefix of source's property.
    partition_exprs: Box<[RpnExpression]>,

    last_logic_row_index: Option<usize>,

    order_exprs: Box<[RpnExpression]>,
    /// This field stores the field type of the results evaluated by the exprs
    /// in `order_exprs`.
    order_exprs_field_type: Box<[FieldType]>,

    /// Whether or not it is descending order for each order by column.
    order_is_desc: Box<[bool]>,

    n: usize,

    recommended_part_num: usize,

    context: EvalContext,
    src: Src,
    is_ended: bool,
}

impl<Src: BatchExecutor> BatchPartitionTopNExecutor<Src> {
    pub fn new(
        config: Arc<EvalConfig>,
        src: Src,
        partition_exprs_def: Vec<Expr>,
        order_exprs_def: Vec<Expr>,
        order_is_desc: Vec<bool>,
        n: usize,
        recommended_part_num: usize,
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

        Ok(Self {
            // Simply large enough to avoid repeated allocations
            heap: TopNHeap::new(n),
            eval_columns_buffer_unsafe: Box::new(Vec::with_capacity(512)),
            partition_exprs: partition_exprs.into_boxed_slice(),
            order_exprs: order_exprs.into_boxed_slice(),
            order_exprs_field_type: order_exprs_field_type.into_boxed_slice(),
            order_is_desc: order_is_desc.into_boxed_slice(),
            n,
            recommended_part_num,
            context: EvalContext::new(config),
            src,
            is_ended: false,
            last_logic_row_index: None,
        })
    }

    fn has_same_partition_with(&self, logical_row_index: usize) -> bool {
        todo!()
    }
}

/// All `NonNull` pointers in `BatchTopNExecutor` cannot be accessed out of the
/// struct and `BatchTopNExecutor` doesn't leak the pointers to other threads.
/// Therefore, with those `NonNull` pointers, BatchTopNExecutor still remains
/// `Send`.
unsafe impl<Src: BatchExecutor> Send for BatchPartitionTopNExecutor<Src> {}

#[async_trait]
impl<Src: BatchExecutor> BatchExecutor for BatchPartitionTopNExecutor<Src> {
    type StorageStats = Src::StorageStats;

    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.src.schema()
    }

    #[inline]
    async fn next_batch(&mut self, _scan_rows: usize) -> BatchExecuteResult {
        assert!(!self.is_ended);

        if self.n == 0 {
            self.is_ended = true;
            return BatchExecuteResult {
                physical_columns: LazyBatchColumnVec::empty(),
                logical_rows: Vec::new(),
                warnings: EvalWarnings::default(),
                is_drained: Ok(true),
            };
        }

        // Use max batch size from the beginning because top N
        // always needs to calculate over all data.
        let src_result = self.src.next_batch(BATCH_MAX_SIZE).await;
        let pinned_source_data = Arc::new(HeapItemSourceData {
            physical_columns: src_result.physical_columns,
            logical_rows: src_result.logical_rows,
        });

        for logical_row_index in 0..pinned_source_data.logical_rows.len() {
            if self.has_same_partition_with(logical_row_index) {
                self.last_logic_row_index = Some(logical_row_index);
            }
            // let row = HeapItemUnsafe {
            //     order_is_desc_ptr: (*self.order_is_desc).into(),
            //     order_exprs_field_type_ptr:
            // (*self.order_exprs_field_type).into(),
            //     source_data: pinned_source_data.clone(),
            //     eval_columns_buffer_ptr:
            // self.eval_columns_buffer_unsafe.as_ref().into(),
            //     eval_columns_offset: todo!(),
            //     logical_row_index,
            // };
            // self.heap.add_row(row)?;
        }
        todo!();

        // Eval and compare whether partition key is equal
        // let real_scan_rows = if self.is_src_scan_executor {
        //     std::cmp::min(scan_rows, self.remaining_rows)
        // } else {
        //     scan_rows
        // };
        // let mut result = self.src.next_batch(real_scan_rows).await;
        // if result.logical_rows.len() < self.remaining_rows {
        //     self.remaining_rows -= result.logical_rows.len();
        // } else {
        //     // We don't need to touch the physical data.
        //     result.logical_rows.truncate(self.remaining_rows);
        //     result.is_drained = Ok(true);
        //     self.remaining_rows = 0;
        // }
        //
        // result
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
