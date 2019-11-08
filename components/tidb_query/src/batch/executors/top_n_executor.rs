// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::ptr::NonNull;

use servo_arc::Arc;

use tipb::{Expr, FieldType, TopN};

use crate::batch::executors::util::*;
use crate::batch::interface::*;
use crate::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use crate::codec::data_type::*;
use crate::expr::EvalWarnings;
use crate::expr::{EvalConfig, EvalContext};
use crate::rpn_expr::RpnStackNode;
use crate::rpn_expr::{RpnExpression, RpnExpressionBuilder};
use crate::storage::IntervalRange;
use crate::Result;

pub struct BatchTopNExecutor<Src: BatchExecutor> {
    /// The heap, which contains N rows at most.
    ///
    /// This field is placed before `eval_columns_buffer_unsafe`, `order_exprs`, `order_is_desc`
    /// and `src` because it relies on data in those fields and we want this field to be dropped
    /// first.
    heap: BinaryHeap<HeapItemUnsafe>,

    /// A collection of all evaluated columns. This is to avoid repeated allocations in
    /// each `next_batch()`.
    ///
    /// DO NOT EVER try to read the content of the elements directly, since it is highly unsafe.
    /// The lifetime of elements is not really 'static. Certain elements are valid only if both
    /// of the following conditions are satisfied:
    ///
    /// 1. `BatchTopNExecutor` is valid (i.e. not dropped).
    ///
    /// 2. The referenced `LazyBatchColumnVec` of the element must be valid, which only happens
    ///    when at least one of the row is in the `heap`. Note that rows may be swapped out from
    ///    `heap` at any time.
    ///
    /// This field is placed before `order_exprs` and `src` because it relies on data in
    /// those fields and we want this field to be dropped first.
    #[allow(clippy::box_vec)]
    eval_columns_buffer_unsafe: Box<Vec<RpnStackNode<'static>>>,

    order_exprs: Box<[RpnExpression]>,

    /// Whether or not it is descending order for each order by column.
    order_is_desc: Box<[bool]>,

    n: usize,

    context: EvalContext,
    src: Src,
    is_ended: bool,
}

/// All `NonNull` pointers in `BatchTopNExecutor` cannot be accessed out of the struct and
/// `BatchTopNExecutor` doesn't leak the pointers to other threads. Therefore, with those `NonNull`
/// pointers, BatchTopNExecutor still remains `Send`.
unsafe impl<Src: BatchExecutor> Send for BatchTopNExecutor<Src> {}

// We assign a dummy type `Box<dyn BatchExecutor<StorageStats = ()>>` so that we can omit the type
// when calling `check_supported`.
impl BatchTopNExecutor<Box<dyn BatchExecutor<StorageStats = ()>>> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &TopN) -> Result<()> {
        if descriptor.get_order_by().is_empty() {
            return Err(other_err!("Missing Top N column"));
        }
        for item in descriptor.get_order_by() {
            RpnExpressionBuilder::check_expr_tree_supported(item.get_expr())?;
        }
        Ok(())
    }
}

impl<Src: BatchExecutor> BatchTopNExecutor<Src> {
    #[cfg(test)]
    pub fn new_for_test(
        src: Src,
        order_exprs: Vec<RpnExpression>,
        order_is_desc: Vec<bool>,
        n: usize,
    ) -> Self {
        assert_eq!(order_exprs.len(), order_is_desc.len());

        Self {
            heap: BinaryHeap::new(),
            eval_columns_buffer_unsafe: Box::new(Vec::new()),
            order_exprs: order_exprs.into_boxed_slice(),
            order_is_desc: order_is_desc.into_boxed_slice(),
            n,

            context: EvalContext::default(),
            src,
            is_ended: false,
        }
    }

    pub fn new(
        config: std::sync::Arc<EvalConfig>,
        src: Src,
        order_exprs_def: Vec<Expr>,
        order_is_desc: Vec<bool>,
        n: usize,
    ) -> Result<Self> {
        assert_eq!(order_exprs_def.len(), order_is_desc.len());

        let mut order_exprs = Vec::with_capacity(order_exprs_def.len());
        let mut ctx = EvalContext::new(config.clone());
        for def in order_exprs_def {
            order_exprs.push(RpnExpressionBuilder::build_from_expr_tree(
                def,
                &mut ctx,
                src.schema().len(),
            )?);
        }

        Ok(Self {
            // Avoid large N causing OOM
            heap: BinaryHeap::with_capacity(n.min(1024)),
            // Simply large enough to avoid repeated allocations
            eval_columns_buffer_unsafe: Box::new(Vec::with_capacity(512)),
            order_exprs: order_exprs.into_boxed_slice(),
            order_is_desc: order_is_desc.into_boxed_slice(),
            n,

            context: EvalContext::new(config),
            src,
            is_ended: false,
        })
    }

    #[inline]
    fn handle_next_batch(&mut self) -> Result<Option<LazyBatchColumnVec>> {
        // Use max batch size from the beginning because top N
        // always needs to calculate over all data.
        let src_result = self.src.next_batch(crate::batch::runner::BATCH_MAX_SIZE);

        self.context.warnings = src_result.warnings;

        let src_is_drained = src_result.is_drained?;

        if !src_result.logical_rows.is_empty() {
            self.process_batch_input(src_result.physical_columns, src_result.logical_rows)?;
        }

        if src_is_drained {
            Ok(Some(self.heap_take_all()))
        } else {
            Ok(None)
        }
    }

    fn process_batch_input(
        &mut self,
        mut physical_columns: LazyBatchColumnVec,
        logical_rows: Vec<usize>,
    ) -> Result<()> {
        ensure_columns_decoded(
            &mut self.context,
            &self.order_exprs,
            self.src.schema(),
            &mut physical_columns,
            &logical_rows,
        )?;

        // Pin data behind an Arc, so that they won't be dropped as long as this `pinned_data`
        // is kept somewhere.
        let pinned_source_data = Arc::new(HeapItemSourceData {
            physical_columns,
            logical_rows,
        });

        let eval_offset = self.eval_columns_buffer_unsafe.len();
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

        for logical_row_index in 0..pinned_source_data.logical_rows.len() {
            let row = HeapItemUnsafe {
                order_is_desc_ptr: (&*self.order_is_desc).into(),
                source_data: pinned_source_data.clone(),
                eval_columns_buffer_ptr: (&*self.eval_columns_buffer_unsafe).into(),
                eval_columns_offset: eval_offset,
                logical_row_index,
            };
            self.heap_add_row(row);
        }

        Ok(())
    }

    fn heap_add_row(&mut self, row: HeapItemUnsafe) {
        if self.heap.len() < self.n {
            // Push into heap when heap is not full.
            self.heap.push(row);
        } else {
            // Swap the greatest row in the heap if this row is smaller than that row.
            let mut greatest_row = self.heap.peek_mut().unwrap();
            if row.cmp(&greatest_row) == Ordering::Less {
                *greatest_row = row;
            }
        }
    }

    #[allow(clippy::clone_on_copy)]
    fn heap_take_all(&mut self) -> LazyBatchColumnVec {
        let heap = std::mem::replace(&mut self.heap, BinaryHeap::default());
        let sorted_items = heap.into_sorted_vec();
        if sorted_items.is_empty() {
            return LazyBatchColumnVec::empty();
        }

        let mut result = sorted_items[0]
            .source_data
            .physical_columns
            .clone_empty(sorted_items.len());

        for (column_index, result_column) in result.as_mut_slice().iter_mut().enumerate() {
            match result_column {
                LazyBatchColumn::Raw(dest_column) => {
                    for item in &sorted_items {
                        let src = item.source_data.physical_columns[column_index].raw();
                        dest_column
                            .push(&src[item.source_data.logical_rows[item.logical_row_index]]);
                    }
                }
                LazyBatchColumn::Decoded(dest_vector_value) => {
                    match_template_evaluable! {
                        TT, match dest_vector_value {
                            VectorValue::TT(dest_column) => {
                                for item in &sorted_items {
                                    let src = item.source_data.physical_columns[column_index].decoded();
                                    let src: &[Option<TT>] = src.as_ref();
                                    // TODO: This clone is not necessary.
                                    dest_column.push(src[item.source_data.logical_rows[item.logical_row_index]].clone());
                                }
                            },
                        }
                    }
                }
            }
        }

        result.assert_columns_equal_length();
        result
    }
}

impl<Src: BatchExecutor> BatchExecutor for BatchTopNExecutor<Src> {
    type StorageStats = Src::StorageStats;

    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.src.schema()
    }

    #[inline]
    fn next_batch(&mut self, _scan_rows: usize) -> BatchExecuteResult {
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

        let result = self.handle_next_batch();

        match result {
            Err(e) => {
                // When there are error, we can just return empty data.
                self.is_ended = true;
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::empty(),
                    logical_rows: Vec::new(),
                    warnings: self.context.take_warnings(),
                    is_drained: Err(e),
                }
            }
            Ok(Some(logical_columns)) => {
                self.is_ended = true;
                let logical_rows = (0..logical_columns.rows_len()).collect();
                BatchExecuteResult {
                    physical_columns: logical_columns,
                    logical_rows,
                    warnings: self.context.take_warnings(),
                    is_drained: Ok(true),
                }
            }
            Ok(None) => BatchExecuteResult {
                physical_columns: LazyBatchColumnVec::empty(),
                logical_rows: Vec::new(),
                warnings: self.context.take_warnings(),
                is_drained: Ok(false),
            },
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
}

struct HeapItemSourceData {
    physical_columns: LazyBatchColumnVec,
    logical_rows: Vec<usize>,
}

/// The item in the heap of `BatchTopNExecutor`.
///
/// WARN: The content of this structure is valid only if `BatchTopNExecutor` is valid (i.e.
/// not dropped). Thus it is called unsafe.
struct HeapItemUnsafe {
    /// A pointer to the `order_is_desc` field in `BatchTopNExecutor`.
    order_is_desc_ptr: NonNull<[bool]>,

    /// The source data that evaluated column in this structure is using.
    source_data: Arc<HeapItemSourceData>,

    /// A pointer to the `eval_columns_buffer` field in `BatchTopNExecutor`.
    eval_columns_buffer_ptr: NonNull<Vec<RpnStackNode<'static>>>,

    /// The begin offset of the evaluated columns stored in the buffer.
    ///
    /// The length of evaluated columns in the buffer is `order_is_desc.len()`.
    eval_columns_offset: usize,

    /// Which logical row in the evaluated columns this heap item is representing.
    logical_row_index: usize,
}

impl HeapItemUnsafe {
    fn get_order_is_desc(&self) -> &[bool] {
        unsafe { self.order_is_desc_ptr.as_ref() }
    }

    fn get_eval_columns(&self, len: usize) -> &[RpnStackNode<'_>] {
        let offset_begin = self.eval_columns_offset;
        let offset_end = offset_begin + len;
        let vec_buf = unsafe { self.eval_columns_buffer_ptr.as_ref() };
        &vec_buf[offset_begin..offset_end]
    }
}

impl Ord for HeapItemUnsafe {
    fn cmp(&self, other: &Self) -> Ordering {
        // Only debug assert because this function is called pretty frequently.
        debug_assert_eq!(self.get_order_is_desc(), other.get_order_is_desc());

        let order_is_desc = self.get_order_is_desc();
        let columns_len = order_is_desc.len();
        let eval_columns_lhs = self.get_eval_columns(columns_len);
        let eval_columns_rhs = other.get_eval_columns(columns_len);

        for column_idx in 0..columns_len {
            let lhs_node = &eval_columns_lhs[column_idx];
            let rhs_node = &eval_columns_rhs[column_idx];
            let lhs = lhs_node.get_logical_scalar_ref(self.logical_row_index);
            let rhs = rhs_node.get_logical_scalar_ref(other.logical_row_index);

            // There is panic inside, but will never panic, since the data type of corresponding
            // column should be consistent for each `HeapItemUnsafe`.
            let ord = lhs.cmp(&rhs);

            if ord == Ordering::Equal {
                continue;
            }
            if !order_is_desc[column_idx] {
                return ord;
            } else {
                return ord.reverse();
            }
        }

        Ordering::Equal
    }
}

impl PartialOrd for HeapItemUnsafe {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapItemUnsafe {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for HeapItemUnsafe {}

#[cfg(test)]
mod tests {
    use super::*;

    use tidb_query_datatype::FieldTypeTp;

    use crate::batch::executors::util::mock_executor::MockExecutor;
    use crate::expr::EvalWarnings;
    use crate::rpn_expr::RpnExpressionBuilder;

    #[test]
    fn test_top_0() {
        let src_exec = MockExecutor::new(
            vec![FieldTypeTp::Double.into()],
            vec![BatchExecuteResult {
                physical_columns: LazyBatchColumnVec::from(vec![VectorValue::Real(vec![
                    None,
                    Real::new(7.0).ok(),
                    None,
                    None,
                ])]),
                logical_rows: (0..1).collect(),
                warnings: EvalWarnings::default(),
                is_drained: Ok(true),
            }],
        );

        let mut exec = BatchTopNExecutor::new_for_test(
            src_exec,
            vec![RpnExpressionBuilder::new().push_constant(1).build()],
            vec![false],
            0,
        );

        let r = exec.next_batch(1);
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(r.is_drained.unwrap());
    }

    #[test]
    fn test_no_row() {
        let src_exec = MockExecutor::new(
            vec![FieldTypeTp::LongLong.into()],
            vec![
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![VectorValue::Int(vec![Some(
                        5,
                    )])]),
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

        let mut exec = BatchTopNExecutor::new_for_test(
            src_exec,
            vec![RpnExpressionBuilder::new().push_column_ref(0).build()],
            vec![false],
            10,
        );

        let r = exec.next_batch(1);
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
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
                        VectorValue::Int(vec![None, None, Some(5), None]),
                        VectorValue::Int(vec![None, Some(1), None, Some(-1)]),
                        VectorValue::Real(vec![
                            Real::new(2.0).ok(),
                            Real::new(4.0).ok(),
                            None,
                            Real::new(-1.0).ok(),
                        ]),
                    ]),
                    logical_rows: vec![3, 0, 1],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(vec![Some(0)]),
                        VectorValue::Int(vec![Some(10)]),
                        VectorValue::Real(vec![Real::new(10.0).ok()]),
                    ]),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(vec![
                            Some(-10),
                            Some(-1),
                            Some(-10),
                            None,
                            Some(-10),
                            None,
                        ]),
                        VectorValue::Int(vec![None, None, Some(10), Some(-9), Some(-10), None]),
                        VectorValue::Real(vec![
                            Real::new(-5.0).ok(),
                            None,
                            Real::new(3.0).ok(),
                            None,
                            Real::new(0.0).ok(),
                            Real::new(9.9).ok(),
                        ]),
                    ]),
                    logical_rows: vec![1, 2, 0, 4],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        )
    }

    #[test]
    fn test_integration_1() {
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

        let mut exec = BatchTopNExecutor::new_for_test(
            src_exec,
            vec![RpnExpressionBuilder::new().push_column_ref(2).build()],
            vec![false],
            100,
        );

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[0, 1, 2, 3, 4, 5, 6]);
        assert_eq!(r.physical_columns.rows_len(), 7);
        assert_eq!(r.physical_columns.columns_len(), 3);
        assert_eq!(
            r.physical_columns[0].decoded().as_int_slice(),
            &[Some(-1), Some(-10), None, Some(-10), None, Some(-10), None]
        );
        assert_eq!(
            r.physical_columns[1].decoded().as_int_slice(),
            &[None, None, Some(-1), Some(-10), None, Some(10), Some(1)]
        );
        assert_eq!(
            r.physical_columns[2].decoded().as_real_slice(),
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
    fn test_integration_2() {
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

        let mut exec = BatchTopNExecutor::new_for_test(
            src_exec,
            vec![
                RpnExpressionBuilder::new().push_column_ref(0).build(),
                RpnExpressionBuilder::new().push_column_ref(1).build(),
            ],
            vec![true, false],
            7,
        );

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[0, 1, 2, 3, 4, 5, 6]);
        assert_eq!(r.physical_columns.rows_len(), 7);
        assert_eq!(r.physical_columns.columns_len(), 3);
        assert_eq!(
            r.physical_columns[0].decoded().as_int_slice(),
            &[Some(-1), Some(-10), Some(-10), Some(-10), None, None, None]
        );
        assert_eq!(
            r.physical_columns[1].decoded().as_int_slice(),
            &[None, None, Some(-10), Some(10), None, Some(-1), Some(1)]
        );
        assert_eq!(
            r.physical_columns[2].decoded().as_real_slice(),
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
    fn test_integration_3() {
        use crate::rpn_expr::impl_arithmetic::{arithmetic_fn_meta, IntIntPlus};
        use crate::rpn_expr::impl_op::is_null_fn_meta;

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

        let mut exec = BatchTopNExecutor::new_for_test(
            src_exec,
            vec![
                RpnExpressionBuilder::new()
                    .push_column_ref(0)
                    .push_fn_call(is_null_fn_meta::<Int>(), 1, FieldTypeTp::LongLong)
                    .build(),
                RpnExpressionBuilder::new().push_column_ref(0).build(),
                RpnExpressionBuilder::new()
                    .push_column_ref(1)
                    .push_constant(1)
                    .push_fn_call(arithmetic_fn_meta::<IntIntPlus>(), 2, FieldTypeTp::LongLong)
                    .build(),
            ],
            vec![false, false, true],
            5,
        );

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[0, 1, 2, 3, 4]);
        assert_eq!(r.physical_columns.rows_len(), 5);
        assert_eq!(r.physical_columns.columns_len(), 3);
        assert_eq!(
            r.physical_columns[0].decoded().as_int_slice(),
            &[Some(-10), Some(-10), Some(-10), Some(-1), None]
        );
        assert_eq!(
            r.physical_columns[1].decoded().as_int_slice(),
            &[Some(10), Some(-10), None, None, Some(1)]
        );
        assert_eq!(
            r.physical_columns[2].decoded().as_real_slice(),
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
}
