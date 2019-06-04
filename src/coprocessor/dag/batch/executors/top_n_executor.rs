// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::Arc;

use tikv_util::erase_lifetime;
use tipb::executor::TopN;
use tipb::expression::{Expr, FieldType};

use crate::coprocessor::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use crate::coprocessor::codec::data_type::VectorValue;
use crate::coprocessor::dag::batch::interface::*;
use crate::coprocessor::dag::expr::EvalWarnings;
use crate::coprocessor::dag::expr::{EvalConfig, EvalContext};
use crate::coprocessor::dag::rpn_expr::types::RpnStackNode;
use crate::coprocessor::dag::rpn_expr::{RpnExpression, RpnExpressionBuilder};
use crate::coprocessor::Result;

pub struct BatchTopNExecutor<Src: BatchExecutor> {
    /// The heap, which contains N rows at most.
    ///
    /// This field is placed before `eval_columns_buffer_unsafe`, `order_exprs`, `order_is_desc`
    /// and `src` because it relies on data in those fields and we want this field to be dropped
    /// first.
    heap: BinaryHeap<HeapItemUnsafe>,

    /// A buffer for all evaluated columns. This is to avoid repeated allocations in
    /// each `next_batch()`.
    ///
    /// See `eval_columns_buffer_ptr` in `HeapItemUnsafe` for where this buffer is used.
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

    /// A buffer for all row index pairs. This is to avoid repeated allocations in
    /// each `next_batch()`.
    ///
    /// See `row_index_buffer_ptr` in `SourceColumnsUnsafe` for where this buffer is used.
    #[allow(clippy::box_vec)]
    row_index_buffer_unsafe: Box<Vec<(u32, u32)>>,

    order_exprs: Box<[RpnExpression]>,

    /// Whether or not it is descending order for each order by column.
    order_is_desc: Box<[bool]>,

    n: usize,

    context: EvalContext,
    src: Src,
    is_ended: bool,
}

unsafe impl<Src: BatchExecutor + Send> Send for BatchTopNExecutor<Src> {}

impl BatchTopNExecutor<Box<dyn BatchExecutor>> {
    /// Checks whether this executor can be used.
    pub fn check_supported(descriptor: &TopN) -> Result<()> {
        if descriptor.get_order_by().is_empty() {
            return Err(box_err!("Missing Top N column"));
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
            row_index_buffer_unsafe: Box::new(Vec::new()),
            order_exprs: order_exprs.into_boxed_slice(),
            order_is_desc: order_is_desc.into_boxed_slice(),
            n,

            context: EvalContext::default(),
            src,
            is_ended: false,
        }
    }
}

impl<Src: BatchExecutor> BatchTopNExecutor<Src> {
    pub fn new(
        config: Arc<EvalConfig>,
        src: Src,
        order_exprs_def: Vec<Expr>,
        order_is_desc: Vec<bool>,
        n: usize,
    ) -> Result<Self> {
        assert_eq!(order_exprs_def.len(), order_is_desc.len());

        let mut order_exprs = Vec::with_capacity(order_exprs_def.len());
        for def in order_exprs_def {
            order_exprs.push(RpnExpressionBuilder::build_from_expr_tree(
                def,
                &config.tz,
                src.schema().len(),
            )?);
        }

        Ok(Self {
            // Avoid large N causing OOM
            heap: BinaryHeap::with_capacity(n.min(1024)),
            // Simply large enough to avoid repeated allocations
            eval_columns_buffer_unsafe: Box::new(Vec::with_capacity(512)),
            // Roughly it should be the number of all possible read rows
            row_index_buffer_unsafe: Box::new(Vec::with_capacity(10240)),
            order_exprs: order_exprs.into_boxed_slice(),
            order_is_desc: order_is_desc.into_boxed_slice(),
            n,

            context: EvalContext::new(config),
            src,
            is_ended: false,
        })
    }

    fn handle_next_batch(&mut self) -> Result<Option<LazyBatchColumnVec>> {
        // Use max batch size from the beginning because top N
        // always needs to calculate over all data.
        let src_result = self
            .src
            .next_batch(crate::coprocessor::dag::batch_handler::BATCH_MAX_SIZE);

        self.context.warnings = src_result.warnings;

        let src_is_drained = src_result.is_drained?;

        if src_result.data.rows_len() > 0 {
            self.process_batch_input(src_result.data)?;
        }

        if src_is_drained {
            Ok(Some(self.heap_take_all()))
        } else {
            Ok(None)
        }
    }

    fn process_batch_input(&mut self, mut data: LazyBatchColumnVec) -> Result<()> {
        let rows_len = data.rows_len();

        let src_schema_unbounded = unsafe { erase_lifetime(self.src.schema()) };
        for expr in self.order_exprs.iter() {
            expr.ensure_columns_decoded(&self.context.cfg.tz, src_schema_unbounded, &mut data)?;
        }

        // Prepare the shared source columns, which will be shared for each row in this batch.
        // These rows may be put in heap, or may be not. If all rows are not in the heap, then
        // this shared source columns will be released thanks to Rc.
        let row_index_buffer_offset = self.row_index_buffer_unsafe.len();
        self.row_index_buffer_unsafe
            .resize(row_index_buffer_offset + rows_len, (0, 0));
        let source_columns = Rc::new(RefCell::new(SourceColumnsUnsafe {
            data,
            row_index_buffer_ptr: (&*self.row_index_buffer_unsafe).into(),
            row_index_buffer_offset,
            row_index_buffer_len: 0,
            consumed: false,
        }));

        // `source_columns` is reference counted. Evaluate columns based on the data in
        // `source_columns` so that we can safely extend the lifetime of these evaluated columns
        // as long as `source_columns` is valid.
        let eval_columns_buffer_offset = self.eval_columns_buffer_unsafe.len();
        let order_exprs_unbounded = unsafe { erase_lifetime(&*self.order_exprs) };
        let data_unbounded = unsafe { erase_lifetime(&source_columns.borrow().data) };
        for expr_unbounded in order_exprs_unbounded.iter() {
            self.eval_columns_buffer_unsafe
                .push(expr_unbounded.eval_unchecked(
                    &mut self.context,
                    rows_len,
                    src_schema_unbounded,
                    data_unbounded,
                )?);
        }

        for row_index in 0..rows_len {
            let row = HeapItemUnsafe {
                order_is_desc_ptr: (&*self.order_is_desc).into(),
                source_column_data: source_columns.clone(),
                eval_columns_buffer_ptr: (&*self.eval_columns_buffer_unsafe).into(),
                eval_columns_buffer_offset,
                row_index,
            };
            // Row may be added to heap, or may be not. Existing rows may be also swapped out.
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
        let mut sorted_items = heap.into_sorted_vec();
        if sorted_items.is_empty() {
            return LazyBatchColumnVec::empty();
        }

        let sorted_len = sorted_items.len();
        let mut target = sorted_items[0]
            .source_column_data
            .borrow()
            .data
            .clone_empty(sorted_len);

        // Now we have sorted heap items. We will do the following:
        // 1. Mark the sorted index of each row in its shared source columns meta data.
        //    This will be random read and random write.
        for (sorted_index, item) in sorted_items.as_mut_slice().iter_mut().enumerate() {
            let mut data = (*item.source_column_data).borrow_mut();
            data.push_row_index_pair(item.row_index, sorted_index);
        }

        // 2. For each heap item, we access its shared source columns meta data, move data
        //    to the target (according to sorted index) column by column. This will be
        //    sequential read and random write. After that, we mark this shared source columns
        //    as "used" through the `consumed` field and next time we will simply skip it.
        for item in &mut sorted_items {
            let mut source = (*item.source_column_data).borrow_mut();
            if source.consumed {
                // This source columns is already processed, skip.
                continue;
            }

            source.consumed = true;

            // Before moving, let's sort these pairs, so that later access can be sequential.
            // Note: we sort pairs in descending order, so that our access to `data` starts
            // from the end, which is critical for step 2.x.
            assert!(source.row_index_buffer_len > 0);
            source.sort_row_index_pairs();

            // To workaround the borrow checker. This is safe since `source.get_row_index_pairs()`
            // does not keep any references of `source.data`.
            let row_index_pairs = unsafe { erase_lifetime(source.get_row_index_pairs()) };

            assert_eq!(target.len(), source.data.len());
            for (column_idx, column) in source.data.iter_mut().enumerate() {
                // Skip `LazyBatchColumn::Raw` since we cannot sequentially write into this
                // kind of target.
                if let LazyBatchColumn::Decoded(src_value) = column {
                    match_template_evaluable! {
                        TT, match (src_value, target[column_idx].mut_decoded()) {
                            (VectorValue::TT(src), VectorValue::TT(dest)) => {
                                for (row_index, target_index) in row_index_pairs {
                                    let row_index = *row_index as usize;
                                    let target_index = *target_index as usize;
                                    unsafe {
                                        // 2.1. Memory copy src[i] to target[j]. Now src[i] lives
                                        //      in two places.
                                        std::ptr::copy_nonoverlapping(
                                            &src[row_index],
                                            dest.as_mut_ptr().add(target_index),
                                            1,
                                        );

                                        // 2.2. Memory copy src[last] to src[i]. Now src[i] lives
                                        //      in one place, but src[last] lives in two places.
                                        //      We will never access index `i` again, and we will
                                        //      never access index `last` (since we access in
                                        //      descending order), so this move is fine.
                                        if row_index < src.len() - 1 {
                                            let src_len = src.len();
                                            // Copy only if src[i] is not the last item.
                                            std::ptr::copy_nonoverlapping(
                                                &src[src_len - 1],
                                                &mut src[row_index],
                                                1,
                                            );
                                        }

                                        // 2.3. Shrink src to exclude src[last]. Now src[last]
                                        //      lives in one place.
                                        src.set_len(src.len() - 1);
                                    }
                                }
                            },
                            _ => unreachable!(),
                        }
                    }
                }
            }
        }

        // Updates target columns' length (since we use copy instead of push).
        for target_column in &mut *target {
            if let LazyBatchColumn::Decoded(v) = target_column {
                match_template_evaluable! {
                    TT, match v {
                        VectorValue::TT(dest) => unsafe { dest.set_len(sorted_len) },
                    }
                }
            }
        }

        // 3. For those target columns that requires a sequential write, we use a random read
        //    but sequential write pattern, which simply iterates each sorted item.
        for (column_idx, column) in target.iter_mut().enumerate() {
            if let LazyBatchColumn::Raw(target_column) = column {
                for item in &sorted_items {
                    let source = (*item.source_column_data).borrow();
                    target_column.push(&source.data[column_idx].raw()[item.row_index]);
                }
            }
        }

        // 4. Now all columns in target should have equal length. Note that source columns
        //    does not have equal length since we didn't remove items for sequential columns.
        target.assert_columns_equal_length();
        assert_eq!(target.rows_len(), sorted_len);

        target
    }
}

impl<Src: BatchExecutor> BatchExecutor for BatchTopNExecutor<Src> {
    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.src.schema()
    }

    fn next_batch(&mut self, _scan_rows: usize) -> BatchExecuteResult {
        assert!(!self.is_ended);

        if self.n == 0 {
            self.is_ended = true;
            return BatchExecuteResult {
                data: LazyBatchColumnVec::empty(),
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
                    data: LazyBatchColumnVec::empty(),
                    warnings: self.context.take_warnings(),
                    is_drained: Err(e),
                }
            }
            Ok(Some(data)) => {
                self.is_ended = true;
                BatchExecuteResult {
                    data,
                    warnings: self.context.take_warnings(),
                    is_drained: Ok(true),
                }
            }
            Ok(None) => BatchExecuteResult {
                data: LazyBatchColumnVec::empty(),
                warnings: self.context.take_warnings(),
                is_drained: Ok(false),
            },
        }
    }

    #[inline]
    fn collect_statistics(&mut self, destination: &mut BatchExecuteStatistics) {
        self.src.collect_statistics(destination);
    }
}

/// Source columns and some meta data about the sorted order of the rows in the source columns.
///
/// WARN: The content of this structure is valid only if `BatchTopNExecutor` is valid (i.e.
/// not dropped). Thus it is called unsafe.
pub struct SourceColumnsUnsafe {
    /// The data of source columns.
    data: LazyBatchColumnVec,

    /// Each item is `(a, b)`, which means the row `a` in `data` should be placed at index `b`
    /// after sorting.
    ///
    /// Sorted index are known and assigned only after the heap is fully built.
    ///
    /// This field is a pointer to the `row_index_buffer` field in `BatchTopNExecutor`.
    row_index_buffer_ptr: NonNull<Vec<(u32, u32)>>,
    /// The begin offset of the target index array stored in the buffer.
    row_index_buffer_offset: usize,
    /// The length of valid row index pairs. We always reserves `data.rows_len()` space.
    row_index_buffer_len: usize,

    /// Whether or not all valid rows in the source column has been consumed.
    consumed: bool,
}

impl SourceColumnsUnsafe {
    /// Returns all valid pairs in `row_index_buffer` for this instance.
    fn get_row_index_pairs(&self) -> &[(u32, u32)] {
        let offset_begin = self.row_index_buffer_offset;
        let offset_end = offset_begin + self.row_index_buffer_len;
        let vec_buf = unsafe { self.row_index_buffer_ptr.as_ref() };
        &vec_buf[offset_begin..offset_end]
    }

    /// Sorts valid pairs belongs to this instance in `row_index_buffer` in DESCENDING order.
    fn sort_row_index_pairs(&mut self) {
        let offset_begin = self.row_index_buffer_offset;
        let offset_end = offset_begin + self.row_index_buffer_len;
        let vec_buf = unsafe { self.row_index_buffer_ptr.as_mut() };
        let slice = &mut vec_buf[offset_begin..offset_end];
        slice.sort_unstable_by(|&a, &b| a.cmp(&b).reverse());
    }

    /// Pushes a pair into `row_index_buffer` for this instance and updates the valid length.
    fn push_row_index_pair(&mut self, row_index: usize, sorted_index: usize) {
        let offset = self.row_index_buffer_offset + self.row_index_buffer_len;
        let vec_buf = unsafe { self.row_index_buffer_ptr.as_mut() };
        vec_buf[offset] = (row_index as u32, sorted_index as u32);
        self.row_index_buffer_len += 1;
        debug_assert!(self.row_index_buffer_len <= self.data.rows_len());
    }
}

/// The item in the heap of `BatchTopNExecutor`.
///
/// WARN: The content of this structure is valid only if `BatchTopNExecutor` is valid (i.e.
/// not dropped). Thus it is called unsafe.
pub struct HeapItemUnsafe {
    /// A pointer to the `order_is_desc` field in `BatchTopNExecutor`.
    order_is_desc_ptr: NonNull<[bool]>,
    /// The source columns that evaluated column in this structure is referring to.
    ///
    /// Multiple `HeapItemUnsafe` may share the same source column. Thus source columns
    /// are placed behind a reference counter. However, there won't be a place other than
    /// `HeapItemUnsafe` holding this reference counter, so Rc won't break
    /// `BatchTopNExecutor: Send`.
    source_column_data: Rc<RefCell<SourceColumnsUnsafe>>,

    /// A pointer to the `eval_columns_buffer` field in `BatchTopNExecutor`.
    eval_columns_buffer_ptr: NonNull<Vec<RpnStackNode<'static>>>,
    /// The begin offset of the evaluated columns stored in the buffer.
    ///
    /// The length of evaluated columns in the buffer is `order_is_desc.len()`.
    eval_columns_buffer_offset: usize,

    /// Which row in the evaluated columns this heap item is representing.
    row_index: usize,
}

impl HeapItemUnsafe {
    fn get_order_is_desc(&self) -> &[bool] {
        unsafe { self.order_is_desc_ptr.as_ref() }
    }

    fn get_eval_columns(&self, len: usize) -> &[RpnStackNode<'_>] {
        let offset_begin = self.eval_columns_buffer_offset;
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
            let lhs = lhs_node.get_scalar_ref(self.row_index);
            let rhs = rhs_node.get_scalar_ref(other.row_index);

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

    use cop_datatype::FieldTypeTp;

    use crate::coprocessor::codec::data_type::*;
    use crate::coprocessor::dag::batch::executors::util::mock_executor::MockExecutor;
    use crate::coprocessor::dag::expr::EvalWarnings;
    use crate::coprocessor::dag::rpn_expr::RpnExpressionBuilder;

    #[test]
    fn test_top_0() {
        let src_exec = MockExecutor::new(
            vec![FieldTypeTp::Double.into()],
            vec![BatchExecuteResult {
                data: LazyBatchColumnVec::from(vec![VectorValue::Real(vec![
                    None,
                    Real::new(7.0).ok(),
                    None,
                    None,
                ])]),
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
        assert_eq!(r.data.rows_len(), 0);
        assert!(r.is_drained.unwrap());
    }

    #[test]
    fn test_no_row() {
        let src_exec = MockExecutor::new(
            vec![FieldTypeTp::LongLong.into()],
            vec![
                BatchExecuteResult {
                    data: LazyBatchColumnVec::empty(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    data: LazyBatchColumnVec::empty(),
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
        assert_eq!(r.data.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 0);
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
                    data: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(vec![None, None, None]),
                        VectorValue::Int(vec![Some(-1), None, Some(1)]),
                        VectorValue::Real(vec![
                            Real::new(-1.0).ok(),
                            Real::new(2.0).ok(),
                            Real::new(4.0).ok(),
                        ]),
                    ]),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    data: LazyBatchColumnVec::empty(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    data: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(vec![Some(-1), Some(-10), Some(-10), Some(-10)]),
                        VectorValue::Int(vec![None, Some(10), None, Some(-10)]),
                        VectorValue::Real(vec![
                            None,
                            Real::new(3.0).ok(),
                            Real::new(-5.0).ok(),
                            Real::new(0.0).ok(),
                        ]),
                    ]),
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
        assert_eq!(r.data.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 7);
        assert_eq!(r.data.columns_len(), 3);
        assert_eq!(
            r.data[0].decoded().as_int_slice(),
            &[Some(-1), Some(-10), None, Some(-10), None, Some(-10), None]
        );
        assert_eq!(
            r.data[1].decoded().as_int_slice(),
            &[None, None, Some(-1), Some(-10), None, Some(10), Some(1)]
        );
        assert_eq!(
            r.data[2].decoded().as_real_slice(),
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
        assert_eq!(r.data.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 7);
        assert_eq!(r.data.columns_len(), 3);
        assert_eq!(
            r.data[0].decoded().as_int_slice(),
            &[Some(-1), Some(-10), Some(-10), Some(-10), None, None, None]
        );
        assert_eq!(
            r.data[1].decoded().as_int_slice(),
            &[None, None, Some(-10), Some(10), None, Some(-1), Some(1)]
        );
        assert_eq!(
            r.data[2].decoded().as_real_slice(),
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
        use crate::coprocessor::dag::rpn_expr::impl_arithmetic::{IntIntPlus, RpnFnArithmetic};
        use crate::coprocessor::dag::rpn_expr::impl_op::RpnFnIsNull;

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
                    .push_fn_call(RpnFnIsNull::<Int>::new(), FieldTypeTp::LongLong)
                    .build(),
                RpnExpressionBuilder::new().push_column_ref(0).build(),
                RpnExpressionBuilder::new()
                    .push_column_ref(1)
                    .push_constant(1)
                    .push_fn_call(RpnFnArithmetic::<IntIntPlus>::new(), FieldTypeTp::LongLong)
                    .build(),
            ],
            vec![false, false, true],
            5,
        );

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 5);
        assert_eq!(r.data.columns_len(), 3);
        assert_eq!(
            r.data[0].decoded().as_int_slice(),
            &[Some(-10), Some(-10), Some(-10), Some(-1), None]
        );
        assert_eq!(
            r.data[1].decoded().as_int_slice(),
            &[Some(10), Some(-10), None, None, Some(1)]
        );
        assert_eq!(
            r.data[2].decoded().as_real_slice(),
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
