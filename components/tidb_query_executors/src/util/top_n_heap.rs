// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp::Ordering, collections::BinaryHeap, ptr::NonNull, sync::Arc};

use tidb_query_common::Result;
use tidb_query_datatype::codec::{
    batch::{LazyBatchColumn, LazyBatchColumnVec},
    data_type::*,
};
use tidb_query_expr::RpnStackNode;
use tipb::FieldType;

/// TopNHeap is the common data structure used in TopN-like executors.
pub struct TopNHeap {
    /// The maximum number of rows in the heap.
    n: usize,
    /// The heap.
    heap: BinaryHeap<HeapItemUnsafe>,
}

impl TopNHeap {
    /// parameters:
    /// - n: The maximum number of rows in the heaps
    /// note: to avoid large N causing OOM, the initial capacity will be limited
    /// up to 1024.
    pub fn new(n: usize) -> Self {
        Self {
            n,
            // Avoid large N causing OOM
            heap: BinaryHeap::with_capacity(n.min(1024)),
        }
    }

    pub fn add_row(&mut self, row: HeapItemUnsafe) -> Result<()> {
        if self.heap.len() < self.n {
            // HeapItemUnsafe must be checked valid to compare in advance, or else it may
            // panic inside BinaryHeap.
            row.cmp_sort_key(&row)?;

            // Push into heap when heap is not full.
            self.heap.push(row);
        } else {
            // Swap the greatest row in the heap if this row is smaller than that row.
            let mut greatest_row = self.heap.peek_mut().unwrap();
            if row.cmp_sort_key(&greatest_row)? == Ordering::Less {
                *greatest_row = row;
            }
        }

        Ok(())
    }

    #[allow(clippy::clone_on_copy)]
    pub fn take_all_append_to(&mut self, result: &mut LazyBatchColumnVec) {
        let heap = std::mem::take(&mut self.heap);
        let sorted_items = heap.into_sorted_vec();
        if sorted_items.is_empty() {
            return;
        }

        // If it is a pure empty LazyBatchColumnVec, we need create columns on it first.
        if result.columns_len() == 0 {
            *result = sorted_items[0]
                .source_data
                .physical_columns
                .clone_empty(self.heap.len());
        }
        // todo: check schema is equal
        assert_eq!(
            result.columns_len(),
            sorted_items[0].source_data.physical_columns.columns_len(),
        );

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
                    match_template::match_template! {
                        TT = [
                            Int,
                            Real,
                            Duration,
                            Decimal,
                            DateTime,
                            Bytes => BytesRef,
                            Json => JsonRef,
                            Enum => EnumRef,
                            Set => SetRef,
                            VectorFloat32 => VectorFloat32Ref,
                        ],
                        match dest_vector_value {
                            VectorValue::TT(dest_column) => {
                                for item in &sorted_items {
                                    let src: &VectorValue = item.source_data.physical_columns[column_index].decoded();
                                    let src_ref = TT::borrow_vector_value(src);
                                    // TODO: This clone is not necessary.
                                    dest_column.push(src_ref.get_option_ref(item.source_data.logical_rows[item.logical_row_index]).map(|x| x.into_owned_value()));
                                }
                            },
                        }
                    }
                }
            }
        }

        result.assert_columns_equal_length();
    }

    #[allow(clippy::clone_on_copy)]
    pub fn take_all(&mut self) -> LazyBatchColumnVec {
        let mut result = LazyBatchColumnVec::empty();
        self.take_all_append_to(&mut result);
        result
    }
}

pub struct HeapItemSourceData {
    pub physical_columns: LazyBatchColumnVec,
    pub logical_rows: Vec<usize>,
}

/// The item in the heap of `BatchTopNExecutor`.
///
/// WARN: The content of this structure is valid only if `BatchTopNExecutor` is
/// valid (i.e. not dropped). Thus it is called unsafe.
pub struct HeapItemUnsafe {
    /// A pointer to the `order_is_desc` field in `BatchTopNExecutor`.
    pub order_is_desc_ptr: NonNull<[bool]>,

    /// A pointer to the `order_exprs_field_type` field in `order_exprs`.
    pub order_exprs_field_type_ptr: NonNull<[FieldType]>,

    /// The source data that evaluated column in this structure is using.
    pub source_data: Arc<HeapItemSourceData>,

    /// A pointer to the `eval_columns_buffer` field in `BatchTopNExecutor`.
    pub eval_columns_buffer_ptr: NonNull<Vec<RpnStackNode<'static>>>,

    /// The begin offset of the evaluated columns stored in the buffer.
    ///
    /// The length of evaluated columns in the buffer is `order_is_desc.len()`.
    pub eval_columns_offset: usize,

    /// Which logical row in the evaluated columns this heap item is
    /// representing.
    pub logical_row_index: usize,
}

impl HeapItemUnsafe {
    fn get_order_is_desc(&self) -> &[bool] {
        unsafe { self.order_is_desc_ptr.as_ref() }
    }

    fn get_order_exprs_field_type(&self) -> &[FieldType] {
        unsafe { self.order_exprs_field_type_ptr.as_ref() }
    }

    fn get_eval_columns(&self, len: usize) -> &[RpnStackNode<'_>] {
        let offset_begin = self.eval_columns_offset;
        let offset_end = offset_begin + len;
        let vec_buf = unsafe { self.eval_columns_buffer_ptr.as_ref() };
        &vec_buf[offset_begin..offset_end]
    }

    fn cmp_sort_key(&self, other: &Self) -> Result<Ordering> {
        // Only debug assert because this function is called pretty frequently.
        debug_assert_eq!(self.get_order_is_desc(), other.get_order_is_desc());

        let order_is_desc = self.get_order_is_desc();
        let order_exprs_field_type = self.get_order_exprs_field_type();
        let columns_len = order_is_desc.len();
        let eval_columns_lhs = self.get_eval_columns(columns_len);
        let eval_columns_rhs = other.get_eval_columns(columns_len);

        for column_idx in 0..columns_len {
            let lhs_node = &eval_columns_lhs[column_idx];
            let rhs_node = &eval_columns_rhs[column_idx];
            let lhs = lhs_node.get_logical_scalar_ref(self.logical_row_index);
            let rhs = rhs_node.get_logical_scalar_ref(other.logical_row_index);

            // There is panic inside, but will never panic, since the data type of
            // corresponding column should be consistent for each
            // `HeapItemUnsafe`.
            let ord = lhs.cmp_sort_key(&rhs, &order_exprs_field_type[column_idx])?;

            if ord == Ordering::Equal {
                continue;
            }
            return if !order_is_desc[column_idx] {
                Ok(ord)
            } else {
                Ok(ord.reverse())
            };
        }

        Ok(Ordering::Equal)
    }
}

/// WARN: HeapItemUnsafe implements partial ordering. It panics when Collator
/// fails to parse. So make sure that it is valid before putting it into a heap.
impl Ord for HeapItemUnsafe {
    fn cmp(&self, other: &Self) -> Ordering {
        self.cmp_sort_key(other).unwrap()
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
