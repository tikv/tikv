// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::cmp::{self, Ordering};
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::usize;

use crate::coprocessor::codec::batch::LazyBatchColumnVec;
use crate::coprocessor::dag::expr::Result;

const HEAP_MAX_CAPACITY: usize = 1024;

/// SortRow wrapping a logical row will be used in the TopNHeap.
pub struct SortRow {
    table: Arc<RefCell<LazyBatchColumnVec>>,
    idx: usize,
    // (col_id, desc)
    order_cols: Arc<Vec<(usize, bool)>>,
}

impl SortRow {
    pub fn new(
        table: Arc<RefCell<LazyBatchColumnVec>>,
        idx: usize,
        order_cols: Arc<Vec<(usize, bool)>>,
    ) -> SortRow {
        SortRow {
            table,
            idx,
            order_cols,
        }
    }
}

/// TopNHeap wrapping a mutable reference to the greatest row on
/// a heap.
pub struct TopNHeap {
    pub rows: BinaryHeap<SortRow>,
    pub table: Arc<RefCell<LazyBatchColumnVec>>,
    limit: usize,
    //(col_id,desc)
    order_cols: Arc<Vec<(usize, bool)>>,
}

impl TopNHeap {
    /// Create an new `TopNHeap`
    pub fn new(
        limit: usize,
        data: LazyBatchColumnVec,
        order_cols: Arc<Vec<(usize, bool)>>,
    ) -> Result<TopNHeap> {
        if limit == usize::MAX || limit == 0 {
            return Err(box_err!("invalid limit:{}", limit));
        }
        let cap = cmp::min(limit, HEAP_MAX_CAPACITY);
        let binary_heap = BinaryHeap::with_capacity(cap);

        let current_rows = data.rows_len();

        let mut heap = TopNHeap {
            rows: binary_heap,
            limit,
            table: Arc::new(RefCell::new(data)),
            order_cols,
        };

        heap.push_logical_rows(current_rows);
        Ok(heap)
    }

    /// Pushes a batch into the binary heap.
    pub fn push_batch(&mut self, mut data: LazyBatchColumnVec) {
        if data.rows_len() == 0 {
            return;
        }
        let data_rows = data.rows_len();
        self.table.borrow_mut().append(&mut data);
        self.push_logical_rows(data_rows);
    }

    /// Consumes the `TopNHeap` and returns a `LazyBatchColumnVec` in sorted order.
    pub fn into_sorted_result(self) -> LazyBatchColumnVec {
        let mut sorted_rows: Vec<usize> =
            self.rows.into_sorted_vec().iter().map(|r| r.idx).collect();
        let mut table = self.table.as_ref().borrow().to_owned();
        let rows_len = sorted_rows.len();
        for row_id in 0..rows_len {
            while row_id != sorted_rows[row_id] {
                let dest_row = sorted_rows[row_id];
                sorted_rows.swap(row_id, dest_row);
                table.swap(row_id, dest_row);
            }
        }
        table
    }

    fn push_logical_rows(&mut self, rows: usize) {
        let start_row = self.rows.len();
        let end_row = start_row + rows;
        for row_id in start_row..end_row {
            self.push_logical_row(row_id);
        }
        if end_row > self.limit {
            self.table.borrow_mut().truncate(self.limit);
        }
    }

    fn push_logical_row(&mut self, row_id: usize) {
        let row = SortRow::new(self.table.clone(), row_id, self.order_cols.clone());
        // push into heap when heap is not full
        if self.rows.len() < self.limit {
            self.rows.push(row);
        } else {
            // swap top value with row when heap is full and current row is less than top data
            let top_data = self.rows.peek_mut().unwrap();
            if row < top_data {
                top_data.table.borrow_mut().swap(top_data.idx, row.idx);
            }
        }
    }
}

impl Ord for SortRow {
    fn cmp(&self, right: &SortRow) -> Ordering {
        for (col_id, desc) in self.order_cols.iter() {
            let lh = self.table.as_ref().borrow();;
            let rh = right.table.as_ref().borrow();
            let ord =
                lh[*col_id]
                    .decoded()
                    .compare_with(rh[*col_id].decoded(), self.idx, right.idx);
            if ord == Ordering::Equal {
                continue;
            }
            if *desc {
                return ord.reverse();
            }
            return ord;
        }
        Ordering::Equal
    }
}

impl PartialEq for SortRow {
    fn eq(&self, right: &SortRow) -> bool {
        self.cmp(right) == Ordering::Equal
    }
}

impl Eq for SortRow {}

impl PartialOrd for SortRow {
    fn partial_cmp(&self, rhs: &SortRow) -> Option<Ordering> {
        Some(self.cmp(rhs))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::coprocessor::codec::batch::LazyBatchColumnVec;
    use crate::coprocessor::codec::data_type::VectorValue;

    use super::*;

    #[test]
    fn test_topn_heap() {
        let order_cols = vec![(0, true), (1, false)];
        // 1-data1-1
        // 1-null-2
        // 2-null-3
        // 3-null-4
        let batch1 = LazyBatchColumnVec::from(vec![
            VectorValue::Int(vec![Some(1), Some(1), Some(2), Some(3)]),
            VectorValue::Bytes(vec![Some(b"data1".to_vec()), None, None, None]),
            VectorValue::Int(vec![Some(1), Some(2), Some(3), Some(4)]),
        ]);

        let mut topn_heap = TopNHeap::new(3, batch1, Arc::new(order_cols)).unwrap();
        // heap:
        // 3-null-4
        // 2-null-3,
        // 1-data1-1

        // push empty batch
        let batch2 = LazyBatchColumnVec::from(vec![
            VectorValue::Int(vec![]),
            VectorValue::Bytes(vec![]),
            VectorValue::Int(vec![]),
        ]);
        topn_heap.push_batch(batch2);

        // new data
        // 1-data1-5
        // 2-data2-6
        // 3-data3-7
        let batch3 = LazyBatchColumnVec::from(vec![
            VectorValue::Int(vec![Some(1), Some(2), Some(3)]),
            VectorValue::Bytes(vec![
                Some(b"data1".to_vec()),
                Some(b"data2".to_vec()),
                Some(b"data3".to_vec()),
            ]),
            VectorValue::Int(vec![Some(5), Some(6), Some(7)]),
        ]);
        topn_heap.push_batch(batch3);

        // heap data:
        // 3-data3-7
        // 3-null-4
        // 2-data2-6
        let res = topn_heap.into_sorted_result();

        let expect = LazyBatchColumnVec::from(vec![
            VectorValue::Int(vec![Some(3), Some(3), Some(2)]),
            VectorValue::Bytes(vec![Some(b"data3".to_vec()), None, Some(b"data2".to_vec())]),
            VectorValue::Int(vec![Some(7), Some(4), Some(6)]),
        ]);
        assert_eq!(res.columns_len(), expect.columns_len());
        for col_id in 0..res.columns_len() {
            assert_eq!(res[col_id].decoded(), expect[col_id].decoded());
        }
    }

    #[test]
    fn test_topn_heap_with_few_data() {
        let order_cols = vec![(0, true), (1, false)];
        let batch1 = LazyBatchColumnVec::from(vec![
            VectorValue::Int(vec![]),
            VectorValue::Bytes(vec![]),
            VectorValue::Int(vec![]),
        ]);

        let mut topn_heap = TopNHeap::new(10, batch1, Arc::new(order_cols)).unwrap();
        // new data:
        // null-data1-1
        // 1-null-2
        // 2-null-3
        // 3-null-4
        let batch2 = LazyBatchColumnVec::from(vec![
            VectorValue::Int(vec![None, Some(1), Some(2), Some(3)]),
            VectorValue::Bytes(vec![Some(b"data1".to_vec()), None, None, None]),
            VectorValue::Int(vec![Some(1), Some(2), Some(3), Some(4)]),
        ]);

        topn_heap.push_batch(batch2);
        // data in heap:
        // null-data1-1
        // 3-null-4
        // 2-null-3
        // 1-null-2
        let expect = LazyBatchColumnVec::from(vec![
            VectorValue::Int(vec![None, Some(3), Some(2), Some(1)]),
            VectorValue::Bytes(vec![Some(b"data1".to_vec()), None, None, None]),
            VectorValue::Int(vec![Some(1), Some(4), Some(3), Some(2)]),
        ]);
        let res = topn_heap.into_sorted_result();
        assert_eq!(res.columns_len(), expect.columns_len());
        for col_id in 0..res.columns_len() {
            assert_eq!(res[col_id].decoded(), expect[col_id].decoded());
        }
    }

    #[test]
    fn test_topn_with_invalid_limit() {
        let invalid_limits = vec![0, usize::MAX];
        for limit in invalid_limits {
            assert!(TopNHeap::new(limit, LazyBatchColumnVec::empty(), Arc::new(vec![]),).is_err());
        }
    }
}
