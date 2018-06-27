// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cell::RefCell;
use std::cmp::{self, Ordering};
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::usize;
use tipb::expression::ByItem;

use coprocessor::codec::datum::Datum;
use coprocessor::codec::table::RowColsDict;
use coprocessor::dag::expr::{EvalContext, Result};

const HEAP_MAX_CAPACITY: usize = 1024;

pub struct SortRow {
    pub handle: i64,
    pub data: RowColsDict,
    pub key: Vec<Datum>,
    order_cols: Arc<Vec<ByItem>>,
    eval_ctx: Arc<RefCell<EvalContext>>,
    err: Arc<RefCell<Option<String>>>,
}

impl SortRow {
    fn new(
        handle: i64,
        data: RowColsDict,
        key: Vec<Datum>,
        order_cols: Arc<Vec<ByItem>>,
        ctx: Arc<RefCell<EvalContext>>,
        err: Arc<RefCell<Option<String>>>,
    ) -> SortRow {
        SortRow {
            handle,
            data,
            key,
            order_cols,
            eval_ctx: ctx,
            err,
        }
    }

    fn cmp_and_check(&self, right: &SortRow) -> Result<Ordering> {
        // check err
        self.check_err()?;
        let values = self.key.iter().zip(right.key.iter());
        let mut ctx = self.eval_ctx.borrow_mut();
        for (col, (v1, v2)) in self.order_cols.as_ref().iter().zip(values) {
            match v1.cmp(&mut ctx, v2) {
                Ok(Ordering::Equal) => {
                    continue;
                }
                Ok(order) => {
                    if col.get_desc() {
                        return Ok(order.reverse());
                    }
                    return Ok(order);
                }
                Err(err) => {
                    self.set_err(format!("cmp failed with:{:?}", err));
                    self.check_err()?;
                }
            }
        }
        Ok(Ordering::Equal)
    }

    #[inline]
    fn check_err(&self) -> Result<()> {
        if let Some(ref err_msg) = *self.err.as_ref().borrow() {
            return Err(box_err!(err_msg.to_owned()));
        }
        Ok(())
    }

    fn set_err(&self, err_msg: String) {
        *self.err.borrow_mut() = Some(err_msg);
    }
}

pub struct TopNHeap {
    pub rows: BinaryHeap<SortRow>,
    limit: usize,
    err: Arc<RefCell<Option<String>>>,
    ctx: Arc<RefCell<EvalContext>>,
}

impl TopNHeap {
    pub fn new(limit: usize, ctx: Arc<RefCell<EvalContext>>) -> Result<TopNHeap> {
        if limit == usize::MAX {
            return Err(box_err!("invalid limit"));
        }
        let cap = cmp::min(limit, HEAP_MAX_CAPACITY);
        Ok(TopNHeap {
            rows: BinaryHeap::with_capacity(cap),
            limit,
            err: Arc::new(RefCell::new(None)),
            ctx,
        })
    }

    #[inline]
    pub fn check_err(&self) -> Result<()> {
        if let Some(ref err_msg) = *self.err.as_ref().borrow() {
            return Err(box_err!(err_msg.to_owned()));
        }
        Ok(())
    }

    pub fn try_add_row(
        &mut self,
        handle: i64,
        data: RowColsDict,
        values: Vec<Datum>,
        order_cols: Arc<Vec<ByItem>>,
    ) -> Result<()> {
        if self.limit == 0 {
            return Ok(());
        }
        let row = SortRow::new(
            handle,
            data,
            values,
            order_cols,
            Arc::clone(&self.ctx),
            Arc::clone(&self.err),
        );
        // push into heap when heap is not full
        if self.rows.len() < self.limit {
            self.rows.push(row);
        } else {
            // swap top value with row when heap is full and current row is less than top data
            let mut top_data = self.rows.peek_mut().unwrap();
            let order = row.cmp_and_check(&top_data)?;
            if Ordering::Less == order {
                *top_data = row;
            }
        }
        self.check_err()
    }

    pub fn into_sorted_vec(self) -> Result<Vec<SortRow>> {
        let sorted_data = self.rows.into_sorted_vec();
        // check is needed here since err may caused by any call of cmp
        if let Some(ref err_msg) = *self.err.as_ref().borrow() {
            return Err(box_err!(err_msg.to_owned()));
        }
        Ok(sorted_data)
    }
}

impl Ord for SortRow {
    fn cmp(&self, right: &SortRow) -> Ordering {
        if let Ok(order) = self.cmp_and_check(right) {
            return order;
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
    use std::cell::RefCell;
    use std::sync::Arc;

    use tipb::expression::{ByItem, Expr, ExprType};

    use coprocessor::codec::table::RowColsDict;
    use coprocessor::codec::Datum;
    use coprocessor::dag::expr::EvalContext;
    use util::codec::number::*;
    use util::collections::HashMap;

    use super::*;

    fn new_order_by(col_id: i64, desc: bool) -> ByItem {
        let mut item = ByItem::new();
        let mut expr = Expr::new();
        expr.set_tp(ExprType::ColumnRef);
        expr.mut_val().encode_i64(col_id).unwrap();
        item.set_expr(expr);
        item.set_desc(desc);
        item
    }

    #[test]
    fn test_topn_heap() {
        let mut order_cols = Vec::new();
        order_cols.push(new_order_by(0, true));
        order_cols.push(new_order_by(1, false));
        let order_cols = Arc::new(order_cols);
        let mut topn_heap =
            TopNHeap::new(5, Arc::new(RefCell::new(EvalContext::default()))).unwrap();
        let test_data = vec![
            (1, String::from("data1"), Datum::Null, Datum::I64(1)),
            (
                2,
                String::from("data2"),
                Datum::Bytes(b"name:0".to_vec()),
                Datum::I64(2),
            ),
            (
                3,
                String::from("data3"),
                Datum::Bytes(b"name:3".to_vec()),
                Datum::I64(1),
            ),
            (
                4,
                String::from("data4"),
                Datum::Bytes(b"name:3".to_vec()),
                Datum::I64(2),
            ),
            (
                5,
                String::from("data5"),
                Datum::Bytes(b"name:0".to_vec()),
                Datum::I64(6),
            ),
            (
                6,
                String::from("data6"),
                Datum::Bytes(b"name:0".to_vec()),
                Datum::I64(4),
            ),
            (
                7,
                String::from("data7"),
                Datum::Bytes(b"name:7".to_vec()),
                Datum::I64(2),
            ),
            (
                8,
                String::from("data8"),
                Datum::Bytes(b"name:8".to_vec()),
                Datum::I64(2),
            ),
            (
                9,
                String::from("data9"),
                Datum::Bytes(b"name:9".to_vec()),
                Datum::I64(2),
            ),
        ];

        let exp = vec![
            (
                9,
                String::from("data9"),
                Datum::Bytes(b"name:9".to_vec()),
                Datum::I64(2),
            ),
            (
                8,
                String::from("data8"),
                Datum::Bytes(b"name:8".to_vec()),
                Datum::I64(2),
            ),
            (
                7,
                String::from("data7"),
                Datum::Bytes(b"name:7".to_vec()),
                Datum::I64(2),
            ),
            (
                3,
                String::from("data3"),
                Datum::Bytes(b"name:3".to_vec()),
                Datum::I64(1),
            ),
            (
                4,
                String::from("data4"),
                Datum::Bytes(b"name:3".to_vec()),
                Datum::I64(2),
            ),
        ];

        for (handle, data, name, count) in test_data {
            let cur_key: Vec<Datum> = vec![name, count];
            let row_data = RowColsDict::new(HashMap::default(), data.into_bytes());
            topn_heap
                .try_add_row(
                    i64::from(handle),
                    row_data,
                    cur_key,
                    Arc::clone(&order_cols),
                )
                .unwrap();
        }
        let result = topn_heap.into_sorted_vec().unwrap();
        assert_eq!(result.len(), exp.len());
        for (row, (handle, _, name, count)) in result.iter().zip(exp) {
            let exp_keys: Vec<Datum> = vec![name, count];
            assert_eq!(row.handle, handle);
            assert_eq!(row.key, exp_keys);
        }
    }

    #[test]
    fn test_topn_heap_with_cmp_error() {
        let mut order_cols = Vec::new();
        order_cols.push(new_order_by(0, true));
        order_cols.push(new_order_by(1, false));
        let order_cols = Arc::new(order_cols);
        let mut topn_heap =
            TopNHeap::new(5, Arc::new(RefCell::new(EvalContext::default()))).unwrap();

        let std_key: Vec<Datum> = vec![Datum::Bytes(b"aaa".to_vec()), Datum::I64(2)];
        let row_data = RowColsDict::new(HashMap::default(), b"name:1".to_vec());
        topn_heap
            .try_add_row(0 as i64, row_data, std_key, Arc::clone(&order_cols))
            .unwrap();

        let std_key2: Vec<Datum> = vec![Datum::Bytes(b"aaa".to_vec()), Datum::I64(3)];
        let row_data2 = RowColsDict::new(HashMap::default(), b"name:2".to_vec());
        topn_heap
            .try_add_row(0 as i64, row_data2, std_key2, Arc::clone(&order_cols))
            .unwrap();

        let bad_key1: Vec<Datum> = vec![Datum::I64(2), Datum::Bytes(b"aaa".to_vec())];
        let row_data3 = RowColsDict::new(HashMap::default(), b"name:3".to_vec());

        assert!(
            topn_heap
                .try_add_row(0 as i64, row_data3, bad_key1, Arc::clone(&order_cols))
                .is_err()
        );

        assert!(topn_heap.into_sorted_vec().is_err());
    }

    #[test]
    fn test_topn_heap_with_few_data() {
        let mut order_cols = Vec::new();
        order_cols.push(new_order_by(0, true));
        order_cols.push(new_order_by(1, false));
        let order_cols = Arc::new(order_cols);
        let mut topn_heap =
            TopNHeap::new(10, Arc::new(RefCell::new(EvalContext::default()))).unwrap();
        let test_data = vec![
            (
                3,
                String::from("data3"),
                Datum::Bytes(b"name:3".to_vec()),
                Datum::I64(1),
            ),
            (
                4,
                String::from("data4"),
                Datum::Bytes(b"name:3".to_vec()),
                Datum::I64(2),
            ),
            (
                7,
                String::from("data7"),
                Datum::Bytes(b"name:7".to_vec()),
                Datum::I64(2),
            ),
            (
                8,
                String::from("data8"),
                Datum::Bytes(b"name:8".to_vec()),
                Datum::I64(2),
            ),
            (
                9,
                String::from("data9"),
                Datum::Bytes(b"name:9".to_vec()),
                Datum::I64(2),
            ),
        ];

        let exp = vec![
            (
                9,
                String::from("data9"),
                Datum::Bytes(b"name:9".to_vec()),
                Datum::I64(2),
            ),
            (
                8,
                String::from("data8"),
                Datum::Bytes(b"name:8".to_vec()),
                Datum::I64(2),
            ),
            (
                7,
                String::from("data7"),
                Datum::Bytes(b"name:7".to_vec()),
                Datum::I64(2),
            ),
            (
                3,
                String::from("data3"),
                Datum::Bytes(b"name:3".to_vec()),
                Datum::I64(1),
            ),
            (
                4,
                String::from("data4"),
                Datum::Bytes(b"name:3".to_vec()),
                Datum::I64(2),
            ),
        ];

        for (handle, data, name, count) in test_data {
            let cur_key: Vec<Datum> = vec![name, count];
            let row_data = RowColsDict::new(HashMap::default(), data.into_bytes());
            topn_heap
                .try_add_row(
                    i64::from(handle),
                    row_data,
                    cur_key,
                    Arc::clone(&order_cols),
                )
                .unwrap();
        }

        let result = topn_heap.into_sorted_vec().unwrap();
        assert_eq!(result.len(), exp.len());
        for (row, (handle, _, name, count)) in result.iter().zip(exp) {
            let exp_keys: Vec<Datum> = vec![name, count];
            assert_eq!(row.handle, handle);
            assert_eq!(row.key, exp_keys);
        }
    }

    #[test]
    fn test_topn_limit_oom() {
        let topn_heap = TopNHeap::new(
            usize::MAX - 1,
            Arc::new(RefCell::new(EvalContext::default())),
        );
        assert!(topn_heap.is_ok());
        let topn_heap = TopNHeap::new(usize::MAX, Arc::new(RefCell::new(EvalContext::default())));
        assert!(topn_heap.is_err());
    }

    #[test]
    fn test_topn_with_empty_limit() {
        let mut topn_heap =
            TopNHeap::new(0, Arc::new(RefCell::new(EvalContext::default()))).unwrap();
        let cur_key: Vec<Datum> = vec![Datum::I64(1), Datum::I64(2)];
        let row_data = RowColsDict::new(HashMap::default(), b"ssss".to_vec());
        topn_heap
            .try_add_row(i64::from(1), row_data, cur_key, Arc::new(Vec::default()))
            .unwrap();

        assert!(topn_heap.into_sorted_vec().unwrap().is_empty());
    }
}
