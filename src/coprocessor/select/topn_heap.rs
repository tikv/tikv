// Copyright 2016 PingCAP, Inc.
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

use std::usize;
use std::collections::BinaryHeap;
use std::rc::Rc;
use std::cmp::{self, Ordering};
use std::cell::RefCell;
use tipb::expression::ByItem;

use coprocessor::codec::table::RowColsDict;
use coprocessor::codec::datum::Datum;
use coprocessor::Result;

use super::xeval::EvalContext;

pub struct SortRow {
    pub handle: i64,
    pub data: RowColsDict,
    pub key: Vec<Datum>,
    order_cols: Rc<Vec<ByItem>>,
    ctx: Rc<EvalContext>,
    err: Rc<RefCell<Option<String>>>,
}

impl SortRow {
    fn new(handle: i64,
           data: RowColsDict,
           key: Vec<Datum>,
           order_cols: Rc<Vec<ByItem>>,
           ctx: Rc<EvalContext>,
           err: Rc<RefCell<Option<String>>>)
           -> SortRow {
        SortRow {
            handle: handle,
            data: data,
            key: key,
            order_cols: order_cols,
            ctx: ctx,
            err: err,
        }
    }

    fn cmp_and_check(&self, right: &SortRow) -> Result<Ordering> {
        // check err
        try!(self.check_err());
        let values = self.key.iter().zip(right.key.iter());
        for (col, (v1, v2)) in self.order_cols.as_ref().iter().zip(values) {
            match v1.cmp(self.ctx.as_ref(), v2) {
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
                    try!(self.check_err());
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
    err: Rc<RefCell<Option<String>>>,
}

const HEAP_MAX_CAPACITY: usize = 1024;

impl TopNHeap {
    pub fn new(limit: usize) -> Result<TopNHeap> {
        if limit == usize::MAX {
            return Err(box_err!("invalid limit"));
        }
        let cap = cmp::min(limit, HEAP_MAX_CAPACITY);
        Ok(TopNHeap {
            rows: BinaryHeap::with_capacity(cap),
            limit: limit,
            err: Rc::new(RefCell::new(None)),
        })
    }

    #[inline]
    pub fn check_err(&self) -> Result<()> {
        if let Some(ref err_msg) = *self.err.as_ref().borrow() {
            return Err(box_err!(err_msg.to_owned()));
        }
        Ok(())
    }

    pub fn try_add_row(&mut self,
                       handle: i64,
                       data: RowColsDict,
                       values: Vec<Datum>,
                       order_cols: Rc<Vec<ByItem>>,
                       ctx: Rc<EvalContext>)
                       -> Result<()> {
        let row = SortRow::new(handle, data, values, order_cols, ctx, self.err.clone());
        // push into heap when heap is not full
        if self.rows.len() < self.limit {
            self.rows.push(row);
        } else {
            // swap top value with row when heap is full and current row is less than top data
            let mut top_data = self.rows.peek_mut().unwrap();
            let order = try!(row.cmp_and_check(&top_data));
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
