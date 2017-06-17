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

// FIXME: remove later
#![allow(dead_code)]

use std::rc::Rc;
use std::collections::HashSet;

use tipb::executor::TopN;
use tipb::schema::ColumnInfo;
use tipb::expression::{Expr, ExprType, ByItem};
use util::codec::number::NumberDecoder;
use util::xeval::{Evaluator, EvalContext};

use super::{Executor, Row};
use super::super::Result;
use super::super::endpoint::{TopNHeap, inflate_with_col};

struct ExprColumnRefVisitor {
    col_ids: HashSet<i64>,
}

impl ExprColumnRefVisitor {
    fn new() -> ExprColumnRefVisitor {
        ExprColumnRefVisitor { col_ids: HashSet::new() }
    }

    fn visit_expr<'e>(&mut self, expr: &'e Expr) -> Result<()> {
        if expr.get_tp() == ExprType::ColumnRef {
            self.col_ids.insert(box_try!(expr.get_val().decode_i64()));
        } else {
            for sub_expr in expr.get_children() {
                try!(self.visit_expr(sub_expr));
            }
        }
        Ok(())
    }
}

pub struct TopNExecutor {
    order_bys: Rc<Vec<ByItem>>,
    heap: TopNHeap,
    columns: Vec<ColumnInfo>,
    cursor: usize,
    executed: bool,
    ctx: Rc<EvalContext>,
    src: Box<Executor>,
    eval: Evaluator,
}

impl TopNExecutor {
    pub fn new(mut meta: TopN,
               ctx: Rc<EvalContext>,
               columns_info: &[ColumnInfo],
               src: Box<Executor>)
               -> Result<TopNExecutor> {
        let order_bys = meta.take_order_by().into_vec();

        let mut visitor = ExprColumnRefVisitor::new();
        for order_by in order_bys.iter() {
            try!(visitor.visit_expr(order_by.get_expr()));
        }
        let columns = columns_info.iter()
            .filter(|col| visitor.col_ids.get(&col.get_column_id()).is_some())
            .cloned()
            .collect();

        Ok(TopNExecutor {
            order_bys: Rc::new(order_bys),
            heap: try!(TopNHeap::new(meta.get_limit() as usize)),
            columns: columns,
            cursor: 0,
            executed: false,
            ctx: ctx,
            src: src,
            eval: Evaluator::default(),
        })
    }

    fn inner_next(&mut self) -> Result<Option<()>> {
        if let Some(row) = try!(self.src.next()) {
            try!(inflate_with_col(&mut self.eval,
                                  &self.ctx,
                                  &row.data,
                                  &self.columns,
                                  row.handle));
            let mut sort_keys = Vec::with_capacity(self.order_bys.len());
            for col in self.order_bys.as_ref().iter() {
                let v = box_try!(self.eval.eval(&self.ctx, col.get_expr()));
                sort_keys.push(v);
            }
            self.heap
                .try_add_row(row.handle,
                             self.order_bys.clone(),
                             self.ctx.clone(),
                             row.data,
                             sort_keys)
                .unwrap();
            Ok(Some(()))
        } else {
            Ok(None)
        }
    }
}

impl Executor for TopNExecutor {
    fn next(&mut self) -> Result<Option<Row>> {
        if !self.executed {
            while self.inner_next().unwrap().is_some() {}
            self.executed = true;
        }
        if self.cursor >= self.heap.rows.len() {
            return Ok(None);
        }
        self.cursor += 1;
        let sort_row = self.heap.rows.pop().unwrap();
        Ok(Some(Row {
            handle: sort_row.handle,
            data: sort_row.data,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use util::codec::Datum;
    use util::collections::HashMap;
    use util::codec::table::RowColsDict;
    use util::codec::number::NumberEncoder;

    // build expr
    use tipb::expression::{Expr, ExprType};

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
        let order_cols = Rc::new(order_cols);
        let ctx = Rc::new(EvalContext::default());
        let mut topn_heap = TopNHeap::new(5).unwrap();
        let test_data = vec![
            (1, String::from("data1"), Datum::Null, Datum::I64(1)),
            (2, String::from("data2"), Datum::Bytes(b"name:0".to_vec()), Datum::I64(2)),
            (3, String::from("data3"), Datum::Bytes(b"name:3".to_vec()), Datum::I64(1)),
            (4, String::from("data4"), Datum::Bytes(b"name:3".to_vec()), Datum::I64(2)),
            (5, String::from("data5"), Datum::Bytes(b"name:0".to_vec()), Datum::I64(6)),
            (6, String::from("data6"), Datum::Bytes(b"name:0".to_vec()), Datum::I64(4)),
            (7, String::from("data7"), Datum::Bytes(b"name:7".to_vec()), Datum::I64(2)),
            (8, String::from("data8"), Datum::Bytes(b"name:8".to_vec()), Datum::I64(2)),
            (9, String::from("data9"), Datum::Bytes(b"name:9".to_vec()), Datum::I64(2)),
        ];

        let exp = vec![
            (9, String::from("data9"), Datum::Bytes(b"name:9".to_vec()), Datum::I64(2)),
            (8, String::from("data8"), Datum::Bytes(b"name:8".to_vec()), Datum::I64(2)),
            (7, String::from("data7"), Datum::Bytes(b"name:7".to_vec()), Datum::I64(2)),
            (3, String::from("data3"), Datum::Bytes(b"name:3".to_vec()), Datum::I64(1)),
            (4, String::from("data4"), Datum::Bytes(b"name:3".to_vec()), Datum::I64(2)),
        ];

        for (handle, data, name, count) in test_data {
            let cur_key: Vec<Datum> = vec![name, count];
            let row_data = RowColsDict::new(HashMap::default(), data.into_bytes());
            topn_heap.try_add_row(handle as i64,
                             order_cols.clone(),
                             ctx.clone(),
                             row_data,
                             cur_key)
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
        let order_cols = Rc::new(order_cols);
        let ctx = Rc::new(EvalContext::default());
        let mut topn_heap = TopNHeap::new(5).unwrap();

        let std_key: Vec<Datum> = vec![Datum::Bytes(b"aaa".to_vec()), Datum::I64(2)];
        let row_data = RowColsDict::new(HashMap::default(), b"name:1".to_vec());
        topn_heap.try_add_row(0 as i64, order_cols.clone(), ctx.clone(), row_data, std_key)
            .unwrap();

        let std_key2: Vec<Datum> = vec![Datum::Bytes(b"aaa".to_vec()), Datum::I64(3)];
        let row_data2 = RowColsDict::new(HashMap::default(), b"name:2".to_vec());
        topn_heap.try_add_row(0 as i64,
                         order_cols.clone(),
                         ctx.clone(),
                         row_data2,
                         std_key2)
            .unwrap();

        let bad_key1: Vec<Datum> = vec![Datum::I64(2), Datum::Bytes(b"aaa".to_vec())];
        let row_data3 = RowColsDict::new(HashMap::default(), b"name:3".to_vec());

        assert!(topn_heap.try_add_row(0 as i64,
                         order_cols.clone(),
                         ctx.clone(),
                         row_data3,
                         bad_key1)
            .is_err());

        assert!(topn_heap.into_sorted_vec().is_err());
    }

    #[test]
    fn test_topn_heap_with_few_data() {
        let mut order_cols = Vec::new();
        order_cols.push(new_order_by(0, true));
        order_cols.push(new_order_by(1, false));
        let order_cols = Rc::new(order_cols);
        let ctx = Rc::new(EvalContext::default());
        let mut topn_heap = TopNHeap::new(10).unwrap();
        let test_data = vec![
            (3, String::from("data3"), Datum::Bytes(b"name:3".to_vec()), Datum::I64(1)),
            (4, String::from("data4"), Datum::Bytes(b"name:3".to_vec()), Datum::I64(2)),
            (7, String::from("data7"), Datum::Bytes(b"name:7".to_vec()), Datum::I64(2)),
            (8, String::from("data8"), Datum::Bytes(b"name:8".to_vec()), Datum::I64(2)),
            (9, String::from("data9"), Datum::Bytes(b"name:9".to_vec()), Datum::I64(2)),
        ];

        let exp = vec![
            (9, String::from("data9"), Datum::Bytes(b"name:9".to_vec()), Datum::I64(2)),
            (8, String::from("data8"), Datum::Bytes(b"name:8".to_vec()), Datum::I64(2)),
            (7, String::from("data7"), Datum::Bytes(b"name:7".to_vec()), Datum::I64(2)),
            (3, String::from("data3"), Datum::Bytes(b"name:3".to_vec()), Datum::I64(1)),
            (4, String::from("data4"), Datum::Bytes(b"name:3".to_vec()), Datum::I64(2)),
        ];

        for (handle, data, name, count) in test_data {
            let cur_key: Vec<Datum> = vec![name, count];
            let row_data = RowColsDict::new(HashMap::default(), data.into_bytes());
            topn_heap.try_add_row(handle as i64,
                             order_cols.clone(),
                             ctx.clone(),
                             row_data,
                             cur_key)
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
}