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

use std::usize;
use std::rc::Rc;
use std::collections::HashSet;
use std::vec::IntoIter;

use tipb::executor::TopN;
use tipb::schema::ColumnInfo;
use tipb::expression::{Expr, ExprType, ByItem};
use util::codec::number::NumberDecoder;
use util::xeval::{Evaluator, EvalContext};

use super::{Executor, Row};
use super::super::Result;
use super::super::endpoint::{inflate_with_col, SortRow, TopNHeap};

struct ExprColumnRefVisitor {
    col_ids: HashSet<i64>,
}

impl ExprColumnRefVisitor {
    fn new() -> ExprColumnRefVisitor {
        ExprColumnRefVisitor { col_ids: HashSet::new() }
    }

    fn visit(&mut self, expr: &Expr) -> Result<()> {
        if expr.get_tp() == ExprType::ColumnRef {
            self.col_ids.insert(box_try!(expr.get_val().decode_i64()));
        } else {
            for sub_expr in expr.get_children() {
                try!(self.visit(sub_expr));
            }
        }
        Ok(())
    }
}

pub struct TopNExecutor<'a> {
    order_by: Rc<Vec<ByItem>>,
    columns: Vec<ColumnInfo>,
    heap: Option<TopNHeap>,
    iter: Option<IntoIter<SortRow>>,
    ctx: Rc<EvalContext>,
    src: Box<Executor + 'a>,
}

impl<'a> TopNExecutor<'a> {
    pub fn new(mut meta: TopN,
               ctx: Rc<EvalContext>,
               columns_info: &[ColumnInfo],
               src: Box<Executor + 'a>)
               -> Result<TopNExecutor<'a>> {
        let order_by = meta.take_order_by().into_vec();

        let mut visitor = ExprColumnRefVisitor::new();
        for by_item in &order_by {
            try!(visitor.visit(by_item.get_expr()));
        }
        let columns = columns_info.iter()
            .filter(|col| visitor.col_ids.get(&col.get_column_id()).is_some())
            .cloned()
            .collect();

        Ok(TopNExecutor {
            order_by: Rc::new(order_by),
            heap: Some(try!(TopNHeap::new(meta.get_limit() as usize))),
            columns: columns,
            iter: None,
            ctx: ctx,
            src: src,
        })
    }

    fn fetch_all(&mut self) -> Result<()> {
        while let Some(row) = try!(self.src.next()) {
            let mut eval = Evaluator::default();
            try!(inflate_with_col(&mut eval, &self.ctx, &row.data, &self.columns, row.handle));
            let mut ob_values = Vec::with_capacity(self.order_by.len());
            for by_item in self.order_by.as_ref().iter() {
                let v = box_try!(eval.eval(&self.ctx, by_item.get_expr()));
                ob_values.push(v);
            }
            try!(self.heap.as_mut().unwrap().try_add_row(row.handle,
                                                         row.data,
                                                         ob_values,
                                                         self.order_by.clone(),
                                                         self.ctx.clone()));
        }
        Ok(())
    }
}

impl<'a> Executor for TopNExecutor<'a> {
    fn next(&mut self) -> Result<Option<Row>> {
        if self.iter.is_none() {
            try!(self.fetch_all());
            self.iter = Some(try!(self.heap.take().unwrap().into_sorted_vec()).into_iter());
        }
        let iter = self.iter.as_mut().unwrap();
        match iter.next() {
            Some(sort_row) => {
                Ok(Some(Row {
                    handle: sort_row.handle,
                    data: sort_row.data,
                }))
            }
            None => Ok(None),
        }
    }
}


#[cfg(test)]
pub mod test {
    use super::*;
    use super::super::table_scan::TableScanExecutor;
    use super::super::scanner::test::{TestStore, get_range, new_col_info};
    use util::codec::Datum;
    use util::collections::HashMap;
    use util::codec::table::{self, RowColsDict};
    use util::codec::number::NumberEncoder;
    use util::codec::mysql::types;
    use storage::Statistics;

    use tipb::executor::TableScan;
    use tipb::expression::{Expr, ExprType};

    use protobuf::RepeatedField;
    use kvproto::kvrpcpb::IsolationLevel;

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
    pub fn test_topn_heap() {
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
            let ob_values: Vec<Datum> = vec![name, count];
            let row_data = RowColsDict::new(HashMap::default(), data.into_bytes());
            topn_heap.try_add_row(handle as i64,
                             row_data,
                             ob_values,
                             order_cols.clone(),
                             ctx.clone())
                .unwrap();
        }
        let result = topn_heap.into_sorted_vec().unwrap();
        assert_eq!(result.len(), exp.len());
        for (row, (handle, _, name, count)) in result.iter().zip(exp) {
            let exp_key: Vec<Datum> = vec![name, count];
            assert_eq!(row.handle, handle);
            assert_eq!(row.key, exp_key);
        }
    }

    #[test]
    fn test_topn_heap_with_cmp_error() {
        let mut order_cols = Vec::new();
        order_cols.push(new_order_by(0, false));
        order_cols.push(new_order_by(1, true));
        let order_cols = Rc::new(order_cols);
        let ctx = Rc::new(EvalContext::default());
        let mut topn_heap = TopNHeap::new(5).unwrap();

        let ob_values1: Vec<Datum> = vec![Datum::Bytes(b"aaa".to_vec()), Datum::I64(2)];
        let row_data = RowColsDict::new(HashMap::default(), b"name:1".to_vec());
        topn_heap.try_add_row(0 as i64,
                         row_data,
                         ob_values1,
                         order_cols.clone(),
                         ctx.clone())
            .unwrap();

        let ob_values2: Vec<Datum> = vec![Datum::Bytes(b"aaa".to_vec()), Datum::I64(3)];
        let row_data2 = RowColsDict::new(HashMap::default(), b"name:2".to_vec());
        topn_heap.try_add_row(0 as i64,
                         row_data2,
                         ob_values2,
                         order_cols.clone(),
                         ctx.clone())
            .unwrap();

        let bad_key1: Vec<Datum> = vec![Datum::I64(2), Datum::Bytes(b"aaa".to_vec())];
        let row_data3 = RowColsDict::new(HashMap::default(), b"name:3".to_vec());

        assert!(topn_heap.try_add_row(0 as i64,
                         row_data3,
                         bad_key1,
                         order_cols.clone(),
                         ctx.clone())
            .is_err());
        assert!(topn_heap.into_sorted_vec().is_err());
    }

    // the first column should be i64 since it will be used as row handle
    pub fn gen_table_data(tid: i64,
                          cis: &[ColumnInfo],
                          rows: &[Vec<Datum>])
                          -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut kv_data = Vec::new();
        let col_ids: Vec<i64> = cis.iter().map(|c| c.get_column_id()).collect();
        for cols in rows.iter() {
            let col_values: Vec<_> = cols.to_vec();
            let value = table::encode_row(col_values, &col_ids).unwrap();
            let mut buf = vec![];
            buf.encode_i64(cols[0].i64()).unwrap();
            let key = table::encode_row_key(tid, &buf);
            kv_data.push((key, value));
        }
        kv_data
    }

    #[test]
    fn test_topn_executor() {
        // prepare data and store
        let tid = 1;
        let cis = vec![new_col_info(1, types::LONG_LONG),
                       new_col_info(2, types::VARCHAR),
                       new_col_info(3, types::NEW_DECIMAL)];
        let raw_data = vec![vec![Datum::I64(1), Datum::Bytes(b"a".to_vec()), Datum::Dec(7.into())],
                            vec![Datum::I64(2), Datum::Bytes(b"b".to_vec()), Datum::Dec(7.into())],
                            vec![Datum::I64(3), Datum::Bytes(b"b".to_vec()), Datum::Dec(8.into())],
                            vec![Datum::I64(4), Datum::Bytes(b"d".to_vec()), Datum::Dec(3.into())],
                            vec![Datum::I64(5), Datum::Bytes(b"f".to_vec()), Datum::Dec(5.into())],
                            vec![Datum::I64(6), Datum::Bytes(b"e".to_vec()), Datum::Dec(9.into())],
                            vec![Datum::I64(7), Datum::Bytes(b"f".to_vec()), Datum::Dec(6.into())]];
        let table_data = gen_table_data(tid, &cis, &raw_data);
        let mut test_store = TestStore::new(&table_data);
        // init table scan meta
        let mut table_scan = TableScan::new();
        table_scan.set_table_id(tid);
        table_scan.set_columns(RepeatedField::from_vec(cis.clone()));
        // prepare range
        let range1 = get_range(tid, 0, 4);
        let range2 = get_range(tid, 5, 10);
        let key_ranges = vec![range1, range2];
        // init TableScan
        let (snapshot, start_ts) = test_store.get_snapshot();
        let mut statistics = Statistics::default();
        let ts_ect = TableScanExecutor::new(table_scan,
                                            key_ranges,
                                            snapshot,
                                            &mut statistics,
                                            start_ts,
                                            IsolationLevel::SI);

        // init TopN meta
        let mut ob_vec = Vec::with_capacity(2);
        ob_vec.push(new_order_by(2, false));
        ob_vec.push(new_order_by(3, true));
        let mut topn = TopN::default();
        topn.set_order_by(RepeatedField::from_vec(ob_vec));
        let limit = 4;
        topn.set_limit(limit);
        // init topn executor
        let mut topn_ect = TopNExecutor::new(topn,
                                             Rc::new(EvalContext::default()),
                                             &cis,
                                             Box::new(ts_ect))
            .unwrap();
        let mut topn_rows = Vec::with_capacity(limit as usize);
        while let Some(row) = topn_ect.next().unwrap() {
            topn_rows.push(row);
        }
        assert_eq!(topn_rows.len(), limit as usize);
        let expect_row_handles = vec![1, 3, 2, 6];
        for (row, handle) in topn_rows.iter().zip(expect_row_handles) {
            assert_eq!(row.handle, handle);
        }
    }
}
