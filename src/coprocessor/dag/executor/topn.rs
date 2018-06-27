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
use std::sync::Arc;
use std::usize;
use std::vec::IntoIter;

use tipb::executor::TopN;
use tipb::expression::ByItem;
use tipb::schema::ColumnInfo;

use coprocessor::codec::datum::Datum;
use coprocessor::dag::expr::{EvalConfig, EvalContext, EvalWarnings, Expression};
use coprocessor::Result;

use super::topn_heap::TopNHeap;
use super::{inflate_with_col_for_dag, Executor, ExecutorMetrics, ExprColumnRefVisitor, Row};

struct OrderBy {
    items: Arc<Vec<ByItem>>,
    exprs: Vec<Expression>,
}

impl OrderBy {
    fn new(ctx: &mut EvalContext, mut order_by: Vec<ByItem>) -> Result<OrderBy> {
        let mut exprs = Vec::with_capacity(order_by.len());
        for v in &mut order_by {
            exprs.push(Expression::build(ctx, v.take_expr())?);
        }
        Ok(OrderBy {
            items: Arc::new(order_by),
            exprs,
        })
    }

    fn eval(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Vec<Datum>> {
        let mut res = Vec::with_capacity(self.exprs.len());
        for expr in &self.exprs {
            res.push(expr.eval(ctx, row)?);
        }
        Ok(res)
    }
}

pub struct TopNExecutor {
    order_by: OrderBy,
    cols: Arc<Vec<ColumnInfo>>,
    related_cols_offset: Vec<usize>, // offset of related columns
    iter: Option<IntoIter<Row>>,
    eval_ctx: Option<EvalContext>,
    eval_warnings: Option<EvalWarnings>,
    src: Box<Executor + Send>,
    limit: usize,
    first_collect: bool,
}

impl TopNExecutor {
    pub fn new(
        mut meta: TopN,
        eval_cfg: Arc<EvalConfig>,
        cols: Arc<Vec<ColumnInfo>>,
        src: Box<Executor + Send>,
    ) -> Result<TopNExecutor> {
        let order_by = meta.take_order_by().into_vec();

        let mut visitor = ExprColumnRefVisitor::new(cols.len());
        for by_item in &order_by {
            visitor.visit(by_item.get_expr())?;
        }
        let mut eval_ctx = EvalContext::new(Arc::clone(&eval_cfg));
        let order_by = OrderBy::new(&mut eval_ctx, order_by)?;
        Ok(TopNExecutor {
            order_by,
            cols,
            related_cols_offset: visitor.column_offsets(),
            iter: None,
            eval_ctx: Some(eval_ctx),
            eval_warnings: None,
            src,
            limit: meta.get_limit() as usize,
            first_collect: true,
        })
    }

    fn fetch_all(&mut self) -> Result<()> {
        if self.limit == 0 {
            self.iter = Some(Vec::default().into_iter());
            return Ok(());
        }

        if self.eval_ctx.is_none() {
            return Ok(());
        }

        let ctx = Arc::new(RefCell::new(self.eval_ctx.take().unwrap()));
        let mut heap = TopNHeap::new(self.limit, Arc::clone(&ctx))?;

        while let Some(row) = self.src.next()? {
            let cols = inflate_with_col_for_dag(
                &mut ctx.borrow_mut(),
                &row.data,
                self.cols.as_ref(),
                &self.related_cols_offset,
                row.handle,
            )?;
            let ob_values = self.order_by.eval(&mut ctx.borrow_mut(), &cols)?;
            heap.try_add_row(
                row.handle,
                row.data,
                ob_values,
                Arc::clone(&self.order_by.items),
            )?;
        }
        let sort_rows = heap.into_sorted_vec()?;
        let data: Vec<Row> = sort_rows
            .into_iter()
            .map(|sort_row| Row {
                handle: sort_row.handle,
                data: sort_row.data,
            })
            .collect();
        self.iter = Some(data.into_iter());
        self.eval_warnings = Some(ctx.borrow_mut().take_warnings());
        Ok(())
    }
}

impl Executor for TopNExecutor {
    fn next(&mut self) -> Result<Option<Row>> {
        if self.iter.is_none() {
            self.fetch_all()?;
        }
        let iter = self.iter.as_mut().unwrap();
        match iter.next() {
            Some(sort_row) => Ok(Some(sort_row)),
            None => Ok(None),
        }
    }

    fn collect_output_counts(&mut self, counts: &mut Vec<i64>) {
        self.src.collect_output_counts(counts);
    }

    fn collect_metrics_into(&mut self, metrics: &mut ExecutorMetrics) {
        self.src.collect_metrics_into(metrics);
        if self.first_collect {
            metrics.executor_count.topn += 1;
            self.first_collect = false;
        }
    }

    fn take_eval_warnings(&mut self) -> Option<EvalWarnings> {
        if let Some(mut warnings) = self.src.take_eval_warnings() {
            if let Some(mut topn_warnings) = self.eval_warnings.take() {
                warnings.merge(topn_warnings);
            }
            Some(warnings)
        } else {
            self.eval_warnings.take()
        }
    }
}

#[cfg(test)]
pub mod test {
    use std::cell::RefCell;
    use std::sync::Arc;

    use kvproto::kvrpcpb::IsolationLevel;
    use protobuf::RepeatedField;
    use tipb::executor::TableScan;
    use tipb::expression::{Expr, ExprType};

    use coprocessor::codec::mysql::types;
    use coprocessor::codec::table::{self, RowColsDict};
    use coprocessor::codec::Datum;
    use util::codec::number::NumberEncoder;
    use util::collections::HashMap;

    use storage::SnapshotStore;

    use super::super::scanner::test::{get_range, new_col_info, TestStore};
    use super::super::table_scan::TableScanExecutor;
    use super::*;

    fn new_order_by(offset: i64, desc: bool) -> ByItem {
        let mut item = ByItem::new();
        let mut expr = Expr::new();
        expr.set_tp(ExprType::ColumnRef);
        expr.mut_val().encode_i64(offset).unwrap();
        item.set_expr(expr);
        item.set_desc(desc);
        item
    }

    #[test]
    pub fn test_topn_heap() {
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
            let ob_values: Vec<Datum> = vec![name, count];
            let row_data = RowColsDict::new(HashMap::default(), data.into_bytes());
            topn_heap
                .try_add_row(
                    i64::from(handle),
                    row_data,
                    ob_values,
                    Arc::clone(&order_cols),
                )
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
        let order_cols = Arc::new(order_cols);
        let mut topn_heap =
            TopNHeap::new(5, Arc::new(RefCell::new(EvalContext::default()))).unwrap();

        let ob_values1: Vec<Datum> = vec![Datum::Bytes(b"aaa".to_vec()), Datum::I64(2)];
        let row_data = RowColsDict::new(HashMap::default(), b"name:1".to_vec());
        topn_heap
            .try_add_row(0 as i64, row_data, ob_values1, Arc::clone(&order_cols))
            .unwrap();

        let ob_values2: Vec<Datum> = vec![Datum::Bytes(b"aaa".to_vec()), Datum::I64(3)];
        let row_data2 = RowColsDict::new(HashMap::default(), b"name:2".to_vec());
        topn_heap
            .try_add_row(0 as i64, row_data2, ob_values2, Arc::clone(&order_cols))
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

    // the first column should be i64 since it will be used as row handle
    pub fn gen_table_data(
        tid: i64,
        cis: &[ColumnInfo],
        rows: &[Vec<Datum>],
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut kv_data = Vec::new();
        let col_ids: Vec<i64> = cis.iter().map(|c| c.get_column_id()).collect();
        for cols in rows.iter() {
            let col_values: Vec<_> = cols.to_vec();
            let value = table::encode_row(col_values, &col_ids).unwrap();
            let key = table::encode_row_key(tid, cols[0].i64());
            kv_data.push((key, value));
        }
        kv_data
    }

    #[test]
    fn test_topn_executor() {
        // prepare data and store
        let tid = 1;
        let cis = vec![
            new_col_info(1, types::LONG_LONG),
            new_col_info(2, types::VARCHAR),
            new_col_info(3, types::NEW_DECIMAL),
        ];
        let raw_data = vec![
            vec![
                Datum::I64(1),
                Datum::Bytes(b"a".to_vec()),
                Datum::Dec(7.into()),
            ],
            vec![
                Datum::I64(2),
                Datum::Bytes(b"b".to_vec()),
                Datum::Dec(7.into()),
            ],
            vec![
                Datum::I64(3),
                Datum::Bytes(b"b".to_vec()),
                Datum::Dec(8.into()),
            ],
            vec![
                Datum::I64(4),
                Datum::Bytes(b"d".to_vec()),
                Datum::Dec(3.into()),
            ],
            vec![
                Datum::I64(5),
                Datum::Bytes(b"f".to_vec()),
                Datum::Dec(5.into()),
            ],
            vec![
                Datum::I64(6),
                Datum::Bytes(b"e".to_vec()),
                Datum::Dec(9.into()),
            ],
            vec![
                Datum::I64(7),
                Datum::Bytes(b"f".to_vec()),
                Datum::Dec(6.into()),
            ],
        ];
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
        let snap = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let ts_ect = TableScanExecutor::new(&table_scan, key_ranges, snap, true).unwrap();

        // init TopN meta
        let mut ob_vec = Vec::with_capacity(2);
        ob_vec.push(new_order_by(1, false));
        ob_vec.push(new_order_by(2, true));
        let mut topn = TopN::default();
        topn.set_order_by(RepeatedField::from_vec(ob_vec));
        let limit = 4;
        topn.set_limit(limit);
        // init topn executor
        let mut topn_ect = TopNExecutor::new(
            topn,
            Arc::new(EvalConfig::default()),
            Arc::new(cis),
            Box::new(ts_ect),
        ).unwrap();
        let mut topn_rows = Vec::with_capacity(limit as usize);
        while let Some(row) = topn_ect.next().unwrap() {
            topn_rows.push(row);
        }
        assert_eq!(topn_rows.len(), limit as usize);
        let expect_row_handles = vec![1, 3, 2, 6];
        for (row, handle) in topn_rows.iter().zip(expect_row_handles) {
            assert_eq!(row.handle, handle);
        }
        let expected_counts = vec![3, 3];
        let mut counts = Vec::with_capacity(2);
        topn_ect.collect_output_counts(&mut counts);
        assert_eq!(expected_counts, counts);
    }

    #[test]
    fn test_limit() {
        // prepare data and store
        let tid = 1;
        let cis = vec![
            new_col_info(1, types::LONG_LONG),
            new_col_info(2, types::VARCHAR),
            new_col_info(3, types::NEW_DECIMAL),
        ];
        let raw_data = vec![vec![
            Datum::I64(1),
            Datum::Bytes(b"a".to_vec()),
            Datum::Dec(7.into()),
        ]];
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
        let snap = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);

        // init TopN meta
        let mut ob_vec = Vec::with_capacity(2);
        ob_vec.push(new_order_by(1, false));
        ob_vec.push(new_order_by(2, true));
        let mut topn = TopN::default();
        topn.set_order_by(RepeatedField::from_vec(ob_vec));
        // test with limit=0
        topn.set_limit(0);
        let mut topn_ect = TopNExecutor::new(
            topn,
            Arc::new(EvalConfig::default()),
            Arc::new(cis),
            Box::new(TableScanExecutor::new(&table_scan, key_ranges, snap, false).unwrap()),
        ).unwrap();
        assert!(topn_ect.next().unwrap().is_none());
    }
}
