// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::sync::Arc;
use std::usize;
use std::vec::IntoIter;

use tipb::ByItem;
use tipb::TopN;

use super::topn_heap::TopNHeap;
use super::{Executor, ExprColumnRefVisitor, Row};
use crate::codec::datum::Datum;
use crate::execute_stats::ExecuteStats;
use crate::expr::{EvalConfig, EvalContext, EvalWarnings, Expression};
use crate::storage::IntervalRange;
use crate::Result;

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

/// Retrieves rows from the source executor, orders rows according to expressions and produces part
/// of the rows.
pub struct TopNExecutor<Src: Executor> {
    order_by: OrderBy,
    related_cols_offset: Vec<usize>, // offset of related columns
    iter: Option<IntoIter<Row>>,
    eval_ctx: Option<EvalContext>,
    eval_warnings: Option<EvalWarnings>,
    src: Src,
    limit: usize,
}

impl<Src: Executor> TopNExecutor<Src> {
    pub fn new(mut meta: TopN, eval_cfg: Arc<EvalConfig>, src: Src) -> Result<Self> {
        let order_by: Vec<_> = meta.take_order_by().into();

        let mut visitor = ExprColumnRefVisitor::new(src.get_len_of_columns());
        for by_item in &order_by {
            visitor.visit(by_item.get_expr())?;
        }
        let mut eval_ctx = EvalContext::new(Arc::clone(&eval_cfg));
        let order_by = OrderBy::new(&mut eval_ctx, order_by)?;
        Ok(TopNExecutor {
            order_by,
            related_cols_offset: visitor.column_offsets(),
            iter: None,
            eval_ctx: Some(eval_ctx),
            eval_warnings: None,
            src,
            limit: meta.get_limit() as usize,
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
            let row = row.take_origin()?;
            let cols = row.inflate_cols_with_offsets(&ctx.borrow(), &self.related_cols_offset)?;
            let ob_values = self.order_by.eval(&mut ctx.borrow_mut(), &cols)?;
            heap.try_add_row(row, ob_values, Arc::clone(&self.order_by.items))?;
        }
        let sort_rows = heap.into_sorted_vec()?;
        let data: Vec<Row> = sort_rows
            .into_iter()
            .map(|sort_row| Row::Origin(sort_row.data))
            .collect();
        self.iter = Some(data.into_iter());
        self.eval_warnings = Some(ctx.borrow_mut().take_warnings());
        Ok(())
    }
}

impl<Src: Executor> Executor for TopNExecutor<Src> {
    type StorageStats = Src::StorageStats;

    fn next(&mut self) -> Result<Option<Row>> {
        if self.iter.is_none() {
            self.fetch_all()?;
        }
        let iter = self.iter.as_mut().unwrap();
        Ok(iter.next())
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
    fn get_len_of_columns(&self) -> usize {
        self.src.get_len_of_columns()
    }

    fn take_eval_warnings(&mut self) -> Option<EvalWarnings> {
        if let Some(mut warnings) = self.src.take_eval_warnings() {
            if let Some(mut topn_warnings) = self.eval_warnings.take() {
                warnings.merge(&mut topn_warnings);
            }
            Some(warnings)
        } else {
            self.eval_warnings.take()
        }
    }

    #[inline]
    fn take_scanned_range(&mut self) -> IntervalRange {
        self.src.take_scanned_range()
    }
}

#[cfg(test)]
pub mod tests {
    use std::cell::RefCell;
    use std::sync::Arc;

    use codec::prelude::NumberEncoder;
    use tidb_query_datatype::FieldTypeTp;
    use tikv_util::collections::HashMap;
    use tipb::{Expr, ExprType};

    use crate::codec::table::RowColsDict;
    use crate::codec::Datum;
    use crate::executor::OriginCols;

    use super::super::tests::*;
    use super::*;

    fn new_order_by(offset: i64, desc: bool) -> ByItem {
        let mut item = ByItem::default();
        let mut expr = Expr::default();
        expr.set_tp(ExprType::ColumnRef);
        expr.mut_val().write_i64(offset).unwrap();
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
                    OriginCols::new(i64::from(handle), row_data, Arc::new(Vec::default())),
                    ob_values,
                    Arc::clone(&order_cols),
                )
                .unwrap();
        }
        let result = topn_heap.into_sorted_vec().unwrap();
        assert_eq!(result.len(), exp.len());
        for (row, (handle, _, name, count)) in result.iter().zip(exp) {
            let exp_key: Vec<Datum> = vec![name, count];
            assert_eq!(row.data.handle, handle);
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
            .try_add_row(
                OriginCols::new(0 as i64, row_data, Arc::new(Vec::default())),
                ob_values1,
                Arc::clone(&order_cols),
            )
            .unwrap();

        let ob_values2: Vec<Datum> = vec![Datum::Bytes(b"aaa".to_vec()), Datum::I64(3)];
        let row_data2 = RowColsDict::new(HashMap::default(), b"name:2".to_vec());
        topn_heap
            .try_add_row(
                OriginCols::new(0 as i64, row_data2, Default::default()),
                ob_values2,
                Arc::clone(&order_cols),
            )
            .unwrap();

        let bad_key1: Vec<Datum> = vec![Datum::I64(2), Datum::Bytes(b"aaa".to_vec())];
        let row_data3 = RowColsDict::new(HashMap::default(), b"name:3".to_vec());

        assert!(topn_heap
            .try_add_row(
                OriginCols::new(0 as i64, row_data3, Arc::default()),
                bad_key1,
                Arc::clone(&order_cols)
            )
            .is_err());
        assert!(topn_heap.into_sorted_vec().is_err());
    }

    #[test]
    fn test_topn_executor() {
        // prepare data and store
        let tid = 1;
        let cis = vec![
            new_col_info(1, FieldTypeTp::LongLong),
            new_col_info(2, FieldTypeTp::VarChar),
            new_col_info(3, FieldTypeTp::NewDecimal),
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
        // prepare range
        let range1 = get_range(tid, 0, 4);
        let range2 = get_range(tid, 5, 10);
        let key_ranges = vec![range1, range2];
        let ts_ect = gen_table_scan_executor(tid, cis, &raw_data, Some(key_ranges));

        // init TopN meta
        let mut ob_vec = Vec::with_capacity(2);
        ob_vec.push(new_order_by(1, false));
        ob_vec.push(new_order_by(2, true));
        let mut topn = TopN::default();
        topn.set_order_by(ob_vec.into());
        let limit = 4;
        topn.set_limit(limit);
        // init topn executor
        let mut topn_ect =
            TopNExecutor::new(topn, Arc::new(EvalConfig::default()), ts_ect).unwrap();
        let mut topn_rows = Vec::with_capacity(limit as usize);
        while let Some(row) = topn_ect.next().unwrap() {
            topn_rows.push(row.take_origin().unwrap());
        }
        assert_eq!(topn_rows.len(), limit as usize);
        let expect_row_handles = vec![1, 3, 2, 6];
        for (row, handle) in topn_rows.iter().zip(expect_row_handles) {
            assert_eq!(row.handle, handle);
        }
        let expected_counts = vec![3, 3];
        let mut exec_stats = ExecuteStats::new(0);
        topn_ect.collect_exec_stats(&mut exec_stats);
        assert_eq!(expected_counts, exec_stats.scanned_rows_per_range);
    }

    #[test]
    fn test_limit() {
        // prepare data and store
        let tid = 1;
        let cis = vec![
            new_col_info(1, FieldTypeTp::LongLong),
            new_col_info(2, FieldTypeTp::VarChar),
            new_col_info(3, FieldTypeTp::NewDecimal),
        ];
        let raw_data = vec![vec![
            Datum::I64(1),
            Datum::Bytes(b"a".to_vec()),
            Datum::Dec(7.into()),
        ]];
        // prepare range
        let range1 = get_range(tid, 0, 4);
        let range2 = get_range(tid, 5, 10);
        let key_ranges = vec![range1, range2];
        let ts_ect = gen_table_scan_executor(tid, cis, &raw_data, Some(key_ranges));

        // init TopN meta
        let mut ob_vec = Vec::with_capacity(2);
        ob_vec.push(new_order_by(1, false));
        ob_vec.push(new_order_by(2, true));
        let mut topn = TopN::default();
        topn.set_order_by(ob_vec.into());
        // test with limit=0
        topn.set_limit(0);
        let mut topn_ect =
            TopNExecutor::new(topn, Arc::new(EvalConfig::default()), ts_ect).unwrap();
        assert!(topn_ect.next().unwrap().is_none());
    }
}
