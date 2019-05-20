// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::sync::Arc;
use std::usize;
use std::vec::IntoIter;

use tipb::executor::TopN;
use tipb::expression::ByItem;

use crate::codec::datum::Datum;
use crate::expr::{EvalConfig, EvalContext, EvalWarnings, Expression};
use crate::Result;

use super::topn_heap::TopNHeap;
use super::{Executor, ExecutorMetrics, ExprColumnRefVisitor, Row};

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
pub struct TopNExecutor {
    order_by: OrderBy,
    related_cols_offset: Vec<usize>, // offset of related columns
    iter: Option<IntoIter<Row>>,
    eval_ctx: Option<EvalContext>,
    eval_warnings: Option<EvalWarnings>,
    src: Box<dyn Executor + Send>,
    limit: usize,
    first_collect: bool,
}

impl TopNExecutor {
    pub fn new(
        mut meta: TopN,
        eval_cfg: Arc<EvalConfig>,
        src: Box<dyn Executor + Send>,
    ) -> Result<TopNExecutor> {
        let order_by = meta.take_order_by().into_vec();

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
            let row = row.take_origin();
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
                warnings.merge(&mut topn_warnings);
            }
            Some(warnings)
        } else {
            self.eval_warnings.take()
        }
    }

    fn get_len_of_columns(&self) -> usize {
        self.src.get_len_of_columns()
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use cop_datatype::FieldTypeTp;
    use protobuf::RepeatedField;
    use tipb::executor::TopN;
    use tipb::expression::{ByItem, Expr, ExprType};

    use crate::codec::Datum;
    use crate::executor::{Executor, TopNExecutor};
    use tikv_util::codec::number::NumberEncoder;

    use super::super::tests::*;
    use crate::expr::EvalConfig;

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
        topn.set_order_by(RepeatedField::from_vec(ob_vec));
        let limit = 4;
        topn.set_limit(limit);
        // init topn executor
        let mut topn_ect =
            TopNExecutor::new(topn, Arc::new(EvalConfig::default()), ts_ect).unwrap();
        let mut topn_rows = Vec::with_capacity(limit as usize);
        while let Some(row) = topn_ect.next().unwrap() {
            topn_rows.push(row.take_origin());
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
        topn.set_order_by(RepeatedField::from_vec(ob_vec));
        // test with limit=0
        topn.set_limit(0);
        let mut topn_ect =
            TopNExecutor::new(topn, Arc::new(EvalConfig::default()), ts_ect).unwrap();
        assert!(topn_ect.next().unwrap().is_none());
    }
}
