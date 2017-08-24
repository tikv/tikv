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

use std::rc::Rc;

use tipb::executor::Selection;
use tipb::schema::ColumnInfo;
use tipb::expression::Expr;

use coprocessor::metrics::*;
use coprocessor::select::xeval::{EvalContext, Evaluator};
use coprocessor::Result;

use super::{inflate_with_col_for_dag, Executor, ExprColumnRefVisitor, Row};

pub struct SelectionExecutor<'a> {
    conditions: Vec<Expr>,
    cols: Rc<Vec<ColumnInfo>>,
    related_cols_offset: Vec<usize>, // offset of related columns
    ctx: Rc<EvalContext>,
    src: Box<Executor + 'a>,
}

impl<'a> SelectionExecutor<'a> {
    pub fn new(
        mut meta: Selection,
        ctx: Rc<EvalContext>,
        columns_info: Rc<Vec<ColumnInfo>>,
        src: Box<Executor + 'a>,
    ) -> Result<SelectionExecutor<'a>> {
        let conditions = meta.take_conditions().into_vec();
        let mut visitor = ExprColumnRefVisitor::new(columns_info.len());
        try!(visitor.batch_visit(&conditions));
        COPR_EXECUTOR_COUNT.with_label_values(&["selection"]).inc();
        Ok(SelectionExecutor {
            conditions: conditions,
            cols: columns_info,
            related_cols_offset: visitor.column_offsets(),
            ctx: ctx,
            src: src,
        })
    }
}

#[allow(never_loop)]
impl<'a> Executor for SelectionExecutor<'a> {
    fn next(&mut self) -> Result<Option<Row>> {
        'next: while let Some(row) = try!(self.src.next()) {
            let mut evaluator = Evaluator::default();
            try!(inflate_with_col_for_dag(
                &mut evaluator,
                &self.ctx,
                &row.data,
                self.cols.clone(),
                &self.related_cols_offset,
                row.handle
            ));
            for expr in &self.conditions {
                let val = box_try!(evaluator.eval(&self.ctx, expr));
                if !box_try!(val.into_bool(&self.ctx)).unwrap_or(false) {
                    continue 'next;
                }
            }
            return Ok(Some(row));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::i64;

    use kvproto::kvrpcpb::IsolationLevel;
    use protobuf::RepeatedField;
    use tipb::executor::TableScan;
    use tipb::expression::{Expr, ExprType};

    use coprocessor::codec::mysql::types;
    use coprocessor::codec::datum::Datum;
    use storage::{SnapshotStore, Statistics};
    use util::codec::number::NumberEncoder;

    use super::*;
    use super::super::topn::test::gen_table_data;
    use super::super::scanner::test::{get_range, new_col_info, TestStore};
    use super::super::table_scan::TableScanExecutor;

    fn new_const_expr() -> Expr {
        let mut expr = Expr::new();
        expr.set_tp(ExprType::NullEQ);
        expr.mut_children().push({
            let mut lhs = Expr::new();
            lhs.set_tp(ExprType::Null);
            lhs
        });
        expr.mut_children().push({
            let mut rhs = Expr::new();
            rhs.set_tp(ExprType::Null);
            rhs
        });
        expr
    }

    fn new_col_gt_u64_expr(offset: i64, val: u64) -> Expr {
        let mut expr = Expr::new();
        expr.set_tp(ExprType::GT);
        expr.mut_children().push({
            let mut lhs = Expr::new();
            lhs.set_tp(ExprType::ColumnRef);
            lhs.mut_val().encode_i64(offset).unwrap();
            lhs
        });
        expr.mut_children().push({
            let mut rhs = Expr::new();
            rhs.set_tp(ExprType::Uint64);
            rhs.mut_val().encode_u64(val).unwrap();
            rhs
        });
        expr
    }

    #[test]
    fn test_selection_executor_simple() {
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

        let mut table_scan = TableScan::new();
        table_scan.set_table_id(tid);
        table_scan.set_columns(RepeatedField::from_vec(cis.clone()));
        // prepare range
        let key_ranges = vec![get_range(tid, 0, i64::MAX)];

        let (snapshot, start_ts) = test_store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI);

        let mut statistics = Statistics::default();

        let inner_table_scan =
            TableScanExecutor::new(table_scan, key_ranges, store, &mut statistics);

        // selection executor
        let mut selection = Selection::new();
        let expr = new_const_expr();
        selection.mut_conditions().push(expr);

        let mut selection_executor = SelectionExecutor::new(
            selection,
            Rc::new(EvalContext::default()),
            Rc::new(cis),
            Box::new(inner_table_scan),
        ).unwrap();

        let mut selection_rows = Vec::with_capacity(raw_data.len());
        while let Some(row) = selection_executor.next().unwrap() {
            selection_rows.push(row);
        }

        assert_eq!(selection_rows.len(), raw_data.len());
        let expect_row_handles = raw_data.iter().map(|r| r[0].i64()).collect::<Vec<_>>();
        let result_row = selection_rows.iter().map(|r| r.handle).collect::<Vec<_>>();
        assert_eq!(result_row, expect_row_handles);
    }

    #[test]
    fn test_selection_executor_condition() {
        let tid = 1;
        let cis = vec![
            new_col_info(1, types::LONG_LONG),
            new_col_info(2, types::VARCHAR),
            new_col_info(3, types::LONG_LONG),
        ];
        let raw_data = vec![
            vec![Datum::I64(1), Datum::Bytes(b"a".to_vec()), Datum::I64(7)],
            vec![Datum::I64(2), Datum::Bytes(b"b".to_vec()), Datum::I64(7)],
            vec![Datum::I64(3), Datum::Bytes(b"b".to_vec()), Datum::I64(8)],
            vec![Datum::I64(4), Datum::Bytes(b"d".to_vec()), Datum::I64(3)],
            vec![Datum::I64(5), Datum::Bytes(b"f".to_vec()), Datum::I64(5)],
            vec![Datum::I64(6), Datum::Bytes(b"e".to_vec()), Datum::I64(9)],
            vec![Datum::I64(7), Datum::Bytes(b"f".to_vec()), Datum::I64(6)],
        ];

        let table_data = gen_table_data(tid, &cis, &raw_data);
        let mut test_store = TestStore::new(&table_data);

        let mut table_scan = TableScan::new();
        table_scan.set_table_id(tid);
        table_scan.set_columns(RepeatedField::from_vec(cis.clone()));
        // prepare range
        let key_ranges = vec![get_range(tid, 0, i64::MAX)];

        let (snapshot, start_ts) = test_store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI);
        let mut statistics = Statistics::default();

        let inner_table_scan =
            TableScanExecutor::new(table_scan, key_ranges, store, &mut statistics);

        // selection executor
        let mut selection = Selection::new();
        let expr = new_col_gt_u64_expr(2, 5);
        selection.mut_conditions().push(expr);

        let mut selection_executor = SelectionExecutor::new(
            selection,
            Rc::new(EvalContext::default()),
            Rc::new(cis),
            Box::new(inner_table_scan),
        ).unwrap();

        let mut selection_rows = Vec::with_capacity(raw_data.len());
        while let Some(row) = selection_executor.next().unwrap() {
            selection_rows.push(row);
        }

        let expect_row_handles = raw_data
            .iter()
            .filter(|r| r[2].i64() > 5)
            .map(|r| r[0].i64())
            .collect::<Vec<_>>();
        assert!(expect_row_handles.len() < raw_data.len());
        assert_eq!(selection_rows.len(), expect_row_handles.len());
        let result_row = selection_rows.iter().map(|r| r.handle).collect::<Vec<_>>();
        assert_eq!(result_row, expect_row_handles);
    }
}
