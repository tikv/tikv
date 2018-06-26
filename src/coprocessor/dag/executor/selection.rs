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

use std::sync::Arc;

use tipb::executor::Selection;
use tipb::schema::ColumnInfo;

use coprocessor::dag::expr::{EvalConfig, EvalContext, EvalWarnings, Expression};
use coprocessor::Result;

use super::{inflate_with_col_for_dag, Executor, ExecutorMetrics, ExprColumnRefVisitor, Row};

pub struct SelectionExecutor {
    conditions: Vec<Expression>,
    cols: Arc<Vec<ColumnInfo>>,
    related_cols_offset: Vec<usize>, // offset of related columns
    ctx: EvalContext,
    src: Box<Executor + Send>,
    first_collect: bool,
}

impl SelectionExecutor {
    pub fn new(
        mut meta: Selection,
        eval_cfg: Arc<EvalConfig>,
        columns_info: Arc<Vec<ColumnInfo>>,
        src: Box<Executor + Send>,
    ) -> Result<SelectionExecutor> {
        let conditions = meta.take_conditions().into_vec();
        let mut visitor = ExprColumnRefVisitor::new(columns_info.len());
        visitor.batch_visit(&conditions)?;
        let mut ctx = EvalContext::new(eval_cfg);
        Ok(SelectionExecutor {
            conditions: Expression::batch_build(&mut ctx, conditions)?,
            cols: columns_info,
            related_cols_offset: visitor.column_offsets(),
            ctx,
            src,
            first_collect: true,
        })
    }
}

#[allow(never_loop)]
impl Executor for SelectionExecutor {
    fn next(&mut self) -> Result<Option<Row>> {
        'next: while let Some(row) = self.src.next()? {
            let cols = inflate_with_col_for_dag(
                &mut self.ctx,
                &row.data,
                self.cols.as_ref(),
                &self.related_cols_offset,
                row.handle,
            )?;
            for filter in &self.conditions {
                let val = filter.eval(&mut self.ctx, &cols)?;
                if !val.into_bool(&mut self.ctx)?.unwrap_or(false) {
                    continue 'next;
                }
            }
            return Ok(Some(row));
        }
        Ok(None)
    }

    fn collect_output_counts(&mut self, counts: &mut Vec<i64>) {
        self.src.collect_output_counts(counts);
    }

    fn collect_metrics_into(&mut self, metrics: &mut ExecutorMetrics) {
        self.src.collect_metrics_into(metrics);
        if self.first_collect {
            metrics.executor_count.selection += 1;
            self.first_collect = false;
        }
    }

    fn take_eval_warnings(&mut self) -> Option<EvalWarnings> {
        if let Some(mut warnings) = self.src.take_eval_warnings() {
            warnings.merge(self.ctx.take_warnings());
            Some(warnings)
        } else {
            Some(self.ctx.take_warnings())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::i64;
    use std::sync::Arc;

    use kvproto::kvrpcpb::IsolationLevel;
    use protobuf::RepeatedField;
    use tipb::executor::TableScan;
    use tipb::expression::{Expr, ExprType, ScalarFuncSig};

    use coprocessor::codec::datum::Datum;
    use coprocessor::codec::mysql::types;
    use storage::SnapshotStore;
    use util::codec::number::NumberEncoder;

    use super::super::scanner::test::{get_range, new_col_info, TestStore};
    use super::super::table_scan::TableScanExecutor;
    use super::super::topn::test::gen_table_data;
    use super::*;

    fn new_const_expr() -> Expr {
        let mut expr = Expr::new();
        expr.set_tp(ExprType::ScalarFunc);
        expr.set_sig(ScalarFuncSig::NullEQInt);
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
        expr.set_tp(ExprType::ScalarFunc);
        expr.set_sig(ScalarFuncSig::GTInt);
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
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);

        let inner_table_scan =
            TableScanExecutor::new(&table_scan, key_ranges, store, false).unwrap();

        // selection executor
        let mut selection = Selection::new();
        let expr = new_const_expr();
        selection.mut_conditions().push(expr);

        let mut selection_executor = SelectionExecutor::new(
            selection,
            Arc::new(EvalConfig::default()),
            Arc::new(cis),
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
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let inner_table_scan =
            TableScanExecutor::new(&table_scan, key_ranges, store, true).unwrap();

        // selection executor
        let mut selection = Selection::new();
        let expr = new_col_gt_u64_expr(2, 5);
        selection.mut_conditions().push(expr);

        let mut selection_executor = SelectionExecutor::new(
            selection,
            Arc::new(EvalConfig::default()),
            Arc::new(cis),
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
        let expected_counts = vec![raw_data.len() as i64];
        let mut counts = Vec::with_capacity(1);
        selection_executor.collect_output_counts(&mut counts);
        assert_eq!(expected_counts, counts);
    }
}
