// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(test)]
mod tests {
    use std::i64;
    use std::sync::Arc;

    use cop_datatype::FieldTypeTp;
    use tipb::executor::Selection;
    use tipb::expression::{Expr, ExprType, ScalarFuncSig};

    use cop_dag::codec::datum::Datum;
    use tikv_util::codec::number::NumberEncoder;

    use super::super::tests::*;
    use cop_dag::executor::{Executor, SelectionExecutor};
    use cop_dag::expr::EvalConfig;

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

        let inner_table_scan = gen_table_scan_executor(1, cis, &raw_data, None);

        // selection executor
        let mut selection = Selection::new();
        let expr = new_const_expr();
        selection.mut_conditions().push(expr);

        let mut selection_executor =
            SelectionExecutor::new(selection, Arc::new(EvalConfig::default()), inner_table_scan)
                .unwrap();

        let mut selection_rows = Vec::with_capacity(raw_data.len());
        while let Some(row) = selection_executor.next().unwrap() {
            selection_rows.push(row.take_origin());
        }

        assert_eq!(selection_rows.len(), raw_data.len());
        let expect_row_handles = raw_data.iter().map(|r| r[0].i64()).collect::<Vec<_>>();
        let result_row = selection_rows.iter().map(|r| r.handle).collect::<Vec<_>>();
        assert_eq!(result_row, expect_row_handles);
    }

    #[test]
    fn test_selection_executor_condition() {
        let cis = vec![
            new_col_info(1, FieldTypeTp::LongLong),
            new_col_info(2, FieldTypeTp::VarChar),
            new_col_info(3, FieldTypeTp::LongLong),
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

        let inner_table_scan = gen_table_scan_executor(1, cis, &raw_data, None);

        // selection executor
        let mut selection = Selection::new();
        let expr = new_col_gt_u64_expr(2, 5);
        selection.mut_conditions().push(expr);

        let mut selection_executor =
            SelectionExecutor::new(selection, Arc::new(EvalConfig::default()), inner_table_scan)
                .unwrap();

        let mut selection_rows = Vec::with_capacity(raw_data.len());
        while let Some(row) = selection_executor.next().unwrap() {
            selection_rows.push(row.take_origin());
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
