// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use cop_datatype::FieldTypeTp;
    use protobuf::RepeatedField;
    use tipb::executor::TopN;
    use tipb::expression::{ByItem, Expr, ExprType};

    use cop_dag::codec::Datum;
    use cop_dag::executor::{Executor, TopNExecutor};
    use tikv_util::codec::number::NumberEncoder;

    use super::super::tests::*;
    use cop_dag::expr::EvalConfig;

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
