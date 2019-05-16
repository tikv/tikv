// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(test)]
mod tests {
    use std::i64;
    use std::sync::Arc;

    use cop_datatype::FieldTypeTp;
    use kvproto::kvrpcpb::IsolationLevel;
    use protobuf::RepeatedField;
    use tipb::executor::Aggregation;
    use tipb::expression::{Expr, ExprType};
    use tipb::schema::ColumnInfo;

    use cop_dag::codec::datum::{self, Datum};
    use cop_dag::codec::mysql::decimal::Decimal;
    use cop_dag::codec::table;
    use cop_dag::executor::{Executor, HashAggExecutor, IndexScanExecutor, Row, StreamAggExecutor};
    use cop_dag::expr::EvalConfig;
    use tikv_util::collections::HashMap;

    use super::super::super::scanner::tests::Data;
    use super::super::index_scan::tests::IndexTestWrapper;
    use super::super::tests::*;
    use tikv::storage::SnapshotStore;

    fn build_group_by(col_ids: &[i64]) -> Vec<Expr> {
        let mut group_by = Vec::with_capacity(col_ids.len());
        for id in col_ids {
            group_by.push(build_expr(ExprType::ColumnRef, Some(*id), None));
        }
        group_by
    }

    fn build_aggr_func(aggrs: &[(ExprType, i64)]) -> Vec<Expr> {
        let mut aggr_func = Vec::with_capacity(aggrs.len());
        for aggr in aggrs {
            let &(tp, id) = aggr;
            let col_ref = build_expr(ExprType::ColumnRef, Some(id), None);
            aggr_func.push(build_expr(tp, None, Some(col_ref)));
        }
        aggr_func
    }

    pub fn generate_index_data(
        table_id: i64,
        index_id: i64,
        handle: i64,
        idx_vals: Vec<(i64, Datum)>,
    ) -> (HashMap<i64, Vec<u8>>, Vec<u8>) {
        let mut expect_row = HashMap::default();
        let mut v: Vec<_> = idx_vals
            .iter()
            .map(|&(ref cid, ref value)| {
                expect_row.insert(*cid, datum::encode_key(&[value.clone()]).unwrap());
                value.clone()
            })
            .collect();
        v.push(Datum::I64(handle));
        let encoded = datum::encode_key(&v).unwrap();
        let idx_key = table::encode_index_seek_key(table_id, index_id, &encoded);
        (expect_row, idx_key)
    }

    pub fn prepare_index_data(
        table_id: i64,
        index_id: i64,
        cols: Vec<ColumnInfo>,
        idx_vals: Vec<Vec<(i64, Datum)>>,
    ) -> Data {
        let mut kv_data = Vec::new();
        let mut expect_rows = Vec::new();

        let mut handle = 1;
        for val in idx_vals {
            let (expect_row, idx_key) =
                generate_index_data(table_id, index_id, i64::from(handle), val);
            expect_rows.push(expect_row);
            let value = vec![1; 0];
            kv_data.push((idx_key, value));
            handle += 1;
        }
        Data {
            kv_data,
            expect_rows,
            cols,
        }
    }

    #[test]
    fn test_stream_agg() {
        // prepare data and store
        let tid = 1;
        let idx_id = 1;
        let col_infos = vec![
            new_col_info(2, FieldTypeTp::VarChar),
            new_col_info(3, FieldTypeTp::NewDecimal),
        ];
        // init aggregation meta
        let mut aggregation = Aggregation::default();
        let group_by_cols = vec![0, 1];
        let group_by = build_group_by(&group_by_cols);
        aggregation.set_group_by(RepeatedField::from_vec(group_by));
        let funcs = vec![(ExprType::Count, 0), (ExprType::Sum, 1), (ExprType::Avg, 1)];
        let agg_funcs = build_aggr_func(&funcs);
        aggregation.set_agg_func(RepeatedField::from_vec(agg_funcs));

        // test no row
        let idx_vals = vec![];
        let idx_data = prepare_index_data(tid, idx_id, col_infos.clone(), idx_vals);
        let idx_row_cnt = idx_data.kv_data.len() as i64;
        let unique = false;
        let mut wrapper = IndexTestWrapper::new(unique, idx_data);
        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let is_executor =
            IndexScanExecutor::index_scan(wrapper.scan, wrapper.ranges, store, unique, true)
                .unwrap();
        // init the stream aggregation executor
        let mut agg_ect = StreamAggExecutor::new(
            Arc::new(EvalConfig::default()),
            Box::new(is_executor),
            aggregation.clone(),
        )
        .unwrap();
        let expect_row_cnt = 0;
        let mut row_data = Vec::with_capacity(1);
        while let Some(Row::Agg(row)) = agg_ect.next().unwrap() {
            row_data.push(row.value);
        }
        assert_eq!(row_data.len(), expect_row_cnt);
        let expected_counts = vec![idx_row_cnt];
        let mut counts = Vec::with_capacity(1);
        agg_ect.collect_output_counts(&mut counts);
        assert_eq!(expected_counts, counts);

        // test one row
        let idx_vals = vec![vec![
            (2, Datum::Bytes(b"a".to_vec())),
            (3, Datum::Dec(12.into())),
        ]];
        let idx_data = prepare_index_data(tid, idx_id, col_infos.clone(), idx_vals);
        let idx_row_cnt = idx_data.kv_data.len() as i64;
        let unique = false;
        let mut wrapper = IndexTestWrapper::new(unique, idx_data);
        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let is_executor =
            IndexScanExecutor::index_scan(wrapper.scan, wrapper.ranges, store, unique, true)
                .unwrap();
        // init the stream aggregation executor
        let mut agg_ect = StreamAggExecutor::new(
            Arc::new(EvalConfig::default()),
            Box::new(is_executor),
            aggregation.clone(),
        )
        .unwrap();
        let expect_row_cnt = 1;
        let mut row_data = Vec::with_capacity(expect_row_cnt);
        while let Some(Row::Agg(row)) = agg_ect.next().unwrap() {
            row_data.push(row.get_binary().unwrap());
        }
        assert_eq!(row_data.len(), expect_row_cnt);
        let expect_row_data = vec![(
            1 as u64,
            Decimal::from(12),
            1 as u64,
            Decimal::from(12),
            b"a".as_ref(),
            Decimal::from(12),
        )];
        let expect_col_cnt = 6;
        for (row, expect_cols) in row_data.into_iter().zip(expect_row_data) {
            let ds = datum::decode(&mut row.as_slice()).unwrap();
            assert_eq!(ds.len(), expect_col_cnt);
            assert_eq!(ds[0], Datum::from(expect_cols.0));
        }
        let expected_counts = vec![idx_row_cnt];
        let mut counts = Vec::with_capacity(1);
        agg_ect.collect_output_counts(&mut counts);
        assert_eq!(expected_counts, counts);

        // test multiple rows
        let idx_vals = vec![
            vec![(2, Datum::Bytes(b"a".to_vec())), (3, Datum::Dec(12.into()))],
            vec![(2, Datum::Bytes(b"c".to_vec())), (3, Datum::Dec(12.into()))],
            vec![(2, Datum::Bytes(b"c".to_vec())), (3, Datum::Dec(2.into()))],
            vec![(2, Datum::Bytes(b"b".to_vec())), (3, Datum::Dec(2.into()))],
            vec![(2, Datum::Bytes(b"a".to_vec())), (3, Datum::Dec(12.into()))],
            vec![(2, Datum::Bytes(b"b".to_vec())), (3, Datum::Dec(2.into()))],
            vec![(2, Datum::Bytes(b"a".to_vec())), (3, Datum::Dec(12.into()))],
        ];
        let idx_data = prepare_index_data(tid, idx_id, col_infos.clone(), idx_vals);
        let idx_row_cnt = idx_data.kv_data.len() as i64;
        let mut wrapper = IndexTestWrapper::new(unique, idx_data);
        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let is_executor =
            IndexScanExecutor::index_scan(wrapper.scan, wrapper.ranges, store, unique, true)
                .unwrap();
        // init the stream aggregation executor
        let mut agg_ect = StreamAggExecutor::new(
            Arc::new(EvalConfig::default()),
            Box::new(is_executor),
            aggregation,
        )
        .unwrap();
        let expect_row_cnt = 4;
        let mut row_data = Vec::with_capacity(expect_row_cnt);
        while let Some(Row::Agg(row)) = agg_ect.next().unwrap() {
            row_data.push(row.get_binary().unwrap());
        }
        assert_eq!(row_data.len(), expect_row_cnt);
        let expect_row_data = vec![
            (
                3 as u64,
                Decimal::from(36),
                3 as u64,
                Decimal::from(36),
                b"a".as_ref(),
                Decimal::from(12),
            ),
            (
                2 as u64,
                Decimal::from(4),
                2 as u64,
                Decimal::from(4),
                b"b".as_ref(),
                Decimal::from(2),
            ),
            (
                1 as u64,
                Decimal::from(2),
                1 as u64,
                Decimal::from(2),
                b"c".as_ref(),
                Decimal::from(2),
            ),
            (
                1 as u64,
                Decimal::from(12),
                1 as u64,
                Decimal::from(12),
                b"c".as_ref(),
                Decimal::from(12),
            ),
        ];
        let expect_col_cnt = 6;
        for (row, expect_cols) in row_data.into_iter().zip(expect_row_data) {
            let ds = datum::decode(&mut row.as_slice()).unwrap();
            assert_eq!(ds.len(), expect_col_cnt);
            assert_eq!(ds[0], Datum::from(expect_cols.0));
            assert_eq!(ds[1], Datum::from(expect_cols.1));
            assert_eq!(ds[2], Datum::from(expect_cols.2));
            assert_eq!(ds[3], Datum::from(expect_cols.3));
        }
        let expected_counts = vec![idx_row_cnt];
        let mut counts = Vec::with_capacity(1);
        agg_ect.collect_output_counts(&mut counts);
        assert_eq!(expected_counts, counts);
    }

    #[test]
    fn test_hash_agg() {
        // prepare data and store
        let tid = 1;
        let cis = vec![
            new_col_info(1, FieldTypeTp::LongLong),
            new_col_info(2, FieldTypeTp::VarChar),
            new_col_info(3, FieldTypeTp::NewDecimal),
            new_col_info(4, FieldTypeTp::Float),
            new_col_info(5, FieldTypeTp::Double),
        ];
        let raw_data = vec![
            vec![
                Datum::I64(1),
                Datum::Bytes(b"a".to_vec()),
                Datum::Dec(7.into()),
                Datum::F64(1.0),
                Datum::F64(1.0),
            ],
            vec![
                Datum::I64(2),
                Datum::Bytes(b"a".to_vec()),
                Datum::Dec(7.into()),
                Datum::F64(2.0),
                Datum::F64(2.0),
            ],
            vec![
                Datum::I64(3),
                Datum::Bytes(b"b".to_vec()),
                Datum::Dec(8.into()),
                Datum::F64(3.0),
                Datum::F64(3.0),
            ],
            vec![
                Datum::I64(4),
                Datum::Bytes(b"a".to_vec()),
                Datum::Dec(7.into()),
                Datum::F64(4.0),
                Datum::F64(4.0),
            ],
            vec![
                Datum::I64(5),
                Datum::Bytes(b"f".to_vec()),
                Datum::Dec(5.into()),
                Datum::F64(5.0),
                Datum::F64(5.0),
            ],
            vec![
                Datum::I64(6),
                Datum::Bytes(b"b".to_vec()),
                Datum::Dec(8.into()),
                Datum::F64(6.0),
                Datum::F64(6.0),
            ],
            vec![
                Datum::I64(7),
                Datum::Bytes(b"f".to_vec()),
                Datum::Dec(6.into()),
                Datum::F64(7.0),
                Datum::F64(7.0),
            ],
        ];

        let key_ranges = vec![get_range(tid, i64::MIN, i64::MAX)];
        let ts_ect = gen_table_scan_executor(tid, cis, &raw_data, Some(key_ranges));

        // init aggregation meta
        let mut aggregation = Aggregation::default();
        let group_by_cols = vec![1, 2];
        let group_by = build_group_by(&group_by_cols);
        aggregation.set_group_by(RepeatedField::from_vec(group_by));
        let aggr_funcs = vec![
            (ExprType::Avg, 0),
            (ExprType::Count, 2),
            (ExprType::Sum, 3),
            (ExprType::Avg, 4),
        ];
        let aggr_funcs = build_aggr_func(&aggr_funcs);
        aggregation.set_agg_func(RepeatedField::from_vec(aggr_funcs));
        // init the hash aggregation executor
        let mut aggr_ect =
            HashAggExecutor::new(aggregation, Arc::new(EvalConfig::default()), ts_ect).unwrap();
        let expect_row_cnt = 4;
        let mut row_data = Vec::with_capacity(expect_row_cnt);
        while let Some(Row::Agg(row)) = aggr_ect.next().unwrap() {
            row_data.push(row.get_binary().unwrap());
        }
        assert_eq!(row_data.len(), expect_row_cnt);
        let expect_row_data = vec![
            (
                3 as u64,
                Decimal::from(7),
                3 as u64,
                7.0 as f64,
                3 as u64,
                7.0 as f64,
                b"a".as_ref(),
                Decimal::from(7),
            ),
            (
                2 as u64,
                Decimal::from(9),
                2 as u64,
                9.0 as f64,
                2 as u64,
                9.0 as f64,
                b"b".as_ref(),
                Decimal::from(8),
            ),
            (
                1 as u64,
                Decimal::from(5),
                1 as u64,
                5.0 as f64,
                1 as u64,
                5.0 as f64,
                b"f".as_ref(),
                Decimal::from(5),
            ),
            (
                1 as u64,
                Decimal::from(7),
                1 as u64,
                7.0 as f64,
                1 as u64,
                7.0 as f64,
                b"f".as_ref(),
                Decimal::from(6),
            ),
        ];
        let expect_col_cnt = 8;
        for (row, expect_cols) in row_data.into_iter().zip(expect_row_data) {
            let ds = datum::decode(&mut row.as_slice()).unwrap();
            assert_eq!(ds.len(), expect_col_cnt);
            assert_eq!(ds[0], Datum::from(expect_cols.0));
            assert_eq!(ds[1], Datum::from(expect_cols.1));
            assert_eq!(ds[2], Datum::from(expect_cols.2));
            assert_eq!(ds[3], Datum::from(expect_cols.3));
            assert_eq!(ds[4], Datum::from(expect_cols.4));
        }
        let expected_counts = vec![raw_data.len() as i64];
        let mut counts = Vec::with_capacity(1);
        aggr_ect.collect_output_counts(&mut counts);
        assert_eq!(expected_counts, counts);
    }
}
