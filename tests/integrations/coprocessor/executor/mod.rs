// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod aggregation;
mod index_scan;
mod limit;
mod selection;
mod table_scan;
mod topn;

#[cfg(test)]
pub mod tests {
    use cop_dag::codec::{table, Datum};
    use cop_dag::executor::{Executor, TableScanExecutor};
    use cop_datatype::{FieldTypeAccessor, FieldTypeTp};
    use kvproto::{
        coprocessor::KeyRange,
        kvrpcpb::{Context, IsolationLevel},
    };
    use protobuf::RepeatedField;
    use tikv::storage::kv::{Engine, Modify, RocksEngine, RocksSnapshot, TestEngineBuilder};
    use tikv::storage::mvcc::MvccTxn;
    use tikv::storage::SnapshotStore;
    use tikv::storage::{Key, Mutation, Options};
    use tikv_util::codec::number::NumberEncoder;
    use tipb::{
        executor::TableScan,
        expression::{Expr, ExprType},
        schema::ColumnInfo,
    };

    pub fn build_expr(tp: ExprType, id: Option<i64>, child: Option<Expr>) -> Expr {
        let mut expr = Expr::new();
        expr.set_tp(tp);
        if tp == ExprType::ColumnRef {
            expr.mut_val().encode_i64(id.unwrap()).unwrap();
        } else {
            expr.mut_children().push(child.unwrap());
        }
        expr
    }

    pub fn new_col_info(cid: i64, tp: FieldTypeTp) -> ColumnInfo {
        let mut col_info = ColumnInfo::new();
        col_info.as_mut_accessor().set_tp(tp);
        col_info.set_column_id(cid);
        col_info
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

    const START_TS: u64 = 10;
    const COMMIT_TS: u64 = 20;

    pub struct TestStore {
        snapshot: RocksSnapshot,
        ctx: Context,
        engine: RocksEngine,
    }

    impl TestStore {
        pub fn new(kv_data: &[(Vec<u8>, Vec<u8>)]) -> TestStore {
            let engine = TestEngineBuilder::new().build().unwrap();
            let ctx = Context::new();
            let snapshot = engine.snapshot(&ctx).unwrap();
            let mut store = TestStore {
                snapshot,
                ctx,
                engine,
            };
            store.init_data(kv_data);
            store
        }

        fn init_data(&mut self, kv_data: &[(Vec<u8>, Vec<u8>)]) {
            if kv_data.is_empty() {
                return;
            }

            // do prewrite.
            let txn_motifies = {
                let mut txn = MvccTxn::new(self.snapshot.clone(), START_TS, true).unwrap();
                let mut pk = vec![];
                for &(ref key, ref value) in kv_data {
                    if pk.is_empty() {
                        pk = key.clone();
                    }
                    txn.prewrite(
                        Mutation::Put((Key::from_raw(key), value.to_vec())),
                        &pk,
                        &Options::default(),
                    )
                    .unwrap();
                }
                txn.into_modifies()
            };
            self.write_modifies(txn_motifies);

            // do commit
            let txn_modifies = {
                let mut txn = MvccTxn::new(self.snapshot.clone(), START_TS, true).unwrap();
                for &(ref key, _) in kv_data {
                    txn.commit(Key::from_raw(key), COMMIT_TS).unwrap();
                }
                txn.into_modifies()
            };
            self.write_modifies(txn_modifies);
        }

        #[inline]
        fn write_modifies(&mut self, txn: Vec<Modify>) {
            self.engine.write(&self.ctx, txn).unwrap();
            self.snapshot = self.engine.snapshot(&self.ctx).unwrap()
        }

        pub fn get_snapshot(&mut self) -> (RocksSnapshot, u64) {
            (self.snapshot.clone(), COMMIT_TS + 1)
        }
    }

    #[inline]
    pub fn get_range(table_id: i64, start: i64, end: i64) -> KeyRange {
        let mut key_range = KeyRange::new();
        key_range.set_start(table::encode_row_key(table_id, start));
        key_range.set_end(table::encode_row_key(table_id, end));
        key_range
    }

    pub fn gen_table_scan_executor(
        tid: i64,
        cis: Vec<ColumnInfo>,
        raw_data: &[Vec<Datum>],
        key_ranges: Option<Vec<KeyRange>>,
    ) -> Box<dyn Executor + Send> {
        let table_data = gen_table_data(tid, &cis, raw_data);
        let mut test_store = TestStore::new(&table_data);

        let mut table_scan = TableScan::new();
        table_scan.set_table_id(tid);
        table_scan.set_columns(RepeatedField::from_vec(cis.clone()));

        let key_ranges = key_ranges.unwrap_or_else(|| vec![get_range(tid, 0, i64::max_value())]);

        let (snapshot, start_ts) = test_store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        Box::new(TableScanExecutor::table_scan(table_scan, key_ranges, store, true).unwrap())
    }
}
