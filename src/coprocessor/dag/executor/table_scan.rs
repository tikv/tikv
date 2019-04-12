// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use tikv_util::collections::HashSet;

use super::{scan::InnerExecutor, Row, ScanExecutor};
use crate::coprocessor::codec::table;
use crate::coprocessor::{util, Result};
use crate::storage::Store;
use kvproto::coprocessor::KeyRange;
use tipb::executor::TableScan;
use tipb::schema::ColumnInfo;

pub struct TableInnerExecutor {
    col_ids: HashSet<i64>,
}

impl TableInnerExecutor {
    fn new(meta: &TableScan) -> Self {
        let col_ids = meta
            .get_columns()
            .iter()
            .filter(|c| !c.get_pk_handle())
            .map(ColumnInfo::get_column_id)
            .collect();
        Self { col_ids }
    }
}

impl InnerExecutor for TableInnerExecutor {
    fn decode_row(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        columns: Arc<Vec<ColumnInfo>>,
    ) -> Result<Option<Row>> {
        let row_data = box_try!(table::cut_row(value, &self.col_ids));
        let h = box_try!(table::decode_handle(&key));
        Ok(Some(Row::origin(h, row_data, columns)))
    }

    #[inline]
    fn is_point(&self, range: &KeyRange) -> bool {
        util::is_point(&range)
    }

    #[inline]
    fn scan_on(&self) -> super::super::scanner::ScanOn {
        super::super::scanner::ScanOn::Table
    }

    #[inline]
    fn key_only(&self) -> bool {
        self.col_ids.is_empty()
    }
}

impl<S: Store> ScanExecutor<S, TableInnerExecutor> {
    pub fn table_scan(
        mut meta: TableScan,
        key_ranges: Vec<KeyRange>,
        store: S,
        collect: bool,
    ) -> Result<Self> {
        let inner = TableInnerExecutor::new(&meta);
        Self::new(
            inner,
            meta.get_desc(),
            meta.take_columns().to_vec(),
            key_ranges,
            store,
            collect,
        )
    }
}

pub type TableScanExecutor<S> = ScanExecutor<S, TableInnerExecutor>;

#[cfg(test)]
mod tests {
    use std::i64;

    use kvproto::{coprocessor::KeyRange, kvrpcpb::IsolationLevel};
    use protobuf::RepeatedField;
    use tipb::{executor::TableScan, schema::ColumnInfo};

    use crate::storage::SnapshotStore;

    use super::super::{
        super::scanner::tests::{get_point_range, prepare_table_data, Data},
        tests::{get_range, TestStore},
        Executor,
    };

    const TABLE_ID: i64 = 1;
    const KEY_NUMBER: usize = 10;

    struct TableScanTestWrapper {
        data: Data,
        store: TestStore,
        table_scan: TableScan,
        ranges: Vec<KeyRange>,
        cols: Vec<ColumnInfo>,
    }

    impl TableScanTestWrapper {
        fn get_point_range(&self, handle: i64) -> KeyRange {
            get_point_range(TABLE_ID, handle)
        }
    }

    impl Default for TableScanTestWrapper {
        fn default() -> TableScanTestWrapper {
            let test_data = prepare_table_data(KEY_NUMBER, TABLE_ID);
            let test_store = TestStore::new(&test_data.kv_data);
            let mut table_scan = TableScan::new();
            // prepare cols
            let cols = test_data.get_prev_2_cols();
            let col_req = RepeatedField::from_vec(cols.clone());
            table_scan.set_columns(col_req);
            // prepare range
            let range = get_range(TABLE_ID, i64::MIN, i64::MAX);
            let key_ranges = vec![range];
            TableScanTestWrapper {
                data: test_data,
                store: test_store,
                table_scan,
                ranges: key_ranges,
                cols,
            }
        }
    }

    #[test]
    fn test_point_get() {
        let mut wrapper = TableScanTestWrapper::default();
        // point get returns none
        let r1 = wrapper.get_point_range(i64::MIN);
        // point get return something
        let handle = 0;
        let r2 = wrapper.get_point_range(handle);
        wrapper.ranges = vec![r1, r2];

        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let mut table_scanner =
            super::TableScanExecutor::table_scan(wrapper.table_scan, wrapper.ranges, store, true)
                .unwrap();

        let row = table_scanner.next().unwrap().unwrap().take_origin();
        assert_eq!(row.handle, handle as i64);
        assert_eq!(row.data.len(), wrapper.cols.len());

        let expect_row = &wrapper.data.expect_rows[handle as usize];
        for col in &wrapper.cols {
            let cid = col.get_column_id();
            let v = row.data.get(cid).unwrap();
            assert_eq!(expect_row[&cid], v.to_vec());
        }
        assert!(table_scanner.next().unwrap().is_none());
        let expected_counts = vec![0, 1];
        let mut counts = Vec::with_capacity(2);
        table_scanner.collect_output_counts(&mut counts);
        assert_eq!(expected_counts, counts);
    }

    #[test]
    fn test_multiple_ranges() {
        let mut wrapper = TableScanTestWrapper::default();
        // prepare range
        let r1 = get_range(TABLE_ID, i64::MIN, 0);
        let r2 = get_range(TABLE_ID, 0, (KEY_NUMBER / 2) as i64);

        // prepare point get
        let handle = KEY_NUMBER / 2;
        let r3 = wrapper.get_point_range(handle as i64);

        let r4 = get_range(TABLE_ID, (handle + 1) as i64, i64::MAX);
        wrapper.ranges = vec![r1, r2, r3, r4];

        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let mut table_scanner =
            super::TableScanExecutor::table_scan(wrapper.table_scan, wrapper.ranges, store, true)
                .unwrap();

        for handle in 0..KEY_NUMBER {
            let row = table_scanner.next().unwrap().unwrap().take_origin();
            assert_eq!(row.handle, handle as i64);
            assert_eq!(row.data.len(), wrapper.cols.len());
            let expect_row = &wrapper.data.expect_rows[handle];
            for col in &wrapper.cols {
                let cid = col.get_column_id();
                let v = row.data.get(cid).unwrap();
                assert_eq!(expect_row[&cid], v.to_vec());
            }
        }
        assert!(table_scanner.next().unwrap().is_none());
    }

    #[test]
    fn test_reverse_scan() {
        let mut wrapper = TableScanTestWrapper::default();
        wrapper.table_scan.set_desc(true);

        // prepare range
        let r1 = get_range(TABLE_ID, i64::MIN, 0);
        let r2 = get_range(TABLE_ID, 0, (KEY_NUMBER / 2) as i64);

        // prepare point get
        let handle = KEY_NUMBER / 2;
        let r3 = wrapper.get_point_range(handle as i64);

        let r4 = get_range(TABLE_ID, (handle + 1) as i64, i64::MAX);
        wrapper.ranges = vec![r1, r2, r3, r4];

        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let mut table_scanner =
            super::TableScanExecutor::table_scan(wrapper.table_scan, wrapper.ranges, store, true)
                .unwrap();

        for tid in 0..KEY_NUMBER {
            let handle = KEY_NUMBER - tid - 1;
            let row = table_scanner.next().unwrap().unwrap().take_origin();
            assert_eq!(row.handle, handle as i64);
            assert_eq!(row.data.len(), wrapper.cols.len());
            let expect_row = &wrapper.data.expect_rows[handle];
            for col in &wrapper.cols {
                let cid = col.get_column_id();
                let v = row.data.get(cid).unwrap();
                assert_eq!(expect_row[&cid], v.to_vec());
            }
        }
        assert!(table_scanner.next().unwrap().is_none());
    }
}
