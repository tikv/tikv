// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use kvproto::coprocessor::KeyRange;
use tikv_util::collections::HashSet;
use tipb::ColumnInfo;
use tipb::TableScan;

use super::{scan::InnerExecutor, Row, ScanExecutor, ScanExecutorOptions};
use crate::codec::table::{self, check_record_key};
use crate::expr::EvalContext;
use crate::storage::Storage;
use crate::Result;

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

    fn is_key_only(&self) -> bool {
        self.col_ids.is_empty()
    }
}

impl InnerExecutor for TableInnerExecutor {
    fn decode_row(
        &self,
        _ctx: &mut EvalContext,
        key: Vec<u8>,
        value: Vec<u8>,
        columns: Arc<Vec<ColumnInfo>>,
    ) -> Result<Option<Row>> {
        check_record_key(key.as_slice())?;
        let row_data = box_try!(table::cut_row(value, &self.col_ids));
        let h = box_try!(table::decode_handle(&key));
        Ok(Some(Row::origin(h, row_data, columns)))
    }
}

pub type TableScanExecutor<S> = ScanExecutor<S, TableInnerExecutor>;

impl<S: Storage> TableScanExecutor<S> {
    pub fn table_scan(
        mut meta: TableScan,
        context: EvalContext,
        key_ranges: Vec<KeyRange>,
        storage: S,
        is_scanned_range_aware: bool,
    ) -> Result<Self> {
        let inner = TableInnerExecutor::new(&meta);
        let is_key_only = inner.is_key_only();

        Self::new(ScanExecutorOptions {
            inner,
            context,
            columns: meta.take_columns().to_vec(),
            key_ranges,
            storage,
            is_backward: meta.get_desc(),
            is_key_only,
            accept_point_range: true,
            is_scanned_range_aware,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::i64;

    use kvproto::coprocessor::KeyRange;
    use tipb::{ColumnInfo, TableScan};

    use super::super::tests::*;
    use super::super::Executor;
    use crate::execute_stats::ExecuteStats;
    use crate::expr::EvalContext;
    use crate::storage::fixture::FixtureStorage;

    const TABLE_ID: i64 = 1;
    const KEY_NUMBER: usize = 10;

    struct TableScanTestWrapper {
        data: TableData,
        store: FixtureStorage,
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
            let test_data = TableData::prepare(KEY_NUMBER, TABLE_ID);
            let store = FixtureStorage::from(test_data.kv_data.clone());
            let mut table_scan = TableScan::default();
            // prepare cols
            let cols = test_data.get_prev_2_cols();
            let col_req = cols.clone().into();
            table_scan.set_columns(col_req);
            // prepare range
            let range = get_range(TABLE_ID, i64::MIN, i64::MAX);
            let key_ranges = vec![range];
            TableScanTestWrapper {
                data: test_data,
                store,
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

        let mut table_scanner = super::TableScanExecutor::table_scan(
            wrapper.table_scan,
            EvalContext::default(),
            wrapper.ranges,
            wrapper.store,
            false,
        )
        .unwrap();

        let row = table_scanner
            .next()
            .unwrap()
            .unwrap()
            .take_origin()
            .unwrap();
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
        let mut exec_stats = ExecuteStats::new(0);
        table_scanner.collect_exec_stats(&mut exec_stats);
        assert_eq!(expected_counts, exec_stats.scanned_rows_per_range);
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

        let mut table_scanner = super::TableScanExecutor::table_scan(
            wrapper.table_scan,
            EvalContext::default(),
            wrapper.ranges,
            wrapper.store,
            false,
        )
        .unwrap();

        for handle in 0..KEY_NUMBER {
            let row = table_scanner
                .next()
                .unwrap()
                .unwrap()
                .take_origin()
                .unwrap();
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

        let mut table_scanner = super::TableScanExecutor::table_scan(
            wrapper.table_scan,
            EvalContext::default(),
            wrapper.ranges,
            wrapper.store,
            true,
        )
        .unwrap();

        for tid in 0..KEY_NUMBER {
            let handle = KEY_NUMBER - tid - 1;
            let row = table_scanner
                .next()
                .unwrap()
                .unwrap()
                .take_origin()
                .unwrap();
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
