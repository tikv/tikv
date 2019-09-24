// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use kvproto::coprocessor::KeyRange;
use tipb::ColumnInfo;
use tipb::IndexScan;

use super::{scan::InnerExecutor, Row, ScanExecutor, ScanExecutorOptions};
use crate::codec::table;
use crate::storage::Storage;
use crate::Result;

pub struct IndexInnerExecutor {
    pk_col: Option<ColumnInfo>,
    col_ids: Vec<i64>,
}

impl IndexInnerExecutor {
    fn new(meta: &mut IndexScan) -> Self {
        let mut pk_col = None;
        let cols = meta.mut_columns();
        if cols.last().map_or(false, ColumnInfo::get_pk_handle) {
            pk_col = Some(cols.pop().unwrap());
        }
        let col_ids = cols.iter().map(ColumnInfo::get_column_id).collect();
        Self { pk_col, col_ids }
    }
}

impl InnerExecutor for IndexInnerExecutor {
    fn decode_row(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        columns: Arc<Vec<ColumnInfo>>,
    ) -> Result<Option<Row>> {
        use crate::codec::datum;
        use byteorder::{BigEndian, ReadBytesExt};
        use tidb_query_datatype::prelude::*;
        use tidb_query_datatype::FieldTypeFlag;

        let (mut values, handle) = box_try!(table::cut_idx_key(key, &self.col_ids));
        let handle = match handle {
            None => box_try!(value.as_slice().read_i64::<BigEndian>()),
            Some(h) => h,
        };

        if let Some(ref pk_col) = self.pk_col {
            let handle_datum = if pk_col
                .as_accessor()
                .flag()
                .contains(FieldTypeFlag::UNSIGNED)
            {
                // PK column is unsigned
                datum::Datum::U64(handle as u64)
            } else {
                datum::Datum::I64(handle)
            };
            let mut bytes = box_try!(datum::encode_key(&[handle_datum]));
            values.append(pk_col.get_column_id(), &mut bytes);
        }
        Ok(Some(Row::origin(handle, values, columns)))
    }
}

pub type IndexScanExecutor<S> = ScanExecutor<S, IndexInnerExecutor>;

impl<S: Storage> IndexScanExecutor<S> {
    pub fn index_scan(
        mut meta: IndexScan,
        key_ranges: Vec<KeyRange>,
        storage: S,
        unique: bool,
        is_scanned_range_aware: bool,
    ) -> Result<Self> {
        let columns = meta.get_columns().to_vec();
        let inner = IndexInnerExecutor::new(&mut meta);
        Self::new(ScanExecutorOptions {
            inner,
            columns,
            key_ranges,
            storage,
            is_backward: meta.get_desc(),
            is_key_only: false,
            accept_point_range: unique,
            is_scanned_range_aware,
        })
    }

    pub fn index_scan_with_cols_len(
        cols: i64,
        key_ranges: Vec<KeyRange>,
        storage: S,
    ) -> Result<Self> {
        let col_ids: Vec<i64> = (0..cols).collect();
        let inner = IndexInnerExecutor {
            col_ids,
            pk_col: None,
        };
        Self::new(ScanExecutorOptions {
            inner,
            columns: vec![],
            key_ranges,
            storage,
            is_backward: false,
            is_key_only: false,
            accept_point_range: false,
            is_scanned_range_aware: false,
        })
    }
}

#[cfg(test)]
pub mod tests {
    use byteorder::{BigEndian, WriteBytesExt};
    use std::i64;

    use tidb_query_datatype::FieldTypeTp;
    use tikv_util::collections::HashMap;
    use tipb::ColumnInfo;

    use super::super::tests::*;
    use super::super::Executor;
    use super::*;
    use crate::codec::datum::{self, Datum};
    use crate::execute_stats::ExecuteStats;
    use crate::storage::fixture::FixtureStorage;

    const TABLE_ID: i64 = 1;
    const INDEX_ID: i64 = 1;
    const KEY_NUMBER: usize = 10;

    // get_idx_range get range for index in [("abc",start),("abc", end))
    pub fn get_idx_range(
        table_id: i64,
        idx_id: i64,
        start: i64,
        end: i64,
        val_start: &Datum,
        val_end: &Datum,
        unique: bool,
    ) -> KeyRange {
        let (_, start_key) = generate_index_data(table_id, idx_id, start, val_start, unique);
        let (_, end_key) = generate_index_data(table_id, idx_id, end, val_end, unique);
        let mut key_range = KeyRange::default();
        key_range.set_start(start_key);
        key_range.set_end(end_key);
        key_range
    }

    pub fn generate_index_data(
        table_id: i64,
        index_id: i64,
        handle: i64,
        col_val: &Datum,
        unique: bool,
    ) -> (HashMap<i64, Vec<u8>>, Vec<u8>) {
        let indice = vec![(2, (*col_val).clone()), (3, Datum::Dec(handle.into()))];
        let mut expect_row = HashMap::default();
        let mut v: Vec<_> = indice
            .iter()
            .map(|&(ref cid, ref value)| {
                expect_row.insert(*cid, datum::encode_key(&[value.clone()]).unwrap());
                value.clone()
            })
            .collect();
        if !unique {
            v.push(Datum::I64(handle));
        }
        let encoded = datum::encode_key(&v).unwrap();
        let idx_key = table::encode_index_seek_key(table_id, index_id, &encoded);
        (expect_row, idx_key)
    }

    pub fn prepare_index_data(
        key_number: usize,
        table_id: i64,
        index_id: i64,
        unique: bool,
    ) -> TableData {
        let cols = vec![
            new_col_info(2, FieldTypeTp::VarChar),
            new_col_info(3, FieldTypeTp::NewDecimal),
        ];

        let mut kv_data = Vec::new();
        let mut expect_rows = Vec::new();

        let idx_col_val = Datum::Bytes(b"abc".to_vec());
        for handle in 0..key_number {
            let (expect_row, idx_key) =
                generate_index_data(table_id, index_id, handle as i64, &idx_col_val, unique);
            expect_rows.push(expect_row);
            let value = if unique {
                let mut value = Vec::with_capacity(8);
                value.write_i64::<BigEndian>(handle as i64).unwrap();
                value
            } else {
                vec![1; 0]
            };
            kv_data.push((idx_key, value));
        }
        TableData {
            kv_data,
            expect_rows,
            cols,
        }
    }

    pub struct IndexTestWrapper {
        data: TableData,
        pub store: FixtureStorage,
        pub scan: IndexScan,
        pub ranges: Vec<KeyRange>,
        cols: Vec<ColumnInfo>,
    }

    impl IndexTestWrapper {
        fn include_pk_cols() -> IndexTestWrapper {
            let unique = false;
            let test_data = prepare_index_data(KEY_NUMBER, TABLE_ID, INDEX_ID, unique);
            let mut wrapper = IndexTestWrapper::new(unique, test_data);
            let mut cols = wrapper.data.cols.clone();
            cols.push(wrapper.data.get_col_pk());
            wrapper.scan.set_columns(cols.clone().into());
            wrapper.cols = cols;
            wrapper
        }

        pub fn new(unique: bool, test_data: TableData) -> IndexTestWrapper {
            let store = FixtureStorage::from(test_data.kv_data.clone());
            let mut scan = IndexScan::default();
            // prepare cols
            let cols = test_data.cols.clone();
            let col_req = cols.clone().into();
            scan.set_columns(col_req);
            // prepare range
            let val_start = Datum::Bytes(b"a".to_vec());
            let val_end = Datum::Bytes(b"z".to_vec());
            let range = get_idx_range(
                TABLE_ID,
                INDEX_ID,
                0,
                i64::MAX,
                &val_start,
                &val_end,
                unique,
            );
            let key_ranges = vec![range];
            IndexTestWrapper {
                data: test_data,
                store,
                scan,
                ranges: key_ranges,
                cols,
            }
        }
    }

    #[test]
    fn test_multiple_ranges() {
        let unique = false;
        let test_data = prepare_index_data(KEY_NUMBER, TABLE_ID, INDEX_ID, unique);
        let mut wrapper = IndexTestWrapper::new(unique, test_data);
        let val_start = Datum::Bytes(b"abc".to_vec());
        let val_end = Datum::Bytes(b"abc".to_vec());
        let r1 = get_idx_range(
            TABLE_ID,
            INDEX_ID,
            0,
            (KEY_NUMBER / 3) as i64,
            &val_start,
            &val_end,
            unique,
        );
        let r2 = get_idx_range(
            TABLE_ID,
            INDEX_ID,
            (KEY_NUMBER / 3) as i64,
            (KEY_NUMBER / 2) as i64,
            &val_start,
            &val_end,
            unique,
        );
        wrapper.ranges = vec![r1, r2];

        let mut scanner = IndexScanExecutor::index_scan(
            wrapper.scan,
            wrapper.ranges,
            wrapper.store,
            false,
            false,
        )
        .unwrap();

        for handle in 0..KEY_NUMBER / 2 {
            let row = scanner.next().unwrap().unwrap().take_origin().unwrap();
            assert_eq!(row.handle, handle as i64);
            assert_eq!(row.data.len(), wrapper.cols.len());
            let expect_row = &wrapper.data.expect_rows[handle];
            for col in &wrapper.cols {
                let cid = col.get_column_id();
                let v = row.data.get(cid).unwrap();
                assert_eq!(expect_row[&cid], v.to_vec());
            }
        }
        assert!(scanner.next().unwrap().is_none());
    }

    #[test]
    fn test_unique_index_scan() {
        let unique = true;
        let test_data = prepare_index_data(KEY_NUMBER, TABLE_ID, INDEX_ID, unique);
        let mut wrapper = IndexTestWrapper::new(unique, test_data);

        let val_start = Datum::Bytes(b"abc".to_vec());
        let val_end = Datum::Bytes(b"abc".to_vec());
        // point get
        let r1 = get_idx_range(TABLE_ID, INDEX_ID, 0, 1, &val_start, &val_end, unique);
        // range seek
        let r2 = get_idx_range(TABLE_ID, INDEX_ID, 1, 4, &val_start, &val_end, unique);
        // point get
        let r3 = get_idx_range(TABLE_ID, INDEX_ID, 4, 5, &val_start, &val_end, unique);
        //range seek
        let r4 = get_idx_range(
            TABLE_ID,
            INDEX_ID,
            5,
            (KEY_NUMBER + 1) as i64,
            &val_start,
            &val_end,
            unique,
        );
        let r5 = get_idx_range(
            TABLE_ID,
            INDEX_ID,
            (KEY_NUMBER + 1) as i64,
            (KEY_NUMBER + 2) as i64,
            &val_start,
            &val_end,
            unique,
        ); // point get but miss
        wrapper.ranges = vec![r1, r2, r3, r4, r5];

        let mut scanner = IndexScanExecutor::index_scan(
            wrapper.scan,
            wrapper.ranges,
            wrapper.store,
            unique,
            false,
        )
        .unwrap();
        for handle in 0..KEY_NUMBER {
            let row = scanner.next().unwrap().unwrap().take_origin().unwrap();
            assert_eq!(row.handle, handle as i64);
            assert_eq!(row.data.len(), 2);
            let expect_row = &wrapper.data.expect_rows[handle];
            for col in &wrapper.cols {
                let cid = col.get_column_id();
                let v = row.data.get(cid).unwrap();
                assert_eq!(expect_row[&cid], v.to_vec());
            }
        }
        assert!(scanner.next().unwrap().is_none());
        let expected_counts = vec![1, 3, 1, 5, 0];
        let mut exec_stats = ExecuteStats::new(0);
        scanner.collect_exec_stats(&mut exec_stats);
        assert_eq!(expected_counts, exec_stats.scanned_rows_per_range);
    }

    #[test]
    fn test_reverse_scan() {
        let unique = false;
        let test_data = prepare_index_data(KEY_NUMBER, TABLE_ID, INDEX_ID, unique);
        let mut wrapper = IndexTestWrapper::new(unique, test_data);
        wrapper.scan.set_desc(true);

        let val_start = Datum::Bytes(b"abc".to_vec());
        let val_end = Datum::Bytes(b"abc".to_vec());
        let r1 = get_idx_range(
            TABLE_ID,
            INDEX_ID,
            0,
            (KEY_NUMBER / 2) as i64,
            &val_start,
            &val_end,
            unique,
        );
        let r2 = get_idx_range(
            TABLE_ID,
            INDEX_ID,
            (KEY_NUMBER / 2) as i64,
            i64::MAX,
            &val_start,
            &val_end,
            unique,
        );
        wrapper.ranges = vec![r1, r2];

        let mut scanner = IndexScanExecutor::index_scan(
            wrapper.scan,
            wrapper.ranges,
            wrapper.store,
            unique,
            false,
        )
        .unwrap();

        for tid in 0..KEY_NUMBER {
            let handle = KEY_NUMBER - tid - 1;
            let row = scanner.next().unwrap().unwrap().take_origin().unwrap();
            assert_eq!(row.handle, handle as i64);
            assert_eq!(row.data.len(), 2);
            let expect_row = &wrapper.data.expect_rows[handle];
            for col in &wrapper.cols {
                let cid = col.get_column_id();
                let v = row.data.get(cid).unwrap();
                assert_eq!(expect_row[&cid], v.to_vec());
            }
        }
        assert!(scanner.next().unwrap().is_none());
    }

    #[test]
    fn test_include_pk() {
        let wrapper = IndexTestWrapper::include_pk_cols();

        let mut scanner = IndexScanExecutor::index_scan(
            wrapper.scan,
            wrapper.ranges,
            wrapper.store,
            false,
            false,
        )
        .unwrap();

        for handle in 0..KEY_NUMBER {
            let row = scanner.next().unwrap().unwrap().take_origin().unwrap();
            assert_eq!(row.handle, handle as i64);
            assert_eq!(row.data.len(), wrapper.cols.len());
            let expect_row = &wrapper.data.expect_rows[handle];
            let handle_datum = datum::Datum::I64(handle as i64);
            let pk = datum::encode_key(&[handle_datum]).unwrap();
            for col in &wrapper.cols {
                let cid = col.get_column_id();
                let v = row.data.get(cid).unwrap();
                if col.get_pk_handle() {
                    assert_eq!(pk, v.to_vec());
                    continue;
                }
                assert_eq!(expect_row[&cid], v.to_vec());
            }
        }
        assert!(scanner.next().unwrap().is_none());
    }
}
