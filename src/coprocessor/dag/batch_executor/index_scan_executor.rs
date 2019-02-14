// Copyright 2019 PingCAP, Inc.
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

use cop_datatype::EvalType;
use kvproto::coprocessor::KeyRange;

use crate::storage::{Key, Store};

use super::interface::*;
use super::ranges_consumer::{ConsumerResult, RangesConsumer};
use crate::coprocessor::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use crate::coprocessor::codec::table;
use crate::coprocessor::dag::Scanner;
use crate::coprocessor::*;

// TODO: Merge with BatchTableScanExecutor

pub struct BatchIndexScanExecutor<S: Store> {
    // Please refer to `BatchTableScanExecutor` for field descriptions.
    context: ExecutorContext,
    store: S,
    desc: bool,
    ranges: RangesConsumer,
    scanner: Option<Scanner<S>>,

    /// Number of interested columns (exclude PK handle column).
    columns_len_without_handle: usize,

    /// Whether PK handle column is interested. Handle will be always placed in the last column.
    decode_handle: bool,

    is_ended: bool,
}

impl<S: Store> BatchIndexScanExecutor<S> {
    pub fn new(
        store: S,
        context: ExecutorContext,
        mut key_ranges: Vec<KeyRange>,
        desc: bool,
        unique: bool,
        // TODO: this does not mean that it is a unique index scan. What does it mean?
    ) -> Result<Self> {
        table::check_table_ranges(&key_ranges)?;
        if desc {
            key_ranges.reverse();
        }

        let mut columns_len_without_handle = 0;
        let mut decode_handle = false;
        for column_info in &context.columns_info {
            if column_info.get_pk_handle() {
                decode_handle = true;
            } else {
                columns_len_without_handle += 1;
            }
        }

        Ok(Self {
            context,
            store,
            desc,
            ranges: RangesConsumer::new(key_ranges, unique),
            scanner: None,

            columns_len_without_handle,
            decode_handle,
            is_ended: false,
        })
    }

    /// Creates or resets the range of inner scanner.
    #[inline]
    fn reset_range(&mut self, range: KeyRange) -> Result<()> {
        use crate::coprocessor::dag::ScanOn;

        self.scanner = Some(Scanner::new(
            &self.store,
            ScanOn::Index,
            self.desc,
            false,
            range,
        )?);
        Ok(())
    }

    /// Scans next row from the scanner.
    #[inline]
    fn scan_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        // TODO: Key and value doesn't have to be owned
        if let Some(scanner) = self.scanner.as_mut() {
            Ok(scanner.next_row()?)
        } else {
            // `self.scanner` should never be `None` when this function is being called.
            unreachable!()
        }
    }

    /// Get one row from the store.
    #[inline]
    fn point_get(&mut self, mut range: KeyRange) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut statistics = crate::storage::Statistics::default();
        // TODO: Key and value doesn't have to be owned
        let key = range.take_start();
        let value = self.store.get(&Key::from_raw(&key), &mut statistics)?;
        Ok(value.map(move |v| (key, v)))
    }

    fn fill_batch_rows(
        &mut self,
        expect_rows: usize,
        columns: &mut LazyBatchColumnVec,
    ) -> Result<bool> {
        use crate::coprocessor::codec::datum;
        use crate::util::codec::number;
        use byteorder::{BigEndian, ReadBytesExt};

        assert!(expect_rows > 0);

        let mut is_drained = false;

        loop {
            let range = self.ranges.next();
            let some_row = match range {
                ConsumerResult::NewPointRange(r) => self.point_get(r)?,
                ConsumerResult::NewNonPointRange(r) => {
                    self.reset_range(r)?;
                    self.scan_next()?
                }
                ConsumerResult::Continue => self.scan_next()?,
                ConsumerResult::Drained => {
                    is_drained = true;
                    break;
                }
            };
            if let Some((key, value)) = some_row {
                // The payload part of the key
                let mut key_payload = &key[table::PREFIX_LEN + table::ID_LEN..];

                for i in 0..self.columns_len_without_handle {
                    let (val, remaining) = datum::split_datum(key_payload, false)?;
                    columns[i].push_raw(val);
                    key_payload = remaining;
                }

                if self.decode_handle {
                    // For normal index, it is placed at the end and any columns prior to it are
                    // ensured to be interested. For unique index, it is placed in the value.
                    let handle_val = if key_payload.is_empty() {
                        // This is a unique index, and we should look up PK handle in value.

                        // NOTE: it is not `number::decode_i64`.
                        value.as_slice().read_i64::<BigEndian>().map_err(|_| {
                            Error::Other(box_err!("Failed to decode handle in value as i64"))
                        })?
                    } else {
                        // This is a normal index. The remaining payload part is the PK handle.
                        // Let's decode it and put in the column.

                        let flag = key_payload[0];
                        let mut val = &key_payload[1..];

                        match flag {
                            datum::INT_FLAG => number::decode_i64(&mut val).map_err(|_| {
                                Error::Other(box_err!("Failed to decode handle in key as i64"))
                            })?,
                            datum::UINT_FLAG => {
                                (number::decode_u64(&mut val).map_err(|_| {
                                    Error::Other(box_err!("Failed to decode handle in key as u64"))
                                })?) as i64
                            }
                            _ => {
                                return Err(Error::Other(box_err!(
                                    "Unexpected handle flag {}",
                                    flag
                                )));
                            }
                        }
                    };

                    columns[self.columns_len_without_handle]
                        .mut_decoded()
                        .push_int(Some(handle_val));
                }

                columns.debug_assert_columns_equal_length();

                if columns.rows_len() >= expect_rows {
                    break;
                }
            } else {
                self.ranges.consume();
            }
        }

        Ok(is_drained)
    }
}

impl<S: Store> BatchExecutor for BatchIndexScanExecutor<S> {
    #[inline]
    fn next_batch(&mut self, expect_rows: usize) -> BatchExecuteResult {
        assert!(!self.is_ended);
        assert!(expect_rows > 0);

        // Construct empty columns, with PK in decoded format and the rest in raw format.
        let columns_len = self.context.columns_info.len();
        let mut columns = Vec::with_capacity(columns_len);
        for _ in 0..self.columns_len_without_handle {
            columns.push(LazyBatchColumn::raw_with_capacity(expect_rows));
        }
        if self.decode_handle {
            // For primary key, we construct a decoded `VectorValue` because it is directly
            // stored as i64, without a datum flag, in the value (for unique index).
            // Note that for normal index, primary key is appended at the end of key with a
            // datum flag.
            columns.push(LazyBatchColumn::decoded_with_capacity_and_tp(
                expect_rows,
                EvalType::Int,
            ));
        }

        let mut data = LazyBatchColumnVec::from(columns);
        let is_drained = self.fill_batch_rows(expect_rows, &mut data);

        // TODO
        // After calling `fill_batch_rows`, columns' length may not be identical in some special
        // cases, for example, meet decoding errors when decoding the last column. We need to trim
        // extra elements.

        // TODO
        // If `is_drained.is_err()`, it means that there is an error after *successfully* retrieving
        // these rows. After that, if we only consumes some of the rows (TopN / Limit), we should
        // ignore this error.

        match &is_drained {
            Err(_) => self.is_ended = true,
            Ok(true) => self.is_ended = true,
            Ok(false) => {}
        };

        BatchExecuteResult { data, is_drained }
    }
}

/*
#[cfg(test)]
mod tests {
    use std::i64;

    use kvproto::kvrpcpb::IsolationLevel;

    use crate::storage::SnapshotStore;

    use super::*;
    use crate::coprocessor::dag::scanner::tests::{
        get_point_range, get_range, prepare_table_data, TestStore,
    };

    const TABLE_ID: i64 = 1;
    const KEY_NUMBER: usize = 100;

    #[test]
    fn test_point_get() {
        let test_data = prepare_table_data(KEY_NUMBER, TABLE_ID);
        let mut test_store = TestStore::new(&test_data.kv_data);
        let context = {
            let columns_info = test_data.get_prev_2_cols();
            ExecutorContext::new(columns_info)
        };

        const HANDLE: i64 = 0;

        // point get returns none
        let r1 = get_point_range(TABLE_ID, i64::MIN);
        // point get return something
        let r2 = get_point_range(TABLE_ID, HANDLE);
        let ranges = vec![r1, r2];

        let (snapshot, start_ts) = test_store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let mut table_scanner =
            BatchTableScanExecutor::new(store, context.clone(), ranges,false).unwrap();

        let result = table_scanner.next_batch(10);
        assert!(result.error.is_none());
        assert_eq!(result.data.columns_len(), 2);
        assert_eq!(result.data.rows_len(), 1);

        let expect_row = &test_data.expect_rows[HANDLE as usize];
        for (idx, col) in context.columns_info.iter().enumerate() {
            let cid = col.get_column_id();
            let v = &result.data[idx].raw()[0];
            assert_eq!(expect_row[&cid].as_slice(), v.as_slice());
        }
    }

    #[test]
    fn test_multiple_ranges() {
        use rand::Rng;

        let test_data = prepare_table_data(KEY_NUMBER, TABLE_ID);
        let mut test_store = TestStore::new(&test_data.kv_data);
        let context = {
            let mut columns_info = test_data.get_prev_2_cols();
            columns_info.push(test_data.get_col_pk());
            ExecutorContext::new(columns_info)
        };

        let r1 = get_range(TABLE_ID, i64::MIN, 0);
        let r2 = get_range(TABLE_ID, 0, (KEY_NUMBER / 2) as i64);
        let r3 = get_point_range(TABLE_ID, (KEY_NUMBER / 2) as i64);
        let r4 = get_range(TABLE_ID, (KEY_NUMBER / 2) as i64 + 1, i64::MAX);
        let ranges = vec![r1, r2, r3, r4];

        let (snapshot, start_ts) = test_store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let mut table_scanner =
            BatchTableScanExecutor::new(context.clone(), false, ranges, store).unwrap();

        let mut data = table_scanner.next_batch(0).data;
        {
            let mut rng = rand::thread_rng();
            loop {
                let mut result = table_scanner.next_batch(rng.gen_range(1, KEY_NUMBER / 5));
                assert!(result.error.is_none());
                if result.data.rows_len() == 0 {
                    break;
                }
                data.append(&mut result.data);
            }
        }
        assert_eq!(data.columns_len(), 3);
        assert_eq!(data.rows_len(), KEY_NUMBER);

        for row_index in 0..KEY_NUMBER {
            // data[2] should be PK column, let's check it first.
            assert_eq!(
                data[2].decoded().as_int_slice()[row_index],
                Some(row_index as i64)
            );
            // check rest columns
            let expect_row = &test_data.expect_rows[row_index];
            for (col_index, col) in context.columns_info.iter().enumerate() {
                if col.get_pk_handle() {
                    continue;
                }
                let cid = col.get_column_id();
                let v = &data[col_index].raw()[row_index];
                assert_eq!(expect_row[&cid].as_slice(), v.as_slice());
            }
        }
    }
}
*/
