// Copyright 2018 PingCAP, Inc.
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

use crate::util::collections::HashMap;

use super::interface::*;
use super::ranges_consumer::{ConsumerResult, RangesConsumer};
use crate::coprocessor::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use crate::coprocessor::codec::table;
use crate::coprocessor::dag::Scanner;
use crate::coprocessor::*;

pub struct BatchTableScanExecutor<S: Store> {
    context: ExecutorContext,
    store: S,
    desc: bool,

    /// Consume and produce ranges.
    ranges: RangesConsumer,

    /// Row scanner.
    ///
    /// It is optional because sometimes it is not needed, e.g. when point range is given.
    /// Also, the value may be re-constructed several times if there are multiple key ranges.
    scanner: Option<Scanner<S>>,

    /// Whether or not KV value can be omitted.
    ///
    /// It will be set to `true` if only PK handle column exists in `context.columns_info`.
    key_only: bool,

    /// The index in output row to put the handle.
    ///
    /// If PK handle column does not exist in `context.columns_info`, this field will be set to
    /// `usize::MAX`.
    handle_index: usize,

    /// A hash map to map column id to the index of `context.columns_info`.
    column_id_index: HashMap<i64, usize>,

    /// A vector of flags indicating whether corresponding column is filled in `next_batch`.
    /// It is a struct level field in order to prevent repeated memory allocations since its length
    /// is fixed for each `next_batch` call.
    is_column_filled: Vec<bool>,

    /// A flag indicating whether this executor is ended. When table is drained or there was an
    /// error scanning the table, this flag will be set to `true` and `next_batch` should be never
    /// called again.
    is_ended: bool,
}

impl<S: Store> BatchTableScanExecutor<S> {
    pub fn new(
        store: S,
        context: ExecutorContext,
        mut key_ranges: Vec<KeyRange>,
        desc: bool,
    ) -> Result<Self> {
        table::check_table_ranges(&key_ranges)?;
        if desc {
            key_ranges.reverse();
        }

        let is_column_filled = vec![false; context.columns_info.len()];

        let mut key_only = true;
        let mut handle_index = std::usize::MAX;
        let mut column_id_index = HashMap::default();
        for (index, column_info) in context.columns_info.iter().enumerate() {
            if column_info.get_pk_handle() {
                handle_index = index;
            } else {
                key_only = false;
                column_id_index.insert(column_info.get_column_id(), index);
            }
        }

        Ok(Self {
            context,
            store,
            desc,
            ranges: RangesConsumer::new(key_ranges, true),
            scanner: None,

            handle_index,
            key_only,
            column_id_index,

            is_column_filled,
            is_ended: false,
        })
    }

    /// Creates or resets the range of inner scanner.
    #[inline]
    fn reset_range(&mut self, range: KeyRange) -> Result<()> {
        use crate::coprocessor::dag::ScanOn;

        self.scanner = Some(Scanner::new(
            &self.store,
            ScanOn::Table,
            self.desc,
            self.key_only,
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

        assert!(expect_rows > 0);

        let mut is_drained = false;
        let columns_len = self.context.columns_info.len();

        loop {
            let range = self.ranges.next();
            let some_row = match range {
                ConsumerResult::NewPointRange(r) => self.point_get(r)?,
                // There might be lock when retriving this row.
                ConsumerResult::NewNonPointRange(r) => {
                    self.reset_range(r)?;
                    self.scan_next()?
                }
                ConsumerResult::Continue => self.scan_next()?,
                // There might be lock when retriving this row.
                ConsumerResult::Drained => {
                    is_drained = true;
                    break;
                }
            };
            if let Some((key, value)) = some_row {
                let mut decoded_columns = 0;

                // Decode handle from key if handle is specified in columns.
                if self.handle_index != std::usize::MAX {
                    if !self.is_column_filled[self.handle_index] {
                        let handle_id = table::decode_handle(&key)?;
                        // FIXME: The columns may be not in the same length if there is error.
                        columns[self.handle_index]
                            .mut_decoded()
                            .push_int(Some(handle_id));
                        decoded_columns += 1;
                        self.is_column_filled[self.handle_index] = true;
                    }
                    // TODO: Shall we just returns error when met
                    // `self.unsafe_raw_rows_cache_filled[self.handle_index] == true`?
                }

                if value.is_empty() || (value.len() == 1 && value[0] == datum::NIL_FLAG) {
                    // Do nothing
                } else {
                    // The layout of value is: [col_id_1, value_1, col_id_2, value_2, ...]
                    // where each element is datum encoded.
                    // The column id datum must be in var i64 type.
                    let mut remaining = value.as_slice();
                    while !remaining.is_empty() && decoded_columns < columns_len {
                        if remaining[0] != datum::VAR_INT_FLAG {
                            return Err(box_err!("Expect VAR_INT flag for column id"));
                        }
                        remaining = &remaining[1..];
                        let column_id = box_try!(number::decode_var_i64(&mut remaining));
                        let (val, new_remaining) = datum::split_datum(remaining, false)?;
                        // FIXME: The columns may be not in the same length if there is error.
                        let some_index = self.column_id_index.get(&column_id);
                        if let Some(index) = some_index {
                            let index = *index;
                            if !self.is_column_filled[index] {
                                columns[index].push_raw(val);
                                decoded_columns += 1;
                                self.is_column_filled[index] = true;
                            }
                            // TODO: Shall we just returns error when met
                            // `self.unsafe_raw_rows_cache_filled[index] == true`?
                        }
                        remaining = new_remaining;
                    }
                }

                // Some fields may be missing in the row, we push empty slot to make all columns
                // in same length.
                for i in 0..columns_len {
                    if !self.is_column_filled[i] {
                        // Missing fields must not be a primary key, so it must be
                        // `LazyBatchColumn::raw`.
                        columns[i].push_raw(&[]);
                    } else {
                        // Reset to not-filled, prepare for next function call.
                        self.is_column_filled[i] = false;
                    }
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

impl<S: Store> BatchExecutor for BatchTableScanExecutor<S> {
    #[inline]
    fn next_batch(&mut self, expect_rows: usize) -> BatchExecuteResult {
        assert!(!self.is_ended);
        assert!(expect_rows > 0);

        // Construct empty columns, with PK in decoded format and the rest in raw format.
        let columns_len = self.context.columns_info.len();
        let mut columns = Vec::with_capacity(columns_len);
        for i in 0..columns_len {
            if i == self.handle_index {
                // For primary key, we construct a decoded `VectorValue` because it is directly
                // stored as i64, without a datum flag, at the end of key.
                columns.push(LazyBatchColumn::decoded_with_capacity_and_tp(
                    expect_rows,
                    EvalType::Int,
                ));
            } else {
                columns.push(LazyBatchColumn::raw_with_capacity(expect_rows));
            }
        }

        let mut data = LazyBatchColumnVec::from(columns);
        let is_drained = self.fill_batch_rows(expect_rows, &mut data);

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
            BatchTableScanExecutor::new(store, context.clone(), ranges, false).unwrap();

        let result = table_scanner.next_batch(10);
        assert!(result.is_drained.as_ref().unwrap(), true);
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
            BatchTableScanExecutor::new(store, context.clone(), ranges, false).unwrap();

        let mut data = table_scanner.next_batch(1).data;
        {
            let mut rng = rand::thread_rng();
            loop {
                let mut result = table_scanner.next_batch(rng.gen_range(1, KEY_NUMBER / 5));
                assert!(result.is_drained.is_ok());
                data.append(&mut result.data);
                if result.is_drained.unwrap() {
                    break;
                }
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
