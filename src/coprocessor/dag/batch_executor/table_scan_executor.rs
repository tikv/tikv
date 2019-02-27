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

use crate::storage::Store;

use crate::util::collections::HashMap;

use super::interface::*;
use crate::coprocessor::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use crate::coprocessor::dag::Scanner;
use crate::coprocessor::Result;

pub struct BatchTableScanExecutor<S: Store>(
    super::scan_executor::ScanExecutor<
        S,
        TableScanExecutorImpl,
        super::ranges_iter::PointRangeEnable,
    >,
);

impl<S: Store> BatchTableScanExecutor<S> {
    pub fn new(
        store: S,
        context: BatchExecutorContext,
        key_ranges: Vec<KeyRange>,
        desc: bool,
    ) -> Result<Self> {
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

        let imp = TableScanExecutorImpl {
            context,
            key_only,
            handle_index,
            column_id_index,
            is_column_filled,
        };
        let wrapper = super::scan_executor::ScanExecutor::new(
            imp,
            store,
            desc,
            key_ranges,
            super::ranges_iter::PointRangeEnable,
        )?;
        Ok(Self(wrapper))
    }
}

impl<S: Store> BatchExecutor for BatchTableScanExecutor<S> {
    #[inline]
    fn next_batch(&mut self, expect_rows: usize) -> BatchExecuteResult {
        self.0.next_batch(expect_rows)
    }

    #[inline]
    fn collect_statistics(&mut self, destination: &mut BatchExecuteStatistics) {
        self.0.collect_statistics(destination);
    }
}

struct TableScanExecutorImpl {
    context: BatchExecutorContext,

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
}

impl super::scan_executor::ScanExecutorImpl for TableScanExecutorImpl {
    fn get_context(&self) -> &BatchExecutorContext {
        &self.context
    }

    fn build_scanner<S: Store>(
        &self,
        store: &S,
        desc: bool,
        range: KeyRange,
    ) -> Result<Scanner<S>> {
        Scanner::new(
            store,
            crate::coprocessor::dag::ScanOn::Table,
            desc,
            self.key_only,
            range,
        )
    }

    fn build_column_vec(&self, expect_rows: usize) -> LazyBatchColumnVec {
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

        LazyBatchColumnVec::from(columns)
    }

    fn process_kv_pair(
        &mut self,
        key: &[u8],
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        use crate::coprocessor::codec::{datum, table};
        use crate::util::codec::number;

        let columns_len = self.context.columns_info.len();
        let mut decoded_columns = 0;

        // Decode handle from key if handle is specified in columns.
        if self.handle_index != std::usize::MAX {
            if !self.is_column_filled[self.handle_index] {
                let handle_id = table::decode_handle(key)?;
                // FIXME: The columns may be not in the same length if there is error.
                // TODO: We should avoid calling `push_int` repeatly. Instead we should specialize
                // a `&mut Vec` first.
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
            let mut remaining = value;
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

        Ok(())
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
            BatchExecutorContext::with_default_config(columns_info)
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
            BatchExecutorContext::with_default_config(columns_info)
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
