// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{marker::PhantomData, sync::Arc};

use api_version::{ApiV1, KvFormat, keyspace::KvPair};
use async_trait::async_trait;
use collections::HashMap;
use kvproto::coprocessor::KeyRange;
use tidb_query_common::{
    Result,
    error::EvaluateError,
    storage::{
        IntervalRange, PointRange, Range, Storage,
        ranges_iter::{IterStatus, RangesIterator},
    },
};
use tidb_query_datatype::{
    codec::batch::LazyBatchColumnVec,
    expr::{EvalConfig, EvalContext},
};
use tipb::{ColumnInfo, FieldType, VersionedLookup};

use crate::{
    interface::*,
    table_scan_executor::{HandleIndicesVec, TableScanExecutorImpl},
    util::scan_executor::{
        ScanExecutorImpl, check_columns_info_supported, field_type_from_column_info,
    },
};

const KEY_BUFFER_CAPACITY: usize = 64;
pub struct BatchVersionedLookupExecutor<S: Storage, F: KvFormat> {
    storage: S,
    ranges_iter: RangesIterator,
    versions: Vec<u64>,
    version_index: usize,
    table_scan_helper: TableScanExecutorImpl,

    // The following fields are used for calculating scanned range.
    // for versioned scan, all its range is point range, we maintain
    // the working_begin_key and working_end_key to calculate the scanned range
    // for working_begin_key, it will be set to the first PointRange's key, and
    // be updated after each take_scanned_range call
    is_scanned_range_aware: bool,
    working_begin_key: Vec<u8>,
    working_end_key: Vec<u8>,

    /// A flag indicating whether this executor is ended. When table is drained
    /// or there was an error scanning the table, this flag will be set to
    /// `true` and `next_batch` should be never called again.
    is_ended: bool,
    _phantom: PhantomData<F>, // Tie F to the struct
}

// We assign a dummy type `Box<dyn Storage<Statistics = ()>>` so that we can
// omit the type when calling `check_supported`.
impl BatchVersionedLookupExecutor<Box<dyn Storage<Statistics = ()>>, ApiV1> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &VersionedLookup) -> Result<()> {
        check_columns_info_supported(descriptor.get_columns())
    }
}

impl<S: Storage, F: KvFormat> BatchVersionedLookupExecutor<S, F> {
    pub fn new(
        storage: S,
        config: Arc<EvalConfig>,
        columns_info: Vec<ColumnInfo>,
        key_ranges: Vec<KeyRange>,
        primary_column_ids: Vec<i64>,
        versions: Vec<u64>,
        is_scanned_range_aware: bool,
    ) -> Result<Self> {
        tidb_query_datatype::codec::table::check_table_ranges::<F>(&key_ranges)?;
        let ranges: Vec<Range> = key_ranges
            .into_iter()
            .map(|r| Range::from_pb_range(r, true))
            .collect();
        if ranges.len() > versions.len() {
            return Err(tidb_query_common::error::EvaluateError::Other(
                "key range and versions not match for BatchVersionedLookupExecutor".into(),
            )
            .into());
        }
        let final_versions = if ranges.len() == versions.len() {
            versions
        } else {
            // in this case, we should use the last `len(ranges)` versions
            versions[versions.len() - ranges.len()..].to_vec()
        };

        for range in &ranges {
            if !matches!(range, Range::Point(_)) {
                // If any range is not a point range, return an error.
                return Err(tidb_query_common::error::EvaluateError::Other(
                    "BatchVersionedLookupExecutor only support point range".into(),
                )
                .into());
            }
        }
        let mut working_begin_key: Vec<u8> = Vec::with_capacity(KEY_BUFFER_CAPACITY);
        // If there are ranges, set the working_begin_key to the first range's key.
        // This is used to calculate the scanned range later.
        if ranges.len() > 0 {
            match &ranges[0] {
                Range::Point(point) => {
                    // If the first range is a point range, set the working_begin_key to its key.
                    working_begin_key.extend_from_slice(&point.0);
                }
                _ => unreachable!("BatchVersionedLookupExecutor should only support point ranges"),
            }
        }

        // todo reuse this with BatchTableScanExecutor
        let is_column_filled = vec![false; columns_info.len()];
        let mut handle_indices = HandleIndicesVec::new();
        let mut schema = Vec::with_capacity(columns_info.len());
        let mut columns_default_value = Vec::with_capacity(columns_info.len());
        let mut column_id_index = HashMap::default();

        for (index, mut ci) in columns_info.into_iter().enumerate() {
            // For each column info, we need to extract the following info:
            // - Corresponding field type (push into `schema`).
            schema.push(field_type_from_column_info(&ci));

            // - Prepare column default value (will be used to fill missing column later).
            columns_default_value.push(ci.take_default_val());

            // - Store the index of the PK handles.
            // - Check whether or not we don't need KV values (iff PK handle is given).
            if ci.get_pk_handle() {
                handle_indices.push(index);
            } else {
                column_id_index.insert(ci.get_column_id(), index);
            }

            // Note: if two PK handles are given, we will only preserve the
            // *last* one. Also if two columns with the same column
            // id are given, we will only preserve the *last* one.
        }

        let ts = TableScanExecutorImpl {
            context: EvalContext::new(config),
            schema,
            columns_default_value,
            column_id_index,
            handle_indices,
            primary_column_ids,
            is_column_filled,
            allow_missing_columns: false,
        };
        Ok(Self {
            storage,
            ranges_iter: RangesIterator::new(ranges),
            versions: final_versions,
            version_index: 0,
            table_scan_helper: ts,
            is_scanned_range_aware,
            working_begin_key,
            working_end_key: Vec::with_capacity(KEY_BUFFER_CAPACITY),
            is_ended: false,
            _phantom: PhantomData, // Initialize PhantomData
        })
    }

    fn update_working_end_key_from_point_range(&mut self, point: &PointRange) {
        self.working_end_key.clear();
        self.working_end_key.extend_from_slice(&point.0);
        self.working_end_key.push(0); // Ensure end key is exclusive
    }

    fn update_working_end_key_from_row(&mut self, row: &F::KvPair) {
        self.working_end_key.clear();
        self.working_end_key.extend(row.key());
        self.working_end_key.push(0); // Ensure end key is exclusive
    }

    fn next(&mut self, update_scanned_range: bool) -> Result<Option<F::KvPair>> {
        loop {
            let range = self.ranges_iter.next();
            let some_row = match range {
                IterStatus::NewRange(Range::Point(r)) => {
                    if self.is_scanned_range_aware && self.ranges_iter.is_drained() {
                        // if ranges_iter is drained, we should update the working end key,
                        // otherwise if update_scanned_range is not true,
                        // the working_end_key will not be updated
                        self.update_working_end_key_from_point_range(&r);
                    }
                    self.ranges_iter.notify_drained();
                    self.version_index += 1;
                    self.storage.get_with_version(
                        self.versions[self.version_index - 1],
                        false,
                        r,
                    )?
                }
                IterStatus::Drained => {
                    return Ok(None); // drained
                }
                _ => unreachable!("BatchVersionedLookupExecutor should only handle point ranges"),
            };
            if let Some(row) = some_row {
                let kv = F::make_kv_pair(row).map_err(|e| EvaluateError::Other(e.into()))?;
                if self.is_scanned_range_aware
                    && !self.ranges_iter.is_drained()
                    && update_scanned_range
                {
                    // update end key
                    self.update_working_end_key_from_row(&kv);
                }
                return Ok(Some(kv));
            } else {
                // the target data is gc-ed, so read the next row
                // this should rarely happens
                self.table_scan_helper.context.warnings.append_warning(
                    tidb_query_datatype::codec::Error::Other("some data is gc-ed".into()),
                );
                continue;
            }
        }
    }
    /// Fills a column vector and returns whether or not all ranges are drained.
    ///
    /// The columns are ensured to be regular even if there are errors during
    /// the process.
    async fn fill_column_vec(
        &mut self,
        scan_rows: usize,
        columns: &mut LazyBatchColumnVec,
    ) -> Result<bool> {
        assert!(scan_rows > 0);

        for i in 0..scan_rows {
            let some_row = self.next(i == scan_rows - 1)?;
            if let Some(row) = some_row {
                // Retrieved one row.

                let (key, value) = row.kv();
                if let Err(e) = self.table_scan_helper.process_kv_pair(key, value, columns) {
                    // When there are errors in `process_kv_pair`, columns' length may not be
                    // identical. For example, the filling process may be partially done so that
                    // first several columns have N rows while the rest have N-1 rows. Since we do
                    // not immediately fail when there are errors, these irregular columns may
                    // further cause future executors to panic. So let's truncate these columns to
                    // make they all have N-1 rows in that case.
                    columns.truncate_into_equal_length();
                    return Err(e);
                }
            } else {
                // Drained
                return Ok(true);
            }
        }
        // Not drained
        Ok(false)
    }
}

#[async_trait]
impl<S: Storage, F: KvFormat> BatchExecutor for BatchVersionedLookupExecutor<S, F> {
    type StorageStats = S::Statistics;

    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.table_scan_helper.schema()
    }

    #[inline]
    async fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        assert!(!self.is_ended);
        assert!(scan_rows > 0);

        let mut logical_columns = self.table_scan_helper.build_column_vec(scan_rows);
        let is_drained = self.fill_column_vec(scan_rows, &mut logical_columns).await;

        logical_columns.assert_columns_equal_length();
        let logical_rows = (0..logical_columns.rows_len()).collect();

        // TODO
        // If `is_drained.is_err()`, it means that there is an error after
        // *successfully* retrieving these rows. After that, if we only consumes
        // some of the rows (TopN / Limit), we should ignore this error.

        let is_drained = match is_drained {
            // Note: `self.is_ended` is only used for assertion purpose.
            Err(e) => {
                self.is_ended = true;
                Err(e)
            }
            Ok(true) => {
                self.is_ended = true;
                Ok(BatchExecIsDrain::Drain)
            }
            Ok(false) => Ok(BatchExecIsDrain::Remain),
        };

        BatchExecuteResult {
            physical_columns: logical_columns,
            logical_rows,
            is_drained,
            warnings: self.table_scan_helper.mut_context().take_warnings(),
        }
    }

    #[inline]
    fn collect_exec_stats(&mut self, _dest: &mut ExecuteStats) {}

    #[inline]
    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        self.storage.collect_statistics(dest);
    }

    #[inline]
    fn take_scanned_range(&mut self) -> IntervalRange {
        assert!(self.is_scanned_range_aware);

        let mut range = IntervalRange::default();
        std::mem::swap(&mut range.lower_inclusive, &mut self.working_begin_key);
        std::mem::swap(&mut range.upper_exclusive, &mut self.working_end_key);

        self.working_begin_key
            .extend_from_slice(&range.upper_exclusive);

        range
    }

    #[inline]
    fn can_be_cached(&self) -> bool {
        // always return false because the cached key in TiDB does not consider the
        // versions in VersionedLookup
        return false;
    }
}
