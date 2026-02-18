// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use DecodeHandleStrategy::*;
use api_version::{ApiV1, KvFormat};
use async_trait::async_trait;
use codec::{number::NumberCodec, prelude::NumberDecoder};
use itertools::izip;
use kvproto::coprocessor::KeyRange;
use tidb_query_common::{
    Result,
    storage::{IntervalRange, Storage},
};
use tidb_query_datatype::{
    EvalType, FieldTypeAccessor,
    codec::{
        Datum,
        batch::{LazyBatchColumn, LazyBatchColumnVec},
        collation::collator::PADDING_SPACE,
        datum,
        datum::DatumDecoder,
        row::v2::{RowSlice, V1CompatibleEncoder, decode_v2_u64},
        table,
        table::{INDEX_VALUE_VERSION_FLAG, MAX_OLD_ENCODED_VALUE_LEN, check_index_key},
    },
    expr::{EvalConfig, EvalContext},
};
use tipb::{ColumnInfo, FieldType, IndexScan};
use txn_types::TimeStamp;

use super::util::scan_executor::*;
use crate::interface::*;

pub struct BatchIndexScanExecutor<S: Storage, F: KvFormat>(
    ScanExecutor<S, IndexScanExecutorImpl, F>,
);

// We assign a dummy type `Box<dyn Storage<Statistics = ()>>` so that we can
// omit the type when calling `check_supported`.
impl BatchIndexScanExecutor<Box<dyn Storage<Statistics = ()>>, ApiV1> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &IndexScan) -> Result<()> {
        check_columns_info_supported(descriptor.get_columns())
    }
}

impl<S: Storage, F: KvFormat> BatchIndexScanExecutor<S, F> {
    pub fn new(
        storage: S,
        config: Arc<EvalConfig>,
        columns_info: Vec<ColumnInfo>,
        key_ranges: Vec<KeyRange>,
        primary_column_ids_len: usize,
        is_backward: bool,
        unique: bool,
        is_scanned_range_aware: bool,
        is_fill_extra_common_handle_key: bool,
    ) -> Result<Self> {
        // Note 1: `unique = true` doesn't completely mean that it is a unique index
        // scan. Instead it just means that we can use point-get for this index.
        // In the following scenarios `unique` will be `false`:
        // - scan from a non-unique index
        // - scan from a unique index with like: where unique-index like xxx
        //
        // Note 2: Unlike table scan executor, the accepted `columns_info` of index scan
        // executor is strictly stipulated. The order of columns in the schema must be
        // the same as index data stored and if PK handle is needed it must be placed as
        // the last one.
        //
        // Note 3: Currently TiDB may send multiple PK handles to TiKV (but only the
        // last one is real). We accept this kind of request for compatibility
        // considerations, but will be forbidden soon.
        //
        // Note 4: When process global indexes, an extra partition ID
        // column with column ID `table::EXTRA_PHYSICAL_TABLE_ID_COL_ID` will
        // append to column info to indicate which partition id handles belong to.
        //
        // Note 5: When process a partitioned table's index under
        // tidb_partition_prune_mode = 'dynamic' and with either an active transaction
        // buffer or with a SelectLock/pessimistic lock, we need to return the physical
        // table id since several partitions may be included in the range.
        //
        // Note 6: Also int_handle (-1), EXTRA_PARTITION_ID_COL_ID (-2) and
        // EXTRA_PHYSICAL_TABLE_ID_COL_ID (-3) must be requested in this order in
        // columns_info! since current implementation looks for them backwards for -3,
        // -2, -1.
        let physical_table_id_column_cnt = columns_info.last().map_or(0, |ci| {
            (ci.get_column_id() == table::EXTRA_PHYSICAL_TABLE_ID_COL_ID) as usize
        });
        let pid_column_cnt = columns_info
            .get(columns_info.len() - 1 - physical_table_id_column_cnt)
            .map_or(0, |ci| {
                (ci.get_column_id() == table::EXTRA_PARTITION_ID_COL_ID) as usize
            });
        let is_int_handle = columns_info
            .get(columns_info.len() - 1 - pid_column_cnt - physical_table_id_column_cnt)
            .is_some_and(|ci| ci.get_pk_handle());
        let is_common_handle = primary_column_ids_len > 0;
        let (decode_handle_strategy, handle_column_cnt) = match (is_int_handle, is_common_handle) {
            (false, false) => (NoDecode, 0),
            (false, true) => (DecodeCommonHandle, primary_column_ids_len),
            (true, false) => (DecodeIntHandle, 1),
            // TiDB may accidentally push down both int handle or common handle.
            // However, we still try to decode int handle.
            _ => {
                return Err(other_err!(
                    "Both int handle and common handle are push downed"
                ));
            }
        };

        if handle_column_cnt + pid_column_cnt + physical_table_id_column_cnt > columns_info.len() {
            return Err(other_err!(
                "The number of handle columns exceeds the length of `columns_info`"
            ));
        }

        let schema: Vec<_> = columns_info
            .iter()
            .map(|ci| field_type_from_column_info(ci))
            .collect();

        let columns_id_without_handle: Vec<_> = columns_info[..columns_info.len()
            - handle_column_cnt
            - pid_column_cnt
            - physical_table_id_column_cnt]
            .iter()
            .map(|ci| ci.get_column_id())
            .collect();

        let columns_id_for_common_handle = columns_info[columns_id_without_handle.len()
            ..columns_info.len() - pid_column_cnt - physical_table_id_column_cnt]
            .iter()
            .map(|ci| ci.get_column_id())
            .collect();

        let imp = IndexScanExecutorImpl {
            context: EvalContext::new(config),
            schema,
            columns_id_without_handle,
            columns_id_for_common_handle,
            decode_handle_strategy,
            pid_column_cnt,
            physical_table_id_column_cnt,
            index_version: -1,
            fill_extra_common_handle_key: is_fill_extra_common_handle_key,
        };
        let wrapper = ScanExecutor::new(ScanExecutorOptions {
            imp,
            storage,
            key_ranges,
            is_backward,
            is_key_only: false,
            accept_point_range: unique,
            is_scanned_range_aware,
            load_commit_ts: false,
        })?;
        Ok(Self(wrapper))
    }
}

#[async_trait]
impl<S: Storage, F: KvFormat> BatchExecutor for BatchIndexScanExecutor<S, F> {
    type StorageStats = S::Statistics;

    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.0.schema()
    }

    #[inline]
    fn intermediate_schema(&self, index: usize) -> Result<&[FieldType]> {
        self.0.intermediate_schema(index)
    }

    #[inline]
    fn consume_and_fill_intermediate_results(
        &mut self,
        results: &mut [Vec<BatchExecuteResult>],
    ) -> Result<()> {
        self.0.consume_and_fill_intermediate_results(results)
    }

    #[inline]
    async fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        self.0.next_batch(scan_rows).await
    }

    #[inline]
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        self.0.collect_exec_stats(dest);
    }

    #[inline]
    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        self.0.collect_storage_stats(dest);
    }

    #[inline]
    fn take_scanned_range(&mut self) -> IntervalRange {
        self.0.take_scanned_range()
    }

    #[inline]
    fn can_be_cached(&self) -> bool {
        self.0.can_be_cached()
    }
}

#[derive(PartialEq, Debug)]
enum DecodeHandleStrategy {
    NoDecode,
    DecodeIntHandle,
    DecodeCommonHandle,
}

struct IndexScanExecutorImpl {
    /// See `TableScanExecutorImpl`'s `context`.
    context: EvalContext,

    /// See `TableScanExecutorImpl`'s `schema`.
    schema: Vec<FieldType>,

    /// ID of interested columns (exclude PK handle column).
    columns_id_without_handle: Vec<i64>,

    columns_id_for_common_handle: Vec<i64>,

    /// If true, also fill the `extra_common_handle_keys` in
    /// `LazyBatchColumnVec` for each row.
    fill_extra_common_handle_key: bool,

    /// The strategy to decode handles.
    /// Handle will be always placed in the last column.
    decode_handle_strategy: DecodeHandleStrategy,

    /// Number of partition ID columns, now it can only be 0 or 1.
    /// Must be after all normal columns and handle, but before
    /// physical_table_id_column
    pid_column_cnt: usize,

    /// Number of Physical Table ID columns, can only be 0 or 1.
    /// Must be last, after pid_column
    physical_table_id_column_cnt: usize,

    index_version: i64,
}

impl ScanExecutorImpl for IndexScanExecutorImpl {
    #[inline]
    fn schema(&self) -> &[FieldType] {
        &self.schema
    }

    #[inline]
    fn mut_context(&mut self) -> &mut EvalContext {
        &mut self.context
    }

    /// Constructs empty columns, with PK containing int handle in decoded
    /// format and the rest in raw format.
    ///
    /// Note: the structure of the constructed column is the same as table scan
    /// executor but due to different reasons.
    fn build_column_vec(&self, scan_rows: usize) -> LazyBatchColumnVec {
        let columns_len = self.schema.len();
        let mut columns = Vec::with_capacity(columns_len);

        for _ in 0..self.columns_id_without_handle.len() {
            columns.push(LazyBatchColumn::raw_with_capacity(scan_rows));
        }

        match self.decode_handle_strategy {
            NoDecode => {}
            DecodeIntHandle => {
                columns.push(LazyBatchColumn::decoded_with_capacity_and_tp(
                    scan_rows,
                    EvalType::Int,
                ));
            }
            DecodeCommonHandle => {
                for _ in self.columns_id_without_handle.len()
                    ..columns_len - self.pid_column_cnt - self.physical_table_id_column_cnt
                {
                    columns.push(LazyBatchColumn::raw_with_capacity(scan_rows));
                }
            }
        }

        if self.pid_column_cnt > 0 {
            columns.push(LazyBatchColumn::decoded_with_capacity_and_tp(
                scan_rows,
                EvalType::Int,
            ));
        }

        if self.physical_table_id_column_cnt > 0 {
            columns.push(LazyBatchColumn::decoded_with_capacity_and_tp(
                scan_rows,
                EvalType::Int,
            ));
        }

        assert_eq!(columns.len(), columns_len);
        LazyBatchColumnVec::from(columns)
    }

    // Value layout: (see https://docs.google.com/document/d/1Co5iMiaxitv3okJmLYLJxZYCNChcjzswJMRr-_45Eqg/edit?usp=sharing)
    // ```text
    // 		+-- IndexValueVersion0  (with restore data, or common handle, or index is global)
    // 		|
    // 		|  Layout: TailLen | Options      | Padding      | [IntHandle] | [UntouchedFlag]
    // 		|  Length:   1     | len(options) | len(padding) |    8        |     1
    // 		|
    // 		|  TailLen:       len(padding) + len(IntHandle) + len(UntouchedFlag)
    // 		|  Options:       Encode some value for new features, such as common handle, new collations or global index.
    // 		|                 See below for more information.
    // 		|  Padding:       Ensure length of value always >= 10. (or >= 11 if UntouchedFlag exists.)
    // 		|  IntHandle:     Only exists when table use int handles and index is unique.
    // 		|  UntouchedFlag: Only exists when index is untouched.
    // 		|
    // 		+-- Old Encoding (without restore data, integer handle, local)
    // 		|
    // 		|  Layout: [Handle] | [UntouchedFlag]
    // 		|  Length:   8      |     1
    // 		|
    // 		|  Handle:        Only exists in unique index.
    // 		|  UntouchedFlag: Only exists when index is untouched.
    // 		|
    // 		|  If neither Handle nor UntouchedFlag exists, value will be one single byte '0' (i.e. []byte{'0'}).
    // 		|  Length of value <= 9, use to distinguish from the new encoding.
    // 		|
    // 		+-- IndexValueForClusteredIndexVersion1
    // 		|
    // 		|  Layout: TailLen |    VersionFlag  |    Version     ｜ Options      |   [UntouchedFlag]
    // 		|  Length:   1     |        1        |      1         |  len(options) |         1
    // 		|
    // 		|  TailLen:       len(UntouchedFlag)
    // 		|  Options:       Encode some value for new features, such as common handle, new collations or global index.
    // 		|                 See below for more information.
    // 		|  UntouchedFlag: Only exists when index is untouched.
    // 		|
    // 		|  Layout of Options:
    // 		|
    // 		|     Segment:             Common Handle                 |     Global Index      |   New Collation
    // 		|     Layout:  CHandle Flag | CHandle Len | CHandle      | PidFlag | PartitionID |    restoreData
    // 		|     Length:     1         | 2           | len(CHandle) |    1    |    8        |   len(restoreData)
    // 		|
    // 		|     Common Handle Segment: Exists when unique index used common handles.
    // 		|     Global Index Segment:  Exists when index is global.
    // 		|     New Collation Segment: Exists when new collation is used and index or handle contains non-binary string.
    // 		|     In v4.0, restored data contains all the index values. For example, (a int, b char(10)) and index (a, b).
    // 		|     The restored data contains both the values of a and b.
    // 		|     In v5.0, restored data contains only non-binary data(except for char and _bin). In the above example, the restored data contains only the value of b.
    // 		|     Besides, if the collation of b is _bin, then restored data is an integer indicate the spaces are truncated. Then we use sortKey
    // 		|     and the restored data together to restore original data.
    // ```
    #[inline]
    fn process_kv_pair(
        &mut self,
        key: &[u8],
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
        _commit_ts: Option<TimeStamp>,
    ) -> Result<()> {
        check_index_key(key)?;
        let key_payload = &key[table::PREFIX_LEN + table::ID_LEN..];
        if self.index_version == -1 {
            self.index_version = Self::get_index_version(value)?
        }
        if value.len() > MAX_OLD_ENCODED_VALUE_LEN {
            self.process_kv_general(key, key_payload, value, columns)
        } else {
            self.process_old_collation_kv(key, key_payload, value, columns)
        }
    }
}

#[derive(PartialEq, Debug, Copy, Clone)]
enum DecodeHandleOp<'a> {
    Nop,
    IntFromKey(&'a [u8]),
    IntFromValue(&'a [u8]),
    CommonHandle(&'a [u8]),
}

#[derive(PartialEq, Debug, Copy, Clone)]
enum RestoreData<'a> {
    NotExists,
    V4(&'a [u8]),
    V5(&'a [u8]),
}

#[derive(PartialEq, Debug, Copy, Clone)]
enum DecodePartitionIdOp<'a> {
    Nop,
    Pid(&'a [u8]),
}

impl IndexScanExecutorImpl {
    #[inline]
    fn decode_int_handle_from_value(&self, mut value: &[u8]) -> Result<i64> {
        // NOTE: it is not `number::decode_i64`.
        value
            .read_u64()
            .map_err(|_| other_err!("Failed to decode handle in value as i64"))
            .map(|x| x as i64)
    }

    /// ## V1/V2 Global Index Version (non-unique, non-clustered):
    /// ```text
    /// [INDEX_VALUE_PARTITION_ID_FLAG][partition_id (8 bytes)][handle_flag][handle_data]
    /// ```
    ///
    /// Where `handle_flag` is `datum::INT_FLAG` or `datum::UINT_FLAG`.
    ///
    /// # Note for V1
    /// In V1, partition ID appears in both KEY and VALUE:
    /// - KEY: Contains partition ID prefix
    /// - VALUE: Still contains partition ID (extracted by `split_partition_id`)
    ///
    /// # Note for V2
    /// In V2, partition ID appears ONLY in KEY, not in the VALUE.
    /// TODO: Add support for PartitionHandle / index pushdown for Global
    /// Indexes.
    #[inline]
    fn decode_int_handle_and_partition_from_key(&self, key: &[u8]) -> Result<(i64, Option<i64>)> {
        let (pid_bytes, val) = Self::split_partition_id(key)?;
        let partition_id = if !pid_bytes.is_empty() {
            Some(NumberCodec::decode_i64(pid_bytes))
        } else {
            None
        };
        let handle = self.decode_int_handle_from_key(val)?;
        Ok((handle, partition_id))
    }

    /// Decode int handle from index key.
    ///
    /// # Key Format
    ///
    /// ## Normal format (local index or old global index):
    /// ```text
    /// [handle_flag][handle_data]
    /// ```
    #[inline]
    fn decode_int_handle_from_key(&self, key: &[u8]) -> Result<i64> {
        if key.is_empty() {
            return Err(other_err!("Key is empty, cannot decode handle"));
        }
        let flag = key[0];
        let mut val = &key[1..];

        // TODO: Better to use `push_datum`. This requires us to allow `push_datum`
        // receiving optional time zone first.

        match flag {
            datum::INT_FLAG => val
                .read_i64()
                .map_err(|_| other_err!("Failed to decode handle in key as i64")),
            datum::UINT_FLAG => val
                .read_u64()
                .map_err(|_| other_err!("Failed to decode handle in key as u64"))
                .map(|x| x as i64),
            _ => Err(other_err!("Unexpected handle flag {}", flag)),
        }
    }

    fn extract_columns_from_row_format(
        &mut self,
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        let row = RowSlice::from_bytes(value)?;
        for (idx, col_id) in self.columns_id_without_handle.iter().enumerate() {
            if let Some((start, offset)) = row.search_in_non_null_ids(*col_id)? {
                let mut buffer_to_write = columns[idx].mut_raw().begin_concat_extend();
                buffer_to_write
                    .write_v2_as_datum(&row.values()[start..offset], &self.schema[idx])?;
            } else if row.search_in_null_ids(*col_id) {
                columns[idx].mut_raw().push(datum::DATUM_DATA_NULL);
            } else {
                return Err(other_err!("Unexpected missing column {}", col_id));
            }
        }
        Ok(())
    }

    fn extract_columns_from_datum_format(
        datum: &mut &[u8],
        columns: &mut [LazyBatchColumn],
    ) -> Result<()> {
        for (i, column) in columns.iter_mut().enumerate() {
            if datum.is_empty() {
                return Err(other_err!("{}th column is missing value", i));
            }
            let (value, remaining) = datum::split_datum(datum, false)?;
            column.mut_raw().push(value);
            *datum = remaining;
        }
        Ok(())
    }

    // Process index values that are in old collation, when
    // `new_collations_enabled_on_first_bootstrap` = true also will access this
    // function.
    // NOTE: We should extract the index columns from the key first,
    // and extract the handles from value if there is no handle in the key.
    // Otherwise, extract the handles from the key.
    fn process_old_collation_kv(
        &mut self,
        key: &[u8],
        mut key_payload: &[u8],
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        Self::extract_columns_from_datum_format(
            &mut key_payload,
            &mut columns[..self.columns_id_without_handle.len()],
        )?;

        // Track partition ID extracted from key (for pid_column_cnt handling below).
        let mut partition_id: Option<i64> = None;

        match self.decode_handle_strategy {
            NoDecode => {
                // Note: V2 key-only partition ID only exists for IntHandle
                // global indexes, when !PKIsHandle, and TiDB always sends
                // the handle column for those, so this path is only reached
                // for local indexes where decode_table_id correctly returns
                // the partition's table ID.
                if self.physical_table_id_column_cnt > 0 {
                    self.process_physical_table_id_column(key, columns)?;
                }
            }
            // For non-unique index, it is placed at the end of the key and any columns prior to it
            // are ensured to be interested. For unique index, it is placed in the
            // value.
            DecodeIntHandle if key_payload.is_empty() => {
                // This is a unique index, and we should look up PK int handle in the value.
                let handle_val = self.decode_int_handle_from_value(value)?;
                columns[self.columns_id_without_handle.len()]
                    .mut_decoded()
                    .push_int(Some(handle_val));
                if self.physical_table_id_column_cnt > 0 {
                    self.process_physical_table_id_column(key, columns)?;
                }
            }
            DecodeIntHandle => {
                // This is a non-unique index, and we should look up PK handle in the key.
                // For non-unique, non-clustered tables, Global Index Version V1+
                // the key also includes the partition id, to allow duplicate _tidb_rowid
                // across partitions.
                let (handle_val, pid) =
                    self.decode_int_handle_and_partition_from_key(key_payload)?;
                partition_id = pid;
                columns[self.columns_id_without_handle.len()]
                    .mut_decoded()
                    .push_int(Some(handle_val));
                // For global indexes, use the partition ID as physical table ID
                // instead of the index table ID from the key prefix.
                if self.physical_table_id_column_cnt > 0 {
                    if let Some(pid) = partition_id {
                        let col_index = columns.columns_len() - 1;
                        columns[col_index].mut_decoded().push_int(Some(pid));
                    } else {
                        self.process_physical_table_id_column(key, columns)?;
                    }
                }
            }
            DecodeCommonHandle => {
                // Otherwise, if the handle is common handle, we extract it from the key.
                let end_index =
                    columns.columns_len() - self.pid_column_cnt - self.physical_table_id_column_cnt;
                if self.fill_extra_common_handle_key {
                    columns
                        .mut_extra_common_handle_keys()
                        .push(key_payload.to_vec());
                }
                Self::extract_columns_from_datum_format(
                    &mut key_payload,
                    &mut columns[self.columns_id_without_handle.len()..end_index],
                )?;
                if self.physical_table_id_column_cnt > 0 {
                    self.process_physical_table_id_column(key, columns)?;
                }
            }
        }

        // Deprecated: Keep this for old tidb version during upgrade.
        // If need partition id, append partition id to the last column before physical
        // table id column if exists.
        if self.pid_column_cnt > 0 {
            let pid_col_idx = columns.columns_len() - self.physical_table_id_column_cnt - 1;
            if let Some(pid) = partition_id {
                columns[pid_col_idx].mut_decoded().push_int(Some(pid));
            } else {
                // No partition ID found in key. Fall back to table ID from key prefix
                // (local index on a partition has the partition's table ID in the key).
                let table_id = table::decode_table_id(key)?;
                columns[pid_col_idx].mut_decoded().push_int(Some(table_id));
            }
        }

        Ok(())
    }

    // restore_original_data restores the index values whose format is introduced in
    // TiDB 5.0. Unlike the format in TiDB 4.0, the new format is optimized for
    // storage space:
    // - If the index is a composed index, only the non-binary string column's value
    //   need to write to value, not all.
    // - If a string column's collation is _bin, then we only write the number of
    //   the truncated spaces to value.
    // - If a string column is char, not varchar, then we use the sortKey directly.
    //
    // The whole logic of this function is:
    // - For each column pass in, check if it needs the restored data to get to
    //   original data. If not, check the next column.
    // - Skip if the `sort key` is NULL, because the original data must be NULL.
    // - Depend on the collation if `_bin` or not. Process them differently to get
    //   the correct original data.
    // - Write the original data into the column, we need to make sure pop() is
    //   called.
    fn restore_original_data<'a>(
        &self,
        restored_values: &[u8],
        column_iter: impl Iterator<Item = (&'a FieldType, &'a i64, &'a mut LazyBatchColumn)>,
    ) -> Result<()> {
        let row = RowSlice::from_bytes(restored_values)?;
        for (field_type, column_id, column) in column_iter {
            if !field_type.need_restored_data() {
                continue;
            }
            let is_bin_collation = field_type
                .collation()
                .map(|col| col.is_bin_collation())
                .unwrap_or(false);

            assert!(!column.is_empty());
            let mut last_value = column.raw().last().unwrap();
            let decoded_value = last_value.read_datum()?;
            if !last_value.is_empty() {
                return Err(other_err!(
                    "Unexpected extra bytes: {}",
                    log_wrappers::Value(last_value)
                ));
            }
            if decoded_value == Datum::Null {
                continue;
            }
            column.mut_raw().pop();

            let original_data = if is_bin_collation {
                // _bin collation, we need to combine data from key and value to form the
                // original data.

                // Unwrap as checked by `decoded_value.read_datum() == Datum::Null`
                let truncate_str = decoded_value.as_string()?.unwrap();

                let space_num_data = row
                    .get(*column_id)?
                    .ok_or_else(|| other_err!("Unexpected missing column {}", column_id))?;
                let space_num = decode_v2_u64(space_num_data)?;

                // Form the original data.
                truncate_str
                    .iter()
                    .cloned()
                    .chain(std::iter::repeat_n(PADDING_SPACE as _, space_num as _))
                    .collect::<Vec<_>>()
            } else {
                let original_data = row
                    .get(*column_id)?
                    .ok_or_else(|| other_err!("Unexpected missing column {}", column_id))?;
                original_data.to_vec()
            };

            let mut buffer_to_write = column.mut_raw().begin_concat_extend();
            buffer_to_write.write_v2_as_datum(&original_data, field_type)?;
        }

        Ok(())
    }

    // get_index_version is the same as getIndexVersion() in the TiDB repo.
    fn get_index_version(value: &[u8]) -> Result<i64> {
        if value.len() == 3 || value.len() == 4 {
            // For the unique index with null value or non-unique index, the length can be 3
            // or 4 if <= 9.
            return Ok(1);
        }
        if value.len() <= MAX_OLD_ENCODED_VALUE_LEN {
            return Ok(0);
        }
        let tail_len = value[0] as usize;
        if tail_len >= value.len() {
            return Err(other_err!("`tail_len`: {} is corrupted", tail_len));
        }
        if (tail_len == 0 || tail_len == 1) && value[1] == INDEX_VALUE_VERSION_FLAG {
            return Ok(value[2] as i64);
        }

        Ok(0)
    }

    // Process new layout index values in an extensible way,
    // see https://docs.google.com/document/d/1Co5iMiaxitv3okJmLYLJxZYCNChcjzswJMRr-_45Eqg/edit?usp=sharing
    fn process_kv_general(
        &mut self,
        key: &[u8],
        key_payload: &[u8],
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        let (decode_handle, decode_pid, restore_data) =
            self.build_operations(key_payload, value)?;
        if self.fill_extra_common_handle_key {
            match decode_handle {
                DecodeHandleOp::CommonHandle(key) => {
                    columns.mut_extra_common_handle_keys().push(key.to_vec());
                }
                op => {
                    return Err(other_err!(
                        "invalid op {:?} for fill_extra_common_handle_key",
                        op
                    ));
                }
            }
        }

        if self.physical_table_id_column_cnt > 0 {
            match decode_pid {
                DecodePartitionIdOp::Nop => {
                    self.process_physical_table_id_column(key, columns)?;
                }
                // When it's a global index, will return partition id instead of table id.
                DecodePartitionIdOp::Pid(_) => {
                    self.decode_pid_columns(columns, columns.columns_len() - 1, decode_pid)?;
                }
            }
        }

        self.decode_index_columns(key_payload, columns, restore_data)?;
        self.decode_handle_columns(decode_handle, columns, restore_data)?;

        // Deprecated: Keep this for old tidb version during upgrade.
        // If need partition id, append partition id to the last column before physical
        // table id column if exists.
        if self.pid_column_cnt > 0 {
            let pid_col_idx = columns.columns_len() - self.physical_table_id_column_cnt - 1;
            match decode_pid {
                DecodePartitionIdOp::Nop => {
                    // No partition ID found in key or value. Fall back to table ID
                    // from key prefix (local index on a partition has the partition's
                    // table ID encoded in the key).
                    let table_id = table::decode_table_id(key)?;
                    columns[pid_col_idx].mut_decoded().push_int(Some(table_id));
                }
                DecodePartitionIdOp::Pid(_) => {
                    self.decode_pid_columns(columns, pid_col_idx, decode_pid)?;
                }
            }
        }

        Ok(())
    }

    #[inline]
    fn build_operations<'a>(
        &self,
        mut key_payload: &'a [u8],
        index_value: &'a [u8],
    ) -> Result<(DecodeHandleOp<'a>, DecodePartitionIdOp<'a>, RestoreData<'a>)> {
        let tail_len = index_value[0] as usize;
        if tail_len >= index_value.len() {
            return Err(other_err!("`tail_len`: {} is corrupted", tail_len));
        }

        let (remaining, tail) = if self.index_version == 1 {
            // Skip the version segment.
            index_value[3..].split_at(index_value.len() - 3 - tail_len)
        } else {
            index_value[1..].split_at(index_value.len() - 1 - tail_len)
        };

        let (common_handle_bytes, remaining) = Self::split_common_handle(remaining)?;

        // partition ID from key for V1/V2 format
        let mut partition_id_from_key: Option<&'a [u8]> = None;

        let (decode_handle_op, remaining) = {
            if !common_handle_bytes.is_empty() && self.decode_handle_strategy != DecodeCommonHandle
            {
                return Err(other_err!(
                    "Expect to decode index values with common handles in `DecodeCommonHandle` mode."
                ));
            }

            let dispatcher = match self.decode_handle_strategy {
                // V2 key-only partition ID only exists for IntHandle global
                // indexes && !PKIsHandle, and TiDB always sends the handle
                // column for those, so NoDecode never needs to parse
                // partition ID from the key.
                NoDecode => DecodeHandleOp::Nop,
                DecodeIntHandle if tail_len < 8 => {
                    // This is a non-unique index, we should extract the int handle from the key.
                    datum::skip_n(&mut key_payload, self.columns_id_without_handle.len())?;

                    // V1/V2: Check for partition ID in key (after indexed columns, before handle)
                    let (pid_from_key, remaining_key) = Self::split_partition_id(key_payload)?;
                    if !pid_from_key.is_empty() {
                        partition_id_from_key = Some(pid_from_key);
                    }
                    key_payload = remaining_key;

                    DecodeHandleOp::IntFromKey(key_payload)
                }
                DecodeIntHandle => {
                    // This is a unique index, we should extract the int handle from the value.
                    DecodeHandleOp::IntFromValue(tail)
                }
                DecodeCommonHandle if common_handle_bytes.is_empty() => {
                    // This is a non-unique index, we should extract the common handle from the key.
                    datum::skip_n(&mut key_payload, self.columns_id_without_handle.len())?;
                    DecodeHandleOp::CommonHandle(key_payload)
                }
                DecodeCommonHandle => {
                    // This is a unique index, we should extract the common handle from the value.
                    DecodeHandleOp::CommonHandle(common_handle_bytes)
                }
            };

            (dispatcher, remaining)
        };

        // Always try to split partition ID from value (for compatibility where it's
        // in the value, V0 - Only in value, V1 both in key and value)
        let (partition_id_from_value, remaining) = Self::split_partition_id(remaining)?;

        // Prefer partition ID from value if available, for backward compatibility
        let decode_pid_op = if !partition_id_from_value.is_empty() {
            // normal/legacy/V1, partition id in value
            DecodePartitionIdOp::Pid(partition_id_from_value)
        } else if let Some(pid_from_key) = partition_id_from_key {
            // V2: partition ID in key ONLY
            DecodePartitionIdOp::Pid(pid_from_key)
        } else {
            DecodePartitionIdOp::Nop
        };

        let (restore_data, remaining) = Self::split_restore_data(remaining)?;
        let restore_data = {
            if restore_data.is_empty() {
                RestoreData::NotExists
            } else if self.index_version == 1 {
                RestoreData::V5(restore_data)
            } else {
                RestoreData::V4(restore_data)
            }
        };

        if !remaining.is_empty() {
            return Err(other_err!(
                "Unexpected corrupted extra bytes: {}",
                log_wrappers::Value(remaining)
            ));
        }

        Ok((decode_handle_op, decode_pid_op, restore_data))
    }

    #[inline]
    fn decode_index_columns(
        &mut self,
        mut key_payload: &[u8],
        columns: &mut LazyBatchColumnVec,
        restore_data: RestoreData<'_>,
    ) -> Result<()> {
        match restore_data {
            RestoreData::NotExists => {
                Self::extract_columns_from_datum_format(
                    &mut key_payload,
                    &mut columns[..self.columns_id_without_handle.len()],
                )?;
            }

            // If there are some restore data, we need to process them to get the original data.
            RestoreData::V4(rst) => {
                // 4.0 version format, use the restore data directly. The restore data contain
                // all the indexed values.
                self.extract_columns_from_row_format(rst, columns)?;
            }
            RestoreData::V5(rst) => {
                // Extract the data from key, then use the restore data to get the original
                // data.
                Self::extract_columns_from_datum_format(
                    &mut key_payload,
                    &mut columns[..self.columns_id_without_handle.len()],
                )?;
                let limit = self.columns_id_without_handle.len();
                self.restore_original_data(
                    rst,
                    izip!(
                        &self.schema[..limit],
                        &self.columns_id_without_handle,
                        &mut columns[..limit],
                    ),
                )?;
            }
        }

        Ok(())
    }

    #[inline]
    fn decode_handle_columns(
        &mut self,
        decode_handle: DecodeHandleOp<'_>,
        columns: &mut LazyBatchColumnVec,
        restore_data: RestoreData<'_>,
    ) -> Result<()> {
        match decode_handle {
            DecodeHandleOp::Nop => {}
            DecodeHandleOp::IntFromKey(handle) => {
                let handle = self.decode_int_handle_from_key(handle)?;
                columns[self.columns_id_without_handle.len()]
                    .mut_decoded()
                    .push_int(Some(handle));
            }
            DecodeHandleOp::IntFromValue(handle) => {
                let handle = self.decode_int_handle_from_value(handle)?;
                columns[self.columns_id_without_handle.len()]
                    .mut_decoded()
                    .push_int(Some(handle));
            }
            DecodeHandleOp::CommonHandle(mut handle) => {
                let end_index =
                    columns.columns_len() - self.pid_column_cnt - self.physical_table_id_column_cnt;
                Self::extract_columns_from_datum_format(
                    &mut handle,
                    &mut columns[self.columns_id_without_handle.len()..end_index],
                )?;
            }
        }

        let restore_data_bytes = match restore_data {
            RestoreData::V5(value) => value,
            _ => return Ok(()),
        };

        if let DecodeHandleOp::CommonHandle(_) = decode_handle {
            let skip = self.columns_id_without_handle.len();
            let end_index =
                columns.columns_len() - self.pid_column_cnt - self.physical_table_id_column_cnt;
            self.restore_original_data(
                restore_data_bytes,
                izip!(
                    &self.schema[skip..end_index],
                    &self.columns_id_for_common_handle,
                    &mut columns[skip..end_index],
                ),
            )?;
        }

        Ok(())
    }

    #[inline]
    fn process_physical_table_id_column(
        &mut self,
        key: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        let table_id = table::decode_table_id(key)?;
        let col_index = columns.columns_len() - 1;
        columns[col_index].mut_decoded().push_int(Some(table_id));
        Ok(())
    }

    #[inline]
    fn decode_pid_columns(
        &mut self,
        columns: &mut LazyBatchColumnVec,
        idx: usize,
        decode_pid: DecodePartitionIdOp<'_>,
    ) -> Result<()> {
        match decode_pid {
            DecodePartitionIdOp::Nop => {}
            DecodePartitionIdOp::Pid(pid) => {
                let pid = NumberCodec::decode_i64(pid);
                columns[idx].mut_decoded().push_int(Some(pid))
            }
        }
        Ok(())
    }

    #[inline]
    fn split_common_handle(value: &[u8]) -> Result<(&[u8], &[u8])> {
        if value
            .first()
            .is_some_and(|c| *c == table::INDEX_VALUE_COMMON_HANDLE_FLAG)
        {
            let handle_len = (&value[1..]).read_u16().map_err(|_| {
                other_err!(
                    "Fail to read common handle's length from value: {}",
                    log_wrappers::Value::value(value)
                )
            })? as usize;
            let handle_end_offset = 3 + handle_len;
            if handle_end_offset > value.len() {
                return Err(other_err!("`handle_len` is corrupted: {}", handle_len));
            }
            Ok(value[3..].split_at(handle_len))
        } else {
            Ok(value.split_at(0))
        }
    }

    #[inline]
    fn split_partition_id(value: &[u8]) -> Result<(&[u8], &[u8])> {
        if value
            .first()
            .is_some_and(|c| *c == table::INDEX_VALUE_PARTITION_ID_FLAG)
        {
            if value.len() < 9 {
                return Err(other_err!(
                    "Remaining len {} is too short to decode partition ID",
                    value.len()
                ));
            }
            Ok(value[1..].split_at(8))
        } else {
            Ok(value.split_at(0))
        }
    }

    #[inline]
    fn split_restore_data(value: &[u8]) -> Result<(&[u8], &[u8])> {
        Ok(
            if value
                .first()
                .is_some_and(|c| *c == table::INDEX_VALUE_RESTORED_DATA_FLAG)
            {
                (value, &value[value.len()..])
            } else {
                (&value[..0], value)
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use codec::prelude::NumberEncoder;
    use futures::executor::block_on;
    use kvproto::coprocessor::KeyRange;
    use tidb_query_common::{storage::test_fixture::FixtureStorage, util::convert_to_prefix_next};
    use tidb_query_datatype::{
        Collation, FieldTypeAccessor, FieldTypeTp,
        codec::{
            Datum,
            data_type::*,
            datum,
            row::v2::encoder_for_test::{Column, RowEncoder},
            table,
        },
        expr::EvalConfig,
    };
    use tipb::ColumnInfo;

    use super::*;

    #[test]
    fn test_basic() {
        const TABLE_ID: i64 = 3;
        const INDEX_ID: i64 = 42;
        let mut ctx = EvalContext::default();

        // Index schema: (INT, FLOAT)

        // the elements in data are: [int index, float index, handle id].
        let data = [
            [Datum::I64(-5), Datum::F64(0.3), Datum::I64(10)],
            [Datum::I64(5), Datum::F64(5.1), Datum::I64(5)],
            [Datum::I64(5), Datum::F64(10.5), Datum::I64(2)],
        ];

        // The column info for each column in `data`. Used to build the executor.
        let columns_info = [
            {
                let mut ci = ColumnInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::Double);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci.set_pk_handle(true);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci.set_column_id(table::EXTRA_PHYSICAL_TABLE_ID_COL_ID);
                ci
            },
        ];

        // The schema of these columns. Used to check executor output.
        let schema = [
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Double.into(),
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::LongLong.into(),
        ];

        // Case 1. Normal index.

        // For a normal index, the PK handle is stored in the key and nothing
        // interesting is stored in the value. So let's build corresponding KV
        // data.

        let store = {
            let kv: Vec<_> = data
                .iter()
                .map(|datums| {
                    let index_data = datum::encode_key(&mut ctx, datums).unwrap();
                    let key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &index_data);
                    let value = vec![];
                    (key, value)
                })
                .collect();
            FixtureStorage::from(kv)
        };

        {
            // Case 1.1. Normal index, without PK, scan total index in reverse order.

            let key_ranges = vec![{
                let mut range = KeyRange::default();
                let start_data = datum::encode_key(&mut ctx, &[Datum::Min]).unwrap();
                let start_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &start_data);
                range.set_start(start_key);
                let end_data = datum::encode_key(&mut ctx, &[Datum::Max]).unwrap();
                let end_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &end_data);
                range.set_end(end_key);
                range
            }];

            let mut executor = BatchIndexScanExecutor::<_, ApiV1>::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![columns_info[0].clone(), columns_info[1].clone()],
                key_ranges,
                0,
                true,
                false,
                false,
                false,
            )
            .unwrap();

            let mut result = block_on(executor.next_batch(10));
            assert!(result.is_drained.as_ref().unwrap().stop());
            assert_eq!(result.physical_columns.columns_len(), 2);
            assert_eq!(result.physical_columns.rows_len(), 3);
            assert!(result.physical_columns[0].is_raw());
            result.physical_columns[0]
                .ensure_all_decoded_for_test(&mut ctx, &schema[0])
                .unwrap();
            assert_eq!(
                result.physical_columns[0].decoded().to_int_vec(),
                &[Some(5), Some(5), Some(-5)]
            );
            assert!(result.physical_columns[1].is_raw());
            result.physical_columns[1]
                .ensure_all_decoded_for_test(&mut ctx, &schema[1])
                .unwrap();
            assert_eq!(
                result.physical_columns[1].decoded().to_real_vec(),
                &[
                    Real::new(10.5).ok(),
                    Real::new(5.1).ok(),
                    Real::new(0.3).ok()
                ]
            );
        }

        {
            // Case 1.1.b Normal index, without PK, scan total index in reverse order.
            // With Physical Table ID Column

            let key_ranges = vec![{
                let mut range = KeyRange::default();
                let start_data = datum::encode_key(&mut ctx, &[Datum::Min]).unwrap();
                let start_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &start_data);
                range.set_start(start_key);
                let end_data = datum::encode_key(&mut ctx, &[Datum::Max]).unwrap();
                let end_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &end_data);
                range.set_end(end_key);
                range
            }];

            let mut executor = BatchIndexScanExecutor::<_, ApiV1>::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![
                    columns_info[0].clone(),
                    columns_info[1].clone(),
                    columns_info[3].clone(),
                ],
                key_ranges,
                0,
                true,
                false,
                false,
                false,
            )
            .unwrap();

            let mut result = block_on(executor.next_batch(10));
            assert!(result.is_drained.as_ref().unwrap().stop());
            assert_eq!(result.physical_columns.columns_len(), 3);
            assert_eq!(result.physical_columns.rows_len(), 3);
            assert!(result.physical_columns[0].is_raw());
            result.physical_columns[0]
                .ensure_all_decoded_for_test(&mut ctx, &schema[0])
                .unwrap();
            assert_eq!(
                result.physical_columns[0].decoded().to_int_vec(),
                &[Some(5), Some(5), Some(-5)]
            );
            assert!(result.physical_columns[1].is_raw());
            result.physical_columns[1]
                .ensure_all_decoded_for_test(&mut ctx, &schema[1])
                .unwrap();
            assert_eq!(
                result.physical_columns[1].decoded().to_real_vec(),
                &[
                    Real::new(10.5).ok(),
                    Real::new(5.1).ok(),
                    Real::new(0.3).ok()
                ]
            );
            result.physical_columns[2]
                .ensure_all_decoded_for_test(&mut ctx, &schema[3])
                .unwrap();
            assert_eq!(
                result.physical_columns[2].decoded().to_int_vec(),
                &[Some(TABLE_ID), Some(TABLE_ID), Some(TABLE_ID)]
            );
        }

        {
            // Case 1.1.c Normal index, without PK, scan total index in reverse order.
            // With Columns in WRONG order!!

            let key_ranges = vec![{
                let mut range = KeyRange::default();
                let start_data = datum::encode_key(&mut ctx, &[Datum::Min]).unwrap();
                let start_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &start_data);
                range.set_start(start_key);
                let end_data = datum::encode_key(&mut ctx, &[Datum::Max]).unwrap();
                let end_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &end_data);
                range.set_end(end_key);
                range
            }];

            let mut executor = BatchIndexScanExecutor::<_, ApiV1>::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![columns_info[1].clone(), columns_info[0].clone()],
                key_ranges,
                0,
                true,
                false,
                false,
                false,
            )
            .unwrap();

            let mut result = block_on(executor.next_batch(10));
            assert!(result.is_drained.as_ref().unwrap().stop());
            assert_eq!(result.physical_columns.columns_len(), 2);
            assert_eq!(result.physical_columns.rows_len(), 3);
            assert!(result.physical_columns[0].is_raw());
            result.physical_columns[0]
                .ensure_all_decoded_for_test(&mut ctx, &schema[1])
                .unwrap_err();
            assert!(result.physical_columns[1].is_raw());
            result.physical_columns[1]
                .ensure_all_decoded_for_test(&mut ctx, &schema[0])
                .unwrap_err();
        }

        {
            // Case 1.1.d Normal index, without PK, scan total index in reverse order.
            // With Physical Table ID Column in WRONG order!!

            let key_ranges = vec![{
                let mut range = KeyRange::default();
                let start_data = datum::encode_key(&mut ctx, &[Datum::Min]).unwrap();
                let start_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &start_data);
                range.set_start(start_key);
                let end_data = datum::encode_key(&mut ctx, &[Datum::Max]).unwrap();
                let end_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &end_data);
                range.set_end(end_key);
                range
            }];

            let mut executor = BatchIndexScanExecutor::<_, ApiV1>::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![
                    columns_info[0].clone(),
                    columns_info[3].clone(),
                    columns_info[1].clone(),
                ],
                key_ranges,
                0,
                true,
                false,
                false,
                false,
            )
            .unwrap();

            let mut result = block_on(executor.next_batch(10));
            assert!(result.is_drained.as_ref().unwrap().stop());
            assert_eq!(result.physical_columns.columns_len(), 3);
            assert_eq!(result.physical_columns.rows_len(), 3);
            assert!(result.physical_columns[0].is_raw());
            result.physical_columns[0]
                .ensure_all_decoded_for_test(&mut ctx, &schema[0])
                .unwrap();
            assert_eq!(
                result.physical_columns[0].decoded().to_int_vec(),
                &[Some(5), Some(5), Some(-5)]
            );
            assert!(result.physical_columns[1].is_raw());
            result.physical_columns[1]
                .ensure_all_decoded_for_test(&mut ctx, &schema[3])
                .unwrap_err();
            assert!(result.physical_columns[2].is_raw());
            result.physical_columns[2]
                .ensure_all_decoded_for_test(&mut ctx, &schema[1])
                .unwrap_err();
        }

        {
            // Case 1.2. Normal index, with PK, scan index prefix.

            let key_ranges = vec![{
                let mut range = KeyRange::default();
                let start_data = datum::encode_key(&mut ctx, &[Datum::I64(2)]).unwrap();
                let start_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &start_data);
                range.set_start(start_key);
                let end_data = datum::encode_key(&mut ctx, &[Datum::I64(6)]).unwrap();
                let end_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &end_data);
                range.set_end(end_key);
                range
            }];

            let mut executor = BatchIndexScanExecutor::<_, ApiV1>::new(
                store,
                Arc::new(EvalConfig::default()),
                vec![
                    columns_info[0].clone(),
                    columns_info[1].clone(),
                    columns_info[2].clone(),
                ],
                key_ranges,
                0,
                false,
                false,
                false,
                false,
            )
            .unwrap();

            let mut result = block_on(executor.next_batch(10));
            assert!(result.is_drained.as_ref().unwrap().stop());
            assert_eq!(result.physical_columns.columns_len(), 3);
            assert_eq!(result.physical_columns.rows_len(), 2);
            assert!(result.physical_columns[0].is_raw());
            result.physical_columns[0]
                .ensure_all_decoded_for_test(&mut ctx, &schema[0])
                .unwrap();
            assert_eq!(
                result.physical_columns[0].decoded().to_int_vec(),
                &[Some(5), Some(5)]
            );
            assert!(result.physical_columns[1].is_raw());
            result.physical_columns[1]
                .ensure_all_decoded_for_test(&mut ctx, &schema[1])
                .unwrap();
            assert_eq!(
                result.physical_columns[1].decoded().to_real_vec(),
                &[Real::new(5.1).ok(), Real::new(10.5).ok()]
            );
            assert!(result.physical_columns[2].is_decoded());
            assert_eq!(
                result.physical_columns[2].decoded().to_int_vec(),
                &[Some(5), Some(2)]
            );
        }

        // Case 2. Unique index.

        // For a unique index, the PK handle is stored in the value.

        let store = {
            let kv: Vec<_> = data
                .iter()
                .map(|datums| {
                    let index_data = datum::encode_key(&mut ctx, &datums[0..2]).unwrap();
                    let key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &index_data);
                    // PK handle in the value
                    let mut value = vec![];
                    value
                        .write_u64(datums[2].as_int().unwrap().unwrap() as u64)
                        .unwrap();
                    (key, value)
                })
                .collect();
            FixtureStorage::from(kv)
        };

        {
            // Case 2.1. Unique index, prefix range scan.

            let key_ranges = vec![{
                let mut range = KeyRange::default();
                let start_data = datum::encode_key(&mut ctx, &[Datum::I64(5)]).unwrap();
                let start_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &start_data);
                range.set_start(start_key);
                range.set_end(range.get_start().to_vec());
                convert_to_prefix_next(range.mut_end());
                range
            }];

            let mut executor = BatchIndexScanExecutor::<_, ApiV1>::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![
                    columns_info[0].clone(),
                    columns_info[1].clone(),
                    columns_info[2].clone(),
                ],
                key_ranges,
                0,
                false,
                false,
                false,
                false,
            )
            .unwrap();

            let mut result = block_on(executor.next_batch(10));
            assert!(result.is_drained.as_ref().unwrap().stop());
            assert_eq!(result.physical_columns.columns_len(), 3);
            assert_eq!(result.physical_columns.rows_len(), 2);
            assert!(result.physical_columns[0].is_raw());
            result.physical_columns[0]
                .ensure_all_decoded_for_test(&mut ctx, &schema[0])
                .unwrap();
            assert_eq!(
                result.physical_columns[0].decoded().to_int_vec(),
                &[Some(5), Some(5)]
            );
            assert!(result.physical_columns[1].is_raw());
            result.physical_columns[1]
                .ensure_all_decoded_for_test(&mut ctx, &schema[1])
                .unwrap();
            assert_eq!(
                result.physical_columns[1].decoded().to_real_vec(),
                &[Real::new(5.1).ok(), Real::new(10.5).ok()]
            );
            assert!(result.physical_columns[2].is_decoded());
            assert_eq!(
                result.physical_columns[2].decoded().to_int_vec(),
                &[Some(5), Some(2)]
            );
        }

        {
            // Case 2.2. Unique index, point scan.

            let key_ranges = vec![{
                let mut range = KeyRange::default();
                let start_data =
                    datum::encode_key(&mut ctx, &[Datum::I64(5), Datum::F64(5.1)]).unwrap();
                let start_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &start_data);
                range.set_start(start_key);
                range.set_end(range.get_start().to_vec());
                convert_to_prefix_next(range.mut_end());
                range
            }];

            let mut executor = BatchIndexScanExecutor::<_, ApiV1>::new(
                store,
                Arc::new(EvalConfig::default()),
                vec![
                    columns_info[0].clone(),
                    columns_info[1].clone(),
                    columns_info[2].clone(),
                ],
                key_ranges,
                0,
                false,
                true,
                false,
                false,
            )
            .unwrap();

            let mut result = block_on(executor.next_batch(10));
            assert!(result.is_drained.as_ref().unwrap().stop());
            assert_eq!(result.physical_columns.columns_len(), 3);
            assert_eq!(result.physical_columns.rows_len(), 1);
            assert!(result.physical_columns[0].is_raw());
            result.physical_columns[0]
                .ensure_all_decoded_for_test(&mut ctx, &schema[0])
                .unwrap();
            assert_eq!(
                result.physical_columns[0].decoded().to_int_vec(),
                &[Some(5)]
            );
            assert!(result.physical_columns[1].is_raw());
            result.physical_columns[1]
                .ensure_all_decoded_for_test(&mut ctx, &schema[1])
                .unwrap();
            assert_eq!(
                result.physical_columns[1].decoded().to_real_vec(),
                &[Real::new(5.1).ok()]
            );
            assert!(result.physical_columns[2].is_decoded());
            assert_eq!(
                result.physical_columns[2].decoded().to_int_vec(),
                &[Some(5)]
            );
        }
    }

    #[test]
    fn test_unique_common_handle_index() {
        const TABLE_ID: i64 = 3;
        const INDEX_ID: i64 = 42;
        let columns_info = vec![
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(1);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(2);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(3);
                ci.as_mut_accessor().set_tp(FieldTypeTp::Double);
                ci
            },
        ];

        // The schema of these columns. Used to check executor output.
        let schema: Vec<FieldType> = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Double.into(),
        ];

        let columns = vec![Column::new(1, 2), Column::new(2, 3), Column::new(3, 4.0)];
        let datums = [Datum::U64(2), Datum::U64(3), Datum::F64(4.0)];

        let mut value_prefix = vec![];
        let mut restore_data = vec![];
        let common_handle =
            datum::encode_value(&mut EvalContext::default(), &[Datum::F64(4.0)]).unwrap();

        restore_data
            .write_row(&mut EvalContext::default(), columns)
            .unwrap();

        // Tail length
        value_prefix.push(0);
        // Common handle flag
        value_prefix.push(127);
        // Common handle length
        value_prefix.write_u16(common_handle.len() as u16).unwrap();

        // Common handle
        value_prefix.extend(common_handle.clone());

        let index_data = datum::encode_key(&mut EvalContext::default(), &datums[0..2]).unwrap();
        let key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &index_data);

        let key_ranges = vec![{
            let mut range = KeyRange::default();
            let start_key = key.clone();
            range.set_start(start_key);
            range.set_end(range.get_start().to_vec());
            convert_to_prefix_next(range.mut_end());
            range
        }];

        // 1. New collation unique common handle.
        let mut value = value_prefix.clone();
        value.extend(restore_data);
        let store = FixtureStorage::from(vec![(key.clone(), value)]);
        let mut executor = BatchIndexScanExecutor::<_, ApiV1>::new(
            store,
            Arc::new(EvalConfig::default()),
            columns_info.clone(),
            key_ranges.clone(),
            1,
            false,
            true,
            false,
            true,
        )
        .unwrap();

        let mut result = block_on(executor.next_batch(10));
        assert!(result.is_drained.as_ref().unwrap().stop());
        assert_eq!(result.physical_columns.columns_len(), 3);
        assert_eq!(result.physical_columns.rows_len(), 1);
        assert!(result.physical_columns[0].is_raw());
        result.physical_columns[0]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[0])
            .unwrap();
        assert_eq!(
            result.physical_columns[0].decoded().to_int_vec(),
            &[Some(2)]
        );
        assert!(result.physical_columns[1].is_raw());
        result.physical_columns[1]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[1])
            .unwrap();
        assert_eq!(
            result.physical_columns[1].decoded().to_int_vec(),
            &[Some(3)]
        );
        assert!(result.physical_columns[2].is_raw());
        result.physical_columns[2]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[2])
            .unwrap();
        assert_eq!(
            result.physical_columns[2].decoded().to_real_vec(),
            &[Real::new(4.0).ok()]
        );
        let extra_common_handle_keys = result.physical_columns.take_extra_common_handle_keys();
        assert_eq!(extra_common_handle_keys, Some(vec![common_handle]));

        let value = value_prefix;
        let store = FixtureStorage::from(vec![(key, value)]);
        let mut executor = BatchIndexScanExecutor::<_, ApiV1>::new(
            store,
            Arc::new(EvalConfig::default()),
            columns_info,
            key_ranges,
            1,
            false,
            true,
            false,
            false,
        )
        .unwrap();

        let mut result = block_on(executor.next_batch(10));
        assert!(result.is_drained.as_ref().unwrap().stop());
        assert_eq!(result.physical_columns.columns_len(), 3);
        assert_eq!(result.physical_columns.rows_len(), 1);
        assert!(result.physical_columns[0].is_raw());
        result.physical_columns[0]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[0])
            .unwrap();
        assert_eq!(
            result.physical_columns[0].decoded().to_int_vec(),
            &[Some(2)]
        );
        assert!(result.physical_columns[1].is_raw());
        result.physical_columns[1]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[1])
            .unwrap();
        assert_eq!(
            result.physical_columns[1].decoded().to_int_vec(),
            &[Some(3)]
        );
        assert!(result.physical_columns[2].is_raw());
        result.physical_columns[2]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[2])
            .unwrap();
        assert_eq!(
            result.physical_columns[2].decoded().to_real_vec(),
            &[Real::new(4.0).ok()]
        );
    }

    #[test]
    fn test_old_collation_non_unique_common_handle_index() {
        const TABLE_ID: i64 = 3;
        const INDEX_ID: i64 = 42;
        let columns_info = vec![
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(1);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(2);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(3);
                ci.as_mut_accessor().set_tp(FieldTypeTp::Double);
                ci
            },
        ];

        // The schema of these columns. Used to check executor output.
        let schema: Vec<FieldType> = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Double.into(),
        ];

        let datums = [Datum::U64(2), Datum::U64(3), Datum::F64(4.0)];

        let common_handle = datum::encode_key(
            &mut EvalContext::default(),
            &[Datum::U64(3), Datum::F64(4.0)],
        )
        .unwrap();

        let index_data = datum::encode_key(&mut EvalContext::default(), &datums[0..1]).unwrap();
        let mut key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &index_data);
        key.extend(common_handle.clone());

        let key_ranges = vec![{
            let mut range = KeyRange::default();
            let start_key = key.clone();
            range.set_start(start_key);
            range.set_end(range.get_start().to_vec());
            convert_to_prefix_next(range.mut_end());
            range
        }];

        let store = FixtureStorage::from(vec![(key, vec![])]);
        let mut executor = BatchIndexScanExecutor::<_, ApiV1>::new(
            store,
            Arc::new(EvalConfig::default()),
            columns_info,
            key_ranges,
            2,
            false,
            false,
            false,
            true,
        )
        .unwrap();

        let mut result = block_on(executor.next_batch(10));
        assert!(result.is_drained.as_ref().unwrap().stop());
        assert_eq!(result.physical_columns.columns_len(), 3);
        assert_eq!(result.physical_columns.rows_len(), 1);
        assert!(result.physical_columns[0].is_raw());
        result.physical_columns[0]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[0])
            .unwrap();
        assert_eq!(
            result.physical_columns[0].decoded().to_int_vec(),
            &[Some(2)]
        );
        assert!(result.physical_columns[1].is_raw());
        result.physical_columns[1]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[1])
            .unwrap();
        assert_eq!(
            result.physical_columns[1].decoded().to_int_vec(),
            &[Some(3)]
        );
        assert!(result.physical_columns[2].is_raw());
        result.physical_columns[2]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[2])
            .unwrap();
        assert_eq!(
            result.physical_columns[2].decoded().to_real_vec(),
            &[Real::new(4.0).ok()]
        );
        let extra_common_handle_keys = result.physical_columns.take_extra_common_handle_keys();
        assert_eq!(extra_common_handle_keys, Some(vec![common_handle]));
    }

    #[test]
    fn test_new_collation_unique_int_handle_index() {
        const TABLE_ID: i64 = 3;
        const INDEX_ID: i64 = 42;
        let columns_info = vec![
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(1);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(2);
                ci.as_mut_accessor().set_tp(FieldTypeTp::Double);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(3);
                ci.set_pk_handle(true);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
        ];

        // The schema of these columns. Used to check executor output.
        let schema: Vec<FieldType> = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Double.into(),
            FieldTypeTp::LongLong.into(),
        ];

        let columns = vec![Column::new(1, 2), Column::new(2, 3.0), Column::new(3, 4)];
        let datums = [Datum::U64(2), Datum::F64(3.0), Datum::U64(4)];
        let index_data = datum::encode_key(&mut EvalContext::default(), &datums[0..2]).unwrap();
        let key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &index_data);

        let mut restore_data = vec![];
        restore_data
            .write_row(&mut EvalContext::default(), columns)
            .unwrap();
        let mut value = vec![8];
        value.extend(restore_data);
        value
            .write_u64(datums[2].as_int().unwrap().unwrap() as u64)
            .unwrap();

        let key_ranges = vec![{
            let mut range = KeyRange::default();
            let start_key = key.clone();
            range.set_start(start_key);
            range.set_end(range.get_start().to_vec());
            convert_to_prefix_next(range.mut_end());
            range
        }];

        let store = FixtureStorage::from(vec![(key, value)]);
        let mut executor = BatchIndexScanExecutor::<_, ApiV1>::new(
            store,
            Arc::new(EvalConfig::default()),
            columns_info,
            key_ranges,
            0,
            false,
            true,
            false,
            false,
        )
        .unwrap();

        let mut result = block_on(executor.next_batch(10));
        assert!(result.is_drained.as_ref().unwrap().stop());
        assert_eq!(result.physical_columns.columns_len(), 3);
        assert_eq!(result.physical_columns.rows_len(), 1);
        assert!(result.physical_columns[0].is_raw());
        result.physical_columns[0]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[0])
            .unwrap();
        assert_eq!(
            result.physical_columns[0].decoded().to_int_vec(),
            &[Some(2)]
        );
        assert!(result.physical_columns[1].is_raw());
        result.physical_columns[1]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[1])
            .unwrap();
        assert_eq!(
            result.physical_columns[1].decoded().to_real_vec(),
            &[Real::new(3.0).ok()]
        );
        assert!(result.physical_columns[2].is_decoded());
        assert_eq!(
            result.physical_columns[2].decoded().to_int_vec(),
            &[Some(4)]
        );
    }

    #[test]
    fn test_new_collation_non_unique_int_handle_index() {
        const TABLE_ID: i64 = 3;
        const INDEX_ID: i64 = 42;
        let columns_info = vec![
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(1);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(2);
                ci.as_mut_accessor().set_tp(FieldTypeTp::Double);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(3);
                ci.set_pk_handle(true);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
        ];

        // The schema of these columns. Used to check executor output.
        let schema: Vec<FieldType> = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Double.into(),
            FieldTypeTp::LongLong.into(),
        ];

        let columns = vec![Column::new(1, 2), Column::new(2, 3.0), Column::new(3, 4)];
        let datums = vec![Datum::U64(2), Datum::F64(3.0), Datum::U64(4)];
        let index_data = datum::encode_key(&mut EvalContext::default(), &datums).unwrap();
        let key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &index_data);

        let mut restore_data = vec![];
        restore_data
            .write_row(&mut EvalContext::default(), columns)
            .unwrap();
        let mut value = vec![0];
        value.extend(restore_data);

        let key_ranges = vec![{
            let mut range = KeyRange::default();
            let start_key = key.clone();
            range.set_start(start_key);
            range.set_end(range.get_start().to_vec());
            convert_to_prefix_next(range.mut_end());
            range
        }];

        let store = FixtureStorage::from(vec![(key, value)]);
        let mut executor = BatchIndexScanExecutor::<_, ApiV1>::new(
            store,
            Arc::new(EvalConfig::default()),
            columns_info,
            key_ranges,
            0,
            false,
            true,
            false,
            false,
        )
        .unwrap();

        let mut result = block_on(executor.next_batch(10));
        assert!(result.is_drained.as_ref().unwrap().stop());
        assert_eq!(result.physical_columns.columns_len(), 3);
        assert_eq!(result.physical_columns.rows_len(), 1);
        assert!(result.physical_columns[0].is_raw());
        result.physical_columns[0]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[0])
            .unwrap();
        assert_eq!(
            result.physical_columns[0].decoded().to_int_vec(),
            &[Some(2)]
        );
        assert!(result.physical_columns[1].is_raw());
        result.physical_columns[1]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[1])
            .unwrap();
        assert_eq!(
            result.physical_columns[1].decoded().to_real_vec(),
            &[Real::new(3.0).ok()]
        );
        assert!(result.physical_columns[2].is_decoded());
        assert_eq!(
            result.physical_columns[2].decoded().to_int_vec(),
            &[Some(4)]
        );
    }

    #[test]
    fn test_new_collation_non_unique_common_handle_index() {
        const TABLE_ID: i64 = 3;
        const INDEX_ID: i64 = 42;
        let columns_info = vec![
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(1);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(2);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(3);
                ci.as_mut_accessor().set_tp(FieldTypeTp::Double);
                ci
            },
        ];

        // The schema of these columns. Used to check executor output.
        let schema: Vec<FieldType> = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Double.into(),
        ];

        let columns = vec![Column::new(1, 2), Column::new(2, 3), Column::new(3, 4.0)];
        let datums = vec![Datum::U64(2), Datum::U64(3), Datum::F64(4.0)];
        let index_data = datum::encode_key(&mut EvalContext::default(), &datums).unwrap();
        let key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &index_data);

        let mut restore_data = vec![];
        restore_data
            .write_row(&mut EvalContext::default(), columns)
            .unwrap();
        let mut value = vec![0];
        value.extend(restore_data);

        let key_ranges = vec![{
            let mut range = KeyRange::default();
            let start_key = key.clone();
            range.set_start(start_key);
            range.set_end(range.get_start().to_vec());
            convert_to_prefix_next(range.mut_end());
            range
        }];

        let store = FixtureStorage::from(vec![(key, value)]);
        let mut executor = BatchIndexScanExecutor::<_, ApiV1>::new(
            store,
            Arc::new(EvalConfig::default()),
            columns_info,
            key_ranges,
            1,
            false,
            true,
            false,
            true,
        )
        .unwrap();

        let mut result = block_on(executor.next_batch(10));
        assert!(result.is_drained.as_ref().unwrap().stop());
        assert_eq!(result.physical_columns.columns_len(), 3);
        assert_eq!(result.physical_columns.rows_len(), 1);
        assert!(result.physical_columns[0].is_raw());
        result.physical_columns[0]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[0])
            .unwrap();
        assert_eq!(
            result.physical_columns[0].decoded().to_int_vec(),
            &[Some(2)]
        );
        assert!(result.physical_columns[1].is_raw());
        result.physical_columns[1]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[1])
            .unwrap();
        assert_eq!(
            result.physical_columns[1].decoded().to_int_vec(),
            &[Some(3)]
        );
        assert!(result.physical_columns[2].is_raw());
        result.physical_columns[2]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[2])
            .unwrap();
        assert_eq!(
            result.physical_columns[2].decoded().to_real_vec(),
            &[Real::new(4.0).ok()]
        );
        let extra_common_handle_keys = result.physical_columns.take_extra_common_handle_keys();
        assert_eq!(
            extra_common_handle_keys,
            Some(vec![
                datum::encode_key(&mut EvalContext::default(), &datums[2..]).unwrap(),
            ])
        );
    }

    #[test]
    fn test_unique_common_handle_global_index() {
        const TABLE_ID: i64 = 3;
        const INDEX_ID: i64 = 42;
        let columns_info = vec![
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(1);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(2);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(3);
                ci.as_mut_accessor().set_tp(FieldTypeTp::Double);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(table::EXTRA_PARTITION_ID_COL_ID);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
        ];

        // The schema of these columns. Used to check executor output.
        let schema: Vec<FieldType> = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Double.into(),
            FieldTypeTp::LongLong.into(),
        ];

        let columns = vec![Column::new(1, 2), Column::new(2, 3), Column::new(3, 4.0)];
        let datums = [Datum::U64(2), Datum::U64(3), Datum::F64(4.0)];

        let mut value_prefix = vec![];
        let mut restore_data = vec![];
        let common_handle =
            datum::encode_value(&mut EvalContext::default(), &[Datum::F64(4.0)]).unwrap();

        restore_data
            .write_row(&mut EvalContext::default(), columns)
            .unwrap();

        // Tail length
        value_prefix.push(0);
        // Common handle flag
        value_prefix.push(127);
        // Common handle length
        value_prefix.write_u16(common_handle.len() as u16).unwrap();

        // Common handle
        value_prefix.extend(common_handle.clone());

        // Partition ID
        let pid = 7;
        let mut pid_bytes = vec![0u8; 8];
        NumberCodec::encode_i64(&mut pid_bytes, pid);
        value_prefix.push(table::INDEX_VALUE_PARTITION_ID_FLAG);
        value_prefix.extend(&pid_bytes[..]);

        let index_data = datum::encode_key(&mut EvalContext::default(), &datums[0..2]).unwrap();
        let key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &index_data);

        let key_ranges = vec![{
            let mut range = KeyRange::default();
            let start_key = key.clone();
            range.set_start(start_key);
            range.set_end(range.get_start().to_vec());
            convert_to_prefix_next(range.mut_end());
            range
        }];

        // New collation unique common global handle.
        let mut value = value_prefix;
        value.extend(restore_data);
        let store = FixtureStorage::from(vec![(key, value)]);
        let mut executor = BatchIndexScanExecutor::<_, ApiV1>::new(
            store,
            Arc::new(EvalConfig::default()),
            columns_info,
            key_ranges,
            1,
            false,
            true,
            false,
            true,
        )
        .unwrap();

        let mut result = block_on(executor.next_batch(10));
        assert!(result.is_drained.as_ref().unwrap().stop());
        assert_eq!(result.physical_columns.columns_len(), 4);
        assert_eq!(result.physical_columns.rows_len(), 1);
        assert!(result.physical_columns[0].is_raw());
        result.physical_columns[0]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[0])
            .unwrap();
        assert_eq!(
            result.physical_columns[0].decoded().to_int_vec(),
            &[Some(2)]
        );
        assert!(result.physical_columns[1].is_raw());
        result.physical_columns[1]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[1])
            .unwrap();
        assert_eq!(
            result.physical_columns[1].decoded().to_int_vec(),
            &[Some(3)]
        );
        assert!(result.physical_columns[2].is_raw());
        result.physical_columns[2]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[2])
            .unwrap();
        assert_eq!(
            result.physical_columns[2].decoded().to_real_vec(),
            &[Real::new(4.0).ok()]
        );
        assert!(result.physical_columns[3].is_decoded());
        assert_eq!(
            result.physical_columns[3].decoded().to_int_vec(),
            &[Some(pid)]
        );
        let extra_common_handle_keys = result.physical_columns.take_extra_common_handle_keys();
        assert_eq!(extra_common_handle_keys, Some(vec![common_handle]));
    }

    #[test]
    fn test_int_handle_char_index() {
        use tidb_query_datatype::builder::FieldTypeBuilder;

        // Schema: create table t(a int, b char(10) collate utf8mb4_bin, c char(10)
        // collate utf8mb4_unicode_ci, key i_a(a), key i_b(b), key i_c(c), key i_abc(a,
        // b, c), unique key i_ua(a),  unique key i_ub(b), unique key i_uc(c),
        // unique key i_uabc(a,b,c)); insert into t values (1, "a ", "A ");

        // i_a and i_ua
        let mut idx_exe = IndexScanExecutorImpl {
            context: Default::default(),
            schema: vec![FieldTypeTp::Long.into(), FieldTypeTp::LongLong.into()],
            columns_id_without_handle: vec![1],
            columns_id_for_common_handle: vec![],
            decode_handle_strategy: DecodeHandleStrategy::DecodeIntHandle,
            pid_column_cnt: 0,
            physical_table_id_column_cnt: 0,
            index_version: -1,
            fill_extra_common_handle_key: false,
        };
        let mut columns = idx_exe.build_column_vec(10);
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x31, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x1, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x3,
                    0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
                ],
                &[0x30],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(columns[1].decoded().to_int_vec().last().unwrap(), &Some(1));
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x35, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x5, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
                ],
                &[0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(columns[1].decoded().to_int_vec().last().unwrap(), &Some(1));

        // i_b and i_ub
        idx_exe = IndexScanExecutorImpl {
            context: Default::default(),
            schema: vec![
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::String)
                    .collation(Collation::Utf8Mb4Bin)
                    .into(),
                FieldTypeTp::LongLong.into(),
            ],
            columns_id_without_handle: vec![2],
            columns_id_for_common_handle: vec![],
            decode_handle_strategy: DecodeHandleStrategy::DecodeIntHandle,
            pid_column_cnt: 0,
            physical_table_id_column_cnt: 0,
            index_version: -1,
            fill_extra_common_handle_key: false,
        };
        columns = idx_exe.build_column_vec(10);
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x31, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x2, 0x1, 0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8,
                    0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
                ],
                &[0x30],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a".as_bytes().to_vec())
        );
        assert_eq!(columns[1].decoded().to_int_vec().last().unwrap(), &Some(1));
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x35, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x6, 0x1, 0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8,
                ],
                &[0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a".as_bytes().to_vec())
        );
        assert_eq!(columns[1].decoded().to_int_vec().last().unwrap(), &Some(1));

        // i_c and i_uc
        idx_exe = IndexScanExecutorImpl {
            context: Default::default(),
            schema: vec![
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::String)
                    .collation(Collation::Utf8Mb4UnicodeCi)
                    .into(),
                FieldTypeTp::LongLong.into(),
            ],
            columns_id_without_handle: vec![3],
            columns_id_for_common_handle: vec![],
            decode_handle_strategy: DecodeHandleStrategy::DecodeIntHandle,
            pid_column_cnt: 0,
            physical_table_id_column_cnt: 0,
            index_version: -1,
            fill_extra_common_handle_key: false,
        };
        columns = idx_exe.build_column_vec(10);
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x31, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x3, 0x1, 0xe, 0x33, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf9,
                    0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
                ],
                &[0x0, 0x80, 0x0, 0x1, 0x0, 0x0, 0x0, 0x3, 0x1, 0x0, 0x41],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A".as_bytes().to_vec())
        );
        assert_eq!(columns[1].decoded().to_int_vec().last().unwrap(), &Some(1));
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x35, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x7, 0x1, 0xe, 0x33, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf9,
                ],
                &[
                    0x8, 0x80, 0x0, 0x1, 0x0, 0x0, 0x0, 0x3, 0x1, 0x0, 0x41, 0x0, 0x0, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x1,
                ],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A".as_bytes().to_vec())
        );
        assert_eq!(columns[1].decoded().to_int_vec().last().unwrap(), &Some(1));

        // i_abc and i_uabc
        idx_exe = IndexScanExecutorImpl {
            context: Default::default(),
            schema: vec![
                FieldTypeTp::Long.into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::String)
                    .collation(Collation::Utf8Mb4Bin)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::String)
                    .collation(Collation::Utf8Mb4UnicodeCi)
                    .into(),
                FieldTypeTp::LongLong.into(),
            ],
            columns_id_without_handle: vec![1, 2, 3],
            columns_id_for_common_handle: vec![],
            decode_handle_strategy: DecodeHandleStrategy::DecodeIntHandle,
            pid_column_cnt: 0,
            physical_table_id_column_cnt: 0,
            index_version: -1,
            fill_extra_common_handle_key: false,
        };
        columns = idx_exe.build_column_vec(10);
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x35, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x4, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x1,
                    0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8, 0x1, 0xe, 0x33, 0x0, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0xf9, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
                ],
                &[
                    0x0, 0x80, 0x0, 0x3, 0x0, 0x0, 0x0, 0x1, 0x2, 0x3, 0x1, 0x0, 0x2, 0x0, 0x3,
                    0x0, 0x1, 0x61, 0x41,
                ],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(
            columns[1].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a".as_bytes().to_vec())
        );
        assert_eq!(
            columns[2].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A".as_bytes().to_vec())
        );
        assert_eq!(columns[3].decoded().to_int_vec().last().unwrap(), &Some(1));
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x35, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x8, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x1,
                    0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8, 0x1, 0xe, 0x33, 0x0, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0xf9,
                ],
                &[
                    0x8, 0x80, 0x0, 0x3, 0x0, 0x0, 0x0, 0x1, 0x2, 0x3, 0x1, 0x0, 0x2, 0x0, 0x3,
                    0x0, 0x1, 0x61, 0x41, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
                ],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(
            columns[1].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a".as_bytes().to_vec())
        );
        assert_eq!(
            columns[2].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A".as_bytes().to_vec())
        );
        assert_eq!(columns[3].decoded().to_int_vec().last().unwrap(), &Some(1));
    }

    #[test]
    fn test_int_handle_varchar_index() {
        use tidb_query_datatype::builder::FieldTypeBuilder;

        // Schema: create table t(a int, b varchar(10) collate utf8mb4_bin, c
        // varchar(10) collate utf8mb4_unicode_ci, key i_a(a), key i_b(b), key i_c(c),
        // key i_abc(a, b, c), unique key i_ua(a),  unique key i_ub(b), unique
        // key i_uc(c), unique key i_uabc(a,b,c)); insert into t values (1, "a
        // ", "A ");

        // i_a and i_ua
        let mut idx_exe = IndexScanExecutorImpl {
            context: Default::default(),
            schema: vec![FieldTypeTp::Long.into(), FieldTypeTp::LongLong.into()],
            columns_id_without_handle: vec![1],
            columns_id_for_common_handle: vec![],
            decode_handle_strategy: DecodeHandleStrategy::DecodeIntHandle,
            pid_column_cnt: 0,
            physical_table_id_column_cnt: 0,
            index_version: -1,
            fill_extra_common_handle_key: false,
        };
        let mut columns = idx_exe.build_column_vec(10);
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x31, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x1, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x3,
                    0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
                ],
                &[0x30],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(columns[1].decoded().to_int_vec().last().unwrap(), &Some(1));
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x35, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x5, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
                ],
                &[0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(columns[1].decoded().to_int_vec().last().unwrap(), &Some(1));

        // i_b and i_ub
        idx_exe = IndexScanExecutorImpl {
            context: Default::default(),
            schema: vec![
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::VarChar)
                    .collation(Collation::Utf8Mb4Bin)
                    .into(),
                FieldTypeTp::LongLong.into(),
            ],
            columns_id_without_handle: vec![2],
            columns_id_for_common_handle: vec![],
            decode_handle_strategy: DecodeHandleStrategy::DecodeIntHandle,
            pid_column_cnt: 0,
            physical_table_id_column_cnt: 0,
            index_version: -1,
            fill_extra_common_handle_key: false,
        };
        columns = idx_exe.build_column_vec(10);
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3d, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x2, 0x1, 0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8,
                    0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
                ],
                &[
                    0x0, 0x80, 0x0, 0x1, 0x0, 0x0, 0x0, 0x2, 0x2, 0x0, 0x61, 0x20,
                ],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a ".as_bytes().to_vec())
        );
        assert_eq!(columns[1].decoded().to_int_vec().last().unwrap(), &Some(1));
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3d, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x6, 0x1, 0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8,
                ],
                &[
                    0x8, 0x80, 0x0, 0x1, 0x0, 0x0, 0x0, 0x2, 0x2, 0x0, 0x61, 0x20, 0x0, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x1,
                ],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a ".as_bytes().to_vec())
        );
        assert_eq!(columns[1].decoded().to_int_vec().last().unwrap(), &Some(1));

        // i_c and i_uc
        idx_exe = IndexScanExecutorImpl {
            context: Default::default(),
            schema: vec![
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::VarChar)
                    .collation(Collation::Utf8Mb4UnicodeCi)
                    .into(),
                FieldTypeTp::LongLong.into(),
            ],
            columns_id_without_handle: vec![3],
            columns_id_for_common_handle: vec![],
            decode_handle_strategy: DecodeHandleStrategy::DecodeIntHandle,
            pid_column_cnt: 0,
            physical_table_id_column_cnt: 0,
            index_version: -1,
            fill_extra_common_handle_key: false,
        };
        columns = idx_exe.build_column_vec(10);
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3d, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x3, 0x1, 0xe, 0x33, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf9,
                    0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
                ],
                &[
                    0x0, 0x80, 0x0, 0x1, 0x0, 0x0, 0x0, 0x3, 0x2, 0x0, 0x41, 0x20,
                ],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A ".as_bytes().to_vec())
        );
        assert_eq!(columns[1].decoded().to_int_vec().last().unwrap(), &Some(1));
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3d, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x7, 0x1, 0xe, 0x33, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf9,
                ],
                &[
                    0x8, 0x80, 0x0, 0x1, 0x0, 0x0, 0x0, 0x3, 0x2, 0x0, 0x41, 0x20, 0x0, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x1,
                ],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A ".as_bytes().to_vec())
        );
        assert_eq!(columns[1].decoded().to_int_vec().last().unwrap(), &Some(1));

        // i_abc and i_uabc
        idx_exe = IndexScanExecutorImpl {
            context: Default::default(),
            schema: vec![
                FieldTypeTp::Long.into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::VarChar)
                    .collation(Collation::Utf8Mb4Bin)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::VarChar)
                    .collation(Collation::Utf8Mb4UnicodeCi)
                    .into(),
                FieldTypeTp::LongLong.into(),
            ],
            columns_id_without_handle: vec![1, 2, 3],
            columns_id_for_common_handle: vec![],
            decode_handle_strategy: DecodeHandleStrategy::DecodeIntHandle,
            pid_column_cnt: 0,
            physical_table_id_column_cnt: 0,
            index_version: -1,
            fill_extra_common_handle_key: false,
        };
        columns = idx_exe.build_column_vec(10);
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3d, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x4, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x1,
                    0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8, 0x1, 0xe, 0x33, 0x0, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0xf9, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
                ],
                &[
                    0x0, 0x80, 0x0, 0x3, 0x0, 0x0, 0x0, 0x1, 0x2, 0x3, 0x1, 0x0, 0x3, 0x0, 0x5,
                    0x0, 0x1, 0x61, 0x20, 0x41, 0x20,
                ],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(
            columns[1].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a ".as_bytes().to_vec())
        );
        assert_eq!(
            columns[2].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A ".as_bytes().to_vec())
        );
        assert_eq!(columns[3].decoded().to_int_vec().last().unwrap(), &Some(1));
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3d, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x8, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x1,
                    0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8, 0x1, 0xe, 0x33, 0x0, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0xf9,
                ],
                &[
                    0x8, 0x80, 0x0, 0x3, 0x0, 0x0, 0x0, 0x1, 0x2, 0x3, 0x1, 0x0, 0x3, 0x0, 0x5,
                    0x0, 0x1, 0x61, 0x20, 0x41, 0x20, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
                ],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(
            columns[1].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a ".as_bytes().to_vec())
        );
        assert_eq!(
            columns[2].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A ".as_bytes().to_vec())
        );
        assert_eq!(columns[3].decoded().to_int_vec().last().unwrap(), &Some(1));
    }

    #[test]
    fn test_common_handle_index() {
        use tidb_query_datatype::builder::FieldTypeBuilder;

        // create table t(a int, b char(10) collate utf8mb4_bin, c char(10) collate
        // utf8mb4_unicode_ci, d varchar(10) collate utf8mb4_bin, e varchar(10) collate
        // utf8mb4_general_ci , primary key(a, b, c, d, e),  key i_a(a), key
        // i_b(b), key i_c(c), key i_d(d), key i_e(e), key i_abcde(a, b, c, d, e),
        // unique key i_ua(a), unique key i_ub(b), unique key i_uc( c), unique
        // key i_ud(d), unique key i_ue(e), unique key i_uabcde(a,b,c, d, e));
        //
        // CREATE TABLE `t` (
        //   `a` int(11) NOT NULL,
        //   `b` char(10) NOT NULL,
        //   `c` char(10) COLLATE utf8mb4_unicode_ci NOT NULL,
        //   `d` varchar(10) NOT NULL,
        //   `e` varchar(10) COLLATE utf8mb4_general_ci NOT NULL,
        //   PRIMARY KEY (`a`,`b`,`c`,`d`,`e`),
        //   KEY `i_a` (`a`),
        //   KEY `i_b` (`b`),
        //   KEY `i_c` (`c`),
        //   KEY `i_d` (`d`),
        //   KEY `i_e` (`e`),
        //   KEY `i_abcde` (`a`,`b`,`c`,`d`,`e`),
        //   UNIQUE KEY `i_ua` (`a`),
        //   UNIQUE KEY `i_ub` (`b`),
        //   UNIQUE KEY `i_uc` (`c`),
        //   UNIQUE KEY `i_ud` (`d`),
        //   UNIQUE KEY `i_ue` (`e`),
        //   UNIQUE KEY `i_uabcde` (`a`,`b`,`c`,`d`,`e`)
        // ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
        //
        // insert into t values (1, "a ", "A ", "a ", "A ");

        // i_a and i_ua
        let mut idx_exe = IndexScanExecutorImpl {
            context: Default::default(),
            schema: vec![
                FieldTypeTp::Long.into(),
                FieldTypeTp::Long.into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::String)
                    .collation(Collation::Utf8Mb4Bin)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::String)
                    .collation(Collation::Utf8Mb4UnicodeCi)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::VarChar)
                    .collation(Collation::Utf8Mb4Bin)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::VarChar)
                    .collation(Collation::Utf8Mb4UnicodeCi)
                    .into(),
            ],
            columns_id_without_handle: vec![1],
            columns_id_for_common_handle: vec![1, 2, 3, 4, 5],
            decode_handle_strategy: DecodeHandleStrategy::DecodeCommonHandle,
            pid_column_cnt: 0,
            physical_table_id_column_cnt: 0,
            index_version: -1,
            fill_extra_common_handle_key: false,
        };
        let mut columns = idx_exe.build_column_vec(10);
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x31, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x2, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x3,
                    0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x1, 0x61, 0x0, 0x0, 0x0, 0x0, 0x0,
                    0x0, 0x0, 0xf8, 0x1, 0xe, 0x33, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf9, 0x1, 0x61,
                    0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8, 0x1, 0x0, 0x41, 0x0, 0x0, 0x0, 0x0,
                    0x0, 0x0, 0xf9,
                ],
                &[
                    0x0, 0x7d, 0x1, 0x80, 0x0, 0x3, 0x0, 0x0, 0x0, 0x3, 0x4, 0x5, 0x1, 0x0, 0x2,
                    0x0, 0x4, 0x0, 0x41, 0x1, 0x41, 0x20,
                ],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(
            columns[1].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(
            columns[2].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a".as_bytes().to_vec())
        );
        assert_eq!(
            columns[3].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A".as_bytes().to_vec())
        );
        assert_eq!(
            columns[4].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a ".as_bytes().to_vec())
        );
        assert_eq!(
            columns[5].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A ".as_bytes().to_vec())
        );

        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x31, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x8, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
                ],
                &[
                    0x0, 0x7d, 0x1, 0x7f, 0x0, 0x31, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
                    0x1, 0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8, 0x1, 0xe, 0x33, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0xf9, 0x1, 0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8,
                    0x1, 0x0, 0x41, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf9, 0x80, 0x0, 0x3, 0x0, 0x0,
                    0x0, 0x3, 0x4, 0x5, 0x1, 0x0, 0x2, 0x0, 0x4, 0x0, 0x41, 0x1, 0x41, 0x20,
                ],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(
            columns[1].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(
            columns[2].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a".as_bytes().to_vec())
        );
        assert_eq!(
            columns[3].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A".as_bytes().to_vec())
        );
        assert_eq!(
            columns[4].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a ".as_bytes().to_vec())
        );
        assert_eq!(
            columns[5].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A ".as_bytes().to_vec())
        );

        // i_b and i_ub
        idx_exe = IndexScanExecutorImpl {
            context: Default::default(),
            schema: vec![
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::String)
                    .collation(Collation::Utf8Mb4Bin)
                    .into(),
                FieldTypeTp::Long.into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::String)
                    .collation(Collation::Utf8Mb4Bin)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::String)
                    .collation(Collation::Utf8Mb4UnicodeCi)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::VarChar)
                    .collation(Collation::Utf8Mb4Bin)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::VarChar)
                    .collation(Collation::Utf8Mb4UnicodeCi)
                    .into(),
            ],
            columns_id_without_handle: vec![2],
            columns_id_for_common_handle: vec![1, 2, 3, 4, 5],
            decode_handle_strategy: DecodeHandleStrategy::DecodeCommonHandle,
            pid_column_cnt: 0,
            physical_table_id_column_cnt: 0,
            index_version: -1,
            fill_extra_common_handle_key: false,
        };
        columns = idx_exe.build_column_vec(10);
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x31, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x3, 0x1, 0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8,
                    0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x1, 0x61, 0x0, 0x0, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0xf8, 0x1, 0xe, 0x33, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf9, 0x1,
                    0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8, 0x1, 0x0, 0x41, 0x0, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0xf9,
                ],
                &[
                    0x0, 0x7d, 0x1, 0x80, 0x0, 0x3, 0x0, 0x0, 0x0, 0x3, 0x4, 0x5, 0x1, 0x0, 0x2,
                    0x0, 0x4, 0x0, 0x41, 0x1, 0x41, 0x20,
                ],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a".as_bytes().to_vec())
        );
        assert_eq!(
            columns[1].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(
            columns[2].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a".as_bytes().to_vec())
        );
        assert_eq!(
            columns[3].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A".as_bytes().to_vec())
        );
        assert_eq!(
            columns[4].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a ".as_bytes().to_vec())
        );
        assert_eq!(
            columns[5].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A ".as_bytes().to_vec())
        );
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x40, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x9, 0x1, 0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8,
                ],
                &[
                    0x0, 0x7d, 0x1, 0x7f, 0x0, 0x31, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
                    0x1, 0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8, 0x1, 0xe, 0x33, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0xf9, 0x1, 0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8,
                    0x1, 0x0, 0x41, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf9, 0x80, 0x0, 0x3, 0x0, 0x0,
                    0x0, 0x3, 0x4, 0x5, 0x1, 0x0, 0x2, 0x0, 0x4, 0x0, 0x41, 0x1, 0x41, 0x20,
                ],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a".as_bytes().to_vec())
        );
        assert_eq!(
            columns[1].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(
            columns[2].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a".as_bytes().to_vec())
        );
        assert_eq!(
            columns[3].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A".as_bytes().to_vec())
        );
        assert_eq!(
            columns[4].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a ".as_bytes().to_vec())
        );
        assert_eq!(
            columns[5].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A ".as_bytes().to_vec())
        );

        // i_c and i_uc
        idx_exe = IndexScanExecutorImpl {
            context: Default::default(),
            schema: vec![
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::String)
                    .collation(Collation::Utf8Mb4UnicodeCi)
                    .into(),
                FieldTypeTp::Long.into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::String)
                    .collation(Collation::Utf8Mb4Bin)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::String)
                    .collation(Collation::Utf8Mb4UnicodeCi)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::VarChar)
                    .collation(Collation::Utf8Mb4Bin)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::VarChar)
                    .collation(Collation::Utf8Mb4UnicodeCi)
                    .into(),
            ],
            columns_id_without_handle: vec![3],
            columns_id_for_common_handle: vec![1, 2, 3, 4, 5],
            decode_handle_strategy: DecodeHandleStrategy::DecodeCommonHandle,
            pid_column_cnt: 0,
            physical_table_id_column_cnt: 0,
            index_version: -1,
            fill_extra_common_handle_key: false,
        };
        columns = idx_exe.build_column_vec(10);
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x40, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x4, 0x1, 0xe, 0x33, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf9,
                    0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x1, 0x61, 0x0, 0x0, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0xf8, 0x1, 0xe, 0x33, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf9, 0x1,
                    0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8, 0x1, 0x0, 0x41, 0x0, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0xf9,
                ],
                &[
                    0x0, 0x7d, 0x1, 0x80, 0x0, 0x3, 0x0, 0x0, 0x0, 0x3, 0x4, 0x5, 0x1, 0x0, 0x2,
                    0x0, 0x4, 0x0, 0x41, 0x1, 0x41, 0x20,
                ],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A".as_bytes().to_vec())
        );
        assert_eq!(
            columns[1].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(
            columns[2].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a".as_bytes().to_vec())
        );
        assert_eq!(
            columns[3].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A".as_bytes().to_vec())
        );
        assert_eq!(
            columns[4].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a ".as_bytes().to_vec())
        );
        assert_eq!(
            columns[5].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A ".as_bytes().to_vec())
        );
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x40, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0xa, 0x1, 0xe, 0x33, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf9,
                ],
                &[
                    0x0, 0x7d, 0x1, 0x7f, 0x0, 0x31, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
                    0x1, 0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8, 0x1, 0xe, 0x33, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0xf9, 0x1, 0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8,
                    0x1, 0x0, 0x41, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf9, 0x80, 0x0, 0x3, 0x0, 0x0,
                    0x0, 0x3, 0x4, 0x5, 0x1, 0x0, 0x2, 0x0, 0x4, 0x0, 0x41, 0x1, 0x41, 0x20,
                ],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A".as_bytes().to_vec())
        );
        assert_eq!(
            columns[1].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(
            columns[2].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a".as_bytes().to_vec())
        );
        assert_eq!(
            columns[3].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A".as_bytes().to_vec())
        );
        assert_eq!(
            columns[4].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a ".as_bytes().to_vec())
        );
        assert_eq!(
            columns[5].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A ".as_bytes().to_vec())
        );

        // i_d and i_ud
        idx_exe = IndexScanExecutorImpl {
            context: Default::default(),
            schema: vec![
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::VarChar)
                    .collation(Collation::Utf8Mb4Bin)
                    .into(),
                FieldTypeTp::Long.into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::String)
                    .collation(Collation::Utf8Mb4Bin)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::String)
                    .collation(Collation::Utf8Mb4UnicodeCi)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::VarChar)
                    .collation(Collation::Utf8Mb4Bin)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::VarChar)
                    .collation(Collation::Utf8Mb4UnicodeCi)
                    .into(),
            ],
            columns_id_without_handle: vec![4],
            columns_id_for_common_handle: vec![1, 2, 3, 4, 5],
            decode_handle_strategy: DecodeHandleStrategy::DecodeCommonHandle,
            pid_column_cnt: 0,
            physical_table_id_column_cnt: 0,
            index_version: -1,
            fill_extra_common_handle_key: false,
        };
        columns = idx_exe.build_column_vec(10);
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x40, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x5, 0x1, 0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8,
                    0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x1, 0x61, 0x0, 0x0, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0xf8, 0x1, 0xe, 0x33, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf9, 0x1,
                    0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8, 0x1, 0x0, 0x41, 0x0, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0xf9,
                ],
                &[
                    0x0, 0x7d, 0x1, 0x80, 0x0, 0x3, 0x0, 0x0, 0x0, 0x3, 0x4, 0x5, 0x1, 0x0, 0x2,
                    0x0, 0x4, 0x0, 0x41, 0x1, 0x41, 0x20,
                ],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a ".as_bytes().to_vec())
        );
        assert_eq!(
            columns[1].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(
            columns[2].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a".as_bytes().to_vec())
        );
        assert_eq!(
            columns[3].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A".as_bytes().to_vec())
        );
        assert_eq!(
            columns[4].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a ".as_bytes().to_vec())
        );
        assert_eq!(
            columns[5].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A ".as_bytes().to_vec())
        );
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x40, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0xb, 0x1, 0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8,
                ],
                &[
                    0x0, 0x7d, 0x1, 0x7f, 0x0, 0x31, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
                    0x1, 0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8, 0x1, 0xe, 0x33, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0xf9, 0x1, 0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8,
                    0x1, 0x0, 0x41, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf9, 0x80, 0x0, 0x3, 0x0, 0x0,
                    0x0, 0x3, 0x4, 0x5, 0x1, 0x0, 0x2, 0x0, 0x4, 0x0, 0x41, 0x1, 0x41, 0x20,
                ],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a ".as_bytes().to_vec())
        );
        assert_eq!(
            columns[1].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(
            columns[2].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a".as_bytes().to_vec())
        );
        assert_eq!(
            columns[3].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A".as_bytes().to_vec())
        );
        assert_eq!(
            columns[4].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a ".as_bytes().to_vec())
        );
        assert_eq!(
            columns[5].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A ".as_bytes().to_vec())
        );

        // i_e and i_ue
        idx_exe = IndexScanExecutorImpl {
            context: Default::default(),
            schema: vec![
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::VarChar)
                    .collation(Collation::Utf8Mb4UnicodeCi)
                    .into(),
                FieldTypeTp::Long.into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::String)
                    .collation(Collation::Utf8Mb4Bin)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::String)
                    .collation(Collation::Utf8Mb4UnicodeCi)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::VarChar)
                    .collation(Collation::Utf8Mb4Bin)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::VarChar)
                    .collation(Collation::Utf8Mb4UnicodeCi)
                    .into(),
            ],
            columns_id_without_handle: vec![5],
            columns_id_for_common_handle: vec![1, 2, 3, 4, 5],
            decode_handle_strategy: DecodeHandleStrategy::DecodeCommonHandle,
            pid_column_cnt: 0,
            physical_table_id_column_cnt: 0,
            index_version: -1,
            fill_extra_common_handle_key: false,
        };
        columns = idx_exe.build_column_vec(10);
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x40, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x6, 0x1, 0x0, 0x41, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf9,
                    0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x1, 0x61, 0x0, 0x0, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0xf8, 0x1, 0xe, 0x33, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf9, 0x1,
                    0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8, 0x1, 0x0, 0x41, 0x0, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0xf9,
                ],
                &[
                    0x0, 0x7d, 0x1, 0x80, 0x0, 0x3, 0x0, 0x0, 0x0, 0x3, 0x4, 0x5, 0x1, 0x0, 0x2,
                    0x0, 0x4, 0x0, 0x41, 0x1, 0x41, 0x20,
                ],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A ".as_bytes().to_vec())
        );
        assert_eq!(
            columns[1].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(
            columns[2].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a".as_bytes().to_vec())
        );
        assert_eq!(
            columns[3].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A".as_bytes().to_vec())
        );
        assert_eq!(
            columns[4].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a ".as_bytes().to_vec())
        );
        assert_eq!(
            columns[5].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A ".as_bytes().to_vec())
        );
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x40, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0xc, 0x1, 0x0, 0x41, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf9,
                ],
                &[
                    0x0, 0x7d, 0x1, 0x7f, 0x0, 0x31, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
                    0x1, 0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8, 0x1, 0xe, 0x33, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0xf9, 0x1, 0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8,
                    0x1, 0x0, 0x41, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf9, 0x80, 0x0, 0x3, 0x0, 0x0,
                    0x0, 0x3, 0x4, 0x5, 0x1, 0x0, 0x2, 0x0, 0x4, 0x0, 0x41, 0x1, 0x41, 0x20,
                ],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A ".as_bytes().to_vec())
        );
        assert_eq!(
            columns[1].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(
            columns[2].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a".as_bytes().to_vec())
        );
        assert_eq!(
            columns[3].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A".as_bytes().to_vec())
        );
        assert_eq!(
            columns[4].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a ".as_bytes().to_vec())
        );
        assert_eq!(
            columns[5].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A ".as_bytes().to_vec())
        );

        // i_abcde and i_uabcde
        idx_exe = IndexScanExecutorImpl {
            context: Default::default(),
            schema: vec![
                FieldTypeTp::Long.into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::String)
                    .collation(Collation::Utf8Mb4Bin)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::String)
                    .collation(Collation::Utf8Mb4UnicodeCi)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::VarChar)
                    .collation(Collation::Utf8Mb4Bin)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::VarChar)
                    .collation(Collation::Utf8Mb4UnicodeCi)
                    .into(),
                FieldTypeTp::Long.into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::String)
                    .collation(Collation::Utf8Mb4Bin)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::String)
                    .collation(Collation::Utf8Mb4UnicodeCi)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::VarChar)
                    .collation(Collation::Utf8Mb4Bin)
                    .into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::VarChar)
                    .collation(Collation::Utf8Mb4UnicodeCi)
                    .into(),
            ],
            columns_id_without_handle: vec![1, 2, 3, 4, 5],
            columns_id_for_common_handle: vec![1, 2, 3, 4, 5],
            decode_handle_strategy: DecodeHandleStrategy::DecodeCommonHandle,
            pid_column_cnt: 0,
            physical_table_id_column_cnt: 0,
            index_version: -1,
            fill_extra_common_handle_key: false,
        };
        columns = idx_exe.build_column_vec(10);
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x40, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x7, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x1,
                    0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8, 0x1, 0xe, 0x33, 0x0, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0xf9, 0x1, 0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8, 0x1,
                    0x0, 0x41, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf9, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0,
                    0x0, 0x0, 0x1, 0x1, 0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8, 0x1, 0xe,
                    0x33, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf9, 0x1, 0x61, 0x0, 0x0, 0x0, 0x0, 0x0,
                    0x0, 0x0, 0xf8, 0x1, 0x0, 0x41, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf9,
                ],
                &[
                    0x0, 0x7d, 0x1, 0x80, 0x0, 0x3, 0x0, 0x0, 0x0, 0x3, 0x4, 0x5, 0x1, 0x0, 0x2,
                    0x0, 0x4, 0x0, 0x41, 0x1, 0x41, 0x20,
                ],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(
            columns[1].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a".as_bytes().to_vec())
        );
        assert_eq!(
            columns[2].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A".as_bytes().to_vec())
        );
        assert_eq!(
            columns[3].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a ".as_bytes().to_vec())
        );
        assert_eq!(
            columns[4].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A ".as_bytes().to_vec())
        );
        assert_eq!(
            columns[5].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(
            columns[6].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a".as_bytes().to_vec())
        );
        assert_eq!(
            columns[7].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A".as_bytes().to_vec())
        );
        assert_eq!(
            columns[8].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a ".as_bytes().to_vec())
        );
        assert_eq!(
            columns[9].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A ".as_bytes().to_vec())
        );
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x40, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0xd, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x1,
                    0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8, 0x1, 0xe, 0x33, 0x0, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0xf9, 0x1, 0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8, 0x1,
                    0x0, 0x41, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf9,
                ],
                &[
                    0x0, 0x7d, 0x1, 0x7f, 0x0, 0x31, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
                    0x1, 0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8, 0x1, 0xe, 0x33, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0xf9, 0x1, 0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8,
                    0x1, 0x0, 0x41, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf9, 0x80, 0x0, 0x3, 0x0, 0x0,
                    0x0, 0x3, 0x4, 0x5, 0x1, 0x0, 0x2, 0x0, 0x4, 0x0, 0x41, 0x1, 0x41, 0x20,
                ],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(
            columns[1].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a".as_bytes().to_vec())
        );
        assert_eq!(
            columns[2].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A".as_bytes().to_vec())
        );
        assert_eq!(
            columns[3].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a ".as_bytes().to_vec())
        );
        assert_eq!(
            columns[4].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A ".as_bytes().to_vec())
        );
        assert_eq!(
            columns[5].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(
            columns[6].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a".as_bytes().to_vec())
        );
        assert_eq!(
            columns[7].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A".as_bytes().to_vec())
        );
        assert_eq!(
            columns[8].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("a ".as_bytes().to_vec())
        );
        assert_eq!(
            columns[9].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("A ".as_bytes().to_vec())
        );
    }

    #[test]
    fn test_common_handle_with_physical_table_id() {
        // CREATE TABLE `tcommonhash` (
        //     `a` int(11) NOT NULL,
        //     `b` int(11) DEFAULT NULL,
        //     `c` int(11) NOT NULL,
        //     `d` int(11) NOT NUL,
        //     PRIMARY KEY (`a`,`c`,`d`) /*T![clustered_index] CLUSTERED */,
        //     KEY `idx_bc` (`b`,`c`)
        //  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
        // insert into tcommonhash values (1, 2, 3, 1);

        // idx_bc
        let mut idx_exe = IndexScanExecutorImpl {
            context: Default::default(),
            schema: vec![
                FieldTypeTp::Long.into(),
                FieldTypeTp::Long.into(),
                FieldTypeTp::Long.into(),
                FieldTypeTp::Long.into(),
                FieldTypeTp::Long.into(),
                // EXTRA_PHYSICAL_TABLE_ID_COL
                FieldTypeTp::Long.into(),
            ],
            columns_id_without_handle: vec![2, 3],
            columns_id_for_common_handle: vec![1, 3, 4],
            decode_handle_strategy: DecodeHandleStrategy::DecodeCommonHandle,
            pid_column_cnt: 0,
            physical_table_id_column_cnt: 1,
            index_version: -1,
            fill_extra_common_handle_key: false,
        };
        let mut columns = idx_exe.build_column_vec(10);
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5c, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x2, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x3,
                    0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0,
                    0x0, 0x1, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3, 0x3, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x1,
                ],
                &[0x0, 0x7d, 0x1],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(2)
        );
        assert_eq!(
            columns[1].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(3)
        );
        assert_eq!(
            columns[2].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(
            columns[3].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(3)
        );
        assert_eq!(
            columns[4].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(
            // physical table id
            columns[5].mut_decoded().to_int_vec()[0].unwrap(),
            92
        );
    }

    #[test]
    fn test_global_index_with_physical_table_id() {
        // CREATE TABLE `t` (
        //     `a` int(11) NOT NULL,
        //     `b` int(11) NOT NUL,
        //     UNIQUE KEY uidx_a(`a`),
        //  ) partition by hash(b) partitions 5;
        // insert into t values (1, 2);

        // uidx_a
        let mut idx_exe = IndexScanExecutorImpl {
            context: Default::default(),
            schema: vec![
                // column `a`
                FieldTypeTp::Long.into(),
                // _tidb_rowid
                FieldTypeTp::Long.into(),
                // EXTRA_PHYSICAL_TABLE_ID_COL
                FieldTypeTp::Long.into(),
            ],
            columns_id_without_handle: vec![1],
            columns_id_for_common_handle: vec![],
            decode_handle_strategy: DecodeHandleStrategy::DecodeIntHandle,
            pid_column_cnt: 0,
            physical_table_id_column_cnt: 1,
            index_version: -1,
            fill_extra_common_handle_key: false,
        };
        let mut columns = idx_exe.build_column_vec(10);
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x7b, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x1, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
                ],
                &[
                    0x8, 0x7e, // INDEX_VALUE_PARTITION_ID_FLAG
                    0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5c, // partition id
                    0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // _tidb_rowid
                ],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            // column a
            columns[0].raw().last().unwrap().read_datum().unwrap(),
            Datum::I64(1)
        );
        assert_eq!(
            // _tidb_rowid
            columns[1].mut_decoded().to_int_vec()[0].unwrap(),
            1
        );
        assert_eq!(
            // partition id
            columns[2].mut_decoded().to_int_vec()[0].unwrap(),
            92
        );
    }

    #[test]
    fn test_common_handle_index_latin1_bin() {
        use tidb_query_datatype::builder::FieldTypeBuilder;

        // create table t(c1 varchar(200) CHARACTER SET latin1 COLLATE latin1_bin, c2
        // int, primary key(c1) clustered, key kk(c2)); idx_exec for index
        // kk(c2), its columns will be <c2, c1>
        let mut idx_exe = IndexScanExecutorImpl {
            context: Default::default(),
            schema: vec![
                FieldTypeTp::Long.into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::String)
                    .collation(Collation::Latin1Bin)
                    .into(),
            ],
            columns_id_without_handle: vec![2],
            columns_id_for_common_handle: vec![1],
            decode_handle_strategy: DecodeHandleStrategy::DecodeCommonHandle,
            pid_column_cnt: 0,
            physical_table_id_column_cnt: 0,
            index_version: -1,
            fill_extra_common_handle_key: false,
        };
        let mut columns = idx_exe.build_column_vec(1);
        idx_exe
            .process_kv_pair(
                &[
                    0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x37, 0x5f, 0x69, 0x80, 0x0, 0x0,
                    0x0, 0x0, 0x0, 0x0, 0x2, 0x3, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x1,
                    0x6a, 0x6f, 0x76, 0x69, 0x61, 0x6c, 0x65, 0x64, 0xff, 0x69, 0x73, 0x6f, 0x6e,
                    0x0, 0x0, 0x0, 0x0, 0xfb,
                ],
                &[
                    0x0, 0x7d, 0x1, 0x80, 0x0, 0x1, 0x0, 0x0, 0x0, 0x1, 0x1, 0x0, 0x0,
                ],
                &mut columns,
                None,
            )
            .unwrap();
        assert_eq!(
            columns[1].raw().last().unwrap().read_datum().unwrap(),
            Datum::Bytes("jovialedison".as_bytes().to_vec())
        );
    }

    #[test]
    fn test_index_version() {
        assert_eq!(
            IndexScanExecutorImpl::get_index_version(&[0x0, 0x7d, 0x1]).unwrap(),
            1
        );
        assert_eq!(
            IndexScanExecutorImpl::get_index_version(&[0x1, 0x7d, 0x1, 0x31]).unwrap(),
            1
        );
        assert_eq!(
            IndexScanExecutorImpl::get_index_version(&[
                0x0, 0x7d, 0x1, 0x80, 0x0, 0x2, 0x0, 0x0, 0x0, 0x1, 0x2, 0x1, 0x0, 0x2, 0x0, 0x61,
                0x31
            ])
            .unwrap(),
            1
        );
        assert_eq!(
            IndexScanExecutorImpl::get_index_version(&[
                0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4, 0x31
            ])
            .unwrap(),
            0
        );
        assert_eq!(
            IndexScanExecutorImpl::get_index_version(&[0x30]).unwrap(),
            0
        );
    }

    #[test]
    fn test_decode_int_handle_and_partition_from_key() {
        let idx_exe = IndexScanExecutorImpl {
            context: Default::default(),
            schema: vec![],
            columns_id_without_handle: vec![],
            columns_id_for_common_handle: vec![],
            decode_handle_strategy: DecodeHandleStrategy::DecodeIntHandle,
            pid_column_cnt: 0,
            physical_table_id_column_cnt: 0,
            index_version: -1,
            fill_extra_common_handle_key: false,
        };

        // Helper to build key with optional partition prefix
        fn build_key(partition_id: Option<i64>, handle_flag: u8, handle_value: i64) -> Vec<u8> {
            let mut key = Vec::new();
            if let Some(pid) = partition_id {
                key.push(table::INDEX_VALUE_PARTITION_ID_FLAG);
                key.write_i64(pid).unwrap();
            }
            key.push(handle_flag);
            if handle_flag == datum::UINT_FLAG {
                key.write_u64(handle_value as u64).unwrap();
            } else {
                key.write_i64(handle_value).unwrap();
            }
            key
        }

        // Test all combinations of partition IDs and handle values
        let partition_ids: Vec<Option<i64>> = vec![None, Some(-100), Some(0), Some(100)];
        let handle_values: Vec<i64> = vec![-999, 0, 999];

        for &partition_id in &partition_ids {
            for &handle_value in &handle_values {
                // Determine which flags to test based on handle value
                let flags: Vec<u8> = if handle_value >= 0 {
                    vec![datum::INT_FLAG, datum::UINT_FLAG]
                } else {
                    vec![datum::INT_FLAG] // negative values only work with INT_FLAG
                };

                for &handle_flag in &flags {
                    let key = build_key(partition_id, handle_flag, handle_value);
                    let result = idx_exe.decode_int_handle_and_partition_from_key(&key);

                    assert_eq!(
                        result.unwrap(),
                        (handle_value, partition_id),
                        "Failed for partition_id={:?}, handle_flag={}, handle_value={}",
                        partition_id,
                        if handle_flag == datum::INT_FLAG {
                            "INT"
                        } else {
                            "UINT"
                        },
                        handle_value
                    );
                }
            }
        }

        // Error cases: malformed keys
        let error_cases: Vec<(Vec<u8>, &str)> = vec![
            (vec![], "empty key"),
            (
                vec![0x99, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
                "invalid handle flag",
            ),
            (
                vec![table::INDEX_VALUE_PARTITION_ID_FLAG, 0x01, 0x02],
                "partition flag with insufficient partition data",
            ),
            (
                vec![datum::INT_FLAG, 0x01],
                "INT_FLAG with insufficient data",
            ),
            (
                vec![datum::UINT_FLAG, 0x01],
                "UINT_FLAG with insufficient data",
            ),
        ];

        for (key, desc) in error_cases {
            assert!(
                idx_exe
                    .decode_int_handle_and_partition_from_key(&key)
                    .is_err(),
                "Expected error for: {}",
                desc
            );
        }

        // Error cases with partition prefix but malformed handle
        let malformed_handles: Vec<(Vec<u8>, &str)> = vec![
            (vec![], "no handle data"),
            (
                vec![0xFF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
                "invalid handle flag",
            ),
            (vec![datum::INT_FLAG, 0x01], "INT_FLAG insufficient data"),
            (vec![datum::UINT_FLAG, 0x01], "UINT_FLAG insufficient data"),
        ];

        for partition_id in [-100i64, 0, 100] {
            for (extra_bytes, handle_desc) in &malformed_handles {
                let mut key = vec![table::INDEX_VALUE_PARTITION_ID_FLAG];
                key.write_i64(partition_id).unwrap();
                key.extend(extra_bytes);
                assert!(
                    idx_exe
                        .decode_int_handle_and_partition_from_key(&key)
                        .is_err(),
                    "Expected error for partition_id={}, handle={}",
                    partition_id,
                    handle_desc
                );
            }
        }
    }

    #[test]
    fn test_global_index_formats() {
        // Tests V1 and V2 global index formats with partition ID handling.
        //
        // V1: partition ID in both KEY and VALUE
        // V2: partition ID ONLY in KEY (reduces storage overhead)
        //
        // Key format: [indexed_cols][PARTITION_ID_FLAG][partition_id][handle]
        // V1 value: [tail_len][PARTITION_ID_FLAG][partition_id]
        // V2 value: [tail_len] (no partition ID)

        const TABLE_ID: i64 = 100;
        const INDEX_ID: i64 = 5;

        // Helper to create column info
        fn make_column(col_id: i64, is_pk_handle: bool) -> ColumnInfo {
            let mut ci = ColumnInfo::default();
            ci.set_column_id(col_id);
            if is_pk_handle {
                ci.set_pk_handle(true);
            }
            ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
            ci
        }

        // Test value ranges
        let partition_ids: Vec<i64> = vec![-100, 0, 100];
        let indexed_values: Vec<i64> = vec![-50, 0, 50];
        let handle_values: Vec<i64> = vec![-999, 0, 999];
        let pid_in_value_options = [true, false]; // V1 vs V2
        let request_pid_column_options = [true, false];
        let request_phys_table_id_column_options = [true, false];

        for &partition_id in &partition_ids {
            for &indexed_value in &indexed_values {
                for &handle_value in &handle_values {
                    for &pid_in_value in &pid_in_value_options {
                        for &request_pid_column in &request_pid_column_options {
                            for &request_phys_table_id_column in
                                &request_phys_table_id_column_options
                            {
                                let version = if pid_in_value { "V1" } else { "V2" };
                                let pid_col = if request_pid_column {
                                    "with_pid_col"
                                } else {
                                    "no_pid_col"
                                };
                                let phys_col = if request_phys_table_id_column {
                                    "with_phys_col"
                                } else {
                                    "no_phys_col"
                                };

                                // Build columns based on test configuration
                                // Order: [indexed_col, handle, pid(-2), phys_table_id(-3)]
                                let mut columns_info = vec![
                                    make_column(1, false), // indexed column
                                    make_column(-1, true), // handle column
                                ];
                                let mut schema: Vec<FieldType> = vec![
                                    FieldTypeTp::LongLong.into(),
                                    FieldTypeTp::LongLong.into(),
                                ];
                                if request_pid_column {
                                    columns_info
                                        .push(make_column(table::EXTRA_PARTITION_ID_COL_ID, false));
                                    schema.push(FieldTypeTp::LongLong.into());
                                }
                                if request_phys_table_id_column {
                                    columns_info.push(make_column(
                                        table::EXTRA_PHYSICAL_TABLE_ID_COL_ID,
                                        false,
                                    ));
                                    schema.push(FieldTypeTp::LongLong.into());
                                }

                                // Build index key:
                                // [indexed_col][PARTITION_ID_FLAG][partition_id][handle]
                                let mut index_key_data = datum::encode_key(
                                    &mut EvalContext::default(),
                                    &[Datum::I64(indexed_value)],
                                )
                                .unwrap();
                                index_key_data.push(table::INDEX_VALUE_PARTITION_ID_FLAG);
                                index_key_data.write_i64(partition_id).unwrap();
                                let handle_data = datum::encode_key(
                                    &mut EvalContext::default(),
                                    &[Datum::I64(handle_value)],
                                )
                                .unwrap();
                                index_key_data.extend(handle_data);
                                let key = table::encode_index_seek_key(
                                    TABLE_ID,
                                    INDEX_ID,
                                    &index_key_data,
                                );

                                // Build index value (with or without partition ID based on
                                // version)
                                let value = if pid_in_value {
                                    let mut v = vec![0u8]; // tail_len = 0
                                    v.push(table::INDEX_VALUE_PARTITION_ID_FLAG);
                                    let mut pid_bytes = vec![0u8; 8];
                                    NumberCodec::encode_i64(&mut pid_bytes, partition_id);
                                    v.extend(&pid_bytes);
                                    v
                                } else {
                                    vec![0u8] // V2: no partition ID in value
                                };

                                let key_ranges = vec![{
                                    let mut range = KeyRange::default();
                                    range.set_start(key.clone());
                                    range.set_end(key.clone());
                                    convert_to_prefix_next(range.mut_end());
                                    range
                                }];

                                let store = FixtureStorage::from(vec![(key, value)]);
                                let mut executor = BatchIndexScanExecutor::<_, ApiV1>::new(
                                    store,
                                    Arc::new(EvalConfig::default()),
                                    columns_info,
                                    key_ranges,
                                    0,
                                    false,
                                    false,
                                    false,
                                    false,
                                )
                                .unwrap();

                                let mut result = block_on(executor.next_batch(10));
                                let expected_cols = 2
                                    + request_pid_column as usize
                                    + request_phys_table_id_column as usize;

                                assert!(
                                    result.is_drained.as_ref().unwrap().stop(),
                                    "{} {} {} pid={} idx={} handle={}",
                                    version,
                                    pid_col,
                                    phys_col,
                                    partition_id,
                                    indexed_value,
                                    handle_value
                                );
                                assert_eq!(
                                    result.physical_columns.columns_len(),
                                    expected_cols,
                                    "{} {} {} pid={} idx={} handle={}",
                                    version,
                                    pid_col,
                                    phys_col,
                                    partition_id,
                                    indexed_value,
                                    handle_value
                                );
                                assert_eq!(
                                    result.physical_columns.rows_len(),
                                    1,
                                    "{} {} {} pid={} idx={} handle={}",
                                    version,
                                    pid_col,
                                    phys_col,
                                    partition_id,
                                    indexed_value,
                                    handle_value
                                );

                                // Verify indexed column
                                result.physical_columns[0]
                                    .ensure_all_decoded_for_test(
                                        &mut EvalContext::default(),
                                        &schema[0],
                                    )
                                    .unwrap();
                                assert_eq!(
                                    result.physical_columns[0].decoded().to_int_vec(),
                                    &[Some(indexed_value)],
                                    "{} {} {} pid={} idx={} handle={}: indexed column mismatch",
                                    version,
                                    pid_col,
                                    phys_col,
                                    partition_id,
                                    indexed_value,
                                    handle_value
                                );

                                // Verify handle
                                assert_eq!(
                                    result.physical_columns[1].decoded().to_int_vec(),
                                    &[Some(handle_value)],
                                    "{} {} {} pid={} idx={} handle={}: handle mismatch",
                                    version,
                                    pid_col,
                                    phys_col,
                                    partition_id,
                                    indexed_value,
                                    handle_value
                                );

                                // Verify partition ID if requested
                                let mut next_col = 2;
                                if request_pid_column {
                                    assert_eq!(
                                        result.physical_columns[next_col].decoded().to_int_vec(),
                                        &[Some(partition_id)],
                                        "{} {} {} pid={} idx={} handle={}: partition ID mismatch",
                                        version,
                                        pid_col,
                                        phys_col,
                                        partition_id,
                                        indexed_value,
                                        handle_value
                                    );
                                    next_col += 1;
                                }

                                // Verify physical_table_id column if requested.
                                // For global indexes, it should return the partition ID
                                // (not the index table ID).
                                if request_phys_table_id_column {
                                    assert_eq!(
                                        result.physical_columns[next_col].decoded().to_int_vec(),
                                        &[Some(partition_id)],
                                        "{} {} {} pid={} idx={} handle={}: \
                                         physical_table_id should equal partition_id",
                                        version,
                                        pid_col,
                                        phys_col,
                                        partition_id,
                                        indexed_value,
                                        handle_value
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Exercises the V2 global index path through `process_kv_general` →
    /// `build_operations`, where partition ID comes from the KEY only (the
    /// `else if partition_id_from_key` fallback). The base
    /// `test_global_index_formats` V2 case uses a 1-byte value which routes
    /// through `process_old_collation_kv` instead.
    #[test]
    fn test_v2_global_index_new_encoding_path() {
        const TABLE_ID: i64 = 100;
        const INDEX_ID: i64 = 5;

        fn make_column(col_id: i64, is_pk_handle: bool) -> ColumnInfo {
            let mut ci = ColumnInfo::default();
            ci.set_column_id(col_id);
            if is_pk_handle {
                ci.set_pk_handle(true);
            }
            ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
            ci
        }

        let partition_id: i64 = 42;
        let indexed_value: i64 = 7;
        let handle_value: i64 = 123;

        for request_phys_table_id_column in [true, false] {
            // Build columns: [indexed_col, handle, phys_table_id?]
            let mut columns_info = vec![
                make_column(1, false), // indexed column
                make_column(-1, true), // handle column
            ];
            let mut schema: Vec<FieldType> =
                vec![FieldTypeTp::LongLong.into(), FieldTypeTp::LongLong.into()];
            if request_phys_table_id_column {
                columns_info.push(make_column(table::EXTRA_PHYSICAL_TABLE_ID_COL_ID, false));
                schema.push(FieldTypeTp::LongLong.into());
            }

            // Build index key:
            // [indexed_col][PARTITION_ID_FLAG][partition_id][handle]
            let mut index_key_data =
                datum::encode_key(&mut EvalContext::default(), &[Datum::I64(indexed_value)])
                    .unwrap();
            index_key_data.push(table::INDEX_VALUE_PARTITION_ID_FLAG);
            index_key_data.write_i64(partition_id).unwrap();
            let handle_data =
                datum::encode_key(&mut EvalContext::default(), &[Datum::I64(handle_value)])
                    .unwrap();
            index_key_data.extend(handle_data);
            let key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &index_key_data);

            // Build V2 new-encoding value (index_version=1, no partition ID in
            // value). Include a minimal v2 row as restore data so that
            // value.len() > MAX_OLD_ENCODED_VALUE_LEN (9), routing to
            // process_kv_general instead of process_old_collation_kv.
            let mut value = Vec::new();
            value.push(0x00); // tail_len = 0 (non-unique, handle in key)
            value.push(INDEX_VALUE_VERSION_FLAG); // version segment marker
            value.push(0x01); // version = 1
            // Minimal valid v2 row (empty row with one null column_id=1):
            //   [CODEC_VERSION=0x80][flags=0x00][non_null_cnt=0][null_cnt=1][null_id=1]
            value.extend_from_slice(&[0x80, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01]);
            assert!(value.len() > MAX_OLD_ENCODED_VALUE_LEN);

            let key_ranges = vec![{
                let mut range = KeyRange::default();
                range.set_start(key.clone());
                range.set_end(key.clone());
                convert_to_prefix_next(range.mut_end());
                range
            }];

            let store = FixtureStorage::from(vec![(key, value)]);
            let mut executor = BatchIndexScanExecutor::<_, ApiV1>::new(
                store,
                Arc::new(EvalConfig::default()),
                columns_info,
                key_ranges,
                0,
                false,
                false,
                false,
                false,
            )
            .unwrap();

            let mut result = block_on(executor.next_batch(10));

            let phys_label = if request_phys_table_id_column {
                "with_phys_col"
            } else {
                "no_phys_col"
            };

            assert!(
                result.is_drained.as_ref().unwrap().stop(),
                "V2 new-encoding {}",
                phys_label
            );
            assert_eq!(
                result.physical_columns.rows_len(),
                1,
                "V2 new-encoding {}",
                phys_label
            );

            // Verify indexed column
            result.physical_columns[0]
                .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[0])
                .unwrap();
            assert_eq!(
                result.physical_columns[0].decoded().to_int_vec(),
                &[Some(indexed_value)],
                "V2 new-encoding {}: indexed column mismatch",
                phys_label
            );

            // Verify handle
            assert_eq!(
                result.physical_columns[1].decoded().to_int_vec(),
                &[Some(handle_value)],
                "V2 new-encoding {}: handle mismatch",
                phys_label
            );

            // Verify physical_table_id column uses the partition ID from the key
            if request_phys_table_id_column {
                assert_eq!(
                    result.physical_columns[2].decoded().to_int_vec(),
                    &[Some(partition_id)],
                    "V2 new-encoding: physical_table_id should equal partition_id"
                );
            }
        }
    }
}
