// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use kvproto::coprocessor::KeyRange;
use tidb_query_datatype::{Collation, EvalType, FieldTypeAccessor};
use tipb::ColumnInfo;
use tipb::FieldType;
use tipb::IndexScan;

use super::util::scan_executor::*;
use crate::interface::*;
use codec::number::NumberCodec;
use codec::prelude::NumberDecoder;
use itertools::izip;
use tidb_query_common::storage::{IntervalRange, Storage};
use tidb_query_common::Result;
use tidb_query_datatype::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use tidb_query_datatype::codec::row::v2::{decode_v2_u64, RowSlice, V1CompatibleEncoder};
use tidb_query_datatype::codec::table::{
    check_index_key, INDEX_VALUE_VERSION_FLAG, MAX_OLD_ENCODED_VALUE_LEN,
};
use tidb_query_datatype::codec::{datum, datum::DatumDecoder, table, Datum};
use tidb_query_datatype::expr::{EvalConfig, EvalContext};

use tidb_query_datatype::codec::collation::collator::PADDING_SPACE;
use DecodeHandleStrategy::*;

pub struct BatchIndexScanExecutor<S: Storage>(ScanExecutor<S, IndexScanExecutorImpl>);

// We assign a dummy type `Box<dyn Storage<Statistics = ()>>` so that we can omit the type
// when calling `check_supported`.
impl BatchIndexScanExecutor<Box<dyn Storage<Statistics = ()>>> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &IndexScan) -> Result<()> {
        check_columns_info_supported(descriptor.get_columns())
    }
}

impl<S: Storage> BatchIndexScanExecutor<S> {
    pub fn new(
        storage: S,
        config: Arc<EvalConfig>,
        columns_info: Vec<ColumnInfo>,
        key_ranges: Vec<KeyRange>,
        primary_column_ids_len: usize,
        is_backward: bool,
        unique: bool,
        is_scanned_range_aware: bool,
    ) -> Result<Self> {
        // Note 1: `unique = true` doesn't completely mean that it is a unique index scan. Instead
        // it just means that we can use point-get for this index. In the following scenarios
        // `unique` will be `false`:
        // - scan from a non-unique index
        // - scan from a unique index with like: where unique-index like xxx
        //
        // Note 2: Unlike table scan executor, the accepted `columns_info` of index scan executor is
        // strictly stipulated. The order of columns in the schema must be the same as index data
        // stored and if PK handle is needed it must be placed as the last one.
        //
        // Note 3: Currently TiDB may send multiple PK handles to TiKV (but only the last one is
        // real). We accept this kind of request for compatibility considerations, but will be
        // forbidden soon.
        //
        // Note 4: When process global indexes, an extra partition ID column with column ID
        // `table::EXTRA_PARTITION_ID_COL_ID` will append to column info to indicate which partiton
        // handles belong to. See https://github.com/pingcap/parser/pull/1010 for more information.
        let pid_column_cnt = columns_info.last().map_or(0, |ci| {
            (ci.get_column_id() == table::EXTRA_PARTITION_ID_COL_ID) as usize
        });
        let is_int_handle = columns_info
            .get(columns_info.len() - 1 - pid_column_cnt)
            .map_or(false, |ci| ci.get_pk_handle());
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

        if handle_column_cnt + pid_column_cnt > columns_info.len() {
            return Err(other_err!(
                "The number of handle columns exceeds the length of `columns_info`"
            ));
        }

        let schema: Vec<_> = columns_info
            .iter()
            .map(|ci| field_type_from_column_info(&ci))
            .collect();

        let columns_id_without_handle: Vec<_> = columns_info
            [..columns_info.len() - handle_column_cnt - pid_column_cnt]
            .iter()
            .map(|ci| ci.get_column_id())
            .collect();

        let columns_id_for_common_handle = columns_info
            [columns_id_without_handle.len()..columns_info.len() - pid_column_cnt]
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
            index_version: -1,
        };
        let wrapper = ScanExecutor::new(ScanExecutorOptions {
            imp,
            storage,
            key_ranges,
            is_backward,
            is_key_only: false,
            accept_point_range: unique,
            is_scanned_range_aware,
        })?;
        Ok(Self(wrapper))
    }
}

impl<S: Storage> BatchExecutor for BatchIndexScanExecutor<S> {
    type StorageStats = S::Statistics;

    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.0.schema()
    }

    #[inline]
    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        self.0.next_batch(scan_rows)
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

    /// The strategy to decode handles.
    /// Handle will be always placed in the last column.
    decode_handle_strategy: DecodeHandleStrategy,

    /// Number of partition ID columns, now it can only be 0 or 1.
    pid_column_cnt: usize,

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

    /// Constructs empty columns, with PK containing int handle in decoded format and the rest in raw format.
    ///
    /// Note: the structure of the constructed column is the same as table scan executor but due
    /// to different reasons.
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
                for _ in self.columns_id_without_handle.len()..columns_len - self.pid_column_cnt {
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

        assert_eq!(columns.len(), columns_len);
        LazyBatchColumnVec::from(columns)
    }

    // Value layout: (see https://docs.google.com/document/d/1Co5iMiaxitv3okJmLYLJxZYCNChcjzswJMRr-_45Eqg/edit?usp=sharing)
    //		+-- IndexValueVersion0  (with restore data, or common handle, or index is global)
    //		|
    //		|  Layout: TailLen | Options      | Padding      | [IntHandle] | [UntouchedFlag]
    //		|  Length:   1     | len(options) | len(padding) |    8        |     1
    //		|
    //		|  TailLen:       len(padding) + len(IntHandle) + len(UntouchedFlag)
    //		|  Options:       Encode some value for new features, such as common handle, new collations or global index.
    //		|                 See below for more information.
    //		|  Padding:       Ensure length of value always >= 10. (or >= 11 if UntouchedFlag exists.)
    //		|  IntHandle:     Only exists when table use int handles and index is unique.
    //		|  UntouchedFlag: Only exists when index is untouched.
    //		|
    //		+-- Old Encoding (without restore data, integer handle, local)
    //		|
    //		|  Layout: [Handle] | [UntouchedFlag]
    //		|  Length:   8      |     1
    //		|
    //		|  Handle:        Only exists in unique index.
    //		|  UntouchedFlag: Only exists when index is untouched.
    //		|
    //		|  If neither Handle nor UntouchedFlag exists, value will be one single byte '0' (i.e. []byte{'0'}).
    //		|  Length of value <= 9, use to distinguish from the new encoding.
    // 		|
    //		+-- IndexValueForClusteredIndexVersion1
    //		|
    //		|  Layout: TailLen |    VersionFlag  |    Version     ｜ Options      |   [UntouchedFlag]
    //		|  Length:   1     |        1        |      1         |  len(options) |         1
    //		|
    //		|  TailLen:       len(UntouchedFlag)
    //		|  Options:       Encode some value for new features, such as common handle, new collations or global index.
    //		|                 See below for more information.
    //		|  UntouchedFlag: Only exists when index is untouched.
    //		|
    //		|  Layout of Options:
    //		|
    //		|     Segment:             Common Handle                 |     Global Index      |   New Collation
    // 		|     Layout:  CHandle Flag | CHandle Len | CHandle      | PidFlag | PartitionID |    restoreData
    //		|     Length:     1         | 2           | len(CHandle) |    1    |    8        |   len(restoreData)
    //		|
    //		|     Common Handle Segment: Exists when unique index used common handles.
    //		|     Global Index Segment:  Exists when index is global.
    //		|     New Collation Segment: Exists when new collation is used and index or handle contains non-binary string.
    //		|     In v4.0, restored data contains all the index values. For example, (a int, b char(10)) and index (a, b).
    //		|     The restored data contains both the values of a and b.
    //		|     In v5.0, restored data contains only non-binary data(except for char and _bin). In the above example, the restored data contains only the value of b.
    //		|     Besides, if the collation of b is _bin, then restored data is an integer indicate the spaces are truncated. Then we use sortKey
    //		|     and the restored data together to restore original data.
    #[inline]
    fn process_kv_pair(
        &mut self,
        mut key: &[u8],
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        check_index_key(key)?;
        key = &key[table::PREFIX_LEN + table::ID_LEN..];
        if self.index_version == -1 {
            self.index_version = Self::get_index_version(value)?
        }
        if value.len() > MAX_OLD_ENCODED_VALUE_LEN {
            self.process_kv_general(key, value, columns)
        } else {
            self.process_old_collation_kv(key, value, columns)
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
    PID(&'a [u8]),
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

    #[inline]
    fn decode_int_handle_from_key(&self, key: &[u8]) -> Result<i64> {
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

    // Process index values that are in old collation but don't contain common handles.
    // NOTE: We should extract the index columns from the key first, and extract the handles from value if there is no handle in the key.
    // Otherwise, extract the handles from the key.
    fn process_old_collation_kv(
        &mut self,
        mut key_payload: &[u8],
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        Self::extract_columns_from_datum_format(
            &mut key_payload,
            &mut columns[..self.columns_id_without_handle.len()],
        )?;

        match self.decode_handle_strategy {
            NoDecode => {}
            // For normal index, it is placed at the end and any columns prior to it are
            // ensured to be interested. For unique index, it is placed in the value.
            DecodeIntHandle if key_payload.is_empty() => {
                // This is a unique index, and we should look up PK int handle in the value.
                let handle_val = self.decode_int_handle_from_value(value)?;
                columns[self.columns_id_without_handle.len()]
                    .mut_decoded()
                    .push_int(Some(handle_val));
            }
            DecodeIntHandle => {
                // This is a normal index, and we should look up PK handle in the key.
                let handle_val = self.decode_int_handle_from_key(key_payload)?;
                columns[self.columns_id_without_handle.len()]
                    .mut_decoded()
                    .push_int(Some(handle_val));
            }
            DecodeCommonHandle => {
                // Otherwise, if the handle is common handle, we extract it from the key.
                Self::extract_columns_from_datum_format(
                    &mut key_payload,
                    &mut columns[self.columns_id_without_handle.len()..],
                )?;
            }
        }

        Ok(())
    }

    // restore_original_data restores the index values whose format is introduced in TiDB 5.0.
    // Unlike the format in TiDB 4.0, the new format is optimized for storage space:
    // 1. If the index is a composed index, only the non-binary string column's value need to write to value, not all.
    // 2. If a string column's collation is _bin, then we only write the number of the truncated spaces to value.
    // 3. If a string column is char, not varchar, then we use the sortKey directly.
    // The whole logic of this function is:
    // 1. For each column pass in, check if it needs the restored data to get to original data. If not, check the next column.
    // 2. Skip if the `sort key` is NULL, because the original data must be NULL.
    // 3. Depend on the collation if `_bin` or not. Process them differently to get the correct original data.
    // 4. Write the original data into the column, we need to make sure pop() is called.
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
                .map(|col| col == Collation::Utf8Mb4Bin)
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
                // _bin collation, we need to combine data from key and value to form the original data.

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
                    .chain(std::iter::repeat(PADDING_SPACE as _).take(space_num as _))
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
        key_payload: &[u8],
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        let (decode_handle, decode_pid, restore_data) =
            self.build_operations(key_payload, value)?;

        self.decode_index_columns(key_payload, columns, restore_data)?;
        self.decode_handle_columns(decode_handle, columns, restore_data)?;
        self.decode_pid_columns(columns, decode_pid)?;

        Ok(())
    }

    #[inline]
    fn build_operations<'a, 'b>(
        &'b self,
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
        let (decode_handle_op, remaining) = {
            if !common_handle_bytes.is_empty() && self.decode_handle_strategy != DecodeCommonHandle
            {
                return Err(other_err!(
                    "Expect to decode index values with common handles in `DecodeCommonHandle` mode."
                ));
            }

            let dispatcher = match self.decode_handle_strategy {
                NoDecode => DecodeHandleOp::Nop,
                DecodeIntHandle if tail_len < 8 => {
                    // This is a non-unique index, we should extract the int handle from the key.
                    datum::skip_n(&mut key_payload, self.columns_id_without_handle.len())?;
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

        let (partition_id_bytes, remaining) = Self::split_partition_id(remaining)?;
        let decode_pid_op = {
            if self.pid_column_cnt > 0 && partition_id_bytes.is_empty() {
                return Err(other_err!(
                    "Expect to decode partition id but payload is empty"
                ));
            } else if partition_id_bytes.is_empty() {
                DecodePartitionIdOp::Nop
            } else {
                DecodePartitionIdOp::PID(partition_id_bytes)
            }
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
        restore_data: RestoreData,
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
                // 4.0 version format, use the restore data directly. The restore data contain all the indexed values.
                self.extract_columns_from_row_format(rst, columns)?;
            }
            RestoreData::V5(rst) => {
                // Extract the data from key, then use the restore data to get the original data.
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
        decode_handle: DecodeHandleOp,
        columns: &mut LazyBatchColumnVec,
        restore_data: RestoreData,
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
                let end_index = columns.columns_len() - self.pid_column_cnt;
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
            let end_index = columns.columns_len() - self.pid_column_cnt;
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
    fn decode_pid_columns(
        &mut self,
        columns: &mut LazyBatchColumnVec,
        decode_pid: DecodePartitionIdOp,
    ) -> Result<()> {
        match decode_pid {
            DecodePartitionIdOp::Nop => {}
            DecodePartitionIdOp::PID(pid) => {
                // If need partition id, append partition id to the last column.
                let pid = NumberCodec::decode_i64(pid);
                let idx = columns.columns_len() - 1;
                columns[idx].mut_decoded().push_int(Some(pid))
            }
        }
        Ok(())
    }

    #[inline]
    fn split_common_handle(value: &[u8]) -> Result<(&[u8], &[u8])> {
        if value
            .get(0)
            .map_or(false, |c| *c == table::INDEX_VALUE_COMMON_HANDLE_FLAG)
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
            .get(0)
            .map_or(false, |c| *c == table::INDEX_VALUE_PARTITION_ID_FLAG)
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
                .get(0)
                .map_or(false, |c| *c == table::INDEX_VALUE_RESTORED_DATA_FLAG)
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
    use super::*;

    use std::sync::Arc;

    use codec::prelude::NumberEncoder;
    use kvproto::coprocessor::KeyRange;
    use tidb_query_datatype::{FieldTypeAccessor, FieldTypeTp};
    use tipb::ColumnInfo;

    use tidb_query_common::storage::test_fixture::FixtureStorage;
    use tidb_query_common::util::convert_to_prefix_next;
    use tidb_query_datatype::codec::data_type::*;
    use tidb_query_datatype::codec::{
        datum,
        row::v2::encoder_for_test::{Column, RowEncoder},
        table, Datum,
    };
    use tidb_query_datatype::expr::EvalConfig;

    #[test]
    fn test_basic() {
        const TABLE_ID: i64 = 3;
        const INDEX_ID: i64 = 42;
        let mut ctx = EvalContext::default();

        // Index schema: (INT, FLOAT)

        // the elements in data are: [int index, float index, handle id].
        let data = vec![
            [Datum::I64(-5), Datum::F64(0.3), Datum::I64(10)],
            [Datum::I64(5), Datum::F64(5.1), Datum::I64(5)],
            [Datum::I64(5), Datum::F64(10.5), Datum::I64(2)],
        ];

        // The column info for each column in `data`. Used to build the executor.
        let columns_info = vec![
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
        ];

        // The schema of these columns. Used to check executor output.
        let schema = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Double.into(),
            FieldTypeTp::LongLong.into(),
        ];

        // Case 1. Normal index.

        // For a normal index, the PK handle is stored in the key and nothing interesting is stored
        // in the value. So let's build corresponding KV data.

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

            let mut executor = BatchIndexScanExecutor::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![columns_info[0].clone(), columns_info[1].clone()],
                key_ranges,
                0,
                true,
                false,
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
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

            let mut executor = BatchIndexScanExecutor::new(
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
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
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

            let mut executor = BatchIndexScanExecutor::new(
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
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
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

            let mut executor = BatchIndexScanExecutor::new(
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
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
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
        let datums = vec![Datum::U64(2), Datum::U64(3), Datum::F64(4.0)];

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
        value_prefix.extend(common_handle);

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
        let mut executor = BatchIndexScanExecutor::new(
            store,
            Arc::new(EvalConfig::default()),
            columns_info.clone(),
            key_ranges.clone(),
            1,
            false,
            true,
            false,
        )
        .unwrap();

        let mut result = executor.next_batch(10);
        assert!(result.is_drained.as_ref().unwrap());
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

        let value = value_prefix;
        let store = FixtureStorage::from(vec![(key, value)]);
        let mut executor = BatchIndexScanExecutor::new(
            store,
            Arc::new(EvalConfig::default()),
            columns_info,
            key_ranges,
            1,
            false,
            true,
            false,
        )
        .unwrap();

        let mut result = executor.next_batch(10);
        assert!(result.is_drained.as_ref().unwrap());
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

        let datums = vec![Datum::U64(2), Datum::U64(3), Datum::F64(4.0)];

        let common_handle = datum::encode_key(
            &mut EvalContext::default(),
            &[Datum::U64(3), Datum::F64(4.0)],
        )
        .unwrap();

        let index_data = datum::encode_key(&mut EvalContext::default(), &datums[0..1]).unwrap();
        let mut key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &index_data);
        key.extend(common_handle);

        let key_ranges = vec![{
            let mut range = KeyRange::default();
            let start_key = key.clone();
            range.set_start(start_key);
            range.set_end(range.get_start().to_vec());
            convert_to_prefix_next(range.mut_end());
            range
        }];

        let store = FixtureStorage::from(vec![(key, vec![])]);
        let mut executor = BatchIndexScanExecutor::new(
            store,
            Arc::new(EvalConfig::default()),
            columns_info,
            key_ranges,
            2,
            false,
            false,
            false,
        )
        .unwrap();

        let mut result = executor.next_batch(10);
        assert!(result.is_drained.as_ref().unwrap());
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
        let datums = vec![Datum::U64(2), Datum::F64(3.0), Datum::U64(4)];
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
        let mut executor = BatchIndexScanExecutor::new(
            store,
            Arc::new(EvalConfig::default()),
            columns_info,
            key_ranges,
            0,
            false,
            true,
            false,
        )
        .unwrap();

        let mut result = executor.next_batch(10);
        assert!(result.is_drained.as_ref().unwrap());
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
        let mut executor = BatchIndexScanExecutor::new(
            store,
            Arc::new(EvalConfig::default()),
            columns_info,
            key_ranges,
            0,
            false,
            true,
            false,
        )
        .unwrap();

        let mut result = executor.next_batch(10);
        assert!(result.is_drained.as_ref().unwrap());
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
        let mut executor = BatchIndexScanExecutor::new(
            store,
            Arc::new(EvalConfig::default()),
            columns_info,
            key_ranges,
            1,
            false,
            true,
            false,
        )
        .unwrap();

        let mut result = executor.next_batch(10);
        assert!(result.is_drained.as_ref().unwrap());
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
        let datums = vec![Datum::U64(2), Datum::U64(3), Datum::F64(4.0)];

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
        value_prefix.extend(common_handle);

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
        let mut executor = BatchIndexScanExecutor::new(
            store,
            Arc::new(EvalConfig::default()),
            columns_info,
            key_ranges,
            1,
            false,
            true,
            false,
        )
        .unwrap();

        let mut result = executor.next_batch(10);
        assert!(result.is_drained.as_ref().unwrap());
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
    }

    #[test]
    fn test_int_handle_char_index() {
        use tidb_query_datatype::builder::FieldTypeBuilder;

        // Schema: create table t(a int, b char(10) collate utf8mb4_bin, c char(10) collate utf8mb4_unicode_ci, key i_a(a), key i_b(b), key i_c(c), key i_abc(a, b, c), unique key i_ua(a),
        //  unique key i_ub(b), unique key i_uc(c), unique key i_uabc(a,b,c));
        // insert into t values (1, "a ", "A ");

        // i_a and i_ua
        let mut idx_exe = IndexScanExecutorImpl {
            context: Default::default(),
            schema: vec![FieldTypeTp::Long.into(), FieldTypeTp::LongLong.into()],
            columns_id_without_handle: vec![1],
            columns_id_for_common_handle: vec![],
            decode_handle_strategy: DecodeHandleStrategy::DecodeIntHandle,
            pid_column_cnt: 0,
            index_version: -1,
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
            index_version: -1,
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
            index_version: -1,
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
            index_version: -1,
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

        // Schema: create table t(a int, b varchar(10) collate utf8mb4_bin, c varchar(10) collate utf8mb4_unicode_ci, key i_a(a), key i_b(b), key i_c(c), key i_abc(a, b, c), unique key i_ua(a),
        //  unique key i_ub(b), unique key i_uc(c), unique key i_uabc(a,b,c));
        // insert into t values (1, "a ", "A ");

        // i_a and i_ua
        let mut idx_exe = IndexScanExecutorImpl {
            context: Default::default(),
            schema: vec![FieldTypeTp::Long.into(), FieldTypeTp::LongLong.into()],
            columns_id_without_handle: vec![1],
            columns_id_for_common_handle: vec![],
            decode_handle_strategy: DecodeHandleStrategy::DecodeIntHandle,
            pid_column_cnt: 0,
            index_version: -1,
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
            index_version: -1,
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
            index_version: -1,
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
            index_version: -1,
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

        // create table t(a int, b char(10) collate utf8mb4_bin, c char(10) collate utf8mb4_unicode_ci, d varchar(10) collate utf8mb4_bin, e varchar(10) collate utf8mb4_general_ci
        // , primary key(a, b, c, d, e),  key i_a(a), key i_b(b), key i_c(c), key i_d(d), key i_e(e), key i_abcde(a, b, c, d, e), unique key i_ua(a), unique key i_ub(b), unique key i_uc(
        // c), unique key i_ud(d), unique key i_ue(e), unique key i_uabcde(a,b,c, d, e));
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
            index_version: -1,
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
            index_version: -1,
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
            index_version: -1,
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
            index_version: -1,
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
            index_version: -1,
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
            index_version: -1,
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
}
