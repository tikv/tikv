use std::{collections::HashSet, sync::Arc};

use api_version::{ApiV1, KvFormat};
use async_trait::async_trait;
use itertools::Itertools;
use kvproto::coprocessor::KeyRange;
use tidb_query_common::{
    execute_stats::ExecuteStats,
    storage::{IntervalRange, Storage},
    Result,
};
use tidb_query_datatype::{
    codec::{
        batch::LazyBatchColumnVec,
        data_type::LogicalRows,
        datum,
        table::{
            encode_common_handle_to_buf, encode_row_key_to_buf, PREFIX_LEN, RECORD_ROW_KEY_LEN,
        },
        Datum,
    },
    expr::{EvalConfig, EvalContext, EvalWarnings},
    FieldTypeTp,
};
use tikv_util::store::check_key_in_region;
use tipb::{self, ColumnInfo, FieldType, IndexLookup};
use txn_types::Key;

use crate::{
    interface::{
        AsyncRegion, BatchExecIsDrain, BatchExecuteResult, BatchExecutor, FnLocateRegionKey,
    },
    util::scan_executor::{check_columns_info_supported, field_type_from_column_info},
    BatchTableScanExecutor,
};

pub struct BatchIndexLookupExecutor<S: Storage, F: KvFormat> {
    schema: Vec<FieldType>,
    cur_probe_index: usize,
    probe_children: Vec<BatchTableScanExecutor<S, F>>,
}

// We assign a dummy type `Box<dyn Storage<Statistics = ()>>` so that we can
// omit the type when calling `check_supported`.
impl BatchIndexLookupExecutor<Box<dyn Storage<Statistics = ()>>, ApiV1> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &IndexLookup) -> Result<()> {
        check_columns_info_supported(descriptor.get_columns())
    }
}

impl<S: Storage, F: KvFormat> BatchIndexLookupExecutor<S, F> {
    pub fn new(
        config: Arc<EvalConfig>,
        columns_info: Vec<ColumnInfo>,
        primary_column_ids: Vec<i64>,
        primary_prefix_column_ids: Vec<i64>,
        probe_ranges: Vec<(S, Vec<KeyRange>)>,
    ) -> Result<Self> {
        let mut schema = Vec::with_capacity(columns_info.len());
        for ci in columns_info.iter() {
            // For each column info, we need to extract the following info:
            // - Corresponding field type (push into `schema`).
            schema.push(field_type_from_column_info(&ci));
        }

        let mut children = Vec::with_capacity(probe_ranges.len());
        for (storage, ranges) in probe_ranges {
            children.push(BatchTableScanExecutor::new(
                storage,
                config.clone(),
                columns_info.clone(),
                ranges,
                primary_column_ids.clone(),
                false,
                false,
                primary_prefix_column_ids.clone(),
            )?)
        }

        Ok(BatchIndexLookupExecutor {
            schema,
            cur_probe_index: 0,
            probe_children: children,
        })
    }
}

#[async_trait]
impl<S: Storage, F: KvFormat> BatchExecutor for BatchIndexLookupExecutor<S, F> {
    type StorageStats = S::Statistics;

    fn schema(&self) -> &[FieldType] {
        &self.schema
    }

    async fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        if self.cur_probe_index >= self.probe_children.len() {
            return BatchExecuteResult {
                is_drained: Ok(BatchExecIsDrain::Drain),
                physical_columns: LazyBatchColumnVec::empty(),
                logical_rows: vec![],
                warnings: EvalWarnings::default(),
            };
        }

        let child = &mut self.probe_children[self.cur_probe_index];
        let mut result = child.next_batch(scan_rows).await;
        match result.is_drained {
            Ok(is_drain) if is_drain.stop() => {
                child.close_storage_scan();
                self.cur_probe_index += 1;
                if self.cur_probe_index < self.probe_children.len() {
                    result.is_drained = Ok(BatchExecIsDrain::Remain);
                }
                result
            }
            _ => result,
        }
    }

    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        for child in &mut self.probe_children {
            child.collect_exec_stats(dest);
        }
    }

    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        for child in &mut self.probe_children {
            child.collect_storage_stats(dest);
        }
    }

    fn take_scanned_range(&mut self) -> IntervalRange {
        unimplemented!()
    }

    fn can_be_cached(&self) -> bool {
        false
    }
}

trait Handle: Eq + Ord + Sized {
    fn new(
        ctx: &mut EvalContext,
        rows: &LazyBatchColumnVec,
        field_types: &[FieldType],
        handle_offsets: &[u32],
        row_index: usize,
    ) -> Result<Self>;
    fn encode_raw_key(&self, table_id: i64) -> Vec<u8>;
    fn is_next(&self, other: &Self) -> bool;
}

#[derive(PartialEq, PartialOrd, Eq, Ord, Clone, Copy, Debug)]
struct IntHandle(i64);

impl Handle for IntHandle {
    #[inline]
    fn new(
        _: &mut EvalContext,
        rows: &LazyBatchColumnVec,
        _: &[FieldType],
        handle_offsets: &[u32],
        row_index: usize,
    ) -> Result<Self> {
        let int_val = *rows[handle_offsets[0] as usize]
            .decoded()
            .get_scalar_ref(row_index)
            .as_int()
            .unwrap();
        Ok(IntHandle(int_val.into()))
    }

    #[inline]
    fn encode_raw_key(&self, table_id: i64) -> Vec<u8> {
        let mut key = Vec::with_capacity(RECORD_ROW_KEY_LEN + 1);
        encode_row_key_to_buf(table_id, self.0, &mut key);
        key
    }

    #[inline]
    fn is_next(&self, other: &Self) -> bool {
        other.0 + 1 == self.0
    }
}

#[derive(PartialEq, PartialOrd, Eq, Ord, Debug)]
struct CommonHandle(Vec<u8>);

impl Handle for CommonHandle {
    #[inline]
    fn new(
        ctx: &mut EvalContext,
        rows: &LazyBatchColumnVec,
        field_types: &[FieldType],
        handle_offsets: &[u32],
        row_index: usize,
    ) -> Result<Self> {
        let mut row = Vec::with_capacity(handle_offsets.len());
        for offset in handle_offsets {
            let offset = *offset as usize;
            row.push(Datum::from_scalar(
                rows[offset].decoded().get_scalar_ref(row_index),
                &field_types[offset],
                true,
            )?);
        }
        Ok(CommonHandle(datum::encode_key(ctx, &row)?))
    }

    #[inline]
    fn encode_raw_key(&self, table_id: i64) -> Vec<u8> {
        let mut key = Vec::with_capacity(PREFIX_LEN + self.0.len() + 1);
        encode_common_handle_to_buf(table_id, &self.0, &mut key);
        key
    }

    #[inline]
    fn is_next(&self, _: &Self) -> bool {
        false
    }
}

async fn build_index_lookup_probe_ranges_for_handles<S: Storage, T: Handle>(
    ctx: &mut EvalContext,
    table_id: i64,
    handle_offsets: &[u32],
    build_side_results: &mut Vec<BatchExecuteResult>,
    result_schema: &[FieldType],
    locate_key: &FnLocateRegionKey<S>,
) -> Result<Vec<(S, Vec<KeyRange>)>> {
    let sorted_handles =
        sort_result_handles::<T>(ctx, build_side_results, result_schema, handle_offsets)?;

    let mut probe_side_ranges = Vec::with_capacity(sorted_handles.handles.len());
    let mut left_handles = HashSet::<(usize, usize)>::new();
    let mut current_region: Option<(AsyncRegion<S>, Vec<KeyRange>, (usize, usize))> = None;
    let handles_len = sorted_handles.handles.len();
    for i in 0..=handles_len {
        let mut region_has_end = None;
        if i < handles_len {
            let handle_idx = sorted_handles.orders[i];
            let handle = &sorted_handles.handles[handle_idx];
            let pos_in_result = sorted_handles.get_position_in_results(i)?;
            let mut raw_key = handle.encode_raw_key(table_id);
            let raw_key_len = raw_key.len();
            let key = Key::from_raw(&raw_key);
            match current_region {
                Some((ref region, ref mut ranges, ref mut slice))
                    if check_key_in_region(key.as_encoded(), region.get_region()) =>
                {
                    let lat_handle = &sorted_handles.handles[sorted_handles.orders[i - 1]];
                    if lat_handle.is_next(handle) {
                        // warn!(
                        //     "[ILP][{} {} {:?}] Append the region: {}, exist range, key: {:?}",
                        //     i,
                        //     handle_idx,
                        //     pos_in_result,
                        //     region.get_region().id,
                        //     key,
                        // );
                        let last_range = ranges.last_mut().unwrap();
                        raw_key.push(0);
                        last_range.set_end(raw_key);
                    } else {
                        // warn!(
                        //     "[ILP][{} {} {:?}] Append the region: {}, new range, key: {:?}",
                        //     i,
                        //     handle_idx,
                        //     pos_in_result,
                        //     region.get_region().id,
                        //     key,
                        // );
                        let mut r = KeyRange::new();
                        r.set_start(raw_key);
                        let mut raw_end = Vec::with_capacity(raw_key_len + 1);
                        raw_end.extend_from_slice(r.get_start());
                        raw_end.push(0);
                        r.set_end(raw_end);
                        ranges.push(r);
                    }
                    slice.1 = i;
                }
                _ => {
                    region_has_end = current_region.take();
                    if let Some(region) = locate_key(key.as_encoded()).take() {
                        // warn!(
                        //     "[ILP] [{} {} {:?}] Start new region: {}, key: {:?}",
                        //     i,
                        //     handle_idx,
                        //     pos_in_result,
                        //     region.get_region().id,
                        //     key,
                        // );
                        let mut r = KeyRange::new();
                        r.set_start(raw_key);
                        let mut raw_end = Vec::with_capacity(raw_key_len + 1);
                        raw_end.extend_from_slice(r.get_start());
                        raw_end.push(0);
                        r.set_end(raw_end);
                        current_region = Some((region, vec![r], (i, i)));
                    } else {
                        // warn!(
                        //     "[ILP] [{} {} {:?}] Miss region, key: {:?}",
                        //     i, handle_idx, pos_in_result, key
                        // );
                        left_handles.insert(sorted_handles.get_position_in_results(i)?);
                    }
                }
            }
        } else {
            region_has_end = current_region.take();
        }

        if let Some((region, ranges, slice)) = region_has_end {
            let store = region.get_region_store(&ranges).await;
            if store.is_some() {
                probe_side_ranges.push((store.unwrap(), ranges));
            } else {
                // warn!(
                //     "[ILP] Region: {} failed to get store",
                //     region.get_region().id
                // );
                for pos in slice.0..=slice.1 {
                    left_handles.insert(sorted_handles.get_position_in_results(pos)?);
                }
            }
        }
    }

    let no_left_handles = left_handles.is_empty();
    // warn!(
    //     "[ILP] Left handles, size: {}, pos: {:?}",
    //     left_handles.len(),
    //     left_handles
    // );
    for (i, result) in build_side_results.iter_mut().enumerate() {
        if no_left_handles {
            result.logical_rows = vec![];
        } else {
            // warn!(
            //     "[ILP] Before retain, logic rows of result {}: {:?}",
            //     i, result.logical_rows
            // );
            result
                .logical_rows
                .retain(|row| left_handles.contains(&(i, *row)));
            // warn!(
            //     "[ILP] After retain, logic rows of result {}: {:?}",
            //     i, result.logical_rows
            // );
        }
    }

    Ok(probe_side_ranges)
}

pub async fn build_index_lookup_probe_ranges<S: Storage>(
    ctx: &mut EvalContext,
    descriptor: &IndexLookup,
    build_side_results: &mut Vec<BatchExecuteResult>,
    result_schema: &[FieldType],
    locate_key: &FnLocateRegionKey<S>,
) -> Result<Vec<(S, Vec<KeyRange>)>> {
    let handle_offsets = descriptor.get_build_side_primary_key_offsets();
    if handle_offsets.is_empty() {
        return Err(other_err!("no handle offsets provided"));
    }

    if result_schema.is_empty() {
        return Err(other_err!("no result schema provided"));
    }

    if handle_offsets.len() == 1 {
        let offset = handle_offsets[0] as usize;
        let tp_code = result_schema[offset].get_tp();
        let tp = FieldTypeTp::from_i32(tp_code);
        match tp {
            Some(
                FieldTypeTp::Long | FieldTypeTp::LongLong | FieldTypeTp::Tiny | FieldTypeTp::Short,
            ) => {
                return build_index_lookup_probe_ranges_for_handles::<S, IntHandle>(
                    ctx,
                    descriptor.get_table_id(),
                    handle_offsets,
                    build_side_results,
                    result_schema,
                    locate_key,
                )
                .await;
            }
            None => {
                return Err(other_err!("unsupported type code: {}", tp_code));
            }
            _ => {}
        }
    }

    build_index_lookup_probe_ranges_for_handles::<S, CommonHandle>(
        ctx,
        descriptor.get_table_id(),
        handle_offsets,
        build_side_results,
        result_schema,
        locate_key,
    )
    .await
}

struct SortedHandles<'a, T: Handle> {
    handles: Vec<T>,
    orders: Vec<usize>,
    results: &'a Vec<BatchExecuteResult>,
}

impl<'a, T: Handle> SortedHandles<'a, T> {
    #[inline]
    fn get_position_in_results(&self, pos_in_orders: usize) -> Result<(usize, usize)> {
        let mut pos = self.orders[pos_in_orders];
        for (i, result) in self.results.iter().enumerate() {
            let row_len = result.logical_rows.len();
            if pos < row_len {
                return Ok((i, result.logical_rows[pos]));
            }
            pos -= row_len
        }
        Err(other_err!("pos: {} out of bounds", pos))
    }
}

fn sort_result_handles<'a, T: Handle>(
    ctx: &mut EvalContext,
    results: &'a mut Vec<BatchExecuteResult>,
    result_schema: &[FieldType],
    handle_offsets: &[u32],
) -> Result<SortedHandles<'a, T>> {
    let mut handles = vec![];
    for result in results.iter_mut() {
        handles.reserve(handles.len() + result.logical_rows.len());
        for offset in handle_offsets {
            let offset = *offset as usize;
            let ft = &result_schema[offset];
            result.physical_columns[offset].ensure_decoded(
                ctx,
                ft,
                LogicalRows::from_slice(&result.logical_rows),
            )?;
        }

        for pos in result.logical_rows.iter() {
            handles.push(T::new(
                ctx,
                &result.physical_columns,
                result_schema,
                handle_offsets,
                *pos,
            )?);
        }
    }

    let orders = (0..handles.len())
        .sorted_by(|a, b| handles[*a].cmp(&handles[*b]))
        .collect::<Vec<_>>();

    Ok(SortedHandles {
        handles,
        orders,
        results,
    })
}
