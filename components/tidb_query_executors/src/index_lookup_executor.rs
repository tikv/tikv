use std::{collections::HashSet, sync::Arc};

use api_version::{ApiV1, KvFormat};
use async_trait::async_trait;
use kvproto::coprocessor::KeyRange;
use tidb_query_common::{
    execute_stats::ExecuteStats,
    storage::{IntervalRange, Storage},
    util::convert_to_prefix_next,
    Result,
};
use tidb_query_datatype::{
    codec::{batch::LazyBatchColumnVec, data_type::LogicalRows, table::encode_row_key},
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

pub async fn build_index_lookup_probe_ranges<S: Storage>(
    ctx: &mut EvalContext,
    descriptor: &IndexLookup,
    build_side_results: &mut Vec<BatchExecuteResult>,
    result_schema: &[FieldType],
    locate_key: &FnLocateRegionKey<S>,
) -> Result<Vec<(S, Vec<KeyRange>)>> {
    let handle_offsets = descriptor.get_build_side_primary_key_offsets();
    check_int_handle_schema(result_schema, handle_offsets)?;
    let int_handles =
        sort_result_int_handles(ctx, build_side_results, result_schema, handle_offsets)?;

    let mut probe_side_ranges = vec![];
    let mut left_handles = HashSet::<(usize, usize)>::new();
    let mut current_region: Option<(AsyncRegion<S>, Vec<KeyRange>, (usize, usize))> = None;
    let mut last_id = 0i64;
    let handles_len = int_handles.len();
    for i in 0..=handles_len {
        let mut region_end = None;
        if i < handles_len {
            let (id, _) = int_handles[i];
            let raw_key = encode_row_key(descriptor.get_table_id(), id);
            let key = Key::from_raw(&raw_key);
            match current_region {
                Some((ref region, ref mut ranges, ref mut slice))
                    if check_key_in_region(key.as_encoded(), region.get_region()) =>
                {
                    if last_id == id - 1 {
                        let last_range = ranges.last_mut().unwrap();
                        last_range.set_end(raw_key);
                        convert_to_prefix_next(last_range.mut_end());
                    } else {
                        let mut r = KeyRange::new();
                        r.set_start(raw_key);
                        r.set_end(r.get_start().to_vec());
                        convert_to_prefix_next(r.mut_end());
                        ranges.push(r);
                    }
                    slice.1 += i;
                    last_id = id;
                }
                _ => {
                    region_end = current_region.take();
                    if let Some(region) = locate_key(key.as_encoded()).take() {
                        let mut r = KeyRange::new();
                        r.set_start(raw_key);
                        r.set_end(r.get_start().to_vec());
                        convert_to_prefix_next(r.mut_end());
                        current_region = Some((region, vec![r], (i, i)));
                        last_id = id;
                    }
                }
            }
        } else {
            region_end = current_region.take();
        }

        if let Some((region, ranges, slice)) = region_end {
            let store = region.get_region_store(&ranges).await;
            if store.is_some() {
                probe_side_ranges.push((store.unwrap(), ranges));
            } else {
                for (_, pos) in &int_handles[slice.0..=slice.1] {
                    left_handles.insert((pos.0, pos.1));
                }
            }
        }
    }

    let no_left_handles = left_handles.is_empty();
    for (i, result) in build_side_results.iter_mut().enumerate() {
        if no_left_handles {
            result.logical_rows = vec![];
        } else {
            result
                .logical_rows
                .retain(|row| left_handles.contains(&(i, *row)))
        }
    }

    Ok(probe_side_ranges)
}

fn sort_result_int_handles(
    ctx: &mut EvalContext,
    results: &mut Vec<BatchExecuteResult>,
    result_schema: &[FieldType],
    handle_offsets: &[u32],
) -> Result<Vec<(i64, (usize, usize))>> {
    let (offset, ft) = check_int_handle_schema(result_schema, handle_offsets)?;
    let mut handles = vec![];
    for (i, result) in results.iter_mut().enumerate() {
        result.physical_columns[offset].ensure_decoded(
            ctx,
            ft,
            LogicalRows::from_slice(&result.logical_rows),
        )?;

        handles.extend(
            result.physical_columns[offset]
                .decoded()
                .to_int_vec()
                .iter()
                .enumerate()
                .filter(|(_, &h)| h.is_some())
                .map(|(j, h)| (h.unwrap(), (i, j))),
        );
    }

    handles.sort();
    Ok(handles)
}

fn check_int_handle_schema<'a>(
    schema: &'a [FieldType],
    handle_offsets: &'a [u32],
) -> Result<(usize, &'a FieldType)> {
    if handle_offsets.len() > 1 {
        return Err(other_err!("common handle not supported yet"));
    }

    let offset = handle_offsets[0] as usize;
    let ft = &schema[offset];
    match FieldTypeTp::from_i32(ft.get_tp()) {
        Some(
            FieldTypeTp::Long | FieldTypeTp::LongLong | FieldTypeTp::Tiny | FieldTypeTp::Short,
        ) => Ok((offset, ft)),
        _ => Err(other_err!("common handle not supported yet")),
    }
}
