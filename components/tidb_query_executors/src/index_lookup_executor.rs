use std::{collections::HashSet, sync::Arc};

use api_version::{ApiV1, KvFormat};
use async_trait::async_trait;
use kvproto::{coprocessor::KeyRange, metapb};
use tidb_query_common::{
    execute_stats::ExecuteStats,
    storage::{IntervalRange, Storage},
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
    interface::{BatchExecIsDrain, BatchExecuteResult, BatchExecutor},
    util::scan_executor::check_columns_info_supported,
    BatchTableScanExecutor,
};

pub struct BatchIndexLookupExecutor<S: Storage, F: KvFormat> {
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
            cur_probe_index: 0,
            probe_children: children,
        })
    }
}

#[async_trait]
impl<S: Storage, F: KvFormat> BatchExecutor for BatchIndexLookupExecutor<S, F> {
    type StorageStats = S::Statistics;

    fn schema(&self) -> &[FieldType] {
        self.probe_children[0].schema()
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
            Ok(is_drain) => {
                if is_drain.stop() {
                    self.cur_probe_index += 1;
                }

                if self.cur_probe_index >= self.probe_children.len() {
                    result.is_drained = Ok(BatchExecIsDrain::Remain);
                }

                result
            }
            Err(_) => result,
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

pub fn build_index_lookup_probe_ranges<S: Storage>(
    ctx: &mut EvalContext,
    descriptor: &IndexLookup,
    build_side_results: &mut Vec<BatchExecuteResult>,
    result_schema: &[FieldType],
    locate_key: fn(key: &[u8]) -> Option<(Arc<metapb::Region>, S)>,
) -> Result<Vec<(S, Vec<KeyRange>)>> {
    let handle_offsets = descriptor.get_build_side_primary_key_offsets();
    check_int_handle_schema(result_schema, handle_offsets)?;
    let int_handles =
        sort_result_int_handles(ctx, build_side_results, result_schema, handle_offsets)?;

    let mut probe_side_ranges = vec![];
    let mut left_handles = HashSet::new();
    let mut current_region: Option<(Arc<metapb::Region>, S, Vec<KeyRange>)> = None;
    let mut last_id = 0i64;
    for (id, pos) in int_handles {
        let key = Key::from_raw(&encode_row_key(descriptor.get_table_id(), id));
        if let Some((ref region, _, ref mut ranges)) = current_region {
            if check_key_in_region(key.as_encoded(), &region) {
                if last_id == id - 1 {
                    ranges
                        .last_mut()
                        .unwrap()
                        .set_end(key.as_encoded().to_vec());
                } else {
                    let mut r = KeyRange::new();
                    r.set_start(key.as_encoded().to_vec());
                    r.set_end(r.get_start().to_vec());
                    ranges.push(r);
                }
                last_id = id;
                continue;
            } else {
                let (_, store, ranges) = current_region.take().unwrap();
                probe_side_ranges.push((store, ranges));
            }
        }

        if let Some((region, store)) = locate_key(key.as_encoded()).take() {
            let mut range = KeyRange::new();
            range.set_start(key.as_encoded().to_vec());
            range.set_end(range.get_start().to_vec());
            current_region = Some((region, store, vec![range]));
            last_id = id;
        } else {
            left_handles.insert(pos);
        }
    }

    for (i, result) in build_side_results.iter_mut().enumerate() {
        result
            .logical_rows
            .retain(|row| left_handles.contains(&(i, *row)))
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
