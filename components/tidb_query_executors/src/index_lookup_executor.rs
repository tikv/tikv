// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::{Ordering, min},
    collections::HashSet,
    mem,
    sync::Arc,
};

use api_version::{ApiV1, KvFormat};
use async_trait::async_trait;
use kvproto::{coprocessor::KeyRange, metapb};
use tidb_query_common::{
    Error,
    error::Result,
    execute_stats::{ExecSummary, ExecSummaryCollector, ExecSummaryCollectorEnabled, ExecuteStats},
    metrics::EXECUTOR_COUNT_METRICS,
    storage::{FindRegionResult, IntervalRange, RegionStorageAccessor, StateRole, Storage},
};
use tidb_query_datatype::{
    codec::{
        batch::LazyBatchColumnVec,
        data_type::{BATCH_MAX_SIZE, LogicalRows},
        table::RowHandle,
    },
    expr::{EvalConfig, EvalContext, EvalWarnings},
};
use tikv_util::{
    Either,
    Either::{Left, Right},
};
use tipb::{ColumnInfo, FieldType, IndexLookUp, TableScan};
use txn_types::Key;

use crate::{
    BatchTableScanExecutor,
    interface::{BatchExecIsDrain, BatchExecuteResult, BatchExecutor, WithSummaryCollector},
    util::scan_executor::field_type_from_column_info,
};

#[derive(Default)]
struct IndexScanState {
    results: Vec<BatchExecuteResult>,
    row_count: usize,
}

struct TableLookUpState<Iter, S: Storage, F: KvFormat> {
    table_task_iter: Option<Iter>,
    table_scan:
        Option<WithSummaryCollector<ExecSummaryCollectorEnabled, BatchTableScanExecutor<S, F>>>,
}

enum IndexLookUpPhase<Iter, S: Storage, F: KvFormat> {
    IndexScan(IndexScanState),
    TableLookUp(TableLookUpState<Iter, S, F>),
    Done,
}

impl<Iter, S: Storage, F: KvFormat> Default for IndexLookUpPhase<Iter, S, F> {
    fn default() -> Self {
        IndexLookUpPhase::IndexScan(IndexScanState::default())
    }
}

impl<Iter, S: Storage, F: KvFormat> IndexLookUpPhase<Iter, S, F> {
    fn mut_index_scan_or_err(&mut self) -> Result<&mut IndexScanState> {
        match self {
            IndexLookUpPhase::IndexScan(ref mut s) => Ok(s),
            _ => Err(other_err!("The current phase is not IndexScan")),
        }
    }

    fn mut_table_lookup_or_err(&mut self) -> Result<&mut TableLookUpState<Iter, S, F>> {
        match self {
            IndexLookUpPhase::TableLookUp(ref mut s) => Ok(s),
            _ => Err(other_err!("The current phase is not TableLookUp")),
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct TableScanParams {
    pub columns_info: Vec<ColumnInfo>,
    pub primary_column_ids: Vec<i64>,
    pub primary_prefix_column_ids: Vec<i64>,
}

#[derive(Clone, PartialEq, Debug)]
pub struct IndexLayout {
    pub handle_types: Vec<FieldType>,
    pub handle_offsets: Vec<usize>,
}

pub struct BatchIndexLookUpExecutor<S, Src, Builder, F>
where
    S: Storage,
    Src: BatchExecutor<StorageStats = S::Statistics>,
    Builder: TableTaskIterBuilder,
    F: KvFormat,
{
    src: Src,
    src_is_drained: BatchExecIsDrain,

    config: Arc<EvalConfig>,
    output_schema: Vec<FieldType>,
    force_no_index_lookup: bool,
    table_lookup_batch_size: usize,
    table_lookup_max_batch_size: usize,
    table_task_iter_builder: Option<Builder>,
    table_scan_params: TableScanParams,

    phase: IndexLookUpPhase<Builder::Iterator, S, F>,
    intermediate_results: Vec<BatchExecuteResult>,
    intermediate_channel_index: usize,
    table_scan_child_index: usize,
    table_scan_exec_summary: [ExecSummary; 1],
}

#[inline]
pub fn build_index_lookup_executor<
    S: Storage + 'static,
    Handle: RowHandle + 'static,
    F: KvFormat,
>(
    config: Arc<EvalConfig>,
    src: impl BatchExecutor<StorageStats = S::Statistics> + 'static,
    mut index_lookup: IndexLookUp,
    mut tbl_scan: TableScan,
    accessor: Option<impl RegionStorageAccessor<Storage = S> + 'static>,
    intermediate_channel_index: usize,
    table_scan_child_index: usize,
) -> Result<impl BatchExecutor<StorageStats = S::Statistics>> {
    if index_lookup.get_keep_order() {
        return Err(other_err!(
            "IndexLookupExecutor does not support keep order currently"
        ));
    }

    let src_schema = src.schema();
    let handle_offsets = index_lookup
        .take_index_handle_offsets()
        .into_iter()
        .map(|offset| offset as usize)
        .collect::<Vec<_>>();

    let handle_types = handle_offsets
        .iter()
        .map(|&offset| src_schema[offset].clone())
        .collect();

    Ok(BatchIndexLookUpExecutor::<_, _, _, F>::new(
        config,
        src,
        TableScanParams {
            columns_info: tbl_scan.take_columns().into(),
            primary_column_ids: tbl_scan.take_primary_column_ids(),
            primary_prefix_column_ids: tbl_scan.take_primary_prefix_column_ids(),
        },
        accessor.map(|acc| {
            AccessorTableTaskIterBuilder::<_, Handle>::new(
                tbl_scan.get_table_id(),
                acc,
                IndexLayout {
                    handle_types,
                    handle_offsets,
                },
            )
        }),
        intermediate_channel_index,
        table_scan_child_index,
    ))
}

// We assign a dummy type `Box<dyn Storage<Statistics = ()>>` so that we can
// omit the type when calling `check_supported`.
impl
    BatchIndexLookUpExecutor<
        Box<dyn Storage<Statistics = ()>>,
        Box<dyn BatchExecutor<StorageStats = ()>>,
        Box<dyn TableTaskIterBuilder<Iterator = ()>>,
        ApiV1,
    >
{
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(_descriptor: &IndexLookUp) -> Result<()> {
        Ok(())
    }
}

impl<S, Src, Builder, F> BatchIndexLookUpExecutor<S, Src, Builder, F>
where
    S: Storage,
    Src: BatchExecutor<StorageStats = S::Statistics>,
    Builder: TableTaskIterBuilder<Iterator: TableTaskIterator<Storage = S>>,
    F: KvFormat,
{
    #[inline]
    pub fn new(
        config: Arc<EvalConfig>,
        src: Src,
        table_scan_params: TableScanParams,
        table_task_iter_builder: Option<Builder>,
        intermediate_channel_index: usize,
        table_scan_child_index: usize,
    ) -> Self {
        let force_no_index_lookup =
            if config.paging_size.is_some() || table_task_iter_builder.is_none() {
                // We did not support index lookup when paging is enabled.
                // TODO: support paging
                // some times we do not have table_task_iter_builder, such as
                // - CommonHandle
                // TODO: support CommonHandle
                true
            } else {
                false
            };

        let output_schema = table_scan_params
            .columns_info
            .iter()
            .map(field_type_from_column_info)
            .collect::<Vec<_>>();

        BatchIndexLookUpExecutor {
            output_schema,
            config,
            force_no_index_lookup,
            table_lookup_batch_size: 0,
            table_lookup_max_batch_size: BATCH_MAX_SIZE,
            src,
            src_is_drained: BatchExecIsDrain::Remain,
            intermediate_results: vec![],
            table_task_iter_builder,
            phase: IndexLookUpPhase::default(),
            table_scan_params,
            intermediate_channel_index,
            table_scan_child_index,
            table_scan_exec_summary: [ExecSummary::default()],
        }
    }

    #[cfg(test)]
    pub fn into_index_child(self) -> Src {
        self.src
    }

    #[inline]
    pub fn config(&self) -> Arc<EvalConfig> {
        self.config.clone()
    }

    #[inline]
    pub fn get_table_scan_params(&self) -> &TableScanParams {
        &self.table_scan_params
    }

    #[inline]
    pub fn get_table_task_builder(&self) -> Option<&Builder> {
        self.table_task_iter_builder.as_ref()
    }

    #[inline]
    pub fn intermediate_channel_index(&self) -> usize {
        self.intermediate_channel_index
    }

    #[inline]
    pub fn table_scan_child_index(&self) -> usize {
        self.table_scan_child_index
    }

    #[inline]
    fn empty_result(is_drained: BatchExecIsDrain) -> BatchExecuteResult {
        BatchExecuteResult {
            physical_columns: LazyBatchColumnVec::empty(),
            logical_rows: vec![],
            warnings: EvalWarnings::default(),
            is_drained: Ok(is_drained),
        }
    }

    #[inline]
    fn err_result(err: Error) -> BatchExecuteResult {
        BatchExecuteResult {
            physical_columns: LazyBatchColumnVec::empty(),
            logical_rows: vec![],
            warnings: EvalWarnings::default(),
            is_drained: Err(err),
        }
    }

    #[inline]
    fn step_to_table_lookup(&mut self, warnings: &mut EvalWarnings) -> Result<()> {
        let state = self.phase.mut_index_scan_or_err()?;
        let builder = self.table_task_iter_builder.as_ref().ok_or(other_err!(
            "No table task builder available for index lookup"
        ))?;

        let mut ctx = EvalContext {
            cfg: self.config.clone(),
            warnings: EvalWarnings::default(),
        };

        let results = mem::take(&mut state.results);
        self.phase = IndexLookUpPhase::TableLookUp(TableLookUpState {
            table_task_iter: Some(builder.build_iterator(&mut ctx, results)?),
            table_scan: None,
        });

        if ctx.warnings.warning_cnt > 0 {
            warnings.merge(&mut ctx.warnings);
        }

        Ok(())
    }

    #[inline]
    fn finish_table_task_iter(&mut self) -> Result<Vec<BatchExecuteResult>> {
        let table_lookup = self.phase.mut_table_lookup_or_err()?;
        if let Some(iter) = table_lookup.table_task_iter.take() {
            let mut results = iter.take_left_results();
            self.intermediate_results.append(&mut results);
            Ok(results)
        } else {
            Ok(vec![])
        }
    }

    #[inline]
    fn step_to_index_scan(&mut self) -> Result<()> {
        let results = self.finish_table_task_iter()?;
        let mut index_scan = IndexScanState::default();
        // reuses results
        index_scan.results = results;
        self.phase = IndexLookUpPhase::IndexScan(index_scan);
        Ok(())
    }

    #[inline]
    fn step_to_done_from_table_lookup(&mut self) -> Result<()> {
        self.finish_table_task_iter()?;
        self.phase = IndexLookUpPhase::Done;
        Ok(())
    }

    async fn on_phase_index_scan(&mut self, scan_rows: usize) -> Result<BatchExecuteResult> {
        if self.src_is_drained.stop() {
            return Err(other_err!(
                "src is already drained, cannot continue index scan"
            ));
        }

        let state = self.phase.mut_index_scan_or_err()?;
        let mut result = self.src.next_batch(scan_rows).await;
        let src_drained = match result.is_drained {
            Ok(is_drained) if is_drained.stop() => {
                self.src_is_drained = is_drained;
                true
            }
            _ => false,
        };

        let mut output_result = Self::empty_result(BatchExecIsDrain::Remain);
        mem::swap(&mut result.is_drained, &mut output_result.is_drained);
        if result.warnings.warning_cnt > 0 {
            mem::swap(&mut result.warnings, &mut output_result.warnings);
        }
        if self.force_no_index_lookup {
            if !result.logical_rows.is_empty() {
                self.intermediate_results.push(result);
            }
            if src_drained {
                self.phase = IndexLookUpPhase::Done;
            }
            return Ok(output_result);
        }

        if !result.logical_rows.is_empty() {
            state.row_count += result.logical_rows.len();
            state.results.push(result);
        }

        if src_drained && state.results.is_empty() {
            self.phase = IndexLookUpPhase::Done;
            return Ok(output_result);
        }

        if self.table_lookup_batch_size == 0 {
            self.table_lookup_batch_size = scan_rows
        }

        if src_drained || state.row_count >= self.table_lookup_batch_size {
            if self.table_lookup_batch_size < self.table_lookup_max_batch_size {
                self.table_lookup_batch_size = min(
                    self.table_lookup_batch_size * 2,
                    self.table_lookup_max_batch_size,
                );
            }
            self.step_to_table_lookup(&mut output_result.warnings)?;
            output_result.is_drained = Ok(BatchExecIsDrain::Remain);
        }

        Ok(output_result)
    }

    async fn on_phase_table_lookup(&mut self, scan_rows: usize) -> Result<BatchExecuteResult> {
        let state = self.phase.mut_table_lookup_or_err()?;
        let executor = match state.table_scan.as_mut() {
            Some(e) => e,
            _ => {
                let table_task_iter = state
                    .table_task_iter
                    .as_mut()
                    .ok_or(other_err!("table task iter is not valid"))?;

                match table_task_iter.next().await {
                    Some(task) => {
                        EXECUTOR_COUNT_METRICS.batch_index_lookup_table_scan.inc();
                        state.table_scan = Some(
                            task.build_table_scan_executor::<F>(
                                self.config.clone(),
                                self.table_scan_params.clone(),
                            )?
                            .collect_summary(0),
                        );
                        state.table_scan.as_mut().unwrap()
                    }
                    None => {
                        if self.src_is_drained.stop() {
                            self.step_to_done_from_table_lookup()?;
                            return Ok(Self::empty_result(self.src_is_drained));
                        }
                        self.step_to_index_scan()?;
                        return Ok(Self::empty_result(BatchExecIsDrain::Remain));
                    }
                }
            }
        };

        let mut result = executor.next_batch(scan_rows).await;
        if let Ok(is_drained) = result.is_drained {
            if is_drained.stop() {
                state
                    .table_scan
                    .as_mut()
                    .unwrap()
                    .summary_collector
                    .collect(&mut self.table_scan_exec_summary);
                state.table_scan = None;
                result.is_drained = Ok(BatchExecIsDrain::Remain);
            }
        }
        Ok(result)
    }
}

#[async_trait]
impl<S, Src, Builder, F> BatchExecutor for BatchIndexLookUpExecutor<S, Src, Builder, F>
where
    S: Storage,
    Src: BatchExecutor<StorageStats = S::Statistics>,
    Builder: TableTaskIterBuilder<Iterator: TableTaskIterator<Storage = S>>,
    F: KvFormat,
{
    type StorageStats = Src::StorageStats;

    #[inline]
    fn schema(&self) -> &[FieldType] {
        &self.output_schema
    }

    // TODO: A concurrent execution implementation so that the index-scan and
    // table-lookup does not block each-other.
    #[inline]
    fn intermediate_schema(&self, index: usize) -> Result<&[FieldType]> {
        if index == self.intermediate_channel_index {
            Ok(self.src.schema())
        } else {
            self.src.intermediate_schema(index)
        }
    }

    #[inline]
    fn consume_and_fill_intermediate_results(
        &mut self,
        results: &mut [Vec<BatchExecuteResult>],
    ) -> Result<()> {
        match results.get_mut(self.intermediate_channel_index) {
            Some(v) => {
                if !self.intermediate_results.is_empty() {
                    v.append(&mut self.intermediate_results);
                }
                self.src.consume_and_fill_intermediate_results(results)
            }
            _ => Err(other_err!(
                "intermediate_channel_index {} exceeds the bound: {}",
                self.intermediate_channel_index,
                results.len()
            )),
        }
    }

    #[inline]
    async fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        if scan_rows == 0 {
            return Self::err_result(other_err!("scan_rows cannot be 0"));
        }

        let result = match self.phase {
            IndexLookUpPhase::IndexScan(..) => self.on_phase_index_scan(scan_rows).await,
            IndexLookUpPhase::TableLookUp(..) => self.on_phase_table_lookup(scan_rows).await,
            IndexLookUpPhase::Done => {
                return Self::empty_result(self.src_is_drained);
            }
        };
        result.unwrap_or_else(Self::err_result)
    }

    #[inline]
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        self.src.collect_exec_stats(dest);
        // to collect the second child TableScan's stats, we need to dump the running
        // executor's stats to `self.table_scan_exec_stats` first.
        let mut summary = mem::take(&mut self.table_scan_exec_summary);
        if let IndexLookUpPhase::TableLookUp(TableLookUpState {
            table_scan: Some(exec),
            ..
        }) = &mut self.phase
        {
            exec.summary_collector.collect(&mut summary);
        }
        dest.summary_per_executor[self.table_scan_child_index] += summary[0];
        // TODO: how to handle `scanned_rows_per_range`
    }

    #[inline]
    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        // TODO: support collecting storage stats for index lookup executors.
        self.src.collect_storage_stats(dest)
    }

    #[inline]
    fn take_scanned_range(&mut self) -> IntervalRange {
        self.src.take_scanned_range()
    }

    #[inline]
    fn can_be_cached(&self) -> bool {
        // The index lookup executor is not cacheable because it also evolves the data
        // from other regions except for the source region.
        // The cop-cache mechanism is not designed to handle this case.
        false
    }
}

/// produce table tasks for the index lookup executor
pub trait TableTaskIterBuilder: Send {
    type Iterator;
    // builds an iterator to produce table tasks
    fn build_iterator(
        &self,
        ctx: &mut EvalContext,
        results: Vec<BatchExecuteResult>,
    ) -> Result<Self::Iterator>;
}

impl<T: TableTaskIterBuilder + ?Sized> TableTaskIterBuilder for Box<T> {
    type Iterator = T::Iterator;

    #[inline]
    fn build_iterator(
        &self,
        ctx: &mut EvalContext,
        results: Vec<BatchExecuteResult>,
    ) -> Result<Self::Iterator> {
        (**self).build_iterator(ctx, results)
    }
}

pub struct AccessorTableTaskIterBuilder<Accessor, Handle>
where
    Accessor: RegionStorageAccessor,
    Handle: RowHandle,
{
    table_id: i64,
    accessor: Accessor,
    index_layout: IndexLayout,
    _phantom: std::marker::PhantomData<Handle>,
}

impl<Accessor, Handle> AccessorTableTaskIterBuilder<Accessor, Handle>
where
    Accessor: RegionStorageAccessor<Storage: Storage>,
    Handle: RowHandle,
{
    #[inline]
    pub fn new(table_id: i64, accessor: Accessor, index_layout: IndexLayout) -> Self {
        AccessorTableTaskIterBuilder {
            table_id,
            accessor,
            index_layout,
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    #[cfg(test)]
    pub fn table_id(&self) -> i64 {
        self.table_id
    }

    #[inline]
    #[cfg(test)]
    pub fn get_index_layout(&self) -> &IndexLayout {
        &self.index_layout
    }
}

impl<Accessor, Handle> TableTaskIterBuilder for AccessorTableTaskIterBuilder<Accessor, Handle>
where
    Accessor: RegionStorageAccessor<Storage: Storage>,
    Handle: RowHandle,
{
    type Iterator = AccessorTableTaskIterator<Accessor, Handle>;

    #[inline]
    fn build_iterator(
        &self,
        ctx: &mut EvalContext,
        results: Vec<BatchExecuteResult>,
    ) -> Result<Self::Iterator> {
        AccessorTableTaskIterator::new(
            self.table_id,
            ctx,
            self.accessor.clone(),
            &self.index_layout,
            results,
        )
    }
}

pub struct TableTask<S> {
    storage: S,
    key_ranges: Vec<KeyRange>,
}

impl<S: Storage> TableTask<S> {
    pub fn build_table_scan_executor<F>(
        self,
        cfg: Arc<EvalConfig>,
        TableScanParams {
            columns_info,
            primary_column_ids,
            primary_prefix_column_ids,
        }: TableScanParams,
    ) -> Result<BatchTableScanExecutor<S, F>>
    where
        F: KvFormat,
    {
        BatchTableScanExecutor::new(
            self.storage,
            cfg,
            columns_info,
            self.key_ranges,
            primary_column_ids,
            // The read order is not required for the table scans in IndexLookupExecutor.
            // Just set is_backward as false to make it easy.
            false,
            // The inner table scan in IndexLookupExecutor does not need to aware scan ranges,
            // because it will be read to drain.
            false,
            primary_prefix_column_ids,
        )
    }
}

/// used for iterating table tasks
#[async_trait]
pub trait TableTaskIterator: Send + Sized {
    type Storage: Storage;
    /// returns the next table task, or None if there are no more tasks
    async fn next(&mut self) -> Option<TableTask<Self::Storage>>;
    /// returns the left results that are not processed by the table tasks.
    fn take_left_results(self) -> Vec<BatchExecuteResult>;
}

pub struct AccessorTableTaskIterator<Accessor, Handle> {
    // Physical table ID.
    table_id: i64,
    // Used to locate the region and the snapshot storage for each handle.
    accessor: Accessor,
    // All the scanned index results.
    results: Vec<BatchExecuteResult>,
    // The handle for each index.
    // Layout: result_index => (logical_row_index => Handle)
    handles: Vec<Vec<Handle>>,
    // The handle orders in asc.
    // Layout: order => (result_index, logical_row_index)
    handle_orders: Vec<(usize, usize)>,
    // The current cursor when iterating the handles to create the table task.
    // We can use handle_orders[cursor_in_handle_orders] to get the handle position we need
    // to process next.
    cursor_in_handle_orders: usize,
    // The rows that are failed to find regions.
    // Layout: result_index => { physical_row_index => () }
    left_rows: Vec<Option<HashSet<usize>>>,
}

impl<Accessor, Handle> AccessorTableTaskIterator<Accessor, Handle>
where
    Handle: RowHandle,
    Accessor: RegionStorageAccessor,
{
    fn new(
        table_id: i64,
        ctx: &mut EvalContext,
        accessor: Accessor,
        index_layout: &IndexLayout,
        mut results: Vec<BatchExecuteResult>,
    ) -> Result<Self> {
        let mut handles = Vec::with_capacity(results.len());
        let mut orders = vec![];
        for (result_index, result) in results.iter_mut().enumerate() {
            // reserve space for the orders
            let logical_rows_len = result.logical_rows.len();
            if logical_rows_len > 0 {
                orders.reserve(logical_rows_len);
            }

            for (i, &handle_offset) in index_layout.handle_offsets.iter().enumerate() {
                let columns_len = result.physical_columns.columns_len();
                if handle_offset >= columns_len {
                    return Err(other_err!(
                        "Invalid handle offset {} for result columns len: {}",
                        handle_offset,
                        columns_len
                    ));
                }
                result.physical_columns[handle_offset].ensure_decoded(
                    ctx,
                    &index_layout.handle_types[i],
                    LogicalRows::from_slice(&result.logical_rows),
                )?
            }

            let mut result_handles = Vec::with_capacity(logical_rows_len);
            for (logical_row_index, &physical_row_index) in result.logical_rows.iter().enumerate() {
                let handle = Handle::from_lazy_batch_column_vec(
                    ctx,
                    &result.physical_columns,
                    physical_row_index,
                    &index_layout.handle_offsets,
                    &index_layout.handle_types,
                )?;
                result_handles.push(handle);
                orders.push((result_index, logical_row_index));
            }
            handles.push(result_handles);
        }

        orders.sort_by(|&a, &b| -> Ordering {
            let h1 = &handles[a.0][a.1];
            let h2 = &handles[b.0][b.1];
            h1.cmp(h2)
        });

        Ok(Self {
            table_id,
            accessor,
            results,
            handles,
            handle_orders: orders,
            cursor_in_handle_orders: 0,
            left_rows: vec![],
        })
    }

    #[inline]
    fn is_exhausted(&self) -> bool {
        self.cursor_in_handle_orders >= self.handle_orders.len()
    }

    /// Get a tuple (result_index, logical_row_index) with the index in
    /// `handle_orders`
    #[inline]
    pub fn get_row_logical_index_with_order_index(&self, order_index: usize) -> (usize, usize) {
        self.handle_orders[order_index]
    }

    #[inline]
    fn get_handle_with_order_index(&self, order_index: usize) -> &Handle {
        let (result_index, logical_index) =
            self.get_row_logical_index_with_order_index(order_index);
        &self.handles[result_index][logical_index]
    }

    #[inline]
    fn new_key_range(start: Vec<u8>, end: Vec<u8>) -> KeyRange {
        let mut range = KeyRange::new();
        range.set_start(start);
        range.set_end(end);
        range
    }

    #[inline]
    fn is_key_before_region_end(key: &[u8], region_end: &[u8]) -> bool {
        region_end.is_empty() || Key::from_raw(key).into_encoded().as_slice() < region_end
    }

    #[inline]
    fn is_key_before_or_eq_region_end(key: &[u8], region_end: &[u8]) -> bool {
        region_end.is_empty() || Key::from_raw(key).into_encoded().as_slice() <= region_end
    }

    async fn find_region_by_handle_index(
        &self,
        order_index: usize,
    ) -> Result<
        Either<
            // The region is found and is valid.
            // layout: (Region, Handle ref, point range of the handle)
            (metapb::Region, &Handle, KeyRange),
            // The region is not found or not valued
            // layout: (The invalid range end, handle)
            (Vec<u8>, &Handle),
        >,
    > {
        let handle = self.get_handle_with_order_index(order_index);
        let key = handle.encode_row_key(self.table_id);
        let result = self
            .accessor
            .find_region_by_key(Key::from_raw(&key).as_encoded())
            .await?;

        let region = match result {
            FindRegionResult::Found { region, role } => {
                if role != StateRole::Leader {
                    return Ok(Right((region.end_key, handle)));
                }
                region
            }
            FindRegionResult::NotFound { next_region_start } => {
                return Ok(Right((next_region_start.unwrap_or(vec![]), handle)));
            }
        };

        Ok(Left((
            region,
            handle,
            Self::new_key_range(key, handle.encode_point_range_end(self.table_id)),
        )))
    }

    // next_task finds the next table task
    // The return value None does not mean there is no more task, you should use
    // is_exhausted to determine whether the iterator is exhausted.
    async fn next_task(&mut self) -> Option<TableTask<Accessor::Storage>> {
        if self.is_exhausted() {
            return None;
        }

        let mut scanned = 1usize;
        let mut previous_handle;
        let mut region = Option::<metapb::Region>::None;
        let mut key_ranges = vec![];
        let invalid_end;
        let end_exclusive;
        match self
            .find_region_by_handle_index(self.cursor_in_handle_orders)
            .await
        {
            Ok(Left((r, handle, key_range))) => {
                region = Some(r);
                previous_handle = handle;
                key_ranges = vec![key_range];
                end_exclusive = region.as_ref().unwrap().get_end_key();
            }
            Ok(Right((end, handle))) => {
                invalid_end = end;
                previous_handle = handle;
                end_exclusive = invalid_end.as_slice();
            }
            Err(_) => {
                // TODO: handle error
                self.advance_orders_index(1, false);
                return None;
            }
        }

        for i in self.cursor_in_handle_orders + 1..self.handle_orders.len() {
            let handle = self.get_handle_with_order_index(i);
            let key = handle.encode_row_key(self.table_id);
            if !Self::is_key_before_region_end(&key, end_exclusive) {
                break;
            }

            if region.is_some() {
                let point_range_end = handle.encode_point_range_end(self.table_id);
                if handle.is_next_of(previous_handle) {
                    key_ranges.last_mut().unwrap().end = point_range_end;
                } else {
                    key_ranges.push(Self::new_key_range(key, point_range_end));
                }
            }

            scanned += 1;
            previous_handle = handle;
        }

        if let Some(key_range) = key_ranges.last_mut() {
            // The point_range_end may exceed the region end even if the key is in the
            // region. For example, the IntHandle has a point range end of
            // encode_row_key(v + 1), but the region end may be
            // encode_row_key(v) + b'\0' which is before the point range end. To
            // handle this case, just use the region end as the end of the key range to
            // avoid error.
            if !Self::is_key_before_or_eq_region_end(key_range.get_end(), end_exclusive) {
                key_range.end = end_exclusive.to_vec();
            }
        }

        let task = match region {
            Some(region) => {
                match self
                    .accessor
                    .get_local_region_storage(&region, &key_ranges)
                    .await
                {
                    Ok(storage) => Some(TableTask {
                        storage,
                        key_ranges,
                    }),
                    Err(_) => {
                        // TODO: handle error
                        None
                    }
                }
            }
            _ => None,
        };
        self.advance_orders_index(scanned, task.is_some());
        task
    }

    fn advance_orders_index(&mut self, count: usize, table_task_ok: bool) {
        let results_len = self.results.len();
        if count == 0 || results_len == 0 {
            return;
        }

        if !table_task_ok {
            if self.left_rows.is_empty() {
                self.left_rows.resize(results_len, None);
            }

            for i in self.cursor_in_handle_orders..self.cursor_in_handle_orders + count {
                let (result_index, logical_row_index) =
                    self.get_row_logical_index_with_order_index(i);
                let physical_row_index = self.results[result_index].logical_rows[logical_row_index];
                match &mut self.left_rows[result_index] {
                    Some(rows) => {
                        rows.insert(physical_row_index);
                    }
                    None => {
                        let mut rows = HashSet::new();
                        rows.insert(physical_row_index);
                        self.left_rows[result_index] = Some(rows);
                    }
                };
            }
        }

        self.cursor_in_handle_orders += count;
    }
}

#[async_trait]
impl<Accessor, Handle> TableTaskIterator for AccessorTableTaskIterator<Accessor, Handle>
where
    Accessor: RegionStorageAccessor<Storage: Storage>,
    Handle: RowHandle,
{
    type Storage = Accessor::Storage;
    async fn next(&mut self) -> Option<TableTask<Self::Storage>> {
        while !self.is_exhausted() {
            let task = self.next_task().await;
            if task.is_some() {
                return task;
            }
        }
        None
    }

    fn take_left_results(mut self) -> Vec<BatchExecuteResult> {
        if self.left_rows.is_empty() {
            return vec![];
        }

        for (result_index, left_physical_rows) in self.left_rows.iter().enumerate() {
            match left_physical_rows {
                Some(rows) => {
                    self.results[result_index]
                        .logical_rows
                        .retain(|&physical_row| rows.contains(&physical_row));
                }
                None => {
                    self.results[result_index].logical_rows = vec![];
                }
            }
        }

        let mut results = self.results;
        results.retain(|r| !r.logical_rows.is_empty());
        results
    }
}

#[cfg(test)]
pub mod tests {
    use std::{collections::HashSet, iter};

    use anyhow::anyhow;
    use api_version::ApiV1;
    use futures::executor::block_on;
    use kvproto::metapb::Region;
    use tidb_query_common::{
        error::StorageError,
        storage::{
            StateRole,
            test_fixture::{ErrorBuilder, FixtureStorage},
        },
    };
    use tidb_query_datatype::{
        Collation, FieldTypeAccessor, FieldTypeTp,
        codec::{
            Datum,
            batch::{LazyBatchColumn, LazyBatchColumnVec},
            data_type::VectorValue,
            datum::DatumEncoder,
            table::{IntHandle, encode_row, encode_row_key},
        },
        expr::{EvalContext, EvalWarnings, Flag},
    };

    use super::*;
    use crate::{
        interface::{BatchExecIsDrain, BatchExecuteResult},
        util::mock_executor::{MockExecutor, MockRegionStorageAccessor, MockStorage},
    };

    const TEST_TABLE_ID: i64 = 13579;

    fn get_bytes_vec_values(from: &[i64], f: impl Fn(i64) -> i64) -> VectorValue {
        let v = from
            .iter()
            .map(|&x| Some(f(x).to_string().into_bytes()))
            .collect::<Vec<_>>();
        VectorValue::Bytes(v.into())
    }

    fn get_int_raw_column(from: &[i64]) -> LazyBatchColumn {
        let mut ctx = EvalContext::default();
        let mut col = LazyBatchColumn::raw_with_capacity(3);
        for &v in from {
            let mut datum_raw = Vec::new();
            datum_raw
                .write_datum(&mut ctx, &[Datum::I64(v)], false)
                .unwrap();
            col.mut_raw().push(&datum_raw);
        }
        col
    }

    fn build_index_result(
        handles: Vec<i64>,
        logical_rows: Vec<usize>,
        layout: (&IndexLayout, &[FieldType]),
    ) -> BatchExecuteResult {
        assert_eq!(
            layout.0.handle_types[0].get_tp(),
            FieldTypeTp::Long.to_u8().unwrap() as i32
        );
        assert_eq!(layout.0.handle_offsets.len(), 1);
        assert_eq!(layout.0.handle_types.len(), 1);
        assert_eq!(
            layout.0.handle_types[0],
            layout.1[layout.0.handle_offsets[0]]
        );
        let mut dedup = HashSet::new();
        for &i in logical_rows.iter() {
            assert!(
                i < handles.len(),
                "index in logical_rows: {} should < {} which is the handles len",
                i,
                logical_rows.len()
            );
            assert!(!dedup.contains(&i), "{} id duplicated in logical_rows", i);
            dedup.insert(i);
        }
        let mut columns = Vec::with_capacity(handles.len());
        for (i, ft) in layout.1.iter().enumerate() {
            match FieldTypeTp::from_i32(ft.get_tp()).unwrap() {
                FieldTypeTp::String => {
                    columns.push(
                        get_bytes_vec_values(&handles, |x| x + ((i as i64 + 1) * 1000000)).into(),
                    );
                }
                FieldTypeTp::Long => {
                    // use raw column for the handle column to test the table task iterator
                    // should ensure the column is decoded first.
                    columns.push(get_int_raw_column(&handles));
                }
                _ => panic!("Unsupported field type: {:?}", ft),
            }
        }

        BatchExecuteResult {
            physical_columns: LazyBatchColumnVec::from(columns),
            logical_rows,
            is_drained: Ok(BatchExecIsDrain::Remain),
            warnings: EvalWarnings::default(),
        }
    }

    fn build_table_task_iter(
        accessor: MockRegionStorageAccessor,
        index_layout: IndexLayout,
        results: Vec<BatchExecuteResult>,
    ) -> AccessorTableTaskIterator<MockRegionStorageAccessor, IntHandle> {
        AccessorTableTaskIterBuilder::<MockRegionStorageAccessor, IntHandle>::new(
            TEST_TABLE_ID,
            accessor,
            index_layout,
        )
        .build_iterator(&mut EvalContext::default(), results)
        .unwrap()
    }

    fn encode_cmp_key(h: i64) -> Vec<u8> {
        Key::from_raw(&encode_row_key(TEST_TABLE_ID, h))
            .as_encoded()
            .to_vec()
    }

    fn encode_cmp_key_and_push(h: i64, b: u8) -> Vec<u8> {
        let mut raw_key = encode_row_key(TEST_TABLE_ID, h);
        raw_key.push(b);
        Key::from_raw(&raw_key).as_encoded().to_vec()
    }

    fn make_region(id: u64, start: Vec<u8>, end: Vec<u8>) -> Region {
        let mut region = Region::default();
        region.set_id(id);
        region.set_start_key(start);
        region.set_end_key(end);
        region
    }

    fn make_scan_key_range(start: i64, end: i64) -> KeyRange {
        let mut range = KeyRange::default();
        range.set_start(encode_row_key(TEST_TABLE_ID, start));
        range.set_end(encode_row_key(TEST_TABLE_ID, end));
        range
    }

    #[test]
    fn test_build_table_task_iterator() {
        let index_layout = IndexLayout {
            handle_types: vec![FieldTypeTp::Long.into()],
            handle_offsets: vec![1],
        };

        let index_scan_layout: (&IndexLayout, &[FieldType]) = (
            &index_layout,
            &[
                FieldTypeTp::String.into(),
                FieldTypeTp::Long.into(),
                FieldTypeTp::String.into(),
            ],
        );

        let iter = build_table_task_iter(
            MockRegionStorageAccessor::with_expect_mode(),
            index_layout.clone(),
            vec![
                // 5 physical rows, 4 logical rows
                build_index_result(
                    vec![1, 10, 100, 10000, 1000],
                    vec![3, 2, 4, 0],
                    index_scan_layout,
                ),
                // 4 physical rows, 4 logical rows
                build_index_result(vec![102, 11, 12, 103], vec![3, 2, 1, 0], index_scan_layout),
                // 4 physical rows, 2 logical rows
                build_index_result(vec![13, 104, 105, 14], vec![0, 3], index_scan_layout),
                // empty
                build_index_result(vec![], vec![], index_scan_layout),
            ],
        );

        assert_eq!(iter.table_id, TEST_TABLE_ID);
        assert_eq!(iter.results.len(), 4);
        assert_eq!(iter.cursor_in_handle_orders, 0);
        assert!(iter.left_rows.is_empty());
        assert_eq!(iter.results[0].logical_rows, vec![3, 2, 4, 0]);
        assert_eq!(iter.results[1].logical_rows, vec![3, 2, 1, 0]);
        assert_eq!(iter.results[2].logical_rows, vec![0, 3]);
        assert!(iter.results[3].logical_rows.is_empty());
        // Some function's return should order it by the handle value
        // `get_row_logical_index_with_order_index` should be ordered and return the
        // logical position
        // `get_handle_with_order_index` should be ordered and return the handle value.
        let mut orders = Vec::with_capacity(iter.handle_orders.len());
        for i in 0..iter.handle_orders.len() {
            let item = (
                iter.get_row_logical_index_with_order_index(i),
                iter.get_handle_with_order_index(i).clone(),
            );
            assert_eq!(iter.handle_orders[i], item.0);
            orders.push(item);
        }
        assert_eq!(
            orders,
            vec![
                ((0, 3), IntHandle::from(1)),
                ((1, 2), IntHandle::from(11)),
                ((1, 1), IntHandle::from(12)),
                ((2, 0), IntHandle::from(13)),
                ((2, 1), IntHandle::from(14)),
                ((0, 1), IntHandle::from(100)),
                ((1, 3), IntHandle::from(102)),
                ((1, 0), IntHandle::from(103)),
                ((0, 2), IntHandle::from(1000)),
                ((0, 0), IntHandle::from(10000)),
            ]
        );
    }

    #[test]
    fn test_table_task_iterator_next_task() {
        let index_layout = IndexLayout {
            handle_types: vec![FieldTypeTp::Long.into()],
            handle_offsets: vec![1],
        };
        let index_scan_layout: (&IndexLayout, &[FieldType]) = (
            &index_layout,
            &[FieldTypeTp::String.into(), FieldTypeTp::Long.into()],
        );

        let mut iter = build_table_task_iter(
            MockRegionStorageAccessor::with_expect_mode(),
            index_layout.clone(),
            // order: [6, 11, 12, 13, 14, 15, 19, 50, 60, 61, 63, 70, 71, 100, 101, 200, 300]
            // 22 is mark as removed
            vec![
                build_index_result(vec![6], vec![0], index_scan_layout),
                build_index_result(
                    vec![
                        13, 11, 50, 70, 12, 14, 60, 22, 15, 19, 61, 63, 300, 101, 100, 71, 200,
                    ],
                    vec![4, 6, 13, 3, 2, 12, 5, 1, 0, 16, 14, 15, 8, 10, 9, 11],
                    index_scan_layout,
                ),
            ],
        );
        let orig_logical_rows = iter.results[1].logical_rows.clone();

        // leader found, the ranges include both point range and normal range.
        iter.accessor.assert_no_exceptions();
        let region = make_region(1, b"".to_vec(), encode_cmp_key(13));
        iter.accessor
            .expect_find_region(encode_cmp_key(6), region.clone(), StateRole::Leader);
        iter.accessor.expect_get_local_region_storage(true);
        let task = block_on(iter.next_task()).unwrap();
        let expected_ranges = vec![make_scan_key_range(6, 7), make_scan_key_range(11, 13)];
        assert_eq!(task.storage, MockStorage(region, expected_ranges.clone()));
        assert_eq!(task.key_ranges, expected_ranges);
        assert_eq!(iter.cursor_in_handle_orders, 3);
        assert!(iter.left_rows.is_empty());

        // leader found, in this case, the key 14's point range is across the region
        // end, like:
        // |----------- region -----------|
        //          |key(13) -- key(14)----- point_range_end(14)|
        // We still need to add 14 to this region but should set the end of the key
        // range to the region end.
        // We get the key range: [key(13), region_end)
        iter.accessor.assert_no_exceptions();
        let region = make_region(2, encode_cmp_key(13), encode_cmp_key_and_push(14, 0u8));
        iter.accessor
            .expect_find_region(encode_cmp_key(13), region.clone(), StateRole::Leader);
        iter.accessor.expect_get_local_region_storage(true);
        let task = block_on(iter.next_task()).unwrap();
        let mut expected_ranges = vec![make_scan_key_range(13, 15)];
        // The end key of the range should be the region end because the point range end
        // of handle 14 exceeds the region.
        expected_ranges.last_mut().unwrap().end = region.get_end_key().to_vec();
        assert_eq!(task.storage, MockStorage(region, expected_ranges.clone()));
        assert_eq!(task.key_ranges, expected_ranges);
        assert_eq!(iter.cursor_in_handle_orders, 5);
        assert!(iter.left_rows.is_empty());

        // leader found, in this case, the region start exceeds the point range
        // end of 14 but still before point range start of 15:
        // |------- region --------|
        //               |key(15)----- point_range_end(15)|
        // We need to get one range with [key(15), region_end)
        iter.accessor.assert_no_exceptions();
        let region = make_region(
            3,
            encode_cmp_key_and_push(14, 0u8),
            encode_cmp_key_and_push(15, 0u8),
        );
        iter.accessor
            .expect_find_region(encode_cmp_key(15), region.clone(), StateRole::Leader);
        iter.accessor.expect_get_local_region_storage(true);
        let task = block_on(iter.next_task()).unwrap();
        let mut expected_range = make_scan_key_range(15, 16);
        // The end key of the range should be the region end because the point range end
        // of handle 15 exceeds the region.
        expected_range.set_end(region.get_end_key().to_vec());
        let expected_ranges = vec![expected_range];
        assert_eq!(task.storage, MockStorage(region, expected_ranges.clone()));
        assert_eq!(task.key_ranges, expected_ranges.clone());
        assert_eq!(iter.cursor_in_handle_orders, 6);
        assert!(iter.left_rows.is_empty());

        // region found, but not leader
        iter.accessor.assert_no_exceptions();
        let region = make_region(4, encode_cmp_key(17), encode_cmp_key(52));
        iter.accessor
            .expect_find_region(encode_cmp_key(19), region.clone(), StateRole::Follower);
        assert!(block_on(iter.next_task()).is_none());
        assert_eq!(iter.cursor_in_handle_orders, 8);
        // left rows should be filled with the left physical row indexes
        assert!(iter.left_rows[0].is_none());
        let left_rows = iter.left_rows[1].as_ref().unwrap();
        assert_eq!(left_rows.len(), 2);
        assert!(left_rows.contains(&9));
        assert!(left_rows.contains(&2));

        // region not found
        iter.accessor.assert_no_exceptions();
        iter.accessor
            .expect_region_not_found(encode_cmp_key(60), Some(encode_cmp_key(63)));
        assert!(block_on(iter.next_task()).is_none());
        assert_eq!(iter.cursor_in_handle_orders, 10);
        assert!(iter.left_rows[0].is_none());
        let left_rows = iter.left_rows[1].as_ref().unwrap();
        assert_eq!(left_rows.len(), 4);
        assert!(left_rows.contains(&6));
        assert!(left_rows.contains(&10));

        // region found, but get get storage error
        iter.accessor.assert_no_exceptions();
        let region = make_region(5, encode_cmp_key(63), encode_cmp_key(101));
        iter.accessor
            .expect_find_region(encode_cmp_key(63), region, StateRole::Leader);
        iter.accessor.expect_get_local_region_storage(false);
        assert!(block_on(iter.next_task()).is_none());
        assert_eq!(iter.cursor_in_handle_orders, 14);
        assert!(iter.left_rows[0].is_none());
        let left_rows = iter.left_rows[1].as_ref().unwrap();
        assert_eq!(left_rows.len(), 8);
        assert!(left_rows.contains(&11));
        assert!(left_rows.contains(&3));
        assert!(left_rows.contains(&15));
        assert!(left_rows.contains(&14));

        // find region error
        iter.accessor.assert_no_exceptions();
        iter.accessor.expect_find_region_error(encode_cmp_key(101));
        assert!(block_on(iter.next_task()).is_none());
        assert_eq!(iter.cursor_in_handle_orders, 15);
        assert!(iter.left_rows[0].is_none());
        let left_rows = iter.left_rows[1].as_ref().unwrap();
        assert_eq!(left_rows.len(), 9);
        assert!(left_rows.contains(&13));

        // find reset regions with a region end ""
        iter.accessor.assert_no_exceptions();
        let region = make_region(6, encode_cmp_key(120), vec![]);
        iter.accessor
            .expect_find_region(encode_cmp_key(200), region.clone(), StateRole::Leader);
        iter.accessor.expect_get_local_region_storage(true);
        let task = block_on(iter.next_task()).unwrap();
        let expected_ranges = vec![make_scan_key_range(200, 201), make_scan_key_range(300, 301)];
        assert_eq!(task.storage, MockStorage(region, expected_ranges.clone()));
        assert_eq!(task.key_ranges, expected_ranges);
        assert_eq!(iter.cursor_in_handle_orders, 17);
        assert!(iter.left_rows[0].is_none());
        assert_eq!(iter.left_rows[1].as_ref().unwrap().len(), 9);

        // The iterator exhausted, call seek_once again should return None
        iter.accessor.assert_no_exceptions();
        assert!(iter.is_exhausted());
        assert!(block_on(iter.next_task()).is_none());
        assert_eq!(iter.cursor_in_handle_orders, 17);
        let left_results = iter.take_left_results();
        assert_eq!(left_results.len(), 1);
        let mut expected_logical_rows = orig_logical_rows.clone();
        expected_logical_rows.retain(|&i| [9, 2, 6, 10, 11, 3, 15, 14, 13].contains(&(i)));
        assert_eq!(left_results[0].logical_rows, expected_logical_rows);
    }

    #[test]
    fn test_table_task_iterator_next() {
        let index_layout = IndexLayout {
            handle_types: vec![FieldTypeTp::Long.into()],
            handle_offsets: vec![0],
        };
        let index_scan_layout: (&IndexLayout, &[FieldType]) =
            (&index_layout, &[FieldTypeTp::Long.into()]);

        let regions = vec![
            (
                make_region(1, encode_cmp_key(100), encode_cmp_key(200)),
                StateRole::Leader,
            ),
            (
                make_region(2, encode_cmp_key(300), encode_cmp_key(400)),
                StateRole::Leader,
            ),
            (
                make_region(3, encode_cmp_key(500), encode_cmp_key(600)),
                StateRole::Leader,
            ),
            (
                make_region(3, encode_cmp_key(700), encode_cmp_key(800)),
                StateRole::Leader,
            ),
            (
                make_region(3, encode_cmp_key(900), encode_cmp_key(1000)),
                StateRole::Leader,
            ),
        ];

        let handles = vec![
            1, 2, 3, 200, 201, 202, 500, 501, 502, 600, 601, 800, 801, 1000, 1001,
        ];

        let logical_rows = (0..handles.len()).collect::<Vec<_>>();
        let mut iter = build_table_task_iter(
            MockRegionStorageAccessor::with_regions_data(regions.clone()),
            index_layout.clone(),
            vec![build_index_result(
                handles.clone(),
                logical_rows.clone(),
                index_scan_layout,
            )],
        );

        // first next returns a task
        let task = block_on(iter.next()).unwrap();
        let expected_ranges = vec![make_scan_key_range(500, 503)];
        assert_eq!(
            task.storage,
            MockStorage(regions[2].0.clone(), expected_ranges.clone())
        );
        assert_eq!(task.key_ranges, expected_ranges);

        // second next exhausted
        assert!(block_on(iter.next()).is_none());
        assert!(iter.is_exhausted());

        // next when exhausted does nothing
        assert!(block_on(iter.next()).is_none());
        assert!(iter.is_exhausted());

        // check left rows
        let mut expected_logical_rows = logical_rows.clone();
        expected_logical_rows.retain(|&i| handles[i] < 500 || handles[i] >= 503);
        assert_eq!(
            iter.take_left_results()[0].logical_rows,
            expected_logical_rows
        )
    }

    #[test]
    fn test_table_task_iterator_take_left_results() {
        let index_layout = IndexLayout {
            handle_types: vec![FieldTypeTp::Long.into()],
            handle_offsets: vec![0],
        };
        let index_scan_layout: (&IndexLayout, &[FieldType]) =
            (&index_layout, &[FieldTypeTp::Long.into()]);

        // no result left
        let mut iter = build_table_task_iter(
            MockRegionStorageAccessor::with_regions_data(vec![(
                make_region(1, vec![], vec![]),
                StateRole::Leader,
            )]),
            index_layout.clone(),
            vec![
                build_index_result((0..10).collect(), (0..10).collect(), index_scan_layout),
                build_index_result((10..20).collect(), (0..10).collect(), index_scan_layout),
            ],
        );
        assert!(block_on(iter.next()).is_some());
        assert!(iter.is_exhausted());
        assert!(iter.left_rows.is_empty());
        assert!(iter.take_left_results().is_empty());

        // some rows left
        let r2 = build_index_result((10..20).collect(), (0..10).collect(), index_scan_layout);
        let r4 = build_index_result((30..40).collect(), (0..10).collect(), index_scan_layout);
        let mut iter = build_table_task_iter(
            MockRegionStorageAccessor::with_regions_data(vec![
                (
                    make_region(1, encode_cmp_key(5), encode_cmp_key(10)),
                    StateRole::Leader,
                ),
                (
                    make_region(2, encode_cmp_key(11), vec![]),
                    StateRole::Leader,
                ),
            ]),
            index_layout.clone(),
            vec![
                build_index_result((0..10).collect(), (0..10).collect(), index_scan_layout),
                r2,
                build_index_result((20..30).collect(), (0..10).collect(), index_scan_layout),
                r4,
            ],
        );
        assert!(block_on(iter.next()).is_some());
        assert!(block_on(iter.next()).is_some());
        assert!(iter.is_exhausted());
        let left_results = iter.take_left_results();
        assert_eq!(left_results.len(), 2);
        let r = left_results.first().unwrap();
        assert_eq!(r.logical_rows, vec![0, 1, 2, 3, 4]);
        let r = left_results.get(1).unwrap();
        assert_eq!(r.logical_rows, vec![0]);
    }

    fn int_handle_table_columns(types: Vec<FieldTypeTp>, handle_index: usize) -> Vec<ColumnInfo> {
        assert!(handle_index < types.len());
        let mut columns = Vec::with_capacity(types.len());
        for (i, &tp) in types.iter().enumerate() {
            let mut ci = ColumnInfo::default();
            if i == handle_index {
                assert_eq!(tp, FieldTypeTp::LongLong);
                ci.set_pk_handle(i == 0);
            }
            ci.as_mut_accessor().set_tp(tp);
            ci.set_column_id(i as i64 + 1);
            if tp == FieldTypeTp::String {
                ci.set_flen(64);
                ci.as_mut_accessor()
                    .set_collation(Collation::Utf8Mb4GeneralCi);
            }
            columns.push(ci);
        }
        columns
    }

    #[test]
    fn test_table_task_build_table_scan_executor() {
        let columns_info =
            int_handle_table_columns(vec![FieldTypeTp::LongLong, FieldTypeTp::Long], 0);
        let mut ctx = EvalContext::default();
        let storage = FixtureStorage::from(vec![
            (
                encode_row_key(TEST_TABLE_ID, 10),
                encode_row(&mut ctx, vec![Datum::I64(10), Datum::I64(100)], &[1, 2]).unwrap(),
            ),
            (
                encode_row_key(TEST_TABLE_ID, 11),
                encode_row(&mut ctx, vec![Datum::I64(11), Datum::I64(101)], &[1, 2]).unwrap(),
            ),
            (
                encode_row_key(TEST_TABLE_ID, 20),
                encode_row(&mut ctx, vec![Datum::I64(20), Datum::I64(200)], &[1, 2]).unwrap(),
            ),
            (
                encode_row_key(TEST_TABLE_ID, 21),
                encode_row(&mut ctx, vec![Datum::I64(21), Datum::I64(201)], &[1, 2]).unwrap(),
            ),
            (
                encode_row_key(TEST_TABLE_ID, 22),
                encode_row(&mut ctx, vec![Datum::I64(22), Datum::I64(202)], &[1, 2]).unwrap(),
            ),
        ]);
        let key_ranges = vec![make_scan_key_range(10, 11), make_scan_key_range(20, 22)];

        let task = TableTask {
            storage,
            key_ranges,
        };

        let expected_schema = columns_info
            .iter()
            .map(field_type_from_column_info)
            .collect::<Vec<_>>();

        let cfg = Arc::new(EvalConfig::default_for_test());
        let mut executor = task
            .build_table_scan_executor::<ApiV1>(
                cfg.clone(),
                TableScanParams {
                    columns_info,
                    primary_column_ids: vec![],
                    primary_prefix_column_ids: vec![],
                },
            )
            .unwrap();

        assert_eq!(executor.schema(), expected_schema);
        let mut result = block_on(executor.next_batch(128));
        check_batch_execute_result(
            &mut result,
            vec![
                vec![Datum::I64(10), Datum::I64(20), Datum::I64(21)],
                vec![Datum::I64(100), Datum::I64(200), Datum::I64(201)],
            ],
            BatchExecIsDrain::Drain,
            0,
        );
    }

    struct MockTableTaskIterator {
        source_results: Vec<BatchExecuteResult>,
        expect_next: Option<Option<TableTask<FixtureStorage>>>,
        expect_take_left_results: Option<Vec<BatchExecuteResult>>,
    }

    impl MockTableTaskIterator {
        fn expect_next_task_err_storage(&mut self) {
            let key_ranges = vec![make_scan_key_range(i64::MIN, i64::MAX)];
            let row_key = encode_row_key(TEST_TABLE_ID, 1);
            let err_builder: ErrorBuilder =
                Box::new(|| StorageError::from(anyhow!("mock storage error")));

            self.expect_next = Some(Some(TableTask {
                storage: FixtureStorage::new(iter::once((row_key, Err(err_builder))).collect()),
                key_ranges,
            }));
        }

        fn expect_next_task(
            &mut self,
            columns_info: Vec<ColumnInfo>,
            columns: Vec<Vec<Datum>>,
            key_ranges: Vec<KeyRange>,
        ) {
            let pk_index = columns_info
                .iter()
                .enumerate()
                .find(|(_, info)| info.get_pk_handle())
                .unwrap()
                .0;
            let col_ids = columns_info
                .iter()
                .map(|ci| ci.get_column_id())
                .collect::<Vec<_>>();

            let mut ctx = EvalContext::default();
            let mut rows = Vec::with_capacity(columns[0].len());
            for (i, d) in columns[pk_index].iter().enumerate() {
                let h = d.as_int().unwrap().unwrap();
                let key = encode_row_key(TEST_TABLE_ID, h);
                let row_vals = columns.iter().map(|col| col[i].clone()).collect::<Vec<_>>();
                let value = encode_row(&mut ctx, row_vals, col_ids.as_slice()).unwrap();
                rows.push((key, value));
            }

            self.expect_next = Some(Some(TableTask {
                storage: FixtureStorage::from(rows),
                key_ranges,
            }));
        }

        fn expect_next_none(&mut self) {
            self.expect_next = Some(None);
        }

        fn expect_take_left_results(&mut self, results: Vec<BatchExecuteResult>) {
            self.expect_take_left_results = Some(results);
        }
    }

    #[async_trait]
    impl TableTaskIterator for MockTableTaskIterator {
        type Storage = FixtureStorage;

        async fn next(&mut self) -> Option<TableTask<Self::Storage>> {
            self.expect_next.take().unwrap()
        }

        fn take_left_results(self) -> Vec<BatchExecuteResult> {
            self.expect_take_left_results.unwrap()
        }
    }

    struct MockTableTaskIterBuilder;

    impl TableTaskIterBuilder for MockTableTaskIterBuilder {
        type Iterator = MockTableTaskIterator;
        fn build_iterator(
            &self,
            ctx: &mut EvalContext,
            results: Vec<BatchExecuteResult>,
        ) -> Result<Self::Iterator> {
            if ctx.cfg.flag.contains(Flag::TRUNCATE_AS_WARNING) {
                // for testing purpose, we append a warning
                ctx.warnings
                    .append_warning(tidb_query_datatype::codec::Error::truncated());
            }
            Ok(MockTableTaskIterator {
                source_results: results,
                expect_next: None,
                expect_take_left_results: None,
            })
        }
    }

    fn build_int_array_results(int_results: Vec<Vec<i64>>) -> Vec<BatchExecuteResult> {
        let index_layout = IndexLayout {
            handle_types: vec![FieldType::from(FieldTypeTp::Long)],
            handle_offsets: vec![0],
        };
        let index_scan_layout: (&IndexLayout, &[FieldType]) =
            (&index_layout, &[FieldTypeTp::Long.into()]);
        let mut results = int_results
            .iter()
            .map(|row| build_index_result(row.clone(), (0..row.len()).collect(), index_scan_layout))
            .collect::<Vec<_>>();

        if let Some(r) = results.last_mut() {
            r.is_drained = Ok(BatchExecIsDrain::Drain);
        }
        results
    }

    fn new_index_lookup_executor_for_test(
        cfg: EvalConfig,
        src_results: Vec<BatchExecuteResult>,
        builder: Option<MockTableTaskIterBuilder>,
        table_columns_info: Vec<ColumnInfo>,
    ) -> BatchIndexLookUpExecutor<FixtureStorage, MockExecutor, MockTableTaskIterBuilder, ApiV1>
    {
        let src = MockExecutor::new(vec![FieldType::from(FieldTypeTp::Long)], src_results);
        BatchIndexLookUpExecutor::new(
            Arc::new(cfg),
            src,
            TableScanParams {
                columns_info: table_columns_info,
                primary_column_ids: vec![],
                primary_prefix_column_ids: vec![],
            },
            builder,
            2,
            3,
        )
    }

    fn check_batch_execute_result(
        result: &mut BatchExecuteResult,
        expected: Vec<Vec<Datum>>,
        is_drained: BatchExecIsDrain,
        warning_cnt: usize,
    ) {
        assert_eq!(*result.is_drained.as_ref().unwrap(), is_drained);
        assert_eq!(result.warnings.warning_cnt, warning_cnt);
        for i in 0..result.physical_columns.columns_len() {
            let col = &mut result.physical_columns[i];
            col.ensure_decoded(
                &mut EvalContext::default(),
                &FieldTypeTp::Long.into(),
                LogicalRows::from_slice(&result.logical_rows),
            )
            .unwrap();

            let expect = &expected[i];
            match expect[0] {
                Datum::I64(_) => {
                    let expect_i64 = expect
                        .iter()
                        .map(|d| d.as_int().unwrap().unwrap())
                        .collect::<Vec<_>>();
                    let values = col
                        .decoded()
                        .to_int_vec()
                        .iter()
                        .map(|v| v.unwrap())
                        .collect::<Vec<_>>();
                    assert_eq!(values, expect_i64);
                }
                Datum::Bytes(_) => {
                    let expect_bs = expect
                        .iter()
                        .map(|d| d.as_string().unwrap().unwrap().to_vec())
                        .collect::<Vec<_>>();
                    let values = col
                        .decoded()
                        .to_bytes_vec()
                        .iter()
                        .map(|v| v.as_ref().unwrap().to_vec())
                        .collect::<Vec<_>>();
                    assert_eq!(values, expect_bs);
                }
                _ => panic!("Unsupported datum type in expected result"),
            }
            assert_eq!(*result.is_drained.as_ref().unwrap(), is_drained);
            assert_eq!(result.warnings.warning_cnt, warning_cnt);
        }
    }

    fn check_int_column_batch_execute_result(
        result: &mut BatchExecuteResult,
        expected: Vec<i64>,
        is_drained: BatchExecIsDrain,
        warning_cnt: usize,
    ) {
        check_batch_execute_result(
            result,
            vec![expected.iter().map(|v| Datum::I64(*v)).collect::<Vec<_>>()],
            is_drained,
            warning_cnt,
        );
    }

    #[test]
    fn test_index_lookup_executor_on_index_scan_phase() {
        fn get_index_scan_phase_batch(
            e: &mut BatchIndexLookUpExecutor<
                FixtureStorage,
                MockExecutor,
                MockTableTaskIterBuilder,
                ApiV1,
            >,
        ) -> (&mut Vec<BatchExecuteResult>, usize) {
            let index_scan = e.phase.mut_index_scan_or_err().unwrap();
            (&mut index_scan.results, index_scan.row_count)
        }

        // Drain case, and the result is empty, enter Done directly.
        for is_drain in [BatchExecIsDrain::Drain, BatchExecIsDrain::PagingDrain] {
            let mut src_results = build_int_array_results(vec![vec![]]);
            src_results[0].warnings.warning_cnt = 10;
            src_results[0].is_drained = Ok(is_drain);
            let mut index_lookup = new_index_lookup_executor_for_test(
                EvalConfig::default_for_test(),
                src_results,
                Some(MockTableTaskIterBuilder),
                int_handle_table_columns(vec![FieldTypeTp::LongLong, FieldTypeTp::String], 0),
            );
            // The first phase should be IndexScan, and it should be empty.
            let index_scan = index_lookup.phase.mut_index_scan_or_err().unwrap();
            assert_eq!(index_scan.row_count, 0);
            assert!(index_scan.results.is_empty());
            let mut r = block_on(index_lookup.next_batch(128));
            check_int_column_batch_execute_result(&mut r, vec![], is_drain, 10);
            match index_lookup.phase {
                IndexLookUpPhase::Done => {
                    // next_batch when Done phase.
                    // should return an empty result with the is_drain.
                    let r = block_on(index_lookup.next_batch(128));
                    assert!(r.logical_rows.is_empty());
                    assert_eq!(r.is_drained.unwrap(), is_drain);
                }
                _ => {
                    panic!("expect phase to be Done");
                }
            }
        }

        let mut src_results = build_int_array_results(vec![
            // index 0, empty result, warning_cnt: 1
            vec![],
            // index 1, result with 5 rows, warning_cnt: 2
            vec![1, 3, 5, 7, 9],
            // index 2, result with 2 rows
            vec![10, 11],
            // index 3, error case
            vec![],
            // index 4, trigger batch full
            vec![100],
            // index 5, 6 continues to add results
            vec![1000, 1001],
            vec![2000],
            // index 7, drain case
            vec![],
        ]);
        src_results[0].warnings.warning_cnt = 1;
        src_results[1].warnings.warning_cnt = 2;
        src_results[3].is_drained = Err(other_err!("mock error"));

        let mut cfg = EvalConfig::default_for_test();
        // set TRUNCATE_AS_WARNING to test warning when build table task iterator
        cfg.flag.set(Flag::TRUNCATE_AS_WARNING, true);
        let mut index_lookup = new_index_lookup_executor_for_test(
            cfg,
            src_results,
            Some(MockTableTaskIterBuilder),
            int_handle_table_columns(vec![FieldTypeTp::LongLong, FieldTypeTp::String], 0),
        );

        // scan_rows == 0 not allowed
        let err = block_on(index_lookup.next_batch(0)).is_drained.unwrap_err();
        assert!(err.to_string().contains("scan_rows cannot be 0"));

        // The first phase should be IndexScan, and it should be empty.
        let index_state = index_lookup.phase.mut_index_scan_or_err().unwrap();
        assert_eq!(index_state.row_count, 0);
        assert!(index_state.results.is_empty());
        assert_eq!(index_lookup.table_lookup_batch_size, 0);

        // if the result is empty, it should be return an empty result with the warnings
        // without adding the result to the batch.
        let mut r = block_on(index_lookup.next_batch(128));
        check_int_column_batch_execute_result(&mut r, vec![], BatchExecIsDrain::Remain, 1);
        let (batch, batch_cnt) = get_index_scan_phase_batch(&mut index_lookup);
        assert!(batch.is_empty());
        assert_eq!(batch_cnt, 0);

        // the first result will set the batch size as the first scan rows
        assert_eq!(index_lookup.table_lookup_batch_size, 128);
        assert_eq!(index_lookup.table_lookup_max_batch_size, BATCH_MAX_SIZE);

        // if the result contains some rows, it should be added to the batch.
        let mut r = block_on(index_lookup.next_batch(128));
        check_int_column_batch_execute_result(&mut r, vec![], BatchExecIsDrain::Remain, 2);
        let (batch, batch_cnt) = get_index_scan_phase_batch(&mut index_lookup);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch_cnt, 5);
        check_int_column_batch_execute_result(
            &mut batch[0],
            vec![1, 3, 5, 7, 9],
            BatchExecIsDrain::Remain,
            0,
        );

        // another result
        let mut r = block_on(index_lookup.next_batch(128));
        check_int_column_batch_execute_result(&mut r, vec![], BatchExecIsDrain::Remain, 0);
        let (batch, batch_cnt) = get_index_scan_phase_batch(&mut index_lookup);
        assert_eq!(batch.len(), 2);
        assert_eq!(batch_cnt, 7);
        check_int_column_batch_execute_result(
            &mut batch[1],
            vec![10, 11],
            BatchExecIsDrain::Remain,
            0,
        );

        // error case, should output an error
        let r = block_on(index_lookup.next_batch(128));
        let err = r.is_drained.unwrap_err();
        assert!(err.to_string().contains("mock error"));
        let (batch, batch_cnt) = get_index_scan_phase_batch(&mut index_lookup);
        assert_eq!(batch.len(), 2);
        assert_eq!(batch_cnt, 7);

        // if lookup phase not triggered, the index_lookup_batch_size should not change
        assert_eq!(index_lookup.table_lookup_batch_size, 128);

        // trigger batch full, step to TableLookup phase
        index_lookup.table_lookup_batch_size = batch_cnt;
        let mut r = block_on(index_lookup.next_batch(128));
        // the mock MockTableTaskIterBuilder::build_iterator will produce 1 test warn.
        check_int_column_batch_execute_result(&mut r, vec![], BatchExecIsDrain::Remain, 1);
        let table_lookup = index_lookup.phase.mut_table_lookup_or_err().unwrap();
        assert!(table_lookup.table_scan.is_none());
        let batch = &mut table_lookup
            .table_task_iter
            .as_mut()
            .unwrap()
            .source_results;
        assert_eq!(batch.len(), 3);
        check_int_column_batch_execute_result(
            &mut batch[2],
            vec![100],
            BatchExecIsDrain::Remain,
            0,
        );

        // index_lookup_batch_size should grow after lookup phase not triggered
        let batch_size = batch_cnt * 2;
        assert_eq!(index_lookup.table_lookup_batch_size, batch_size);

        // set index_lookup.table_lookup_batch_max_size to a small value to test growing
        let max_batch_size = batch_size + 2;
        assert!(max_batch_size < batch_size * 2);
        index_lookup.table_lookup_max_batch_size = max_batch_size;

        // go back to IndexLookUp and add more results
        index_lookup.phase = IndexLookUpPhase::IndexScan(IndexScanState::default());
        assert_eq!(index_lookup.src_is_drained, BatchExecIsDrain::Remain);
        let mut r = block_on(index_lookup.next_batch(128));
        check_int_column_batch_execute_result(&mut r, vec![], BatchExecIsDrain::Remain, 0);
        r = block_on(index_lookup.next_batch(128));
        check_int_column_batch_execute_result(&mut r, vec![], BatchExecIsDrain::Remain, 0);
        let (batch, batch_cnt) = get_index_scan_phase_batch(&mut index_lookup);
        assert_eq!(batch.len(), 2);
        assert_eq!(batch_cnt, 3);
        check_int_column_batch_execute_result(
            &mut batch[0],
            vec![1000, 1001],
            BatchExecIsDrain::Remain,
            0,
        );
        check_int_column_batch_execute_result(
            &mut batch[1],
            vec![2000],
            BatchExecIsDrain::Remain,
            0,
        );

        // should enter TableLookup phase because src is drained
        assert_eq!(index_lookup.src_is_drained, BatchExecIsDrain::Remain);
        let mut r = block_on(index_lookup.next_batch(128));
        check_int_column_batch_execute_result(&mut r, vec![], BatchExecIsDrain::Remain, 1);
        let table_lookup = index_lookup.phase.mut_table_lookup_or_err().unwrap();
        assert!(table_lookup.table_scan.is_none());
        let batch = &mut table_lookup
            .table_task_iter
            .as_mut()
            .unwrap()
            .source_results;
        assert_eq!(batch.len(), 2);
        check_int_column_batch_execute_result(
            &mut batch[0],
            vec![1000, 1001],
            BatchExecIsDrain::Remain,
            0,
        );
        check_int_column_batch_execute_result(
            &mut batch[1],
            vec![2000],
            BatchExecIsDrain::Remain,
            0,
        );
        assert_eq!(index_lookup.src_is_drained, BatchExecIsDrain::Drain);

        // the index_lookup_batch_size should grow to the max value
        assert_eq!(index_lookup.table_lookup_batch_size, max_batch_size);

        // PagingDrain should also trigger the TableLookup phase
        let mut src_results = build_int_array_results(vec![vec![99, 101]]);
        src_results[0].is_drained = Ok(BatchExecIsDrain::PagingDrain);
        let mut index_lookup = new_index_lookup_executor_for_test(
            EvalConfig::default_for_test(),
            src_results,
            Some(MockTableTaskIterBuilder),
            int_handle_table_columns(vec![FieldTypeTp::LongLong, FieldTypeTp::String], 0),
        );
        let mut r = block_on(index_lookup.next_batch(128));
        check_int_column_batch_execute_result(&mut r, vec![], BatchExecIsDrain::Remain, 0);
        let table_lookup = index_lookup.phase.mut_table_lookup_or_err().unwrap();
        assert!(table_lookup.table_scan.is_none());
        let batch = &mut table_lookup
            .table_task_iter
            .as_mut()
            .unwrap()
            .source_results;
        assert_eq!(batch.len(), 1);
        check_int_column_batch_execute_result(
            &mut batch[0],
            vec![99, 101],
            BatchExecIsDrain::Remain,
            0,
        );
        assert_eq!(index_lookup.src_is_drained, BatchExecIsDrain::PagingDrain);
    }

    #[test]
    fn test_index_lookup_executor_on_table_lookup_phase() {
        let src_results = build_int_array_results(vec![vec![1, 2, 3, 4], vec![2]]);
        let columns = int_handle_table_columns(vec![FieldTypeTp::LongLong], 0);
        let mut index_lookup = new_index_lookup_executor_for_test(
            EvalConfig::default_for_test(),
            src_results,
            Some(MockTableTaskIterBuilder),
            columns.clone(),
        );

        // make sure the index lookup executor is in TableLookup phase
        index_lookup.table_lookup_batch_size = 2;
        block_on(index_lookup.next_batch(4)).is_drained.unwrap();
        assert_eq!(index_lookup.src_is_drained, BatchExecIsDrain::Remain);
        let state = index_lookup.phase.mut_table_lookup_or_err().unwrap();
        assert!(state.table_scan.is_none());

        let full_ranges = vec![make_scan_key_range(i64::MIN, i64::MAX)];
        // When TableScan executor is none, build executor first and then call
        // next_batch
        assert!(state.table_scan.is_none());
        let iter = state.table_task_iter.as_mut().unwrap();
        iter.expect_next_task(
            columns.clone(),
            vec![vec![
                Datum::I64(1),
                Datum::I64(2),
                Datum::I64(3),
                Datum::I64(4),
                Datum::I64(5),
            ]],
            full_ranges.clone(),
        );
        let mut r = block_on(index_lookup.next_batch(2));
        check_batch_execute_result(
            &mut r,
            vec![vec![Datum::I64(1), Datum::I64(2)]],
            BatchExecIsDrain::Remain,
            0,
        );
        let state = index_lookup.phase.mut_table_lookup_or_err().unwrap();
        assert!(state.table_scan.is_some());

        // After TableScan executor is created, next_batch will continue return
        // the result from the iterator.
        let mut r = block_on(index_lookup.next_batch(2));
        check_batch_execute_result(
            &mut r,
            vec![vec![Datum::I64(3), Datum::I64(4)]],
            BatchExecIsDrain::Remain,
            0,
        );
        let state = index_lookup.phase.mut_table_lookup_or_err().unwrap();
        assert!(state.table_scan.is_some());

        // After TableScan drained, the executor should be released and the
        // return result.is_drained should not stop because the task
        // iterator is still not drained.
        let mut r = block_on(index_lookup.next_batch(2));
        check_batch_execute_result(
            &mut r,
            vec![vec![Datum::I64(5)]],
            BatchExecIsDrain::Remain,
            0,
        );
        let state = index_lookup.phase.mut_table_lookup_or_err().unwrap();
        assert!(state.table_scan.is_none());

        // the next task, the table scan executor is drained after the first
        // next_batch.
        let state = index_lookup.phase.mut_table_lookup_or_err().unwrap();
        let iter = state.table_task_iter.as_mut().unwrap();
        iter.expect_next_task(
            columns.clone(),
            vec![vec![Datum::I64(10)]],
            full_ranges.clone(),
        );
        let mut r = block_on(index_lookup.next_batch(2));
        check_batch_execute_result(
            &mut r,
            vec![vec![Datum::I64(10)]],
            BatchExecIsDrain::Remain,
            0,
        );
        let state = index_lookup.phase.mut_table_lookup_or_err().unwrap();
        assert!(state.table_scan.is_none());

        // iter exhausted
        let index_layout = IndexLayout {
            handle_types: vec![FieldTypeTp::Long.into()],
            handle_offsets: vec![0],
        };
        let index_scan_layout: (&IndexLayout, &[FieldType]) =
            (&index_layout, &[FieldTypeTp::Long.into()]);
        let state = index_lookup.phase.mut_table_lookup_or_err().unwrap();
        let iter = state.table_task_iter.as_mut().unwrap();
        iter.expect_next_none();
        iter.expect_take_left_results(vec![
            build_index_result(vec![7, 8], vec![0, 1], index_scan_layout),
            build_index_result(vec![3, 4], vec![0, 1], index_scan_layout),
        ]);
        assert!(index_lookup.intermediate_results.is_empty());
        let r = block_on(index_lookup.next_batch(2));
        assert_eq!(r.is_drained.unwrap(), BatchExecIsDrain::Remain);
        assert_eq!(index_lookup.intermediate_results.len(), 2);
        check_batch_execute_result(
            &mut index_lookup.intermediate_results[0],
            vec![vec![Datum::I64(7), Datum::I64(8)]],
            BatchExecIsDrain::Remain,
            0,
        );
        check_batch_execute_result(
            &mut index_lookup.intermediate_results[1],
            vec![vec![Datum::I64(3), Datum::I64(4)]],
            BatchExecIsDrain::Remain,
            0,
        );

        // continue to next batch
        let index_scan = index_lookup.phase.mut_index_scan_or_err().unwrap();
        assert_eq!(index_scan.results.capacity(), 2); // reuse results vec
        assert_eq!(index_scan.row_count, 0);
        let r = block_on(index_lookup.next_batch(2));
        assert_eq!(r.is_drained.unwrap(), BatchExecIsDrain::Remain);
        assert_eq!(index_lookup.src_is_drained, BatchExecIsDrain::Drain);
        let state = index_lookup.phase.mut_table_lookup_or_err().unwrap();
        let iter = state.table_task_iter.as_mut().unwrap();
        iter.expect_next_none();
        iter.expect_take_left_results(vec![build_index_result(
            vec![200, 300],
            vec![0, 1],
            index_scan_layout,
        )]);
        let mut r = block_on(index_lookup.next_batch(2));
        check_batch_execute_result(&mut r, vec![], BatchExecIsDrain::Drain, 0);
        assert!(matches!(index_lookup.phase, IndexLookUpPhase::Done));
        assert_eq!(index_lookup.intermediate_results.len(), 3);
        check_batch_execute_result(
            &mut index_lookup.intermediate_results[2],
            vec![vec![Datum::I64(200), Datum::I64(300)]],
            BatchExecIsDrain::Remain,
            0,
        );

        // test iter exhausted when PageDrain
        let mut src_results = build_int_array_results(vec![vec![1]]);
        src_results[0].is_drained = Ok(BatchExecIsDrain::PagingDrain);
        let mut index_lookup = new_index_lookup_executor_for_test(
            EvalConfig::default_for_test(),
            src_results,
            Some(MockTableTaskIterBuilder),
            columns.clone(),
        );
        let r = block_on(index_lookup.next_batch(128)).is_drained.unwrap();
        assert_eq!(r, BatchExecIsDrain::Remain);
        assert_eq!(index_lookup.src_is_drained, BatchExecIsDrain::PagingDrain);
        let state = index_lookup.phase.mut_table_lookup_or_err().unwrap();
        let iter = state.table_task_iter.as_mut().unwrap();
        assert!(state.table_scan.is_none());
        iter.expect_next_none();
        iter.expect_take_left_results(vec![build_index_result(
            vec![100, 101],
            vec![0, 1],
            index_scan_layout,
        )]);
        let mut r = block_on(index_lookup.next_batch(2));
        check_batch_execute_result(&mut r, vec![], BatchExecIsDrain::PagingDrain, 0);
        assert!(matches!(index_lookup.phase, IndexLookUpPhase::Done));
        assert_eq!(index_lookup.intermediate_results.len(), 1);
        check_batch_execute_result(
            &mut index_lookup.intermediate_results[0],
            vec![vec![Datum::I64(100), Datum::I64(101)]],
            BatchExecIsDrain::Remain,
            0,
        );

        // error case
        let src_results = build_int_array_results(vec![vec![1]]);
        let mut index_lookup = new_index_lookup_executor_for_test(
            EvalConfig::default_for_test(),
            src_results,
            Some(MockTableTaskIterBuilder),
            columns.clone(),
        );
        block_on(index_lookup.next_batch(128)).is_drained.unwrap();
        let state = index_lookup.phase.mut_table_lookup_or_err().unwrap();
        let iter = state.table_task_iter.as_mut().unwrap();
        iter.expect_next_task_err_storage();
        let r = block_on(index_lookup.next_batch(2));
        assert!(
            r.is_drained
                .unwrap_err()
                .to_string()
                .contains("mock storage error")
        );
    }

    #[test]
    fn test_force_no_lookup() {
        let columns = int_handle_table_columns(vec![FieldTypeTp::LongLong], 0);
        // default should not enable force_no_index_lookup
        let index_lookup = new_index_lookup_executor_for_test(
            EvalConfig::default_for_test(),
            build_int_array_results(vec![vec![1]]),
            Some(MockTableTaskIterBuilder),
            columns.clone(),
        );
        assert!(!index_lookup.force_no_index_lookup);

        // does not support paging currently
        let mut cfg = EvalConfig::default_for_test();
        cfg.paging_size = Some(128);
        let index_lookup = new_index_lookup_executor_for_test(
            cfg,
            build_int_array_results(vec![vec![1]]),
            Some(MockTableTaskIterBuilder),
            columns.clone(),
        );
        assert!(index_lookup.force_no_index_lookup);

        // When table task builder is not provided, disable index lookup
        let mut results = build_int_array_results(vec![vec![123, 456], vec![], vec![789]]);
        results[1].is_drained = Err(other_err!("mock error"));
        let mut index_lookup = new_index_lookup_executor_for_test(
            EvalConfig::default_for_test(),
            results,
            None,
            columns.clone(),
        );
        assert!(index_lookup.force_no_index_lookup);

        // when force_no_index_lookup the results should into intermediate_results
        // directly
        let mut r = block_on(index_lookup.next_batch(128));
        check_batch_execute_result(&mut r, vec![], BatchExecIsDrain::Remain, 0);
        let r = block_on(index_lookup.next_batch(128));
        assert!(r.is_drained.unwrap_err().to_string().contains("mock error"));
        let mut r = block_on(index_lookup.next_batch(128));
        check_batch_execute_result(&mut r, vec![], BatchExecIsDrain::Drain, 0);
        assert_eq!(index_lookup.intermediate_results.len(), 2);
        check_batch_execute_result(
            &mut index_lookup.intermediate_results[0],
            vec![vec![Datum::I64(123), Datum::I64(456)]],
            BatchExecIsDrain::Remain,
            0,
        );
        check_batch_execute_result(
            &mut index_lookup.intermediate_results[1],
            vec![vec![Datum::I64(789)]],
            BatchExecIsDrain::Remain,
            0,
        );
    }

    #[test]
    fn test_consume_and_fill_intermediate_results() {
        let mut index_lookup = new_index_lookup_executor_for_test(
            EvalConfig::default_for_test(),
            build_int_array_results(vec![vec![]]),
            None,
            int_handle_table_columns(vec![FieldTypeTp::LongLong], 0),
        );

        assert_eq!(index_lookup.intermediate_channel_index, 2);
        assert!(index_lookup.intermediate_results.is_empty());

        index_lookup.intermediate_results = build_int_array_results(vec![vec![1, 2], vec![3]]);

        // when the channel len does not match, return error
        let err = index_lookup
            .consume_and_fill_intermediate_results(&mut [vec![], vec![]])
            .unwrap_err();
        assert!(err.to_string().contains("exceeds the bound"));

        // take results successfully
        let mut channels = iter::repeat_with(Vec::new).take(4).collect::<Vec<_>>();
        index_lookup
            .consume_and_fill_intermediate_results(&mut channels)
            .unwrap();
        assert!(index_lookup.intermediate_results.is_empty());
        assert!(channels[0].is_empty());
        assert!(channels[1].is_empty());
        assert!(channels[3].is_empty());
        let results = &mut channels[2];
        assert_eq!(results.len(), 2);
        check_int_column_batch_execute_result(
            &mut results[0],
            vec![1, 2],
            BatchExecIsDrain::Remain,
            0,
        );
        check_int_column_batch_execute_result(&mut results[1], vec![3], BatchExecIsDrain::Drain, 0);

        // take results should append to existing results
        index_lookup.intermediate_results = build_int_array_results(vec![vec![7, 4]]);
        index_lookup
            .consume_and_fill_intermediate_results(&mut channels)
            .unwrap();
        assert!(index_lookup.intermediate_results.is_empty());
        assert!(channels[0].is_empty());
        assert!(channels[1].is_empty());
        assert!(channels[3].is_empty());
        let results = &mut channels[2];
        assert_eq!(results.len(), 3);
        check_int_column_batch_execute_result(
            &mut results[2],
            vec![7, 4],
            BatchExecIsDrain::Drain,
            0,
        );

        // take results should also take the child executor's intermediate
        // results
        index_lookup.src.intermediate_schema = Some((1, vec![FieldType::from(FieldTypeTp::Long)]));
        index_lookup
            .src
            .set_next_intermediate_results(build_int_array_results(vec![vec![100]]));
        index_lookup
            .consume_and_fill_intermediate_results(&mut channels)
            .unwrap();
        assert!(channels[0].is_empty());
        assert!(channels[3].is_empty());
        assert_eq!(channels[2].len(), 3);
        assert_eq!(channels[1].len(), 1);
        let results = &mut channels[1];
        assert_eq!(results.len(), 1);
        check_int_column_batch_execute_result(
            &mut results[0],
            vec![100],
            BatchExecIsDrain::Drain,
            0,
        );

        index_lookup.intermediate_results = build_int_array_results(vec![vec![300]]);
        index_lookup
            .src
            .set_next_intermediate_results(build_int_array_results(vec![vec![200]]));
        index_lookup
            .consume_and_fill_intermediate_results(&mut channels)
            .unwrap();
        assert!(channels[0].is_empty());
        assert!(channels[3].is_empty());
        let results = &mut channels[2];
        assert_eq!(results.len(), 4);
        check_int_column_batch_execute_result(
            &mut results[3],
            vec![300],
            BatchExecIsDrain::Drain,
            0,
        );
        let results = &mut channels[1];
        assert_eq!(results.len(), 2);
        check_int_column_batch_execute_result(
            &mut results[1],
            vec![200],
            BatchExecIsDrain::Drain,
            0,
        );
    }

    #[test]
    fn test_intermediate_schema() {
        let mut index_lookup = new_index_lookup_executor_for_test(
            EvalConfig::default_for_test(),
            build_int_array_results(vec![vec![]]),
            None,
            int_handle_table_columns(vec![FieldTypeTp::LongLong], 0),
        );

        let schema1 = vec![
            FieldType::from(FieldTypeTp::Long),
            FieldType::from(FieldTypeTp::VarChar),
        ];
        let schema2 = vec![FieldType::from(FieldTypeTp::Long)];

        assert_eq!(index_lookup.intermediate_channel_index, 2);
        index_lookup.src.schema = schema1.clone();
        index_lookup.src.intermediate_schema = Some((1, schema2.clone()));

        // test missing intermediate schema
        assert!(
            index_lookup
                .intermediate_schema(0)
                .unwrap_err()
                .to_string()
                .contains("no intermediate schema for index")
        );

        // test index lookup's intermediate schema
        assert_eq!(schema1, index_lookup.intermediate_schema(2).unwrap());

        // test index lookup's child's intermediate schema
        assert_eq!(schema2, index_lookup.intermediate_schema(1).unwrap());
    }

    #[test]
    fn test_collect_table_scan_summary() {
        let columns = int_handle_table_columns(vec![FieldTypeTp::LongLong], 0);
        let src_results = build_int_array_results(vec![vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]);
        let mut index_lookup = new_index_lookup_executor_for_test(
            EvalConfig::default_for_test(),
            src_results,
            Some(MockTableTaskIterBuilder),
            int_handle_table_columns(vec![FieldTypeTp::LongLong, FieldTypeTp::String], 0),
        );

        // Test the below case
        // 1. run next_batch to trigger table scan phase
        // 2. table scan phase loops twice, the first time exhausts the first task,
        // and the second time consumes 3 rows without exhausting the second task.
        // 3. collect the summary info
        assert_eq!(
            BatchExecIsDrain::Remain,
            block_on(index_lookup.next_batch(128)).is_drained.unwrap()
        );
        let s = index_lookup.phase.mut_table_lookup_or_err().unwrap();
        s.table_task_iter.as_mut().unwrap().expect_next_task(
            columns.clone(),
            vec![vec![
                Datum::I64(1),
                Datum::I64(2),
                Datum::I64(3),
                Datum::I64(4),
                Datum::I64(5),
            ]],
            vec![make_scan_key_range(i64::MIN, i64::MAX)],
        );
        assert_eq!(
            BatchExecIsDrain::Remain,
            block_on(index_lookup.next_batch(128)).is_drained.unwrap()
        );
        let s = index_lookup.phase.mut_table_lookup_or_err().unwrap();
        s.table_task_iter.as_mut().unwrap().expect_next_task(
            columns.clone(),
            vec![vec![
                Datum::I64(6),
                Datum::I64(7),
                Datum::I64(8),
                Datum::I64(9),
                Datum::I64(10),
            ]],
            vec![make_scan_key_range(i64::MIN, i64::MAX)],
        );
        assert_eq!(
            BatchExecIsDrain::Remain,
            block_on(index_lookup.next_batch(2)).is_drained.unwrap()
        );

        // check summary
        let mut stats = ExecuteStats::new(index_lookup.table_scan_child_index + 2);
        index_lookup.collect_exec_stats(&mut stats);
        let summary = stats.summary_per_executor[index_lookup.table_scan_child_index];
        assert_eq!(7, summary.num_produced_rows);
        assert_eq!(2, summary.num_iterations);
    }
}
