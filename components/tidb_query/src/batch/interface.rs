// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// TODO: Maybe we can find a better place to put these interfaces, e.g. naming it as prelude?

//! Batch executor common structures.

pub use super::super::execute_stats::{ExecSummaryCollector, ExecuteStats, WithSummaryCollector};

use tipb::FieldType;

use crate::codec::batch::LazyBatchColumnVec;
use crate::execute_stats::ExecSummaryCollectorEnabled;
use crate::expr::EvalWarnings;
use crate::storage::IntervalRange;
use crate::Result;

/// The interface for pull-based executors. It is similar to the Volcano Iterator model, but
/// pulls data in batch and stores data by column.
pub trait BatchExecutor: Send {
    type StorageStats;

    /// Gets the schema of the output.
    fn schema(&self) -> &[FieldType];

    /// Pulls next several rows of data (stored by column).
    ///
    /// This function might return zero rows, which doesn't mean that there is no more result.
    /// See `is_drained` in `BatchExecuteResult`.
    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult;

    /// Collects execution statistics (including but not limited to metrics and execution summaries)
    /// accumulated during execution and prepares for next collection.
    ///
    /// The executor implementation must invoke this function for each children executor. However
    /// the invocation order of children executors is not stipulated.
    ///
    /// This function may be invoked several times during execution. For each invocation, it should
    /// not contain accumulated meta data in last invocation. Normally the invocation frequency of
    /// this function is less than `next_batch()`.
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats);

    /// Collects underlying storage statistics accumulated during execution and prepares for
    /// next collection.
    ///
    /// Similar to `collect_exec_stats()`, the implementation must invoke this function for each
    /// children executor and this function may be invoked several times during execution.
    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats);

    fn take_scanned_range(&mut self) -> IntervalRange;

    fn collect_summary(
        self,
        output_index: usize,
    ) -> WithSummaryCollector<ExecSummaryCollectorEnabled, Self>
    where
        Self: Sized,
    {
        WithSummaryCollector {
            summary_collector: ExecSummaryCollectorEnabled::new(output_index),
            inner: self,
        }
    }
}

impl<T: BatchExecutor + ?Sized> BatchExecutor for Box<T> {
    type StorageStats = T::StorageStats;

    fn schema(&self) -> &[FieldType] {
        (**self).schema()
    }

    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        (**self).next_batch(scan_rows)
    }

    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        (**self).collect_exec_stats(dest);
    }

    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        (**self).collect_storage_stats(dest);
    }

    fn take_scanned_range(&mut self) -> IntervalRange {
        (**self).take_scanned_range()
    }
}

impl<C: ExecSummaryCollector + Send, T: BatchExecutor> BatchExecutor
    for WithSummaryCollector<C, T>
{
    type StorageStats = T::StorageStats;

    fn schema(&self) -> &[FieldType] {
        self.inner.schema()
    }

    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        let timer = self.summary_collector.on_start_iterate();
        let result = self.inner.next_batch(scan_rows);
        self.summary_collector
            .on_finish_iterate(timer, result.logical_rows.len());
        result
    }

    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        self.summary_collector
            .collect(&mut dest.summary_per_executor);
        self.inner.collect_exec_stats(dest);
    }

    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        self.inner.collect_storage_stats(dest);
    }

    fn take_scanned_range(&mut self) -> IntervalRange {
        self.inner.take_scanned_range()
    }
}

/// Data to be flowed between parent and child executors' single `next_batch()` invocation.
///
/// Note: there are other data flow between executors, like metrics and output statistics.
/// However they are flowed at once, just before response, instead of each step during execution.
/// Hence they are not covered by this structure. See `BatchExecuteMetaData`.
///
/// It is only `Send` but not `Sync` because executor returns its own data copy. However `Send`
/// enables executors to live in different threads.
///
/// It is designed to be used in new generation executors, i.e. executors support batch execution.
/// The old executors will not be refined to return this kind of result.
pub struct BatchExecuteResult {
    /// The *physical* columns data generated during this invocation.
    ///
    /// Note 1: Empty column data doesn't mean that there is no more data. See `is_drained`.
    ///
    /// Note 2: This is only a *physical* store of data. The data may not be in desired order and
    ///         there could be filtered out data stored inside. You should access the *logical*
    ///         data via the `logical_rows` field. For the same reason, `rows_len() > 0` doesn't
    ///         mean that there is logical data inside.
    pub physical_columns: LazyBatchColumnVec,

    /// Valid row offsets in `physical_columns`, placed in the logical order.
    pub logical_rows: Vec<usize>,

    /// The warnings generated during this invocation.
    // TODO: It can be more general, e.g. `ExecuteWarnings` instead of `EvalWarnings`.
    // TODO: Should be recorded by row.
    pub warnings: EvalWarnings,

    /// Whether or not there is no more data.
    ///
    /// This structure is a `Result`. When it is:
    /// - `Ok(false)`: The normal case, means that there could be more data. The caller should
    ///                continue calling `next_batch()` although for each call the returned data may
    ///                be empty.
    /// - `Ok(true)`:  Means that the executor is drained and no more data will be returned in
    ///                future. However there could be some (last) data in the `data` field this
    ///                time. The caller should NOT call `next_batch()` any more.
    /// - `Err(_)`:    Means that there is an error when trying to retrieve more data. In this case,
    ///                the error is returned and the executor is also drained. Similar to
    ///                `Ok(true)`, there could be some remaining data in the `data` field which is
    ///                valid data and should be processed. The caller should NOT call `next_batch()`
    ///                any more.
    pub is_drained: Result<bool>,
}
