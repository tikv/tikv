// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// TODO: Maybe we can find a better place to put these interfaces, e.g. naming
// it as prelude?

//! Batch executor common structures.

use async_trait::async_trait;
pub use tidb_query_common::execute_stats::{
    ExecSummaryCollector, ExecuteStats, WithSummaryCollector,
};
use tidb_query_common::{
    execute_stats::ExecSummaryCollectorEnabled, storage::IntervalRange, Result,
};
use tidb_query_datatype::{codec::batch::LazyBatchColumnVec, expr::EvalWarnings};
use tipb::FieldType;

/// The interface for pull-based executors. It is similar to the Volcano
/// Iterator model, but pulls data in batch and stores data by column.
#[async_trait]
pub trait BatchExecutor: Send {
    type StorageStats;

    /// Gets the schema of the output.
    fn schema(&self) -> &[FieldType];

    /// Pulls next several rows of data (stored by column).
    ///
    /// This function might return zero rows, which doesn't mean that there is
    /// no more result. See `is_drained` in `BatchExecuteResult`.
    async fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult;

    /// Collects execution statistics (including but not limited to metrics and
    /// execution summaries) accumulated during execution and prepares for
    /// next collection.
    ///
    /// The executor implementation must invoke this function for each children
    /// executor. However the invocation order of children executors is not
    /// stipulated.
    ///
    /// This function may be invoked several times during execution. For each
    /// invocation, it should not contain accumulated meta data in last
    /// invocation. Normally the invocation frequency of this function is
    /// less than `next_batch()`.
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats);

    /// Collects underlying storage statistics accumulated during execution and
    /// prepares for next collection.
    ///
    /// Similar to `collect_exec_stats()`, the implementation must invoke this
    /// function for each children executor and this function may be invoked
    /// several times during execution.
    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats);

    fn take_scanned_range(&mut self) -> IntervalRange;

    fn can_be_cached(&self) -> bool;

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

#[async_trait]
impl<T: BatchExecutor + ?Sized> BatchExecutor for Box<T> {
    type StorageStats = T::StorageStats;

    fn schema(&self) -> &[FieldType] {
        (**self).schema()
    }

    async fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        (**self).next_batch(scan_rows).await
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

    fn can_be_cached(&self) -> bool {
        (**self).can_be_cached()
    }
}

#[async_trait]
impl<C: ExecSummaryCollector + Send, T: BatchExecutor> BatchExecutor
    for WithSummaryCollector<C, T>
{
    type StorageStats = T::StorageStats;

    fn schema(&self) -> &[FieldType] {
        self.inner.schema()
    }

    async fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        let timer = self.summary_collector.on_start_iterate();
        let result = self.inner.next_batch(scan_rows).await;
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

    fn can_be_cached(&self) -> bool {
        self.inner.can_be_cached()
    }
}

/// Data to be flowed between parent and child executors' single `next_batch()`
/// invocation.
///
/// Note: there are other data flow between executors, like metrics and output
/// statistics. However they are flowed at once, just before response, instead
/// of each step during execution. Hence they are not covered by this structure.
/// See `BatchExecuteMetaData`.
///
/// It is only `Send` but not `Sync` because executor returns its own data copy.
/// However `Send` enables executors to live in different threads.
///
/// It is designed to be used in new generation executors, i.e. executors
/// support batch execution. The old executors will not be refined to return
/// this kind of result.
pub struct BatchExecuteResult {
    /// The *physical* columns data generated during this invocation.
    ///
    /// Note 1: Empty column data doesn't mean that there is no more data. See
    /// `is_drained`.
    ///
    /// Note 2: This is only a *physical* store of data. The data may not be in
    /// desired order and there could be filtered out data stored inside. You
    /// should access the *logical* data via the `logical_rows` field. For the
    /// same reason, `rows_len() > 0` doesn't mean that there is logical data
    /// inside.
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
    /// - `Ok(batch_exec_is_drain)`: See the comment of `BatchExecIsDrain`.
    /// - `Err(_)`: Means that there is an error when trying to retrieve more
    ///   data. In this case, the error is returned and the executor is also
    ///   drained. Similar to `Ok(true)`, there could be some remaining data in
    ///   the `data` field which is valid data and should be processed. The
    ///   caller should NOT call `next_batch()` any more.
    pub is_drained: Result<BatchExecIsDrain>,
}

/// The result of batch execution.
/// - `Drain`: The executor is completely drained and no more data will be
///   returned in the given range.However there could be some (last) data in
///   `data` field this time. The caller should NOT call `next_batch()` any
///   more.
/// - `PagingDrain`: The executor output enough rows of the paging request,
///   there may be following data in the next paging request, the paging request
///   should be returned with scanned range in this case. Only used in paging
///   mode, Also check the last data in `data` field.
/// - `Remain`: The normal case, means that there could be more data. The caller
///   should continue calling `next_batch()` although for each call the returned
///   data may be empty.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchExecIsDrain {
    Remain,
    Drain,
    PagingDrain,
}

impl BatchExecIsDrain {
    #[inline]
    pub fn is_remain(&self) -> bool {
        *self == BatchExecIsDrain::Remain
    }

    /// the batch execution need to stop when the result status is Drain or
    /// PagingDrain, but only when we meet Drain, the resultset is really
    /// drained.
    #[inline]
    pub fn stop(&self) -> bool {
        !self.is_remain()
    }
}
