// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
// TODO: Maybe we can find a better place to put these interfaces, e.g. naming it as prelude?

//! Batch executor common structures.

pub use super::statistics::BatchExecuteStatistics;

use tipb::expression::FieldType;

use crate::coprocessor::codec::batch::LazyBatchColumnVec;
use crate::coprocessor::dag::expr::EvalWarnings;
use crate::coprocessor::Error;

/// The interface for pull-based executors. It is similar to the Volcano Iterator model, but
/// pulls data in batch and stores data by column.
pub trait BatchExecutor: Send {
    /// Gets the schema of the output.
    ///
    /// Provides an `Arc` instead of a pure reference to make it possible to share this schema in
    /// multiple executors. Actually the schema is only possible to be shared in executors, but it
    /// requires the ability to store a reference to a field in the same structure. There are other
    /// solutions so far but looks like `Arc` is the simplest one.
    fn schema(&self) -> &[FieldType];

    /// Pulls next several rows of data (stored by column).
    ///
    /// This function might return zero rows, which doesn't mean that there is no more result.
    /// See `is_drained` in `BatchExecuteResult`.
    fn next_batch(&mut self, expect_rows: usize) -> BatchExecuteResult;

    /// Collects statistics (including but not limited to metrics and execution summaries)
    /// accumulated during execution and prepares for next collection.
    ///
    /// The executor implementation must invoke this function for each children executor. However
    /// the invocation order of children executors is not stipulated.
    ///
    /// This function may be invoked several times during execution. For each invocation, it should
    /// not contain accumulated meta data in last invocation. Normally the invocation frequency of
    /// this function is less than `next_batch()`.
    fn collect_statistics(&mut self, destination: &mut BatchExecuteStatistics);
}

impl<T: BatchExecutor + ?Sized> BatchExecutor for Box<T> {
    fn schema(&self) -> &[FieldType] {
        (**self).schema()
    }

    fn next_batch(&mut self, expect_rows: usize) -> BatchExecuteResult {
        (**self).next_batch(expect_rows)
    }

    fn collect_statistics(&mut self, destination: &mut BatchExecuteStatistics) {
        (**self).collect_statistics(destination)
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
    /// The columns data generated during this invocation. Note that empty column data doesn't mean
    /// that there is no more data. See `is_drained`.
    pub data: LazyBatchColumnVec,

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
    // TODO: The name of this field is confusing and not obvious, that we need so many comments to
    // explain what it is. We can change it to a better name or use an enum if necessary.
    pub is_drained: Result<bool, Error>,
}
