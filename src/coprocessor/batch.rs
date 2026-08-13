// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Batched unary request collection, result merging, and response finalization.

use std::{future::Future, sync::Arc};

use ::tracker::{TokenFutureTracker, TrackerToken, track};
use futures::{
    channel::oneshot,
    future::{self, Either},
    prelude::*,
};
use kvproto::{coprocessor as coppb, kvrpcpb::CommandPri};
use resource_control::{ResourceLimiter, TaskMetadata};
use resource_metering::{FutureExt, ResourceMeteringTag};
use tikv_alloc::trace::MemoryTraceGuard;
use tikv_util::{deadline::Deadline, future::async_timeout};
use tokio::{sync::Semaphore, task::yield_now};

use super::{
    Error, HandlerOutput, HandlerOutputState, MergeableResult, ResponseMaterializationFailure,
    TracedResponse,
    endpoint::{make_error_batch_response, make_error_response},
    metrics::record_coprocessor_response_size,
};
use crate::read_pool::ReadPoolHandle;

/// A batched task response with an optional unserialized result. While the
/// result is present, `response.data` is empty; it is either merged into the
/// top task or serialized by `into_response`. The response rides in its trace
/// guard so its data stays accounted until it is attached or dropped.
pub(super) struct BatchTaskOutput {
    pub(super) response: MemoryTraceGuard<coppb::StoreBatchTaskResponse>,
    pub(super) mergeable_result: Option<Box<dyn MergeableResult>>,
}

impl BatchTaskOutput {
    /// Materializes an unmerged result, confining serialization errors to this
    /// task.
    pub(super) fn into_response(mut self) -> MemoryTraceGuard<coppb::StoreBatchTaskResponse> {
        if let Some(result) = self.mergeable_result {
            match result.into_data() {
                Ok(data) => {
                    self.response.set_data(data);
                    let data_len = self.response.get_data().len();
                    self.response.retrace(data_len);
                }
                Err(e) => make_error_batch_response(&mut self.response, e),
            }
        }
        self.response
    }
}

/// Runs batch-result merging in the read pool under the request's resource,
/// concurrency, and deadline constraints.
///
/// Results remain cancelable until the task starts, so admission failures and
/// queue timeouts do not consume them and are safe to retry.
pub(super) struct BatchMergeFinalizer {
    pub(super) read_pool: ReadPoolHandle,
    pub(super) semaphore: Option<Arc<Semaphore>>,
    pub(super) resource_tag: ResourceMeteringTag,
    pub(super) priority: CommandPri,
    pub(super) metadata: TaskMetadata<'static>,
    pub(super) resource_limiter: Option<Arc<ResourceLimiter>>,
    pub(super) deadline: Deadline,
    pub(super) task_id: u64,
}

impl BatchMergeFinalizer {
    pub(super) async fn finalize(
        self,
        output: HandlerOutput,
        batch_outputs: Vec<BatchTaskOutput>,
        tracker: TrackerToken,
    ) -> TracedResponse {
        let Self {
            read_pool,
            semaphore,
            resource_tag,
            priority,
            metadata,
            resource_limiter,
            deadline,
            task_id,
        } = self;
        // Attribute merge work and pool wait time to the top request.
        let poll_tracker = TokenFutureTracker::new(tracker);
        let merge_work = track(
            async move {
                let _permit = match &semaphore {
                    Some(semaphore) => {
                        match async_timeout(semaphore.acquire(), deadline.remaining_duration())
                            .await
                        {
                            Ok(permit) => Some(permit.expect("the semaphore never be closed")),
                            Err(_) => {
                                return make_error_response(Error::DeadlineExceeded).into();
                            }
                        }
                    }
                    None => None,
                };
                merge_batch_task_responses(output, batch_outputs, tracker, deadline).await
            },
            poll_tracker,
        )
        .in_resource_metering_tag(resource_tag);
        // Keep the merge in a shared slot so a submission failure or timeout
        // can drop it before the pool task starts. Once the pool task takes the
        // slot, it owns and executes the merge.
        let merge_slot = Arc::new(std::sync::Mutex::new(Some(merge_work)));
        let pool_merge_slot = Arc::clone(&merge_slot);
        let (response_tx, mut response_rx) = oneshot::channel();
        let pool_task = async move {
            let merge = pool_merge_slot.lock().unwrap().take();
            if let Some(merge) = merge {
                let _ = response_tx.send(merge.await);
            }
        };
        // Handler tasks already paid the memory admission cost of these results,
        // so submitting the merge must not charge them again. `submission` only
        // reports admission and enqueue; `response_rx` reports merge completion.
        let submission = read_pool.spawn(pool_task, priority, task_id, metadata, resource_limiter);
        let submission_error = match async_timeout(submission, deadline.remaining_duration()).await
        {
            Ok(Ok(())) => None,
            Ok(Err(_)) => Some(Error::MaxPendingTasksExceeded),
            Err(_) => Some(Error::DeadlineExceeded),
        };
        if let Some(error) = submission_error {
            drop(merge_slot.lock().unwrap().take());
            return make_error_response(error).into();
        }

        let completion_error =
            match async_timeout(&mut response_rx, deadline.remaining_duration()).await {
                Ok(Ok(response)) => return response,
                Ok(Err(_)) => Error::MaxPendingTasksExceeded,
                Err(_) => match response_rx.try_recv() {
                    // The response may already be ready when the timeout wins the poll race.
                    Ok(Some(response)) => return response,
                    Ok(None) => Error::DeadlineExceeded,
                    Err(_) => Error::MaxPendingTasksExceeded,
                },
            };
        drop(merge_slot.lock().unwrap().take());
        make_error_response(completion_error).into()
    }
}

/// Collects top and batch outputs concurrently, preserving batch order.
///
/// Polling batch tasks alongside the top task promptly schedules lazy read-pool
/// implementations and prevents scheduling delays that could exhaust deadlines.
pub(super) async fn collect_batch_task_outputs_concurrently(
    top_output: impl Future<Output = HandlerOutput>,
    batch_outputs: impl Stream<Item = BatchTaskOutput>,
) -> (HandlerOutput, Vec<BatchTaskOutput>) {
    let mut completed_outputs = Vec::new();
    let batch_outputs = batch_outputs.fuse();
    futures::pin_mut!(top_output, batch_outputs);
    let output = loop {
        match future::select(top_output.as_mut(), batch_outputs.next()).await {
            Either::Left((output, _)) => break output,
            Either::Right((Some(batch_output), _)) => completed_outputs.push(batch_output),
            Either::Right((None, _)) => break top_output.await,
        }
    };
    while let Some(batch_output) = batch_outputs.next().await {
        completed_outputs.push(batch_output);
    }
    (output, completed_outputs)
}

/// Collects the top output before the batch outputs.
///
/// The caller must provide lazily scheduled tasks in a sequential stream to
/// ensure that at most one read-pool task from this request is active at a
/// time.
pub(super) async fn collect_batch_task_outputs_sequentially(
    top_output: impl Future<Output = HandlerOutput>,
    batch_outputs: impl Stream<Item = BatchTaskOutput>,
) -> (HandlerOutput, Vec<BatchTaskOutput>) {
    let output = top_output.await;
    let batch_outputs = batch_outputs.collect().await;
    (output, batch_outputs)
}

/// Merges compatible batch results into the top result and serializes the rest
/// as per-task responses identified by task ID.
///
/// It yields and checks the deadline between tasks and once more after
/// top-result serialization. A timeout returns only a top-level error so every
/// task can be retried safely. Final response bytes are attributed to
/// `tracker`.
async fn merge_batch_task_responses(
    mut output: HandlerOutput,
    batch_outputs: Vec<BatchTaskOutput>,
    tracker: TrackerToken,
    deadline: Deadline,
) -> TracedResponse {
    let merge_batch_results = matches!(&output.state, HandlerOutputState::Mergeable(_))
        && !unary_response_has_error(&output.response);
    let mut batch_responses = Vec::with_capacity(batch_outputs.len());

    for mut batch_output in batch_outputs {
        if deadline.check().is_err() {
            return make_error_response(Error::DeadlineExceeded).into();
        }
        let can_merge = merge_batch_results
            && !batch_response_has_error(&batch_output.response)
            && batch_output.mergeable_result.is_some();
        if can_merge {
            debug_assert!(batch_output.response.get_data().is_empty());
            let HandlerOutputState::Mergeable(result) = &mut output.state else {
                unreachable!("batch merging requires a mergeable top result");
            };
            result.merge(batch_output.mergeable_result.take().unwrap());
            batch_output.response.set_data_merged_into_response(true);
        }
        batch_responses.push(batch_output.into_response());
        yield_now().await;
    }

    if deadline.check().is_err() {
        return make_error_response(Error::DeadlineExceeded).into();
    }
    finalize_batch_merge_response(output, batch_responses, tracker, deadline)
}

/// Materializes and commits the response produced by the batch merge path.
fn finalize_batch_merge_response(
    output: HandlerOutput,
    batch_responses: Vec<MemoryTraceGuard<coppb::StoreBatchTaskResponse>>,
    tracker: TrackerToken,
    deadline: Deadline,
) -> TracedResponse {
    let merged_batch_result = batch_responses
        .iter()
        .any(|response| response.get_data_merged_into_response());
    let batch_data_len: u64 = batch_responses
        .iter()
        .map(|response| response.get_data().len() as u64)
        .sum();
    let response = match output.into_response() {
        Ok(response) => response,
        // Once a child result has been merged it cannot be recovered for an
        // individual response. No acknowledgments or partial data are returned,
        // so a retry cannot lose or double-count results.
        Err(ResponseMaterializationFailure { error, .. }) if merged_batch_result => {
            return make_error_response(error).into();
        }
        // Without a merged child, confine serialization failure to the top task
        // and preserve its trace guard for the attached batch responses.
        Err(ResponseMaterializationFailure {
            error,
            partial_response,
        }) => (*partial_response).map(|_| make_error_response(error)),
    };
    if deadline.check().is_err() {
        return make_error_response(Error::DeadlineExceeded).into();
    }
    record_coprocessor_response_size(response.get_data().len() as u64 + batch_data_len, tracker);
    attach_batch_responses(response, batch_responses)
}

/// Attaches per-task responses and rebuilds the trace guard so the combined
/// data stays accounted until the response drops. The guard keeps the top
/// response's node, adopting the first tracked batch response's node when the
/// top is untracked (e.g. a top task error); with no tracked participant the
/// response stays untracked.
pub(super) fn attach_batch_responses(
    mut response: TracedResponse,
    batch_responses: Vec<MemoryTraceGuard<coppb::StoreBatchTaskResponse>>,
) -> TracedResponse {
    let node = response
        .trace_node()
        .or_else(|| batch_responses.iter().find_map(|r| r.trace_node()));
    // Batched tasks are clones of the top request, so every tracked
    // participant shares one node and adoption cannot misattribute memory.
    debug_assert!(node.as_ref().is_none_or(|node| {
        batch_responses
            .iter()
            .filter_map(|r| r.trace_node())
            .all(|child| Arc::ptr_eq(node, &child))
    }));
    let mut response = response.consume();
    response.set_batch_responses(
        batch_responses
            .into_iter()
            .map(|mut r| r.consume())
            .collect::<Vec<_>>()
            .into(),
    );
    let data_len = response
        .get_batch_responses()
        .iter()
        .fold(response.get_data().len(), |len, batch_response| {
            len.saturating_add(batch_response.get_data().len())
        });
    match node {
        Some(node) => node.trace_guard(response, data_len),
        None => response.into(),
    }
}

fn unary_response_has_error(resp: &coppb::Response) -> bool {
    resp.has_region_error() || resp.has_locked() || !resp.get_other_error().is_empty()
}

fn batch_response_has_error(resp: &coppb::StoreBatchTaskResponse) -> bool {
    resp.has_region_error() || resp.has_locked() || !resp.get_other_error().is_empty()
}
