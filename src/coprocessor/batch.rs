// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Batched unary request collection and response finalization.

use std::{future::Future, sync::Arc};

use futures::{
    future::{self, Either},
    prelude::*,
};
use kvproto::coprocessor as coppb;
use tikv_alloc::trace::MemoryTraceGuard;

use super::{HandlerOutput, MergeableResult, TracedResponse, endpoint::make_error_batch_response};

/// A batched task response with an optional unserialized result. While the
/// result is present, `response.data` is empty; it is serialized by
/// `into_response` unless a later finalizer consumes it. The response rides in
/// its trace guard so its data stays accounted until it is attached or dropped.
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
                    let data_capacity = data.capacity();
                    self.response.set_data(data);
                    self.response.retrace(data_capacity);
                }
                Err(e) => make_error_batch_response(&mut self.response, e),
            }
        }
        self.response
    }
}

/// Collects top and batch outputs concurrently, preserving batch order.
///
/// Batch tasks are enqueued only when polled, so polling them alongside the top
/// task prevents scheduling delays that could exhaust their deadlines.
pub(super) async fn collect_batch_task_outputs(
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

/// Attaches per-task responses and rebuilds the trace guard so the combined
/// data stays accounted until the response drops. The guard keeps the top
/// response's node, adopting the first tracked batch response's node when the
/// top is untracked (e.g. a top task error); with no tracked participant the
/// response stays untracked. Attaching nothing returns the response unchanged.
pub(super) fn attach_batch_responses(
    mut response: TracedResponse,
    batch_responses: Vec<MemoryTraceGuard<coppb::StoreBatchTaskResponse>>,
) -> TracedResponse {
    // Ordinary unary requests carry no batch tasks; keep their original guard
    // rather than rebuilding an identical one.
    if batch_responses.is_empty() {
        return response;
    }
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
    let mut data_capacity = response.mut_data().capacity();
    for batch_response in response.mut_batch_responses().iter_mut() {
        data_capacity = data_capacity.saturating_add(batch_response.mut_data().capacity());
    }
    match node {
        Some(node) => node.trace_guard(response, data_capacity),
        None => response.into(),
    }
}
