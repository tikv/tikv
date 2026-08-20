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

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        sync::{
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        task::Poll,
        thread,
        time::{Duration, Instant},
    };

    use ::tracker::{GLOBAL_TRACKERS, RequestInfo, RequestType, Tracker, TrackerToken};
    use file_system::IoBytes;
    use futures::{StreamExt, channel::oneshot, executor::block_on, future, stream};
    use kvproto::kvrpcpb;
    use raftstore::store::{ReadStats, WriteStats};
    use resource_control::{ResourceGroupManager, ResourceLimiter};
    use resource_metering::ResourceTagFactory;
    use tikv_alloc::trace::{MemoryTrace, MemoryTraceGuard};
    use tikv_util::yatp_pool::CleanupMethod;

    use super::*;
    use crate::{
        config::{CoprReadPoolConfig, UnifiedReadPoolConfig},
        coprocessor::readpool_impl::build_read_pool_for_test,
        read_pool::{ReadPool, build_yatp_read_pool},
        storage::{FlowStatsReporter, TestEngineBuilder},
    };

    #[derive(Clone)]
    struct NoopFlowStatsReporter;

    impl FlowStatsReporter for NoopFlowStatsReporter {
        fn report_read_stats(&self, _read_stats: ReadStats) {}

        fn report_write_stats(&self, _write_stats: WriteStats) {}
    }

    struct ConcatMergeable {
        values: Vec<u8>,
        fail_serialize: bool,
        serialize_gate: Option<mpsc::Receiver<()>>,
        serialize_delay: Duration,
        _trace_guard: MemoryTraceGuard<()>,
    }

    impl ConcatMergeable {
        fn new(values: Vec<u8>) -> Self {
            Self {
                values,
                fail_serialize: false,
                serialize_gate: None,
                serialize_delay: Duration::ZERO,
                _trace_guard: MemoryTraceGuard::default(),
            }
        }

        fn failing(values: Vec<u8>) -> Self {
            Self {
                fail_serialize: true,
                ..Self::new(values)
            }
        }
    }

    impl MergeableResult for ConcatMergeable {
        fn merge(&mut self, other: Box<dyn MergeableResult>) {
            let other = (other as Box<dyn std::any::Any>)
                .downcast::<ConcatMergeable>()
                .unwrap();
            self.values.extend(other.values);
        }

        fn into_data(mut self: Box<Self>) -> super::super::Result<Vec<u8>> {
            if self.fail_serialize {
                return Err(Error::Other("cannot serialize".to_owned()));
            }
            if let Some(gate) = self.serialize_gate.take() {
                gate.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            if !self.serialize_delay.is_zero() {
                thread::sleep(self.serialize_delay);
            }
            self.values.sort_unstable();
            Ok(self.values)
        }
    }

    fn batch_output(task_id: u64) -> BatchTaskOutput {
        let mut response = coppb::StoreBatchTaskResponse::default();
        response.set_task_id(task_id);
        BatchTaskOutput {
            response: response.into(),
            mergeable_result: None,
        }
    }

    fn tracked_future<T>(
        output: T,
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
    ) -> impl Future<Output = T> {
        let mut output = Some(output);
        let mut started = false;
        future::poll_fn(move |cx| {
            if !started {
                started = true;
                let active = active.fetch_add(1, Ordering::SeqCst) + 1;
                max_active.fetch_max(active, Ordering::SeqCst);
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            active.fetch_sub(1, Ordering::SeqCst);
            Poll::Ready(output.take().unwrap())
        })
    }

    /// Fills every counter so equality checks catch any execution detail being
    /// dropped or assigned to the wrong task.
    fn filled_exec_details_v2(value: u64) -> kvrpcpb::ExecDetailsV2 {
        let mut details = kvrpcpb::ExecDetailsV2::default();
        let scan = details.mut_scan_detail_v2();
        scan.processed_versions = value;
        scan.processed_versions_size = value;
        scan.total_versions = value;
        scan.rocksdb_delete_skipped_count = value;
        scan.rocksdb_key_skipped_count = value;
        scan.rocksdb_block_cache_hit_count = value;
        scan.rocksdb_block_read_count = value;
        scan.rocksdb_block_read_byte = value;
        scan.rocksdb_block_read_nanos = value;
        scan.get_snapshot_nanos = value;
        scan.read_index_propose_wait_nanos = value;
        scan.read_index_confirm_wait_nanos = value;
        scan.read_pool_schedule_wait_nanos = value;
        let time = details.mut_time_detail_v2();
        time.wait_wall_time_ns = value;
        time.process_wall_time_ns = value;
        time.process_suspend_wall_time_ns = value;
        time.kv_read_wall_time_ns = value;
        details
    }

    fn mergeable_batch_output(values: Vec<u8>) -> BatchTaskOutput {
        BatchTaskOutput {
            response: coppb::StoreBatchTaskResponse::default().into(),
            mergeable_result: Some(Box::new(ConcatMergeable::new(values))),
        }
    }

    fn mergeable_output_with_held_trace(
        values: Vec<u8>,
        trace: &Arc<MemoryTrace>,
        held_bytes: usize,
    ) -> HandlerOutput {
        let result = ConcatMergeable {
            values,
            fail_serialize: false,
            serialize_gate: None,
            serialize_delay: Duration::ZERO,
            _trace_guard: trace.trace_guard((), held_bytes),
        };
        HandlerOutput::mergeable_with_trace(coppb::Response::default(), Box::new(result), trace)
    }

    fn mergeable_batch_output_with_exec_details(
        values: Vec<u8>,
        exec_details: u64,
    ) -> BatchTaskOutput {
        let mut output = mergeable_batch_output(values);
        output
            .response
            .set_exec_details_v2(filled_exec_details_v2(exec_details));
        output
    }

    fn merge_batch_task_responses_for_test(
        output: HandlerOutput,
        batch_outputs: Vec<BatchTaskOutput>,
    ) -> TracedResponse {
        block_on(merge_batch_task_responses(
            output,
            batch_outputs,
            ::tracker::INVALID_TRACKER_TOKEN,
            Deadline::from_now(Duration::from_secs(60)),
        ))
    }

    fn build_single_thread_read_pool() -> ReadPool {
        let engine = TestEngineBuilder::new().build().unwrap();
        ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig {
                normal_concurrency: 1,
                ..CoprReadPoolConfig::default_for_test()
            },
            engine,
        ))
    }

    fn batch_merge_finalizer_for_test(
        read_pool: &ReadPool,
        context: &kvrpcpb::Context,
        semaphore: Option<Arc<Semaphore>>,
        resource_limiter: Option<Arc<ResourceLimiter>>,
        timeout: Duration,
    ) -> BatchMergeFinalizer {
        BatchMergeFinalizer {
            read_pool: read_pool.handle(),
            semaphore,
            resource_tag: ResourceTagFactory::new_for_test().new_tag(context),
            priority: context.get_priority(),
            metadata: TaskMetadata::default(),
            resource_limiter,
            deadline: Deadline::from_now(timeout),
            task_id: 0,
        }
    }

    fn tracked_response_bytes(token: TrackerToken) -> u64 {
        let mut response_bytes = 0;
        GLOBAL_TRACKERS.with_tracker(token, |tracker| {
            response_bytes = tracker.metrics.coprocessor_response_bytes;
        });
        response_bytes
    }

    #[test]
    fn test_batch_merge_finalizer() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            engine,
        ));
        let context = kvrpcpb::Context::default();
        let trace = tikv_alloc::mem_trace!(test_batch_merge_finalizer);
        let top_output = || {
            HandlerOutput::mergeable_with_trace(
                coppb::Response::default(),
                Box::new(ConcatMergeable::new(vec![1])),
                &trace,
            )
        };
        let token = GLOBAL_TRACKERS.insert(Tracker::new(RequestInfo::new(
            &context,
            RequestType::Unknown,
            0,
        )));

        let resp = block_on(
            batch_merge_finalizer_for_test(
                &read_pool,
                &context,
                Some(Arc::new(Semaphore::new(1))),
                None,
                Duration::from_secs(60),
            )
            .finalize(top_output(), vec![mergeable_batch_output(vec![2])], token),
        );
        let tracker = GLOBAL_TRACKERS.remove(token).unwrap();

        assert_eq!(resp.get_data(), &[1, 2]);
        let batch_responses = resp.get_batch_responses();
        assert_eq!(batch_responses.len(), 1);
        assert!(batch_responses[0].get_data_merged_into_response());
        assert!(batch_responses[0].get_data().is_empty());
        // Poll time from the foreign read-pool task is attributed to the top
        // request's tracker.
        assert!(tracker.metrics.future_process_nanos > 0);

        let resp = block_on(
            batch_merge_finalizer_for_test(
                &read_pool,
                &context,
                Some(Arc::new(Semaphore::new(0))),
                None,
                Duration::from_millis(500),
            )
            .finalize(
                top_output(),
                vec![mergeable_batch_output(vec![2])],
                ::tracker::INVALID_TRACKER_TOKEN,
            ),
        );

        // A permit timeout cannot expose consumed data or acknowledge children.
        assert!(resp.has_region_error());
        assert!(resp.get_data().is_empty());
        assert!(resp.get_batch_responses().is_empty());
    }

    #[test]
    fn test_batch_merge_finalizer_pool_rejection() {
        // Zero capacity makes spawn rejection deterministic without occupying a worker.
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig {
                max_tasks_per_worker_normal: 0,
                ..CoprReadPoolConfig::default_for_test()
            },
            engine,
        ));
        let context = kvrpcpb::Context::default();
        let token = GLOBAL_TRACKERS.insert(Tracker::new(RequestInfo::new(
            &context,
            RequestType::Unknown,
            0,
        )));
        let trace = tikv_alloc::mem_trace!(test_batch_merge_pool_rejection);
        let output = mergeable_output_with_held_trace(vec![1], &trace, 5);
        assert_eq!(trace.sum(), 5);

        let resp = block_on(
            batch_merge_finalizer_for_test(
                &read_pool,
                &context,
                None,
                None,
                Duration::from_secs(60),
            )
            .finalize(output, vec![mergeable_batch_output(vec![2])], token),
        );
        let tracker = GLOBAL_TRACKERS.remove(token).unwrap();

        assert!(resp.get_region_error().has_server_is_busy());
        assert!(resp.get_data().is_empty());
        assert!(resp.get_batch_responses().is_empty());
        assert_eq!(trace.sum(), 0);
        assert_eq!(tracker.metrics.coprocessor_response_bytes, 0);
    }

    #[test]
    fn test_batch_merge_finalizer_admission_respects_deadline() {
        let resource_manager = Arc::new(ResourceGroupManager::default());
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = build_yatp_read_pool(
            &UnifiedReadPoolConfig {
                min_thread_count: 1,
                max_thread_count: 1,
                max_tasks_per_worker: 10,
                ..UnifiedReadPoolConfig::default()
            },
            NoopFlowStatsReporter,
            engine,
            None,
            Some(resource_manager),
            CleanupMethod::InPlace,
            false,
        );
        let limiter = Arc::new(ResourceLimiter::new(
            "batch-merge-finalizer".to_owned(),
            10_000.0,
            f64::INFINITY,
            0,
            true,
        ));

        // Without deadline cancellation, admission would take at least one second.
        let mut admission_delay = Duration::ZERO;
        for _ in 0..1_000 {
            limiter.consume(
                Duration::from_micros(1_000),
                IoBytes::default(),
                false,
                true,
            );
            admission_delay = limiter.admission_delay(true);
            if admission_delay >= Duration::from_secs(1) {
                break;
            }
        }
        assert!(admission_delay >= Duration::from_secs(1));

        let context = kvrpcpb::Context::default();
        let trace = tikv_alloc::mem_trace!(test_batch_merge_admission_deadline);
        let output = mergeable_output_with_held_trace(vec![1], &trace, 5);
        assert_eq!(trace.sum(), 5);
        let finalizer = batch_merge_finalizer_for_test(
            &read_pool,
            &context,
            None,
            Some(limiter),
            Duration::from_millis(20),
        );

        let started_at = Instant::now();
        let resp = block_on(finalizer.finalize(
            output,
            vec![mergeable_batch_output(vec![2])],
            ::tracker::INVALID_TRACKER_TOKEN,
        ));
        let elapsed = started_at.elapsed();

        assert!(
            elapsed < Duration::from_millis(500),
            "finalizer waited {elapsed:?} for admission"
        );
        assert!(resp.get_region_error().has_server_is_busy());
        assert_eq!(
            resp.get_region_error().get_server_is_busy().get_reason(),
            "deadline is exceeded"
        );
        assert!(resp.get_data().is_empty());
        assert!(resp.get_batch_responses().is_empty());
        assert_eq!(trace.sum(), 0);
    }

    #[test]
    fn test_batch_merge_finalizer_queue_respects_deadline() {
        let read_pool = build_single_thread_read_pool();
        let handle = read_pool.handle();
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        // Hold the only normal worker so the finalizer is queued but cannot start.
        block_on(handle.spawn(
            async move {
                started_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            },
            CommandPri::Normal,
            1,
            TaskMetadata::default(),
            None,
        ))
        .unwrap();
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();

        let context = kvrpcpb::Context::default();
        let trace = tikv_alloc::mem_trace!(test_batch_merge_queue_deadline);
        let output = mergeable_output_with_held_trace(vec![1], &trace, 5);
        assert_eq!(trace.sum(), 5);
        let finalizer = batch_merge_finalizer_for_test(
            &read_pool,
            &context,
            None,
            None,
            Duration::from_millis(20),
        );

        let started_at = Instant::now();
        let resp = block_on(finalizer.finalize(
            output,
            vec![mergeable_batch_output(vec![2])],
            ::tracker::INVALID_TRACKER_TOKEN,
        ));
        let elapsed = started_at.elapsed();
        release_tx.send(()).unwrap();

        assert!(
            elapsed < Duration::from_secs(1),
            "finalizer waited {elapsed:?} for its queued task"
        );
        assert!(resp.get_region_error().has_server_is_busy());
        assert_eq!(
            resp.get_region_error().get_server_is_busy().get_reason(),
            "deadline is exceeded"
        );
        assert!(resp.get_data().is_empty());
        assert!(resp.get_batch_responses().is_empty());
        assert_eq!(trace.sum(), 0);
    }

    #[test]
    fn test_batch_merge_finalizer_preserves_completed_response() {
        let read_pool = build_single_thread_read_pool();
        let context = kvrpcpb::Context::default();
        let token = GLOBAL_TRACKERS.insert(Tracker::new(RequestInfo::new(
            &context,
            RequestType::Unknown,
            0,
        )));
        let timeout = Duration::from_millis(500);
        let finalizer = batch_merge_finalizer_for_test(&read_pool, &context, None, None, timeout);
        let trace = tikv_alloc::mem_trace!(test_batch_merge_completed_response);
        let (serialize_tx, serialize_rx) = mpsc::channel();
        let mut result = ConcatMergeable::new(vec![1]);
        result.serialize_gate = Some(serialize_rx);
        let output = HandlerOutput::mergeable_with_trace(
            coppb::Response::default(),
            Box::new(result),
            &trace,
        );
        let mut finalize =
            Box::pin(finalizer.finalize(output, vec![mergeable_batch_output(vec![2])], token));

        let state = block_on(future::poll_fn(|cx| {
            Poll::Ready(finalize.as_mut().poll(cx))
        }));
        assert!(state.is_pending());
        serialize_tx.send(()).unwrap();
        let wait_started = Instant::now();
        while tracked_response_bytes(token) == 0 {
            assert!(
                wait_started.elapsed() < Duration::from_secs(5),
                "merge response did not complete"
            );
            thread::sleep(Duration::from_millis(1));
        }

        // The response is complete; leave the caller unpolled until its deadline
        // expires.
        thread::sleep(timeout);
        let resp = block_on(finalize);
        let tracker = GLOBAL_TRACKERS.remove(token).unwrap();

        assert_eq!(resp.get_data(), &[1, 2]);
        assert_eq!(tracker.metrics.coprocessor_response_bytes, 2);
    }

    #[test]
    fn test_collect_batch_task_outputs_polls_batch_tasks_concurrently() {
        // The top task waits for the first batch task to be polled. Awaiting the
        // top task first would therefore deadlock.
        let (batch_polled_tx, batch_polled_rx) = oneshot::channel::<()>();
        let top = async move {
            batch_polled_rx.await.unwrap();
            HandlerOutput::ready(coppb::Response::default())
        };
        let mut batch_polled_tx = Some(batch_polled_tx);
        let batch_outputs = stream::iter([2, 3]).then(move |task_id| {
            if let Some(tx) = batch_polled_tx.take() {
                tx.send(()).unwrap();
            }
            future::ready(batch_output(task_id))
        });

        let (output, batch_outputs) =
            block_on(collect_batch_task_outputs_concurrently(top, batch_outputs));

        assert!(matches!(&output.state, HandlerOutputState::Ready));
        let task_ids: Vec<_> = batch_outputs
            .into_iter()
            .map(|output| output.response.get_task_id())
            .collect();
        assert_eq!(task_ids, vec![2, 3]);
    }

    #[test]
    fn test_collect_batch_task_outputs_does_not_increase_concurrency() {
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let top = tracked_future(
            HandlerOutput::ready(coppb::Response::default()),
            active.clone(),
            max_active.clone(),
        );
        let batch_tasks = vec![
            tracked_future(batch_output(2), active.clone(), max_active.clone()),
            tracked_future(batch_output(3), active.clone(), max_active.clone()),
        ];
        let batch_outputs = stream::iter(batch_tasks).then(|task| task);

        let (_, batch_outputs) =
            block_on(collect_batch_task_outputs_sequentially(top, batch_outputs));

        assert_eq!(batch_outputs.len(), 2);
        assert_eq!(max_active.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_merge_batch_task_responses_merge() {
        // Successful mergeable results are merged into the top result and
        // acknowledged data-less while keeping their own execution details.
        // Serializing the merged result charges its bytes to the top
        // result's trace node until the response drops.
        let mut response = coppb::Response::default();
        response.set_exec_details_v2(filled_exec_details_v2(1));
        let batch_outputs = vec![
            mergeable_batch_output_with_exec_details(vec![3], 100),
            mergeable_batch_output_with_exec_details(vec![2], 10),
        ];

        let trace = tikv_alloc::mem_trace!(test_merge_batch_responses);
        let output = HandlerOutput::mergeable_with_trace(
            response,
            Box::new(ConcatMergeable::new(vec![1])),
            &trace,
        );
        let resp = merge_batch_task_responses_for_test(output, batch_outputs);

        assert_eq!(resp.get_data(), &[1, 2, 3]);
        assert!(!unary_response_has_error(&resp));
        assert_eq!(resp.get_exec_details_v2(), &filled_exec_details_v2(1));
        let batch_resps = resp.get_batch_responses();
        assert_eq!(batch_resps.len(), 2);
        for (batch_resp, value) in batch_resps.iter().zip([100, 10]) {
            assert!(batch_resp.get_data_merged_into_response());
            assert!(batch_resp.get_data().is_empty());
            assert_eq!(
                batch_resp.get_exec_details_v2(),
                &filled_exec_details_v2(value)
            );
        }
        // The response guard owns all serialized data until the response drops.
        assert_eq!(trace.sum(), 3);
        drop(resp);
        assert_eq!(trace.sum(), 0);
    }

    #[test]
    fn test_merge_batch_task_responses_top_error_blocks_merge() {
        // A failed top task blocks merging even when every batched task kept a
        // mergeable result. A real top failure is an untracked error response,
        // so the attached child data must be adopted by the child's own trace
        // node to stay accounted until the response drops.
        let mut response = coppb::Response::default();
        response.set_other_error("top failed".to_owned());
        let output = HandlerOutput::ready(response);
        let trace = tikv_alloc::mem_trace!(test_top_error_blocks_merge);
        let child = BatchTaskOutput {
            response: trace.trace_guard(coppb::StoreBatchTaskResponse::default(), 0),
            mergeable_result: Some(Box::new(ConcatMergeable::new(vec![2]))),
        };

        let resp = merge_batch_task_responses_for_test(output, vec![child]);

        assert_eq!(resp.get_other_error(), "top failed");
        assert!(resp.get_data().is_empty());
        let batch_resps = resp.get_batch_responses();
        assert_eq!(batch_resps.len(), 1);
        assert!(!batch_resps[0].get_data_merged_into_response());
        assert_eq!(batch_resps[0].get_data(), &[2]);
        assert_eq!(trace.sum(), 1);
        drop(resp);
        assert_eq!(trace.sum(), 0);
    }

    #[test]
    fn test_merge_batch_task_responses_deadline() {
        // An expired deadline discards the complete response so every task can
        // retry, and releases the discarded top and child traced data.
        let trace = tikv_alloc::mem_trace!(test_merge_deadline);
        let mut response = coppb::Response::default();
        response.set_data(vec![1]);
        let output = HandlerOutput::ready_with_trace(response, &trace);
        let child = BatchTaskOutput {
            response: trace.trace_guard(coppb::StoreBatchTaskResponse::default(), 2),
            mergeable_result: Some(Box::new(ConcatMergeable::new(vec![2]))),
        };
        assert_eq!(trace.sum(), 3);

        let resp = block_on(merge_batch_task_responses(
            output,
            vec![child],
            ::tracker::INVALID_TRACKER_TOKEN,
            Deadline::from_now(Duration::ZERO),
        ));

        assert!(resp.has_region_error());
        assert!(resp.get_data().is_empty());
        assert!(resp.get_batch_responses().is_empty());
        assert_eq!(trace.sum(), 0);
    }

    #[test]
    fn test_merge_batch_task_responses_commit_deadline() {
        // Top-result serialization is synchronous. If the deadline expires
        // during it, the serialized response must not be committed or charged.
        let token = GLOBAL_TRACKERS.insert(Tracker::new(RequestInfo::new(
            &kvrpcpb::Context::default(),
            RequestType::Unknown,
            0,
        )));
        let trace = tikv_alloc::mem_trace!(test_commit_deadline);
        let result = ConcatMergeable {
            values: vec![1],
            fail_serialize: false,
            serialize_gate: None,
            serialize_delay: Duration::from_secs(1),
            _trace_guard: trace.trace_guard((), 5),
        };
        let output = HandlerOutput::mergeable_with_trace(
            coppb::Response::default(),
            Box::new(result),
            &trace,
        );
        // Start charged so the zero below proves cleanup after commit rejection.
        assert_eq!(trace.sum(), 5);

        let resp = block_on(merge_batch_task_responses(
            output,
            vec![mergeable_batch_output(vec![2])],
            token,
            Deadline::from_now(Duration::from_millis(500)),
        ));
        let tracker = GLOBAL_TRACKERS.remove(token).unwrap();

        assert!(resp.has_region_error());
        assert!(resp.get_data().is_empty());
        assert!(resp.get_batch_responses().is_empty());
        assert_eq!(trace.sum(), 0);
        assert_eq!(tracker.metrics.coprocessor_response_bytes, 0);
    }

    #[test]
    fn test_merge_batch_task_responses_partial_merge_and_accounting() {
        // A successful child is merged while a failed child stays separate;
        // byte and memory accounting cover both exactly once.
        let token = GLOBAL_TRACKERS.insert(Tracker::new(RequestInfo::new(
            &kvrpcpb::Context::default(),
            RequestType::Unknown,
            0,
        )));
        let mut failed = mergeable_batch_output_with_exec_details(vec![9], 100);
        failed.response.set_other_error("boom".to_owned());
        let trace = tikv_alloc::mem_trace!(test_partial_merge_and_accounting);
        let output = HandlerOutput::mergeable_with_trace(
            coppb::Response::default(),
            Box::new(ConcatMergeable::new(vec![1])),
            &trace,
        );

        let resp = block_on(merge_batch_task_responses(
            output,
            vec![mergeable_batch_output(vec![2]), failed],
            token,
            Deadline::from_now(Duration::from_secs(60)),
        ));
        let tracker = GLOBAL_TRACKERS.remove(token).unwrap();

        assert_eq!(resp.get_data(), &[1, 2]);
        let batch_responses = resp.get_batch_responses();
        assert_eq!(batch_responses.len(), 2);
        // The successful child is acknowledged without duplicating merged data.
        assert!(batch_responses[0].get_data_merged_into_response());
        assert!(batch_responses[0].get_data().is_empty());
        // The failed child remains standalone with its error and task details.
        assert!(!batch_responses[1].get_data_merged_into_response());
        assert_eq!(batch_responses[1].get_data(), &[9]);
        assert_eq!(batch_responses[1].get_other_error(), "boom");
        assert_eq!(
            batch_responses[1].get_exec_details_v2(),
            &filled_exec_details_v2(100)
        );
        let batch_data_len: u64 = batch_responses
            .iter()
            .map(|response| response.get_data().len() as u64)
            .sum();
        let response_data_len = resp.get_data().len() as u64 + batch_data_len;
        // Both accounting paths cover every byte retained by the response.
        assert_eq!(
            tracker.metrics.coprocessor_response_bytes,
            response_data_len
        );
        assert_eq!(trace.sum() as u64, response_data_len);
        drop(resp);
        assert_eq!(trace.sum(), 0);
    }

    #[test]
    fn test_merge_batch_task_responses_serialize_errors_confined() {
        // A child serialization failure stays on that child and does not
        // affect successful siblings or the top response.
        let failing_child = BatchTaskOutput {
            response: coppb::StoreBatchTaskResponse::default().into(),
            mergeable_result: Some(Box::new(ConcatMergeable::failing(vec![2]))),
        };
        let resp = merge_batch_task_responses_for_test(
            HandlerOutput::ready(coppb::Response::default()),
            vec![failing_child, mergeable_batch_output(vec![3])],
        );

        assert!(!unary_response_has_error(&resp));
        let batch_responses = resp.get_batch_responses();
        assert_eq!(batch_responses.len(), 2);
        assert!(!batch_responses[0].get_other_error().is_empty());
        assert!(batch_responses[0].get_data().is_empty());
        assert_eq!(batch_responses[1].get_data(), &[3]);

        // A top serialization failure can retain batch responses while no
        // child result has been merged into it.
        let trace = tikv_alloc::mem_trace!(test_serialize_errors);
        let output = HandlerOutput::mergeable_with_trace(
            coppb::Response::default(),
            Box::new(ConcatMergeable::failing(vec![1])),
            &trace,
        );
        let resp = merge_batch_task_responses_for_test(
            output,
            vec![BatchTaskOutput {
                response: coppb::StoreBatchTaskResponse::default().into(),
                mergeable_result: None,
            }],
        );

        assert!(!resp.get_other_error().is_empty());
        assert_eq!(resp.get_batch_responses().len(), 1);

        // Once a child has been merged, a top serialization failure must not
        // acknowledge the now-consumed child separately.
        let output = HandlerOutput::mergeable_with_trace(
            coppb::Response::default(),
            Box::new(ConcatMergeable::failing(vec![1])),
            &trace,
        );
        let resp =
            merge_batch_task_responses_for_test(output, vec![mergeable_batch_output(vec![2])]);

        assert!(!resp.get_other_error().is_empty());
        assert!(resp.get_batch_responses().is_empty());
    }
}
