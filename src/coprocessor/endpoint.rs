// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{borrow::Cow, future::Future, marker::PhantomData, sync::Arc, time::Duration};

use ::tracker::{
    set_tls_tracker_token, with_tls_tracker, RequestInfo, RequestType, GLOBAL_TRACKERS,
};
use async_stream::try_stream;
use concurrency_manager::ConcurrencyManager;
use engine_traits::PerfLevel;
use futures::{channel::mpsc, prelude::*};
use kvproto::{coprocessor as coppb, errorpb, kvrpcpb};
use protobuf::{CodedInputStream, Message};
use resource_metering::{FutureExt, ResourceTagFactory, StreamExt};
use tidb_query_common::execute_stats::ExecSummary;
use tikv_alloc::trace::MemoryTraceGuard;
use tikv_kv::SnapshotExt;
use tikv_util::{quota_limiter::QuotaLimiter, time::Instant};
use tipb::{AnalyzeReq, AnalyzeType, ChecksumRequest, ChecksumScanOn, DagRequest, ExecType};
use tokio::sync::Semaphore;
use txn_types::Lock;

use crate::{
    coprocessor::{cache::CachedRequestHandler, interceptors::*, metrics::*, tracker::Tracker, *},
    read_pool::ReadPoolHandle,
    server::Config,
    storage::{
        self,
        kv::{self, with_tls_engine, SnapContext},
        mvcc::Error as MvccError,
        need_check_locks, need_check_locks_in_replica_read, Engine, Snapshot, SnapshotStore,
    },
};

/// Requests that need time of less than `LIGHT_TASK_THRESHOLD` is considered as light ones,
/// which means they don't need a permit from the semaphore before execution.
const LIGHT_TASK_THRESHOLD: Duration = Duration::from_millis(5);

/// A pool to build and run Coprocessor request handlers.
#[derive(Clone)]
pub struct Endpoint<E: Engine> {
    /// The thread pool to run Coprocessor requests.
    read_pool: ReadPoolHandle,

    /// The concurrency limiter of the coprocessor.
    semaphore: Option<Arc<Semaphore>>,

    concurrency_manager: ConcurrencyManager,

    // Perf stats level
    perf_level: PerfLevel,

    resource_tag_factory: ResourceTagFactory,

    /// The recursion limit when parsing Coprocessor Protobuf requests.
    recursion_limit: u32,

    batch_row_limit: usize,
    stream_batch_row_limit: usize,
    stream_channel_size: usize,

    /// The soft time limit of handling Coprocessor requests.
    max_handle_duration: Duration,

    slow_log_threshold: Duration,

    _phantom: PhantomData<E>,

    quota_limiter: Arc<QuotaLimiter>,
}

impl<E: Engine> tikv_util::AssertSend for Endpoint<E> {}

impl<E: Engine> Endpoint<E> {
    pub fn new(
        cfg: &Config,
        read_pool: ReadPoolHandle,
        concurrency_manager: ConcurrencyManager,
        resource_tag_factory: ResourceTagFactory,
        quota_limiter: Arc<QuotaLimiter>,
    ) -> Self {
        // FIXME: When yatp is used, we need to limit coprocessor requests in progress to avoid
        // using too much memory. However, if there are a number of large requests, small requests
        // will still be blocked. This needs to be improved.
        let semaphore = match &read_pool {
            ReadPoolHandle::Yatp { .. } => {
                Some(Arc::new(Semaphore::new(cfg.end_point_max_concurrency)))
            }
            _ => None,
        };
        Self {
            read_pool,
            semaphore,
            concurrency_manager,
            perf_level: cfg.end_point_perf_level,
            resource_tag_factory,
            recursion_limit: cfg.end_point_recursion_limit,
            batch_row_limit: cfg.end_point_batch_row_limit,
            stream_batch_row_limit: cfg.end_point_stream_batch_row_limit,
            stream_channel_size: cfg.end_point_stream_channel_size,
            max_handle_duration: cfg.end_point_request_max_handle_duration.0,
            slow_log_threshold: cfg.end_point_slow_log_threshold.0,
            _phantom: Default::default(),
            quota_limiter,
        }
    }

    fn check_memory_locks(&self, req_ctx: &ReqContext) -> Result<()> {
        let start_ts = req_ctx.txn_start_ts;
        if !req_ctx.context.get_stale_read() {
            self.concurrency_manager.update_max_ts(start_ts);
        }
        if need_check_locks(req_ctx.context.get_isolation_level()) {
            let begin_instant = Instant::now();
            for range in &req_ctx.ranges {
                let start_key = txn_types::Key::from_raw_maybe_unbounded(range.get_start());
                let end_key = txn_types::Key::from_raw_maybe_unbounded(range.get_end());
                self.concurrency_manager
                    .read_range_check(start_key.as_ref(), end_key.as_ref(), |key, lock| {
                        Lock::check_ts_conflict(
                            Cow::Borrowed(lock),
                            key,
                            start_ts,
                            &req_ctx.bypass_locks,
                            req_ctx.context.get_isolation_level(),
                        )
                    })
                    .map_err(|e| {
                        MEM_LOCK_CHECK_HISTOGRAM_VEC_STATIC
                            .locked
                            .observe(begin_instant.saturating_elapsed().as_secs_f64());
                        MvccError::from(e)
                    })?;
            }
            MEM_LOCK_CHECK_HISTOGRAM_VEC_STATIC
                .unlocked
                .observe(begin_instant.saturating_elapsed().as_secs_f64());
        }
        Ok(())
    }

    /// Parse the raw `Request` to create `RequestHandlerBuilder` and `ReqContext`.
    /// Returns `Err` if fails.
    ///
    /// It also checks if there are locks in memory blocking this read request.
    fn parse_request_and_check_memory_locks(
        &self,
        mut req: coppb::Request,
        peer: Option<String>,
        is_streaming: bool,
    ) -> Result<(RequestHandlerBuilder<E::Snap>, ReqContext)> {
        fail_point!("coprocessor_parse_request", |_| Err(box_err!(
            "unsupported tp (failpoint)"
        )));

        let (context, data, ranges, mut start_ts) = (
            req.take_context(),
            req.take_data(),
            req.take_ranges().to_vec(),
            req.get_start_ts(),
        );
        let cache_match_version = if req.get_is_cache_enabled() {
            Some(req.get_cache_if_match_version())
        } else {
            None
        };

        let mut input = CodedInputStream::from_bytes(&data);
        input.set_recursion_limit(self.recursion_limit);

        let req_ctx: ReqContext;
        let builder: RequestHandlerBuilder<E::Snap>;

        match req.get_tp() {
            REQ_TYPE_DAG => {
                let mut dag = DagRequest::default();
                box_try!(dag.merge_from(&mut input));
                let mut table_scan = false;
                let mut is_desc_scan = false;
                if let Some(scan) = dag.get_executors().iter().next() {
                    table_scan = scan.get_tp() == ExecType::TypeTableScan;
                    if table_scan {
                        is_desc_scan = scan.get_tbl_scan().get_desc();
                    } else {
                        is_desc_scan = scan.get_idx_scan().get_desc();
                    }
                }
                if start_ts == 0 {
                    start_ts = dag.get_start_ts_fallback();
                }
                let tag = if table_scan {
                    ReqTag::select
                } else {
                    ReqTag::index
                };

                req_ctx = ReqContext::new(
                    tag,
                    context,
                    ranges,
                    self.max_handle_duration,
                    peer,
                    Some(is_desc_scan),
                    start_ts.into(),
                    cache_match_version,
                    self.perf_level,
                );
                with_tls_tracker(|tracker| {
                    tracker.req_info.request_type = RequestType::CoprocessorDag;
                    tracker.req_info.start_ts = start_ts;
                });

                self.check_memory_locks(&req_ctx)?;

                let batch_row_limit = self.get_batch_row_limit(is_streaming);
                let quota_limiter = self.quota_limiter.clone();
                builder = Box::new(move |snap, req_ctx| {
                    let data_version = snap.ext().get_data_version();
                    let store = SnapshotStore::new(
                        snap,
                        start_ts.into(),
                        req_ctx.context.get_isolation_level(),
                        !req_ctx.context.get_not_fill_cache(),
                        req_ctx.bypass_locks.clone(),
                        req_ctx.access_locks.clone(),
                        req.get_is_cache_enabled(),
                    );
                    let paging_size = match req.get_paging_size() {
                        0 => None,
                        i => Some(i),
                    };
                    dag::DagHandlerBuilder::new(
                        dag,
                        req_ctx.ranges.clone(),
                        store,
                        req_ctx.deadline,
                        batch_row_limit,
                        is_streaming,
                        req.get_is_cache_enabled(),
                        paging_size,
                        quota_limiter,
                    )
                    .data_version(data_version)
                    .build()
                });
            }
            REQ_TYPE_ANALYZE => {
                let mut analyze = AnalyzeReq::default();
                box_try!(analyze.merge_from(&mut input));
                if start_ts == 0 {
                    start_ts = analyze.get_start_ts_fallback();
                }

                let tag = match analyze.get_tp() {
                    AnalyzeType::TypeIndex | AnalyzeType::TypeCommonHandle => ReqTag::analyze_index,
                    AnalyzeType::TypeColumn | AnalyzeType::TypeMixed => ReqTag::analyze_table,
                    AnalyzeType::TypeFullSampling => ReqTag::analyze_full_sampling,
                    AnalyzeType::TypeSampleIndex => unimplemented!(),
                };
                req_ctx = ReqContext::new(
                    tag,
                    context,
                    ranges,
                    self.max_handle_duration,
                    peer,
                    None,
                    start_ts.into(),
                    cache_match_version,
                    self.perf_level,
                );
                with_tls_tracker(|tracker| {
                    tracker.req_info.request_type = RequestType::CoprocessorAnalyze;
                    tracker.req_info.start_ts = start_ts;
                });

                self.check_memory_locks(&req_ctx)?;

                let quota_limiter = self.quota_limiter.clone();

                builder = Box::new(move |snap, req_ctx| {
                    statistics::analyze::AnalyzeContext::new(
                        analyze,
                        req_ctx.ranges.clone(),
                        start_ts,
                        snap,
                        req_ctx,
                        quota_limiter,
                    )
                    .map(|h| h.into_boxed())
                });
            }
            REQ_TYPE_CHECKSUM => {
                let mut checksum = ChecksumRequest::default();
                box_try!(checksum.merge_from(&mut input));
                let table_scan = checksum.get_scan_on() == ChecksumScanOn::Table;
                if start_ts == 0 {
                    start_ts = checksum.get_start_ts_fallback();
                }

                let tag = if table_scan {
                    ReqTag::checksum_table
                } else {
                    ReqTag::checksum_index
                };
                req_ctx = ReqContext::new(
                    tag,
                    context,
                    ranges,
                    self.max_handle_duration,
                    peer,
                    None,
                    start_ts.into(),
                    cache_match_version,
                    self.perf_level,
                );
                with_tls_tracker(|tracker| {
                    tracker.req_info.request_type = RequestType::CoprocessorChecksum;
                    tracker.req_info.start_ts = start_ts;
                });

                self.check_memory_locks(&req_ctx)?;

                builder = Box::new(move |snap, req_ctx| {
                    checksum::ChecksumContext::new(
                        checksum,
                        req_ctx.ranges.clone(),
                        start_ts,
                        snap,
                        req_ctx,
                    )
                    .map(|h| h.into_boxed())
                });
            }
            tp => return Err(box_err!("unsupported tp {}", tp)),
        };

        Ok((builder, req_ctx))
    }

    /// Get the batch row limit configuration.
    #[inline]
    fn get_batch_row_limit(&self, is_streaming: bool) -> usize {
        if is_streaming {
            self.stream_batch_row_limit
        } else {
            self.batch_row_limit
        }
    }

    #[inline]
    fn async_snapshot(
        engine: &E,
        ctx: &ReqContext,
    ) -> impl std::future::Future<Output = Result<E::Snap>> {
        let mut snap_ctx = SnapContext {
            pb_ctx: &ctx.context,
            start_ts: ctx.txn_start_ts,
            ..Default::default()
        };
        // need to pass start_ts and ranges to check memory locks for replica read
        if need_check_locks_in_replica_read(&ctx.context) {
            for r in &ctx.ranges {
                let start_key = txn_types::Key::from_raw(r.get_start());
                let end_key = txn_types::Key::from_raw(r.get_end());
                let mut key_range = kvrpcpb::KeyRange::default();
                key_range.set_start_key(start_key.into_encoded());
                key_range.set_end_key(end_key.into_encoded());
                snap_ctx.key_ranges.push(key_range);
            }
        }
        kv::snapshot(engine, snap_ctx).map_err(Error::from)
    }

    /// The real implementation of handling a unary request.
    ///
    /// It first retrieves a snapshot, then builds the `RequestHandler` over the snapshot and
    /// the given `handler_builder`. Finally, it calls the unary request interface of the
    /// `RequestHandler` to process the request and produce a result.
    async fn handle_unary_request_impl(
        semaphore: Option<Arc<Semaphore>>,
        mut tracker: Box<Tracker<E>>,
        handler_builder: RequestHandlerBuilder<E::Snap>,
    ) -> Result<MemoryTraceGuard<coppb::Response>> {
        // When this function is being executed, it may be queued for a long time, so that
        // deadline may exceed.
        tracker.on_scheduled();
        tracker.req_ctx.deadline.check()?;

        // Safety: spawning this function using a `FuturePool` ensures that a TLS engine
        // exists.
        let snapshot =
            unsafe { with_tls_engine(|engine| Self::async_snapshot(engine, &tracker.req_ctx)) }
                .await?;
        // When snapshot is retrieved, deadline may exceed.
        tracker.on_snapshot_finished();
        tracker.req_ctx.deadline.check()?;
        tracker.buckets = snapshot.ext().get_buckets();
        let buckets_version = tracker.buckets.as_ref().map_or(0, |b| b.version);

        let mut handler = if tracker.req_ctx.cache_match_version.is_some()
            && tracker.req_ctx.cache_match_version == snapshot.ext().get_data_version()
        {
            // Build a cached request handler instead if cache version is matching.
            CachedRequestHandler::builder()(snapshot, &tracker.req_ctx)?
        } else {
            handler_builder(snapshot, &tracker.req_ctx)?
        };

        tracker.on_begin_all_items();

        let deadline = tracker.req_ctx.deadline;
        let handle_request_future = check_deadline(handler.handle_request(), deadline);
        let handle_request_future = track(handle_request_future, &mut tracker);

        let deadline_res = if let Some(semaphore) = &semaphore {
            limit_concurrency(handle_request_future, semaphore, LIGHT_TASK_THRESHOLD).await
        } else {
            handle_request_future.await
        };
        let result = deadline_res.map_err(Error::from).and_then(|res| res);

        // There might be errors when handling requests. In this case, we still need its
        // execution metrics.
        let mut exec_summary = ExecSummary::default();
        handler.collect_scan_summary(&mut exec_summary);
        tracker.collect_scan_process_time(exec_summary);
        let mut storage_stats = Statistics::default();
        handler.collect_scan_statistics(&mut storage_stats);
        tracker.collect_storage_statistics(storage_stats);
        let (exec_details, exec_details_v2) = tracker.get_exec_details();
        tracker.on_finish_all_items();

        let mut resp = match result {
            Ok(resp) => {
                COPR_RESP_SIZE.inc_by(resp.data.len() as u64);
                resp
            }
            Err(e) => make_error_response(e).into(),
        };
        resp.set_exec_details(exec_details);
        resp.set_exec_details_v2(exec_details_v2);
        resp.set_latest_buckets_version(buckets_version);
        Ok(resp)
    }

    /// Handle a unary request and run on the read pool.
    ///
    /// Returns `Err(err)` if the read pool is full. Returns `Ok(future)` in other cases.
    /// The future inside may be an error however.
    fn handle_unary_request(
        &self,
        req_ctx: ReqContext,
        handler_builder: RequestHandlerBuilder<E::Snap>,
    ) -> impl Future<Output = Result<MemoryTraceGuard<coppb::Response>>> {
        let priority = req_ctx.context.get_priority();
        let task_id = req_ctx.build_task_id();
        let key_ranges = req_ctx
            .ranges
            .iter()
            .map(|key_range| (key_range.get_start().to_vec(), key_range.get_end().to_vec()))
            .collect();
        let resource_tag = self
            .resource_tag_factory
            .new_tag_with_key_ranges(&req_ctx.context, key_ranges);
        // box the tracker so that moving it is cheap.
        let tracker = Box::new(Tracker::new(req_ctx, self.slow_log_threshold));

        let res = self
            .read_pool
            .spawn_handle(
                Self::handle_unary_request_impl(self.semaphore.clone(), tracker, handler_builder)
                    .in_resource_metering_tag(resource_tag),
                priority,
                task_id,
            )
            .map_err(|_| Error::MaxPendingTasksExceeded);
        async move { res.await? }
    }

    /// Parses and handles a unary request. Returns a future that will never fail. If there are
    /// errors during parsing or handling, they will be converted into a `Response` as the success
    /// result of the future.
    #[inline]
    pub fn parse_and_handle_unary_request(
        &self,
        req: coppb::Request,
        peer: Option<String>,
    ) -> impl Future<Output = MemoryTraceGuard<coppb::Response>> {
        let tracker = GLOBAL_TRACKERS.insert(::tracker::Tracker::new(RequestInfo::new(
            req.get_context(),
            RequestType::Unknown,
            req.start_ts,
        )));
        set_tls_tracker_token(tracker);
        let result_of_future = self
            .parse_request_and_check_memory_locks(req, peer, false)
            .map(|(handler_builder, req_ctx)| self.handle_unary_request(req_ctx, handler_builder));

        async move {
            let res = match result_of_future {
                Err(e) => make_error_response(e).into(),
                Ok(handle_fut) => handle_fut
                    .await
                    .unwrap_or_else(|e| make_error_response(e).into()),
            };
            GLOBAL_TRACKERS.remove(tracker);
            res
        }
    }

    /// The real implementation of handling a stream request.
    ///
    /// It first retrieves a snapshot, then builds the `RequestHandler` over the snapshot and
    /// the given `handler_builder`. Finally, it calls the stream request interface of the
    /// `RequestHandler` multiple times to process the request and produce multiple results.
    fn handle_stream_request_impl(
        semaphore: Option<Arc<Semaphore>>,
        mut tracker: Box<Tracker<E>>,
        handler_builder: RequestHandlerBuilder<E::Snap>,
    ) -> impl futures::stream::Stream<Item = Result<coppb::Response>> {
        try_stream! {
            let _permit = if let Some(semaphore) = semaphore.as_ref() {
                Some(semaphore.acquire().await.expect("the semaphore never be closed"))
            } else {
                None
            };

            // When this function is being executed, it may be queued for a long time, so that
            // deadline may exceed.
            tracker.on_scheduled();
            tracker.req_ctx.deadline.check()?;

            // Safety: spawning this function using a `FuturePool` ensures that a TLS engine
            // exists.
            let snapshot = unsafe {
                with_tls_engine(|engine| Self::async_snapshot(engine, &tracker.req_ctx))
            }
            .await?;
            // When snapshot is retrieved, deadline may exceed.
            tracker.on_snapshot_finished();
            tracker.req_ctx.deadline.check()?;

            let mut handler = handler_builder(snapshot, &tracker.req_ctx)?;

            tracker.on_begin_all_items();

            loop {
                let result = {
                    tracker.on_begin_item();

                    let result = handler.handle_streaming_request();

                    let mut storage_stats = Statistics::default();
                    handler.collect_scan_statistics(&mut storage_stats);
                    tracker.on_finish_item(Some(storage_stats));

                    result
                };

                let (exec_details, exec_details_v2) = tracker.get_item_exec_details();

                match result {
                    Err(e) => {
                        let mut resp = make_error_response(e);
                        resp.set_exec_details(exec_details);
                        resp.set_exec_details_v2(exec_details_v2);
                        yield resp;
                        break;
                    },
                    Ok((None, _)) => break,
                    Ok((Some(mut resp), finished)) => {
                        COPR_RESP_SIZE.inc_by(resp.data.len() as u64);
                        resp.set_exec_details(exec_details);
                        resp.set_exec_details_v2(exec_details_v2);
                        yield resp;
                        if finished {
                            break;
                        }
                    }
                }
            }
            tracker.on_finish_all_items();
        }
    }

    /// Handle a stream request and run on the read pool.
    ///
    /// Returns `Err(err)` if the read pool is full. Returns `Ok(stream)` in other cases.
    /// The stream inside may produce errors however.
    fn handle_stream_request(
        &self,
        req_ctx: ReqContext,
        handler_builder: RequestHandlerBuilder<E::Snap>,
    ) -> Result<impl futures::stream::Stream<Item = Result<coppb::Response>>> {
        let (tx, rx) = mpsc::channel::<Result<coppb::Response>>(self.stream_channel_size);
        let priority = req_ctx.context.get_priority();
        let key_ranges = req_ctx
            .ranges
            .iter()
            .map(|key_range| (key_range.get_start().to_vec(), key_range.get_end().to_vec()))
            .collect();
        let resource_tag = self
            .resource_tag_factory
            .new_tag_with_key_ranges(&req_ctx.context, key_ranges);
        let task_id = req_ctx.build_task_id();
        let tracker = Box::new(Tracker::new(req_ctx, self.slow_log_threshold));

        self.read_pool
            .spawn(
                Self::handle_stream_request_impl(self.semaphore.clone(), tracker, handler_builder)
                    .in_resource_metering_tag(resource_tag)
                    .then(futures::future::ok::<_, mpsc::SendError>)
                    .forward(tx)
                    .unwrap_or_else(|e| {
                        warn!("coprocessor stream send error"; "error" => %e);
                    }),
                priority,
                task_id,
            )
            .map_err(|_| Error::MaxPendingTasksExceeded)?;
        Ok(rx)
    }

    /// Parses and handles a stream request. Returns a stream that produce each result in a
    /// `Response` and will never fail. If there are errors during parsing or handling, they will
    /// be converted into a `Response` as the only stream item.
    #[inline]
    pub fn parse_and_handle_stream_request(
        &self,
        req: coppb::Request,
        peer: Option<String>,
    ) -> impl futures::stream::Stream<Item = coppb::Response> {
        let result_of_stream = self
            .parse_request_and_check_memory_locks(req, peer, true)
            .and_then(|(handler_builder, req_ctx)| {
                self.handle_stream_request(req_ctx, handler_builder)
            }); // Result<Stream<Resp, Error>, Error>

        futures::stream::once(futures::future::ready(result_of_stream)) // Stream<Stream<Resp, Error>, Error>
            .try_flatten() // Stream<Resp, Error>
            .or_else(|e| futures::future::ok(make_error_response(e))) // Stream<Resp, ()>
            .map(|item: std::result::Result<_, ()>| item.unwrap())
    }
}

fn make_error_response(e: Error) -> coppb::Response {
    warn!(
        "error-response";
        "err" => %e
    );
    let mut resp = coppb::Response::default();
    let tag;
    match e {
        Error::Region(e) => {
            tag = storage::get_tag_from_header(&e);
            resp.set_region_error(e);
        }
        Error::Locked(info) => {
            tag = "meet_lock";
            resp.set_locked(info);
        }
        Error::DeadlineExceeded => {
            tag = "deadline_exceeded";
            resp.set_other_error(e.to_string());
        }
        Error::MaxPendingTasksExceeded => {
            tag = "max_pending_tasks_exceeded";
            let mut server_is_busy_err = errorpb::ServerIsBusy::default();
            server_is_busy_err.set_reason(e.to_string());
            let mut errorpb = errorpb::Error::default();
            errorpb.set_message(e.to_string());
            errorpb.set_server_is_busy(server_is_busy_err);
            resp.set_region_error(errorpb);
        }
        Error::Other(_) => {
            tag = "other";
            resp.set_other_error(e.to_string());
        }
    };
    COPR_REQ_ERROR.with_label_values(&[tag]).inc();
    resp
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{atomic, mpsc},
        thread, vec,
    };

    use futures::executor::{block_on, block_on_stream};
    use kvproto::kvrpcpb::IsolationLevel;
    use protobuf::Message;
    use tipb::{Executor, Expr};
    use txn_types::{Key, LockType};

    use super::*;
    use crate::{
        config::CoprReadPoolConfig,
        coprocessor::readpool_impl::build_read_pool_for_test,
        read_pool::ReadPool,
        storage::{kv::RocksEngine, TestEngineBuilder},
    };

    /// A unary `RequestHandler` that always produces a fixture.
    struct UnaryFixture {
        handle_duration_millis: u64,
        yieldable: bool,
        result: Option<Result<coppb::Response>>,
    }

    impl UnaryFixture {
        pub fn new(result: Result<coppb::Response>) -> UnaryFixture {
            UnaryFixture {
                handle_duration_millis: 0,
                yieldable: false,
                result: Some(result),
            }
        }

        pub fn new_with_duration(
            result: Result<coppb::Response>,
            handle_duration_millis: u64,
        ) -> UnaryFixture {
            UnaryFixture {
                handle_duration_millis,
                yieldable: false,
                result: Some(result),
            }
        }

        pub fn new_with_duration_yieldable(
            result: Result<coppb::Response>,
            handle_duration_millis: u64,
        ) -> UnaryFixture {
            UnaryFixture {
                handle_duration_millis,
                yieldable: true,
                result: Some(result),
            }
        }
    }

    #[async_trait]
    impl RequestHandler for UnaryFixture {
        async fn handle_request(&mut self) -> Result<MemoryTraceGuard<coppb::Response>> {
            if self.yieldable {
                // We split the task into small executions of 100 milliseconds.
                for _ in 0..self.handle_duration_millis / 100 {
                    thread::sleep(Duration::from_millis(100));
                    yatp::task::future::reschedule().await;
                }
                thread::sleep(Duration::from_millis(self.handle_duration_millis % 100));
            } else {
                thread::sleep(Duration::from_millis(self.handle_duration_millis));
            }

            self.result.take().unwrap().map(|x| x.into())
        }
    }

    /// A streaming `RequestHandler` that always produces a fixture.
    struct StreamFixture {
        result_len: usize,
        result_iter: vec::IntoIter<Result<coppb::Response>>,
        handle_durations_millis: vec::IntoIter<u64>,
        nth: usize,
    }

    impl StreamFixture {
        pub fn new(result: Vec<Result<coppb::Response>>) -> StreamFixture {
            let len = result.len();
            StreamFixture {
                result_len: len,
                result_iter: result.into_iter(),
                handle_durations_millis: vec![0; len].into_iter(),
                nth: 0,
            }
        }

        pub fn new_with_duration(
            result: Vec<Result<coppb::Response>>,
            handle_durations_millis: Vec<u64>,
        ) -> StreamFixture {
            assert_eq!(result.len(), handle_durations_millis.len());
            StreamFixture {
                result_len: result.len(),
                result_iter: result.into_iter(),
                handle_durations_millis: handle_durations_millis.into_iter(),
                nth: 0,
            }
        }
    }

    impl RequestHandler for StreamFixture {
        fn handle_streaming_request(&mut self) -> Result<(Option<coppb::Response>, bool)> {
            let is_finished = if self.result_len == 0 {
                true
            } else {
                self.nth >= (self.result_len - 1)
            };
            let ret = match self.result_iter.next() {
                None => {
                    assert!(is_finished);
                    Ok((None, is_finished))
                }
                Some(val) => {
                    let handle_duration_ms = self.handle_durations_millis.next().unwrap();
                    thread::sleep(Duration::from_millis(handle_duration_ms));
                    match val {
                        Ok(resp) => Ok((Some(resp), is_finished)),
                        Err(e) => Err(e),
                    }
                }
            };
            self.nth += 1;

            ret
        }
    }

    /// A streaming `RequestHandler` that produces values according a closure.
    struct StreamFromClosure {
        result_generator: Box<dyn Fn(usize) -> HandlerStreamStepResult + Send>,
        nth: usize,
    }

    impl StreamFromClosure {
        pub fn new<F>(result_generator: F) -> StreamFromClosure
        where
            F: Fn(usize) -> HandlerStreamStepResult + Send + 'static,
        {
            StreamFromClosure {
                result_generator: Box::new(result_generator),
                nth: 0,
            }
        }
    }

    impl RequestHandler for StreamFromClosure {
        fn handle_streaming_request(&mut self) -> Result<(Option<coppb::Response>, bool)> {
            let result = (self.result_generator)(self.nth);
            self.nth += 1;
            result
        }
    }

    #[test]
    fn test_outdated_request() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            engine,
        ));
        let cm = ConcurrencyManager::new(1.into());
        let copr = Endpoint::<RocksEngine>::new(
            &Config::default(),
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
        );

        // a normal request
        let handler_builder =
            Box::new(|_, _: &_| Ok(UnaryFixture::new(Ok(coppb::Response::default())).into_boxed()));
        let resp =
            block_on(copr.handle_unary_request(ReqContext::default_for_test(), handler_builder))
                .unwrap();
        assert!(resp.get_other_error().is_empty());

        // an outdated request
        let handler_builder =
            Box::new(|_, _: &_| Ok(UnaryFixture::new(Ok(coppb::Response::default())).into_boxed()));
        let outdated_req_ctx = ReqContext::new(
            ReqTag::test,
            Default::default(),
            Vec::new(),
            Duration::from_secs(0),
            None,
            None,
            TimeStamp::max(),
            None,
            PerfLevel::EnableCount,
        );
        assert!(block_on(copr.handle_unary_request(outdated_req_ctx, handler_builder)).is_err());
    }

    #[test]
    fn test_stack_guard() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            engine,
        ));
        let cm = ConcurrencyManager::new(1.into());
        let mut copr = Endpoint::<RocksEngine>::new(
            &Config::default(),
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
        );
        copr.recursion_limit = 100;

        let req = {
            let mut expr = Expr::default();
            for _ in 0..101 {
                let mut e = Expr::default();
                e.mut_children().push(expr);
                expr = e;
            }
            let mut e = Executor::default();
            e.mut_selection().mut_conditions().push(expr);
            let mut dag = DagRequest::default();
            dag.mut_executors().push(e);
            let mut req = coppb::Request::default();
            req.set_tp(REQ_TYPE_DAG);
            req.set_data(dag.write_to_bytes().unwrap());
            req
        };

        let resp = block_on(copr.parse_and_handle_unary_request(req, None));
        assert!(!resp.get_other_error().is_empty());
    }

    #[test]
    fn test_invalid_req_type() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            engine,
        ));
        let cm = ConcurrencyManager::new(1.into());
        let copr = Endpoint::<RocksEngine>::new(
            &Config::default(),
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
        );

        let mut req = coppb::Request::default();
        req.set_tp(9999);

        let resp = block_on(copr.parse_and_handle_unary_request(req, None));
        assert!(!resp.get_other_error().is_empty());
    }

    #[test]
    fn test_invalid_req_body() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            engine,
        ));
        let cm = ConcurrencyManager::new(1.into());
        let copr = Endpoint::<RocksEngine>::new(
            &Config::default(),
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
        );

        let mut req = coppb::Request::default();
        req.set_tp(REQ_TYPE_DAG);
        req.set_data(vec![1, 2, 3]);

        let resp = block_on(copr.parse_and_handle_unary_request(req, None));
        assert!(!resp.get_other_error().is_empty());
    }

    #[test]
    fn test_full() {
        use std::sync::Mutex;

        use tikv_util::yatp_pool::{DefaultTicker, YatpPoolBuilder};

        use crate::storage::kv::{destroy_tls_engine, set_tls_engine};

        let engine = TestEngineBuilder::new().build().unwrap();

        let read_pool = ReadPool::from(
            CoprReadPoolConfig {
                normal_concurrency: 1,
                max_tasks_per_worker_normal: 2,
                ..CoprReadPoolConfig::default_for_test()
            }
            .to_yatp_pool_configs()
            .into_iter()
            .map(|config| {
                let engine = Arc::new(Mutex::new(engine.clone()));
                YatpPoolBuilder::new(DefaultTicker::default())
                    .config(config)
                    .name_prefix("coprocessor_endpoint_test_full")
                    .after_start(move || set_tls_engine(engine.lock().unwrap().clone()))
                    // Safety: we call `set_` and `destroy_` with the same engine type.
                    .before_stop(|| unsafe { destroy_tls_engine::<RocksEngine>() })
                    .build_future_pool()
            })
            .collect::<Vec<_>>(),
        );

        let cm = ConcurrencyManager::new(1.into());
        let copr = Endpoint::<RocksEngine>::new(
            &Config::default(),
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
        );

        let (tx, rx) = mpsc::channel();

        // first 2 requests are processed as normal and laters are returned as errors
        for i in 0..5 {
            let mut response = coppb::Response::default();
            response.set_data(vec![1, 2, i]);

            let mut context = kvrpcpb::Context::default();
            context.set_priority(kvrpcpb::CommandPri::Normal);

            let handler_builder = Box::new(|_, _: &_| {
                Ok(UnaryFixture::new_with_duration(Ok(response), 1000).into_boxed())
            });
            let future = copr.handle_unary_request(ReqContext::default_for_test(), handler_builder);
            let tx = tx.clone();
            thread::spawn(move || {
                tx.send(block_on(future)).unwrap();
            });
            thread::sleep(Duration::from_millis(100));
        }

        // verify
        for _ in 2..5 {
            assert!(rx.recv().unwrap().is_err());
        }
        for i in 0..2 {
            let resp = rx.recv().unwrap().unwrap();
            assert_eq!(resp.get_data(), [1, 2, i]);
            assert!(!resp.has_region_error());
        }
    }

    #[test]
    fn test_error_unary_response() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            engine,
        ));
        let cm = ConcurrencyManager::new(1.into());
        let copr = Endpoint::<RocksEngine>::new(
            &Config::default(),
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
        );

        let handler_builder =
            Box::new(|_, _: &_| Ok(UnaryFixture::new(Err(box_err!("foo"))).into_boxed()));
        let resp =
            block_on(copr.handle_unary_request(ReqContext::default_for_test(), handler_builder))
                .unwrap();
        assert_eq!(resp.get_data().len(), 0);
        assert!(!resp.get_other_error().is_empty());
    }

    #[test]
    fn test_error_streaming_response() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            engine,
        ));
        let cm = ConcurrencyManager::new(1.into());
        let copr = Endpoint::<RocksEngine>::new(
            &Config::default(),
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
        );

        // Fail immediately
        let handler_builder =
            Box::new(|_, _: &_| Ok(StreamFixture::new(vec![Err(box_err!("foo"))]).into_boxed()));
        let resp_vec = block_on_stream(
            copr.handle_stream_request(ReqContext::default_for_test(), handler_builder)
                .unwrap(),
        )
        .collect::<Result<Vec<_>>>()
        .unwrap();
        assert_eq!(resp_vec.len(), 1);
        assert_eq!(resp_vec[0].get_data().len(), 0);
        assert!(!resp_vec[0].get_other_error().is_empty());

        // Fail after some success responses
        let mut responses = Vec::new();
        for i in 0..5 {
            let mut resp = coppb::Response::default();
            resp.set_data(vec![1, 2, i]);
            responses.push(Ok(resp));
        }
        responses.push(Err(box_err!("foo")));

        let handler_builder = Box::new(|_, _: &_| Ok(StreamFixture::new(responses).into_boxed()));
        let resp_vec = block_on_stream(
            copr.handle_stream_request(ReqContext::default_for_test(), handler_builder)
                .unwrap(),
        )
        .collect::<Result<Vec<_>>>()
        .unwrap();
        assert_eq!(resp_vec.len(), 6);
        for (i, resp) in resp_vec.iter().enumerate().take(5) {
            assert_eq!(resp.get_data(), [1, 2, i as u8]);
        }
        assert_eq!(resp_vec[5].get_data().len(), 0);
        assert!(!resp_vec[5].get_other_error().is_empty());
    }

    #[test]
    fn test_empty_streaming_response() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            engine,
        ));
        let cm = ConcurrencyManager::new(1.into());
        let copr = Endpoint::<RocksEngine>::new(
            &Config::default(),
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
        );

        let handler_builder = Box::new(|_, _: &_| Ok(StreamFixture::new(vec![]).into_boxed()));
        let resp_vec = block_on_stream(
            copr.handle_stream_request(ReqContext::default_for_test(), handler_builder)
                .unwrap(),
        )
        .collect::<Result<Vec<_>>>()
        .unwrap();
        assert_eq!(resp_vec.len(), 0);
    }

    // TODO: Test panic?

    #[test]
    fn test_special_streaming_handlers() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            engine,
        ));
        let cm = ConcurrencyManager::new(1.into());
        let copr = Endpoint::<RocksEngine>::new(
            &Config::default(),
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
        );

        // handler returns `finished == true` should not be called again.
        let counter = Arc::new(atomic::AtomicIsize::new(0));
        let counter_clone = Arc::clone(&counter);
        let handler = StreamFromClosure::new(move |nth| match nth {
            0 => {
                let mut resp = coppb::Response::default();
                resp.set_data(vec![1, 2, 7]);
                Ok((Some(resp), true))
            }
            _ => {
                // we cannot use `unreachable!()` here because CpuPool catches panic.
                counter_clone.store(1, atomic::Ordering::SeqCst);
                Err(box_err!("unreachable"))
            }
        });
        let handler_builder = Box::new(move |_, _: &_| Ok(handler.into_boxed()));
        let resp_vec = block_on_stream(
            copr.handle_stream_request(ReqContext::default_for_test(), handler_builder)
                .unwrap(),
        )
        .collect::<Result<Vec<_>>>()
        .unwrap();
        assert_eq!(resp_vec.len(), 1);
        assert_eq!(resp_vec[0].get_data(), [1, 2, 7]);
        assert_eq!(counter.load(atomic::Ordering::SeqCst), 0);

        // handler returns `None` but `finished == false` should not be called again.
        let counter = Arc::new(atomic::AtomicIsize::new(0));
        let counter_clone = Arc::clone(&counter);
        let handler = StreamFromClosure::new(move |nth| match nth {
            0 => {
                let mut resp = coppb::Response::default();
                resp.set_data(vec![1, 2, 13]);
                Ok((Some(resp), false))
            }
            1 => Ok((None, false)),
            _ => {
                counter_clone.store(1, atomic::Ordering::SeqCst);
                Err(box_err!("unreachable"))
            }
        });
        let handler_builder = Box::new(move |_, _: &_| Ok(handler.into_boxed()));
        let resp_vec = block_on_stream(
            copr.handle_stream_request(ReqContext::default_for_test(), handler_builder)
                .unwrap(),
        )
        .collect::<Result<Vec<_>>>()
        .unwrap();
        assert_eq!(resp_vec.len(), 1);
        assert_eq!(resp_vec[0].get_data(), [1, 2, 13]);
        assert_eq!(counter.load(atomic::Ordering::SeqCst), 0);

        // handler returns `Err(..)` should not be called again.
        let counter = Arc::new(atomic::AtomicIsize::new(0));
        let counter_clone = Arc::clone(&counter);
        let handler = StreamFromClosure::new(move |nth| match nth {
            0 => {
                let mut resp = coppb::Response::default();
                resp.set_data(vec![1, 2, 23]);
                Ok((Some(resp), false))
            }
            1 => Err(box_err!("foo")),
            _ => {
                counter_clone.store(1, atomic::Ordering::SeqCst);
                Err(box_err!("unreachable"))
            }
        });
        let handler_builder = Box::new(move |_, _: &_| Ok(handler.into_boxed()));
        let resp_vec = block_on_stream(
            copr.handle_stream_request(ReqContext::default_for_test(), handler_builder)
                .unwrap(),
        )
        .collect::<Result<Vec<_>>>()
        .unwrap();
        assert_eq!(resp_vec.len(), 2);
        assert_eq!(resp_vec[0].get_data(), [1, 2, 23]);
        assert!(!resp_vec[1].get_other_error().is_empty());
        assert_eq!(counter.load(atomic::Ordering::SeqCst), 0);
    }

    #[test]
    fn test_channel_size() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            engine,
        ));
        let cm = ConcurrencyManager::new(1.into());
        let copr = Endpoint::<RocksEngine>::new(
            &Config {
                end_point_stream_channel_size: 3,
                ..Config::default()
            },
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
        );

        let counter = Arc::new(atomic::AtomicIsize::new(0));
        let counter_clone = Arc::clone(&counter);
        let handler = StreamFromClosure::new(move |nth| {
            // produce an infinite stream
            let mut resp = coppb::Response::default();
            resp.set_data(vec![1, 2, nth as u8]);
            counter_clone.fetch_add(1, atomic::Ordering::SeqCst);
            Ok((Some(resp), false))
        });
        let handler_builder = Box::new(move |_, _: &_| Ok(handler.into_boxed()));
        let resp_vec = block_on_stream(
            copr.handle_stream_request(ReqContext::default_for_test(), handler_builder)
                .unwrap(),
        )
        .take(7)
        .collect::<Result<Vec<_>>>()
        .unwrap();
        assert_eq!(resp_vec.len(), 7);
        assert!(counter.load(atomic::Ordering::SeqCst) < 14);
    }

    #[test]
    fn test_handle_time() {
        use tikv_util::config::ReadableDuration;

        /// Asserted that the snapshot can be retrieved in 500ms.
        const SNAPSHOT_DURATION_MS: i64 = 500;

        /// Asserted that the delay caused by OS scheduling other tasks is smaller than 200ms.
        /// This is mostly for CI.
        const HANDLE_ERROR_MS: i64 = 200;

        /// The acceptable error range for a coarse timer. Note that we use CLOCK_MONOTONIC_COARSE
        /// which can be slewed by time adjustment code (e.g., NTP, PTP).
        const COARSE_ERROR_MS: i64 = 50;

        /// The duration that payload executes.
        const PAYLOAD_SMALL: i64 = 3000;
        const PAYLOAD_LARGE: i64 = 6000;

        let engine = TestEngineBuilder::new().build().unwrap();

        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig {
                low_concurrency: 1,
                normal_concurrency: 1,
                high_concurrency: 1,
                ..CoprReadPoolConfig::default_for_test()
            },
            engine,
        ));

        let config = Config {
            end_point_request_max_handle_duration: ReadableDuration::millis(
                (PAYLOAD_SMALL + PAYLOAD_LARGE) as u64 * 2,
            ),
            ..Default::default()
        };

        let cm = ConcurrencyManager::new(1.into());
        let copr = Endpoint::<RocksEngine>::new(
            &config,
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
        );

        let (tx, rx) = std::sync::mpsc::channel();

        // A request that requests execution details.
        let mut req_with_exec_detail = ReqContext::default_for_test();
        req_with_exec_detail.context.set_record_time_stat(true);

        {
            let mut wait_time: i64 = 0;

            // Request 1: Unary, success response.
            let handler_builder = Box::new(|_, _: &_| {
                Ok(UnaryFixture::new_with_duration(
                    Ok(coppb::Response::default()),
                    PAYLOAD_SMALL as u64,
                )
                .into_boxed())
            });
            let resp_future_1 =
                copr.handle_unary_request(req_with_exec_detail.clone(), handler_builder);
            let sender = tx.clone();
            thread::spawn(move || sender.send(vec![block_on(resp_future_1).unwrap()]).unwrap());
            // Sleep a while to make sure that thread is spawn and snapshot is taken.
            thread::sleep(Duration::from_millis(SNAPSHOT_DURATION_MS as u64));

            // Request 2: Unary, error response.
            let handler_builder = Box::new(|_, _: &_| {
                Ok(
                    UnaryFixture::new_with_duration(Err(box_err!("foo")), PAYLOAD_LARGE as u64)
                        .into_boxed(),
                )
            });
            let resp_future_2 =
                copr.handle_unary_request(req_with_exec_detail.clone(), handler_builder);
            let sender = tx.clone();
            thread::spawn(move || sender.send(vec![block_on(resp_future_2).unwrap()]).unwrap());
            thread::sleep(Duration::from_millis(SNAPSHOT_DURATION_MS as u64));

            // Response 1
            let resp = &rx.recv().unwrap()[0];
            assert!(resp.get_other_error().is_empty());
            assert_ge!(
                resp.get_exec_details()
                    .get_time_detail()
                    .get_process_wall_time_ms(),
                PAYLOAD_SMALL - COARSE_ERROR_MS
            );
            assert_lt!(
                resp.get_exec_details()
                    .get_time_detail()
                    .get_process_wall_time_ms(),
                PAYLOAD_SMALL + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
            assert_ge!(
                resp.get_exec_details()
                    .get_time_detail()
                    .get_wait_wall_time_ms(),
                wait_time - HANDLE_ERROR_MS - COARSE_ERROR_MS
            );
            assert_lt!(
                resp.get_exec_details()
                    .get_time_detail()
                    .get_wait_wall_time_ms(),
                wait_time + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
            wait_time += PAYLOAD_SMALL - SNAPSHOT_DURATION_MS;

            // Response 2
            let resp = &rx.recv().unwrap()[0];
            assert!(!resp.get_other_error().is_empty());
            assert_ge!(
                resp.get_exec_details()
                    .get_time_detail()
                    .get_process_wall_time_ms(),
                PAYLOAD_LARGE - COARSE_ERROR_MS
            );
            assert_lt!(
                resp.get_exec_details()
                    .get_time_detail()
                    .get_process_wall_time_ms(),
                PAYLOAD_LARGE + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
            assert_ge!(
                resp.get_exec_details()
                    .get_time_detail()
                    .get_wait_wall_time_ms(),
                wait_time - HANDLE_ERROR_MS - COARSE_ERROR_MS
            );
            assert_lt!(
                resp.get_exec_details()
                    .get_time_detail()
                    .get_wait_wall_time_ms(),
                wait_time + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
        }

        {
            // Test multi-stage tasks
            // Request 1: Unary, success response.
            let handler_builder = Box::new(|_, _: &_| {
                Ok(UnaryFixture::new_with_duration_yieldable(
                    Ok(coppb::Response::default()),
                    PAYLOAD_SMALL as u64,
                )
                .into_boxed())
            });
            let resp_future_1 =
                copr.handle_unary_request(req_with_exec_detail.clone(), handler_builder);
            let sender = tx.clone();
            thread::spawn(move || sender.send(vec![block_on(resp_future_1).unwrap()]).unwrap());
            // Sleep a while to make sure that thread is spawn and snapshot is taken.
            thread::sleep(Duration::from_millis(SNAPSHOT_DURATION_MS as u64));

            // Request 2: Unary, error response.
            let handler_builder = Box::new(|_, _: &_| {
                Ok(UnaryFixture::new_with_duration_yieldable(
                    Err(box_err!("foo")),
                    PAYLOAD_LARGE as u64,
                )
                .into_boxed())
            });
            let resp_future_2 =
                copr.handle_unary_request(req_with_exec_detail.clone(), handler_builder);
            let sender = tx.clone();
            thread::spawn(move || sender.send(vec![block_on(resp_future_2).unwrap()]).unwrap());
            thread::sleep(Duration::from_millis(SNAPSHOT_DURATION_MS as u64));

            // Response 1
            //
            // Note: `process_wall_time_ms` includes `total_process_time` and `total_suspend_time`.
            // Someday it will be separated, but for now, let's just consider the combination.
            //
            // In the worst case, `total_suspend_time` could be totally req2 payload. So here:
            // req1 payload <= process time <= (req1 payload + req2 payload)
            let resp = &rx.recv().unwrap()[0];
            assert!(resp.get_other_error().is_empty());
            assert_ge!(
                resp.get_exec_details()
                    .get_time_detail()
                    .get_process_wall_time_ms(),
                PAYLOAD_SMALL - COARSE_ERROR_MS
            );
            assert_lt!(
                resp.get_exec_details()
                    .get_time_detail()
                    .get_process_wall_time_ms(),
                PAYLOAD_SMALL + PAYLOAD_LARGE + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );

            // Response 2
            //
            // Note: `process_wall_time_ms` includes `total_process_time` and `total_suspend_time`.
            // Someday it will be separated, but for now, let's just consider the combination.
            //
            // In the worst case, `total_suspend_time` could be totally req1 payload. So here:
            // req2 payload <= process time <= (req1 payload + req2 payload)
            let resp = &rx.recv().unwrap()[0];
            assert!(!resp.get_other_error().is_empty());
            assert_ge!(
                resp.get_exec_details()
                    .get_time_detail()
                    .get_process_wall_time_ms(),
                PAYLOAD_LARGE - COARSE_ERROR_MS
            );
            assert_lt!(
                resp.get_exec_details()
                    .get_time_detail()
                    .get_process_wall_time_ms(),
                PAYLOAD_SMALL + PAYLOAD_LARGE + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
        }

        {
            let mut wait_time: i64 = 0;

            // Request 1: Unary, success response.
            let handler_builder = Box::new(|_, _: &_| {
                Ok(UnaryFixture::new_with_duration(
                    Ok(coppb::Response::default()),
                    PAYLOAD_LARGE as u64,
                )
                .into_boxed())
            });
            let resp_future_1 =
                copr.handle_unary_request(req_with_exec_detail.clone(), handler_builder);
            let sender = tx.clone();
            thread::spawn(move || sender.send(vec![block_on(resp_future_1).unwrap()]).unwrap());
            // Sleep a while to make sure that thread is spawn and snapshot is taken.
            thread::sleep(Duration::from_millis(SNAPSHOT_DURATION_MS as u64));

            // Request 2: Stream.
            let handler_builder = Box::new(|_, _: &_| {
                Ok(StreamFixture::new_with_duration(
                    vec![
                        Ok(coppb::Response::default()),
                        Err(box_err!("foo")),
                        Ok(coppb::Response::default()),
                    ],
                    vec![
                        PAYLOAD_SMALL as u64,
                        PAYLOAD_LARGE as u64,
                        PAYLOAD_SMALL as u64,
                    ],
                )
                .into_boxed())
            });
            let resp_future_3 = copr
                .handle_stream_request(req_with_exec_detail, handler_builder)
                .unwrap()
                .map(|x| x.map(|x| x.into()));
            thread::spawn(move || {
                tx.send(
                    block_on_stream(resp_future_3)
                        .collect::<Result<Vec<_>>>()
                        .unwrap(),
                )
                .unwrap()
            });

            // Response 1
            let resp = &rx.recv().unwrap()[0];
            assert!(resp.get_other_error().is_empty());
            assert_ge!(
                resp.get_exec_details()
                    .get_time_detail()
                    .get_process_wall_time_ms(),
                PAYLOAD_LARGE - COARSE_ERROR_MS
            );
            assert_lt!(
                resp.get_exec_details()
                    .get_time_detail()
                    .get_process_wall_time_ms(),
                PAYLOAD_LARGE + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
            assert_ge!(
                resp.get_exec_details()
                    .get_time_detail()
                    .get_wait_wall_time_ms(),
                wait_time - HANDLE_ERROR_MS - COARSE_ERROR_MS
            );
            assert_lt!(
                resp.get_exec_details()
                    .get_time_detail()
                    .get_wait_wall_time_ms(),
                wait_time + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
            wait_time += PAYLOAD_LARGE - SNAPSHOT_DURATION_MS;

            // Response 2
            let resp = &rx.recv().unwrap();
            assert_eq!(resp.len(), 2);
            assert!(resp[0].get_other_error().is_empty());
            assert_ge!(
                resp[0]
                    .get_exec_details()
                    .get_time_detail()
                    .get_process_wall_time_ms(),
                PAYLOAD_SMALL - COARSE_ERROR_MS
            );
            assert_lt!(
                resp[0]
                    .get_exec_details()
                    .get_time_detail()
                    .get_process_wall_time_ms(),
                PAYLOAD_SMALL + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
            assert_ge!(
                resp[0]
                    .get_exec_details()
                    .get_time_detail()
                    .get_wait_wall_time_ms(),
                wait_time - HANDLE_ERROR_MS - COARSE_ERROR_MS
            );
            assert_lt!(
                resp[0]
                    .get_exec_details()
                    .get_time_detail()
                    .get_wait_wall_time_ms(),
                wait_time + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );

            assert!(!resp[1].get_other_error().is_empty());
            assert_ge!(
                resp[1]
                    .get_exec_details()
                    .get_time_detail()
                    .get_process_wall_time_ms(),
                PAYLOAD_LARGE - COARSE_ERROR_MS
            );
            assert_lt!(
                resp[1]
                    .get_exec_details()
                    .get_time_detail()
                    .get_process_wall_time_ms(),
                PAYLOAD_LARGE + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
            assert_ge!(
                resp[1]
                    .get_exec_details()
                    .get_time_detail()
                    .get_wait_wall_time_ms(),
                wait_time - HANDLE_ERROR_MS - COARSE_ERROR_MS
            );
            assert_lt!(
                resp[1]
                    .get_exec_details()
                    .get_time_detail()
                    .get_wait_wall_time_ms(),
                wait_time + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
        }
    }

    #[test]
    fn test_exceed_deadline() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            engine,
        ));
        let cm = ConcurrencyManager::new(1.into());
        let copr = Endpoint::<RocksEngine>::new(
            &Config::default(),
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
        );

        {
            let handler_builder = Box::new(|_, _: &_| {
                thread::sleep(Duration::from_millis(600));
                Ok(UnaryFixture::new(Ok(coppb::Response::default())).into_boxed())
            });

            let mut config = ReqContext::default_for_test();
            config.deadline = Deadline::from_now(Duration::from_millis(500));

            let resp = block_on(copr.handle_unary_request(config, handler_builder)).unwrap();
            assert_eq!(resp.get_data().len(), 0);
            assert!(!resp.get_other_error().is_empty());
        }

        {
            let handler_builder = Box::new(|_, _: &_| {
                Ok(
                    UnaryFixture::new_with_duration_yieldable(Ok(coppb::Response::default()), 1500)
                        .into_boxed(),
                )
            });

            let mut config = ReqContext::default_for_test();
            config.deadline = Deadline::from_now(Duration::from_millis(500));

            let resp = block_on(copr.handle_unary_request(config, handler_builder)).unwrap();
            assert_eq!(resp.get_data().len(), 0);
            assert!(!resp.get_other_error().is_empty());
        }
    }

    #[test]
    fn test_check_memory_locks() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            engine,
        ));
        let cm = ConcurrencyManager::new(1.into());
        let key = Key::from_raw(b"key");
        let guard = block_on(cm.lock_key(&key));
        guard.with_lock(|lock| {
            *lock = Some(txn_types::Lock::new(
                LockType::Put,
                b"key".to_vec(),
                10.into(),
                100,
                Some(vec![]),
                0.into(),
                1,
                20.into(),
            ));
        });

        let config = Config::default();
        let copr = Endpoint::<RocksEngine>::new(
            &config,
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
        );
        let mut req = coppb::Request::default();
        req.mut_context().set_isolation_level(IsolationLevel::Si);
        req.set_start_ts(100);
        req.set_tp(REQ_TYPE_DAG);
        let mut key_range = coppb::KeyRange::default();
        key_range.set_start(b"a".to_vec());
        key_range.set_end(b"z".to_vec());
        req.mut_ranges().push(key_range);
        let mut dag = DagRequest::default();
        dag.mut_executors().push(Executor::default());
        req.set_data(dag.write_to_bytes().unwrap());

        let resp = block_on(copr.parse_and_handle_unary_request(req, None));
        assert_eq!(resp.get_locked().get_key(), b"key");
    }
}
