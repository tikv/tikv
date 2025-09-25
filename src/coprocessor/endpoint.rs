// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::Cow, fmt::Display, future::Future, iter::FromIterator, marker::PhantomData, mem,
    sync::Arc, time::Duration,
};

use ::tracker::{
    GLOBAL_TRACKERS, RequestInfo, RequestType, set_tls_tracker_token, track, with_tls_tracker,
};
use anyhow::anyhow;
use api_version::{KvFormat, dispatch_api_version};
use async_stream::try_stream;
use concurrency_manager::ConcurrencyManager;
use engine_traits::PerfLevel;
use futures::{
    channel::{mpsc, oneshot},
    future::Either,
    prelude::*,
};
use kvproto::{coprocessor as coppb, errorpb, kvrpcpb, kvrpcpb::CommandPri, metapb};
use online_config::ConfigManager;
use protobuf::{CodedInputStream, Message};
use resource_control::{ResourceGroupManager, ResourceLimiter, TaskMetadata};
use resource_metering::{
    FutureExt, ResourceTagFactory, StreamExt, record_logical_read_bytes, record_network_in_bytes,
    record_network_out_bytes,
};
use tidb_query_common::{
    error::StorageError,
    execute_stats::ExecSummary,
    storage::{FindRegionResult, RegionStorageAccessor, Result as StorageResult},
};
use tikv_alloc::trace::MemoryTraceGuard;
use tikv_kv::{ExtraRegionOverride, SnapshotExt};
use tikv_util::{
    deadline::set_deadline_exceeded_busy_error,
    memory::{MemoryQuota, OwnedAllocated},
    quota_limiter::QuotaLimiter,
    store::find_peer,
    time::Instant,
};
use tipb::{AnalyzeReq, AnalyzeType, ChecksumRequest, ChecksumScanOn, DagRequest, ExecType};
use tokio::sync::Semaphore;
use txn_types::Lock;

use super::config_manager::CopConfigManager;
use crate::{
    coprocessor::{
        cache::CachedRequestHandler, interceptors::*, metrics::*,
        statistics::analyze_context::AnalyzeContext, tracker::Tracker, *,
    },
    read_pool::ReadPoolHandle,
    server::Config,
    storage::{
        self, Engine, Snapshot, SnapshotStore,
        kv::{self, SnapContext, with_tls_engine},
        mvcc::Error as MvccError,
        need_check_locks, need_check_locks_in_replica_read,
    },
    tikv_util::time::InstantExt,
};

/// Requests that need time of less than `LIGHT_TASK_THRESHOLD` is considered as
/// light ones, which means they don't need a permit from the semaphore before
/// execution.
const LIGHT_TASK_THRESHOLD: Duration = Duration::from_millis(5);

/// A pool to build and run Coprocessor request handlers.
#[derive(Clone)]
pub struct Endpoint<E: Engine> {
    /// The thread pool to run Coprocessor requests.
    read_pool: ReadPoolHandle,

    /// The concurrency limiter of the coprocessor.
    semaphore: Option<Arc<Semaphore>>,
    /// The memory quota for coprocessor requests.
    memory_quota: Arc<MemoryQuota>,

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

    quota_limiter: Arc<QuotaLimiter>,
    resource_ctl: Option<Arc<ResourceGroupManager>>,

    _phantom: PhantomData<E>,
}

/// The result of parsing a Coprocessor request.
pub struct ParseCopRequestResult<Snap> {
    req_tag: ReqTag,
    req_ctx: ReqContext,
    handler_builder: RequestHandlerBuilder<Snap>,
}

impl<Snap> ParseCopRequestResult<Snap> {
    #[cfg(test)]
    pub fn default_for_test(handler_builder: RequestHandlerBuilder<Snap>) -> Self {
        Self {
            req_tag: ReqTag::test,
            req_ctx: ReqContext::default_for_test(),
            handler_builder,
        }
    }
}

impl<E: Engine> tikv_util::AssertSend for Endpoint<E> {}

impl<E: Engine> Endpoint<E> {
    pub fn new(
        cfg: &Config,
        read_pool: ReadPoolHandle,
        concurrency_manager: ConcurrencyManager,
        resource_tag_factory: ResourceTagFactory,
        quota_limiter: Arc<QuotaLimiter>,
        resource_ctl: Option<Arc<ResourceGroupManager>>,
    ) -> Self {
        let semaphore = match &read_pool {
            ReadPoolHandle::Yatp { .. } => {
                Some(Arc::new(Semaphore::new(cfg.end_point_max_concurrency)))
            }
            _ => None,
        };
        let memory_quota = Arc::new(MemoryQuota::new(cfg.end_point_memory_quota.0 as _));
        register_coprocessor_memory_quota_metrics(memory_quota.clone());
        Self {
            read_pool,
            semaphore,
            memory_quota,
            concurrency_manager,
            perf_level: cfg.end_point_perf_level,
            resource_tag_factory,
            recursion_limit: cfg.end_point_recursion_limit,
            batch_row_limit: cfg.end_point_batch_row_limit,
            stream_batch_row_limit: cfg.end_point_stream_batch_row_limit,
            stream_channel_size: cfg.end_point_stream_channel_size,
            max_handle_duration: cfg.end_point_request_max_handle_duration().0,
            slow_log_threshold: cfg.end_point_slow_log_threshold.0,
            quota_limiter,
            resource_ctl,
            _phantom: Default::default(),
        }
    }

    pub fn config_manager(&self) -> Box<dyn ConfigManager> {
        Box::new(CopConfigManager::new(self.memory_quota.clone()))
    }

    fn check_memory_locks(&self, req_ctx: &ReqContext) -> Result<()> {
        let start_ts = req_ctx.txn_start_ts;
        if !req_ctx.context.get_stale_read() {
            self.concurrency_manager
                .update_max_ts(start_ts, || format!("coprocessor-{}", start_ts))?;
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

    /// Parse the raw `Request` to create `RequestHandlerBuilder` and
    /// `ReqContext`. Returns `Err` if fails.
    ///
    /// It also checks if there are locks in memory blocking this read request.
    fn parse_request_and_check_memory_locks(
        &self,
        req: coppb::Request,
        peer: Option<String>,
        is_streaming: bool,
    ) -> Result<ParseCopRequestResult<E::IMSnap>> {
        dispatch_api_version!(req.get_context().get_api_version(), {
            self.parse_request_and_check_memory_locks_impl::<API>(req, peer, is_streaming)
        })
    }

    /// Parse the raw `Request` to create `RequestHandlerBuilder` and
    /// `ReqContext`. Returns `Err` if fails.
    ///
    /// It also checks if there are locks in memory blocking this read request.
    fn parse_request_and_check_memory_locks_impl<F: KvFormat>(
        &self,
        mut req: coppb::Request,
        peer: Option<String>,
        is_streaming: bool,
    ) -> Result<ParseCopRequestResult<E::IMSnap>> {
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
        let handler_builder: RequestHandlerBuilder<E::IMSnap>;
        let req_tag: ReqTag;
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
                req_tag = if table_scan {
                    ReqTag::select
                } else {
                    ReqTag::index
                };

                req_ctx = ReqContext::new(
                    context,
                    ranges,
                    self.max_handle_duration,
                    peer,
                    Some(is_desc_scan),
                    start_ts.into(),
                    cache_match_version,
                    self.perf_level,
                    false,
                );
                with_tls_tracker(|tracker| {
                    tracker.req_info.request_type = RequestType::CoprocessorDag;
                    tracker.req_info.start_ts = start_ts;
                });

                self.check_memory_locks(&req_ctx)?;

                let batch_row_limit = self.get_batch_row_limit(is_streaming);
                let quota_limiter = self.quota_limiter.clone();
                handler_builder = Box::new(move |snap, req_ctx| {
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
                    dag::DagHandlerBuilder::<_, _, F>::new(
                        dag,
                        req_ctx.ranges.clone(),
                        store,
                        ExtraSnapStoreAccessor::<E>::new(req_ctx.clone()),
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

                req_tag = match analyze.get_tp() {
                    AnalyzeType::TypeIndex | AnalyzeType::TypeCommonHandle => ReqTag::analyze_index,
                    AnalyzeType::TypeColumn | AnalyzeType::TypeMixed => ReqTag::analyze_table,
                    AnalyzeType::TypeFullSampling => ReqTag::analyze_full_sampling,
                    AnalyzeType::TypeSampleIndex => unimplemented!(),
                };
                req_ctx = ReqContext::new(
                    context,
                    ranges,
                    self.max_handle_duration,
                    peer,
                    None,
                    start_ts.into(),
                    cache_match_version,
                    self.perf_level,
                    false,
                );
                with_tls_tracker(|tracker| {
                    tracker.req_info.request_type = RequestType::CoprocessorAnalyze;
                    tracker.req_info.start_ts = start_ts;
                });

                self.check_memory_locks(&req_ctx)?;

                let quota_limiter = self.quota_limiter.clone();

                handler_builder = Box::new(move |snap, req_ctx| {
                    AnalyzeContext::<_, F>::new(
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

                req_tag = if table_scan {
                    ReqTag::checksum_table
                } else {
                    ReqTag::checksum_index
                };
                req_ctx = ReqContext::new(
                    context,
                    ranges,
                    self.max_handle_duration,
                    peer,
                    None,
                    start_ts.into(),
                    cache_match_version,
                    self.perf_level,
                    // Checksum is allowed during the flashback period to make sure the tool such
                    // like BR can work.
                    true,
                );

                with_tls_tracker(|tracker| {
                    tracker.req_info.request_type = RequestType::CoprocessorChecksum;
                    tracker.req_info.start_ts = start_ts;
                });

                self.check_memory_locks(&req_ctx)?;

                handler_builder = Box::new(move |snap, req_ctx| {
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

        Ok(ParseCopRequestResult {
            req_tag,
            req_ctx,
            handler_builder,
        })
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
    fn async_in_memory_snapshot(
        engine: &mut E,
        ctx: &ReqContext,
    ) -> impl std::future::Future<Output = Result<E::IMSnap>> {
        let mut snap_ctx = SnapContext {
            pb_ctx: &ctx.context,
            start_ts: Some(ctx.txn_start_ts),
            allowed_in_flashback: ctx.allowed_in_flashback,
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
        kv::in_memory_snapshot(engine, snap_ctx).map_err(Error::from)
    }

    /// The real implementation of handling a unary request.
    ///
    /// It first retrieves a snapshot, then builds the `RequestHandler` over the
    /// snapshot and the given `handler_builder`. Finally, it calls the unary
    /// request interface of the `RequestHandler` to process the request and
    /// produce a result.
    async fn handle_unary_request_impl(
        semaphore: Option<Arc<Semaphore>>,
        mut tracker: Box<Tracker<E>>,
        handler_builder: RequestHandlerBuilder<E::IMSnap>,
    ) -> Result<MemoryTraceGuard<coppb::Response>> {
        with_tls_tracker(|tracker1| {
            record_network_in_bytes(tracker1.metrics.grpc_req_size);
        });
        // When this function is being executed, it may be queued for a long time, so
        // that deadline may exceed.
        tracker.on_scheduled();
        tracker.req_ctx.deadline.check()?;

        // Safety: spawning this function using a `FuturePool` ensures that a TLS engine
        // exists.
        let snapshot = unsafe {
            with_tls_engine(|engine| Self::async_in_memory_snapshot(engine, &tracker.req_ctx))
        }
        .await?;
        let latest_buckets = snapshot.ext().get_buckets();

        let region_cache_snap = snapshot.ext().in_memory_engine_hit();
        tracker.adjust_snapshot_type(region_cache_snap);

        // Check if the buckets version is latest.
        // skip if request don't carry this bucket version.
        if let Some(ref buckets) = latest_buckets
            && buckets.version > tracker.req_ctx.context.buckets_version
            && tracker.req_ctx.context.buckets_version != 0
        {
            let mut bucket_not_match = errorpb::BucketVersionNotMatch::default();
            bucket_not_match.set_version(buckets.version);
            bucket_not_match.set_keys(buckets.keys.clone().into());
            let mut err = errorpb::Error::default();
            err.set_bucket_version_not_match(bucket_not_match);
            return Err(Error::Region(err));
        }
        // When snapshot is retrieved, deadline may exceed.
        tracker.on_snapshot_finished();
        tracker.req_ctx.deadline.check()?;
        tracker.buckets = latest_buckets;
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
        let handle_request_future = track(handle_request_future, tracker.as_mut());

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
        record_logical_read_bytes(exec_details_v2.get_scan_detail_v2().processed_versions_size);
        let mut resp = match result {
            Ok(resp) => {
                let resp_size = resp.data.len() as u64;
                COPR_RESP_SIZE.inc_by(resp_size);
                record_network_out_bytes(resp_size);
                resp
            }
            Err(e) => {
                if let Error::DefaultNotFound(errmsg) = &e {
                    error!("default not found in coprocessor request processing";
                        "err" => errmsg,
                        "reqCtx" => ?&tracker.req_ctx,
                    );
                }
                make_error_response(e).into()
            }
        };
        resp.set_exec_details(exec_details);
        resp.set_exec_details_v2(exec_details_v2);
        resp.set_latest_buckets_version(buckets_version);
        Ok(resp)
    }

    /// Handle a unary request and run on the read pool.
    ///
    /// Returns `Err(err)` if the read pool is full. Returns `Ok(future)` in
    /// other cases. The future inside may be an error however.
    fn handle_unary_request(
        &self,
        r: ParseCopRequestResult<E::IMSnap>,
    ) -> impl Future<Output = Result<MemoryTraceGuard<coppb::Response>>> {
        let req_ctx = r.req_ctx;
        let priority = req_ctx.context.get_priority();
        let task_id = req_ctx.build_task_id();
        let key_ranges: Vec<_> = req_ctx
            .ranges
            .iter()
            .map(|key_range| (key_range.get_start().to_vec(), key_range.get_end().to_vec()))
            .collect();
        let resource_tag = self
            .resource_tag_factory
            .new_tag_with_key_ranges(&req_ctx.context, key_ranges);
        let mut allocated_bytes = resource_tag.approximate_heap_size();

        let metadata = TaskMetadata::from_ctx(req_ctx.context.get_resource_control_context());
        let resource_limiter = self.resource_ctl.as_ref().and_then(|r| {
            r.get_resource_limiter(
                req_ctx
                    .context
                    .get_resource_control_context()
                    .get_resource_group_name(),
                req_ctx.context.get_request_source(),
                req_ctx
                    .context
                    .get_resource_control_context()
                    .get_override_priority(),
            )
        });
        // box the tracker so that moving it is cheap.
        let tracker = Box::new(Tracker::new(req_ctx, r.req_tag, self.slow_log_threshold));
        allocated_bytes += tracker.approximate_mem_size();

        let (tx, rx) = oneshot::channel();
        let future =
            Self::handle_unary_request_impl(self.semaphore.clone(), tracker, r.handler_builder)
                .in_resource_metering_tag(resource_tag)
                .map(move |res| {
                    let _ = tx.send(res);
                });
        let res = self.read_pool_spawn_with_memory_quota_check(
            allocated_bytes,
            future,
            priority,
            task_id,
            metadata,
            resource_limiter,
        );
        async move {
            res?;
            rx.map_err(|_| Error::MaxPendingTasksExceeded).await?
        }
    }

    /// Parses and handles a unary request. Returns a future that will never
    /// fail. If there are errors during parsing or handling, they will be
    /// converted into a `Response` as the success result of the future.
    #[inline]
    pub fn parse_and_handle_unary_request(
        &self,
        mut req: coppb::Request,
        peer: Option<String>,
    ) -> impl Future<Output = MemoryTraceGuard<coppb::Response>> {
        let tracker = GLOBAL_TRACKERS.insert(::tracker::Tracker::new(RequestInfo::new(
            req.get_context(),
            RequestType::Unknown,
            req.start_ts,
        )));
        // Check the load of the read pool. If it's too busy, generate and return
        // error in the gRPC thread to avoid waiting in the queue of the read pool.
        if let Err(busy_err) = self.read_pool.check_busy_threshold(Duration::from_millis(
            req.get_context().get_busy_threshold_ms() as u64,
        )) {
            let mut pb_error = errorpb::Error::new();
            pb_error.set_server_is_busy(busy_err);
            let resp = make_error_response(Error::Region(pb_error));
            return Either::Left(async move { resp.into() });
        }

        let result_of_batch = self.process_batch_tasks(&mut req, &peer);
        set_tls_tracker_token(tracker);
        with_tls_tracker(|tracker| {
            tracker.metrics.grpc_req_size = req.compute_size() as u64;
        });
        let result_of_future = self
            .parse_request_and_check_memory_locks(req, peer, false)
            .map(|r| self.handle_unary_request(r));
        with_tls_tracker(|tracker| {
            tracker.metrics.grpc_process_nanos =
                tracker.req_info.begin.saturating_elapsed().as_nanos() as u64;
        });
        let fut = async move {
            let res = match result_of_future {
                Err(e) => {
                    let mut res = make_error_response(e);
                    let batch_res = result_of_batch.await;
                    res.set_batch_responses(batch_res.into());
                    res.into()
                }
                Ok(handle_fut) => {
                    let (handle_res, batch_res) = futures::join!(handle_fut, result_of_batch);
                    let mut res = handle_res.unwrap_or_else(|e| make_error_response(e).into());
                    res.set_batch_responses(batch_res.into());
                    GLOBAL_TRACKERS.with_tracker(tracker, |tracker| {
                        let exec_detail_v2 = res.mut_exec_details_v2();
                        tracker.write_scan_detail(exec_detail_v2.mut_scan_detail_v2());
                        tracker.merge_time_detail(exec_detail_v2.mut_time_detail_v2());
                    });
                    res
                }
            };
            GLOBAL_TRACKERS.remove(tracker);
            res
        };
        Either::Right(fut)
    }

    // process_batch_tasks process the input batched coprocessor tasks if any,
    // prepare all the requests and schedule them into the read pool, then
    // collect all the responses and convert them into the `StoreBatchResponse`
    // type.
    pub fn process_batch_tasks(
        &self,
        req: &mut coppb::Request,
        peer: &Option<String>,
    ) -> impl Future<Output = Vec<coppb::StoreBatchTaskResponse>> {
        let mut batch_futs = Vec::with_capacity(req.tasks.len());
        let batch_reqs: Vec<(coppb::Request, u64)> = req
            .take_tasks()
            .iter_mut()
            .map(|task| {
                let mut new_req = req.clone();
                // Disable the coprocessor cache path for the batched tasks, the
                // coprocessor cache related fields are not passed in the "task" by now.
                new_req.is_cache_enabled = false;
                new_req.ranges = task.take_ranges();
                let new_context = new_req.mut_context();
                new_context.set_region_id(task.get_region_id());
                new_context.set_region_epoch(task.take_region_epoch());
                new_context.set_peer(task.take_peer());
                (new_req, task.get_task_id())
            })
            .collect();
        for (cur_req, task_id) in batch_reqs.into_iter() {
            let request_info = RequestInfo::new(
                cur_req.get_context(),
                RequestType::Unknown,
                cur_req.start_ts,
            );
            let mut response = coppb::StoreBatchTaskResponse::new();
            response.set_task_id(task_id);
            match self.parse_request_and_check_memory_locks(cur_req, peer.clone(), false) {
                Ok(r) => {
                    let cur_tracker = GLOBAL_TRACKERS.insert(::tracker::Tracker::new(request_info));
                    set_tls_tracker_token(cur_tracker);
                    let fut = self.handle_unary_request(r);
                    let fut = async move {
                        let res = fut.await;
                        match res {
                            Ok(mut resp) => {
                                response.set_data(resp.take_data());
                                if let Some(err) = resp.region_error.take() {
                                    response.set_region_error(err);
                                }
                                if let Some(lock_info) = resp.locked.take() {
                                    response.set_locked(lock_info);
                                }
                                response.set_other_error(resp.take_other_error());
                                // keep the exec details already generated.
                                response.set_exec_details_v2(resp.take_exec_details_v2());
                                GLOBAL_TRACKERS.with_tracker(cur_tracker, |tracker| {
                                    tracker.write_scan_detail(
                                        response.mut_exec_details_v2().mut_scan_detail_v2(),
                                    );
                                });
                            }
                            Err(e) => {
                                make_error_batch_response(&mut response, e);
                            }
                        }
                        GLOBAL_TRACKERS.remove(cur_tracker);
                        response
                    };

                    batch_futs.push(future::Either::Left(fut));
                }
                Err(e) => batch_futs.push(future::Either::Right(async move {
                    make_error_batch_response(&mut response, e);
                    response
                })),
            }
        }
        stream::FuturesOrdered::from_iter(batch_futs).collect()
    }

    /// The real implementation of handling a stream request.
    ///
    /// It first retrieves a snapshot, then builds the `RequestHandler` over the
    /// snapshot and the given `handler_builder`. Finally, it calls the stream
    /// request interface of the `RequestHandler` multiple times to process the
    /// request and produce multiple results.
    fn handle_stream_request_impl(
        semaphore: Option<Arc<Semaphore>>,
        mut tracker: Box<Tracker<E>>,
        handler_builder: RequestHandlerBuilder<E::IMSnap>,
    ) -> impl futures::stream::Stream<Item = Result<coppb::Response>> {
        try_stream! {
            let _permit = if let Some(semaphore) = semaphore.as_ref() {
                Some(semaphore.acquire().await.expect("the semaphore never be closed"))
            } else {
                None
            };

            with_tls_tracker(|tracker| {
                record_network_in_bytes(tracker.metrics.grpc_req_size);
            });
            // When this function is being executed, it may be queued for a long time, so that
            // deadline may exceed.
            tracker.on_scheduled();
            tracker.req_ctx.deadline.check()?;

            // Safety: spawning this function using a `FuturePool` ensures that a TLS engine
            // exists.
            let snapshot = unsafe {
                with_tls_engine(|engine| Self::async_in_memory_snapshot(engine, &tracker.req_ctx))
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

                    let result = handler.handle_streaming_request().await;

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
                        let resp_size = resp.data.len() as u64;
                        COPR_RESP_SIZE.inc_by(resp_size);
                        record_network_out_bytes(resp_size);
                        record_logical_read_bytes(exec_details_v2.get_scan_detail_v2().processed_versions_size);
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
    /// Returns `Err(err)` if the read pool is full. Returns `Ok(stream)` in
    /// other cases. The stream inside may produce errors however.
    fn handle_stream_request(
        &self,
        r: ParseCopRequestResult<E::IMSnap>,
    ) -> Result<impl futures::stream::Stream<Item = Result<coppb::Response>>> {
        let req_ctx = r.req_ctx;
        let (tx, rx) = mpsc::channel::<Result<coppb::Response>>(self.stream_channel_size);
        let priority = req_ctx.context.get_priority();
        let metadata = TaskMetadata::from_ctx(req_ctx.context.get_resource_control_context());
        let resource_limiter = self.resource_ctl.as_ref().and_then(|r| {
            r.get_resource_limiter(
                req_ctx
                    .context
                    .get_resource_control_context()
                    .get_resource_group_name(),
                req_ctx.context.get_request_source(),
                req_ctx
                    .context
                    .get_resource_control_context()
                    .get_override_priority(),
            )
        });
        let key_ranges = req_ctx
            .ranges
            .iter()
            .map(|key_range| (key_range.get_start().to_vec(), key_range.get_end().to_vec()))
            .collect();
        let resource_tag = self
            .resource_tag_factory
            .new_tag_with_key_ranges(&req_ctx.context, key_ranges);
        let mut allocated_bytes = resource_tag.approximate_heap_size();

        let task_id = req_ctx.build_task_id();
        let tracker = Box::new(Tracker::new(req_ctx, r.req_tag, self.slow_log_threshold));
        allocated_bytes += tracker.approximate_mem_size();

        let future =
            Self::handle_stream_request_impl(self.semaphore.clone(), tracker, r.handler_builder)
                .in_resource_metering_tag(resource_tag)
                .then(futures::future::ok::<_, mpsc::SendError>)
                .forward(tx)
                .unwrap_or_else(|e| {
                    warn!("coprocessor stream send error"; "error" => %e);
                });

        self.read_pool_spawn_with_memory_quota_check(
            allocated_bytes,
            future,
            priority,
            task_id,
            metadata,
            resource_limiter,
        )?;
        Ok(rx)
    }

    /// Parses and handles a stream request. Returns a stream that produce each
    /// result in a `Response` and will never fail. If there are errors during
    /// parsing or handling, they will be converted into a `Response` as the
    /// only stream item.
    #[inline]
    pub fn parse_and_handle_stream_request(
        &self,
        req: coppb::Request,
        peer: Option<String>,
    ) -> impl futures::stream::Stream<Item = coppb::Response> {
        let tracker = GLOBAL_TRACKERS.insert(::tracker::Tracker::new(RequestInfo::new(
            req.get_context(),
            RequestType::Unknown,
            req.start_ts,
        )));
        set_tls_tracker_token(tracker);
        with_tls_tracker(|tracker| {
            tracker.metrics.grpc_req_size = req.compute_size() as u64;
        });
        let result_of_stream = self
            .parse_request_and_check_memory_locks(req, peer, true)
            .and_then(|r| self.handle_stream_request(r)); // Result<Stream<Resp, Error>, Error>

        futures::stream::once(futures::future::ready(result_of_stream)) // Stream<Stream<Resp, Error>, Error>
            .try_flatten() // Stream<Resp, Error>
            .or_else(|e| futures::future::ok(make_error_response(e))) // Stream<Resp, ()>
            .map(|item: std::result::Result<_, ()>| item.unwrap())
    }

    fn read_pool_spawn_with_memory_quota_check<F>(
        &self,
        mut allocated_bytes: usize,
        future: F,
        priority: CommandPri,
        task_id: u64,
        metadata: TaskMetadata<'_>,
        resource_limiter: Option<Arc<ResourceLimiter>>,
    ) -> Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        allocated_bytes += mem::size_of_val(&future);
        let mut owned_quota = OwnedAllocated::new(self.memory_quota.clone());
        owned_quota.alloc(allocated_bytes)?;
        let fut = future.map(move |_| {
            // Release quota after handle completed.
            drop(owned_quota);
        });
        self.read_pool
            .spawn(fut, priority, task_id, metadata, resource_limiter)
            .map_err(|_| Error::MaxPendingTasksExceeded)
    }
}

macro_rules! make_error_response_common {
    ($resp:expr, $tag:expr, $e:expr) => {{
        match $e {
            Error::Region(e) => {
                $tag = storage::get_tag_from_header(&e);
                $resp.set_region_error(e);
            }
            Error::Locked(info) => {
                $tag = "meet_lock";
                $resp.set_locked(info);
            }
            Error::DeadlineExceeded => {
                $tag = "deadline_exceeded";
                let mut err = errorpb::Error::default();
                set_deadline_exceeded_busy_error(&mut err);
                err.set_message($e.to_string());
                $resp.set_region_error(err);
            }
            Error::MaxPendingTasksExceeded => {
                $tag = "max_pending_tasks_exceeded";
                let mut server_is_busy_err = errorpb::ServerIsBusy::default();
                server_is_busy_err.set_reason($e.to_string());
                let mut errorpb = errorpb::Error::default();
                errorpb.set_message($e.to_string());
                errorpb.set_server_is_busy(server_is_busy_err);
                $resp.set_region_error(errorpb);
            }
            Error::MemoryQuotaExceeded => {
                $tag = "memory_quota_exceeded";
                let mut server_is_busy_err = errorpb::ServerIsBusy::default();
                server_is_busy_err.set_reason($e.to_string());
                let mut errorpb = errorpb::Error::default();
                errorpb.set_message($e.to_string());
                errorpb.set_server_is_busy(server_is_busy_err);
                $resp.set_region_error(errorpb);
            }
            Error::InvalidMaxTsUpdate(e) => {
                $tag = "invalid_max_ts_update";
                let mut err = errorpb::Error::default();
                err.set_message(e.to_string());
                $resp.set_region_error(err);
            }
            Error::DefaultNotFound(_) => {
                $tag = "default_not_found";
                $resp.set_other_error($e.to_string());
            }
            Error::Other(_) => {
                $tag = "other";
                warn!("unexpected other error encountered processing coprocessor task";
                    "error" => ?&$e,
                );
                $resp.set_other_error($e.to_string());
            }
        };
        COPR_REQ_ERROR.with_label_values(&[$tag]).inc();
    }};
}

fn make_error_batch_response(batch_resp: &mut coppb::StoreBatchTaskResponse, e: Error) {
    debug!(
        "batch cop task error-response";
        "err" => %e
    );
    let tag;
    make_error_response_common!(batch_resp, tag, e);
}

fn make_error_response(e: Error) -> coppb::Response {
    debug!(
        "error-response";
        "err" => %e
    );
    let tag;
    let mut resp = coppb::Response::default();
    make_error_response_common!(resp, tag, e);
    resp
}

/// ExtraSnapStoreAccessor is used to get the snapshot stores for the
/// extra regions.
/// The "extra region" means the regions that are not the source region in a
/// request.
/// For example, if a cop-task contains a `IndexLookUp` executor which needs to
/// access look up the primary rows, it will use this accessor to locate and get
/// the snapshot of the regions which these primary rows located.
#[derive(Clone, Debug)]
pub struct ExtraSnapStoreAccessor<E> {
    store_id: u64,
    req_ctx: ReqContext,
    _phantom: PhantomData<fn() -> E>,
}

impl<E: Engine> ExtraSnapStoreAccessor<E> {
    /// new creates an Optional EngineSnapshotStoreAccessor.
    /// Please note that not all scenes are supported.
    /// If the current request is not supported, a `None` value will be
    /// returned to force the request to access the source region only.
    pub fn new(req_ctx: ReqContext) -> Option<Self> {
        let pb_ctx = &req_ctx.context;
        let store_id = match pb_ctx.peer.as_ref() {
            // Though it is possible that the request carries a wrong store_id, we can still use
            // it to find the peer in the region because it will be validated in get snapshot phase.
            Some(peer) => peer.get_store_id(),
            None => return None,
        };

        if pb_ctx.get_isolation_level() == kvrpcpb::IsolationLevel::Si
            && !pb_ctx.get_stale_read()
            && !pb_ctx.get_replica_read()
        {
            // Some scenes are not supported before an effective argument, including:
            // - stale-read & replica-read, TODO: support it later
            // - non-SI isolation level, TODO: support it later
            return Some(Self {
                store_id,
                req_ctx,
                _phantom: PhantomData,
            });
        }
        None
    }

    #[inline]
    fn err<S: Display>(s: S) -> StorageError {
        StorageError::from(anyhow!("{}", s))
    }
}

#[async_trait]
impl<E: Engine> RegionStorageAccessor for ExtraSnapStoreAccessor<E> {
    type Storage = SnapshotStore<E::IMSnap>;

    /// find the region by the specified key.
    /// The argument `key` should be the comparable format, you should use
    /// `Key::from_raw` encode the raw key.
    async fn find_region_by_key(&self, key: &[u8]) -> StorageResult<FindRegionResult> {
        let key_in_vec = key.to_vec();
        let (tx, rx) = oneshot::channel();
        unsafe {
            with_tls_engine(|engine: &mut E| -> StorageResult<()> {
                engine
                    .seek_region(
                        key,
                        Box::new(move |iter| {
                            let result = match iter.next() {
                                Some(info)
                                    if info.region.get_start_key() <= key_in_vec.as_slice() =>
                                {
                                    FindRegionResult::with_found(info.region.clone(), info.role)
                                }
                                Some(info) => FindRegionResult::with_not_found(Some(
                                    info.region.start_key.clone(),
                                )),
                                None => FindRegionResult::with_not_found(None),
                            };

                            if tx.send(result).is_err() {
                                warn!("failed to send find_region_by_key result");
                            }
                        }),
                    )
                    .map_err(Self::err)
            })?;
        }
        rx.map_err(Self::err).await
    }

    async fn get_local_region_storage(
        &self,
        region: &metapb::Region,
        _key_range: &[coppb::KeyRange],
    ) -> StorageResult<Self::Storage> {
        let peer = match find_peer(region, self.store_id) {
            Some(peer) => peer.clone(),
            None => {
                return Err(Self::err(format!(
                    "cannot find peer in region, region_id: {}, request store_id: {}",
                    self.store_id,
                    region.get_id()
                )));
            }
        };

        let pb_ctx = &self.req_ctx.context;
        let start_ts = self.req_ctx.txn_start_ts;
        let snap_ctx = SnapContext {
            pb_ctx,
            // read_id is used by batch_get to cache the snapshot.
            // We set it as None here to disable this optimization for simple.
            read_id: None,
            start_ts: Some(start_ts),
            // does not support in-flashback execution currently to avoid some potential
            // issues for simple.
            allowed_in_flashback: false,
            // key_ranges is always empty because replica-read is not supported.
            // TODO: check the locks for the input key_ranges for replica-read if
            // it is supported in the future.
            key_ranges: Vec::default(),
            extra_region_override: Some(ExtraRegionOverride {
                region_id: region.get_id(),
                region_epoch: region.get_region_epoch().clone(),
                peer,
                // Do not need to check the term because only readonly requests are
                // supported currently.
                check_term: None,
            }),
        };

        let snap = unsafe {
            with_tls_engine(move |engine: &mut E| kv::in_memory_snapshot(engine, snap_ctx))
        }
        .await
        .map_err(Self::err)?;

        Ok(SnapshotStore::new(
            snap,
            start_ts,
            pb_ctx.get_isolation_level(),
            !pb_ctx.get_not_fill_cache(),
            self.req_ctx.bypass_locks.clone(),
            self.req_ctx.access_locks.clone(),
            // If `check_has_newer_ts_data` is enabled, the internal scanner will check if there is
            // newer data in the region to tell the TiDB whether to cache the cop response or not.
            // However, the current cop-cache implementation only supports the source region.
            // So here we force to set it to `false` to avoid caching the cop response when
            // any other region snapshot is accessed in a request.
            false,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Mutex, atomic, mpsc},
        thread, vec,
    };

    use futures::executor::{block_on, block_on_stream};
    use kvproto::kvrpcpb::IsolationLevel;
    use protobuf::Message;
    use raft::StateRole;
    use raftstore::coprocessor::region_info_accessor::MockRegionInfoProvider;
    use tidb_query_common::storage::Storage;
    use tikv_kv::{MockEngine, MockEngineBuilder, destroy_tls_engine, set_tls_engine};
    use tipb::{Executor, Expr};
    use txn_types::{Key, LockType};

    use super::*;
    use crate::{
        config::CoprReadPoolConfig,
        coprocessor::readpool_impl::build_read_pool_for_test,
        read_pool::ReadPool,
        storage::{Store, TestEngineBuilder, kv::RocksEngine},
    };

    /// A unary `RequestHandler` that always produces a fixture.
    struct UnaryFixture {
        handle_duration: Duration,
        yieldable: bool,
        result: Option<Result<coppb::Response>>,
    }

    impl UnaryFixture {
        pub fn new(result: Result<coppb::Response>) -> UnaryFixture {
            UnaryFixture {
                handle_duration: Default::default(),
                yieldable: false,
                result: Some(result),
            }
        }

        pub fn new_with_duration(
            result: Result<coppb::Response>,
            handle_duration: Duration,
        ) -> UnaryFixture {
            UnaryFixture {
                handle_duration,
                yieldable: false,
                result: Some(result),
            }
        }

        pub fn new_with_duration_yieldable(
            result: Result<coppb::Response>,
            handle_duration: Duration,
        ) -> UnaryFixture {
            UnaryFixture {
                handle_duration,
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
                for _ in 0..self.handle_duration.as_millis() as u64 / 100 {
                    thread::sleep(Duration::from_millis(100));
                    yatp::task::future::reschedule().await;
                }
                thread::sleep(Duration::from_millis(
                    self.handle_duration.as_millis() as u64 % 100,
                ));
            } else {
                thread::sleep(self.handle_duration);
            }

            self.result.take().unwrap().map(|x| x.into())
        }
    }

    /// A streaming `RequestHandler` that always produces a fixture.
    struct StreamFixture {
        result_len: usize,
        result_iter: vec::IntoIter<Result<coppb::Response>>,
        handle_durations: vec::IntoIter<Duration>,
        nth: usize,
    }

    impl StreamFixture {
        pub fn new(result: Vec<Result<coppb::Response>>) -> StreamFixture {
            let len = result.len();
            StreamFixture {
                result_len: len,
                result_iter: result.into_iter(),
                handle_durations: vec![Duration::default(); len].into_iter(),
                nth: 0,
            }
        }

        pub fn new_with_duration(
            result: Vec<Result<coppb::Response>>,
            handle_durations: Vec<Duration>,
        ) -> StreamFixture {
            assert_eq!(result.len(), handle_durations.len());
            StreamFixture {
                result_len: result.len(),
                result_iter: result.into_iter(),
                handle_durations: handle_durations.into_iter(),
                nth: 0,
            }
        }
    }

    #[async_trait]
    impl RequestHandler for StreamFixture {
        async fn handle_streaming_request(&mut self) -> Result<(Option<coppb::Response>, bool)> {
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
                    let handle_duration = self.handle_durations.next().unwrap();
                    thread::sleep(handle_duration);
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

    #[async_trait]
    impl RequestHandler for StreamFromClosure {
        async fn handle_streaming_request(&mut self) -> Result<(Option<coppb::Response>, bool)> {
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
        let cm = ConcurrencyManager::new_for_test(1.into());
        let copr = Endpoint::<RocksEngine>::new(
            &Config::default(),
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            None,
        );

        // a normal request
        let handler_builder =
            Box::new(|_, _: &_| Ok(UnaryFixture::new(Ok(coppb::Response::default())).into_boxed()));
        let resp = block_on(
            copr.handle_unary_request(ParseCopRequestResult::default_for_test(handler_builder)),
        )
        .unwrap();
        assert!(resp.get_other_error().is_empty());

        // an outdated request
        let handler_builder =
            Box::new(|_, _: &_| Ok(UnaryFixture::new(Ok(coppb::Response::default())).into_boxed()));
        let outdated_req_ctx = ReqContext::new(
            Default::default(),
            Vec::new(),
            Duration::from_secs(0),
            None,
            None,
            TimeStamp::max(),
            None,
            PerfLevel::EnableCount,
            false,
        );
        block_on(copr.handle_unary_request(ParseCopRequestResult {
            req_ctx: outdated_req_ctx,
            req_tag: ReqTag::test,
            handler_builder,
        }))
        .unwrap_err();
    }

    #[test]
    fn test_stack_guard() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            engine,
        ));
        let cm = ConcurrencyManager::new_for_test(1.into());
        let mut copr = Endpoint::<RocksEngine>::new(
            &Config::default(),
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            None,
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
        let cm = ConcurrencyManager::new_for_test(1.into());
        let copr = Endpoint::<RocksEngine>::new(
            &Config::default(),
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            None,
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
        let cm = ConcurrencyManager::new_for_test(1.into());
        let copr = Endpoint::<RocksEngine>::new(
            &Config::default(),
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            None,
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

        let cm = ConcurrencyManager::new_for_test(1.into());
        let copr = Endpoint::<RocksEngine>::new(
            &Config::default(),
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            None,
        );

        let (tx, rx) = mpsc::channel();

        // first 2 requests are processed as normal and laters are returned as errors
        for i in 0..5 {
            let mut response = coppb::Response::default();
            response.set_data(vec![1, 2, i]);

            let mut context = kvrpcpb::Context::default();
            context.set_priority(kvrpcpb::CommandPri::Normal);

            let handler_builder = Box::new(|_, _: &_| {
                Ok(
                    UnaryFixture::new_with_duration(Ok(response), Duration::from_millis(1000))
                        .into_boxed(),
                )
            });
            let future =
                copr.handle_unary_request(ParseCopRequestResult::default_for_test(handler_builder));
            let tx = tx.clone();
            thread::spawn(move || {
                tx.send(block_on(future)).unwrap();
            });
            thread::sleep(Duration::from_millis(100));
        }

        // verify
        for _ in 2..5 {
            rx.recv().unwrap().unwrap_err();
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
        let cm = ConcurrencyManager::new_for_test(1.into());
        let copr = Endpoint::<RocksEngine>::new(
            &Config::default(),
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            None,
        );

        let handler_builder =
            Box::new(|_, _: &_| Ok(UnaryFixture::new(Err(box_err!("foo"))).into_boxed()));
        let resp = block_on(
            copr.handle_unary_request(ParseCopRequestResult::default_for_test(handler_builder)),
        )
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
        let cm = ConcurrencyManager::new_for_test(1.into());
        let copr = Endpoint::<RocksEngine>::new(
            &Config::default(),
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            None,
        );

        // Fail immediately
        let handler_builder =
            Box::new(|_, _: &_| Ok(StreamFixture::new(vec![Err(box_err!("foo"))]).into_boxed()));
        let resp_vec = block_on_stream(
            copr.handle_stream_request(ParseCopRequestResult::default_for_test(handler_builder))
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
            copr.handle_stream_request(ParseCopRequestResult::default_for_test(handler_builder))
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
        let cm = ConcurrencyManager::new_for_test(1.into());
        let copr = Endpoint::<RocksEngine>::new(
            &Config::default(),
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            None,
        );

        let handler_builder = Box::new(|_, _: &_| Ok(StreamFixture::new(vec![]).into_boxed()));
        let resp_vec = block_on_stream(
            copr.handle_stream_request(ParseCopRequestResult::default_for_test(handler_builder))
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
        let cm = ConcurrencyManager::new_for_test(1.into());
        let copr = Endpoint::<RocksEngine>::new(
            &Config::default(),
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            None,
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
            copr.handle_stream_request(ParseCopRequestResult::default_for_test(handler_builder))
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
            copr.handle_stream_request(ParseCopRequestResult::default_for_test(handler_builder))
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
            copr.handle_stream_request(ParseCopRequestResult::default_for_test(handler_builder))
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
        let cm = ConcurrencyManager::new_for_test(1.into());
        let copr = Endpoint::<RocksEngine>::new(
            &Config {
                end_point_stream_channel_size: 3,
                ..Config::default()
            },
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            None,
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
            copr.handle_stream_request(ParseCopRequestResult::default_for_test(handler_builder))
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
        const SNAPSHOT_DURATION: Duration = Duration::from_millis(500);

        /// Asserted that the delay caused by OS scheduling other tasks is
        /// smaller than 200ms. This is mostly for CI.
        const HANDLE_ERROR: Duration = Duration::from_millis(200);

        /// The acceptable error range for a coarse timer. Note that we use
        /// CLOCK_MONOTONIC_COARSE which can be slewed by time
        /// adjustment code (e.g., NTP, PTP).
        const COARSE_ERROR: Duration = Duration::from_millis(50);

        /// The duration that payload executes.
        const PAYLOAD_SMALL: Duration = Duration::from_millis(3000);
        const PAYLOAD_LARGE: Duration = Duration::from_millis(6000);

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
            end_point_request_max_handle_duration: Some(ReadableDuration(
                (PAYLOAD_SMALL + PAYLOAD_LARGE) * 2,
            )),
            ..Default::default()
        };

        let cm = ConcurrencyManager::new_for_test(1.into());
        let copr = Endpoint::<RocksEngine>::new(
            &config,
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            None,
        );

        let (tx, rx) = std::sync::mpsc::channel();

        // A request that requests execution details.
        let req_with_exec_detail: ReqContext = {
            let mut inner = ReqContextInner::default_for_test();
            inner.context.set_record_time_stat(true);
            inner.into()
        };
        {
            let mut wait_time: Duration = Duration::default();

            // Request 1: Unary, success response.
            let handler_builder = Box::new(|_, _: &_| {
                Ok(
                    UnaryFixture::new_with_duration(Ok(coppb::Response::default()), PAYLOAD_SMALL)
                        .into_boxed(),
                )
            });
            let resp_future_1 = copr.handle_unary_request(ParseCopRequestResult {
                req_tag: ReqTag::test,
                req_ctx: req_with_exec_detail.clone(),
                handler_builder,
            });
            let sender = tx.clone();
            thread::spawn(move || sender.send(vec![block_on(resp_future_1).unwrap()]).unwrap());
            // Sleep a while to make sure that thread is spawn and snapshot is taken.
            thread::sleep(SNAPSHOT_DURATION);

            // Request 2: Unary, error response.
            let handler_builder = Box::new(|_, _: &_| {
                Ok(
                    UnaryFixture::new_with_duration(Err(box_err!("foo")), PAYLOAD_LARGE)
                        .into_boxed(),
                )
            });
            let resp_future_2 = copr.handle_unary_request(ParseCopRequestResult {
                req_tag: ReqTag::test,
                req_ctx: req_with_exec_detail.clone(),
                handler_builder,
            });
            let sender = tx.clone();
            thread::spawn(move || sender.send(vec![block_on(resp_future_2).unwrap()]).unwrap());
            thread::sleep(SNAPSHOT_DURATION);

            // Response 1
            let resp = &rx.recv().unwrap()[0];
            assert!(resp.get_other_error().is_empty());
            assert_ge!(
                Duration::from_nanos(
                    resp.get_exec_details_v2()
                        .get_time_detail_v2()
                        .get_process_wall_time_ns()
                ),
                PAYLOAD_SMALL.saturating_sub(COARSE_ERROR)
            );
            assert_lt!(
                Duration::from_nanos(
                    resp.get_exec_details_v2()
                        .get_time_detail_v2()
                        .get_process_wall_time_ns()
                ),
                PAYLOAD_SMALL + HANDLE_ERROR + COARSE_ERROR
            );
            assert_ge!(
                Duration::from_nanos(
                    resp.get_exec_details_v2()
                        .get_time_detail_v2()
                        .get_wait_wall_time_ns()
                ),
                wait_time.saturating_sub(HANDLE_ERROR + COARSE_ERROR)
            );
            assert_lt!(
                Duration::from_nanos(
                    resp.get_exec_details_v2()
                        .get_time_detail_v2()
                        .get_wait_wall_time_ns()
                ),
                wait_time + HANDLE_ERROR + COARSE_ERROR
            );
            wait_time += PAYLOAD_SMALL - SNAPSHOT_DURATION;

            // Response 2
            let resp = &rx.recv().unwrap()[0];
            assert!(!resp.get_other_error().is_empty());
            assert_ge!(
                Duration::from_nanos(
                    resp.get_exec_details_v2()
                        .get_time_detail_v2()
                        .get_process_wall_time_ns()
                ),
                PAYLOAD_LARGE.saturating_sub(COARSE_ERROR)
            );
            assert_lt!(
                Duration::from_nanos(
                    resp.get_exec_details_v2()
                        .get_time_detail_v2()
                        .get_process_wall_time_ns()
                ),
                PAYLOAD_LARGE + HANDLE_ERROR + COARSE_ERROR
            );
            assert_ge!(
                Duration::from_nanos(
                    resp.get_exec_details_v2()
                        .get_time_detail_v2()
                        .get_wait_wall_time_ns()
                ),
                wait_time.saturating_sub(HANDLE_ERROR + COARSE_ERROR)
            );
            assert_lt!(
                Duration::from_nanos(
                    resp.get_exec_details_v2()
                        .get_time_detail_v2()
                        .get_wait_wall_time_ns()
                ),
                wait_time + HANDLE_ERROR + COARSE_ERROR
            );

            // check TimeDetail and TimeDetailV2 has the same value.
            let time_detail = resp.get_exec_details_v2().get_time_detail();
            let time_detail_v2 = resp.get_exec_details_v2().get_time_detail_v2();
            assert_eq!(
                time_detail.get_process_wall_time_ms(),
                time_detail_v2.get_process_wall_time_ns() / 1_000_000,
            );
            assert_eq!(
                time_detail.get_wait_wall_time_ms(),
                time_detail_v2.get_wait_wall_time_ns() / 1_000_000,
            );
            assert_eq!(
                time_detail.get_kv_read_wall_time_ms(),
                time_detail_v2.get_kv_read_wall_time_ns() / 1_000_000,
            );
        }

        {
            // Test multi-stage tasks
            // Request 1: Unary, success response.
            let handler_builder = Box::new(|_, _: &_| {
                Ok(UnaryFixture::new_with_duration_yieldable(
                    Ok(coppb::Response::default()),
                    PAYLOAD_SMALL,
                )
                .into_boxed())
            });
            let resp_future_1 = copr.handle_unary_request(ParseCopRequestResult {
                req_tag: ReqTag::test,
                req_ctx: req_with_exec_detail.clone(),
                handler_builder,
            });
            let sender = tx.clone();
            thread::spawn(move || sender.send(vec![block_on(resp_future_1).unwrap()]).unwrap());
            // Sleep a while to make sure that thread is spawn and snapshot is taken.
            thread::sleep(SNAPSHOT_DURATION);

            // Request 2: Unary, error response.
            let handler_builder = Box::new(|_, _: &_| {
                Ok(
                    UnaryFixture::new_with_duration_yieldable(Err(box_err!("foo")), PAYLOAD_LARGE)
                        .into_boxed(),
                )
            });
            let resp_future_2 = copr.handle_unary_request(ParseCopRequestResult {
                req_tag: ReqTag::test,
                req_ctx: req_with_exec_detail.clone(),
                handler_builder,
            });
            let sender = tx.clone();
            thread::spawn(move || sender.send(vec![block_on(resp_future_2).unwrap()]).unwrap());
            thread::sleep(SNAPSHOT_DURATION);

            // Response 1
            //
            // In the worst case, `total_suspend_time` could be totally req2 payload.
            // So here: req1 payload <= process time <= (req1 payload + req2 payload)
            let resp = &rx.recv().unwrap()[0];
            assert!(resp.get_other_error().is_empty());
            assert_ge!(
                Duration::from_nanos(
                    resp.get_exec_details_v2()
                        .get_time_detail_v2()
                        .get_process_wall_time_ns()
                ),
                PAYLOAD_SMALL.saturating_sub(COARSE_ERROR)
            );
            assert_lt!(
                Duration::from_nanos(
                    resp.get_exec_details_v2()
                        .get_time_detail_v2()
                        .get_process_wall_time_ns()
                ),
                PAYLOAD_SMALL + PAYLOAD_LARGE + HANDLE_ERROR + COARSE_ERROR
            );

            // Response 2
            //
            // In the worst case, `total_suspend_time` could be totally req1 payload.
            // So here: req2 payload <= process time <= (req1 payload + req2 payload)
            let resp = &rx.recv().unwrap()[0];
            assert!(!resp.get_other_error().is_empty());
            assert_ge!(
                Duration::from_nanos(
                    resp.get_exec_details_v2()
                        .get_time_detail_v2()
                        .get_process_wall_time_ns()
                ),
                PAYLOAD_LARGE.saturating_sub(COARSE_ERROR)
            );
            assert_lt!(
                Duration::from_nanos(
                    resp.get_exec_details_v2()
                        .get_time_detail_v2()
                        .get_process_wall_time_ns()
                ),
                PAYLOAD_SMALL + PAYLOAD_LARGE + HANDLE_ERROR + COARSE_ERROR
            );
        }

        {
            let mut wait_time = Duration::default();

            // Request 1: Unary, success response.
            let handler_builder = Box::new(|_, _: &_| {
                Ok(
                    UnaryFixture::new_with_duration(Ok(coppb::Response::default()), PAYLOAD_LARGE)
                        .into_boxed(),
                )
            });
            let resp_future_1 = copr.handle_unary_request(ParseCopRequestResult {
                req_tag: ReqTag::test,
                req_ctx: req_with_exec_detail.clone(),
                handler_builder,
            });
            let sender = tx.clone();
            thread::spawn(move || sender.send(vec![block_on(resp_future_1).unwrap()]).unwrap());
            // Sleep a while to make sure that thread is spawn and snapshot is taken.
            thread::sleep(SNAPSHOT_DURATION);

            // Request 2: Stream.
            let handler_builder = Box::new(|_, _: &_| {
                Ok(StreamFixture::new_with_duration(
                    vec![
                        Ok(coppb::Response::default()),
                        Err(box_err!("foo")),
                        Ok(coppb::Response::default()),
                    ],
                    vec![PAYLOAD_SMALL, PAYLOAD_LARGE, PAYLOAD_SMALL],
                )
                .into_boxed())
            });
            let resp_future_3 = copr
                .handle_stream_request(ParseCopRequestResult {
                    req_tag: ReqTag::test,
                    req_ctx: req_with_exec_detail.clone(),
                    handler_builder,
                })
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
                Duration::from_nanos(
                    resp.get_exec_details_v2()
                        .get_time_detail_v2()
                        .get_process_wall_time_ns()
                ),
                PAYLOAD_LARGE.saturating_sub(COARSE_ERROR)
            );
            assert_lt!(
                Duration::from_nanos(
                    resp.get_exec_details_v2()
                        .get_time_detail_v2()
                        .get_process_wall_time_ns()
                ),
                PAYLOAD_LARGE + HANDLE_ERROR + COARSE_ERROR
            );
            assert_ge!(
                Duration::from_nanos(
                    resp.get_exec_details_v2()
                        .get_time_detail_v2()
                        .get_wait_wall_time_ns()
                ),
                wait_time.saturating_sub(HANDLE_ERROR + COARSE_ERROR)
            );
            assert_lt!(
                Duration::from_nanos(
                    resp.get_exec_details_v2()
                        .get_time_detail_v2()
                        .get_wait_wall_time_ns()
                ),
                wait_time + HANDLE_ERROR + COARSE_ERROR
            );
            wait_time += PAYLOAD_LARGE - SNAPSHOT_DURATION;

            // Response 2
            let resp = &rx.recv().unwrap();
            assert_eq!(resp.len(), 2);
            assert!(resp[0].get_other_error().is_empty());
            assert_ge!(
                Duration::from_nanos(
                    resp[0]
                        .get_exec_details_v2()
                        .get_time_detail_v2()
                        .get_process_wall_time_ns()
                ),
                PAYLOAD_SMALL.saturating_sub(COARSE_ERROR)
            );
            assert_lt!(
                Duration::from_nanos(
                    resp[0]
                        .get_exec_details_v2()
                        .get_time_detail_v2()
                        .get_process_wall_time_ns()
                ),
                PAYLOAD_SMALL + HANDLE_ERROR + COARSE_ERROR
            );
            assert_ge!(
                Duration::from_nanos(
                    resp[0]
                        .get_exec_details_v2()
                        .get_time_detail_v2()
                        .get_wait_wall_time_ns()
                ),
                wait_time.saturating_sub(HANDLE_ERROR + COARSE_ERROR)
            );
            assert_lt!(
                Duration::from_nanos(
                    resp[0]
                        .get_exec_details_v2()
                        .get_time_detail_v2()
                        .get_wait_wall_time_ns()
                ),
                wait_time + HANDLE_ERROR + COARSE_ERROR
            );

            assert!(!resp[1].get_other_error().is_empty());
            assert_ge!(
                Duration::from_nanos(
                    resp[1]
                        .get_exec_details_v2()
                        .get_time_detail_v2()
                        .get_process_wall_time_ns()
                ),
                PAYLOAD_LARGE.saturating_sub(COARSE_ERROR)
            );
            assert_lt!(
                Duration::from_nanos(
                    resp[1]
                        .get_exec_details_v2()
                        .get_time_detail_v2()
                        .get_process_wall_time_ns()
                ),
                PAYLOAD_LARGE + HANDLE_ERROR + COARSE_ERROR
            );
            assert_ge!(
                Duration::from_nanos(
                    resp[1]
                        .get_exec_details_v2()
                        .get_time_detail_v2()
                        .get_wait_wall_time_ns()
                ),
                wait_time.saturating_sub(HANDLE_ERROR + COARSE_ERROR)
            );
            assert_lt!(
                Duration::from_nanos(
                    resp[1]
                        .get_exec_details_v2()
                        .get_time_detail_v2()
                        .get_wait_wall_time_ns()
                ),
                wait_time + HANDLE_ERROR + COARSE_ERROR
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
        let cm = ConcurrencyManager::new_for_test(1.into());
        let copr = Endpoint::<RocksEngine>::new(
            &Config::default(),
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            None,
        );

        {
            let handler_builder = Box::new(|_, _: &_| {
                thread::sleep(Duration::from_millis(600));
                Ok(UnaryFixture::new(Ok(coppb::Response::default())).into_boxed())
            });

            let mut inner = ReqContextInner::default_for_test();
            inner.deadline = Deadline::from_now(Duration::from_millis(500));
            let config: ReqContext = inner.into();

            let resp = block_on(copr.handle_unary_request(ParseCopRequestResult {
                req_tag: ReqTag::test,
                req_ctx: config,
                handler_builder,
            }))
            .unwrap();
            assert_eq!(resp.get_data().len(), 0);
            let region_err = resp.get_region_error();
            assert_eq!(
                region_err.get_server_is_busy().reason,
                "deadline is exceeded".to_string()
            );
        }

        {
            let handler_builder = Box::new(|_, _: &_| {
                Ok(UnaryFixture::new_with_duration_yieldable(
                    Ok(coppb::Response::default()),
                    Duration::from_millis(1500),
                )
                .into_boxed())
            });

            let mut inner = ReqContextInner::default_for_test();
            inner.deadline = Deadline::from_now(Duration::from_millis(500));
            let config: ReqContext = inner.into();

            let resp = block_on(copr.handle_unary_request(ParseCopRequestResult {
                req_tag: ReqTag::test,
                req_ctx: config,
                handler_builder,
            }))
            .unwrap();
            assert_eq!(resp.get_data().len(), 0);
            let region_err = resp.get_region_error();
            assert_eq!(
                region_err.get_server_is_busy().reason,
                "deadline is exceeded".to_string()
            );
        }
    }

    #[test]
    fn test_check_memory_locks() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            engine,
        ));
        let cm = ConcurrencyManager::new_for_test(1.into());
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
                false,
            ));
        });

        let config = Config::default();
        let copr = Endpoint::<RocksEngine>::new(
            &config,
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            None,
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

    #[test]
    fn test_make_error_response() {
        let resp = make_error_response(Error::DeadlineExceeded);
        let region_err = resp.get_region_error();
        assert_eq!(
            region_err.get_server_is_busy().reason,
            "deadline is exceeded".to_string()
        );
        assert_eq!(
            region_err.get_message(),
            "Coprocessor task terminated due to exceeding the deadline"
        );
    }

    #[test]
    fn test_memory_quota() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            engine,
        ));
        let cm = ConcurrencyManager::new_for_test(1.into());
        let copr = Endpoint::<RocksEngine>::new(
            &Config::default(),
            read_pool.handle(),
            cm,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            None,
        );

        // By default, coprocessor does not return memory quota exceeded error.
        {
            let handler_builder = Box::new(|_, _: &_| {
                Ok(UnaryFixture::new(Ok(coppb::Response::default())).into_boxed())
            });

            let mut inner = ReqContextInner::default_for_test();
            inner.deadline = Deadline::from_now(Duration::from_millis(500));
            let config: ReqContext = inner.into();

            let resp = block_on(copr.handle_unary_request(ParseCopRequestResult {
                req_tag: ReqTag::test,
                req_ctx: config,
                handler_builder,
            }))
            .unwrap();
            assert!(!resp.has_region_error(), "{:?}", resp);
        }

        // Trigger memory quota exceeded error.
        copr.memory_quota.set_capacity(1);
        {
            let handler_builder = Box::new(|_, _: &_| {
                Ok(UnaryFixture::new(Ok(coppb::Response::default())).into_boxed())
            });

            let mut inner = ReqContextInner::default_for_test();
            inner.deadline = Deadline::from_now(Duration::from_millis(500));
            let config: ReqContext = inner.into();

            let res = block_on(copr.handle_unary_request(ParseCopRequestResult {
                req_tag: ReqTag::test,
                req_ctx: config,
                handler_builder,
            }));
            assert!(res.is_err(), "{:?}", res);
            let resp = make_error_response(res.unwrap_err());
            assert_eq!(resp.get_data().len(), 0);
            let region_err = resp.get_region_error();
            assert!(
                region_err
                    .get_server_is_busy()
                    .reason
                    .contains("exceeding memory quota"),
                "{:?}",
                region_err.get_server_is_busy().reason
            );
        }
    }

    // A default ReqContext that support to access another snapshot in a
    // request. It can be used to create an not None value of
    // Option<EngineSnapshotStoreAccessor>
    fn default_req_ctx_support_snap_accessor() -> ReqContextInner {
        let mut pb_ctx = kvrpcpb::Context::default();
        pb_ctx.set_isolation_level(IsolationLevel::Si);
        pb_ctx.set_stale_read(false);
        pb_ctx.set_replica_read(false);
        pb_ctx.set_peer(metapb::Peer {
            id: 12,
            store_id: 100,
            ..Default::default()
        });
        pb_ctx.set_region_id(23);
        let mut req_ctx = ReqContextInner::default_for_test();
        req_ctx.context = pb_ctx;
        req_ctx.txn_start_ts = TimeStamp::new(1234567);
        req_ctx.is_desc_scan = Some(true);
        req_ctx.ranges = vec![
            coppb::KeyRange {
                start: b"a1".to_vec(),
                end: b"b1".to_vec(),
                ..Default::default()
            },
            coppb::KeyRange {
                start: b"b3".to_vec(),
                end: b"b4".to_vec(),
                ..Default::default()
            },
        ];
        req_ctx
    }

    #[test]
    fn test_secondary_snap_store_accessor_new() {
        type StoreAccessor = ExtraSnapStoreAccessor<RocksEngine>;
        // construct a ReqContext that support to access another snapshot in a request
        let req_ctx = default_req_ctx_support_snap_accessor();

        // accessor support case
        let mut ctx = req_ctx.clone();
        assert!(StoreAccessor::new(ctx.into()).is_some());

        // does not support Rc / RcCheckTs
        ctx = req_ctx.clone();
        ctx.context.set_isolation_level(IsolationLevel::Rc);
        assert!(StoreAccessor::new(ctx.into()).is_none());
        ctx = req_ctx.clone();
        ctx.context.set_isolation_level(IsolationLevel::RcCheckTs);
        assert!(StoreAccessor::new(ctx.into()).is_none());

        // does not support stale read
        ctx = req_ctx.clone();
        ctx.context.set_stale_read(true);
        assert!(StoreAccessor::new(ctx.into()).is_none());

        // does not support replica read
        ctx = req_ctx.clone();
        ctx.context.set_replica_read(true);
        assert!(StoreAccessor::new(ctx.into()).is_none());
    }

    #[test]
    fn test_extra_snap_store_accessor_locate_region_by_key() {
        set_tls_engine(TestEngineBuilder::new().build().unwrap());
        defer! {
            unsafe {destroy_tls_engine::<RocksEngine>()}
        }

        let (r1, r2, r3) = (
            metapb::Region {
                id: 1,
                start_key: b"b".to_vec(),
                end_key: b"d".to_vec(),
                ..Default::default()
            },
            metapb::Region {
                id: 2,
                start_key: b"e".to_vec(),
                end_key: b"g".to_vec(),
                ..Default::default()
            },
            metapb::Region {
                id: 3,
                start_key: b"g".to_vec(),
                end_key: b"h".to_vec(),
                ..Default::default()
            },
        );

        unsafe {
            let provider = MockRegionInfoProvider::new(vec![r1.clone(), r2.clone(), r3.clone()]);
            provider.set_role(2, StateRole::Follower);
            with_tls_engine(|e: &mut RocksEngine| e.set_region_info_provider(provider))
        }

        type StoreAccessor = ExtraSnapStoreAccessor<RocksEngine>;
        let accessor = StoreAccessor::new(default_req_ctx_support_snap_accessor().into()).unwrap();

        // key is before any region, not found
        assert_eq!(
            block_on(accessor.find_region_by_key(b"a")).unwrap(),
            FindRegionResult::with_not_found(Some(b"b".to_vec())),
        );
        // key is region start, found
        assert_eq!(
            block_on(accessor.find_region_by_key(b"b")).unwrap(),
            FindRegionResult::with_found(r1.clone(), StateRole::Leader),
        );
        // key is in a region range, found
        assert_eq!(
            block_on(accessor.find_region_by_key(b"b1")).unwrap(),
            FindRegionResult::with_found(r1.clone(), StateRole::Leader),
        );
        assert_eq!(
            block_on(accessor.find_region_by_key(b"c")).unwrap(),
            FindRegionResult::with_found(r1.clone(), StateRole::Leader),
        );
        assert_eq!(
            block_on(accessor.find_region_by_key(b"c1")).unwrap(),
            FindRegionResult::with_found(r1.clone(), StateRole::Leader),
        );
        // key is a region's end, but not any region's end, not found
        assert_eq!(
            block_on(accessor.find_region_by_key(b"d")).unwrap(),
            FindRegionResult::with_not_found(Some(b"e".to_vec())),
        );
        // key is between a region's end and another region's start, not found
        assert_eq!(
            block_on(accessor.find_region_by_key(b"d1")).unwrap(),
            FindRegionResult::with_not_found(Some(b"e".to_vec())),
        );
        // key is in a region, but the region is not a leader
        assert_eq!(
            block_on(accessor.find_region_by_key(b"e")).unwrap(),
            FindRegionResult::with_found(r2.clone(), StateRole::Follower),
        );
        assert_eq!(
            block_on(accessor.find_region_by_key(b"f")).unwrap(),
            FindRegionResult::with_found(r2.clone(), StateRole::Follower),
        );
        // key is a region's end, and another region's start found
        assert_eq!(
            block_on(accessor.find_region_by_key(b"g")).unwrap(),
            FindRegionResult::with_found(r3.clone(), StateRole::Leader),
        );
        // key is after all regions, not found without next_region_start
        assert_eq!(
            block_on(accessor.find_region_by_key(b"h")).unwrap(),
            FindRegionResult::with_not_found(None),
        );
        assert_eq!(
            block_on(accessor.find_region_by_key(b"i")).unwrap(),
            FindRegionResult::with_not_found(None),
        );

        // clear_region_info_provider makes RocksEngine::seek_region returns error
        unsafe { with_tls_engine(|e: &mut RocksEngine| e.clear_region_info_provider()) }
        let err = block_on(accessor.find_region_by_key(b"a")).unwrap_err();
        assert!(
            err.to_string()
                .contains("region_info_accessor is not available"),
        );
    }

    #[test]
    fn test_secondary_snap_store_accessor_get_local_region_storage() {
        type StoreAccessor = ExtraSnapStoreAccessor<MockEngine>;
        #[derive(Clone)]
        struct TestCtx {
            store_id: u64,
            req_ctx: Arc<Mutex<ReqContextInner>>,
            req_region: Arc<Mutex<metapb::Region>>,
            req_ranges: Arc<Mutex<Vec<coppb::KeyRange>>>,
            called: Arc<atomic::AtomicBool>,
        }

        impl TestCtx {
            fn new_accessor(&self) -> StoreAccessor {
                StoreAccessor::new(self.get_req_ctx()).unwrap()
            }

            fn get_req_ctx(&self) -> ReqContext {
                ReqContext(Arc::new(self.req_ctx.lock().unwrap().clone()))
            }

            fn get_req_region(&self) -> metapb::Region {
                self.req_region.lock().unwrap().clone()
            }

            fn get_req_ranges(&self) -> Vec<coppb::KeyRange> {
                self.req_ranges.lock().unwrap().clone()
            }

            fn get_local_region_storage_with_check(
                &self,
            ) -> StorageResult<SnapshotStore<<MockEngine as Engine>::Snap>> {
                let accessor = &self.new_accessor();
                assert!(!self.called.load(atomic::Ordering::SeqCst));
                let result = block_on(
                    accessor
                        .get_local_region_storage(&self.get_req_region(), &self.get_req_ranges()),
                );
                let called = self.called.swap(false, atomic::Ordering::SeqCst);

                if let Ok(ref store) = result {
                    assert!(called);
                    let req_ctx = self.get_req_ctx();
                    let pb_ctx = &req_ctx.context;
                    assert_eq!(store.get_start_ts(), req_ctx.txn_start_ts);
                    assert_eq!(store.get_isolation_level(), pb_ctx.isolation_level);
                    assert_eq!(store.is_fill_cache(), !pb_ctx.get_not_fill_cache());
                    assert_eq!(store.get_by_pass_locks(), req_ctx.bypass_locks);
                    assert_eq!(store.get_access_locks(), req_ctx.access_locks);
                    // check_has_newer_ts_data should always be false to avoid caching the cop
                    // response
                    assert!(!store.is_check_has_newer_ts_data());
                }

                result
            }
        }

        let test_ctx = {
            let req_ctx = Arc::new(Mutex::new(default_req_ctx_support_snap_accessor()));
            let store_id = req_ctx.lock().unwrap().context.get_peer().get_store_id();
            assert!(store_id > 0);
            TestCtx {
                store_id,
                req_ctx,
                req_region: Arc::new(Mutex::new(metapb::Region {
                    id: 123,
                    start_key: b"a".to_vec(),
                    end_key: b"z".to_vec(),
                    peers: vec![
                        metapb::Peer {
                            id: 1,
                            store_id: store_id - 1,
                            ..Default::default()
                        },
                        metapb::Peer {
                            id: 2,
                            store_id,
                            ..Default::default()
                        },
                        metapb::Peer {
                            id: 3,
                            store_id: store_id + 1,
                            ..Default::default()
                        },
                    ]
                    .into(),
                    ..Default::default()
                })),
                req_ranges: Arc::new(Mutex::new(vec![coppb::KeyRange {
                    start: b"a".to_vec(),
                    end: b"b".to_vec(),
                    ..Default::default()
                }])),
                called: Arc::new(atomic::AtomicBool::new(false)),
            }
        };

        let test_ctx_for_engine = test_ctx.clone();
        set_tls_engine(
            MockEngineBuilder::from_rocks_engine(TestEngineBuilder::new().build().unwrap())
                // set the hook to check the snap_ctx as the argument of Engine::async_snapshot
                .set_pre_async_snapshot(move |snap_ctx| {
                    let test_ctx = test_ctx_for_engine.clone();
                    assert!(!test_ctx.called.swap(
                        true,
                        atomic::Ordering::SeqCst,
                    ));

                    let check_ctx = test_ctx.get_req_ctx();
                    let check_region = test_ctx.get_req_region();
                    // Currently, only leader read is supported in this test.
                    assert!(!check_ctx.context.get_replica_read() && !check_ctx.context.get_stale_read());
                    assert!(!check_ctx.ranges.is_empty());
                    // snap_ctx.pb_ctx should be the same as ReqContext.context
                    assert_eq!(snap_ctx.pb_ctx.clone(), check_ctx.context.clone());
                    // snap_ctx.extra_snap_override should be present with the correct region info
                    assert_eq!(
                        snap_ctx.extra_region_override,
                        Some(ExtraRegionOverride {
                            region_id: check_region.id,
                            region_epoch: check_region.get_region_epoch().clone(),
                            peer: check_region.get_peers()[1].clone(),
                            check_term: None,
                        })
                    );
                    // should select a peer with the right store_id.
                    assert_eq!(
                        snap_ctx.extra_region_override.as_ref().unwrap().peer.store_id,
                        test_ctx.store_id
                    );
                    // snapshot cache is not supported currently, so snap_ctx.read_id is always None.
                    assert!(snap_ctx.read_id.is_none());
                    // snap_ctx.start_ts should be the same as req_ctx.txn_start_ts
                    assert!(check_ctx.txn_start_ts > TimeStamp::zero());
                    assert_eq!(snap_ctx.start_ts, Some(check_ctx.txn_start_ts));
                    // Even if req_ctx.ranges is not empty, snap_ctx.key_ranges should be empty in
                    // leader read because leader read does not need to check keys locks.
                    assert!(!test_ctx.get_req_ranges().is_empty());
                    assert_eq!(snap_ctx.key_ranges.len(), 0);
                    // not allowed in flashback
                    assert!(!snap_ctx.allowed_in_flashback);
                })
                .build(),
        );
        defer! {
            unsafe {destroy_tls_engine::<MockEngine>()}
        }

        // normal case
        test_ctx
            .get_local_region_storage_with_check()
            .expect("should succeed");

        // if set_not_fill_cache, it should work
        {
            let mut req_ctx = test_ctx.req_ctx.lock().unwrap();
            // in previous test, get_not_fill_cache should return false.
            assert!(!req_ctx.context.get_not_fill_cache());
            req_ctx.context.set_not_fill_cache(true);
        }
        let store = test_ctx
            .get_local_region_storage_with_check()
            .expect("should succeed");
        assert!(!store.is_fill_cache());

        // cannot find peer
        {
            test_ctx.req_region.lock().unwrap().peers[1].store_id = 99999999;
        }
        let err = test_ctx
            .get_local_region_storage_with_check()
            .err()
            .unwrap();
        println!("error: {}", err);
        assert!(err.to_string().contains("cannot find peer in region"));
        {
            test_ctx.req_region.lock().unwrap().peers[1].store_id = test_ctx.store_id;
        }

        // always not allow in flashback
        {
            test_ctx.req_ctx.lock().unwrap().allowed_in_flashback = true;
        }
        test_ctx
            // snap_ctx.allowed_in_flashback will be validated in pre_async_snapshot func
            .get_local_region_storage_with_check()
            .expect("should succeed");

        // should return error when engine returns error
        unsafe {
            with_tls_engine(|e: &mut MockEngine| e.rocks_engine().trigger_not_leader());
        }
        let err = test_ctx
            .get_local_region_storage_with_check()
            .err()
            .unwrap();
        assert!(err.to_string().contains("not_leader"));
    }

    #[test]
    fn test_secondary_tikv_storage_accessor() {
        set_tls_engine(TestEngineBuilder::new().build().unwrap());
        defer! {
            unsafe {destroy_tls_engine::<RocksEngine>()}
        }
        let def_req = default_req_ctx_support_snap_accessor();
        let store_id = def_req.context.get_peer().get_store_id();
        let store_accessor = ExtraSnapStoreAccessor::<RocksEngine>::new(def_req.into()).unwrap();
        let storage_accessor = dag::ExtraTiKVStorageAccessor::<
            ExtraSnapStoreAccessor<RocksEngine>,
        >::from_store_accessor(store_accessor);

        let storage = block_on(
            storage_accessor.get_local_region_storage(
                &metapb::Region {
                    id: 123,
                    start_key: b"a".to_vec(),
                    end_key: b"z".to_vec(),
                    peers: vec![metapb::Peer {
                        id: 1,
                        store_id,
                        ..Default::default()
                    }]
                    .into(),
                    ..Default::default()
                },
                &[coppb::KeyRange {
                    start: b"a".to_vec(),
                    end: b"b".to_vec(),
                    ..Default::default()
                }],
            ),
        )
        .unwrap();

        // should always disable check_can_be_cached
        assert!(storage.met_uncacheable_data().is_none());
    }
}
