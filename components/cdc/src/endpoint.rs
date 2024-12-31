// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::{Ord, Ordering as CmpOrdering, PartialOrd, Reverse},
    collections::BinaryHeap,
    fmt,
    marker::PhantomData,
    sync::{
        atomic::{AtomicIsize, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::Duration,
};

use causal_ts::{CausalTsProvider, CausalTsProviderImpl};
use collections::{HashMap, HashMapEntry, HashSet};
use concurrency_manager::ConcurrencyManager;
use crossbeam::atomic::AtomicCell;
use engine_traits::KvEngine;
use futures::{channel::mpsc::UnboundedSender, executor::block_on, lock::Mutex, StreamExt};
use grpcio::Environment;
use kvproto::{
    cdcpb::{
        ChangeDataRequest, ChangeDataRequestKvApi, ClusterIdMismatch as ErrorClusterIdMismatch,
        Compatibility as ErrorCompatibility, DuplicateRequest as ErrorDuplicateRequest,
        Error as EventError,
    },
    kvrpcpb::ApiVersion,
    metapb::RegionEpoch,
};
use online_config::{ConfigChange, OnlineConfig};
use pd_client::{Feature, PdClient};
use raftstore::{
    coprocessor::{ObserveHandle, ObserveId},
    router::CdcHandle,
    store::fsm::store::StoreRegionMeta,
};
use resolved_ts::{resolve_by_raft, LeadershipResolver};
use security::SecurityManager;
use tikv::{
    config::{CdcConfig, ResolvedTsConfig},
    storage::kv::LocalTablets,
};
use tikv_util::{
    debug, error, impl_display_as_debug, info,
    memory::MemoryQuota,
    slow_log,
    sys::thread::ThreadBuildWrapper,
    time::{Limiter, SlowTimer},
    warn,
    worker::{Runnable, RunnableWithTimer, ScheduleError, Scheduler},
    DeferContext,
};
use tokio::{
    runtime::{Builder, Handle, Runtime},
    sync::Semaphore,
};
use txn_types::{TimeStamp, TxnExtra, TxnExtraScheduler};

use crate::{
    channel::DownstreamSink,
    delegate::{
        Delegate, DelegateMeta, DelegateTask, Downstream, DownstreamId, DownstreamState,
        ObservedRange,
    },
    fair_queues::{self, FairQueues},
    initializer::Initializer,
    metrics::*,
    old_value::OldValueCache,
    service::{validate_kv_api, Conn, ConnId, FeatureGate, RequestId},
    CdcObserver, Error,
};

const FEATURE_RESOLVED_TS_STORE: Feature = Feature::require(5, 0, 0);
const METRICS_FLUSH_INTERVAL: u64 = 1_000; // 1s

pub enum Deregister {
    Conn(ConnId),
    Request {
        conn_id: ConnId,
        request_id: RequestId,
    },
    Region {
        conn_id: ConnId,
        request_id: RequestId,
        region_id: u64,
    },
    Downstream {
        conn_id: ConnId,
        request_id: RequestId,
        region_id: u64,
        downstream_id: DownstreamId,
    },
    Delegate {
        region_id: u64,
        observe_id: ObserveId,
    },
}

impl_display_as_debug!(Deregister);

impl fmt::Debug for Deregister {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut de = f.debug_struct("Deregister");
        match self {
            Deregister::Conn(ref conn_id) => {
                de.field("type", &"Conn").field("conn_id", conn_id).finish()
            }
            Deregister::Request {
                ref conn_id,
                ref request_id,
            } => de
                .field("type", &"Request")
                .field("conn_id", conn_id)
                .field("request_id", request_id)
                .finish(),
            Deregister::Region {
                ref conn_id,
                ref request_id,
                ref region_id,
            } => de
                .field("type", &"Region")
                .field("conn_id", conn_id)
                .field("request_id", request_id)
                .field("region_id", region_id)
                .finish(),
            Deregister::Downstream {
                ref conn_id,
                ref request_id,
                ref region_id,
                ref downstream_id,
            } => de
                .field("type", &"Downstream")
                .field("conn_id", conn_id)
                .field("request_id", request_id)
                .field("region_id", region_id)
                .field("downstream_id", downstream_id)
                .finish(),
            Deregister::Delegate {
                ref region_id,
                ref observe_id,
            } => de
                .field("type", &"Delegate")
                .field("region_id", region_id)
                .field("observe_id", observe_id)
                .finish(),
        }
    }
}

pub enum Validate {
    Region(u64, Box<dyn FnOnce(Option<&Delegate>) + Send>),
    OldValueCache(Box<dyn FnOnce(&OldValueCache) + Send>),
}

pub enum Task {
    OpenConn {
        conn: Conn,
    },
    Register {
        request: ChangeDataRequest,
        downstream: Downstream,
    },
    Deregister(Deregister),
    MinTs {
        regions: Vec<u64>,
        min_ts: TimeStamp,
        current_ts: TimeStamp,
    },
    CollectProgress,
    RegisterMinTsEvent {
        leader_resolver: LeadershipResolver,
    },
    // The result of ChangeCmd should be returned from CDC Endpoint to ensure
    // the downstream switches to Normal after the previous commands was sunk.
    TxnExtra(TxnExtra),
    ChangeConfig(ConfigChange),
    Validate(Validate),
}

impl_display_as_debug!(Task);

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut de = f.debug_struct("CdcTask");
        let de = match self {
            Task::OpenConn { ref conn } => de.field("type", &"OpenConn").field("conn_id", &conn.id),
            Task::Register {
                ref request,
                ref downstream,
                ..
            } => de
                .field("type", &"Register")
                .field("request", request)
                .field("downstream", downstream),
            Task::Deregister(deregister) => de
                .field("type", &"Deregister")
                .field("deregister", deregister),
            Task::MinTs {
                ref min_ts,
                ref current_ts,
                ..
            } => de
                .field("type", &"MinTs")
                .field("current_ts", current_ts)
                .field("min_ts", min_ts),
            Task::CollectProgress => de.field("type", &"CollectProgress"),
            Task::RegisterMinTsEvent { .. } => de.field("type", &"RegisterMinTsEvent"),
            Task::TxnExtra(_) => de.field("type", &"TxnExtra"),
            Task::ChangeConfig(change) => de.field("type", &"ChangeConfig").field("change", change),
            Task::Validate(..) => de.field("type", &"Validate"),
        };
        de.finish()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct ResolvedRegion {
    region_id: u64,
    resolved_ts: TimeStamp,
}

impl PartialOrd for ResolvedRegion {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for ResolvedRegion {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.resolved_ts.cmp(&other.resolved_ts)
    }
}

#[derive(Default, Debug)]
pub(crate) struct ResolvedRegionHeap {
    // BinaryHeap is max heap, so we reverse order to get a min heap.
    heap: BinaryHeap<Reverse<ResolvedRegion>>,
}

impl ResolvedRegionHeap {
    pub(crate) fn push(&mut self, region_id: u64, resolved_ts: TimeStamp) {
        self.heap.push(Reverse(ResolvedRegion {
            region_id,
            resolved_ts,
        }))
    }

    // Pop slow regions and the minimum resolved ts among them.
    fn pop(&mut self, count: usize) -> (TimeStamp, HashSet<u64>) {
        let mut min_resolved_ts = TimeStamp::max();
        let mut outliers = HashSet::with_capacity_and_hasher(count, Default::default());
        for _ in 0..count {
            if let Some(resolved_region) = self.heap.pop() {
                outliers.insert(resolved_region.0.region_id);
                if min_resolved_ts > resolved_region.0.resolved_ts {
                    min_resolved_ts = resolved_region.0.resolved_ts;
                }
            } else {
                break;
            }
        }
        (min_resolved_ts, outliers)
    }

    fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

#[derive(Default, Debug)]
pub(crate) struct Advance {
    // multiplexing means one region can be subscribed multiple times in one `Conn`,
    // in which case progresses are grouped by (ConnId, request_id).
    pub(crate) multiplexing: HashMap<(ConnId, RequestId), ResolvedRegionHeap>,

    // exclusive means one region can only be subscribed one time in one `Conn`,
    // in which case progresses are grouped by ConnId.
    pub(crate) exclusive: HashMap<ConnId, ResolvedRegionHeap>,

    // To be compatible with old TiCDC client before v4.0.8.
    // TODO(qupeng): we can deprecate support for too old TiCDC clients.
    // map[(ConnId, region_id)]->(request_id, ts).
    pub(crate) compat: HashMap<(ConnId, u64), (RequestId, TimeStamp)>,

    min_resolved_ts: u64,
    min_ts_region_id: u64,
}

impl Advance {
    fn emit_resolved_ts(&mut self, connections: &HashMap<ConnId, Conn>) {
        let mut batch_min_resolved_ts = 0;
        let mut batch_min_ts_region_id = 0;
        let mut batch_send = |ts: u64, conn: &Conn, request_id: RequestId, regions: Vec<u64>| {
            if batch_min_resolved_ts == 0 || batch_min_resolved_ts > ts {
                batch_min_resolved_ts = ts;
                if !regions.is_empty() {
                    batch_min_ts_region_id = regions[0];
                }
            }
            conn.sink.send_batch_resolved_ts(regions, request_id.0, ts);
        };

        let mut compat_min_resolved_ts = 0;
        let mut compat_min_ts_region_id = 0;
        let mut compat_send = |ts: u64, conn: &Conn, region_id: u64, request_id: RequestId| {
            if compat_min_resolved_ts == 0 || compat_min_resolved_ts > ts {
                compat_min_resolved_ts = ts;
                compat_min_ts_region_id = region_id;
            }
            conn.sink
                .send_region_resolved_ts(region_id, request_id.0, ts);
        };

        let multiplexing = std::mem::take(&mut self.multiplexing).into_iter();
        let exclusive = std::mem::take(&mut self.exclusive).into_iter();
        let unioned = multiplexing
            .map(|((a, b), c)| (a, b, c))
            .chain(exclusive.map(|(a, c)| (a, RequestId(0), c)));

        for (conn_id, request_id, mut region_ts_heap) in unioned {
            let conn = connections.get(&conn_id).unwrap();
            let mut batch_count = 8;
            while !region_ts_heap.is_empty() {
                let (ts, regions) = region_ts_heap.pop(batch_count);
                batch_send(ts.into_inner(), conn, request_id, Vec::from_iter(regions));
                batch_count *= 4;
            }
        }

        for ((conn_id, region_id), (request_id, ts)) in std::mem::take(&mut self.compat) {
            let conn = connections.get(&conn_id).unwrap();
            compat_send(ts.into_inner(), conn, region_id, request_id);
        }

        if batch_min_resolved_ts > 0 {
            self.min_resolved_ts = batch_min_resolved_ts;
            self.min_ts_region_id = batch_min_ts_region_id;
        } else {
            self.min_resolved_ts = compat_min_resolved_ts;
            self.min_ts_region_id = compat_min_ts_region_id;
        }
    }
}

pub struct Endpoint<T, E, S> {
    cluster_id: u64,

    capture_regions: HashMap<u64, DelegateMeta>,
    connections: HashMap<ConnId, Conn>,
    scheduler: Scheduler<Task>,
    cdc_handle: T,
    tablets: LocalTablets<E>,
    observer: CdcObserver,

    pd_client: Arc<dyn PdClient>,
    tso_worker: Runtime,
    store_meta: Arc<StdMutex<S>>,
    /// The concurrency manager for transactions. It's needed for CDC to check
    /// locks when calculating resolved_ts.
    concurrency_manager: ConcurrencyManager,

    raftstore_v2: bool,
    config: CdcConfig,
    resolved_ts_config: ResolvedTsConfig,
    api_version: ApiVersion,

    workers: Runtime,

    // Incremental scan stuffs.
    scan_workers: Runtime,
    scan_task_counter: Arc<AtomicIsize>,
    scan_concurrency_semaphore: Arc<Semaphore>,
    scan_speed_limiter: Limiter,
    fetch_speed_limiter: Limiter,
    max_scan_batch_bytes: usize,
    max_scan_batch_size: usize,
    pending_scans: FairQueues<(ConnId, RequestId), PendingInitialize<E>>,

    sink_memory_quota: Arc<MemoryQuota>,
    old_value_cache: Arc<Mutex<OldValueCache>>,
    causal_ts_provider: Option<Arc<CausalTsProviderImpl>>,

    // Metrics and logging.
    current_ts: TimeStamp,
    min_resolved_ts: TimeStamp,
    min_ts_region_id: u64,
    resolved_region_count: usize,
    unresolved_region_count: usize,

    pending_progress_collecting: usize,
}

impl<T: 'static + CdcHandle<E>, E: KvEngine, S: StoreRegionMeta> Endpoint<T, E, S> {
    pub fn new(
        cluster_id: u64,
        config: &CdcConfig,
        resolved_ts_config: &ResolvedTsConfig,
        raftstore_v2: bool,
        api_version: ApiVersion,
        pd_client: Arc<dyn PdClient>,
        scheduler: Scheduler<Task>,
        cdc_handle: T,
        tablets: LocalTablets<E>,
        observer: CdcObserver,
        store_meta: Arc<StdMutex<S>>,
        concurrency_manager: ConcurrencyManager,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
        sink_memory_quota: Arc<MemoryQuota>,
        causal_ts_provider: Option<Arc<CausalTsProviderImpl>>,
    ) -> Endpoint<T, E, S> {
        let (pending_scans, scan_consumer) = fair_queues::create();

        let ep = Self::new_inner(
            cluster_id,
            config,
            resolved_ts_config,
            raftstore_v2,
            api_version,
            pd_client.clone(),
            scheduler,
            cdc_handle,
            tablets,
            observer,
            store_meta.clone(),
            concurrency_manager,
            sink_memory_quota,
            causal_ts_provider,
            pending_scans,
        );

        ep.handle_pending_scans(scan_consumer);

        let store_id = store_meta.lock().unwrap().store_id();
        let read_progress = store_meta.lock().unwrap().region_read_progress().clone();
        let leader_resolver = LeadershipResolver::new(
            store_id,
            pd_client,
            env,
            security_mgr,
            read_progress,
            Duration::from_secs(60),
        );

        ep.register_min_ts_event(leader_resolver);
        ep
    }

    // TODO: the only difference with `new` is `scan_max_batch_size`.
    // We will update cases depend on it and remove `new_for_integration_tests`.
    pub fn new_for_integration_tests(
        cluster_id: u64,
        config: &CdcConfig,
        resolved_ts_config: &ResolvedTsConfig,
        raftstore_v2: bool,
        api_version: ApiVersion,
        pd_client: Arc<dyn PdClient>,
        scheduler: Scheduler<Task>,
        cdc_handle: T,
        tablets: LocalTablets<E>,
        observer: CdcObserver,
        store_meta: Arc<StdMutex<S>>,
        concurrency_manager: ConcurrencyManager,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
        sink_memory_quota: Arc<MemoryQuota>,
        causal_ts_provider: Option<Arc<CausalTsProviderImpl>>,
    ) -> Endpoint<T, E, S> {
        let (pending_scans, scan_consumer) = fair_queues::create();

        let mut ep = Self::new_inner(
            cluster_id,
            config,
            resolved_ts_config,
            raftstore_v2,
            api_version,
            pd_client.clone(),
            scheduler,
            cdc_handle,
            tablets,
            observer,
            store_meta.clone(),
            concurrency_manager,
            sink_memory_quota,
            causal_ts_provider,
            pending_scans,
        );
        ep.max_scan_batch_size = 2;

        ep.handle_pending_scans(scan_consumer);

        let store_id = store_meta.lock().unwrap().store_id();
        let read_progress = store_meta.lock().unwrap().region_read_progress().clone();
        let leader_resolver = LeadershipResolver::new(
            store_id,
            pd_client,
            env,
            security_mgr,
            read_progress,
            Duration::from_secs(60),
        );

        ep.register_min_ts_event(leader_resolver);
        ep
    }

    fn new_inner(
        cluster_id: u64,
        config: &CdcConfig,
        resolved_ts_config: &ResolvedTsConfig,
        raftstore_v2: bool,
        api_version: ApiVersion,
        pd_client: Arc<dyn PdClient>,
        scheduler: Scheduler<Task>,
        cdc_handle: T,
        tablets: LocalTablets<E>,
        observer: CdcObserver,
        store_meta: Arc<StdMutex<S>>,
        concurrency_manager: ConcurrencyManager,
        sink_memory_quota: Arc<MemoryQuota>,
        causal_ts_provider: Option<Arc<CausalTsProviderImpl>>,
        pending_scans: FairQueues<(ConnId, RequestId), PendingInitialize<E>>,
    ) -> Endpoint<T, E, S> {
        CDC_SINK_CAP.set(sink_memory_quota.capacity() as i64);

        let workers = Builder::new_multi_thread()
            .thread_name("cdc-main-workers")
            .enable_time()
            .worker_threads(config.responser_threads)
            .with_sys_hooks()
            .build()
            .unwrap();
        let scan_workers = Builder::new_multi_thread()
            .thread_name("cdc-scan-workers")
            .worker_threads(config.incremental_scan_threads)
            .with_sys_hooks()
            .build()
            .unwrap();
        let tso_worker = Builder::new_multi_thread()
            .thread_name("cdc-tso")
            .worker_threads(config.tso_worker_threads)
            .enable_time()
            .with_sys_hooks()
            .build()
            .unwrap();

        Endpoint {
            cluster_id,

            capture_regions: HashMap::default(),
            connections: HashMap::default(),
            scheduler,
            cdc_handle,
            tablets,
            observer,

            pd_client,
            tso_worker,
            store_meta,
            concurrency_manager,

            raftstore_v2,
            config: config.clone(),
            resolved_ts_config: resolved_ts_config.clone(),
            api_version,

            workers,

            scan_workers,
            scan_task_counter: Default::default(),
            scan_concurrency_semaphore: Arc::new(Semaphore::new(
                config.incremental_scan_concurrency,
            )),
            scan_speed_limiter: Limiter::new(config.incremental_scan_speed_limit.0 as _),
            fetch_speed_limiter: Limiter::new(config.incremental_fetch_speed_limit.0 as _),
            max_scan_batch_bytes: 1024 * 1024,
            max_scan_batch_size: 1024,
            pending_scans,

            sink_memory_quota,
            old_value_cache: Arc::new(Mutex::new(OldValueCache::new(
                config.old_value_cache_memory_quota,
            ))),
            causal_ts_provider,

            current_ts: TimeStamp::zero(),
            min_resolved_ts: TimeStamp::max(),
            min_ts_region_id: 0,
            resolved_region_count: 0,
            unresolved_region_count: 0,

            pending_progress_collecting: 0,
        }
    }

    pub fn get_responser_workers(&self) -> Handle {
        self.workers.handle().clone()
    }

    fn on_change_cfg(&mut self, change: ConfigChange) {
        // Validate first.
        let mut validate_cfg = self.config.clone();
        if let Err(e) = validate_cfg.update(change) {
            warn!("cdc config update failed"; "error" => ?e);
            return;
        }
        if let Err(e) = validate_cfg.validate(self.raftstore_v2) {
            warn!("cdc config update failed"; "error" => ?e);
            return;
        }
        let change = self.config.diff(&validate_cfg);
        info!(
            "cdc config updated";
            "current_config" => ?self.config,
            "change" => ?change
        );
        // Update the config here. The following adjustments will all use the new
        // values.
        self.config.update(change.clone()).unwrap();

        // Maybe the cache will be lost due to smaller capacity,
        // but it is acceptable.
        if change.get("old_value_cache_memory_quota").is_some() {
            let mut cache = block_on(self.old_value_cache.lock());
            cache.resize(self.config.old_value_cache_memory_quota);
        }

        // Maybe the limit will be exceeded for a while after the concurrency becomes
        // smaller, but it is acceptable.
        if change.get("incremental_scan_concurrency").is_some() {
            self.scan_concurrency_semaphore =
                Arc::new(Semaphore::new(self.config.incremental_scan_concurrency))
        }

        if change.get("sink_memory_quota").is_some() {
            self.sink_memory_quota
                .set_capacity(self.config.sink_memory_quota.0 as usize);
            CDC_SINK_CAP.set(self.sink_memory_quota.capacity() as i64);
        }

        if change.get("incremental_scan_speed_limit").is_some() {
            let new_speed_limit = if self.config.incremental_scan_speed_limit.0 > 0 {
                self.config.incremental_scan_speed_limit.0 as f64
            } else {
                f64::INFINITY
            };

            self.scan_speed_limiter.set_speed_limit(new_speed_limit);
        }
        if change.get("incremental_fetch_speed_limit").is_some() {
            let new_speed_limit = if self.config.incremental_fetch_speed_limit.0 > 0 {
                self.config.incremental_fetch_speed_limit.0 as f64
            } else {
                f64::INFINITY
            };

            self.fetch_speed_limiter.set_speed_limit(new_speed_limit);
        }
    }

    pub fn set_max_scan_batch_size(&mut self, max_scan_batch_size: usize) {
        self.max_scan_batch_size = max_scan_batch_size;
    }

    fn on_deregister(&mut self, deregister: Deregister) {
        info!("cdc deregister"; "deregister" => ?deregister);
        match deregister {
            Deregister::Conn(conn_id) => {
                let conn = self.connections.remove(&conn_id).unwrap();
                conn.iter_downstreams(|_, region_id, downstream_id, _| {
                    if let Some(delegate) = self.capture_regions.get(&region_id) {
                        let _ = delegate.sched.unbounded_send(DelegateTask::StopDownstream {
                            err: None,
                            downstream_id,
                        });
                    }
                });
            }
            Deregister::Request {
                conn_id,
                request_id,
            } => {
                let conn = self.connections.get_mut(&conn_id).unwrap();
                for (region_id, downstream_id) in conn.unsubscribe_request(request_id) {
                    if let Some(delegate) = self.capture_regions.get(&region_id) {
                        let _ = delegate.sched.unbounded_send(DelegateTask::StopDownstream {
                            err: Some(Error::Other("region not found".into())),
                            downstream_id,
                        });
                    }
                }
            }
            Deregister::Region {
                conn_id,
                request_id,
                region_id,
            } => {
                let conn = self.connections.get_mut(&conn_id).unwrap();
                if let Some(downstream_id) = conn.get_downstream(request_id, region_id) {
                    if let Some(delegate) = self.capture_regions.get(&region_id) {
                        let _ = delegate.sched.unbounded_send(DelegateTask::StopDownstream {
                            err: Some(Error::Other("region not found".into())),
                            downstream_id,
                        });
                    }
                }
            }
            Deregister::Downstream {
                conn_id,
                request_id,
                region_id,
                downstream_id,
            } => {
                let conn = match self.connections.get_mut(&conn_id) {
                    Some(conn) => conn,
                    None => return,
                };
                if let Some(new_downstream_id) = conn.get_downstream(request_id, region_id) {
                    // To avoid ABA problem, we must check the unique DownstreamId.
                    if new_downstream_id == downstream_id {
                        conn.unsubscribe(request_id, region_id);
                    }
                }
            }
            Deregister::Delegate {
                region_id,
                observe_id,
            } => {
                if let HashMapEntry::Occupied(x) = self.capture_regions.entry(region_id) {
                    // To avoid ABA problem, we must check the unique ObserveId.
                    if x.get().handle.id == observe_id {
                        let delegate = x.remove();
                        assert!(delegate.sched.is_closed());
                    }
                }
                self.observer.unsubscribe_region(region_id, observe_id);
            }
        }
    }

    pub fn on_register(&mut self, mut request: ChangeDataRequest, downstream: Downstream) {
        let kv_api = request.get_kv_api();
        let region_id = request.region_id;
        let request_id = RequestId(request.request_id);
        let conn_id = downstream.conn_id;
        let downstream_id = downstream.id;
        let filter_loop = downstream.filter_loop;
        let downstream_state = downstream.get_state();
        let downstream_sink = downstream.sink.clone();
        let observed_range = downstream.observed_range.clone();

        // The connection can be deregistered by some internal errors. Clients will
        // always be notified by closing the GRPC server stream, so it's OK to drop
        // the task directly.
        let conn = match self.connections.get_mut(&conn_id) {
            Some(conn) => conn,
            None => {
                info!("cdc register region on an deregistered connection, ignore";
                    "region_id" => region_id,
                    "conn_id" => ?conn_id,
                    "request_id" => ?request_id,
                    "downstream_id" => ?downstream_id);
                return;
            }
        };

        // Check if the cluster id matches if supported.
        if conn.features.contains(FeatureGate::VALIDATE_CLUSTER_ID) {
            let request_cluster_id = request.get_header().get_cluster_id();
            if self.cluster_id != request_cluster_id {
                let mut err_event = EventError::default();
                let mut err = ErrorClusterIdMismatch::default();
                err.set_current(self.cluster_id);
                err.set_request(request_cluster_id);
                err_event.set_cluster_id_mismatch(err);
                let _ = block_on(downstream_sink.cancel_by_error(err_event));
                return;
            }
        }

        if !validate_kv_api(kv_api, self.api_version) {
            error!("cdc RawKv is supported by api-version 2 only. TxnKv is not supported now.");
            let mut err_event = EventError::default();
            let mut err = ErrorCompatibility::default();
            err.set_required_version("6.2.0".to_string());
            err_event.set_compatibility(err);
            let _ = block_on(downstream_sink.cancel_by_error(err_event));
            return;
        }

        let scan_task_counter = self.scan_task_counter.clone();
        let scan_task_count = scan_task_counter.fetch_add(1, Ordering::Relaxed);
        let release_scan_task_counter = DeferContext::new(Box::new(move || {
            scan_task_counter.fetch_sub(1, Ordering::Relaxed);
        }) as _);
        if scan_task_count >= self.config.incremental_scan_concurrency_limit as isize {
            debug!("cdc rejects registration, too many scan tasks";
                "region_id" => region_id,
                "conn_id" => ?conn_id,
                "request_id" => ?request_id,
                "scan_task_count" => scan_task_count,
                "incremental_scan_concurrency_limit" => self.config.incremental_scan_concurrency_limit,
            );
            // To avoid OOM (e.g., https://github.com/tikv/tikv/issues/16035),
            // TiKV needs to reject and return error immediately.
            let mut err_event = EventError::default();
            err_event.mut_server_is_busy().reason = "too many pending incremental scans".to_owned();
            let _ = block_on(downstream_sink.cancel_by_error(err_event));
            return;
        }

        let txn_extra_op = match self.store_meta.lock().unwrap().reader(region_id) {
            Some(reader) => reader.txn_extra_op.clone(),
            None => {
                error!("cdc register for a not found region"; "region_id" => region_id);
                let mut err_event = EventError::default();
                err_event.mut_region_not_found().region_id = region_id;
                let _ = block_on(downstream_sink.cancel_by_error(err_event));
                return;
            }
        };

        if conn.subscribe(request_id, region_id, &downstream).is_some() {
            let mut err_event = EventError::default();
            let mut err = ErrorDuplicateRequest::default();
            err.set_region_id(region_id);
            err_event.set_duplicate_request(err);
            let _ = block_on(downstream_sink.cancel_by_error(err_event));
            error!("cdc duplicate register";
                "region_id" => region_id,
                "conn_id" => ?conn_id,
                "request_id" => ?request_id,
                "downstream_id" => ?downstream_id);
            return;
        }

        let mut is_new_delegate = false;
        let delegate = match self.capture_regions.entry(region_id) {
            HashMapEntry::Occupied(e) => e.get().clone(),
            HashMapEntry::Vacant(e) => {
                let mut d = Delegate::new(
                    region_id,
                    self.scheduler.clone(),
                    self.sink_memory_quota.clone(),
                    self.old_value_cache.clone(),
                    txn_extra_op,
                );
                let delegate = d.meta();
                e.insert(delegate.clone());

                let m = delegate.clone();
                self.workers
                    .spawn(async move { m.flush_stats_periodically().await });
                self.workers.spawn(async move { d.handle_tasks().await });

                let old_ob = self.observer.subscribe_region(
                    region_id,
                    delegate.handle.id,
                    delegate.sched.clone(),
                );
                assert!(old_ob.is_none());

                is_new_delegate = true;
                delegate
            }
        };

        let (cb, fut) = tikv_util::future::paired_future_callback();
        if delegate
            .sched
            .unbounded_send(DelegateTask::Subscribe { downstream, cb })
            .is_err()
        {
            error!("cdc delegate is stopped when subscribe";
                "region_id" => region_id,
                "conn_id" => ?conn_id,
                "request_id" => ?request_id,
                "downstream_id" => ?downstream_id);
            let mut err_event = EventError::default();
            err_event.mut_region_not_found().region_id = region_id;
            let _ = block_on(downstream_sink.cancel_by_error(err_event));

            self.on_deregister(Deregister::Downstream {
                conn_id,
                request_id,
                region_id,
                downstream_id,
            });
            if is_new_delegate {
                let observe_id = delegate.handle.id;
                self.on_deregister(Deregister::Delegate {
                    region_id,
                    observe_id,
                });
            }
            return;
        }

        let handle = delegate.handle.clone();
        info!("cdc register region";
            "region_id" => region_id,
            "conn_id" => ?conn.id,
            "request_id" => ?request_id,
            "observe_id" => ?handle.id,
            "downstream_id" => ?downstream_id);

        CDC_SCAN_TASKS.with_label_values(&["total"]).inc();
        let scan_task = PendingInitialize {
            region_id,
            checkpoint_ts: request.checkpoint_ts.into(),
            region_epoch: request.take_region_epoch(),
            observed_range,
            kv_api,
            filter_loop,

            observe_handle: handle,
            downstream_id,
            downstream_state,
            sched: delegate.sched.clone(),
            sink: downstream_sink.clone(),

            fut,
            release_scan_task_counter,
            _phantom: Default::default(),
        };
        assert!(self.pending_scans.push((conn_id, request_id), scan_task));
    }

    fn on_min_ts(&mut self, regions: Vec<u64>, min_ts: TimeStamp, current_ts: TimeStamp) {
        self.current_ts = current_ts;
        self.min_resolved_ts = current_ts;

        let mut futs = Vec::with_capacity(regions.len());
        for region_id in regions {
            if let Some(d) = self.capture_regions.get(&region_id) {
                let (cb, fut) = tikv_util::future::paired_future_callback();
                let task = DelegateTask::MinTs {
                    min_ts,
                    current_ts,
                    cb,
                };
                let _ = d.sched.unbounded_send(task);
                futs.push(fut);
            }
        }
        if self.pending_progress_collecting > 0 {
            return;
        }

        self.pending_progress_collecting += 1;
        let scheduler = self.scheduler.clone();
        self.workers.spawn(async move {
            for fut in futs {
                let _ = fut.await;
            }
            let _ = scheduler.schedule(Task::CollectProgress);
        });
    }

    fn on_collect_progress(&mut self) {
        self.pending_progress_collecting -= 1;

        let mut advance = Advance::default();
        for (conn_id, conn) in &self.connections {
            if conn.features.contains(FeatureGate::STREAM_MULTIPLEXING) {
                conn.iter_downstreams(|request_id, region_id, _, advanced_to| {
                    let advanced_to = TimeStamp::from(advanced_to.load(Ordering::Acquire));
                    if !advanced_to.is_zero() {
                        let heap = advance
                            .multiplexing
                            .entry((*conn_id, request_id))
                            .or_default();
                        heap.push(region_id, advanced_to);
                    }
                });
            } else if conn.features.contains(FeatureGate::BATCH_RESOLVED_TS) {
                conn.iter_downstreams(|_, region_id, _, advanced_to| {
                    let advanced_to = TimeStamp::from(advanced_to.load(Ordering::Acquire));
                    if !advanced_to.is_zero() {
                        let heap = advance.exclusive.entry(*conn_id).or_default();
                        heap.push(region_id, advanced_to);
                    }
                });
            } else {
                conn.iter_downstreams(|request_id, region_id, _, advanced_to| {
                    let advanced_to = TimeStamp::from(advanced_to.load(Ordering::Acquire));
                    if !advanced_to.is_zero() {
                        advance
                            .compat
                            .insert((*conn_id, region_id), (request_id, advanced_to));
                    }
                });
            }
        }
        advance.emit_resolved_ts(&self.connections);
        self.min_resolved_ts = advance.min_resolved_ts.into();
        self.min_ts_region_id = advance.min_ts_region_id;
    }

    fn on_register_min_ts_event(&self, mut leader_resolver: LeadershipResolver) {
        let min_ts_interval = self.config.min_ts_interval.0;
        let advance_ts_interval = self.resolved_ts_config.advance_ts_interval.0;
        let pd_client = self.pd_client.clone();
        let scheduler = self.scheduler.clone();
        let cdc_handle = self.cdc_handle.clone();
        let regions: Vec<u64> = self.capture_regions.keys().copied().collect();
        let cm = self.concurrency_manager.clone();
        let hibernate_regions_compatible = self.config.hibernate_regions_compatible;
        let causal_ts_provider = self.causal_ts_provider.clone();

        let fut = async move {
            // Ignore get tso errors since we will retry every `min_ts_interval`.
            let current_ts = match causal_ts_provider {
                // TiKV API v2 is enabled when causal_ts_provider is Some.
                // In this scenario, get TSO from causal_ts_provider to make sure that
                // RawKV write requests will get larger TSO after this point.
                // RawKV CDC's resolved_ts is guaranteed by ConcurrencyManager::global_min_lock_ts,
                // which lock flying keys's ts in raw put and delete interfaces in `Storage`.
                Some(provider) => provider.async_get_ts().await.unwrap_or_default(),
                None => pd_client.get_tso().await.unwrap_or_default(),
            };
            let mut min_ts = current_ts;

            // Sync with concurrency manager so that it can work correctly when
            // optimizations like async commit is enabled.
            // Note: This step must be done before scheduling `Task::MinTs` task, and the
            // resolver must be checked in or after `Task::MinTs`' execution.
            cm.update_max_ts(min_ts, "cdc").unwrap();
            if let Some(min_mem_lock_ts) = cm.global_min_lock_ts() {
                if min_mem_lock_ts < min_ts {
                    min_ts = min_mem_lock_ts;
                }
            }

            let slow_timer = SlowTimer::default();
            let regions = if hibernate_regions_compatible
                && pd_client
                    .feature_gate()
                    .can_enable(FEATURE_RESOLVED_TS_STORE)
            {
                CDC_RESOLVED_TS_ADVANCE_METHOD.set(1);
                leader_resolver
                    .resolve(regions, min_ts, Some(advance_ts_interval))
                    .await
            } else {
                CDC_RESOLVED_TS_ADVANCE_METHOD.set(0);
                resolve_by_raft(regions, min_ts, cdc_handle).await
            };
            slow_log!(T slow_timer, "cdc resolve region leadership");

            if !regions.is_empty() {
                match scheduler.schedule(Task::MinTs {
                    regions,
                    min_ts,
                    current_ts,
                }) {
                    Ok(_) | Err(ScheduleError::Stopped(_)) => (),
                    Err(e) => warn!("cdc failed to schedule MinTs"; "err" => ?e),
                }
            }

            tokio::time::sleep(min_ts_interval).await;
            match scheduler.schedule_force(Task::RegisterMinTsEvent { leader_resolver }) {
                Ok(_) | Err(ScheduleError::Stopped(..)) => (),
                Err(ScheduleError::Full(..)) => unreachable!(),
            }
        };
        self.tso_worker.spawn(fut);
    }

    fn on_open_conn(&mut self, conn: Conn) {
        self.connections.insert(conn.id, conn);
    }

    fn handle_pending_scans(
        &self,
        mut scan_consumer: fair_queues::Receiver<(ConnId, RequestId), PendingInitialize<E>>,
    ) {
        let tablets = self.tablets.clone();
        let scan_concurrency_semaphore = self.scan_concurrency_semaphore.clone();
        let scan_speed_limiter = self.scan_speed_limiter.clone();
        let fetch_speed_limiter = self.fetch_speed_limiter.clone();
        let max_scan_batch_bytes = self.max_scan_batch_bytes;
        let max_scan_batch_size = self.max_scan_batch_size;
        let ts_filter_ratio = self.config.incremental_scan_ts_filter_ratio;
        let cdc_handle = self.cdc_handle.clone();

        let workers_handle = self.scan_workers.handle().clone();
        self.scan_workers.spawn(async move {
            while let Some(((conn_id, request_id), task)) = scan_consumer.next().await {
                let permit = scan_concurrency_semaphore.clone().acquire_owned().await;
                let release_scan_task_counter = task.release_scan_task_counter;
                let wait_until = task.fut;
                let cdc_handle_ = cdc_handle.clone();

                let mut init = Initializer {
                    region_id: task.region_id,
                    conn_id,
                    request_id,
                    checkpoint_ts: task.checkpoint_ts,
                    region_epoch: task.region_epoch,

                    build_resolver: Arc::new(Default::default()),
                    observed_range: task.observed_range,
                    observe_handle: task.observe_handle,
                    downstream_id: task.downstream_id,
                    downstream_state: task.downstream_state,

                    tablet: tablets.get(task.region_id).map(|t| t.into_owned()),
                    sched: task.sched,
                    sink: task.sink,

                    scan_speed_limiter: scan_speed_limiter.clone(),
                    fetch_speed_limiter: fetch_speed_limiter.clone(),
                    max_scan_batch_bytes,
                    max_scan_batch_size,

                    ts_filter_ratio,
                    kv_api: task.kv_api,
                    filter_loop: task.filter_loop,
                };

                workers_handle.spawn(async move {
                    if wait_until.await.is_err() {
                        info!("cdc initialize is canceled before start"; "region_id" => task.region_id,
                            "conn_id" => ?conn_id, "request_id" => ?request_id);
                        CDC_SCAN_TASKS.with_label_values(&["abort"]).inc();
                        return;
                    }
                    match init.initialize(cdc_handle_).await {
                        Ok(()) => {
                            CDC_SCAN_TASKS.with_label_values(&["finish"]).inc();
                        }
                        Err(e) => {
                            CDC_SCAN_TASKS.with_label_values(&["abort"]).inc();
                            error!("cdc initialize fail: {}", e; "region_id" => task.region_id,
                                "conn_id" => ?conn_id, "request_id" => ?request_id);
                            init.deregister_downstream(e);
                        }
                    }
                    drop(permit);
                    drop(release_scan_task_counter);
                });
            }
        });
    }

    fn register_min_ts_event(&self, leader_resolver: LeadershipResolver) {
        let scheduler = self.scheduler.clone();
        let min_ts_interval = self.config.min_ts_interval.0;
        self.tso_worker.spawn(async move {
            tokio::time::sleep(min_ts_interval).await;
            match scheduler.schedule_force(Task::RegisterMinTsEvent { leader_resolver }) {
                Ok(_) | Err(ScheduleError::Stopped(..)) => (),
                Err(ScheduleError::Full(..)) => unreachable!(),
            }
        });
    }
}

impl<T: 'static + CdcHandle<E>, E: KvEngine, S: StoreRegionMeta + Send> Runnable
    for Endpoint<T, E, S>
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        debug!("cdc run task"; "task" => %task);

        match task {
            Task::OpenConn { conn } => self.on_open_conn(conn),
            Task::Register {
                request,
                downstream,
            } => self.on_register(request, downstream),
            Task::Deregister(deregister) => self.on_deregister(deregister),
            Task::MinTs {
                regions,
                min_ts,
                current_ts,
            } => self.on_min_ts(regions, min_ts, current_ts),
            Task::CollectProgress => self.on_collect_progress(),
            Task::RegisterMinTsEvent { leader_resolver } => {
                self.on_register_min_ts_event(leader_resolver)
            }
            Task::TxnExtra(txn_extra) => {
                self.sink_memory_quota.free(txn_extra.size());
                let mut cache = block_on(self.old_value_cache.lock());
                for (k, v) in txn_extra.old_values {
                    cache.insert(k, v);
                }
                drop(cache);
            }
            Task::ChangeConfig(change) => self.on_change_cfg(change),
            Task::Validate(validate) => match validate {
                Validate::Region(region_id, validate) => {
                    match self.capture_regions.get(&region_id) {
                        Some(d) => {
                            let task = DelegateTask::Validate(validate);
                            d.sched.unbounded_send(task).unwrap();
                        }
                        None => validate(None),
                    }
                }
                Validate::OldValueCache(validate) => {
                    let cache = block_on(self.old_value_cache.lock());
                    validate(&cache);
                }
            },
        }
    }
}

impl<T: 'static + CdcHandle<E>, E: KvEngine, S: StoreRegionMeta + Send> RunnableWithTimer
    for Endpoint<T, E, S>
{
    fn on_timeout(&mut self) {
        CDC_ENDPOINT_PENDING_TASKS.set(self.scheduler.pending_tasks() as _);
        CDC_CAPTURED_REGION_COUNT.set(self.capture_regions.len() as i64);
        CDC_REGION_RESOLVE_STATUS_GAUGE_VEC
            .with_label_values(&["unresolved"])
            .set(self.unresolved_region_count as _);
        CDC_REGION_RESOLVE_STATUS_GAUGE_VEC
            .with_label_values(&["resolved"])
            .set(self.resolved_region_count as _);

        if self.min_resolved_ts != TimeStamp::max() {
            CDC_MIN_RESOLVED_TS_REGION.set(self.min_ts_region_id as i64);
            CDC_MIN_RESOLVED_TS.set(self.min_resolved_ts.physical() as i64);
            CDC_MIN_RESOLVED_TS_LAG.set(
                self.current_ts
                    .physical()
                    .saturating_sub(self.min_resolved_ts.physical()) as i64,
            );
            CDC_RESOLVED_TS_GAP_HISTOGRAM.observe(
                self.current_ts
                    .physical()
                    .saturating_sub(self.min_resolved_ts.physical()) as f64
                    / 1000f64,
            );
        }
        self.min_resolved_ts = TimeStamp::max();
        self.current_ts = TimeStamp::max();
        self.min_ts_region_id = 0;

        block_on(self.old_value_cache.lock()).flush_metrics();
        CDC_SINK_BYTES.set(self.sink_memory_quota.in_use() as i64);
    }

    fn get_interval(&self) -> Duration {
        // Currently there is only one timeout for CDC.
        Duration::from_millis(METRICS_FLUSH_INTERVAL)
    }
}

impl<T, E, S> Drop for Endpoint<T, E, S> {
    fn drop(&mut self) {
        self.pending_scans.close();
    }
}

struct PendingInitialize<E> {
    region_id: u64,
    checkpoint_ts: TimeStamp,
    region_epoch: RegionEpoch,
    observed_range: ObservedRange,
    kv_api: ChangeDataRequestKvApi,
    filter_loop: bool,

    observe_handle: ObserveHandle,
    downstream_id: DownstreamId,
    downstream_state: Arc<AtomicCell<DownstreamState>>,
    sched: UnboundedSender<DelegateTask>,
    sink: DownstreamSink,

    fut: futures::channel::oneshot::Receiver<()>,
    release_scan_task_counter: DeferContext<Box<dyn FnOnce() + Send + 'static>>,
    _phantom: PhantomData<E>,
}

pub struct CdcTxnExtraScheduler {
    scheduler: Scheduler<Task>,
    memory_quota: Arc<MemoryQuota>,
}

impl CdcTxnExtraScheduler {
    pub fn new(scheduler: Scheduler<Task>, memory_quota: Arc<MemoryQuota>) -> CdcTxnExtraScheduler {
        CdcTxnExtraScheduler {
            scheduler,
            memory_quota,
        }
    }
}

impl TxnExtraScheduler for CdcTxnExtraScheduler {
    fn schedule(&self, txn_extra: TxnExtra) {
        let size = txn_extra.size();
        if let Err(e) = self.memory_quota.alloc(size) {
            CDC_DROP_TXN_EXTRA_TASKS_COUNT.inc();
            debug!("cdc schedule txn extra failed on alloc memory quota";
                "in_use" => self.memory_quota.in_use(), "err" => ?e);
            return;
        }
        if let Err(e) = self.scheduler.schedule(Task::TxnExtra(txn_extra)) {
            warn!("cdc failed to schedule TxnExtra"; "err" => ?e);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::btree_map::BTreeMap,
        ops::{Deref, DerefMut},
        sync::mpsc::{self, RecvTimeoutError},
    };

    use crossbeam::atomic::AtomicCell;
    use engine_rocks::RocksEngine;
    use futures::executor::block_on;
    use kvproto::{cdcpb::ChangeDataRequestKvApi, errorpb::Error as ErrorHeader, metapb::Region};
    use raftstore::{
        errors::{DiscardReason, Error as RaftStoreError},
        router::{CdcRaftRouter, RaftStoreRouter},
        store::{fsm::StoreMeta, msg::CasualMessage, PeerMsg, ReadDelegate},
    };
    use semver::Version;
    use test_pd_client::TestPdClient;
    use test_raftstore::MockRaftStoreRouter;
    use tikv::{
        server::DEFAULT_CLUSTER_ID,
        storage::{kv::Engine, TestEngineBuilder},
    };
    use tikv_util::{
        config::{ReadableDuration, ReadableSize},
        worker::{dummy_scheduler, ReceiverWrapper},
    };
    use txn_types::Key;

    use super::*;
    use crate::{
        channel,
        channel::{DownstreamSink, Drain},
        delegate::{post_init_downstream, DownstreamState, MiniLock, ObservedRange},
        recv_events_timely, recv_resolved_ts_timely, recv_timeout,
    };

    struct TestEndpointSuite {
        // The order must ensure `endpoint` be dropped before other fields.
        endpoint: Endpoint<CdcRaftRouter<MockRaftStoreRouter>, RocksEngine, StoreMeta>,
        cdc_handle: CdcRaftRouter<MockRaftStoreRouter>,
        task_rx: ReceiverWrapper<Task>,
        raft_rxs: HashMap<u64, tikv_util::mpsc::Receiver<PeerMsg<RocksEngine>>>,
        leader_resolver: Option<LeadershipResolver>,
    }

    impl TestEndpointSuite {
        // It's important to matain raft receivers in `raft_rxs`, otherwise all cases
        // need to drop `endpoint` and `rx` in order manually.
        fn add_region(&mut self, region_id: u64, cap: usize) {
            let rx = self.cdc_handle.add_region(region_id, cap);
            self.raft_rxs.insert(region_id, rx);
            self.add_local_reader(region_id);
        }

        fn add_local_reader(&self, region_id: u64) {
            self.store_meta
                .lock()
                .unwrap()
                .readers
                .insert(region_id, ReadDelegate::mock(region_id));
        }

        fn fill_raft_rx(&self, region_id: u64) {
            let router = &self.cdc_handle;
            loop {
                match router.send_casual_msg(region_id, CasualMessage::ClearRegionSize) {
                    Ok(_) => continue,
                    Err(RaftStoreError::Transport(DiscardReason::Full)) => break,
                    _ => unreachable!(),
                }
            }
        }

        fn raft_rx(&self, region_id: u64) -> &tikv_util::mpsc::Receiver<PeerMsg<RocksEngine>> {
            self.raft_rxs.get(&region_id).unwrap()
        }

        fn recv_task_timely(&mut self) -> Task {
            self.task_rx
                .recv_timeout(Duration::from_millis(500))
                .unwrap()
                .unwrap()
        }

        fn recv_no_task_timely(&mut self) {
            let _ = self
                .task_rx
                .recv_timeout(Duration::from_millis(100))
                .unwrap_err();
        }

        fn init_downstream(
            &self,
            region_id: u64,
            downstream_id: DownstreamId,
            downstream_state: Arc<AtomicCell<DownstreamState>>,
        ) {
            let delegate = self.capture_regions.get(&region_id).unwrap();
            let observe_id = delegate.handle.id;
            let (tx, rx) = mpsc::sync_channel::<()>(1);
            let _ = delegate.sched.unbounded_send(DelegateTask::InitDownstream {
                observe_id,
                downstream_id,
                build_resolver: Default::default(),
                cb: Box::new(move || drop(tx)),
            });
            match rx.recv_timeout(Duration::from_millis(100)) {
                Err(RecvTimeoutError::Timeout) => panic!("should receive an disconnected"),
                Err(RecvTimeoutError::Disconnected) => {}
                Ok(_) => unreachable!(),
            }
            post_init_downstream(&downstream_state);
        }

        fn finish_scan_locks(
            &self,
            region_id: u64,
            region: Region,
            locks: BTreeMap<Key, MiniLock>,
        ) {
            let delegate = self.capture_regions.get(&region_id).unwrap();
            let observe_id = delegate.handle.id;
            let _ = delegate
                .sched
                .unbounded_send(DelegateTask::FinishScanLocks {
                    observe_id,
                    region,
                    locks,
                });
        }

        fn stop_downstream(&self, region_id: u64, downstream_id: DownstreamId, err: Option<Error>) {
            let delegate = self.capture_regions.get(&region_id).unwrap();
            let _ = delegate
                .sched
                .unbounded_send(DelegateTask::StopDownstream { err, downstream_id });
        }

        fn stop_delegate(&self, region_id: u64) {
            let delegate = self.capture_regions.get(&region_id).unwrap();
            let observe_id = delegate.handle.id;
            let _ = delegate.sched.unbounded_send(DelegateTask::Stop {
                observe_id,
                err: None,
            });
        }
    }

    impl Deref for TestEndpointSuite {
        type Target = Endpoint<CdcRaftRouter<MockRaftStoreRouter>, RocksEngine, StoreMeta>;
        fn deref(&self) -> &Self::Target {
            &self.endpoint
        }
    }

    impl DerefMut for TestEndpointSuite {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.endpoint
        }
    }

    fn mock_endpoint(
        cfg: &CdcConfig,
        engine: Option<RocksEngine>,
        api_version: ApiVersion,
    ) -> TestEndpointSuite {
        mock_endpoint_with_ts_provider(cfg, engine, api_version, None)
    }

    fn mock_endpoint_with_ts_provider(
        cfg: &CdcConfig,
        engine: Option<RocksEngine>,
        api_version: ApiVersion,
        causal_ts_provider: Option<Arc<CausalTsProviderImpl>>,
    ) -> TestEndpointSuite {
        let (task_sched, task_rx) = dummy_scheduler();
        let cdc_handle = CdcRaftRouter(MockRaftStoreRouter::new());
        let mut store_meta = StoreMeta::new(0);
        store_meta.store_id = Some(1);
        let pd_client = Arc::new(TestPdClient::new(0, true));
        let env = Arc::new(Environment::new(1));
        let security_mgr = Arc::new(SecurityManager::default());
        let leader_resolver = LeadershipResolver::new(
            1,
            pd_client.clone(),
            env.clone(),
            security_mgr.clone(),
            store_meta.region_read_progress.clone(),
            Duration::from_secs(60),
        );

        let memory_quota = Arc::new(MemoryQuota::new(usize::MAX));
        let ep = Endpoint::new(
            DEFAULT_CLUSTER_ID,
            cfg,
            &ResolvedTsConfig::default(),
            false,
            api_version,
            pd_client,
            task_sched.clone(),
            cdc_handle.clone(),
            LocalTablets::Singleton(engine.unwrap_or_else(|| {
                TestEngineBuilder::new()
                    .build_without_cache()
                    .unwrap()
                    .kv_engine()
                    .unwrap()
            })),
            CdcObserver::new(memory_quota.clone()),
            Arc::new(StdMutex::new(store_meta)),
            ConcurrencyManager::new(1.into()),
            env,
            security_mgr,
            memory_quota,
            causal_ts_provider,
        );

        TestEndpointSuite {
            endpoint: ep,
            cdc_handle,
            task_rx,
            raft_rxs: HashMap::default(),
            leader_resolver: Some(leader_resolver),
        }
    }

    #[test]
    fn test_api_version_check() {
        let mut cfg = CdcConfig::default();
        // To make the case more stable.
        cfg.min_ts_interval = ReadableDuration(Duration::from_secs(1));

        let mut suite = mock_endpoint(&cfg, None, ApiVersion::V1);
        suite.add_region(1, 100);
        let quota = Arc::new(MemoryQuota::new(usize::MAX));
        let conn_id = ConnId::new();
        let (tx, mut rx) = channel::channel(conn_id, quota);

        let conn = Conn::new(
            conn_id,
            tx.clone(),
            FeatureGate::batch_resolved_ts(),
            vec![],
        );
        suite.run(Task::OpenConn { conn });

        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        req.set_kv_api(ChangeDataRequestKvApi::TiDb);
        let region_epoch = req.get_region_epoch().clone();

        // Compatibility error.
        let downstream = Downstream::new(
            RequestId(1),
            conn_id,
            "".to_string(),
            region_epoch.clone(),
            ChangeDataRequestKvApi::RawKv,
            false,
            ObservedRange::default(),
            DownstreamSink::new(1, RequestId(1), tx.clone()),
        );
        req.set_kv_api(ChangeDataRequestKvApi::RawKv);
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        let events = recv_events_timely(&mut rx);
        assert_eq!(events.len(), 1);
        assert!(events[0].has_error());
        assert!(events[0].get_error().has_compatibility());
        suite.recv_no_task_timely();

        // Compatibility error.
        let downstream = Downstream::new(
            RequestId(2),
            conn_id,
            "".to_string(),
            region_epoch.clone(),
            ChangeDataRequestKvApi::TxnKv,
            false,
            ObservedRange::default(),
            DownstreamSink::new(1, RequestId(2), tx.clone()),
        );
        req.set_kv_api(ChangeDataRequestKvApi::TxnKv);
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        let events = recv_events_timely(&mut rx);
        assert_eq!(events.len(), 1);
        assert!(events[0].has_error());
        assert!(events[0].get_error().has_compatibility());
        suite.recv_no_task_timely();

        suite.api_version = ApiVersion::V2;
        // Compatibility error.
        let downstream = Downstream::new(
            RequestId(3),
            conn_id,
            "".to_string(),
            region_epoch,
            ChangeDataRequestKvApi::TxnKv,
            false,
            ObservedRange::default(),
            DownstreamSink::new(1, RequestId(3), tx.clone()),
        );
        req.set_kv_api(ChangeDataRequestKvApi::TxnKv);
        suite.run(Task::Register {
            request: req,
            downstream,
        });
        let events = recv_events_timely(&mut rx);
        assert_eq!(events.len(), 1);
        assert!(events[0].has_error());
        assert!(events[0].get_error().has_compatibility());
        suite.recv_no_task_timely();
    }

    #[test]
    fn test_change_endpoint_cfg() {
        let cfg = CdcConfig::default();
        let mut suite = mock_endpoint(&cfg, None, ApiVersion::V2);
        let ep = &mut suite.endpoint;

        // Modify min_ts_interval and hibernate_regions_compatible.
        {
            let mut updated_cfg = cfg.clone();
            {
                // Update it to 0, this will be an invalid change and will be lost.
                updated_cfg.min_ts_interval = ReadableDuration::secs(0);
            }
            let diff = cfg.diff(&updated_cfg);
            ep.run(Task::ChangeConfig(diff));
            assert_eq!(ep.config.min_ts_interval, ReadableDuration::secs(1));
            assert_eq!(ep.config.hibernate_regions_compatible, true);

            {
                // update fields.
                updated_cfg.min_ts_interval = ReadableDuration::secs(100);
                updated_cfg.hibernate_regions_compatible = false
            }
            let diff = cfg.diff(&updated_cfg);
            ep.run(Task::ChangeConfig(diff));
            assert_eq!(ep.config.min_ts_interval, ReadableDuration::secs(100));
            assert_eq!(ep.config.hibernate_regions_compatible, false);
        }

        // Modify old_value_cache_memory_quota.
        {
            let mut updated_cfg = cfg.clone();
            {
                updated_cfg.old_value_cache_memory_quota = ReadableSize::mb(1024);
            }
            let diff = cfg.diff(&updated_cfg);

            assert_eq!(
                ep.config.old_value_cache_memory_quota,
                ReadableSize::mb(512)
            );
            assert_eq!(
                block_on(ep.old_value_cache.lock()).capacity(),
                ReadableSize::mb(512).0 as usize
            );
            ep.run(Task::ChangeConfig(diff));
            assert_eq!(
                ep.config.old_value_cache_memory_quota,
                ReadableSize::mb(1024)
            );
            assert_eq!(
                block_on(ep.old_value_cache.lock()).capacity(),
                ReadableSize::mb(1024).0 as usize
            );
        }

        // Modify incremental_scan_concurrency.
        {
            let mut updated_cfg = cfg.clone();
            {
                // Update it to be smaller than incremental_scan_threads,
                // which will be an invalid change and will modified to
                // incremental_scan_threads.
                updated_cfg.incremental_scan_concurrency = 2;
            }
            let diff = cfg.diff(&updated_cfg);
            ep.run(Task::ChangeConfig(diff));
            assert_eq!(ep.config.incremental_scan_concurrency, 4);
            assert_eq!(ep.scan_concurrency_semaphore.available_permits(), 4);

            {
                // Correct update.
                updated_cfg.incremental_scan_concurrency = 8;
            }
            let diff = cfg.diff(&updated_cfg);
            ep.run(Task::ChangeConfig(diff));
            assert_eq!(ep.config.incremental_scan_concurrency, 8);
            assert_eq!(ep.scan_concurrency_semaphore.available_permits(), 8);
        }

        // Modify sink_memory_quota.
        {
            let mut updated_cfg = cfg.clone();
            {
                updated_cfg.sink_memory_quota = ReadableSize::mb(1024);
            }
            let diff = cfg.diff(&updated_cfg);

            assert_eq!(ep.sink_memory_quota.capacity(), isize::MAX as usize);
            ep.run(Task::ChangeConfig(diff));
            assert_eq!(ep.config.sink_memory_quota, ReadableSize::mb(1024));
            assert_eq!(
                ep.sink_memory_quota.capacity(),
                ReadableSize::mb(1024).0 as usize
            );
        }

        // Modify incremental_scan_speed_limit.
        {
            let mut updated_cfg = cfg.clone();
            {
                updated_cfg.incremental_scan_speed_limit = ReadableSize::mb(1024);
            }
            let diff = cfg.diff(&updated_cfg);

            assert_eq!(
                ep.config.incremental_scan_speed_limit,
                ReadableSize::mb(128)
            );
            assert!(
                (ep.scan_speed_limiter.speed_limit() - ReadableSize::mb(128).0 as f64).abs()
                    < f64::EPSILON
            );
            ep.run(Task::ChangeConfig(diff));
            assert_eq!(
                ep.config.incremental_scan_speed_limit,
                ReadableSize::mb(1024)
            );
            assert!(
                (ep.scan_speed_limiter.speed_limit() - ReadableSize::mb(1024).0 as f64).abs()
                    < f64::EPSILON
            );
        }

        // Modify incremental_fetch_speed_limit.
        {
            let mut updated_cfg = cfg.clone();
            {
                updated_cfg.incremental_fetch_speed_limit = ReadableSize::mb(2048);
            }
            let diff = cfg.diff(&updated_cfg);

            assert_eq!(
                ep.config.incremental_fetch_speed_limit,
                ReadableSize::mb(512)
            );
            assert!(
                (ep.fetch_speed_limiter.speed_limit() - ReadableSize::mb(512).0 as f64).abs()
                    < f64::EPSILON
            );
            ep.run(Task::ChangeConfig(diff));
            assert_eq!(
                ep.config.incremental_fetch_speed_limit,
                ReadableSize::mb(2048)
            );
            assert!(
                (ep.fetch_speed_limiter.speed_limit() - ReadableSize::mb(2048).0 as f64).abs()
                    < f64::EPSILON
            );
        }
    }

    #[test]
    fn test_raftstore_is_busy() {
        let conn_id = ConnId::new();
        let quota = Arc::new(MemoryQuota::new(usize::MAX));
        let (tx, _rx) = channel::channel(conn_id, quota);
        let mut suite = mock_endpoint(&CdcConfig::default(), None, ApiVersion::V1);

        // Fill the channel.
        suite.add_region(1 /* region id */, 1 /* cap */);
        suite.fill_raft_rx(1);

        let conn = Conn::new(conn_id, tx.clone(), Version::new(0, 0, 0), vec![]);
        let conn_id = conn.id;
        suite.run(Task::OpenConn { conn });

        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new(
            RequestId(0),
            conn_id,
            "".to_string(),
            region_epoch,
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
            DownstreamSink::new(1, RequestId(0), tx.clone()),
        );
        suite.run(Task::Register {
            request: req,
            downstream,
        });
        assert_eq!(suite.capture_regions.len(), 1);
        for _ in 0..5 {
            suite.recv_no_task_timely();
        }
    }

    #[test]
    fn test_register() {
        let cfg = CdcConfig {
            min_ts_interval: ReadableDuration(Duration::from_secs(60)),
            ..Default::default()
        };
        let mut suite = mock_endpoint(&cfg, None, ApiVersion::V1);
        suite.add_region(1, 100);
        let quota = Arc::new(MemoryQuota::new(usize::MAX));
        let conn_id = ConnId::new();
        let (tx, mut rx) = channel::channel(conn_id, quota);

        let conn = Conn::new(conn_id, tx.clone(), Version::new(4, 0, 8), vec![]);
        suite.run(Task::OpenConn { conn });

        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        req.set_request_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new(
            RequestId(1),
            conn_id,
            "".to_string(),
            region_epoch.clone(),
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
            DownstreamSink::new(1, RequestId(1), tx.clone()),
        );
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        assert_eq!(suite.capture_regions.len(), 1);

        // duplicate request error.
        req.set_request_id(1);
        let downstream = Downstream::new(
            RequestId(1),
            conn_id,
            "".to_string(),
            region_epoch,
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
            DownstreamSink::new(1, RequestId(1), tx.clone()),
        );
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        assert_eq!(suite.capture_regions.len(), 1);

        let events: Vec<_> = recv_events_timely(&mut rx);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].region_id, 1);
        assert_eq!(events[0].request_id, 1);
        assert!(events[0].has_error());
        assert!(events[0].get_error().has_duplicate_request());

        // The first scan task of a region is initiated in register, and when it
        // fails, it should send a deregister region task, otherwise the region
        // delegate does not have resolver.
        //
        // Test non-exist region in raft router.
        let mut req = ChangeDataRequest::default();
        req.set_region_id(100);
        req.set_request_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new(
            RequestId(1),
            conn_id,
            "".to_string(),
            region_epoch.clone(),
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
            DownstreamSink::new(1, RequestId(1), tx.clone()),
        );
        suite.add_local_reader(100);
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        // Region 100 is inserted into capture_regions.
        assert_eq!(suite.capture_regions.len(), 2);
        match suite.recv_task_timely() {
            Task::Deregister(Deregister::Downstream {
                request_id,
                region_id,
                ..
            }) => {
                assert_eq!(request_id.0, 1);
                assert_eq!(region_id, 100);
            }
            other => panic!("unexpected task {:?}", other),
        }
        match suite.recv_task_timely() {
            Task::Deregister(Deregister::Delegate { region_id, .. }) => {
                assert_eq!(region_id, 100);
            }
            other => panic!("unexpected task {:?}", other),
        }

        // Test errors on CaptureChange message.
        req.set_region_id(101);
        req.set_request_id(1);
        suite.add_region(101, 100);
        let downstream = Downstream::new(
            RequestId(1),
            conn_id,
            "".to_string(),
            region_epoch,
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
            DownstreamSink::new(1, RequestId(1), tx.clone()),
        );
        suite.run(Task::Register {
            request: req,
            downstream,
        });
        // Drop CaptureChange message, it should cause scan task failure.
        let timeout = Duration::from_millis(100);
        let _ = suite.raft_rx(101).recv_timeout(timeout).unwrap();
        assert_eq!(suite.capture_regions.len(), 3);
        match suite.recv_task_timely() {
            Task::Deregister(Deregister::Downstream { region_id, .. }) => {
                assert_eq!(region_id, 101);
            }
            other => panic!("unexpected task {:?}", other),
        }
        match suite.recv_task_timely() {
            Task::Deregister(Deregister::Delegate { region_id, .. }) => {
                assert_eq!(region_id, 101);
            }
            other => panic!("unexpected task {:?}", other),
        }
    }

    #[test]
    fn test_too_many_scan_tasks() {
        let cfg = CdcConfig {
            min_ts_interval: ReadableDuration(Duration::from_secs(60)),
            incremental_scan_threads: 1,
            incremental_scan_concurrency: 1,
            incremental_scan_concurrency_limit: 1,
            ..Default::default()
        };
        let mut suite = mock_endpoint(&cfg, None, ApiVersion::V1);

        // Pause scan task runtime.
        let (pause_tx, pause_rx) = std::sync::mpsc::channel::<()>();
        suite.scan_workers.spawn(async move {
            let _ = pause_rx.recv();
        });

        suite.add_region(1, 100);

        let conn_id = ConnId::new();
        let quota = Arc::new(MemoryQuota::new(usize::MAX));
        let (tx, mut rx) = channel::channel(ConnId::new(), quota);

        let conn = Conn::new(conn_id, tx.clone(), Version::new(4, 0, 8), vec![]);
        suite.run(Task::OpenConn { conn });

        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        req.set_request_id(1);
        let downstream = Downstream::new(
            RequestId(1),
            conn_id,
            "".to_string(),
            Default::default(),
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
            DownstreamSink::new(1, RequestId(1), tx.clone()),
        );
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        assert_eq!(suite.capture_regions.len(), 1);

        // Test too many scan tasks error.
        req.set_request_id(2);
        let downstream = Downstream::new(
            RequestId(2),
            conn_id,
            "".to_string(),
            Default::default(),
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
            DownstreamSink::new(1, RequestId(2), tx),
        );
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });

        let events = recv_events_timely(&mut rx);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].region_id, 1);
        assert_eq!(events[0].request_id, 2);
        assert!(events[0].has_error());
        assert!(events[0].get_error().has_server_is_busy());

        drop(pause_tx);
    }

    #[test]
    fn test_raw_causal_min_ts() {
        let cfg = CdcConfig {
            min_ts_interval: ReadableDuration(Duration::from_millis(100)),
            ..Default::default()
        };
        let ts_provider: Arc<CausalTsProviderImpl> =
            Arc::new(causal_ts::tests::TestProvider::default().into());
        let start_ts = block_on(ts_provider.async_get_ts()).unwrap();
        let mut suite =
            mock_endpoint_with_ts_provider(&cfg, None, ApiVersion::V2, Some(ts_provider.clone()));
        let leader_resolver = suite.leader_resolver.take().unwrap();
        suite.run(Task::RegisterMinTsEvent { leader_resolver });
        suite.recv_task_timely();
        let end_ts = block_on(ts_provider.async_get_ts()).unwrap();
        assert!(end_ts.into_inner() > start_ts.next().into_inner()); // may trigger more than once.
    }

    #[test]
    fn test_feature_gate() {
        let cfg = CdcConfig {
            min_ts_interval: ReadableDuration(Duration::from_secs(60)),
            ..Default::default()
        };
        let mut suite = mock_endpoint(&cfg, None, ApiVersion::V1);
        suite.add_region(1, 100);

        let conn_id = ConnId::new();
        let quota = Arc::new(MemoryQuota::new(usize::MAX));
        let (tx, mut rx) = channel::channel(conn_id, quota);
        let conn = Conn::new(conn_id, tx.clone(), Version::new(4, 0, 8), vec![]);
        suite.run(Task::OpenConn { conn });

        let mut region = Region::default();
        region.set_id(1);

        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        let downstream = Downstream::new(
            RequestId(0),
            conn_id,
            "".to_string(),
            Default::default(),
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
            DownstreamSink::new(1, RequestId(0), tx.clone()),
        );
        let downstream_id = downstream.id;
        let downstream_state = downstream.get_state();
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        suite.init_downstream(1, downstream_id, downstream_state);
        suite.finish_scan_locks(1, Default::default(), Default::default());
        suite.run(Task::MinTs {
            regions: vec![1],
            min_ts: TimeStamp::from(1),
            current_ts: TimeStamp::zero(),
        });
        let task = suite.recv_task_timely();
        suite.run(task);

        let r = recv_resolved_ts_timely(&mut rx);
        assert_eq!(r.regions, &[1]);
        assert_eq!(r.ts, 1);

        // Register region 2 to the conn.
        req.set_region_id(2);
        let downstream = Downstream::new(
            RequestId(0),
            conn_id,
            "".to_string(),
            Default::default(),
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
            DownstreamSink::new(1, RequestId(0), tx.clone()),
        );
        let downstream_id = downstream.id;
        let downstream_state = downstream.get_state();
        suite.add_region(2, 100);
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        suite.init_downstream(2, downstream_id, downstream_state);
        suite.finish_scan_locks(2, Default::default(), Default::default());
        suite.run(Task::MinTs {
            regions: vec![1, 2],
            min_ts: TimeStamp::from(2),
            current_ts: TimeStamp::zero(),
        });
        let task = suite.recv_task_timely();
        suite.run(task);

        let mut r = recv_resolved_ts_timely(&mut rx);
        r.regions.as_mut_slice().sort_unstable();
        assert_eq!(r.regions, &[1, 2]);
        assert_eq!(r.ts, 2);

        // Register region 3 to another conn which is not support batch resolved ts.
        let conn_id = ConnId::new();
        let quota = Arc::new(MemoryQuota::new(usize::MAX));
        let (tx1, mut rx1) = channel::channel(conn_id, quota);
        let conn = Conn::new(conn_id, tx1.clone(), Version::new(0, 0, 0), vec![]);
        suite.run(Task::OpenConn { conn });

        let mut region = Region::default();
        region.set_id(3);
        req.set_region_id(3);
        req.set_request_id(3);
        let downstream = Downstream::new(
            RequestId(3),
            conn_id,
            "".to_string(),
            Default::default(),
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
            DownstreamSink::new(3, RequestId(3), tx1.clone()),
        );
        let downstream_id = downstream.id;
        let downstream_state = downstream.get_state();
        suite.add_region(3, 100);
        suite.run(Task::Register {
            request: req,
            downstream,
        });
        suite.init_downstream(3, downstream_id, downstream_state);
        suite.finish_scan_locks(3, Default::default(), Default::default());
        suite.run(Task::MinTs {
            regions: vec![1, 2, 3],
            min_ts: TimeStamp::from(3),
            current_ts: TimeStamp::zero(),
        });
        let task = suite.recv_task_timely();
        suite.run(task);

        let mut r = recv_resolved_ts_timely(&mut rx);
        r.regions.as_mut_slice().sort_unstable();
        assert_eq!(r.regions, &[1, 2]);
        assert_eq!(r.ts, 3);

        let events = recv_events_timely(&mut rx1);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].region_id, 3);
        assert_eq!(events[0].request_id, 3);
        assert!(events[0].has_resolved_ts());
        assert_eq!(events[0].get_resolved_ts(), 3);
    }

    #[test]
    fn test_deregister() {
        let mut suite = mock_endpoint(&CdcConfig::default(), None, ApiVersion::V1);
        suite.add_region(1, 100);

        let conn_id = ConnId::new();
        let quota = Arc::new(MemoryQuota::new(usize::MAX));
        let (tx, mut rx) = channel::channel(conn_id, quota);

        let conn = Conn::new(conn_id, tx.clone(), Version::new(0, 0, 0), vec![]);
        suite.run(Task::OpenConn { conn });

        let mut err_header = ErrorHeader::default();
        err_header.set_not_leader(Default::default());

        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new(
            RequestId(0),
            conn_id,
            "".to_string(),
            region_epoch.clone(),
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
            DownstreamSink::new(1, RequestId(0), tx.clone()),
        );
        let downstream_id = downstream.id;
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        assert_eq!(suite.capture_regions.len(), 1);

        suite.stop_downstream(1, downstream_id, Some(Error::request(err_header.clone())));

        let events = recv_events_timely(&mut rx);
        assert_eq!(events.len(), 1);
        assert!(events[0].has_error());
        assert!(events[0].get_error().has_not_leader());

        let task = suite.recv_task_timely();
        suite.run(task);
        let task = suite.recv_task_timely();
        suite.run(task);
        assert_eq!(suite.capture_regions.len(), 0);

        let new_downstream = Downstream::new(
            RequestId(0),
            conn_id,
            "".to_string(),
            region_epoch.clone(),
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
            DownstreamSink::new(1, RequestId(0), tx.clone()),
        );
        let new_downstream_id = new_downstream.id;
        suite.run(Task::Register {
            request: req.clone(),
            downstream: new_downstream,
        });
        assert_eq!(suite.capture_regions.len(), 1);

        suite.stop_downstream(1, downstream_id, Some(Error::request(err_header.clone())));
        recv_timeout(&mut rx, Duration::from_millis(100)).unwrap_err();
        assert_eq!(suite.capture_regions.len(), 1);

        suite.stop_downstream(
            1,
            new_downstream_id,
            Some(Error::request(err_header.clone())),
        );

        let events = recv_events_timely(&mut rx);
        assert_eq!(events.len(), 1);
        assert!(events[0].has_error());
        assert!(events[0].get_error().has_not_leader());

        let task = suite.recv_task_timely();
        suite.run(task);
        let task = suite.recv_task_timely();
        suite.run(task);
        assert_eq!(suite.capture_regions.len(), 0);
    }

    #[test]
    fn test_broadcast_resolved_ts() {
        let cfg = CdcConfig {
            min_ts_interval: ReadableDuration(Duration::from_secs(60)),
            ..Default::default()
        };
        let mut suite = mock_endpoint(&cfg, None, ApiVersion::V1);

        // Open two connections a and b, registers region 1, 2 to conn a and
        // region 3 to conn b.
        let mut conn_rxs = vec![];
        let quota = Arc::new(MemoryQuota::new(usize::MAX));
        for region_ids in [vec![1, 2], vec![3]] {
            let conn_id = ConnId::new();
            let (tx, rx) = channel::channel(conn_id, quota.clone());
            conn_rxs.push(rx);
            let conn = Conn::new(conn_id, tx.clone(), Version::new(4, 0, 8), vec![]);
            suite.run(Task::OpenConn { conn });

            for region_id in region_ids {
                suite.add_region(region_id, 100);
                let mut req = ChangeDataRequest::default();
                req.set_region_id(region_id);
                let downstream = Downstream::new(
                    RequestId(0),
                    conn_id,
                    "".to_string(),
                    Default::default(),
                    ChangeDataRequestKvApi::TiDb,
                    false,
                    ObservedRange::default(),
                    DownstreamSink::new(region_id, RequestId(0), tx.clone()),
                );
                let downstream_id = downstream.id;
                let downstream_state = downstream.get_state();
                suite.run(Task::Register {
                    request: req,
                    downstream,
                });
                suite.init_downstream(region_id, downstream_id, downstream_state);
                suite.finish_scan_locks(region_id, Default::default(), Default::default());
            }
        }

        let assert_batch_resolved_ts = |rx: &mut Drain, regions: Vec<u64>, resolved_ts: u64| {
            let mut r = recv_resolved_ts_timely(rx);
            r.regions.as_mut_slice().sort_unstable();
            assert_eq!(r.regions, regions);
            assert_eq!(r.ts, resolved_ts);
        };

        suite.run(Task::MinTs {
            regions: vec![1],
            min_ts: TimeStamp::from(1),
            current_ts: TimeStamp::zero(),
        });
        let task = suite.recv_task_timely();
        suite.run(task);

        // conn a must receive a resolved ts that only contains region 1.
        assert_batch_resolved_ts(&mut conn_rxs[0], vec![1], 1);
        // conn b must not receive any messages.
        recv_timeout(&mut conn_rxs[1], Duration::from_millis(100)).unwrap_err();

        suite.run(Task::MinTs {
            regions: vec![1, 2],
            min_ts: TimeStamp::from(2),
            current_ts: TimeStamp::zero(),
        });
        let task = suite.recv_task_timely();
        suite.run(task);

        // conn a must receive a resolved ts that contains region 1 and region 2.
        assert_batch_resolved_ts(conn_rxs.get_mut(0).unwrap(), vec![1, 2], 2);
        // conn b must not receive any messages.
        recv_timeout(&mut conn_rxs[1], Duration::from_millis(100)).unwrap_err();

        suite.run(Task::MinTs {
            regions: vec![1, 2, 3],
            min_ts: TimeStamp::from(3),
            current_ts: TimeStamp::zero(),
        });
        let task = suite.recv_task_timely();
        suite.run(task);

        // conn a must receive a resolved ts that contains region 1 and region 2.
        assert_batch_resolved_ts(conn_rxs.get_mut(0).unwrap(), vec![1, 2], 3);
        // conn b must receive a resolved ts that contains region 3.
        assert_batch_resolved_ts(conn_rxs.get_mut(1).unwrap(), vec![3], 3);

        suite.run(Task::MinTs {
            regions: vec![1, 3],
            min_ts: TimeStamp::from(4),
            current_ts: TimeStamp::zero(),
        });
        let task = suite.recv_task_timely();
        suite.run(task);

        // conn a must receive a resolved ts that contains region 1 and region 2.
        assert_batch_resolved_ts(conn_rxs.get_mut(0).unwrap(), vec![1, 2], 3);
        // conn b must receive a resolved ts that contains region 3.
        assert_batch_resolved_ts(conn_rxs.get_mut(1).unwrap(), vec![3], 4);
    }

    // Suppose there are two Conn that capture the same region,
    // Region epoch = 2, Conn A with epoch = 2, Conn B with epoch = 1,
    // Conn A builds resolver successfully, but is disconnected before
    // scheduling resolver ready. Downstream in Conn A is unsubscribed.
    // When resolver ready is installed, downstream in Conn B is unsubscribed
    // too, because epoch not match.
    #[test]
    fn test_deregister_conn_then_delegate() {
        let mut suite = mock_endpoint(&CdcConfig::default(), None, ApiVersion::V1);
        suite.add_region(1, 100);
        let quota = Arc::new(MemoryQuota::new(usize::MAX));

        // Open conn a
        let conn_id_a = ConnId::new();
        let (tx1, _rx1) = channel::channel(conn_id_a, quota.clone());
        let conn_a = Conn::new(conn_id_a, tx1.clone(), Version::new(0, 0, 0), vec![]);
        suite.run(Task::OpenConn { conn: conn_a });

        // Open conn b
        let conn_id_b = ConnId::new();
        let (tx2, mut rx2) = channel::channel(conn_id_b, quota);
        let conn_b = Conn::new(conn_id_b, tx2.clone(), Version::new(0, 0, 0), vec![]);
        suite.run(Task::OpenConn { conn: conn_b });

        // Register region 1 (epoch 2) at conn a.
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        req.mut_region_epoch().set_version(2);
        let region_epoch_1 = req.get_region_epoch().clone();
        let downstream = Downstream::new(
            RequestId(0),
            conn_id_a,
            "".to_string(),
            region_epoch_1.clone(),
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
            DownstreamSink::new(1, RequestId(0), tx1.clone()),
        );
        let downstream1_id = downstream.id;
        let downstream1_state = downstream.get_state();
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        suite.init_downstream(1, downstream1_id, downstream1_state);

        // Register region 1 (epoch 1) at conn b.
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        req.mut_region_epoch().set_version(1);
        let region_epoch_2 = req.get_region_epoch().clone();
        let downstream = Downstream::new(
            RequestId(0),
            conn_id_b,
            "".to_string(),
            region_epoch_2,
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
            DownstreamSink::new(1, RequestId(0), tx2.clone()),
        );
        let downstream2_id = downstream.id;
        let downstream2_state = downstream.get_state();
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        suite.init_downstream(1, downstream2_id, downstream2_state);

        // Deregister conn a.
        suite.run(Task::Deregister(Deregister::Conn(conn_id_a)));
        assert_eq!(suite.capture_regions.len(), 1);

        // Schedule resolver ready (resolver is built by conn a).
        let mut region = Region::default();
        region.set_region_epoch(region_epoch_1);
        suite.finish_scan_locks(1, region, Default::default());

        // Must receive 2 deregister for conn_1 and conn_2.
        let task = suite.recv_task_timely();
        assert!(
            matches!(task, Task::Deregister(Deregister::Downstream { conn_id, .. }) if conn_id == conn_id_a)
        );
        suite.run(task);
        let task = suite.recv_task_timely();
        assert!(
            matches!(task, Task::Deregister(Deregister::Downstream { conn_id, .. }) if conn_id == conn_id_b)
        );
        suite.run(task);

        // The region will be deregistered finally.
        let task = suite.recv_task_timely();
        suite.run(task);
        assert_eq!(suite.capture_regions.len(), 0);

        let events = recv_events_timely(&mut rx2);
        assert_eq!(events.len(), 1);
        assert!(events[0].has_error());
        assert!(events[0].get_error().has_epoch_not_match());
    }

    #[test]
    fn test_resolved_region_heap() {
        let mut heap = ResolvedRegionHeap {
            heap: BinaryHeap::new(),
        };
        heap.push(5, 5.into());
        heap.push(4, 4.into());
        heap.push(6, 6.into());
        heap.push(3, 3.into());

        let (ts, regions) = heap.pop(0);
        assert_eq!(ts, TimeStamp::max());
        assert!(regions.is_empty());

        let (ts, regions) = heap.pop(2);
        assert_eq!(ts, 3.into());
        assert_eq!(regions.len(), 2);
        assert!(regions.contains(&3));
        assert!(regions.contains(&4));

        // Pop outliers more then it has.
        let (ts, regions) = heap.pop(3);
        assert_eq!(ts, 5.into());
        assert_eq!(regions.len(), 2);
        assert!(regions.contains(&5));
        assert!(regions.contains(&6));

        let mut heap1 = ResolvedRegionHeap {
            heap: BinaryHeap::new(),
        };
        heap1.push(5, 5.into());
        heap1.push(4, 4.into());
        heap1.push(6, 6.into());
        heap1.push(3, 3.into());

        let (ts, regions) = heap1.pop(1);
        assert_eq!(ts, 3.into());
        assert_eq!(regions.len(), 1);
        assert!(regions.contains(&3));
    }

    #[test]
    fn test_on_min_ts() {
        let cfg = CdcConfig {
            // Disable automatic advance resolved ts during test.
            min_ts_interval: ReadableDuration(Duration::from_secs(1000)),
            ..Default::default()
        };
        let mut suite = mock_endpoint(&cfg, None, ApiVersion::V1);

        let conn_id = ConnId::new();
        let quota = Arc::new(MemoryQuota::new(usize::MAX));
        let (tx, mut rx) = channel::channel(conn_id, quota);
        let conn = Conn::new(conn_id, tx.clone(), Version::new(4, 0, 8), vec![]);
        suite.run(Task::OpenConn { conn });

        let mut regions = vec![];
        for id in 1..4097 {
            regions.push(id);
            suite.add_region(id, 100);

            let mut req = ChangeDataRequest::default();
            req.set_region_id(id);
            let downstream = Downstream::new(
                RequestId(0),
                conn_id,
                "".to_string(),
                Default::default(),
                ChangeDataRequestKvApi::TiDb,
                false,
                ObservedRange::default(),
                DownstreamSink::new(id, RequestId(0), tx.clone()),
            );
            let downstream_id = downstream.id;
            let downstream_state = downstream.get_state();
            suite.run(Task::Register {
                request: req.clone(),
                downstream,
            });
            suite.init_downstream(id, downstream_id, downstream_state);

            let mut locks = BTreeMap::<Key, MiniLock>::default();
            locks.insert(
                Key::from_encoded(vec![]),
                MiniLock::from_ts(TimeStamp::compose(0, id)),
            );
            suite.finish_scan_locks(id, Default::default(), locks);
        }
        suite.recv_no_task_timely();

        suite.run(Task::MinTs {
            regions,
            min_ts: TimeStamp::compose(0, 4096),
            current_ts: TimeStamp::compose(0, 4096),
        });
        let task = suite.recv_task_timely();
        suite.run(task);

        // There should be at least 3 resolved ts events.
        let mut last_resolved_ts = 0;
        let mut last_batch_count = 0;
        for _ in 0..3 {
            let r = recv_resolved_ts_timely(&mut rx);
            assert!(last_resolved_ts < r.ts);
            assert!(last_batch_count < r.regions.len());
            last_resolved_ts = r.ts;
            last_batch_count = r.regions.len();
        }
    }

    #[test]
    fn test_register_deregister_with_multiplexing() {
        let cfg = CdcConfig {
            min_ts_interval: ReadableDuration(Duration::from_secs(60)),
            ..Default::default()
        };
        let mut suite = mock_endpoint(&cfg, None, ApiVersion::V1);
        suite.add_region(1, 100);

        let conn_id = ConnId::new();
        let quota = Arc::new(MemoryQuota::new(usize::MAX));
        let (tx, mut rx) = channel::channel(conn_id, quota);

        let conn = Conn::new(conn_id, tx.clone(), Version::new(4, 0, 8), vec![]);
        suite.run(Task::OpenConn { conn });
        let mut downstream_ids = Vec::with_capacity(16);

        let mut req = ChangeDataRequest::default();

        req.set_region_id(1);
        req.set_request_id(1);
        let downstream = Downstream::new(
            RequestId(1),
            conn_id,
            "".to_string(),
            Default::default(),
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
            DownstreamSink::new(1, RequestId(1), tx.clone()),
        );
        downstream_ids.push(downstream.id);
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        assert_eq!(suite.connections[&conn_id].downstreams_count(), 1);

        // Subscribe one region with a different request_id is allowed.
        req.set_request_id(2);
        let downstream = Downstream::new(
            RequestId(2),
            conn_id,
            "".to_string(),
            Default::default(),
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
            DownstreamSink::new(1, RequestId(2), tx.clone()),
        );
        downstream_ids.push(downstream.id);
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        assert_eq!(suite.connections[&conn_id].downstreams_count(), 2);

        // Subscribe one region with a same request_id is not allowed.
        req.set_request_id(2);
        let downstream = Downstream::new(
            RequestId(2),
            conn_id,
            "".to_string(),
            Default::default(),
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
            DownstreamSink::new(1, RequestId(2), tx.clone()),
        );
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        assert_eq!(suite.connections[&conn_id].downstreams_count(), 2);

        let events = recv_events_timely(&mut rx);
        assert_eq!(events.len(), 1);
        assert!(events[0].has_error());
        assert!(events[0].get_error().has_duplicate_request());

        // Deregister an unexist downstream.
        suite.stop_downstream(1, DownstreamId::new(), None);
        suite.recv_no_task_timely();
        assert_eq!(suite.connections[&conn_id].downstreams_count(), 2);

        // Deregister an unexist delegate.
        suite.run(Task::Deregister(Deregister::Delegate {
            region_id: 1,
            observe_id: ObserveId::new(),
        }));
        assert_eq!(suite.connections[&conn_id].downstreams_count(), 2);

        // Deregister an exist downstream.
        suite.stop_downstream(1, downstream_ids.swap_remove(0), None);
        let task = suite.recv_task_timely();
        suite.run(task);
        assert_eq!(suite.connections[&conn_id].downstreams_count(), 1);

        // Subscribe one region with a different request_id is allowed.
        req.set_request_id(1);
        let downstream = Downstream::new(
            RequestId(1),
            conn_id,
            "".to_string(),
            Default::default(),
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
            DownstreamSink::new(1, RequestId(1), tx.clone()),
        );
        downstream_ids.push(downstream.id);
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        assert_eq!(suite.connections[&conn_id].downstreams_count(), 2);

        // Deregister an exist delegate.
        suite.stop_delegate(1);
        for _ in 0..downstream_ids.len() {
            let task = suite.recv_task_timely();
            suite.run(task);
        }
        assert_eq!(suite.connections[&conn_id].downstreams_count(), 0);
        let task = suite.recv_task_timely();
        suite.run(task);
        assert_eq!(suite.capture_regions.len(), 0);

        // Resubscribe the region.
        for i in 1..=2 {
            req.set_request_id(i as _);
            let downstream = Downstream::new(
                RequestId(i),
                conn_id,
                "".to_string(),
                Default::default(),
                ChangeDataRequestKvApi::TiDb,
                false,
                ObservedRange::default(),
                DownstreamSink::new(1, RequestId(i), tx.clone()),
            );
            suite.run(Task::Register {
                request: req.clone(),
                downstream,
            });
            assert_eq!(suite.connections[&conn_id].downstreams_count(), i as usize);
        }

        // Deregister the request.
        suite.run(Task::Deregister(Deregister::Request {
            conn_id,
            request_id: RequestId(1),
        }));
        let task = suite.recv_task_timely();
        suite.run(task);
        assert_eq!(suite.connections[&conn_id].downstreams_count(), 1);

        suite.run(Task::Deregister(Deregister::Request {
            conn_id,
            request_id: RequestId(2),
        }));
        let task = suite.recv_task_timely();
        suite.run(task);
        assert_eq!(suite.connections[&conn_id].downstreams_count(), 0);

        let task = suite.recv_task_timely();
        suite.run(task);
        assert_eq!(suite.capture_regions.len(), 0);

        // Resubscribe the region.
        suite.add_region(2, 100);
        for i in 1..=2 {
            req.set_request_id(1);
            req.set_region_id(i);
            let downstream = Downstream::new(
                RequestId(1),
                conn_id,
                "".to_string(),
                Default::default(),
                ChangeDataRequestKvApi::TiDb,
                false,
                ObservedRange::default(),
                DownstreamSink::new(1, RequestId(1), tx.clone()),
            );
            suite.run(Task::Register {
                request: req.clone(),
                downstream,
            });
            assert_eq!(suite.connections[&conn_id].downstreams_count(), i as usize);
        }

        // Deregister regions one by one in the request.
        suite.run(Task::Deregister(Deregister::Region {
            conn_id,
            request_id: RequestId(1),
            region_id: 1,
        }));
        let task = suite.recv_task_timely();
        suite.run(task);
        assert_eq!(suite.connections[&conn_id].downstreams_count(), 1);
        let task = suite.recv_task_timely();
        suite.run(task);
        assert_eq!(suite.capture_regions.len(), 1);

        suite.run(Task::Deregister(Deregister::Region {
            conn_id,
            request_id: RequestId(1),
            region_id: 2,
        }));
        let task = suite.recv_task_timely();
        suite.run(task);
        assert_eq!(suite.connections[&conn_id].downstreams_count(), 0);
        let task = suite.recv_task_timely();
        suite.run(task);
        assert_eq!(suite.capture_regions.len(), 0);
    }

    #[test]
    fn test_register_after_connection_deregistered() {
        let cfg = CdcConfig {
            min_ts_interval: ReadableDuration(Duration::from_secs(60)),
            ..Default::default()
        };
        let mut suite = mock_endpoint(&cfg, None, ApiVersion::V1);
        suite.add_region(1, 100);

        let conn_id = ConnId::new();
        let quota = Arc::new(MemoryQuota::new(usize::MAX));
        let (tx, _rx) = channel::channel(conn_id, quota);

        let conn = Conn::new(conn_id, tx.clone(), Version::new(0, 0, 0), vec![]);
        suite.run(Task::OpenConn { conn });

        suite.run(Task::Deregister(Deregister::Conn(conn_id)));

        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        req.set_request_id(1);
        let downstream = Downstream::new(
            RequestId(1),
            conn_id,
            "".to_string(),
            Default::default(),
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
            DownstreamSink::new(1, RequestId(1), tx.clone()),
        );
        suite.run(Task::Register {
            request: req,
            downstream,
        });
        assert!(suite.connections.is_empty());
    }
}
