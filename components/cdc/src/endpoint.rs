// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::{Ord, Ordering as CmpOrdering, PartialOrd, Reverse},
    collections::BinaryHeap,
    fmt,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use collections::{HashMap, HashMapEntry, HashSet};
use concurrency_manager::ConcurrencyManager;
use crossbeam::atomic::AtomicCell;
use engine_traits::KvEngine;
use fail::fail_point;
use futures::compat::Future01CompatExt;
use grpcio::Environment;
use kvproto::{
    cdcpb::{
        ChangeDataRequest, ChangeDataRequestKvApi, ClusterIdMismatch as ErrorClusterIdMismatch,
        Compatibility as ErrorCompatibility, DuplicateRequest as ErrorDuplicateRequest,
        Error as EventError, Event, Event_oneof_event, ResolvedTs,
    },
    kvrpcpb::ApiVersion,
    metapb::Region,
    tikvpb::TikvClient,
};
use online_config::{ConfigChange, OnlineConfig};
use pd_client::{Feature, PdClient};
use raftstore::{
    coprocessor::{CmdBatch, ObserveID},
    router::RaftStoreRouter,
    store::{
        fsm::{ChangeObserver, StoreMeta},
        msg::{Callback, SignificantMsg},
        RegionReadProgressRegistry,
    },
};
use resolved_ts::Resolver;
use security::SecurityManager;
use tikv::{config::CdcConfig, storage::Statistics};
use tikv_util::{
    debug, error, impl_display_as_debug, info,
    sys::thread::ThreadBuildWrapper,
    time::Limiter,
    timer::SteadyTimer,
    warn,
    worker::{Runnable, RunnableWithTimer, ScheduleError, Scheduler},
};
use tokio::{
    runtime::{Builder, Runtime},
    sync::{Mutex, Semaphore},
};
use txn_types::{TimeStamp, TxnExtra, TxnExtraScheduler};

use crate::{
    channel::{CdcEvent, MemoryQuota, SendError},
    delegate::{on_init_downstream, Delegate, Downstream, DownstreamID, DownstreamState},
    initializer::Initializer,
    metrics::*,
    old_value::{OldValueCache, OldValueCallback},
    service::{Conn, ConnID, FeatureGate},
    CdcObserver, Error,
};

const FEATURE_RESOLVED_TS_STORE: Feature = Feature::require(5, 0, 0);
const METRICS_FLUSH_INTERVAL: u64 = 10_000; // 10s
// 10 minutes, it's the default gc life time of TiDB
// and is long enough for most transactions.
const WARN_RESOLVED_TS_LAG_THRESHOLD: Duration = Duration::from_secs(600);
// Suppress repeat resolved ts lag warning.
const WARN_RESOLVED_TS_COUNT_THRESHOLD: usize = 10;

pub enum Deregister {
    Downstream {
        region_id: u64,
        downstream_id: DownstreamID,
        conn_id: ConnID,
        err: Option<Error>,
    },
    Delegate {
        region_id: u64,
        observe_id: ObserveID,
        err: Error,
    },
    Conn(ConnID),
}

impl_display_as_debug!(Deregister);

impl fmt::Debug for Deregister {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut de = f.debug_struct("Deregister");
        match self {
            Deregister::Downstream {
                ref region_id,
                ref downstream_id,
                ref conn_id,
                ref err,
            } => de
                .field("deregister", &"downstream")
                .field("region_id", region_id)
                .field("downstream_id", downstream_id)
                .field("conn_id", conn_id)
                .field("err", err)
                .finish(),
            Deregister::Delegate {
                ref region_id,
                ref observe_id,
                ref err,
            } => de
                .field("deregister", &"delegate")
                .field("region_id", region_id)
                .field("observe_id", observe_id)
                .field("err", err)
                .finish(),
            Deregister::Conn(ref conn_id) => de
                .field("deregister", &"conn")
                .field("conn_id", conn_id)
                .finish(),
        }
    }
}

type InitCallback = Box<dyn FnOnce() + Send>;

pub enum Validate {
    Region(u64, Box<dyn FnOnce(Option<&Delegate>) + Send>),
    OldValueCache(Box<dyn FnOnce(&OldValueCache) + Send>),
}

pub enum Task {
    Register {
        request: ChangeDataRequest,
        downstream: Downstream,
        conn_id: ConnID,
        version: semver::Version,
    },
    Deregister(Deregister),
    OpenConn {
        conn: Conn,
    },
    MultiBatch {
        multi: Vec<CmdBatch>,
        old_value_cb: OldValueCallback,
    },
    MinTS {
        regions: Vec<u64>,
        min_ts: TimeStamp,
    },
    ResolverReady {
        observe_id: ObserveID,
        region: Region,
        resolver: Resolver,
    },
    RegisterMinTsEvent,
    // The result of ChangeCmd should be returned from CDC Endpoint to ensure
    // the downstream switches to Normal after the previous commands was sunk.
    InitDownstream {
        region_id: u64,
        downstream_id: DownstreamID,
        downstream_state: Arc<AtomicCell<DownstreamState>>,
        // `incremental_scan_barrier` will be sent into `sink` to ensure all delta changes
        // are delivered to the downstream. And then incremental scan can start.
        sink: crate::channel::Sink,
        incremental_scan_barrier: CdcEvent,
        cb: InitCallback,
    },
    TxnExtra(TxnExtra),
    Validate(Validate),
    ChangeConfig(ConfigChange),
}

impl_display_as_debug!(Task);

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut de = f.debug_struct("CdcTask");
        match self {
            Task::Register {
                ref request,
                ref downstream,
                ref conn_id,
                ref version,
                ..
            } => de
                .field("type", &"register")
                .field("register request", request)
                .field("request", request)
                .field("id", &downstream.get_id())
                .field("conn_id", conn_id)
                .field("version", version)
                .finish(),
            Task::Deregister(deregister) => de
                .field("type", &"deregister")
                .field("deregister", deregister)
                .finish(),
            Task::OpenConn { ref conn } => de
                .field("type", &"open_conn")
                .field("conn_id", &conn.get_id())
                .finish(),
            Task::MultiBatch { multi, .. } => de
                .field("type", &"multi_batch")
                .field("multi_batch", &multi.len())
                .finish(),
            Task::MinTS { ref min_ts, .. } => {
                de.field("type", &"mit_ts").field("min_ts", min_ts).finish()
            }
            Task::ResolverReady {
                ref observe_id,
                ref region,
                ..
            } => de
                .field("type", &"resolver_ready")
                .field("observe_id", &observe_id)
                .field("region_id", &region.get_id())
                .finish(),
            Task::RegisterMinTsEvent => de.field("type", &"register_min_ts").finish(),
            Task::InitDownstream {
                ref region_id,
                ref downstream_id,
                ..
            } => de
                .field("type", &"init_downstream")
                .field("region_id", &region_id)
                .field("downstream", &downstream_id)
                .finish(),
            Task::TxnExtra(_) => de.field("type", &"txn_extra").finish(),
            Task::Validate(validate) => match validate {
                Validate::Region(region_id, _) => de.field("region_id", &region_id).finish(),
                Validate::OldValueCache(_) => de.finish(),
            },
            Task::ChangeConfig(change) => de
                .field("type", &"change_config")
                .field("change", change)
                .finish(),
        }
    }
}

#[derive(PartialEq, Eq)]
struct ResolvedRegion {
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

struct ResolvedRegionHeap {
    // BinaryHeap is max heap, so we reverse order to get a min heap.
    heap: BinaryHeap<Reverse<ResolvedRegion>>,
}

impl ResolvedRegionHeap {
    fn push(&mut self, region_id: u64, resolved_ts: TimeStamp) {
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

    fn to_hash_set(&self) -> (TimeStamp, HashSet<u64>) {
        let mut min_resolved_ts = TimeStamp::max();
        let mut regions = HashSet::with_capacity_and_hasher(self.heap.len(), Default::default());
        for resolved_region in &self.heap {
            regions.insert(resolved_region.0.region_id);
            if min_resolved_ts > resolved_region.0.resolved_ts {
                min_resolved_ts = resolved_region.0.resolved_ts;
            }
        }
        (min_resolved_ts, regions)
    }

    fn clear(&mut self) {
        self.heap.clear();
    }

    fn reset_and_shrink_to(&mut self, min_capacity: usize) {
        self.clear();
        self.heap.shrink_to(min_capacity);
    }
}

pub struct Endpoint<T, E> {
    cluster_id: u64,

    capture_regions: HashMap<u64, Delegate>,
    connections: HashMap<ConnID, Conn>,
    scheduler: Scheduler<Task>,
    raft_router: T,
    engine: E,
    observer: CdcObserver,

    pd_client: Arc<dyn PdClient>,
    timer: SteadyTimer,
    tso_worker: Runtime,
    store_meta: Arc<StdMutex<StoreMeta>>,
    /// The concurrency manager for transactions. It's needed for CDC to check locks when
    /// calculating resolved_ts.
    concurrency_manager: ConcurrencyManager,

    config: CdcConfig,
    api_version: ApiVersion,

    // Incremental scan
    workers: Runtime,
    scan_concurrency_semaphore: Arc<Semaphore>,
    scan_speed_limiter: Limiter,
    max_scan_batch_bytes: usize,
    max_scan_batch_size: usize,
    sink_memory_quota: MemoryQuota,

    old_value_cache: OldValueCache,
    resolved_region_heap: ResolvedRegionHeap,

    // Check leader
    // store_id -> client
    tikv_clients: Arc<Mutex<HashMap<u64, TikvClient>>>,
    env: Arc<Environment>,
    security_mgr: Arc<SecurityManager>,
    region_read_progress: RegionReadProgressRegistry,

    // Metrics and logging.
    min_resolved_ts: TimeStamp,
    min_ts_region_id: u64,
    resolved_region_count: usize,
    unresolved_region_count: usize,
    warn_resolved_ts_repeat_count: usize,
}

impl<T: 'static + RaftStoreRouter<E>, E: KvEngine> Endpoint<T, E> {
    pub fn new(
        cluster_id: u64,
        config: &CdcConfig,
        api_version: ApiVersion,
        pd_client: Arc<dyn PdClient>,
        scheduler: Scheduler<Task>,
        raft_router: T,
        engine: E,
        observer: CdcObserver,
        store_meta: Arc<StdMutex<StoreMeta>>,
        concurrency_manager: ConcurrencyManager,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
        sink_memory_quota: MemoryQuota,
    ) -> Endpoint<T, E> {
        let workers = Builder::new_multi_thread()
            .thread_name("cdcwkr")
            .worker_threads(config.incremental_scan_threads)
            .after_start_wrapper(|| {})
            .before_stop_wrapper(|| {})
            .build()
            .unwrap();
        let tso_worker = Builder::new_multi_thread()
            .thread_name("tso")
            .worker_threads(config.tso_worker_threads)
            .enable_time()
            .after_start_wrapper(|| {})
            .before_stop_wrapper(|| {})
            .build()
            .unwrap();

        // Initialized for the first time, subsequent adjustments will be made based on configuration updates.
        let scan_concurrency_semaphore =
            Arc::new(Semaphore::new(config.incremental_scan_concurrency));
        let old_value_cache = OldValueCache::new(config.old_value_cache_memory_quota);
        let speed_limiter = Limiter::new(if config.incremental_scan_speed_limit.0 > 0 {
            config.incremental_scan_speed_limit.0 as f64
        } else {
            f64::INFINITY
        });

        CDC_SINK_CAP.set(sink_memory_quota.capacity() as i64);
        // For scan efficiency, the scan batch bytes should be around 1MB.
        let max_scan_batch_bytes = 1024 * 1024;
        // Assume 1KB per entry.
        let max_scan_batch_size = 1024;

        let region_read_progress = store_meta.lock().unwrap().region_read_progress.clone();
        let ep = Endpoint {
            cluster_id,
            env,
            security_mgr,
            capture_regions: HashMap::default(),
            connections: HashMap::default(),
            scheduler,
            pd_client,
            tso_worker,
            timer: SteadyTimer::default(),
            scan_speed_limiter: speed_limiter,
            max_scan_batch_bytes,
            max_scan_batch_size,
            config: config.clone(),
            api_version,
            workers,
            scan_concurrency_semaphore,
            raft_router,
            engine,
            observer,
            store_meta,
            concurrency_manager,
            min_resolved_ts: TimeStamp::max(),
            min_ts_region_id: 0,
            resolved_region_heap: ResolvedRegionHeap {
                heap: BinaryHeap::new(),
            },
            old_value_cache,
            resolved_region_count: 0,
            unresolved_region_count: 0,
            sink_memory_quota,
            tikv_clients: Arc::new(Mutex::new(HashMap::default())),
            region_read_progress,
            // Log the first resolved ts warning.
            warn_resolved_ts_repeat_count: WARN_RESOLVED_TS_COUNT_THRESHOLD,
        };
        ep.register_min_ts_event();
        ep
    }

    fn on_change_cfg(&mut self, change: ConfigChange) {
        // Validate first.
        let mut validate_cfg = self.config.clone();
        validate_cfg.update(change);
        if let Err(e) = validate_cfg.validate() {
            warn!("cdc config update failed"; "error" => ?e);
            return;
        }
        let change = self.config.diff(&validate_cfg);
        info!(
            "cdc config updated";
            "current config" => ?self.config,
            "change" => ?change
        );
        // Update the config here. The following adjustments will all use the new values.
        self.config.update(change.clone());

        // Maybe the cache will be lost due to smaller capacity,
        // but it is acceptable.
        if change.get("old_value_cache_memory_quota").is_some() {
            self.old_value_cache
                .resize(self.config.old_value_cache_memory_quota);
        }

        // Maybe the limit will be exceeded for a while after the concurrency becomes smaller,
        // but it is acceptable.
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
    }

    pub fn set_max_scan_batch_size(&mut self, max_scan_batch_size: usize) {
        self.max_scan_batch_size = max_scan_batch_size;
    }

    fn on_deregister(&mut self, deregister: Deregister) {
        info!("cdc deregister"; "deregister" => ?deregister);
        fail_point!("cdc_before_handle_deregister", |_| {});
        match deregister {
            Deregister::Downstream {
                region_id,
                downstream_id,
                conn_id,
                err,
            } => {
                // The downstream wants to deregister
                let mut is_last = false;
                if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
                    is_last = delegate.unsubscribe(downstream_id, err);
                }
                if let Some(conn) = self.connections.get_mut(&conn_id) {
                    if let Some(id) = conn.downstream_id(region_id) {
                        if downstream_id == id {
                            conn.unsubscribe(region_id);
                        }
                    }
                }
                if is_last {
                    let delegate = self.capture_regions.remove(&region_id).unwrap();
                    // Do not continue to observe the events of the region.
                    let id = delegate.handle.id;
                    let oid = self.observer.unsubscribe_region(region_id, id);
                    assert!(
                        oid.is_some(),
                        "unsubscribe region {} failed, ObserveID {:?}",
                        region_id,
                        id
                    );
                }
            }
            Deregister::Delegate {
                region_id,
                observe_id,
                err,
            } => {
                // Something went wrong, deregister all downstreams of the region.

                // To avoid ABA problem, we must check the unique ObserveID.
                let need_remove = self
                    .capture_regions
                    .get(&region_id)
                    .map_or(false, |d| d.handle.id == observe_id);
                if need_remove {
                    if let Some(mut delegate) = self.capture_regions.remove(&region_id) {
                        delegate.stop(err);
                    }
                    self.connections
                        .iter_mut()
                        .for_each(|(_, conn)| conn.unsubscribe(region_id));
                }
                // Do not continue to observe the events of the region.
                let oid = self.observer.unsubscribe_region(region_id, observe_id);
                assert_eq!(
                    need_remove,
                    oid.is_some(),
                    "unsubscribe region {} failed, ObserveID {:?}",
                    region_id,
                    observe_id
                );
            }
            Deregister::Conn(conn_id) => {
                // The connection is closed, deregister all downstreams of the connection.
                if let Some(conn) = self.connections.remove(&conn_id) {
                    conn.take_downstreams().into_iter().for_each(
                        |(region_id, (downstream_id, _))| {
                            if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
                                delegate.unsubscribe(downstream_id, None);
                                if delegate.downstreams().is_empty() {
                                    let delegate = self.capture_regions.remove(&region_id).unwrap();
                                    // Do not continue to observe the events of the region.
                                    let id = delegate.handle.id;
                                    let oid = self.observer.unsubscribe_region(region_id, id);
                                    assert!(
                                        oid.is_some(),
                                        "unsubscribe region {} failed, ObserveID {:?}",
                                        region_id,
                                        id
                                    );
                                }
                            }
                        },
                    );
                }
            }
        }
    }

    pub fn on_register(
        &mut self,
        mut request: ChangeDataRequest,
        mut downstream: Downstream,
        conn_id: ConnID,
        version: semver::Version,
    ) {
        let region_id = request.region_id;
        let kv_api = request.get_kv_api();
        let api_version = self.api_version;
        let downstream_id = downstream.get_id();
        let downstream_state = downstream.get_state();

        // Register must follow OpenConn, so the connection must be available.
        let conn = self.connections.get_mut(&conn_id).unwrap();
        downstream.set_sink(conn.get_sink().clone());

        // Check if the cluster id matches.
        let request_cluster_id = request.get_header().get_cluster_id();
        if version >= FeatureGate::validate_cluster_id() && self.cluster_id != request_cluster_id {
            let mut err_event = EventError::default();
            let mut err = ErrorClusterIdMismatch::default();
            err.set_current(self.cluster_id);
            err.set_request(request_cluster_id);
            err_event.set_cluster_id_mismatch(err);

            let _ = downstream.sink_error_event(region_id, err_event);
            return;
        }

        if !FeatureGate::validate_kv_api(kv_api, api_version) {
            error!("cdc RawKv is supported by api-version 2 only. TxnKv is not supported now.");
            let mut err_event = EventError::default();
            let mut err = ErrorCompatibility::default();
            err.set_required_version("6.2.0".to_string());
            err_event.set_compatibility(err);

            let _ = downstream.sink_error_event(region_id, err_event);
            return;
        }

        let txn_extra_op = match self.store_meta.lock().unwrap().readers.get(&region_id) {
            Some(reader) => reader.txn_extra_op.clone(),
            None => {
                error!("cdc register for a not found region"; "region_id" => region_id);
                let _ = downstream.sink_region_not_found(region_id);
                return;
            }
        };

        // TODO: Add a new task to close incompatible features.
        if let Some(e) = conn.check_version_and_set_feature(version) {
            // The downstream has not registered yet, send error right away.
            let mut err_event = EventError::default();
            err_event.set_compatibility(e);
            let _ = downstream.sink_error_event(region_id, err_event);
            return;
        }

        if !conn.subscribe(region_id, downstream_id, downstream_state) {
            let mut err_event = EventError::default();
            let mut err = ErrorDuplicateRequest::default();
            err.set_region_id(region_id);
            err_event.set_duplicate_request(err);
            let _ = downstream.sink_error_event(region_id, err_event);
            error!("cdc duplicate register";
                "region_id" => region_id,
                "conn_id" => ?conn_id,
                "req_id" => request.get_request_id(),
                "downstream_id" => ?downstream_id);
            return;
        }

        let mut is_new_delegate = false;
        let delegate = match self.capture_regions.entry(region_id) {
            HashMapEntry::Occupied(e) => e.into_mut(),
            HashMapEntry::Vacant(e) => {
                is_new_delegate = true;
                e.insert(Delegate::new(region_id, txn_extra_op))
            }
        };

        let observe_id = delegate.handle.id;
        info!("cdc register region";
            "region_id" => region_id,
            "conn_id" => ?conn.get_id(),
            "req_id" => request.get_request_id(),
            "observe_id" => ?observe_id,
            "downstream_id" => ?downstream_id);

        let downstream_state = downstream.get_state();
        let checkpoint_ts = request.checkpoint_ts;
        let sched = self.scheduler.clone();

        // Now resolver is only used by tidb downstream.
        // Resolver is created when the first tidb cdc request arrive.
        let is_build_resolver = kv_api == ChangeDataRequestKvApi::TiDb && !delegate.has_resolver();

        let downstream_ = downstream.clone();
        if let Err(err) = delegate.subscribe(downstream) {
            let error_event = err.into_error_event(region_id);
            let _ = downstream_.sink_error_event(region_id, error_event);
            conn.unsubscribe(request.get_region_id());
            if is_new_delegate {
                self.capture_regions.remove(&request.get_region_id());
            }
            return;
        }
        if is_new_delegate {
            // The region has never been registered.
            // Subscribe the change events of the region.
            let old_observe_id = self.observer.subscribe_region(region_id, observe_id);
            assert!(
                old_observe_id.is_none(),
                "region {} must not be observed twice, old ObserveID {:?}, new ObserveID {:?}",
                region_id,
                old_observe_id,
                observe_id
            );
        };

        let change_cmd = ChangeObserver::from_cdc(region_id, delegate.handle.clone());

        let region_epoch = request.take_region_epoch();
        let mut init = Initializer {
            engine: self.engine.clone(),
            sched,
            region_id,
            region_epoch,
            conn_id,
            downstream_id,
            sink: conn.get_sink().clone(),
            request_id: request.get_request_id(),
            downstream_state,
            speed_limiter: self.scan_speed_limiter.clone(),
            max_scan_batch_bytes: self.max_scan_batch_bytes,
            max_scan_batch_size: self.max_scan_batch_size,
            observe_id,
            checkpoint_ts: checkpoint_ts.into(),
            build_resolver: is_build_resolver,
            ts_filter_ratio: self.config.incremental_scan_ts_filter_ratio,
            kv_api,
        };

        let raft_router = self.raft_router.clone();
        let concurrency_semaphore = self.scan_concurrency_semaphore.clone();
        self.workers.spawn(async move {
            CDC_SCAN_TASKS.with_label_values(&["total"]).inc();
            match init
                .initialize(change_cmd, raft_router, concurrency_semaphore)
                .await
            {
                Ok(()) => {
                    CDC_SCAN_TASKS.with_label_values(&["finish"]).inc();
                }
                Err(e) => {
                    CDC_SCAN_TASKS.with_label_values(&["abort"]).inc();
                    error!("cdc initialize fail: {}", e; "region_id" => region_id);
                    init.deregister_downstream(e)
                }
            }
        });
    }

    pub fn on_multi_batch(&mut self, multi: Vec<CmdBatch>, old_value_cb: OldValueCallback) {
        fail_point!("cdc_before_handle_multi_batch", |_| {});
        let mut statistics = Statistics::default();
        for batch in multi {
            let region_id = batch.region_id;
            let mut deregister = None;
            if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
                if delegate.has_failed() {
                    // Skip the batch if the delegate has failed.
                    continue;
                }
                if let Err(e) = delegate.on_batch(
                    batch,
                    &old_value_cb,
                    &mut self.old_value_cache,
                    &mut statistics,
                ) {
                    assert!(delegate.has_failed());
                    // Delegate has error, deregister the delegate.
                    deregister = Some(Deregister::Delegate {
                        region_id,
                        observe_id: delegate.handle.id,
                        err: e,
                    });
                }
            }
            if let Some(deregister) = deregister {
                self.on_deregister(deregister);
            }
        }
        flush_oldvalue_stats(&statistics, TAG_DELTA_CHANGE);
    }

    fn on_region_ready(&mut self, observe_id: ObserveID, resolver: Resolver, region: Region) {
        let region_id = region.get_id();
        let mut failed_downstreams = Vec::new();
        if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
            if delegate.handle.id == observe_id {
                let region_id = delegate.region_id;
                for (downstream, e) in delegate.on_region_ready(resolver, region) {
                    failed_downstreams.push(Deregister::Downstream {
                        region_id,
                        downstream_id: downstream.get_id(),
                        conn_id: downstream.get_conn_id(),
                        err: Some(e),
                    });
                }
            } else {
                debug!("cdc stale region ready";
                    "region_id" => region.get_id(),
                    "observe_id" => ?observe_id,
                    "current_id" => ?delegate.handle.id);
            }
        } else {
            debug!("cdc region not found on region ready (finish building resolver)";
                "region_id" => region.get_id());
        }

        // Deregister downstreams if there is any downstream fails to subscribe.
        for deregister in failed_downstreams {
            self.on_deregister(deregister);
        }
    }

    fn on_min_ts(&mut self, regions: Vec<u64>, min_ts: TimeStamp) {
        // Reset resolved_regions to empty.
        let resolved_regions = &mut self.resolved_region_heap;
        resolved_regions.clear();

        let total_region_count = regions.len();
        self.min_resolved_ts = TimeStamp::max();
        let mut advance_ok = 0;
        let mut advance_failed_none = 0;
        let mut advance_failed_same = 0;
        let mut advance_failed_stale = 0;
        for region_id in regions {
            if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
                let old_resolved_ts = delegate
                    .resolver
                    .as_ref()
                    .map_or(TimeStamp::zero(), |r| r.resolved_ts());
                if old_resolved_ts > min_ts {
                    advance_failed_stale += 1;
                }
                if let Some(resolved_ts) = delegate.on_min_ts(min_ts) {
                    if resolved_ts < self.min_resolved_ts {
                        self.min_resolved_ts = resolved_ts;
                        self.min_ts_region_id = region_id;
                    }
                    resolved_regions.push(region_id, resolved_ts);

                    if resolved_ts == old_resolved_ts {
                        advance_failed_same += 1;
                    } else {
                        advance_ok += 1;
                    }
                } else {
                    advance_failed_none += 1;
                }
            }
        }
        let lag_millis = min_ts
            .physical()
            .saturating_sub(self.min_resolved_ts.physical());
        if Duration::from_millis(lag_millis) > WARN_RESOLVED_TS_LAG_THRESHOLD {
            self.warn_resolved_ts_repeat_count += 1;
            if self.warn_resolved_ts_repeat_count >= WARN_RESOLVED_TS_COUNT_THRESHOLD {
                self.warn_resolved_ts_repeat_count = 0;
                warn!("cdc resolved ts lag too large";
                    "min_resolved_ts" => self.min_resolved_ts,
                    "min_ts_region_id" => self.min_ts_region_id,
                    "min_ts" => min_ts,
                    "ok" => advance_ok,
                    "none" => advance_failed_none,
                    "stale" => advance_failed_stale,
                    "same" => advance_failed_same);
            }
        }
        self.resolved_region_count = resolved_regions.heap.len();
        self.unresolved_region_count = total_region_count - self.resolved_region_count;

        // Separate broadcasting outlier regions and normal regions,
        // so 1) downstreams know where they should send resolve lock requests,
        // and 2) resolved ts of normal regions does not fallback.
        //
        // Max number of outliers, in most cases, only a few regions are outliers.
        // TODO: figure out how to avoid create hashset every time, saving some CPU.
        let max_outlier_count = 32;
        let (outlier_min_resolved_ts, outlier_regions) = resolved_regions.pop(max_outlier_count);
        let (normal_min_resolved_ts, normal_regions) = resolved_regions.to_hash_set();
        self.broadcast_resolved_ts(outlier_min_resolved_ts, outlier_regions);
        self.broadcast_resolved_ts(normal_min_resolved_ts, normal_regions);
    }

    fn broadcast_resolved_ts(&self, min_resolved_ts: TimeStamp, regions: HashSet<u64>) {
        let min_resolved_ts = min_resolved_ts.into_inner();
        let send_cdc_event = |regions: &HashSet<u64>, min_resolved_ts: u64, conn: &Conn| {
            let downstream_regions = conn.get_downstreams();
            let mut resolved_ts = ResolvedTs::default();
            resolved_ts.ts = min_resolved_ts;
            resolved_ts.regions = Vec::with_capacity(downstream_regions.len());
            // Only send region ids that are captured by the connection.
            for (region_id, (_, downstream_state)) in conn.get_downstreams() {
                if regions.contains(region_id) && downstream_state.load().ready_for_advancing_ts() {
                    resolved_ts.regions.push(*region_id);
                }
            }
            if resolved_ts.regions.is_empty() {
                // Skip empty resolved ts message.
                return;
            }
            // No need force send, as resolved ts messages is sent regularly.
            // And errors can be ignored.
            let force_send = false;
            match conn
                .get_sink()
                .unbounded_send(CdcEvent::ResolvedTs(resolved_ts), force_send)
            {
                Ok(_) => (),
                Err(SendError::Disconnected) => {
                    debug!("cdc send event failed, disconnected";
                        "conn_id" => ?conn.get_id(), "downstream" => ?conn.get_peer());
                }
                Err(SendError::Full) | Err(SendError::Congested) => {
                    info!("cdc send event failed, full";
                        "conn_id" => ?conn.get_id(), "downstream" => ?conn.get_peer());
                }
            }
        };
        for conn in self.connections.values() {
            let features = if let Some(features) = conn.get_feature() {
                features
            } else {
                // None means there is no downstream registered yet.
                continue;
            };

            if features.contains(FeatureGate::BATCH_RESOLVED_TS) {
                send_cdc_event(&regions, min_resolved_ts, conn);
            } else {
                // Fallback to previous non-batch resolved ts event.
                for region_id in &regions {
                    self.broadcast_resolved_ts_compact(*region_id, min_resolved_ts, conn);
                }
            }
        }
    }

    fn broadcast_resolved_ts_compact(&self, region_id: u64, resolved_ts: u64, conn: &Conn) {
        let downstream_id = match conn.downstream_id(region_id) {
            Some(downstream_id) => downstream_id,
            // No such region registers in the connection.
            None => {
                debug!("cdc send resolved ts failed, no region downstream id found";
                    "region_id" => region_id);
                return;
            }
        };
        let delegate = match self.capture_regions.get(&region_id) {
            Some(delegate) => delegate,
            // No such region registers in the endpoint.
            None => {
                info!("cdc send resolved ts failed, no region delegate found";
                    "region_id" => region_id, "downstream_id" => ?downstream_id);
                return;
            }
        };
        let downstream = match delegate.downstream(downstream_id) {
            Some(downstream) => downstream,
            // No such downstream registers in the delegate.
            None => {
                info!("cdc send resolved ts failed, no region downstream found";
                    "region_id" => region_id, "downstream_id" => ?downstream_id);
                return;
            }
        };
        if !downstream.get_state().load().ready_for_advancing_ts() {
            // Only send resolved timestamp if the downstream is ready.
            return;
        }
        let resolved_ts_event = Event {
            region_id,
            event: Some(Event_oneof_event::ResolvedTs(resolved_ts)),
            ..Default::default()
        };
        // No need force send, as resolved ts messages is sent regularly.
        // And errors can be ignored.
        let force_send = false;
        let _ = downstream.sink_event(resolved_ts_event, force_send);
    }

    fn register_min_ts_event(&self) {
        let timeout = self.timer.delay(self.config.min_ts_interval.0);
        let pd_client = self.pd_client.clone();
        let scheduler = self.scheduler.clone();
        let raft_router = self.raft_router.clone();
        let regions: Vec<(u64, ObserveID)> = self
            .capture_regions
            .iter()
            .map(|(region_id, delegate)| (*region_id, delegate.handle.id))
            .collect();
        let cm: ConcurrencyManager = self.concurrency_manager.clone();
        let env = self.env.clone();
        let security_mgr = self.security_mgr.clone();
        let store_meta = self.store_meta.clone();
        let tikv_clients = self.tikv_clients.clone();
        let hibernate_regions_compatible = self.config.hibernate_regions_compatible;
        let region_read_progress = self.region_read_progress.clone();

        let fut = async move {
            let _ = timeout.compat().await;
            // Ignore get tso errors since we will retry every `min_ts_interval`.
            let min_ts_pd = pd_client.get_tso().await.unwrap_or_default();
            let mut min_ts = min_ts_pd;
            let mut min_ts_min_lock = min_ts_pd;

            // Sync with concurrency manager so that it can work correctly when optimizations
            // like async commit is enabled.
            // Note: This step must be done before scheduling `Task::MinTS` task, and the
            // resolver must be checked in or after `Task::MinTS`' execution.
            cm.update_max_ts(min_ts);
            if let Some(min_mem_lock_ts) = cm.global_min_lock_ts() {
                if min_mem_lock_ts < min_ts {
                    min_ts = min_mem_lock_ts;
                }
                min_ts_min_lock = min_mem_lock_ts;
            }

            match scheduler.schedule(Task::RegisterMinTsEvent) {
                Ok(_) | Err(ScheduleError::Stopped(_)) => (),
                // Must schedule `RegisterMinTsEvent` event otherwise resolved ts can not
                // advance normally.
                Err(err) => panic!("failed to regiester min ts event, error: {:?}", err),
            }

            let gate = pd_client.feature_gate();

            let regions =
                if hibernate_regions_compatible && gate.can_enable(FEATURE_RESOLVED_TS_STORE) {
                    CDC_RESOLVED_TS_ADVANCE_METHOD.set(1);
                    let regions = regions
                        .into_iter()
                        .map(|(region_id, _)| region_id)
                        .collect();
                    resolved_ts::region_resolved_ts_store(
                        regions,
                        store_meta,
                        region_read_progress,
                        pd_client,
                        security_mgr,
                        env,
                        tikv_clients,
                        min_ts,
                    )
                    .await
                } else {
                    CDC_RESOLVED_TS_ADVANCE_METHOD.set(0);
                    Self::region_resolved_ts_raft(regions, &scheduler, raft_router, min_ts).await
                };

            if !regions.is_empty() {
                match scheduler.schedule(Task::MinTS { regions, min_ts }) {
                    Ok(_) | Err(ScheduleError::Stopped(_)) => (),
                    // Must schedule `RegisterMinTsEvent` event otherwise resolved ts can not
                    // advance normally.
                    Err(err) => panic!("failed to schedule min ts event, error: {:?}", err),
                }
            }
            let lag_millis = min_ts_pd.physical().saturating_sub(min_ts.physical());
            if Duration::from_millis(lag_millis) > WARN_RESOLVED_TS_LAG_THRESHOLD {
                // TODO: Suppress repeat logs by using WARN_RESOLVED_TS_COUNT_THRESHOLD.
                info!("cdc min_ts lag too large";
                    "min_ts" => min_ts, "min_ts_pd" => min_ts_pd,
                    "min_ts_min_lock" => min_ts_min_lock);
            }
        };
        self.tso_worker.spawn(fut);
    }

    async fn region_resolved_ts_raft(
        regions: Vec<(u64, ObserveID)>,
        scheduler: &Scheduler<Task>,
        raft_router: T,
        min_ts: TimeStamp,
    ) -> Vec<u64> {
        // TODO: send a message to raftstore would consume too much cpu time,
        // try to handle it outside raftstore.
        let regions: Vec<_> = regions
            .iter()
            .copied()
            .map(|(region_id, observe_id)| {
                let scheduler_clone = scheduler.clone();
                let raft_router_clone = raft_router.clone();
                async move {
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    if let Err(e) = raft_router_clone.significant_send(
                        region_id,
                        SignificantMsg::LeaderCallback(Callback::Read(Box::new(move |resp| {
                            let resp = if resp.response.get_header().has_error() {
                                None
                            } else {
                                Some(region_id)
                            };
                            if tx.send(resp).is_err() {
                                error!("cdc send tso response failed"; "region_id" => region_id);
                            }
                        }))),
                    ) {
                        warn!("cdc send LeaderCallback failed"; "err" => ?e, "min_ts" => min_ts);
                        let deregister = Deregister::Delegate {
                            observe_id,
                            region_id,
                            err: Error::request(e.into()),
                        };
                        if let Err(e) = scheduler_clone.schedule(Task::Deregister(deregister)) {
                            error!("cdc schedule cdc task failed"; "error" => ?e);
                        }
                        return None;
                    }
                    rx.await.unwrap_or(None)
                }
            })
            .collect();
        let resps = futures::future::join_all(regions).await;
        resps.into_iter().flatten().collect::<Vec<u64>>()
    }

    fn on_open_conn(&mut self, conn: Conn) {
        self.connections.insert(conn.get_id(), conn);
    }
}

impl<T: 'static + RaftStoreRouter<E>, E: KvEngine> Runnable for Endpoint<T, E> {
    type Task = Task;

    fn run(&mut self, task: Task) {
        debug!("cdc run task"; "task" => %task);

        match task {
            Task::MinTS { regions, min_ts } => self.on_min_ts(regions, min_ts),
            Task::Register {
                request,
                downstream,
                conn_id,
                version,
            } => self.on_register(request, downstream, conn_id, version),
            Task::ResolverReady {
                observe_id,
                resolver,
                region,
            } => self.on_region_ready(observe_id, resolver, region),
            Task::Deregister(deregister) => self.on_deregister(deregister),
            Task::MultiBatch {
                multi,
                old_value_cb,
            } => self.on_multi_batch(multi, old_value_cb),
            Task::OpenConn { conn } => self.on_open_conn(conn),
            Task::RegisterMinTsEvent => self.register_min_ts_event(),
            Task::InitDownstream {
                region_id,
                downstream_id,
                downstream_state,
                sink,
                incremental_scan_barrier,
                cb,
            } => {
                if let Err(e) = sink.unbounded_send(incremental_scan_barrier, true) {
                    error!("cdc failed to schedule barrier for delta before delta scan";
                        "region_id" => region_id,
                        "error" => ?e);
                    return;
                }
                if on_init_downstream(&downstream_state) {
                    info!("cdc downstream starts to initialize";
                        "region_id" => region_id,
                        "downstream_id" => ?downstream_id);
                } else {
                    warn!("cdc downstream fails to initialize";
                        "region_id" => region_id,
                        "downstream_id" => ?downstream_id);
                }
                cb();
            }
            Task::TxnExtra(txn_extra) => {
                for (k, v) in txn_extra.old_values {
                    self.old_value_cache.insert(k, v);
                }
            }
            Task::Validate(validate) => match validate {
                Validate::Region(region_id, validate) => {
                    validate(self.capture_regions.get(&region_id));
                }
                Validate::OldValueCache(validate) => {
                    validate(&self.old_value_cache);
                }
            },
            Task::ChangeConfig(change) => self.on_change_cfg(change),
        }
    }
}

impl<T: 'static + RaftStoreRouter<E>, E: KvEngine> RunnableWithTimer for Endpoint<T, E> {
    fn on_timeout(&mut self) {
        CDC_ENDPOINT_PENDING_TASKS.set(self.scheduler.pending_tasks() as _);

        // Reclaim resolved_region_heap memory.
        self.resolved_region_heap
            .reset_and_shrink_to(self.capture_regions.len());

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
        }
        self.min_resolved_ts = TimeStamp::max();
        self.min_ts_region_id = 0;

        self.old_value_cache.flush_metrics();
        CDC_SINK_BYTES.set(self.sink_memory_quota.in_use() as i64);
    }

    fn get_interval(&self) -> Duration {
        // Currently there is only one timeout for CDC.
        Duration::from_millis(METRICS_FLUSH_INTERVAL)
    }
}

pub struct CdcTxnExtraScheduler {
    scheduler: Scheduler<Task>,
}

impl CdcTxnExtraScheduler {
    pub fn new(scheduler: Scheduler<Task>) -> CdcTxnExtraScheduler {
        CdcTxnExtraScheduler { scheduler }
    }
}

impl TxnExtraScheduler for CdcTxnExtraScheduler {
    fn schedule(&self, txn_extra: TxnExtra) {
        if let Err(e) = self.scheduler.schedule(Task::TxnExtra(txn_extra)) {
            error!("cdc schedule txn extra failed"; "err" => ?e);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::{Deref, DerefMut};

    use engine_rocks::RocksEngine;
    use kvproto::{
        cdcpb::{ChangeDataRequestKvApi, Header},
        errorpb::Error as ErrorHeader,
    };
    use raftstore::{
        errors::{DiscardReason, Error as RaftStoreError},
        store::{msg::CasualMessage, PeerMsg, ReadDelegate},
    };
    use test_raftstore::{MockRaftStoreRouter, TestPdClient};
    use tikv::{
        server::DEFAULT_CLUSTER_ID,
        storage::{kv::Engine, TestEngineBuilder},
    };
    use tikv_util::{
        config::{ReadableDuration, ReadableSize},
        worker::{dummy_scheduler, ReceiverWrapper},
    };

    use super::*;
    use crate::{channel, recv_timeout};

    struct TestEndpointSuite {
        // The order must ensure `endpoint` be dropped before other fields.
        endpoint: Endpoint<MockRaftStoreRouter, RocksEngine>,
        raft_router: MockRaftStoreRouter,
        task_rx: ReceiverWrapper<Task>,
        raft_rxs: HashMap<u64, tikv_util::mpsc::Receiver<PeerMsg<RocksEngine>>>,
    }

    impl TestEndpointSuite {
        // It's important to matain raft receivers in `raft_rxs`, otherwise all cases
        // need to drop `endpoint` and `rx` in order manually.
        fn add_region(&mut self, region_id: u64, cap: usize) {
            let rx = self.raft_router.add_region(region_id, cap);
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
            let router = &self.raft_router;
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
    }

    impl Deref for TestEndpointSuite {
        type Target = Endpoint<MockRaftStoreRouter, RocksEngine>;
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
        let (task_sched, task_rx) = dummy_scheduler();
        let raft_router = MockRaftStoreRouter::new();
        let ep = Endpoint::new(
            DEFAULT_CLUSTER_ID,
            cfg,
            api_version,
            Arc::new(TestPdClient::new(0, true)),
            task_sched.clone(),
            raft_router.clone(),
            engine.unwrap_or_else(|| {
                TestEngineBuilder::new()
                    .build_without_cache()
                    .unwrap()
                    .kv_engine()
            }),
            CdcObserver::new(task_sched),
            Arc::new(StdMutex::new(StoreMeta::new(0))),
            ConcurrencyManager::new(1.into()),
            Arc::new(Environment::new(1)),
            Arc::new(SecurityManager::default()),
            MemoryQuota::new(usize::MAX),
        );

        TestEndpointSuite {
            endpoint: ep,
            raft_router,
            task_rx,
            raft_rxs: HashMap::default(),
        }
    }

    #[test]
    fn test_api_version_check() {
        let cfg = CdcConfig::default();
        let mut suite = mock_endpoint(&cfg, None, ApiVersion::V1);
        suite.add_region(1, 100);
        let quota = crate::channel::MemoryQuota::new(usize::MAX);
        let (tx, mut rx) = channel::channel(1, quota);
        let mut rx = rx.drain();

        let conn = Conn::new(tx, String::new());
        let conn_id = conn.get_id();
        suite.run(Task::OpenConn { conn });
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        req.set_kv_api(ChangeDataRequestKvApi::TiDb);
        let region_epoch = req.get_region_epoch().clone();
        let version = FeatureGate::batch_resolved_ts();

        // Compatibility error.
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch.clone(),
            1,
            conn_id,
            ChangeDataRequestKvApi::RawKv,
        );
        req.set_kv_api(ChangeDataRequestKvApi::RawKv);
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: version.clone(),
        });
        let cdc_event = channel::recv_timeout(&mut rx, Duration::from_millis(500))
            .unwrap()
            .unwrap();
        if let CdcEvent::Event(mut e) = cdc_event.0 {
            assert_eq!(e.region_id, 1);
            let event = e.event.take().unwrap();
            match event {
                Event_oneof_event::Error(err) => {
                    assert!(err.has_compatibility());
                }
                other => panic!("unknown event {:?}", other),
            }
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }
        suite
            .task_rx
            .recv_timeout(Duration::from_millis(100))
            .unwrap_err();

        // Compatibility error.
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch.clone(),
            2,
            conn_id,
            ChangeDataRequestKvApi::TxnKv,
        );
        req.set_kv_api(ChangeDataRequestKvApi::TxnKv);
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: version.clone(),
        });
        let cdc_event = channel::recv_timeout(&mut rx, Duration::from_millis(500))
            .unwrap()
            .unwrap();
        if let CdcEvent::Event(mut e) = cdc_event.0 {
            assert_eq!(e.region_id, 1);
            let event = e.event.take().unwrap();
            match event {
                Event_oneof_event::Error(err) => {
                    assert!(err.has_compatibility());
                }
                other => panic!("unknown event {:?}", other),
            }
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }
        suite
            .task_rx
            .recv_timeout(Duration::from_millis(100))
            .unwrap_err();

        suite.api_version = ApiVersion::V2;
        // Compatibility error.
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch,
            3,
            conn_id,
            ChangeDataRequestKvApi::TxnKv,
        );
        req.set_kv_api(ChangeDataRequestKvApi::TxnKv);
        suite.run(Task::Register {
            request: req,
            downstream,
            conn_id,
            version,
        });
        let cdc_event = channel::recv_timeout(&mut rx, Duration::from_millis(500))
            .unwrap()
            .unwrap();
        if let CdcEvent::Event(mut e) = cdc_event.0 {
            assert_eq!(e.region_id, 1);
            let event = e.event.take().unwrap();
            match event {
                Event_oneof_event::Error(err) => {
                    assert!(err.has_compatibility());
                }
                other => panic!("unknown event {:?}", other),
            }
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }
        suite
            .task_rx
            .recv_timeout(Duration::from_millis(100))
            .unwrap_err();
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
                ep.old_value_cache.capacity(),
                ReadableSize::mb(512).0 as usize
            );
            ep.run(Task::ChangeConfig(diff));
            assert_eq!(
                ep.config.old_value_cache_memory_quota,
                ReadableSize::mb(1024)
            );
            assert_eq!(
                ep.old_value_cache.capacity(),
                ReadableSize::mb(1024).0 as usize
            );
        }

        // Modify incremental_scan_concurrency.
        {
            let mut updated_cfg = cfg.clone();
            {
                // Update it to be smaller than incremental_scan_threads,
                // which will be an invalid change and will modified to incremental_scan_threads.
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

            assert_eq!(ep.sink_memory_quota.capacity(), usize::MAX);
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
    }

    #[test]
    fn test_raftstore_is_busy() {
        let quota = crate::channel::MemoryQuota::new(usize::MAX);
        let (tx, _rx) = channel::channel(1, quota);
        let mut suite = mock_endpoint(&CdcConfig::default(), None, ApiVersion::V1);

        // Fill the channel.
        suite.add_region(1 /* region id */, 1 /* cap */);
        suite.fill_raft_rx(1);

        let conn = Conn::new(tx, String::new());
        let conn_id = conn.get_id();
        suite.run(Task::OpenConn { conn });
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch,
            0,
            conn_id,
            ChangeDataRequestKvApi::TiDb,
        );
        suite.run(Task::Register {
            request: req,
            downstream,
            conn_id,
            version: semver::Version::new(0, 0, 0),
        });
        assert_eq!(suite.endpoint.capture_regions.len(), 1);

        for _ in 0..5 {
            if let Ok(Some(Task::Deregister(Deregister::Downstream {
                err: Some(Error::Request(err)),
                ..
            }))) = suite.task_rx.recv_timeout(Duration::from_secs(1))
            {
                assert!(!err.has_server_is_busy());
            }
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
        let quota = crate::channel::MemoryQuota::new(usize::MAX);
        let (tx, mut rx) = channel::channel(1, quota);
        let mut rx = rx.drain();

        let conn = Conn::new(tx, String::new());
        let conn_id = conn.get_id();
        suite.run(Task::OpenConn { conn });
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch.clone(),
            1,
            conn_id,
            ChangeDataRequestKvApi::TiDb,
        );
        // Enable batch resolved ts in the test.
        let version = FeatureGate::batch_resolved_ts();
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: version.clone(),
        });
        assert_eq!(suite.endpoint.capture_regions.len(), 1);
        suite
            .task_rx
            .recv_timeout(Duration::from_millis(100))
            .unwrap_err();

        // duplicate request error.
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch.clone(),
            2,
            conn_id,
            ChangeDataRequestKvApi::TiDb,
        );
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: version.clone(),
        });
        let cdc_event = channel::recv_timeout(&mut rx, Duration::from_millis(500))
            .unwrap()
            .unwrap();
        if let CdcEvent::Event(mut e) = cdc_event.0 {
            assert_eq!(e.region_id, 1);
            assert_eq!(e.request_id, 2);
            let event = e.event.take().unwrap();
            match event {
                Event_oneof_event::Error(err) => {
                    assert!(err.has_duplicate_request());
                }
                other => panic!("unknown event {:?}", other),
            }
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }
        assert_eq!(suite.endpoint.capture_regions.len(), 1);
        suite
            .task_rx
            .recv_timeout(Duration::from_millis(100))
            .unwrap_err();

        // Compatibility error.
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch,
            3,
            conn_id,
            ChangeDataRequestKvApi::TiDb,
        );
        suite.run(Task::Register {
            request: req,
            downstream,
            conn_id,
            // The version that does not support batch resolved ts.
            version: semver::Version::new(0, 0, 0),
        });
        let cdc_event = channel::recv_timeout(&mut rx, Duration::from_millis(500))
            .unwrap()
            .unwrap();
        if let CdcEvent::Event(mut e) = cdc_event.0 {
            assert_eq!(e.region_id, 1);
            assert_eq!(e.request_id, 3);
            let event = e.event.take().unwrap();
            match event {
                Event_oneof_event::Error(err) => {
                    assert!(err.has_compatibility());
                }
                other => panic!("unknown event {:?}", other),
            }
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }
        assert_eq!(suite.endpoint.capture_regions.len(), 1);
        suite
            .task_rx
            .recv_timeout(Duration::from_millis(100))
            .unwrap_err();

        // The first scan task of a region is initiated in register, and when it
        // fails, it should send a deregister region task, otherwise the region
        // delegate does not have resolver.
        //
        // Test non-exist region in raft router.
        let mut req = ChangeDataRequest::default();
        req.set_region_id(100);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch.clone(),
            1,
            conn_id,
            ChangeDataRequestKvApi::TiDb,
        );
        suite.add_local_reader(100);
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: version.clone(),
        });
        // Region 100 is inserted into capture_regions.
        assert_eq!(suite.endpoint.capture_regions.len(), 2);
        let task = suite
            .task_rx
            .recv_timeout(Duration::from_millis(100))
            .unwrap();
        match task.unwrap() {
            Task::Deregister(Deregister::Delegate { region_id, err, .. }) => {
                assert_eq!(region_id, 100);
                assert!(matches!(err, Error::Request(_)), "{:?}", err);
            }
            other => panic!("unexpected task {:?}", other),
        }

        // Test errors on CaptureChange message.
        req.set_region_id(101);
        suite.add_region(101, 100);
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch,
            1,
            conn_id,
            ChangeDataRequestKvApi::TiDb,
        );
        suite.run(Task::Register {
            request: req,
            downstream,
            conn_id,
            version,
        });
        // Drop CaptureChange message, it should cause scan task failure.
        let timeout = Duration::from_millis(100);
        let _ = suite.raft_rx(101).recv_timeout(timeout).unwrap();
        assert_eq!(suite.endpoint.capture_regions.len(), 3);
        let task = suite.task_rx.recv_timeout(timeout).unwrap();
        match task.unwrap() {
            Task::Deregister(Deregister::Delegate { region_id, err, .. }) => {
                assert_eq!(region_id, 101);
                assert!(matches!(err, Error::Other(_)), "{:?}", err);
            }
            other => panic!("unexpected task {:?}", other),
        }
    }

    #[test]
    fn test_feature_gate() {
        let cfg = CdcConfig {
            min_ts_interval: ReadableDuration(Duration::from_secs(60)),
            ..Default::default()
        };
        let mut suite = mock_endpoint(&cfg, None, ApiVersion::V1);
        suite.add_region(1, 100);

        let quota = crate::channel::MemoryQuota::new(usize::MAX);
        let (tx, mut rx) = channel::channel(1, quota);
        let mut rx = rx.drain();
        let mut region = Region::default();
        region.set_id(1);
        let conn = Conn::new(tx, String::new());
        let conn_id = conn.get_id();
        suite.run(Task::OpenConn { conn });
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch.clone(),
            0,
            conn_id,
            ChangeDataRequestKvApi::TiDb,
        );
        downstream.get_state().store(DownstreamState::Normal);
        // Enable batch resolved ts in the test.
        let version = FeatureGate::batch_resolved_ts();
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: version.clone(),
        });
        let resolver = Resolver::new(1);
        let observe_id = suite.endpoint.capture_regions[&1].handle.id;
        suite.on_region_ready(observe_id, resolver, region.clone());
        suite.run(Task::MinTS {
            regions: vec![1],
            min_ts: TimeStamp::from(1),
        });
        let cdc_event = channel::recv_timeout(&mut rx, Duration::from_millis(500))
            .unwrap()
            .unwrap();
        if let CdcEvent::ResolvedTs(r) = cdc_event.0 {
            assert_eq!(r.regions, vec![1]);
            assert_eq!(r.ts, 1);
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }

        // Register region 2 to the conn.
        req.set_region_id(2);
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch.clone(),
            0,
            conn_id,
            ChangeDataRequestKvApi::TiDb,
        );
        downstream.get_state().store(DownstreamState::Normal);
        suite.add_region(2, 100);
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version,
        });
        let resolver = Resolver::new(2);
        region.set_id(2);
        let observe_id = suite.endpoint.capture_regions[&2].handle.id;
        suite.on_region_ready(observe_id, resolver, region);
        suite.run(Task::MinTS {
            regions: vec![1, 2],
            min_ts: TimeStamp::from(2),
        });
        let cdc_event = channel::recv_timeout(&mut rx, Duration::from_millis(500))
            .unwrap()
            .unwrap();
        if let CdcEvent::ResolvedTs(mut r) = cdc_event.0 {
            r.regions.as_mut_slice().sort_unstable();
            assert_eq!(r.regions, vec![1, 2]);
            assert_eq!(r.ts, 2);
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }

        // Register region 3 to another conn which is not support batch resolved ts.
        let quota = crate::channel::MemoryQuota::new(usize::MAX);
        let (tx, mut rx2) = channel::channel(1, quota);
        let mut rx2 = rx2.drain();
        let mut region = Region::default();
        region.set_id(3);
        let conn = Conn::new(tx, String::new());
        let conn_id = conn.get_id();
        suite.run(Task::OpenConn { conn });
        req.set_region_id(3);
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch,
            3,
            conn_id,
            ChangeDataRequestKvApi::TiDb,
        );
        downstream.get_state().store(DownstreamState::Normal);
        suite.add_region(3, 100);
        suite.run(Task::Register {
            request: req,
            downstream,
            conn_id,
            version: semver::Version::new(4, 0, 5),
        });
        let resolver = Resolver::new(3);
        region.set_id(3);
        let observe_id = suite.endpoint.capture_regions[&3].handle.id;
        suite.on_region_ready(observe_id, resolver, region);
        suite.run(Task::MinTS {
            regions: vec![1, 2, 3],
            min_ts: TimeStamp::from(3),
        });
        let cdc_event = channel::recv_timeout(&mut rx, Duration::from_millis(500))
            .unwrap()
            .unwrap();
        if let CdcEvent::ResolvedTs(mut r) = cdc_event.0 {
            r.regions.as_mut_slice().sort_unstable();
            // Region 3 resolved ts must not be send to the first conn when
            // batch resolved ts is enabled.
            assert_eq!(r.regions, vec![1, 2]);
            assert_eq!(r.ts, 3);
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }
        let cdc_event = channel::recv_timeout(&mut rx2, Duration::from_millis(500))
            .unwrap()
            .unwrap();
        if let CdcEvent::Event(mut e) = cdc_event.0 {
            assert_eq!(e.region_id, 3);
            assert_eq!(e.request_id, 3);
            let event = e.event.take().unwrap();
            match event {
                Event_oneof_event::ResolvedTs(ts) => {
                    assert_eq!(ts, 3);
                }
                other => panic!("unknown event {:?}", other),
            }
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }
    }

    #[test]
    fn test_deregister() {
        let mut suite = mock_endpoint(&CdcConfig::default(), None, ApiVersion::V1);
        suite.add_region(1, 100);
        let quota = crate::channel::MemoryQuota::new(usize::MAX);
        let (tx, mut rx) = channel::channel(1, quota);
        let mut rx = rx.drain();

        let conn = Conn::new(tx, String::new());
        let conn_id = conn.get_id();
        suite.run(Task::OpenConn { conn });
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch.clone(),
            0,
            conn_id,
            ChangeDataRequestKvApi::TiDb,
        );
        let downstream_id = downstream.get_id();
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: semver::Version::new(0, 0, 0),
        });
        assert_eq!(suite.endpoint.capture_regions.len(), 1);

        let mut err_header = ErrorHeader::default();
        err_header.set_not_leader(Default::default());
        let deregister = Deregister::Downstream {
            region_id: 1,
            downstream_id,
            conn_id,
            err: Some(Error::request(err_header.clone())),
        };
        suite.run(Task::Deregister(deregister));
        loop {
            let cdc_event = channel::recv_timeout(&mut rx, Duration::from_millis(500))
                .unwrap()
                .unwrap();
            if let CdcEvent::Event(mut e) = cdc_event.0 {
                let event = e.event.take().unwrap();
                match event {
                    Event_oneof_event::Error(err) => {
                        assert!(err.has_not_leader());
                        break;
                    }
                    other => panic!("unknown event {:?}", other),
                }
            }
        }
        assert_eq!(suite.endpoint.capture_regions.len(), 0);

        let downstream = Downstream::new(
            "".to_string(),
            region_epoch.clone(),
            0,
            conn_id,
            ChangeDataRequestKvApi::TiDb,
        );
        let new_downstream_id = downstream.get_id();
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: semver::Version::new(0, 0, 0),
        });
        assert_eq!(suite.endpoint.capture_regions.len(), 1);

        let deregister = Deregister::Downstream {
            region_id: 1,
            downstream_id,
            conn_id,
            err: Some(Error::request(err_header.clone())),
        };
        suite.run(Task::Deregister(deregister));
        assert!(channel::recv_timeout(&mut rx, Duration::from_millis(200)).is_err());
        assert_eq!(suite.endpoint.capture_regions.len(), 1);

        let deregister = Deregister::Downstream {
            region_id: 1,
            downstream_id: new_downstream_id,
            conn_id,
            err: Some(Error::request(err_header.clone())),
        };
        suite.run(Task::Deregister(deregister));
        let cdc_event = channel::recv_timeout(&mut rx, Duration::from_millis(500))
            .unwrap()
            .unwrap();
        loop {
            if let CdcEvent::Event(mut e) = cdc_event.0 {
                let event = e.event.take().unwrap();
                match event {
                    Event_oneof_event::Error(err) => {
                        assert!(err.has_not_leader());
                        break;
                    }
                    other => panic!("unknown event {:?}", other),
                }
            }
        }
        assert_eq!(suite.endpoint.capture_regions.len(), 0);

        // Stale deregister should be filtered.
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch,
            0,
            conn_id,
            ChangeDataRequestKvApi::TiDb,
        );
        suite.run(Task::Register {
            request: req,
            downstream,
            conn_id,
            version: semver::Version::new(0, 0, 0),
        });
        assert_eq!(suite.endpoint.capture_regions.len(), 1);
        let deregister = Deregister::Delegate {
            region_id: 1,
            // A stale ObserveID (different from the actual one).
            observe_id: ObserveID::new(),
            err: Error::request(err_header),
        };
        suite.run(Task::Deregister(deregister));
        match channel::recv_timeout(&mut rx, Duration::from_millis(500)) {
            Err(_) => (),
            Ok(other) => panic!("unknown event {:?}", other),
        }
        assert_eq!(suite.endpoint.capture_regions.len(), 1);
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
        let quota = channel::MemoryQuota::new(usize::MAX);
        for region_ids in vec![vec![1, 2], vec![3]] {
            let (tx, rx) = channel::channel(1, quota.clone());
            conn_rxs.push(rx);
            let conn = Conn::new(tx, String::new());
            let conn_id = conn.get_id();
            suite.run(Task::OpenConn { conn });

            for region_id in region_ids {
                suite.add_region(region_id, 100);
                let mut req_header = Header::default();
                req_header.set_cluster_id(0);
                let mut req = ChangeDataRequest::default();
                req.set_region_id(region_id);
                let region_epoch = req.get_region_epoch().clone();
                let downstream = Downstream::new(
                    "".to_string(),
                    region_epoch.clone(),
                    0,
                    conn_id,
                    ChangeDataRequestKvApi::TiDb,
                );
                downstream.get_state().store(DownstreamState::Normal);
                suite.run(Task::Register {
                    request: req.clone(),
                    downstream,
                    conn_id,
                    version: FeatureGate::batch_resolved_ts(),
                });
                let resolver = Resolver::new(region_id);
                let observe_id = suite.endpoint.capture_regions[&region_id].handle.id;
                let mut region = Region::default();
                region.set_id(region_id);
                suite.on_region_ready(observe_id, resolver, region);
            }
        }

        let assert_batch_resolved_ts = |drain: &mut channel::Drain,
                                        regions: Vec<u64>,
                                        resolved_ts: u64| {
            let cdc_event = channel::recv_timeout(&mut drain.drain(), Duration::from_millis(500))
                .unwrap()
                .unwrap();
            if let CdcEvent::ResolvedTs(r) = cdc_event.0 {
                assert_eq!(r.regions, regions);
                assert_eq!(r.ts, resolved_ts);
            } else {
                panic!("unknown cdc event {:?}", cdc_event);
            }
        };

        suite.run(Task::MinTS {
            regions: vec![1],
            min_ts: TimeStamp::from(1),
        });
        // conn a must receive a resolved ts that only contains region 1.
        assert_batch_resolved_ts(conn_rxs.get_mut(0).unwrap(), vec![1], 1);
        // conn b must not receive any messages.
        channel::recv_timeout(
            &mut conn_rxs.get_mut(0).unwrap().drain(),
            Duration::from_millis(100),
        )
        .unwrap_err();

        suite.run(Task::MinTS {
            regions: vec![1, 2],
            min_ts: TimeStamp::from(2),
        });
        // conn a must receive a resolved ts that contains region 1 and region 2.
        assert_batch_resolved_ts(conn_rxs.get_mut(0).unwrap(), vec![1, 2], 2);
        // conn b must not receive any messages.
        channel::recv_timeout(
            &mut conn_rxs.get_mut(1).unwrap().drain(),
            Duration::from_millis(100),
        )
        .unwrap_err();

        suite.run(Task::MinTS {
            regions: vec![1, 2, 3],
            min_ts: TimeStamp::from(3),
        });
        // conn a must receive a resolved ts that contains region 1 and region 2.
        assert_batch_resolved_ts(conn_rxs.get_mut(0).unwrap(), vec![1, 2], 3);
        // conn b must receive a resolved ts that contains region 3.
        assert_batch_resolved_ts(conn_rxs.get_mut(1).unwrap(), vec![3], 3);

        suite.run(Task::MinTS {
            regions: vec![1, 3],
            min_ts: TimeStamp::from(4),
        });
        // conn a must receive a resolved ts that only contains region 1.
        assert_batch_resolved_ts(conn_rxs.get_mut(0).unwrap(), vec![1], 4);
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
        let quota = crate::channel::MemoryQuota::new(usize::MAX);

        // Open conn a
        let (tx1, _rx1) = channel::channel(1, quota.clone());
        let conn_a = Conn::new(tx1, String::new());
        let conn_id_a = conn_a.get_id();
        suite.run(Task::OpenConn { conn: conn_a });

        // Open conn b
        let (tx2, mut rx2) = channel::channel(1, quota);
        let mut rx2 = rx2.drain();
        let conn_b = Conn::new(tx2, String::new());
        let conn_id_b = conn_b.get_id();
        suite.run(Task::OpenConn { conn: conn_b });

        // Register region 1 (epoch 2) at conn a.
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        req.mut_region_epoch().set_version(2);
        let region_epoch_2 = req.get_region_epoch().clone();
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch_2.clone(),
            0,
            conn_id_a,
            ChangeDataRequestKvApi::TiDb,
        );
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id: conn_id_a,
            version: semver::Version::new(0, 0, 0),
        });
        assert_eq!(suite.endpoint.capture_regions.len(), 1);
        let observe_id = suite.endpoint.capture_regions[&1].handle.id;

        // Register region 1 (epoch 1) at conn b.
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        req.mut_region_epoch().set_version(1);
        let region_epoch_1 = req.get_region_epoch().clone();
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch_1,
            0,
            conn_id_b,
            ChangeDataRequestKvApi::TiDb,
        );
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id: conn_id_b,
            version: semver::Version::new(0, 0, 0),
        });
        assert_eq!(suite.endpoint.capture_regions.len(), 1);

        // Deregister conn a.
        suite.run(Task::Deregister(Deregister::Conn(conn_id_a)));
        assert_eq!(suite.endpoint.capture_regions.len(), 1);

        // Schedule resolver ready (resolver is built by conn a).
        let mut region = Region::default();
        region.id = 1;
        region.set_region_epoch(region_epoch_2);
        suite.run(Task::ResolverReady {
            observe_id,
            region: region.clone(),
            resolver: Resolver::new(1),
        });

        // Deregister deletgate due to epoch not match for conn b.
        let mut epoch_not_match = ErrorHeader::default();
        epoch_not_match
            .mut_epoch_not_match()
            .mut_current_regions()
            .push(region);
        suite.run(Task::Deregister(Deregister::Delegate {
            region_id: 1,
            observe_id,
            err: Error::request(epoch_not_match),
        }));
        assert_eq!(suite.endpoint.capture_regions.len(), 0);

        let event = recv_timeout(&mut rx2, Duration::from_millis(100))
            .unwrap()
            .unwrap()
            .0;
        assert!(
            event.event().get_error().has_epoch_not_match(),
            "{:?}",
            event
        );
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

        // Empty regions
        let (ts, regions) = heap.to_hash_set();
        assert_eq!(ts, TimeStamp::max());
        assert!(regions.is_empty());

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

        let (ts, regions) = heap1.to_hash_set();
        assert_eq!(ts, 4.into());
        assert_eq!(regions.len(), 3);
        assert!(regions.contains(&4));
        assert!(regions.contains(&5));
        assert!(regions.contains(&6));

        heap1.reset_and_shrink_to(3);
        assert_eq!(3, heap1.heap.capacity());
        assert!(heap1.heap.is_empty());

        heap1.push(1, 1.into());
        heap1.clear();
        assert!(heap1.heap.is_empty());
    }
}
