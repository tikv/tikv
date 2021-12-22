// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use collections::{HashMap, HashSet};
use concurrency_manager::ConcurrencyManager;
use crossbeam::atomic::AtomicCell;
use engine_rocks::PROP_MAX_TS;
use engine_traits::{
    KvEngine, Range, Snapshot as EngineSnapshot, TablePropertiesCollection, TablePropertiesExt,
    UserCollectedProperties, CF_DEFAULT, CF_WRITE,
};
use fail::fail_point;
use futures::compat::Future01CompatExt;
use grpcio::Environment;
use keys::{data_end_key, data_key};
use kvproto::cdcpb::{
    ChangeDataRequest, ClusterIdMismatch as ErrorClusterIdMismatch,
    DuplicateRequest as ErrorDuplicateRequest, Error as EventError, Event, Event_oneof_event,
    ResolvedTs,
};
use kvproto::kvrpcpb::ExtraOp as TxnExtraOp;
use kvproto::metapb::{Region, RegionEpoch};
use kvproto::tikvpb::TikvClient;
use online_config::{ConfigChange, OnlineConfig};
use pd_client::{Feature, PdClient};
use raftstore::coprocessor::CmdBatch;
use raftstore::coprocessor::ObserveID;
use raftstore::router::RaftStoreRouter;
use raftstore::store::fsm::{ChangeObserver, StoreMeta};
use raftstore::store::msg::{Callback, ReadResponse, SignificantMsg};
use raftstore::store::RegionReadProgressRegistry;
use resolved_ts::Resolver;
use security::SecurityManager;
use tikv::config::CdcConfig;
use tikv::storage::kv::{PerfStatisticsInstant, Snapshot};
use tikv::storage::mvcc::{DeltaScanner, ScannerBuilder};
use tikv::storage::txn::{TxnEntry, TxnEntryScanner};
use tikv::storage::Statistics;
use tikv_kv::PerfStatisticsDelta;
use tikv_util::codec::number;
use tikv_util::sys::inspector::{self_thread_inspector, ThreadInspector};
use tikv_util::time::{Instant, Limiter};
use tikv_util::timer::SteadyTimer;
use tikv_util::worker::{Runnable, RunnableWithTimer, ScheduleError, Scheduler};
use tikv_util::{box_err, debug, error, impl_display_as_debug, info, warn, Either};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{Mutex, Semaphore};
use txn_types::{Key, Lock, LockType, OldValue, TimeStamp, TxnExtra, TxnExtraScheduler};

use crate::channel::{CdcEvent, MemoryQuota, SendError};
use crate::delegate::{Delegate, Downstream, DownstreamID, DownstreamState};
use crate::metrics::*;
use crate::old_value::{
    near_seek_old_value, new_old_value_cursor, OldValueCache, OldValueCallback, OldValueCursors,
};
use crate::service::{Conn, ConnID, FeatureGate};
use crate::{CdcObserver, Error, Result};

const FEATURE_RESOLVED_TS_STORE: Feature = Feature::require(5, 0, 0);

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
                .field("type", &"multibatch")
                .field("multibatch", &multi.len())
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
                ref downstream_id, ..
            } => de
                .field("type", &"init_downstream")
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

const METRICS_FLUSH_INTERVAL: u64 = 10_000; // 10s

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
    workers: Runtime,
    scan_concurrency_semaphore: Arc<Semaphore>,

    scan_speed_limiter: Limiter,
    max_scan_batch_bytes: usize,
    max_scan_batch_size: usize,

    min_resolved_ts: TimeStamp,
    min_ts_region_id: u64,
    old_value_cache: OldValueCache,

    // stats
    resolved_region_count: usize,
    unresolved_region_count: usize,

    sink_memory_quota: MemoryQuota,

    // store_id -> client
    tikv_clients: Arc<Mutex<HashMap<u64, TikvClient>>>,
    env: Arc<Environment>,
    security_mgr: Arc<SecurityManager>,
    region_read_progress: RegionReadProgressRegistry,
}

impl<T: 'static + RaftStoreRouter<E>, E: KvEngine> Endpoint<T, E> {
    pub fn new(
        cluster_id: u64,
        config: &CdcConfig,
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
            .build()
            .unwrap();
        let tso_worker = Builder::new_multi_thread()
            .thread_name("tso")
            .worker_threads(1)
            .enable_time()
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
            workers,
            scan_concurrency_semaphore,
            raft_router,
            engine,
            observer,
            store_meta,
            concurrency_manager,
            min_resolved_ts: TimeStamp::max(),
            min_ts_region_id: 0,
            old_value_cache,
            resolved_region_count: 0,
            unresolved_region_count: 0,
            sink_memory_quota,
            tikv_clients: Arc::new(Mutex::new(HashMap::default())),
            region_read_progress,
        };
        ep.register_min_ts_event();
        ep
    }

    fn on_change_cfg(&mut self, change: ConfigChange) {
        // Validate first.
        let mut validate_cfg = self.config.clone();
        validate_cfg.update(change.clone());
        if let Err(e) = validate_cfg.validate() {
            warn!("cdc config update failed"; "error" => ?e);
            return;
        }

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
                    if let Some(reader) = self.store_meta.lock().unwrap().readers.get(&region_id) {
                        reader.txn_extra_op.store(TxnExtraOp::Noop);
                    }
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
                    if let Some(reader) = self.store_meta.lock().unwrap().readers.get(&region_id) {
                        reader.txn_extra_op.store(TxnExtraOp::Noop);
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
                                if delegate.unsubscribe(downstream_id, None) {
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
        let downstream_id = downstream.get_id();
        let downstream_state = downstream.get_state();
        let conn = match self.connections.get_mut(&conn_id) {
            Some(conn) => conn,
            None => {
                error!("cdc register for a nonexistent connection";
                    "region_id" => region_id, "conn_id" => ?conn_id);
                return;
            }
        };
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
        let delegate = self.capture_regions.entry(region_id).or_insert_with(|| {
            let d = Delegate::new(region_id);
            is_new_delegate = true;
            d
        });
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
        let txn_extra_op = request.get_extra_op();
        if txn_extra_op != TxnExtraOp::Noop {
            delegate.txn_extra_op = request.get_extra_op();
            if let Some(reader) = self.store_meta.lock().unwrap().readers.get(&region_id) {
                reader.txn_extra_op.store(txn_extra_op);
            }
        }
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
            txn_extra_op: delegate.txn_extra_op,
            speed_limiter: self.scan_speed_limiter.clone(),
            max_scan_batch_bytes: self.max_scan_batch_bytes,
            max_scan_batch_size: self.max_scan_batch_size,
            observe_id,
            checkpoint_ts: checkpoint_ts.into(),
            build_resolver: is_new_delegate,
            ts_filter_ratio: self.config.incremental_scan_ts_filter_ratio,
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
                for downstream in delegate.on_region_ready(resolver, region) {
                    let conn_id = downstream.get_conn_id();
                    let downstream_id = downstream.get_id();
                    if let Err(err) = delegate.subscribe(downstream) {
                        failed_downstreams.push(Deregister::Downstream {
                            region_id,
                            downstream_id,
                            conn_id,
                            err: Some(err),
                        });
                    }
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
        let total_region_count = regions.len();
        // TODO: figure out how to avoid create a hashset every time,
        //       saving some CPU.
        let mut resolved_regions =
            HashSet::with_capacity_and_hasher(regions.len(), Default::default());
        self.min_resolved_ts = TimeStamp::max();
        for region_id in regions {
            if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
                if let Some(resolved_ts) = delegate.on_min_ts(min_ts) {
                    if resolved_ts < self.min_resolved_ts {
                        self.min_resolved_ts = resolved_ts;
                        self.min_ts_region_id = region_id;
                    }
                    resolved_regions.insert(region_id);
                }
            }
        }
        self.resolved_region_count = resolved_regions.len();
        self.unresolved_region_count = total_region_count - self.resolved_region_count;
        self.broadcast_resolved_ts(resolved_regions);
    }

    fn broadcast_resolved_ts(&self, regions: HashSet<u64>) {
        let min_resolved_ts = self.min_resolved_ts.into_inner();
        let send_cdc_event = |regions: &HashSet<u64>, min_resolved_ts: u64, conn: &Conn| {
            let downstream_regions = conn.get_downstreams();
            let mut resolved_ts = ResolvedTs::default();
            resolved_ts.ts = min_resolved_ts;
            resolved_ts.regions = Vec::with_capacity(downstream_regions.len());
            // Only send region ids that are captured by the connection.
            for (region_id, (_, downstream_state)) in conn.get_downstreams() {
                if regions.contains(region_id)
                    && matches!(downstream_state.load(), DownstreamState::Normal)
                {
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
            let mut min_ts = pd_client.get_tso().await.unwrap_or_default();

            // Sync with concurrency manager so that it can work correctly when optimizations
            // like async commit is enabled.
            // Note: This step must be done before scheduling `Task::MinTS` task, and the
            // resolver must be checked in or after `Task::MinTS`' execution.
            cm.update_max_ts(min_ts);
            if let Some(min_mem_lock_ts) = cm.global_min_lock_ts() {
                if min_mem_lock_ts < min_ts {
                    min_ts = min_mem_lock_ts;
                }
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

struct Initializer<E> {
    engine: E,
    sched: Scheduler<Task>,
    sink: crate::channel::Sink,

    region_id: u64,
    region_epoch: RegionEpoch,
    observe_id: ObserveID,
    downstream_id: DownstreamID,
    downstream_state: Arc<AtomicCell<DownstreamState>>,
    conn_id: ConnID,
    request_id: u64,
    checkpoint_ts: TimeStamp,
    txn_extra_op: TxnExtraOp,

    speed_limiter: Limiter,
    max_scan_batch_bytes: usize,
    max_scan_batch_size: usize,

    build_resolver: bool,
    ts_filter_ratio: f64,
}

impl<E: KvEngine> Initializer<E> {
    async fn initialize<T: 'static + RaftStoreRouter<E>>(
        &mut self,
        change_cmd: ChangeObserver,
        raft_router: T,
        concurrency_semaphore: Arc<Semaphore>,
    ) -> Result<()> {
        fail_point!("cdc_before_initialize");
        let _permit = concurrency_semaphore.acquire().await;

        // When downstream_state is Stopped, it means the corresponding delegate
        // is stopped. The initialization can be safely canceled.
        //
        // Acquiring a permit may take some time, it is possiable that
        // initialization can be canceled.
        if self.downstream_state.load() == DownstreamState::Stopped {
            info!("cdc async incremental scan canceled";
                "region_id" => self.region_id,
                "downstream_id" => ?self.downstream_id,
                "observe_id" => ?self.observe_id,
                "conn_id" => ?self.conn_id);
            return Err(box_err!("scan canceled"));
        }

        CDC_SCAN_TASKS.with_label_values(&["ongoing"]).inc();
        tikv_util::defer!({
            CDC_SCAN_TASKS.with_label_values(&["ongoing"]).dec();
        });

        // To avoid holding too many snapshots and holding them too long,
        // we need to acquire scan concurrency permit before taking snapshot.
        let sched = self.sched.clone();
        let region_epoch = self.region_epoch.clone();
        let downstream_id = self.downstream_id;
        let downstream_state = self.downstream_state.clone();
        let (cb, fut) = tikv_util::future::paired_future_callback();
        let sink = self.sink.clone();
        let (incremental_scan_barrier_cb, incremental_scan_barrier_fut) =
            tikv_util::future::paired_future_callback();
        let barrier = CdcEvent::Barrier(Some(incremental_scan_barrier_cb));
        if let Err(e) = raft_router.significant_send(
            self.region_id,
            SignificantMsg::CaptureChange {
                cmd: change_cmd,
                region_epoch,
                callback: Callback::Read(Box::new(move |resp| {
                    if let Err(e) = sched.schedule(Task::InitDownstream {
                        downstream_id,
                        downstream_state,
                        sink,
                        incremental_scan_barrier: barrier,
                        cb: Box::new(move || cb(resp)),
                    }) {
                        error!("cdc schedule cdc task failed"; "error" => ?e);
                    }
                })),
            },
        ) {
            warn!("cdc send capture change cmd failed";
            "region_id" => self.region_id, "error" => ?e);
            return Err(Error::request(e.into()));
        }

        // Wait all delta changes earlier than the incremental scan snapshot be
        // sent to the downstream, so that they must be consumed before the
        // incremental scan result.
        if let Err(e) = incremental_scan_barrier_fut.await {
            return Err(Error::Other(box_err!(e)));
        }

        match fut.await {
            Ok(resp) => self.on_change_cmd_response(resp).await,
            Err(e) => Err(Error::Other(box_err!(e))),
        }
    }

    async fn on_change_cmd_response(
        &mut self,
        mut resp: ReadResponse<impl EngineSnapshot>,
    ) -> Result<()> {
        if let Some(region_snapshot) = resp.snapshot {
            assert_eq!(self.region_id, region_snapshot.get_region().get_id());
            let region = region_snapshot.get_region().clone();
            self.async_incremental_scan(region_snapshot, region).await
        } else {
            assert!(
                resp.response.get_header().has_error(),
                "no snapshot and no error? {:?}",
                resp.response
            );
            let err = resp.response.take_header().take_error();
            Err(Error::request(err))
        }
    }

    async fn async_incremental_scan<S: Snapshot + 'static>(
        &mut self,
        snap: S,
        region: Region,
    ) -> Result<()> {
        let downstream_id = self.downstream_id;
        let region_id = region.get_id();
        debug!("cdc async incremental scan";
            "region_id" => region_id,
            "downstream_id" => ?downstream_id,
            "observe_id" => ?self.observe_id,
            "start_key" => log_wrappers::Value::key(snap.lower_bound().unwrap_or_default()),
            "end_key" => log_wrappers::Value::key(snap.upper_bound().unwrap_or_default()));

        let mut resolver = if self.build_resolver {
            Some(Resolver::new(region_id))
        } else {
            None
        };

        let (mut hint_min_ts, mut old_value_cursors) = (None, None);
        if self.txn_extra_op == TxnExtraOp::Noop {
            hint_min_ts = Some(self.checkpoint_ts);
        } else if self.ts_filter_is_helpful(&snap) {
            hint_min_ts = Some(self.checkpoint_ts);
            let wc = new_old_value_cursor(&snap, CF_WRITE);
            let dc = new_old_value_cursor(&snap, CF_DEFAULT);
            old_value_cursors = Some(OldValueCursors::new(wc, dc));
        }

        // Time range: (checkpoint_ts, max]
        let mut scanner = ScannerBuilder::new(snap, TimeStamp::max())
            .fill_cache(false)
            .range(None, None)
            .hint_min_ts(hint_min_ts)
            .build_delta_scanner(self.checkpoint_ts, self.txn_extra_op)
            .unwrap();

        fail_point!("cdc_incremental_scan_start");
        let conn_id = self.conn_id;
        let mut done = false;
        let start = Instant::now_coarse();
        while !done {
            // When downstream_state is Stopped, it means the corresponding
            // delegate is stopped. The initialization can be safely canceled.
            if self.downstream_state.load() == DownstreamState::Stopped {
                info!("cdc async incremental scan canceled";
                    "region_id" => region_id,
                    "downstream_id" => ?downstream_id,
                    "observe_id" => ?self.observe_id,
                    "conn_id" => ?conn_id);
                return Err(box_err!("scan canceled"));
            }
            let cursors = old_value_cursors.as_mut();
            let resolver = resolver.as_mut();
            let entries = self.scan_batch(&mut scanner, cursors, resolver).await?;
            if let Some(None) = entries.last() {
                // If the last element is None, it means scanning is finished.
                done = true;
            }
            debug!("cdc scan entries"; "len" => entries.len(), "region_id" => region_id);
            fail_point!("before_schedule_incremental_scan");
            self.sink_scan_events(entries, done).await?;
        }

        let takes = start.saturating_elapsed();
        if let Some(resolver) = resolver {
            self.finish_building_resolver(resolver, region, takes);
        }

        CDC_SCAN_DURATION_HISTOGRAM.observe(takes.as_secs_f64());
        Ok(())
    }

    // It's extracted from `Initializer::scan_batch` to avoid becoming an asynchronous block,
    // so that we can limit scan speed based on the thread disk I/O or RocksDB block read bytes.
    fn do_scan<S: Snapshot>(
        &self,
        scanner: &mut DeltaScanner<S>,
        mut old_value_cursors: Option<&mut OldValueCursors<S::Iter>>,
        entries: &mut Vec<Option<TxnEntry>>,
    ) -> Result<ScanStat> {
        let mut read_old_value = |v: &mut OldValue, stats: &mut Statistics| -> Result<()> {
            let (wc, dc) = match old_value_cursors {
                Some(ref mut x) => (&mut x.write, &mut x.default),
                None => return Ok(()),
            };
            if let OldValue::SeekWrite(ref key) = v {
                match near_seek_old_value(key, wc, Either::<&S, _>::Right(dc), stats)? {
                    Some(x) => *v = OldValue::value(x),
                    None => *v = OldValue::None,
                }
            }
            Ok(())
        };

        // This code block shouldn't be switched to other threads.
        let mut total_bytes = 0;
        let mut total_size = 0;
        let perf_instant = PerfStatisticsInstant::new();
        let inspector = self_thread_inspector().ok();
        let old_io_stat = inspector.as_ref().and_then(|x| x.io_stat().unwrap_or(None));
        let mut stats = Statistics::default();
        while total_bytes <= self.max_scan_batch_bytes && total_size < self.max_scan_batch_size {
            total_size += 1;
            match scanner.next_entry()? {
                Some(mut entry) => {
                    read_old_value(entry.old_value(), &mut stats)?;
                    total_bytes += entry.size();
                    entries.push(Some(entry));
                }
                None => {
                    entries.push(None);
                    break;
                }
            }
        }
        flush_oldvalue_stats(&stats, TAG_INCREMENTAL_SCAN);
        let new_io_stat = inspector.as_ref().and_then(|x| x.io_stat().unwrap_or(None));
        let disk_read = match (old_io_stat, new_io_stat) {
            (Some(s1), Some(s2)) => Some((s2.read - s1.read) as usize),
            _ => None,
        };
        let perf_delta = perf_instant.delta();
        let emit = total_bytes;
        Ok(ScanStat {
            emit,
            disk_read,
            perf_delta,
        })
    }

    async fn scan_batch<S: Snapshot>(
        &self,
        scanner: &mut DeltaScanner<S>,
        old_value_cursors: Option<&mut OldValueCursors<S::Iter>>,
        resolver: Option<&mut Resolver>,
    ) -> Result<Vec<Option<TxnEntry>>> {
        let mut entries = Vec::with_capacity(self.max_scan_batch_size);
        let ScanStat {
            emit,
            disk_read,
            perf_delta,
        } = self.do_scan(scanner, old_value_cursors, &mut entries)?;

        CDC_SCAN_BYTES.inc_by(emit as _);
        TLS_CDC_PERF_STATS.with(|x| *x.borrow_mut() += perf_delta);
        tls_flush_perf_stats();
        let require = if let Some(bytes) = disk_read {
            CDC_SCAN_DISK_READ_BYTES.inc_by(bytes as _);
            bytes
        } else {
            perf_delta.0.block_read_byte
        };
        self.speed_limiter.consume(require).await;

        if let Some(resolver) = resolver {
            // Track the locks.
            for entry in entries.iter().flatten() {
                if let TxnEntry::Prewrite { ref lock, .. } = entry {
                    let (encoded_key, value) = lock;
                    let key = Key::from_encoded_slice(encoded_key).into_raw().unwrap();
                    let lock = Lock::parse(value)?;
                    match lock.lock_type {
                        LockType::Put | LockType::Delete => resolver.track_lock(lock.ts, key, None),
                        _ => (),
                    };
                }
            }
        }
        Ok(entries)
    }

    async fn sink_scan_events(&mut self, entries: Vec<Option<TxnEntry>>, done: bool) -> Result<()> {
        let mut barrier = None;
        let mut events = Delegate::convert_to_grpc_events(self.region_id, self.request_id, entries);
        if done {
            let (cb, fut) = tikv_util::future::paired_future_callback();
            events.push(CdcEvent::Barrier(Some(cb)));
            barrier = Some(fut);
        }
        if let Err(e) = self.sink.send_all(events).await {
            error!("cdc send scan event failed"; "req_id" => ?self.request_id);
            return Err(Error::Sink(e));
        }

        if let Some(barrier) = barrier {
            // CDC needs to make sure resovled ts events can only be sent after
            // incremental scan is finished.
            // Wait the barrier to ensure tikv sends out all scan events.
            let _ = barrier.await;
        }

        Ok(())
    }

    fn finish_building_resolver(&self, mut resolver: Resolver, region: Region, takes: Duration) {
        let observe_id = self.observe_id;
        let rts = resolver.resolve(TimeStamp::zero());
        info!(
            "cdc resolver initialized and schedule resolver ready";
            "region_id" => region.get_id(),
            "conn_id" => ?self.conn_id,
            "downstream_id" => ?self.downstream_id,
            "resolved_ts" => rts,
            "lock_count" => resolver.locks().len(),
            "observe_id" => ?observe_id,
            "takes" => ?takes,
        );

        fail_point!("before_schedule_resolver_ready");
        if let Err(e) = self.sched.schedule(Task::ResolverReady {
            observe_id,
            resolver,
            region,
        }) {
            error!("cdc schedule task failed"; "error" => ?e);
        }
    }

    // Deregister downstream when the Initializer fails to initialize.
    fn deregister_downstream(&self, err: Error) {
        let deregister = if self.build_resolver || err.has_region_error() {
            // Deregister delegate on the conditions,
            // * It fails to build a resolver. A delegate requires a resolver
            //   to advance resolved ts.
            // * A region error. It usually mean a peer is not leader or
            //   a leader meets an error and can not serve.
            Deregister::Delegate {
                region_id: self.region_id,
                observe_id: self.observe_id,
                err,
            }
        } else {
            Deregister::Downstream {
                region_id: self.region_id,
                downstream_id: self.downstream_id,
                conn_id: self.conn_id,
                err: Some(err),
            }
        };

        if let Err(e) = self.sched.schedule(Task::Deregister(deregister)) {
            error!("cdc schedule cdc task failed"; "error" => ?e);
        }
    }

    fn ts_filter_is_helpful<S: Snapshot>(&self, snap: &S) -> bool {
        if self.ts_filter_ratio < f64::EPSILON {
            return false;
        }

        let start_key = data_key(snap.lower_bound().unwrap_or_default());
        let end_key = data_end_key(snap.upper_bound().unwrap_or_default());
        let range = Range::new(&start_key, &end_key);
        let collection = match self.engine.table_properties_collection(CF_WRITE, &[range]) {
            Ok(collection) => collection,
            Err(_) => return false,
        };

        let hint_min_ts = self.checkpoint_ts.into_inner();
        let (mut total_count, mut filtered_count, mut tables) = (0, 0, 0);
        collection.iter_user_collected_properties(|prop| {
            tables += 1;
            if let Some((_, keys)) = prop.approximate_size_and_keys(&start_key, &end_key) {
                total_count += keys;
                if Self::parse_u64_prop(prop, PROP_MAX_TS)
                    .map_or(false, |max_ts| max_ts < hint_min_ts)
                {
                    filtered_count += keys;
                }
            }
            true
        });

        let valid_count = total_count - filtered_count;
        let use_ts_filter = valid_count as f64 / total_count as f64 <= self.ts_filter_ratio;
        info!("cdc incremental scan uses ts filter: {}", use_ts_filter;
            "region_id" => self.region_id,
            "hint_min_ts" => hint_min_ts,
            "mvcc_versions" => total_count,
            "filtered_versions" => filtered_count,
            "tables" => tables);
        use_ts_filter
    }

    fn parse_u64_prop(
        prop: &<<E as TablePropertiesExt>::TablePropertiesCollection as TablePropertiesCollection>::UserCollectedProperties,
        field: &str,
    ) -> Option<u64> {
        prop.get(field.as_bytes())
            .and_then(|mut x| number::decode_u64(&mut x).ok())
    }
}

#[derive(Clone, Copy, Debug)]
struct ScanStat {
    // Fetched bytes to the scanner.
    emit: usize,
    // Bytes from the device, `None` if not possible to get it.
    disk_read: Option<usize>,
    // Perf delta for RocksDB.
    perf_delta: PerfStatisticsDelta,
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
                downstream_id,
                downstream_state,
                sink,
                incremental_scan_barrier,
                cb,
            } => {
                if let Err(e) = sink.unbounded_send(incremental_scan_barrier, true) {
                    error!(
                        "cdc failed to schedule barrier for delta before delta scan";
                        "error" => ?e
                    );
                    return;
                }
                match downstream_state
                    .compare_exchange(DownstreamState::Uninitialized, DownstreamState::Normal)
                {
                    Ok(_) => {
                        info!("cdc downstream is initialized"; "downstream_id" => ?downstream_id);
                    }
                    Err(state) => {
                        warn!("cdc downstream fails to initialize";
                            "downstream_id" => ?downstream_id,
                            "state" => ?state);
                    }
                }
                cb();
            }
            Task::TxnExtra(txn_extra) => {
                for (k, v) in txn_extra.old_values {
                    self.old_value_cache.cache.insert(k, v);
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

        let cache_size = self.old_value_cache.cache.size();
        CDC_OLD_VALUE_CACHE_BYTES.set(cache_size as i64);
        CDC_OLD_VALUE_CACHE_ACCESS.add(self.old_value_cache.access_count as i64);
        CDC_OLD_VALUE_CACHE_MISS.add(self.old_value_cache.miss_count as i64);
        CDC_OLD_VALUE_CACHE_MISS_NONE.add(self.old_value_cache.miss_none_count as i64);
        CDC_OLD_VALUE_CACHE_LEN.set(self.old_value_cache.cache.len() as i64);
        self.old_value_cache.access_count = 0;
        self.old_value_cache.miss_count = 0;
        self.old_value_cache.miss_none_count = 0;
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
    use std::collections::BTreeMap;
    use std::fmt::Display;
    use std::ops::{Deref, DerefMut};
    use std::sync::mpsc::{channel, sync_channel, Receiver, RecvTimeoutError, Sender};

    use collections::HashSet;
    use engine_rocks::RocksEngine;
    use engine_traits::{MiscExt, CF_WRITE};
    use futures::executor::block_on;
    use futures::StreamExt;
    use kvproto::cdcpb::Header;
    use kvproto::errorpb::Error as ErrorHeader;
    use raftstore::coprocessor::ObserveHandle;
    use raftstore::errors::{DiscardReason, Error as RaftStoreError};
    use raftstore::store::msg::CasualMessage;
    use raftstore::store::util::RegionReadProgress;
    use raftstore::store::{PeerMsg, ReadDelegate, RegionSnapshot, TrackVer};
    use test_raftstore::{MockRaftStoreRouter, TestPdClient};
    use tikv::server::DEFAULT_CLUSTER_ID;
    use tikv::storage::kv::Engine;
    use tikv::storage::txn::tests::{
        must_acquire_pessimistic_lock, must_commit, must_prewrite_delete, must_prewrite_put,
    };
    use tikv::storage::TestEngineBuilder;
    use tikv_util::config::{ReadableDuration, ReadableSize};
    use tikv_util::worker::{dummy_scheduler, LazyWorker, ReceiverWrapper};
    use time::Timespec;

    use super::*;
    use crate::{channel, recv_timeout};

    struct ReceiverRunnable<T: Display + Send> {
        tx: Sender<T>,
    }

    impl<T: Display + Send + 'static> Runnable for ReceiverRunnable<T> {
        type Task = T;

        fn run(&mut self, task: T) {
            let _ = self.tx.send(task);
        }
    }

    fn new_receiver_worker<T: Display + Send + 'static>() -> (LazyWorker<T>, Receiver<T>) {
        let (tx, rx) = channel();
        let runnable = ReceiverRunnable { tx };
        let mut worker = LazyWorker::new("test-receiver-worker");
        worker.start(runnable);
        (worker, rx)
    }

    fn mock_initializer(
        speed_limit: usize,
        buffer: usize,
        engine: Option<RocksEngine>,
    ) -> (
        LazyWorker<Task>,
        Runtime,
        Initializer<RocksEngine>,
        Receiver<Task>,
        crate::channel::Drain,
    ) {
        let (receiver_worker, rx) = new_receiver_worker();
        let quota = crate::channel::MemoryQuota::new(usize::MAX);
        let (sink, drain) = crate::channel::channel(buffer, quota);

        let pool = Builder::new_multi_thread()
            .thread_name("test-initializer-worker")
            .worker_threads(4)
            .build()
            .unwrap();
        let downstream_state = Arc::new(AtomicCell::new(DownstreamState::Normal));
        let initializer = Initializer {
            engine: engine.unwrap_or_else(|| {
                TestEngineBuilder::new()
                    .build_without_cache()
                    .unwrap()
                    .kv_engine()
            }),
            sched: receiver_worker.scheduler(),
            sink,

            region_id: 1,
            region_epoch: RegionEpoch::default(),
            observe_id: ObserveID::new(),
            downstream_id: DownstreamID::new(),
            downstream_state,
            conn_id: ConnID::new(),
            request_id: 0,
            checkpoint_ts: 1.into(),
            speed_limiter: Limiter::new(speed_limit as _),
            max_scan_batch_bytes: 1024 * 1024,
            max_scan_batch_size: 1024,
            txn_extra_op: TxnExtraOp::Noop,
            build_resolver: true,
            ts_filter_ratio: 1.0, // always enable it.
        };

        (receiver_worker, pool, initializer, rx, drain)
    }

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

    fn mock_endpoint(cfg: &CdcConfig, engine: Option<RocksEngine>) -> TestEndpointSuite {
        let mut region = Region::default();
        region.set_id(1);
        let store_meta = Arc::new(StdMutex::new(StoreMeta::new(0)));
        let read_delegate = ReadDelegate {
            tag: String::new(),
            region: Arc::new(region),
            peer_id: 2,
            term: 1,
            applied_index_term: 1,
            leader_lease: None,
            last_valid_ts: Timespec::new(0, 0),
            txn_extra_op: Arc::new(AtomicCell::new(TxnExtraOp::default())),
            txn_ext: Arc::new(Default::default()),
            track_ver: TrackVer::new(),
            read_progress: Arc::new(RegionReadProgress::new(
                &Region::default(),
                0,
                0,
                "".to_owned(),
            )),
        };
        store_meta.lock().unwrap().readers.insert(1, read_delegate);
        let (task_sched, task_rx) = dummy_scheduler();
        let raft_router = MockRaftStoreRouter::new();
        let observer = CdcObserver::new(task_sched.clone());
        let pd_client = Arc::new(TestPdClient::new(0, true));
        let env = Arc::new(Environment::new(1));
        let security_mgr = Arc::new(SecurityManager::default());
        let ep = Endpoint::new(
            DEFAULT_CLUSTER_ID,
            cfg,
            pd_client,
            task_sched,
            raft_router.clone(),
            engine.unwrap_or_else(|| {
                TestEngineBuilder::new()
                    .build_without_cache()
                    .unwrap()
                    .kv_engine()
            }),
            observer,
            store_meta,
            ConcurrencyManager::new(1.into()),
            env,
            security_mgr,
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
    fn test_initializer_build_resolver() {
        let engine = TestEngineBuilder::new().build_without_cache().unwrap();

        let mut expected_locks = BTreeMap::<TimeStamp, HashSet<Arc<[u8]>>>::new();

        let mut total_bytes = 0;
        // Pessimistic locks should not be tracked
        for i in 0..10 {
            let k = &[b'k', i];
            total_bytes += k.len();
            let ts = TimeStamp::new(i as _);
            must_acquire_pessimistic_lock(&engine, k, k, ts, ts);
        }

        for i in 10..100 {
            let (k, v) = (&[b'k', i], &[b'v', i]);
            total_bytes += k.len();
            total_bytes += v.len();
            let ts = TimeStamp::new(i as _);
            must_prewrite_put(&engine, k, v, k, ts);
            expected_locks
                .entry(ts)
                .or_default()
                .insert(k.to_vec().into());
        }

        let region = Region::default();
        let snap = engine.snapshot(Default::default()).unwrap();
        // Buffer must be large enough to unblock async incremental scan.
        let buffer = 1000;
        let (mut worker, pool, mut initializer, rx, mut drain) =
            mock_initializer(total_bytes, buffer, Some(engine.kv_engine()));
        let check_result = || loop {
            let task = rx.recv().unwrap();
            match task {
                Task::ResolverReady { resolver, .. } => {
                    assert_eq!(resolver.locks(), &expected_locks);
                    return;
                }
                t => panic!("unexpected task {} received", t),
            }
        };
        // To not block test by barrier.
        pool.spawn(async move {
            let mut d = drain.drain();
            while d.next().await.is_some() {}
        });

        block_on(initializer.async_incremental_scan(snap.clone(), region.clone())).unwrap();
        check_result();

        initializer.max_scan_batch_bytes = total_bytes;
        block_on(initializer.async_incremental_scan(snap.clone(), region.clone())).unwrap();
        check_result();

        initializer.build_resolver = false;
        block_on(initializer.async_incremental_scan(snap.clone(), region.clone())).unwrap();

        loop {
            let task = rx.recv_timeout(Duration::from_millis(100));
            match task {
                Ok(t) => panic!("unexpected task {} received", t),
                Err(RecvTimeoutError::Timeout) => break,
                Err(e) => panic!("unexpected err {:?}", e),
            }
        }

        // Test cancellation.
        initializer.downstream_state.store(DownstreamState::Stopped);
        block_on(initializer.async_incremental_scan(snap.clone(), region)).unwrap_err();

        // Cancel error should trigger a deregsiter.
        let mut region = Region::default();
        region.set_id(initializer.region_id);
        region.mut_peers().push(Default::default());
        let snapshot = Some(RegionSnapshot::from_snapshot(snap, Arc::new(region)));
        let resp = ReadResponse {
            snapshot,
            response: Default::default(),
            txn_extra_op: Default::default(),
        };
        block_on(initializer.on_change_cmd_response(resp.clone())).unwrap_err();

        // Disconnect sink by dropping runtime (it also drops drain).
        drop(pool);
        initializer.downstream_state.store(DownstreamState::Normal);
        block_on(initializer.on_change_cmd_response(resp)).unwrap_err();

        worker.stop();
    }

    // Test `hint_min_ts` works fine with `ExtraOp::ReadOldValue`.
    // Whether `DeltaScanner` emits correct old values or not is already tested by
    // another case `test_old_value_with_hint_min_ts`, so here we only care about
    // hanlding `OldValue::SeekWrite` with `OldValueReader`.
    #[test]
    fn test_incremental_scanner_with_hint_min_ts() {
        let engine = TestEngineBuilder::new().build_without_cache().unwrap();

        let v_suffix = |suffix: usize| -> Vec<u8> {
            let suffix = suffix.to_string().into_bytes();
            let mut v = Vec::with_capacity(1000 + suffix.len());
            (0..100).for_each(|_| v.extend_from_slice(b"vvvvvvvvvv"));
            v.extend_from_slice(&suffix);
            v
        };

        let check_handling_old_value_seek_write = || {
            // Do incremental scan with different `hint_min_ts` values.
            for checkpoint_ts in [200, 100, 150] {
                let (mut worker, pool, mut initializer, _rx, mut drain) =
                    mock_initializer(usize::MAX, 1000, Some(engine.kv_engine()));
                initializer.txn_extra_op = TxnExtraOp::ReadOldValue;
                initializer.checkpoint_ts = checkpoint_ts.into();
                let mut drain = drain.drain();

                let snap = engine.snapshot(Default::default()).unwrap();
                let th = pool.spawn(async move {
                    initializer
                        .async_incremental_scan(snap, Region::default())
                        .await
                        .unwrap();
                });

                while let Some((event, _)) = block_on(drain.next()) {
                    let event = match event {
                        CdcEvent::Event(x) if x.event.is_some() => x.event.unwrap(),
                        _ => continue,
                    };
                    let entries = match event {
                        Event_oneof_event::Entries(mut x) => x.take_entries().into_vec(),
                        _ => continue,
                    };
                    for entry in entries.into_iter().filter(|x| x.start_ts == 200) {
                        // Check old value is expected in all cases.
                        assert_eq!(entry.get_old_value(), &v_suffix(100));
                    }
                }
                block_on(th).unwrap();
                worker.stop();
            }
        };

        // Create the initial data with CF_WRITE L0: |zkey_110, zkey1_160|
        must_prewrite_put(&engine, b"zkey", &v_suffix(100), b"zkey", 100);
        must_commit(&engine, b"zkey", 100, 110);
        must_prewrite_put(&engine, b"zzzz", &v_suffix(150), b"zzzz", 150);
        must_commit(&engine, b"zzzz", 150, 160);
        engine.kv_engine().flush_cf(CF_WRITE, true).unwrap();
        must_prewrite_delete(&engine, b"zkey", b"zkey", 200);
        check_handling_old_value_seek_write(); // For TxnEntry::Prewrite.

        // CF_WRITE L0: |zkey_110, zkey1_160|, |zkey_210|
        must_commit(&engine, b"zkey", 200, 210);
        engine.kv_engine().flush_cf(CF_WRITE, false).unwrap();
        check_handling_old_value_seek_write(); // For TxnEntry::Commit.
    }

    #[test]
    fn test_initializer_deregister_downstream() {
        let total_bytes = 1;
        let buffer = 1;
        let (mut worker, _pool, mut initializer, rx, _drain) =
            mock_initializer(total_bytes, buffer, None);

        // Errors reported by region should deregister region.
        initializer.build_resolver = false;
        initializer.deregister_downstream(Error::request(ErrorHeader::default()));
        let task = rx.recv_timeout(Duration::from_millis(100));
        match task {
            Ok(Task::Deregister(Deregister::Delegate { region_id, .. })) => {
                assert_eq!(region_id, initializer.region_id);
            }
            Ok(other) => panic!("unexpected task {:?}", other),
            Err(e) => panic!("unexpected err {:?}", e),
        }

        initializer.build_resolver = false;
        initializer.deregister_downstream(Error::Other(box_err!("test")));
        let task = rx.recv_timeout(Duration::from_millis(100));
        match task {
            Ok(Task::Deregister(Deregister::Downstream { region_id, .. })) => {
                assert_eq!(region_id, initializer.region_id);
            }
            Ok(other) => panic!("unexpected task {:?}", other),
            Err(e) => panic!("unexpected err {:?}", e),
        }

        // Test deregister region when resolver fails to build.
        initializer.build_resolver = true;
        initializer.deregister_downstream(Error::Other(box_err!("test")));
        let task = rx.recv_timeout(Duration::from_millis(100));
        match task {
            Ok(Task::Deregister(Deregister::Delegate { region_id, .. })) => {
                assert_eq!(region_id, initializer.region_id);
            }
            Ok(other) => panic!("unexpected task {:?}", other),
            Err(e) => panic!("unexpected err {:?}", e),
        }

        worker.stop();
    }

    #[test]
    fn test_initializer_initialize() {
        let total_bytes = 1;
        let buffer = 1;
        let (mut worker, pool, mut initializer, _rx, _drain) =
            mock_initializer(total_bytes, buffer, None);

        let change_cmd = ChangeObserver::from_cdc(1, ObserveHandle::new());
        let raft_router = MockRaftStoreRouter::new();
        let concurrency_semaphore = Arc::new(Semaphore::new(1));

        initializer.downstream_state.store(DownstreamState::Stopped);
        block_on(initializer.initialize(
            change_cmd,
            raft_router.clone(),
            concurrency_semaphore.clone(),
        ))
        .unwrap_err();

        let (tx, rx) = sync_channel(1);
        let concurrency_semaphore_ = concurrency_semaphore.clone();
        pool.spawn(async move {
            let _permit = concurrency_semaphore_.acquire().await;
            tx.send(()).unwrap();
            tx.send(()).unwrap();
            tx.send(()).unwrap();
        });
        rx.recv_timeout(Duration::from_millis(200)).unwrap();

        let (tx1, rx1) = sync_channel(1);
        let change_cmd = ChangeObserver::from_cdc(1, ObserveHandle::new());
        pool.spawn(async move {
            let res = initializer
                .initialize(change_cmd, raft_router, concurrency_semaphore)
                .await;
            tx1.send(res).unwrap();
        });
        // Must timeout because there is no enough permit.
        rx1.recv_timeout(Duration::from_millis(200)).unwrap_err();

        // Release the permit
        rx.recv_timeout(Duration::from_millis(200)).unwrap();
        let res = rx1.recv_timeout(Duration::from_millis(200)).unwrap();
        res.unwrap_err();

        worker.stop();
    }

    #[test]
    fn test_change_endpoint_cfg() {
        let cfg = CdcConfig::default();
        let mut suite = mock_endpoint(&cfg, None);
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
                // which will be an invalid change and will be lost.
                updated_cfg.incremental_scan_concurrency = 2;
            }
            let diff = cfg.diff(&updated_cfg);
            ep.run(Task::ChangeConfig(diff));
            assert_eq!(ep.config.incremental_scan_concurrency, 6);
            assert_eq!(ep.scan_concurrency_semaphore.available_permits(), 6);

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
        let mut suite = mock_endpoint(&CdcConfig::default(), None);

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
        let downstream = Downstream::new("".to_string(), region_epoch, 0, conn_id, true);
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
        let mut suite = mock_endpoint(&cfg, None);
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
        let downstream = Downstream::new("".to_string(), region_epoch.clone(), 1, conn_id, true);
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
        let downstream = Downstream::new("".to_string(), region_epoch.clone(), 2, conn_id, true);
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
        let downstream = Downstream::new("".to_string(), region_epoch, 3, conn_id, true);
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
        let downstream = Downstream::new("".to_string(), region_epoch.clone(), 1, conn_id, true);
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
        let downstream = Downstream::new("".to_string(), region_epoch, 1, conn_id, true);
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
        let mut suite = mock_endpoint(&cfg, None);
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
        let downstream = Downstream::new("".to_string(), region_epoch.clone(), 0, conn_id, true);
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
        let downstream = Downstream::new("".to_string(), region_epoch.clone(), 0, conn_id, true);
        downstream.get_state().store(DownstreamState::Normal);
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
        let downstream = Downstream::new("".to_string(), region_epoch, 3, conn_id, true);
        downstream.get_state().store(DownstreamState::Normal);
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
        let mut suite = mock_endpoint(&CdcConfig::default(), None);
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
        let downstream = Downstream::new("".to_string(), region_epoch.clone(), 0, conn_id, true);
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

        let downstream = Downstream::new("".to_string(), region_epoch.clone(), 0, conn_id, true);
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
        let downstream = Downstream::new("".to_string(), region_epoch, 0, conn_id, true);
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
        let mut suite = mock_endpoint(&cfg, None);

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
                let downstream =
                    Downstream::new("".to_string(), region_epoch.clone(), 0, conn_id, true);
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
        let mut suite = mock_endpoint(&CdcConfig::default(), None);
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
        let downstream =
            Downstream::new("".to_string(), region_epoch_2.clone(), 0, conn_id_a, true);
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
        let downstream = Downstream::new("".to_string(), region_epoch_1, 0, conn_id_b, true);
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
}
