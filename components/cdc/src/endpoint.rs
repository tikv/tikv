// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::f64::INFINITY;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use collections::{HashMap, HashSet};
use concurrency_manager::ConcurrencyManager;
use configuration::{ConfigChange, Configuration};
use crossbeam::atomic::AtomicCell;
use engine_rocks::{RocksEngine, RocksSnapshot};
use fail::fail_point;
use futures::compat::Future01CompatExt;
use futures::future::select_all;
use futures::FutureExt;
use grpcio::{ChannelBuilder, Environment};
#[cfg(feature = "prost-codec")]
use kvproto::cdcpb::{
    event::Event as Event_oneof_event, ChangeDataRequest,
    DuplicateRequest as ErrorDuplicateRequest, Error as EventError, Event, ResolvedTs,
};
#[cfg(not(feature = "prost-codec"))]
use kvproto::cdcpb::{
    ChangeDataRequest, DuplicateRequest as ErrorDuplicateRequest, Error as EventError, Event,
    Event_oneof_event, ResolvedTs,
};
use kvproto::kvrpcpb::{CheckLeaderRequest, ExtraOp as TxnExtraOp, LeaderInfo};
use kvproto::metapb::{PeerRole, Region, RegionEpoch};
use kvproto::tikvpb::TikvClient;
use pd_client::{Feature, PdClient};
use raftstore::coprocessor::CmdBatch;
use raftstore::coprocessor::ObserveID;
use raftstore::router::RaftStoreRouter;
use raftstore::store::fsm::{ChangeObserver, StoreMeta};
use raftstore::store::msg::{Callback, ReadResponse, SignificantMsg};
use resolved_ts::Resolver;
use security::SecurityManager;
use tikv::config::CdcConfig;
use tikv::storage::kv::Snapshot;
use tikv::storage::mvcc::{DeltaScanner, ScannerBuilder};
use tikv::storage::txn::TxnEntry;
use tikv::storage::txn::TxnEntryScanner;
use tikv_util::time::{Instant, Limiter};
use tikv_util::timer::SteadyTimer;
use tikv_util::worker::{Runnable, RunnableWithTimer, ScheduleError, Scheduler};
use tikv_util::{box_err, box_try, debug, error, impl_display_as_debug, info, warn};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};
use txn_types::{Key, Lock, LockType, TimeStamp, TxnExtra, TxnExtraScheduler};

use crate::channel::{CdcEvent, MemoryQuota, SendError};
use crate::delegate::{Delegate, Downstream, DownstreamID, DownstreamState};
use crate::metrics::*;
use crate::old_value::{OldValueCache, OldValueCallback};
use crate::service::{Conn, ConnID, FeatureGate};
use crate::{CdcObserver, Error, Result};

const FEATURE_RESOLVED_TS_STORE: Feature = Feature::require(5, 0, 0);
const DEFAULT_CHECK_LEADER_TIMEOUT_MILLISECONDS: u64 = 5_000; // 5s

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

pub struct Endpoint<T> {
    capture_regions: HashMap<u64, Delegate>,
    connections: HashMap<ConnID, Conn>,
    scheduler: Scheduler<Task>,
    raft_router: T,
    observer: CdcObserver,

    pd_client: Arc<dyn PdClient>,
    timer: SteadyTimer,
    tso_worker: Runtime,
    store_meta: Arc<Mutex<StoreMeta>>,
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
    tikv_clients: Arc<AsyncMutex<HashMap<u64, TikvClient>>>,
    env: Arc<Environment>,
    security_mgr: Arc<SecurityManager>,
}

impl<T: 'static + RaftStoreRouter<RocksEngine>> Endpoint<T> {
    pub fn new(
        config: &CdcConfig,
        pd_client: Arc<dyn PdClient>,
        scheduler: Scheduler<Task>,
        raft_router: T,
        observer: CdcObserver,
        store_meta: Arc<Mutex<StoreMeta>>,
        concurrency_manager: ConcurrencyManager,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
        sink_memory_quota: MemoryQuota,
    ) -> Endpoint<T> {
        let workers = Builder::new()
            .threaded_scheduler()
            .thread_name("cdcwkr")
            .core_threads(config.incremental_scan_threads)
            .build()
            .unwrap();
        let tso_worker = Builder::new()
            .threaded_scheduler()
            .thread_name("tso")
            .enable_time()
            .core_threads(1)
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
            INFINITY
        });

        CDC_SINK_CAP.set(sink_memory_quota.capacity() as i64);
        // For scan efficiency, the scan batch bytes should be around 1MB.
        let max_scan_batch_bytes = 1024 * 1024;
        // Assume 1KB per entry.
        let max_scan_batch_size = 1024;

        let ep = Endpoint {
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
            observer,
            store_meta,
            concurrency_manager,
            min_resolved_ts: TimeStamp::max(),
            min_ts_region_id: 0,
            old_value_cache,
            resolved_region_count: 0,
            unresolved_region_count: 0,
            sink_memory_quota,
            tikv_clients: Arc::new(AsyncMutex::new(HashMap::default())),
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
                INFINITY
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
                    init.deregister_downstream(e)
                }
            }
        });
    }

    pub fn on_multi_batch(&mut self, multi: Vec<CmdBatch>, old_value_cb: OldValueCallback) {
        fail_point!("cdc_before_handle_multi_batch", |_| {});
        for batch in multi {
            let region_id = batch.region.get_id();
            let mut deregister = None;
            if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
                if delegate.has_failed() {
                    // Skip the batch if the delegate has failed.
                    continue;
                }
                if let Err(e) = delegate.on_batch(batch, &old_value_cb, &mut self.old_value_cache) {
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
                // None means there is no downsteam registered yet.
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
                    Self::region_resolved_ts_store(
                        regions,
                        store_meta,
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

    async fn region_resolved_ts_store(
        regions: Vec<(u64, ObserveID)>,
        store_meta: Arc<Mutex<StoreMeta>>,
        pd_client: Arc<dyn PdClient>,
        security_mgr: Arc<SecurityManager>,
        env: Arc<Environment>,
        tikv_clients: Arc<AsyncMutex<HashMap<u64, TikvClient>>>,
        min_ts: TimeStamp,
    ) -> Vec<u64> {
        let region_has_quorum = |region: &Region, stores: &[u64]| {
            let mut voters = 0;
            let mut incoming_voters = 0;
            let mut demoting_voters = 0;

            let mut resp_voters = 0;
            let mut resp_incoming_voters = 0;
            let mut resp_demoting_voters = 0;

            region.get_peers().iter().for_each(|peer| {
                let mut in_resp = false;
                for store_id in stores {
                    if *store_id == peer.store_id {
                        in_resp = true;
                        break;
                    }
                }
                match peer.get_role() {
                    PeerRole::Voter => {
                        voters += 1;
                        if in_resp {
                            resp_voters += 1;
                        }
                    }
                    PeerRole::IncomingVoter => {
                        incoming_voters += 1;
                        if in_resp {
                            resp_incoming_voters += 1;
                        }
                    }
                    PeerRole::DemotingVoter => {
                        demoting_voters += 1;
                        if in_resp {
                            resp_demoting_voters += 1;
                        }
                    }
                    PeerRole::Learner => (),
                }
            });

            let has_incoming_majority =
                (resp_voters + resp_incoming_voters) >= ((voters + incoming_voters) / 2 + 1);
            let has_demoting_majority =
                (resp_voters + resp_demoting_voters) >= ((voters + demoting_voters) / 2 + 1);

            has_incoming_majority && has_demoting_majority
        };

        let find_store_id = |region: &Region, peer_id| {
            for peer in region.get_peers() {
                if peer.id == peer_id {
                    return Some(peer.store_id);
                }
            }
            None
        };

        // store_id -> leaders info, record the request to each stores
        let mut store_map: HashMap<u64, Vec<LeaderInfo>> = HashMap::default();
        // region_id -> region, cache the information of regions
        let mut region_map: HashMap<u64, Region> = HashMap::default();
        // region_id -> peers id, record the responses
        let mut resp_map: HashMap<u64, Vec<u64>> = HashMap::default();
        let mut valid_regions = HashSet::default();
        {
            let meta = store_meta.lock().unwrap();
            let store_id = match meta.store_id {
                Some(id) => id,
                None => return vec![],
            };
            // TODO: should using `RegionReadProgressRegistry` to dump leader info like `resolved-ts`
            // to reduce the time holding the `store_meta` mutex
            for (region_id, _) in &regions {
                if let Some(region) = meta.regions.get(region_id) {
                    if let Some((term, leader_id)) = meta.leaders.get(region_id) {
                        let leader_store_id = find_store_id(&region, *leader_id);
                        if leader_store_id.is_none() {
                            continue;
                        }
                        if leader_store_id.unwrap() != meta.store_id.unwrap() {
                            continue;
                        }
                        let peer_list = region.get_peers();
                        for peer in peer_list {
                            if peer.store_id == store_id && peer.id == *leader_id {
                                resp_map.entry(*region_id).or_default().push(store_id);
                                if peer_list.len() == 1 {
                                    valid_regions.insert(*region_id);
                                }
                                continue;
                            }
                            if peer.get_role() == PeerRole::Learner {
                                continue;
                            }
                            let mut leader_info = LeaderInfo::default();
                            leader_info.set_peer_id(*leader_id);
                            leader_info.set_term(*term);
                            leader_info.set_region_id(*region_id);
                            leader_info.set_region_epoch(region.get_region_epoch().clone());
                            store_map
                                .entry(peer.store_id)
                                .or_default()
                                .push(leader_info);
                        }
                        region_map.insert(*region_id, region.clone());
                    }
                }
            }
        }
        let store_count = store_map.len();
        let mut stores: Vec<_> = store_map
            .into_iter()
            .map(|(store_id, regions)| {
                let tikv_clients = tikv_clients.clone();
                let env = env.clone();
                let pd_client = pd_client.clone();
                let security_mgr = security_mgr.clone();
                async move {
                    let client = {
                        let mut clients = tikv_clients.lock().await;
                        match clients.get(&store_id).cloned() {
                            Some(client) => client,
                            None => {
                                let store = box_try!(pd_client.get_store_async(store_id).await);
                                let cb = ChannelBuilder::new(env.clone());
                                let channel = security_mgr.connect(cb, &store.address);
                                let client = TikvClient::new(channel);
                                clients.insert(store_id, client.clone());
                                client
                            }
                        }
                    };
                    let mut req = CheckLeaderRequest::default();
                    req.set_regions(regions.into());
                    req.set_ts(min_ts.into_inner());
                    let res = box_try!(
                        tokio::time::timeout(
                            Duration::from_millis(DEFAULT_CHECK_LEADER_TIMEOUT_MILLISECONDS),
                            box_try!(client.check_leader_async(&req))
                        )
                        .await
                    );
                    let resp = match res {
                        Ok(resp) => resp,
                        Err(err) => {
                            tikv_clients.lock().await.remove(&store_id);
                            return Err(box_err!(err));
                        }
                    };
                    Result::Ok((store_id, resp))
                }
                .boxed()
            })
            .collect();

        for _ in 0..store_count {
            // Use `select_all` to avoid the process getting blocked when some TiKVs were down.
            let (res, _, remains) = select_all(stores).await;
            stores = remains;
            if let Ok((store_id, resp)) = res {
                for region_id in resp.regions {
                    resp_map.entry(region_id).or_default().push(store_id);
                    if region_has_quorum(&region_map[&region_id], &resp_map[&region_id]) {
                        valid_regions.insert(region_id);
                    }
                }
            }
            // Return early if all regions had already got quorum.
            if valid_regions.len() == regions.len() {
                // break here because all regions have quorum,
                // so there is no need waiting for other stores to respond.
                break;
            }
        }
        valid_regions.into_iter().collect()
    }

    fn on_open_conn(&mut self, conn: Conn) {
        self.connections.insert(conn.get_id(), conn);
    }
}

struct Initializer {
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
}

impl Initializer {
    async fn initialize<T: 'static + RaftStoreRouter<RocksEngine>>(
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
        mut resp: ReadResponse<RocksSnapshot>,
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
            "observe_id" => ?self.observe_id);

        let mut resolver = if self.build_resolver {
            Some(Resolver::new(region_id))
        } else {
            None
        };

        fail_point!("cdc_incremental_scan_start");

        let start = Instant::now_coarse();
        // Time range: (checkpoint_ts, current]
        let current = TimeStamp::max();
        let mut scanner = ScannerBuilder::new(snap, current, false)
            .fill_cache(false)
            .range(None, None)
            .build_delta_scanner(self.checkpoint_ts, self.txn_extra_op)
            .unwrap();
        let conn_id = self.conn_id;
        let mut done = false;
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
            let entries = self.scan_batch(&mut scanner, resolver.as_mut()).await?;
            // If the last element is None, it means scanning is finished.
            if let Some(None) = entries.last() {
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

    async fn scan_batch<S: Snapshot>(
        &self,
        scanner: &mut DeltaScanner<S>,
        resolver: Option<&mut Resolver>,
    ) -> Result<Vec<Option<TxnEntry>>> {
        let mut entries = Vec::with_capacity(self.max_scan_batch_size);
        let mut total_bytes = 0;
        let mut total_size = 0;
        while total_bytes <= self.max_scan_batch_bytes && total_size < self.max_scan_batch_size {
            total_size += 1;
            match scanner.next_entry()? {
                Some(entry) => {
                    total_bytes += entry.size();
                    entries.push(Some(entry));
                }
                None => {
                    entries.push(None);
                    break;
                }
            }
        }
        if total_bytes > 0 {
            self.speed_limiter.consume(total_bytes).await;
            CDC_SCAN_BYTES.inc_by(total_bytes as _);
        }

        if let Some(resolver) = resolver {
            // Track the locks.
            for entry in entries.iter().flatten() {
                if let TxnEntry::Prewrite { lock, .. } = entry {
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
}

impl<T: 'static + RaftStoreRouter<RocksEngine>> Runnable for Endpoint<T> {
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

impl<T: 'static + RaftStoreRouter<RocksEngine>> RunnableWithTimer for Endpoint<T> {
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
    use collections::HashSet;
    use engine_traits::DATA_CFS;
    use futures::executor::block_on;
    use futures::StreamExt;
    use kvproto::cdcpb::Header;
    #[cfg(feature = "prost-codec")]
    use kvproto::cdcpb::{event::Event as Event_oneof_event, Header};
    use kvproto::errorpb::Error as ErrorHeader;
    use raftstore::coprocessor::ObserveHandle;
    use raftstore::errors::Error as RaftStoreError;
    use raftstore::store::msg::CasualMessage;
    use raftstore::store::util::RegionReadProgress;
    use raftstore::store::{ReadDelegate, RegionSnapshot, TrackVer};
    use std::collections::BTreeMap;
    use std::fmt::Display;
    use std::sync::atomic::AtomicU64;
    use std::sync::mpsc::{channel, sync_channel, Receiver, RecvTimeoutError, Sender};
    use tempfile::TempDir;
    use test_raftstore::MockRaftStoreRouter;
    use test_raftstore::TestPdClient;
    use tikv::storage::kv::Engine;
    use tikv::storage::txn::tests::{must_acquire_pessimistic_lock, must_prewrite_put};
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
            self.tx.send(task).unwrap();
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
    ) -> (
        LazyWorker<Task>,
        Runtime,
        Initializer,
        Receiver<Task>,
        crate::channel::Drain,
    ) {
        let (receiver_worker, rx) = new_receiver_worker();
        let quota = crate::channel::MemoryQuota::new(usize::MAX);
        let (sink, drain) = crate::channel::channel(buffer, quota);

        let pool = Builder::new()
            .threaded_scheduler()
            .thread_name("test-initializer-worker")
            .core_threads(4)
            .build()
            .unwrap();
        let downstream_state = Arc::new(AtomicCell::new(DownstreamState::Normal));
        let initializer = Initializer {
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
        };

        (receiver_worker, pool, initializer, rx, drain)
    }

    fn mock_endpoint(
        cfg: &CdcConfig,
    ) -> (
        Endpoint<MockRaftStoreRouter>,
        MockRaftStoreRouter,
        ReceiverWrapper<Task>,
    ) {
        let mut region = Region::default();
        region.set_id(1);
        let store_meta = Arc::new(Mutex::new(StoreMeta::new(0)));
        let read_delegate = ReadDelegate {
            tag: String::new(),
            region: Arc::new(region),
            peer_id: 2,
            term: 1,
            applied_index_term: 1,
            leader_lease: None,
            last_valid_ts: Timespec::new(0, 0),
            txn_extra_op: Arc::new(AtomicCell::new(TxnExtraOp::default())),
            max_ts_sync_status: Arc::new(AtomicU64::new(0)),
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
            cfg,
            pd_client,
            task_sched,
            raft_router.clone(),
            observer,
            store_meta,
            ConcurrencyManager::new(1.into()),
            env,
            security_mgr,
            MemoryQuota::new(usize::MAX),
        );
        (ep, raft_router, task_rx)
    }

    #[test]
    fn test_initializer_build_resolver() {
        let temp = TempDir::new().unwrap();
        let engine = TestEngineBuilder::new()
            .path(temp.path())
            .cfs(DATA_CFS)
            .build()
            .unwrap();

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
            mock_initializer(total_bytes, buffer);
        let check_result = || loop {
            let task = rx.recv().unwrap();
            match task {
                Task::ResolverReady { resolver, .. } => {
                    assert_eq!(resolver.locks(), &expected_locks);
                    return;
                }
                t => panic!("unepxected task {} received", t),
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

        initializer.max_scan_batch_bytes = total_bytes / 3;
        let start_1_3 = Instant::now();
        block_on(initializer.async_incremental_scan(snap.clone(), region.clone())).unwrap();
        check_result();
        // 2s to allow certain inaccuracy.
        assert!(
            start_1_3.saturating_elapsed() >= Duration::new(2, 0),
            "{:?}",
            start_1_3.saturating_elapsed()
        );

        let start_1_6 = Instant::now();
        initializer.max_scan_batch_bytes = total_bytes / 6;
        block_on(initializer.async_incremental_scan(snap.clone(), region.clone())).unwrap();
        check_result();
        // 4s to allow certain inaccuracy.
        assert!(
            start_1_6.saturating_elapsed() >= Duration::new(4, 0),
            "{:?}",
            start_1_6.saturating_elapsed()
        );

        initializer.build_resolver = false;
        block_on(initializer.async_incremental_scan(snap.clone(), region.clone())).unwrap();

        loop {
            let task = rx.recv_timeout(Duration::from_millis(100));
            match task {
                Ok(t) => panic!("unepxected task {} received", t),
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

    #[test]
    fn test_initializer_deregister_downstream() {
        let total_bytes = 1;
        let buffer = 1;
        let (mut worker, _pool, mut initializer, rx, _drain) =
            mock_initializer(total_bytes, buffer);

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
            mock_initializer(total_bytes, buffer);

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
        let (mut ep, _raft_router, mut _task_rx) = mock_endpoint(&cfg);

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
        let (mut ep, raft_router, mut task_rx) = mock_endpoint(&CdcConfig::default());
        // Fill the channel.
        let _raft_rx = raft_router.add_region(1 /* region id */, 1 /* cap */);
        loop {
            if let Err(RaftStoreError::Transport(_)) =
                raft_router.send_casual_msg(1, CasualMessage::ClearRegionSize)
            {
                break;
            }
        }
        // Make sure channel is full.
        raft_router
            .send_casual_msg(1, CasualMessage::ClearRegionSize)
            .unwrap_err();

        let conn = Conn::new(tx, String::new());
        let conn_id = conn.get_id();
        ep.run(Task::OpenConn { conn });
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new("".to_string(), region_epoch, 0, conn_id, true);
        ep.run(Task::Register {
            request: req,
            downstream,
            conn_id,
            version: semver::Version::new(0, 0, 0),
        });
        assert_eq!(ep.capture_regions.len(), 1);

        for _ in 0..5 {
            if let Ok(Some(Task::Deregister(Deregister::Downstream {
                err: Some(Error::Request(err)),
                ..
            }))) = task_rx.recv_timeout(Duration::from_secs(1))
            {
                assert!(!err.has_server_is_busy());
            }
        }
    }

    #[test]
    fn test_register() {
        let (mut ep, raft_router, mut task_rx) = mock_endpoint(&CdcConfig {
            min_ts_interval: ReadableDuration(Duration::from_secs(60)),
            ..Default::default()
        });
        let _raft_rx = raft_router.add_region(1 /* region id */, 100 /* cap */);
        let quota = crate::channel::MemoryQuota::new(usize::MAX);
        let (tx, mut rx) = channel::channel(1, quota);
        let mut rx = rx.drain();

        let conn = Conn::new(tx, String::new());
        let conn_id = conn.get_id();
        ep.run(Task::OpenConn { conn });
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new("".to_string(), region_epoch.clone(), 1, conn_id, true);
        // Enable batch resolved ts in the test.
        let version = FeatureGate::batch_resolved_ts();
        ep.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: version.clone(),
        });
        assert_eq!(ep.capture_regions.len(), 1);
        task_rx
            .recv_timeout(Duration::from_millis(100))
            .unwrap_err();

        // duplicate request error.
        let downstream = Downstream::new("".to_string(), region_epoch.clone(), 2, conn_id, true);
        ep.run(Task::Register {
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
        assert_eq!(ep.capture_regions.len(), 1);
        task_rx
            .recv_timeout(Duration::from_millis(100))
            .unwrap_err();

        // Compatibility error.
        let downstream = Downstream::new("".to_string(), region_epoch, 3, conn_id, true);
        ep.run(Task::Register {
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
        assert_eq!(ep.capture_regions.len(), 1);
        task_rx
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
        ep.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: version.clone(),
        });
        // Region 100 is inserted into capture_regions.
        assert_eq!(ep.capture_regions.len(), 2);
        let task = task_rx.recv_timeout(Duration::from_millis(100)).unwrap();
        match task.unwrap() {
            Task::Deregister(Deregister::Delegate { region_id, err, .. }) => {
                assert_eq!(region_id, 100);
                assert!(matches!(err, Error::Request(_)), "{:?}", err);
            }
            other => panic!("unexpected task {:?}", other),
        }

        // Test errors on CaptureChange message.
        req.set_region_id(101);
        let raft_rx = raft_router.add_region(101 /* region id */, 100 /* cap */);
        let downstream = Downstream::new("".to_string(), region_epoch, 1, conn_id, true);
        ep.run(Task::Register {
            request: req,
            downstream,
            conn_id,
            version,
        });
        // Drop CaptureChange message, it should cause scan task failure.
        let _ = raft_rx.recv_timeout(Duration::from_millis(100)).unwrap();
        assert_eq!(ep.capture_regions.len(), 3);
        let task = task_rx.recv_timeout(Duration::from_millis(100)).unwrap();
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
        let (mut ep, raft_router, _task_rx) = mock_endpoint(&CdcConfig {
            min_ts_interval: ReadableDuration(Duration::from_secs(60)),
            ..Default::default()
        });
        let _raft_rx = raft_router.add_region(1 /* region id */, 100 /* cap */);

        let quota = crate::channel::MemoryQuota::new(usize::MAX);
        let (tx, mut rx) = channel::channel(1, quota);
        let mut rx = rx.drain();
        let mut region = Region::default();
        region.set_id(1);
        let conn = Conn::new(tx, String::new());
        let conn_id = conn.get_id();
        ep.run(Task::OpenConn { conn });
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new("".to_string(), region_epoch.clone(), 0, conn_id, true);
        downstream.get_state().store(DownstreamState::Normal);
        // Enable batch resolved ts in the test.
        let version = FeatureGate::batch_resolved_ts();
        ep.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: version.clone(),
        });
        let resolver = Resolver::new(1);
        let observe_id = ep.capture_regions[&1].handle.id;
        ep.on_region_ready(observe_id, resolver, region.clone());
        ep.run(Task::MinTS {
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
        ep.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version,
        });
        let resolver = Resolver::new(2);
        region.set_id(2);
        let observe_id = ep.capture_regions[&2].handle.id;
        ep.on_region_ready(observe_id, resolver, region);
        ep.run(Task::MinTS {
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
        ep.run(Task::OpenConn { conn });
        req.set_region_id(3);
        let downstream = Downstream::new("".to_string(), region_epoch, 3, conn_id, true);
        downstream.get_state().store(DownstreamState::Normal);
        ep.run(Task::Register {
            request: req,
            downstream,
            conn_id,
            version: semver::Version::new(4, 0, 5),
        });
        let resolver = Resolver::new(3);
        region.set_id(3);
        let observe_id = ep.capture_regions[&3].handle.id;
        ep.on_region_ready(observe_id, resolver, region);
        ep.run(Task::MinTS {
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
        let (mut ep, raft_router, _task_rx) = mock_endpoint(&CdcConfig::default());
        let _raft_rx = raft_router.add_region(1 /* region id */, 100 /* cap */);
        let quota = crate::channel::MemoryQuota::new(usize::MAX);
        let (tx, mut rx) = channel::channel(1, quota);
        let mut rx = rx.drain();

        let conn = Conn::new(tx, String::new());
        let conn_id = conn.get_id();
        ep.run(Task::OpenConn { conn });
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new("".to_string(), region_epoch.clone(), 0, conn_id, true);
        let downstream_id = downstream.get_id();
        ep.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: semver::Version::new(0, 0, 0),
        });
        assert_eq!(ep.capture_regions.len(), 1);

        let mut err_header = ErrorHeader::default();
        err_header.set_not_leader(Default::default());
        let deregister = Deregister::Downstream {
            region_id: 1,
            downstream_id,
            conn_id,
            err: Some(Error::request(err_header.clone())),
        };
        ep.run(Task::Deregister(deregister));
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
        assert_eq!(ep.capture_regions.len(), 0);

        let downstream = Downstream::new("".to_string(), region_epoch.clone(), 0, conn_id, true);
        let new_downstream_id = downstream.get_id();
        ep.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: semver::Version::new(0, 0, 0),
        });
        assert_eq!(ep.capture_regions.len(), 1);

        let deregister = Deregister::Downstream {
            region_id: 1,
            downstream_id,
            conn_id,
            err: Some(Error::request(err_header.clone())),
        };
        ep.run(Task::Deregister(deregister));
        assert!(channel::recv_timeout(&mut rx, Duration::from_millis(200)).is_err());
        assert_eq!(ep.capture_regions.len(), 1);

        let deregister = Deregister::Downstream {
            region_id: 1,
            downstream_id: new_downstream_id,
            conn_id,
            err: Some(Error::request(err_header.clone())),
        };
        ep.run(Task::Deregister(deregister));
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
        assert_eq!(ep.capture_regions.len(), 0);

        // Stale deregister should be filtered.
        let downstream = Downstream::new("".to_string(), region_epoch, 0, conn_id, true);
        ep.run(Task::Register {
            request: req,
            downstream,
            conn_id,
            version: semver::Version::new(0, 0, 0),
        });
        assert_eq!(ep.capture_regions.len(), 1);
        let deregister = Deregister::Delegate {
            region_id: 1,
            // A stale ObserveID (different from the actual one).
            observe_id: ObserveID::new(),
            err: Error::request(err_header),
        };
        ep.run(Task::Deregister(deregister));
        match channel::recv_timeout(&mut rx, Duration::from_millis(500)) {
            Err(_) => (),
            Ok(other) => panic!("unknown event {:?}", other),
        }
        assert_eq!(ep.capture_regions.len(), 1);
    }

    #[test]
    fn test_broadcast_resolved_ts() {
        let (mut ep, raft_router, _task_rx) = mock_endpoint(&CdcConfig {
            min_ts_interval: ReadableDuration(Duration::from_secs(60)),
            ..Default::default()
        });

        // Open two connections a and b, registers region 1, 2 to conn a and
        // region 3 to conn b.
        let mut conn_rxs = vec![];
        let quota = channel::MemoryQuota::new(usize::MAX);
        // Hold raft_rxs to avoid SendError panic.
        let mut raft_rxs = vec![];
        for region_ids in vec![vec![1, 2], vec![3]] {
            let (tx, rx) = channel::channel(1, quota.clone());
            conn_rxs.push(rx);
            let conn = Conn::new(tx, String::new());
            let conn_id = conn.get_id();
            ep.run(Task::OpenConn { conn });

            for region_id in region_ids {
                let raft_rx = raft_router.add_region(region_id, 100 /* cap */);
                raft_rxs.push(raft_rx);
                let mut req_header = Header::default();
                req_header.set_cluster_id(0);
                let mut req = ChangeDataRequest::default();
                req.set_region_id(region_id);
                let region_epoch = req.get_region_epoch().clone();
                let downstream =
                    Downstream::new("".to_string(), region_epoch.clone(), 0, conn_id, true);
                downstream.get_state().store(DownstreamState::Normal);
                ep.run(Task::Register {
                    request: req.clone(),
                    downstream,
                    conn_id,
                    version: FeatureGate::batch_resolved_ts(),
                });
                let resolver = Resolver::new(region_id);
                let observe_id = ep.capture_regions[&region_id].handle.id;
                let mut region = Region::default();
                region.set_id(region_id);
                ep.on_region_ready(observe_id, resolver, region);
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

        ep.run(Task::MinTS {
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

        ep.run(Task::MinTS {
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

        ep.run(Task::MinTS {
            regions: vec![1, 2, 3],
            min_ts: TimeStamp::from(3),
        });
        // conn a must receive a resolved ts that contains region 1 and region 2.
        assert_batch_resolved_ts(conn_rxs.get_mut(0).unwrap(), vec![1, 2], 3);
        // conn b must receive a resolved ts that contains region 3.
        assert_batch_resolved_ts(conn_rxs.get_mut(1).unwrap(), vec![3], 3);

        ep.run(Task::MinTS {
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
        let (mut ep, raft_router, _task_rx) = mock_endpoint(&CdcConfig::default());
        let _raft_rx = raft_router.add_region(1 /* region id */, 100 /* cap */);
        let quota = crate::channel::MemoryQuota::new(usize::MAX);

        // Open conn a
        let (tx1, _rx1) = channel::channel(1, quota.clone());
        let conn_a = Conn::new(tx1, String::new());
        let conn_id_a = conn_a.get_id();
        ep.run(Task::OpenConn { conn: conn_a });

        // Open conn b
        let (tx2, mut rx2) = channel::channel(1, quota);
        let mut rx2 = rx2.drain();
        let conn_b = Conn::new(tx2, String::new());
        let conn_id_b = conn_b.get_id();
        ep.run(Task::OpenConn { conn: conn_b });

        // Register region 1 (epoch 2) at conn a.
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        req.mut_region_epoch().set_version(2);
        let region_epoch_2 = req.get_region_epoch().clone();
        let downstream =
            Downstream::new("".to_string(), region_epoch_2.clone(), 0, conn_id_a, true);
        ep.run(Task::Register {
            request: req,
            downstream,
            conn_id: conn_id_a,
            version: semver::Version::new(0, 0, 0),
        });
        assert_eq!(ep.capture_regions.len(), 1);
        let observe_id = ep.capture_regions[&1].handle.id;

        // Register region 1 (epoch 1) at conn b.
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        req.mut_region_epoch().set_version(1);
        let region_epoch_1 = req.get_region_epoch().clone();
        let downstream = Downstream::new("".to_string(), region_epoch_1, 0, conn_id_b, true);
        ep.run(Task::Register {
            request: req,
            downstream,
            conn_id: conn_id_b,
            version: semver::Version::new(0, 0, 0),
        });
        assert_eq!(ep.capture_regions.len(), 1);

        // Deregister conn a.
        ep.run(Task::Deregister(Deregister::Conn(conn_id_a)));
        assert_eq!(ep.capture_regions.len(), 1);

        // Schedule resolver ready (resolver is built by conn a).
        let mut region = Region::default();
        region.id = 1;
        region.set_region_epoch(region_epoch_2);
        ep.run(Task::ResolverReady {
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
        ep.run(Task::Deregister(Deregister::Delegate {
            region_id: 1,
            observe_id,
            err: Error::request(epoch_not_match),
        }));
        assert_eq!(ep.capture_regions.len(), 0);

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
