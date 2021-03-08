// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use collections::HashMap;
use concurrency_manager::ConcurrencyManager;
use crossbeam::atomic::AtomicCell;
use engine_rocks::{RocksEngine, RocksSnapshot};
use futures::compat::Future01CompatExt;
use grpcio::{ChannelBuilder, Environment};
#[cfg(feature = "prost-codec")]
use kvproto::cdcpb::event::Event as Event_oneof_event;
use kvproto::cdcpb::*;
use kvproto::kvrpcpb::{CheckLeaderRequest, ExtraOp as TxnExtraOp, LeaderInfo};
use kvproto::metapb::{PeerRole, Region};
use kvproto::tikvpb::TikvClient;
use pd_client::PdClient;
use raftstore::coprocessor::CmdBatch;
use raftstore::router::RaftStoreRouter;
use raftstore::store::fsm::{ChangeCmd, ObserveID, StoreMeta};
use raftstore::store::msg::{Callback, ReadResponse, SignificantMsg};
use resolved_ts::Resolver;
use security::SecurityManager;
use tikv::config::CdcConfig;
use tikv::storage::kv::Snapshot;
use tikv::storage::mvcc::{DeltaScanner, ScannerBuilder};
use tikv::storage::txn::TxnEntry;
use tikv::storage::txn::TxnEntryScanner;
use tikv::storage::Statistics;
use tikv_util::impl_display_as_debug;
use tikv_util::lru::LruCache;
use tikv_util::time::Instant;
use tikv_util::timer::SteadyTimer;
use tikv_util::worker::{Runnable, RunnableWithTimer, ScheduleError, Scheduler};
use tokio::runtime::{Builder, Runtime};
use txn_types::{
    Key, Lock, LockType, MutationType, OldValue, TimeStamp, TxnExtra, TxnExtraScheduler,
};

use crate::delegate::{Delegate, Downstream, DownstreamID, DownstreamState};
use crate::metrics::*;
use crate::service::{CdcEvent, Conn, ConnID, FeatureGate};
use crate::{CdcObserver, Error, Result};

pub enum Deregister {
    Downstream {
        region_id: u64,
        downstream_id: DownstreamID,
        conn_id: ConnID,
        err: Option<Error>,
    },
    Region {
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
            Deregister::Region {
                ref region_id,
                ref observe_id,
                ref err,
            } => de
                .field("deregister", &"region")
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
pub(crate) type OldValueCallback =
    Box<dyn FnMut(Key, TimeStamp, &mut OldValueCache, &mut Statistics) -> Option<Vec<u8>> + Send>;

pub struct OldValueCache {
    pub cache: LruCache<Key, (OldValue, MutationType)>,
    pub miss_count: usize,
    pub access_count: usize,
}

impl OldValueCache {
    pub fn new(size: usize) -> OldValueCache {
        OldValueCache {
            cache: LruCache::with_capacity(size),
            miss_count: 0,
            access_count: 0,
        }
    }
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
    IncrementalScan {
        region_id: u64,
        downstream_id: DownstreamID,
        entries: Vec<Option<TxnEntry>>,
    },
    RegisterMinTsEvent,
    // The result of ChangeCmd should be returned from CDC Endpoint to ensure
    // the downstream switches to Normal after the previous commands was sunk.
    InitDownstream {
        downstream_id: DownstreamID,
        downstream_state: Arc<AtomicCell<DownstreamState>>,
        cb: InitCallback,
    },
    TxnExtra(TxnExtra),
    Validate(u64, Box<dyn FnOnce(Option<&Delegate>) + Send>),
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
            Task::IncrementalScan {
                ref region_id,
                ref downstream_id,
                ref entries,
            } => de
                .field("type", &"incremental_scan")
                .field("region_id", &region_id)
                .field("downstream", &downstream_id)
                .field("scan_entries", &entries.len())
                .finish(),
            Task::RegisterMinTsEvent => de.field("type", &"register_min_ts").finish(),
            Task::InitDownstream {
                ref downstream_id, ..
            } => de
                .field("type", &"init_downstream")
                .field("downstream", &downstream_id)
                .finish(),
            Task::TxnExtra(_) => de.field("type", &"txn_extra").finish(),
            Task::Validate(region_id, _) => de.field("region_id", &region_id).finish(),
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
    min_ts_interval: Duration,
    scan_batch_size: usize,
    tso_worker: Runtime,
    store_meta: Arc<Mutex<StoreMeta>>,
    /// The concurrency manager for transactions. It's needed for CDC to check locks when
    /// calculating resolved_ts.
    concurrency_manager: ConcurrencyManager,

    workers: Runtime,

    min_resolved_ts: TimeStamp,
    min_ts_region_id: u64,
    old_value_cache: OldValueCache,
    hibernate_regions_compatible: bool,

    // store_id -> client
    tikv_clients: Arc<Mutex<HashMap<u64, TikvClient>>>,
    env: Arc<Environment>,
    security_mgr: Arc<SecurityManager>,
}

impl<T: 'static + RaftStoreRouter<RocksEngine>> Endpoint<T> {
    pub fn new(
        cfg: &CdcConfig,
        pd_client: Arc<dyn PdClient>,
        scheduler: Scheduler<Task>,
        raft_router: T,
        observer: CdcObserver,
        store_meta: Arc<Mutex<StoreMeta>>,
        concurrency_manager: ConcurrencyManager,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
    ) -> Endpoint<T> {
        let workers = Builder::new()
            .threaded_scheduler()
            .thread_name("cdcwkr")
            .core_threads(4)
            .build()
            .unwrap();
        let tso_worker = Builder::new()
            .threaded_scheduler()
            .thread_name("tso")
            .core_threads(1)
            .build()
            .unwrap();
        let ep = Endpoint {
            env,
            security_mgr,
            capture_regions: HashMap::default(),
            connections: HashMap::default(),
            scheduler,
            pd_client,
            tso_worker,
            timer: SteadyTimer::default(),
            workers,
            raft_router,
            observer,
            store_meta,
            concurrency_manager,
            scan_batch_size: 1024,
            min_ts_interval: cfg.min_ts_interval.0,
            min_resolved_ts: TimeStamp::max(),
            min_ts_region_id: 0,
            old_value_cache: OldValueCache::new(cfg.old_value_cache_size),
            hibernate_regions_compatible: cfg.hibernate_regions_compatible,
            tikv_clients: Arc::new(Mutex::new(HashMap::default())),
        };
        ep.register_min_ts_event();
        ep
    }

    pub fn set_min_ts_interval(&mut self, dur: Duration) {
        self.min_ts_interval = dur;
    }

    pub fn set_scan_batch_size(&mut self, scan_batch_size: usize) {
        self.scan_batch_size = scan_batch_size;
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
                // The peer wants to deregister
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
                        reader
                            .txn_extra_op
                            .compare_and_swap(TxnExtraOp::ReadOldValue, TxnExtraOp::Noop);
                    }
                    // Do not continue to observe the events of the region.
                    let oid = self.observer.unsubscribe_region(region_id, delegate.id);
                    assert!(
                        oid.is_some(),
                        "unsubscribe region {} failed, ObserveID {:?}",
                        region_id,
                        delegate.id
                    );
                }
            }
            Deregister::Region {
                region_id,
                observe_id,
                err,
            } => {
                // Something went wrong, deregister all downstreams of the region.

                // To avoid ABA problem, we must check the unique ObserveID.
                let need_remove = self
                    .capture_regions
                    .get(&region_id)
                    .map_or(false, |d| d.id == observe_id);
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
                    conn.take_downstreams()
                        .into_iter()
                        .for_each(|(region_id, downstream_id)| {
                            if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
                                if delegate.unsubscribe(downstream_id, None) {
                                    let delegate = self.capture_regions.remove(&region_id).unwrap();
                                    // Do not continue to observe the events of the region.
                                    let oid =
                                        self.observer.unsubscribe_region(region_id, delegate.id);
                                    assert!(
                                        oid.is_some(),
                                        "unsubscribe region {} failed, ObserveID {:?}",
                                        region_id,
                                        delegate.id
                                    );
                                }
                            }
                        });
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
        let conn = match self.connections.get_mut(&conn_id) {
            Some(conn) => conn,
            None => {
                error!("register for a nonexistent connection";
                    "region_id" => region_id, "conn_id" => ?conn_id);
                return;
            }
        };
        downstream.set_sink(conn.get_sink());

        // TODO: Add a new task to close incompatible features.
        if let Some(e) = conn.check_version_and_set_feature(version) {
            // The downstream has not registered yet, send error right away.
            downstream.sink_compatibility_error(region_id, e);
            return;
        }
        if !conn.subscribe(request.get_region_id(), downstream_id) {
            downstream.sink_duplicate_error(request.get_region_id());
            error!("duplicate register";
                "region_id" => region_id,
                "conn_id" => ?conn_id,
                "req_id" => request.get_request_id(),
                "downstream_id" => ?downstream_id);
            return;
        }

        info!("cdc register region";
            "region_id" => region_id,
            "conn_id" => ?conn.get_id(),
            "req_id" => request.get_request_id(),
            "downstream_id" => ?downstream_id);
        let mut is_new_delegate = false;
        let delegate = self.capture_regions.entry(region_id).or_insert_with(|| {
            let d = Delegate::new(region_id);
            is_new_delegate = true;
            d
        });

        let downstream_state = downstream.get_state();
        let checkpoint_ts = request.checkpoint_ts;
        let sched = self.scheduler.clone();
        let batch_size = self.scan_batch_size;

        if !delegate.subscribe(downstream) {
            conn.unsubscribe(request.get_region_id());
            if is_new_delegate {
                self.capture_regions.remove(&request.get_region_id());
            }
            return;
        }
        let change_cmd = if is_new_delegate {
            // The region has never been registered.
            // Subscribe the change events of the region.
            let old_id = self.observer.subscribe_region(region_id, delegate.id);
            assert!(
                old_id.is_none(),
                "region {} must not be observed twice, old ObserveID {:?}, new ObserveID {:?}",
                region_id,
                old_id,
                delegate.id
            );

            ChangeCmd::RegisterObserver {
                observe_id: delegate.id,
                region_id,
                enabled: delegate.enabled(),
            }
        } else {
            ChangeCmd::Snapshot {
                observe_id: delegate.id,
                region_id,
            }
        };
        let txn_extra_op = request.get_extra_op();
        if txn_extra_op != TxnExtraOp::Noop {
            delegate.txn_extra_op = request.get_extra_op();
            if let Some(reader) = self.store_meta.lock().unwrap().readers.get(&region_id) {
                reader.txn_extra_op.store(txn_extra_op);
            }
        }
        let init = Initializer {
            sched,
            region_id,
            conn_id,
            downstream_id,
            batch_size,
            downstream_state: downstream_state.clone(),
            txn_extra_op: delegate.txn_extra_op,
            observe_id: delegate.id,
            checkpoint_ts: checkpoint_ts.into(),
            build_resolver: is_new_delegate,
        };

        let (cb, fut) = tikv_util::future::paired_future_callback();
        let scheduler = self.scheduler.clone();
        let deregister_downstream = move |err| {
            warn!("cdc send capture change cmd failed"; "region_id" => region_id, "error" => ?err);
            let deregister = Deregister::Downstream {
                region_id,
                downstream_id,
                conn_id,
                err: Some(err),
            };
            if let Err(e) = scheduler.schedule(Task::Deregister(deregister)) {
                error!("schedule cdc task failed"; "error" => ?e);
            }
        };
        let scheduler = self.scheduler.clone();
        if let Err(e) = self.raft_router.significant_send(
            region_id,
            SignificantMsg::CaptureChange {
                cmd: change_cmd,
                region_epoch: request.take_region_epoch(),
                callback: Callback::Read(Box::new(move |resp| {
                    if let Err(e) = scheduler.schedule(Task::InitDownstream {
                        downstream_id,
                        downstream_state,
                        cb: Box::new(move || {
                            cb(resp);
                        }),
                    }) {
                        error!("schedule cdc task failed"; "error" => ?e);
                    }
                })),
            },
        ) {
            deregister_downstream(Error::Request(e.into()));
            return;
        }
        self.workers.spawn(async move {
            match fut.await {
                Ok(resp) => init.on_change_cmd(resp),
                Err(e) => deregister_downstream(Error::Other(box_err!(e))),
            }
        });
    }

    pub fn on_multi_batch(&mut self, multi: Vec<CmdBatch>, old_value_cb: OldValueCallback) {
        fail_point!("cdc_before_handle_multi_batch", |_| {});
        let old_value_cb = Rc::new(RefCell::new(old_value_cb));
        for batch in multi {
            let region_id = batch.region_id;
            let mut deregister = None;
            if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
                if delegate.has_failed() {
                    // Skip the batch if the delegate has failed.
                    continue;
                }
                if let Err(e) =
                    delegate.on_batch(batch, old_value_cb.clone(), &mut self.old_value_cache)
                {
                    assert!(delegate.has_failed());
                    // Delegate has error, deregister the corresponding region.
                    deregister = Some(Deregister::Region {
                        region_id,
                        observe_id: delegate.id,
                        err: e,
                    });
                }
            }
            if let Some(deregister) = deregister {
                self.on_deregister(deregister);
            }
        }
    }

    pub fn on_incremental_scan(
        &mut self,
        region_id: u64,
        downstream_id: DownstreamID,
        entries: Vec<Option<TxnEntry>>,
    ) {
        if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
            delegate.on_scan(downstream_id, entries);
        } else {
            warn!("region not found on incremental scan"; "region_id" => region_id);
        }
    }

    fn on_region_ready(&mut self, observe_id: ObserveID, resolver: Resolver, region: Region) {
        let region_id = region.get_id();
        if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
            if delegate.id == observe_id {
                for downstream in delegate.on_region_ready(resolver, region) {
                    let conn_id = downstream.get_conn_id();
                    if !delegate.subscribe(downstream) {
                        let conn = self.connections.get_mut(&conn_id).unwrap();
                        conn.unsubscribe(region_id);
                    }
                }
            } else {
                debug!("stale region ready";
                    "region_id" => region.get_id(),
                    "observe_id" => ?observe_id,
                    "current_id" => ?delegate.id);
            }
        } else {
            debug!("region not found on region ready (finish building resolver)";
                "region_id" => region.get_id());
        }
    }

    fn on_min_ts(&mut self, regions: Vec<u64>, min_ts: TimeStamp) {
        let mut resolved_regions = Vec::with_capacity(regions.len());
        self.min_resolved_ts = TimeStamp::max();
        for region_id in regions {
            if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
                if let Some(resolved_ts) = delegate.on_min_ts(min_ts) {
                    if resolved_ts < self.min_resolved_ts {
                        self.min_resolved_ts = resolved_ts;
                        self.min_ts_region_id = region_id;
                    }
                    resolved_regions.push(region_id);
                }
            }
        }
        self.broadcast_resolved_ts(resolved_regions);
    }

    fn broadcast_resolved_ts(&self, regions: Vec<u64>) {
        let resolved_ts = ResolvedTs {
            regions,
            ts: self.min_resolved_ts.into_inner(),
            ..Default::default()
        };

        let send_cdc_event = |conn: &Conn, event| {
            if let Err(e) = conn.get_sink().try_send(event) {
                match e {
                    crossbeam::TrySendError::Disconnected(_) => {
                        debug!("send event failed, disconnected";
                            "conn_id" => ?conn.get_id(), "downstream" => conn.get_peer());
                    }
                    crossbeam::TrySendError::Full(_) => {
                        info!("send event failed, full";
                            "conn_id" => ?conn.get_id(), "downstream" => conn.get_peer());
                    }
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
                send_cdc_event(conn, CdcEvent::ResolvedTs(resolved_ts.clone()));
            } else {
                // Fallback to previous non-batch resolved ts event.
                for region_id in &resolved_ts.regions {
                    self.broadcast_resolved_ts_compact(*region_id, resolved_ts.ts, conn);
                }
            }
        }
    }

    fn broadcast_resolved_ts_compact(&self, region_id: u64, resolved_ts: u64, conn: &Conn) {
        let downstream_id = match conn.downstream_id(region_id) {
            Some(downstream_id) => downstream_id,
            // No such region registers in the connection.
            None => return,
        };
        let delegate = match self.capture_regions.get(&region_id) {
            Some(delegate) => delegate,
            // No such region registers in the endpoint.
            None => return,
        };
        let downstream = match delegate.downstream(downstream_id) {
            Some(downstream) => downstream,
            // No such downstream registers in the delegate.
            None => return,
        };
        downstream.sink_event(Event {
            region_id,
            event: Some(Event_oneof_event::ResolvedTs(resolved_ts)),
            ..Default::default()
        });
    }

    fn register_min_ts_event(&self) {
        let timeout = self.timer.delay(self.min_ts_interval);
        let pd_client = self.pd_client.clone();
        let scheduler = self.scheduler.clone();
        let raft_router = self.raft_router.clone();
        let regions: Vec<(u64, ObserveID)> = self
            .capture_regions
            .iter()
            .map(|(region_id, delegate)| (*region_id, delegate.id))
            .collect();
        let cm: ConcurrencyManager = self.concurrency_manager.clone();
        let env = self.env.clone();
        let security_mgr = self.security_mgr.clone();
        let store_meta = self.store_meta.clone();
        let tikv_clients = self.tikv_clients.clone();
        let hibernate_regions_compatible = self.hibernate_regions_compatible;

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

            let regions = if hibernate_regions_compatible {
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
                        let deregister = Deregister::Region {
                            observe_id,
                            region_id,
                            err: Error::Request(e.into()),
                        };
                        if let Err(e) = scheduler_clone.schedule(Task::Deregister(deregister)) {
                            error!("schedule cdc task failed"; "error" => ?e);
                        }
                        return None;
                    }
                    rx.await.unwrap_or(None)
                }
            })
            .collect();
        let resps = futures::future::join_all(regions).await;
        resps
            .into_iter()
            .filter_map(|resp| resp)
            .collect::<Vec<u64>>()
    }

    async fn region_resolved_ts_store(
        regions: Vec<(u64, ObserveID)>,
        store_meta: Arc<Mutex<StoreMeta>>,
        pd_client: Arc<dyn PdClient>,
        security_mgr: Arc<SecurityManager>,
        env: Arc<Environment>,
        cdc_clients: Arc<Mutex<HashMap<u64, TikvClient>>>,
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
        {
            let meta = store_meta.lock().unwrap();
            let store_id = match meta.store_id {
                Some(id) => id,
                None => return vec![],
            };
            for (region_id, _) in regions {
                if let Some(region) = meta.regions.get(&region_id) {
                    if let Some((term, leader_id)) = meta.leaders.get(&region_id) {
                        let leader_store_id = find_store_id(&region, *leader_id);
                        if leader_store_id.is_none() {
                            continue;
                        }
                        if leader_store_id.unwrap() != meta.store_id.unwrap() {
                            continue;
                        }
                        for peer in region.get_peers() {
                            if peer.store_id == store_id && peer.id == *leader_id {
                                resp_map.entry(region_id).or_default().push(store_id);
                                continue;
                            }
                            if peer.get_role() == PeerRole::Learner {
                                continue;
                            }
                            let mut leader_info = LeaderInfo::default();
                            leader_info.set_peer_id(*leader_id);
                            leader_info.set_term(*term);
                            leader_info.set_region_id(region_id);
                            leader_info.set_region_epoch(region.get_region_epoch().clone());
                            store_map
                                .entry(peer.store_id)
                                .or_default()
                                .push(leader_info);
                        }
                        region_map.insert(region_id, region.clone());
                    }
                }
            }
        }
        let stores = store_map.into_iter().map(|(store_id, regions)| {
            let cdc_clients = cdc_clients.clone();
            let env = env.clone();
            let pd_client = pd_client.clone();
            let security_mgr = security_mgr.clone();
            async move {
                if cdc_clients.lock().unwrap().get(&store_id).is_none() {
                    let store = box_try!(pd_client.get_store_async(store_id).await);
                    let cb = ChannelBuilder::new(env.clone());
                    let channel = security_mgr.connect(cb, &store.address);
                    cdc_clients
                        .lock()
                        .unwrap()
                        .insert(store_id, TikvClient::new(channel));
                }
                let client = cdc_clients.lock().unwrap().get(&store_id).unwrap().clone();
                let mut req = CheckLeaderRequest::default();
                req.set_regions(regions.into());
                req.set_ts(min_ts.into_inner());
                let res = box_try!(client.check_leader_async(&req)).await;
                let resp = box_try!(res);
                Result::Ok((store_id, resp))
            }
        });
        let resps = futures::future::join_all(stores).await;
        resps
            .into_iter()
            .filter_map(|resp| match resp {
                Ok(resp) => Some(resp),
                Err(e) => {
                    debug!("cdc check leader error"; "err" =>?e);
                    None
                }
            })
            .map(|(store_id, resp)| {
                resp.regions
                    .into_iter()
                    .map(move |region_id| (store_id, region_id))
            })
            .flatten()
            .for_each(|(store_id, region_id)| {
                resp_map.entry(region_id).or_default().push(store_id);
            });
        resp_map
            .into_iter()
            .filter_map(|(region_id, stores)| {
                if region_has_quorum(&region_map[&region_id], &stores) {
                    Some(region_id)
                } else {
                    debug!("cdc cannot get quorum for resolved ts"; "region_id" => region_id, "stores" => ?stores, "region" => ?&region_map[&region_id]);
                    None
                }
            })
            .collect()
    }

    fn on_open_conn(&mut self, conn: Conn) {
        self.connections.insert(conn.get_id(), conn);
    }

    fn flush_all(&self) {
        self.connections.iter().for_each(|(_, conn)| conn.flush());
    }
}

struct Initializer {
    sched: Scheduler<Task>,

    region_id: u64,
    observe_id: ObserveID,
    downstream_id: DownstreamID,
    downstream_state: Arc<AtomicCell<DownstreamState>>,
    conn_id: ConnID,
    checkpoint_ts: TimeStamp,
    batch_size: usize,
    txn_extra_op: TxnExtraOp,

    build_resolver: bool,
}

impl Initializer {
    fn on_change_cmd(&self, mut resp: ReadResponse<RocksSnapshot>) {
        if let Some(region_snapshot) = resp.snapshot {
            assert_eq!(self.region_id, region_snapshot.get_region().get_id());
            let region = region_snapshot.get_region().clone();
            self.async_incremental_scan(region_snapshot, region);
        } else {
            assert!(
                resp.response.get_header().has_error(),
                "no snapshot and no error? {:?}",
                resp.response
            );
            let err = resp.response.take_header().take_error();
            let deregister = Deregister::Region {
                region_id: self.region_id,
                observe_id: self.observe_id,
                err: Error::Request(err),
            };
            if let Err(e) = self.sched.schedule(Task::Deregister(deregister)) {
                error!("schedule cdc task failed"; "error" => ?e);
            }
        }
    }

    fn async_incremental_scan<S: Snapshot + 'static>(&self, snap: S, region: Region) {
        let downstream_id = self.downstream_id;
        let conn_id = self.conn_id;
        let region_id = region.get_id();
        debug!("async incremental scan";
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
            .range(None, None)
            .build_delta_scanner(self.checkpoint_ts, self.txn_extra_op)
            .unwrap();
        let mut done = false;
        while !done {
            if self.downstream_state.load() != DownstreamState::Normal {
                info!("async incremental scan canceled";
                    "region_id" => region_id,
                    "downstream_id" => ?downstream_id,
                    "observe_id" => ?self.observe_id);
                return;
            }
            let entries = match Self::scan_batch(&mut scanner, self.batch_size, resolver.as_mut()) {
                Ok(res) => res,
                Err(e) => {
                    error!("cdc scan entries failed"; "error" => ?e, "region_id" => region_id);
                    // TODO: record in metrics.
                    let deregister = Deregister::Downstream {
                        region_id,
                        downstream_id,
                        conn_id,
                        err: Some(e),
                    };
                    if let Err(e) = self.sched.schedule(Task::Deregister(deregister)) {
                        error!("schedule cdc task failed"; "error" => ?e, "region_id" => region_id);
                    }
                    return;
                }
            };
            // If the last element is None, it means scanning is finished.
            if let Some(None) = entries.last() {
                done = true;
            }
            debug!("cdc scan entries"; "len" => entries.len(), "region_id" => region_id);
            fail_point!("before_schedule_incremental_scan");
            let scanned = Task::IncrementalScan {
                region_id,
                downstream_id,
                entries,
            };
            if let Err(e) = self.sched.schedule(scanned) {
                error!("schedule cdc task failed"; "error" => ?e, "region_id" => region_id);
                return;
            }
        }

        let takes = start.elapsed();
        if let Some(resolver) = resolver {
            self.finish_building_resolver(resolver, region, takes);
        }

        CDC_SCAN_DURATION_HISTOGRAM.observe(takes.as_secs_f64());
    }

    fn scan_batch<S: Snapshot>(
        scanner: &mut DeltaScanner<S>,
        batch_size: usize,
        resolver: Option<&mut Resolver>,
    ) -> Result<Vec<Option<TxnEntry>>> {
        let mut entries = Vec::with_capacity(batch_size);
        while entries.len() < entries.capacity() {
            match scanner.next_entry()? {
                Some(entry) => {
                    entries.push(Some(entry));
                }
                None => {
                    entries.push(None);
                    break;
                }
            }
        }

        if let Some(resolver) = resolver {
            // Track the locks.
            for entry in &entries {
                if let Some(TxnEntry::Prewrite { lock, .. }) = entry {
                    let (encoded_key, value) = lock;
                    let key = Key::from_encoded_slice(encoded_key).into_raw().unwrap();
                    let lock = Lock::parse(value)?;
                    match lock.lock_type {
                        LockType::Put | LockType::Delete => resolver.track_lock(lock.ts, key),
                        _ => (),
                    };
                }
            }
        }

        Ok(entries)
    }

    fn finish_building_resolver(&self, mut resolver: Resolver, region: Region, takes: Duration) {
        let observe_id = self.observe_id;
        resolver.init();
        let rts = resolver.resolve(TimeStamp::zero());
        info!(
            "resolver initialized and schedule resolver ready";
            "region_id" => region.get_id(),
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
            error!("schedule task failed"; "error" => ?e);
        }
    }
}

impl<T: 'static + RaftStoreRouter<RocksEngine>> Runnable for Endpoint<T> {
    type Task = Task;

    fn run(&mut self, task: Task) {
        debug!("run cdc task"; "task" => %task);
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
            Task::IncrementalScan {
                region_id,
                downstream_id,
                entries,
            } => {
                self.on_incremental_scan(region_id, downstream_id, entries);
            }
            Task::MultiBatch {
                multi,
                old_value_cb,
            } => self.on_multi_batch(multi, old_value_cb),
            Task::OpenConn { conn } => self.on_open_conn(conn),
            Task::RegisterMinTsEvent => self.register_min_ts_event(),
            Task::InitDownstream {
                downstream_id,
                downstream_state,
                cb,
            } => {
                debug!("downstream was initialized"; "downstream_id" => ?downstream_id);
                downstream_state
                    .compare_and_swap(DownstreamState::Uninitialized, DownstreamState::Normal);
                cb();
            }
            Task::TxnExtra(txn_extra) => {
                for (k, v) in txn_extra.old_values {
                    self.old_value_cache.cache.insert(k, v);
                }
            }
            Task::Validate(region_id, validate) => {
                validate(self.capture_regions.get(&region_id));
            }
        }
        self.flush_all();
    }
}

impl<T: 'static + RaftStoreRouter<RocksEngine>> RunnableWithTimer for Endpoint<T> {
    fn on_timeout(&mut self) {
        CDC_CAPTURED_REGION_COUNT.set(self.capture_regions.len() as i64);
        if self.min_resolved_ts != TimeStamp::max() {
            CDC_MIN_RESOLVED_TS_REGION.set(self.min_ts_region_id as i64);
            CDC_MIN_RESOLVED_TS.set(self.min_resolved_ts.physical() as i64);
        }
        self.min_resolved_ts = TimeStamp::max();
        self.min_ts_region_id = 0;

        let cache_size: usize = self
            .old_value_cache
            .cache
            .iter()
            .map(|(k, v)| k.as_encoded().len() + v.0.size())
            .sum();
        CDC_OLD_VALUE_CACHE_BYTES.set(cache_size as i64);
        CDC_OLD_VALUE_CACHE_ACCESS.add(self.old_value_cache.access_count as i64);
        CDC_OLD_VALUE_CACHE_MISS.add(self.old_value_cache.miss_count as i64);
        self.old_value_cache.access_count = 0;
        self.old_value_cache.miss_count = 0;
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
    use super::*;
    use collections::HashSet;
    use engine_traits::DATA_CFS;
    #[cfg(feature = "prost-codec")]
    use kvproto::cdcpb::event::Event as Event_oneof_event;
    use kvproto::errorpb::Error as ErrorHeader;
    use raftstore::errors::Error as RaftStoreError;
    use raftstore::store::msg::CasualMessage;
    use std::collections::BTreeMap;
    use std::fmt::Display;
    use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender};
    use tempfile::TempDir;
    use test_raftstore::MockRaftStoreRouter;
    use test_raftstore::TestPdClient;
    use tikv::storage::kv::Engine;
    use tikv::storage::txn::tests::{must_acquire_pessimistic_lock, must_prewrite_put};
    use tikv::storage::TestEngineBuilder;
    use tikv_util::config::ReadableDuration;
    use tikv_util::mpsc::batch;
    use tikv_util::worker::{dummy_scheduler, LazyWorker, ReceiverWrapper};

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

    fn mock_initializer() -> (LazyWorker<Task>, Runtime, Initializer, Receiver<Task>) {
        let (receiver_worker, rx) = new_receiver_worker();

        let pool = Builder::new()
            .threaded_scheduler()
            .thread_name("test-initializer-worker")
            .core_threads(4)
            .build()
            .unwrap();
        let downstream_state = Arc::new(AtomicCell::new(DownstreamState::Normal));
        let initializer = Initializer {
            sched: receiver_worker.scheduler(),

            region_id: 1,
            observe_id: ObserveID::new(),
            downstream_id: DownstreamID::new(),
            downstream_state,
            conn_id: ConnID::new(),
            checkpoint_ts: 1.into(),
            batch_size: 1,
            txn_extra_op: TxnExtraOp::Noop,
            build_resolver: true,
        };

        (receiver_worker, pool, initializer, rx)
    }

    fn mock_endpoint(
        cfg: &CdcConfig,
    ) -> (
        Endpoint<MockRaftStoreRouter>,
        MockRaftStoreRouter,
        ReceiverWrapper<Task>,
    ) {
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
            Arc::new(Mutex::new(StoreMeta::new(0))),
            ConcurrencyManager::new(1.into()),
            env,
            security_mgr,
        );
        (ep, raft_router, task_rx)
    }

    #[test]
    fn test_initializer_build_resolver() {
        let (mut worker, _pool, mut initializer, rx) = mock_initializer();

        let temp = TempDir::new().unwrap();
        let engine = TestEngineBuilder::new()
            .path(temp.path())
            .cfs(DATA_CFS)
            .build()
            .unwrap();

        let mut expected_locks = BTreeMap::<TimeStamp, HashSet<Arc<[u8]>>>::new();

        // Pessimistic locks should not be tracked
        for i in 0..10 {
            let k = &[b'k', i];
            let ts = TimeStamp::new(i as _);
            must_acquire_pessimistic_lock(&engine, k, k, ts, ts);
        }

        for i in 10..100 {
            let (k, v) = (&[b'k', i], &[b'v', i]);
            let ts = TimeStamp::new(i as _);
            must_prewrite_put(&engine, k, v, k, ts);
            expected_locks
                .entry(ts)
                .or_default()
                .insert(k.to_vec().into());
        }

        let region = Region::default();
        let snap = engine.snapshot(Default::default()).unwrap();

        let check_result = || loop {
            let task = rx.recv().unwrap();
            match task {
                Task::ResolverReady { resolver, .. } => {
                    assert_eq!(resolver.locks(), &expected_locks);
                    return;
                }
                Task::IncrementalScan { .. } => continue,
                t => panic!("unepxected task {} received", t),
            }
        };

        initializer.async_incremental_scan(snap.clone(), region.clone());
        check_result();
        initializer.batch_size = 1000;
        initializer.async_incremental_scan(snap.clone(), region.clone());
        check_result();

        initializer.batch_size = 10;
        initializer.async_incremental_scan(snap.clone(), region.clone());
        check_result();

        initializer.batch_size = 11;
        initializer.async_incremental_scan(snap.clone(), region.clone());
        check_result();

        initializer.build_resolver = false;
        initializer.async_incremental_scan(snap.clone(), region.clone());

        loop {
            let task = rx.recv_timeout(Duration::from_secs(1));
            match task {
                Ok(Task::IncrementalScan { .. }) => continue,
                Ok(t) => panic!("unepxected task {} received", t),
                Err(RecvTimeoutError::Timeout) => break,
                Err(e) => panic!("unexpected err {:?}", e),
            }
        }

        // Test cancellation.
        initializer.downstream_state.store(DownstreamState::Stopped);
        initializer.async_incremental_scan(snap, region);

        loop {
            let task = rx.recv_timeout(Duration::from_secs(1));
            match task {
                Ok(t) => panic!("unepxected task {} received", t),
                Err(RecvTimeoutError::Timeout) => break,
                Err(e) => panic!("unexpected err {:?}", e),
            }
        }

        worker.stop();
    }

    #[test]
    fn test_raftstore_is_busy() {
        let (tx, _rx) = batch::unbounded(1);
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
        let (mut ep, raft_router, _task_rx) = mock_endpoint(&CdcConfig {
            min_ts_interval: ReadableDuration(Duration::from_secs(60)),
            ..Default::default()
        });
        let _raft_rx = raft_router.add_region(1 /* region id */, 100 /* cap */);
        let (tx, rx) = batch::unbounded(1);

        let conn = Conn::new(tx, String::new());
        let conn_id = conn.get_id();
        ep.run(Task::OpenConn { conn });
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new("".to_string(), region_epoch.clone(), 1, conn_id, true);
        ep.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: semver::Version::new(4, 0, 6),
        });
        assert_eq!(ep.capture_regions.len(), 1);

        // duplicate request error.
        let downstream = Downstream::new("".to_string(), region_epoch.clone(), 2, conn_id, true);
        ep.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: semver::Version::new(4, 0, 6),
        });
        let cdc_event = rx.recv_timeout(Duration::from_millis(500)).unwrap();
        if let CdcEvent::Event(mut e) = cdc_event {
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

        // Compatibility error.
        let downstream = Downstream::new("".to_string(), region_epoch, 3, conn_id, true);
        ep.run(Task::Register {
            request: req,
            downstream,
            conn_id,
            version: semver::Version::new(0, 0, 0),
        });
        let cdc_event = rx.recv_timeout(Duration::from_millis(500)).unwrap();
        if let CdcEvent::Event(mut e) = cdc_event {
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
    }

    #[test]
    fn test_feature_gate() {
        let (mut ep, raft_router, _task_rx) = mock_endpoint(&CdcConfig {
            min_ts_interval: ReadableDuration(Duration::from_secs(60)),
            ..Default::default()
        });
        let _raft_rx = raft_router.add_region(1 /* region id */, 100 /* cap */);

        let (tx, rx) = batch::unbounded(1);
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
        ep.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: semver::Version::new(4, 0, 6),
        });
        let mut resolver = Resolver::new(1);
        resolver.init();
        let observe_id = ep.capture_regions[&1].id;
        ep.on_region_ready(observe_id, resolver, region.clone());
        ep.run(Task::MinTS {
            regions: vec![1],
            min_ts: TimeStamp::from(1),
        });
        let cdc_event = rx.recv_timeout(Duration::from_millis(500)).unwrap();
        if let CdcEvent::ResolvedTs(r) = cdc_event {
            assert_eq!(r.regions, vec![1]);
            assert_eq!(r.ts, 1);
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }

        // Register region 2 to the conn.
        req.set_region_id(2);
        let downstream = Downstream::new("".to_string(), region_epoch.clone(), 0, conn_id, true);
        ep.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: semver::Version::new(4, 0, 6),
        });
        let mut resolver = Resolver::new(2);
        resolver.init();
        region.set_id(2);
        let observe_id = ep.capture_regions[&2].id;
        ep.on_region_ready(observe_id, resolver, region);
        ep.run(Task::MinTS {
            regions: vec![1, 2],
            min_ts: TimeStamp::from(2),
        });
        let cdc_event = rx.recv_timeout(Duration::from_millis(500)).unwrap();
        if let CdcEvent::ResolvedTs(mut r) = cdc_event {
            r.regions.as_mut_slice().sort_unstable();
            assert_eq!(r.regions, vec![1, 2]);
            assert_eq!(r.ts, 2);
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }

        // Register region 3 to another conn which is not support batch resolved ts.
        let (tx, rx2) = batch::unbounded(1);
        let mut region = Region::default();
        region.set_id(3);
        let conn = Conn::new(tx, String::new());
        let conn_id = conn.get_id();
        ep.run(Task::OpenConn { conn });
        req.set_region_id(3);
        let downstream = Downstream::new("".to_string(), region_epoch, 3, conn_id, true);
        ep.run(Task::Register {
            request: req,
            downstream,
            conn_id,
            version: semver::Version::new(4, 0, 5),
        });
        let mut resolver = Resolver::new(3);
        resolver.init();
        region.set_id(3);
        let observe_id = ep.capture_regions[&3].id;
        ep.on_region_ready(observe_id, resolver, region);
        ep.run(Task::MinTS {
            regions: vec![1, 2, 3],
            min_ts: TimeStamp::from(3),
        });
        let cdc_event = rx.recv_timeout(Duration::from_millis(500)).unwrap();
        if let CdcEvent::ResolvedTs(mut r) = cdc_event {
            r.regions.as_mut_slice().sort_unstable();
            // Although region 3 is not register in the first conn, batch resolved ts
            // sends all region ids.
            assert_eq!(r.regions, vec![1, 2, 3]);
            assert_eq!(r.ts, 3);
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }
        let cdc_event = rx2.recv_timeout(Duration::from_millis(500)).unwrap();
        if let CdcEvent::Event(mut e) = cdc_event {
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
        let (tx, rx) = batch::unbounded(1);

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
            err: Some(Error::Request(err_header.clone())),
        };
        ep.run(Task::Deregister(deregister));
        loop {
            let cdc_event = rx.recv_timeout(Duration::from_millis(500)).unwrap();
            if let CdcEvent::Event(mut e) = cdc_event {
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
            err: Some(Error::Request(err_header.clone())),
        };
        ep.run(Task::Deregister(deregister));
        assert!(rx.recv_timeout(Duration::from_millis(200)).is_err());
        assert_eq!(ep.capture_regions.len(), 1);

        let deregister = Deregister::Downstream {
            region_id: 1,
            downstream_id: new_downstream_id,
            conn_id,
            err: Some(Error::Request(err_header.clone())),
        };
        ep.run(Task::Deregister(deregister));
        let cdc_event = rx.recv_timeout(Duration::from_millis(500)).unwrap();
        loop {
            if let CdcEvent::Event(mut e) = cdc_event {
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
        let deregister = Deregister::Region {
            region_id: 1,
            // A stale ObserveID (different from the actual one).
            observe_id: ObserveID::new(),
            err: Error::Request(err_header),
        };
        ep.run(Task::Deregister(deregister));
        match rx.recv_timeout(Duration::from_millis(500)) {
            Err(_) => (),
            Ok(other) => panic!("unknown event {:?}", other),
        }
        assert_eq!(ep.capture_regions.len(), 1);
    }
}
