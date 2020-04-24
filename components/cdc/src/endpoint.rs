// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use engine_rocks::RocksEngine;
use futures::future::Future;
use kvproto::cdcpb::*;
use kvproto::metapb::Region;
use pd_client::PdClient;
use raftstore::coprocessor::CmdBatch;
use raftstore::router::RaftStoreRouter;
use raftstore::store::fsm::{ChangeCmd, ObserveID};
use raftstore::store::msg::{Callback, ReadResponse, SignificantMsg};
use resolved_ts::Resolver;
use tikv::storage::kv::Snapshot;
use tikv::storage::mvcc::{DeltaScanner, ScannerBuilder};
use tikv::storage::txn::TxnEntry;
use tikv::storage::txn::TxnEntryScanner;
use tikv_util::collections::HashMap;
use tikv_util::time::Instant;
use tikv_util::timer::{SteadyTimer, Timer};
use tikv_util::worker::{Runnable, RunnableWithTimer, ScheduleError, Scheduler};
use tokio_threadpool::{Builder, ThreadPool};
use txn_types::{Key, Lock, LockType, TimeStamp};

use crate::delegate::{Delegate, Downstream, DownstreamID, DownstreamState};
use crate::metrics::*;
use crate::service::{Conn, ConnID};
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

impl fmt::Display for Deregister {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

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

pub enum Task {
    Register {
        request: ChangeDataRequest,
        downstream: Downstream,
        conn_id: ConnID,
    },
    Deregister(Deregister),
    OpenConn {
        conn: Conn,
    },
    MultiBatch {
        multi: Vec<CmdBatch>,
    },
    MinTS {
        region_id: u64,
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
        downstream_state: DownstreamState,
        cb: InitCallback,
    },
    Validate(u64, Box<dyn FnOnce(Option<&Delegate>) + Send>),
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut de = f.debug_struct("CdcTask");
        match self {
            Task::Register {
                ref request,
                ref downstream,
                ref conn_id,
                ..
            } => de
                .field("type", &"register")
                .field("register request", request)
                .field("request", request)
                .field("id", &downstream.get_id())
                .field("conn_id", conn_id)
                .finish(),
            Task::Deregister(deregister) => de
                .field("type", &"deregister")
                .field("deregister", deregister)
                .finish(),
            Task::OpenConn { ref conn } => de
                .field("type", &"open_conn")
                .field("conn_id", &conn.get_id())
                .finish(),
            Task::MultiBatch { multi } => de
                .field("type", &"multibatch")
                .field("multibatch", &multi.len())
                .finish(),
            Task::MinTS {
                ref region_id,
                ref min_ts,
            } => de
                .field("type", &"mit_ts")
                .field("region_id", region_id)
                .field("min_ts", min_ts)
                .finish(),
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
    tso_worker: ThreadPool,

    workers: ThreadPool,

    min_resolved_ts: TimeStamp,
    min_ts_region_id: u64,
}

impl<T: 'static + RaftStoreRouter<RocksEngine>> Endpoint<T> {
    pub fn new(
        pd_client: Arc<dyn PdClient>,
        scheduler: Scheduler<Task>,
        raft_router: T,
        observer: CdcObserver,
    ) -> Endpoint<T> {
        let workers = Builder::new().name_prefix("cdcwkr").pool_size(4).build();
        let tso_worker = Builder::new().name_prefix("tso").pool_size(1).build();
        let ep = Endpoint {
            capture_regions: HashMap::default(),
            connections: HashMap::default(),
            scheduler,
            pd_client,
            tso_worker,
            timer: SteadyTimer::default(),
            workers,
            raft_router,
            observer,
            scan_batch_size: 1024,
            min_ts_interval: Duration::from_secs(1),
            min_resolved_ts: TimeStamp::max(),
            min_ts_region_id: 0,
        };
        ep.register_min_ts_event();
        ep
    }

    pub fn new_timer(&self) -> Timer<()> {
        // Currently there is only one timeout for CDC.
        let cdc_timer_cap = 1;
        let mut timer = Timer::new(cdc_timer_cap);
        timer.add_task(Duration::from_millis(METRICS_FLUSH_INTERVAL), ());
        timer
    }

    pub fn set_min_ts_interval(&mut self, dur: Duration) {
        self.min_ts_interval = dur;
    }

    pub fn set_scan_batch_size(&mut self, scan_batch_size: usize) {
        self.scan_batch_size = scan_batch_size;
    }

    fn on_deregister(&mut self, deregister: Deregister) {
        info!("cdc deregister region"; "deregister" => ?deregister);
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
                }
                self.connections
                    .iter_mut()
                    .for_each(|(_, conn)| conn.unsubscribe(region_id));
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
    ) {
        let region_id = request.region_id;
        let conn = match self.connections.get_mut(&conn_id) {
            Some(conn) => conn,
            None => {
                error!("register for a nonexistent connection";
                    "region_id" => region_id, "conn_id" => ?conn_id);
                return;
            }
        };
        downstream.set_sink(conn.get_sink());
        if !conn.subscribe(request.get_region_id(), downstream.get_id()) {
            downstream.sink_duplicate_error(request.get_region_id());
            error!("duplicate register";
                "region_id" => region_id,
                "downstream_id" => ?downstream.get_id());
            return;
        }

        info!("cdc register region";
            "region_id" => region_id,
            "conn_id" => ?conn.get_id(),
            "downstream_id" => ?downstream.get_id());
        let mut is_new_delegate = false;
        let delegate = self.capture_regions.entry(region_id).or_insert_with(|| {
            let d = Delegate::new(region_id);
            is_new_delegate = true;
            d
        });

        let downstream_id = downstream.get_id();
        let downstream_state = downstream.get_state();
        let checkpoint_ts = request.checkpoint_ts;
        let sched = self.scheduler.clone();
        let batch_size = self.scan_batch_size;

        let init = Initializer {
            sched,
            region_id,
            conn_id,
            downstream_id,
            batch_size,
            observe_id: delegate.id,
            downstream_state: downstream_state.clone(),
            checkpoint_ts: checkpoint_ts.into(),
            build_resolver: is_new_delegate,
        };
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
        self.workers.spawn(fut.then(move |res| {
            match res {
                Ok(resp) => init.on_change_cmd(resp),
                Err(e) => deregister_downstream(Error::Other(box_err!(e))),
            };
            Ok(())
        }));
    }

    pub fn on_multi_batch(&mut self, multi: Vec<CmdBatch>) {
        for batch in multi {
            let region_id = batch.region_id;
            let mut deregister = None;
            if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
                if delegate.has_failed() {
                    // Skip the batch if the delegate has failed.
                    continue;
                }
                if let Err(e) = delegate.on_batch(batch) {
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
                if let Err(e) = delegate.on_region_ready(resolver, region) {
                    assert!(delegate.has_failed());
                    // Delegate has error, deregister the corresponding region.
                    let deregister = Deregister::Region {
                        region_id,
                        observe_id: delegate.id,
                        err: e,
                    };
                    self.on_deregister(deregister);
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

    fn on_min_ts(&mut self, region_id: u64, min_ts: TimeStamp) {
        if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
            if let Some(resolved_ts) = delegate.on_min_ts(min_ts) {
                if resolved_ts < self.min_resolved_ts {
                    self.min_resolved_ts = resolved_ts;
                    self.min_ts_region_id = region_id;
                }
            }
        }
    }

    fn register_min_ts_event(&self) {
        let timeout = self.timer.delay(self.min_ts_interval);
        let tso = self.pd_client.get_tso();
        let scheduler = self.scheduler.clone();
        let raft_router = self.raft_router.clone();
        let regions: Vec<(u64, ObserveID)> = self
            .capture_regions
            .iter()
            .map(|(region_id, delegate)| (*region_id, delegate.id))
            .collect();
        let fut = tso.join(timeout.map_err(|_| unreachable!())).then(
            move |tso: pd_client::Result<(TimeStamp, ())>| {
                // Ignore get tso errors since we will retry every `min_ts_interval`.
                let (min_ts, _) = tso.unwrap_or((TimeStamp::default(), ()));
                // TODO: send a message to raftstore would consume too much cpu time,
                // try to handle it outside raftstore.
                for (region_id, observe_id) in regions {
                    let scheduler_clone = scheduler.clone();
                    if let Err(e) = raft_router.significant_send(
                        region_id,
                        SignificantMsg::LeaderCallback(Callback::Read(Box::new(move |resp| {
                            if !resp.response.get_header().has_error() {
                                match scheduler_clone.schedule(Task::MinTS { region_id, min_ts }) {
                                    Ok(_) | Err(ScheduleError::Stopped(_)) => (),
                                    Err(err) => panic!(
                                        "failed to schedule min_ts event, min_ts: {}, error: {:?}",
                                        min_ts, err
                                    ),
                                }
                            }
                        }))),
                    ) {
                        warn!(
                            "send LeaderCallback for advancing resolved ts failed";
                            "err" => ?e,
                            "min_ts" => min_ts,
                        );
                        let deregister = Deregister::Region {
                            observe_id,
                            region_id,
                            err: Error::Request(e.into()),
                        };
                        if let Err(e) = scheduler.schedule(Task::Deregister(deregister)) {
                            error!("schedule cdc task failed"; "error" => ?e);
                        }
                    }
                }
                match scheduler.schedule(Task::RegisterMinTsEvent) {
                    Ok(_) | Err(ScheduleError::Stopped(_)) => (),
                    // Must schedule `RegisterMinTsEvent` event otherwise resolved ts can not
                    // advance normally.
                    Err(err) => panic!(
                        "failed to schedule regiester min ts event, error: {:?}",
                        err
                    ),
                }
                Ok(())
            },
        );
        self.tso_worker.spawn(fut);
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
    downstream_state: DownstreamState,
    conn_id: ConnID,
    checkpoint_ts: TimeStamp,
    batch_size: usize,

    build_resolver: bool,
}

impl Initializer {
    fn on_change_cmd(&self, mut resp: ReadResponse<RocksEngine>) {
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
        info!("async incremental scan";
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
            .build_delta_scanner(self.checkpoint_ts)
            .unwrap();
        let mut done = false;
        while !done {
            if !self.downstream_state.is_normal() {
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

        if let Some(resolver) = resolver {
            Self::finish_building_resolver(self.observe_id, resolver, region, self.sched.clone());
        }

        CDC_SCAN_DURATION_HISTOGRAM.observe(start.elapsed().as_secs_f64());
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

    fn finish_building_resolver(
        observe_id: ObserveID,
        mut resolver: Resolver,
        region: Region,
        sched: Scheduler<Task>,
    ) {
        resolver.init();
        if resolver.locks().is_empty() {
            info!(
                "no lock found";
                "region_id" => region.get_id()
            );
        } else {
            let rts = resolver.resolve(TimeStamp::zero());
            info!(
                "resolver initialized";
                "region_id" => region.get_id(),
                "resolved_ts" => rts,
                "lock_count" => resolver.locks().len(),
                "observe_id" => ?observe_id,
            );
        }

        fail_point!("before_schedule_resolver_ready");
        info!("schedule resolver ready"; "region_id" => region.get_id(), "observe_id" => ?observe_id);
        if let Err(e) = sched.schedule(Task::ResolverReady {
            observe_id,
            resolver,
            region,
        }) {
            error!("schedule task failed"; "error" => ?e);
        }
    }
}

impl<T: 'static + RaftStoreRouter<RocksEngine>> Runnable<Task> for Endpoint<T> {
    fn run(&mut self, task: Task) {
        debug!("run cdc task"; "task" => %task);
        match task {
            Task::MinTS { region_id, min_ts } => self.on_min_ts(region_id, min_ts),
            Task::Register {
                request,
                downstream,
                conn_id,
            } => self.on_register(request, downstream, conn_id),
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
            Task::MultiBatch { multi } => self.on_multi_batch(multi),
            Task::OpenConn { conn } => self.on_open_conn(conn),
            Task::RegisterMinTsEvent => self.register_min_ts_event(),
            Task::InitDownstream {
                downstream_id,
                downstream_state,
                cb,
            } => {
                debug!("downstream was initialized"; "downstream_id" => ?downstream_id);
                downstream_state.uninitialized_to_normal();
                cb();
            }
            Task::Validate(region_id, validate) => {
                validate(self.capture_regions.get(&region_id));
            }
        }
        self.flush_all();
    }
}

impl<T: 'static + RaftStoreRouter<RocksEngine>> RunnableWithTimer<Task, ()> for Endpoint<T> {
    fn on_timeout(&mut self, timer: &mut Timer<()>, _: ()) {
        CDC_CAPTURED_REGION_COUNT.set(self.capture_regions.len() as i64);
        if self.min_resolved_ts != TimeStamp::max() {
            CDC_MIN_RESOLVED_TS_REGION.set(self.min_ts_region_id as i64);
            CDC_MIN_RESOLVED_TS.set(self.min_resolved_ts.physical() as i64);
        }
        self.min_resolved_ts = TimeStamp::max();
        self.min_ts_region_id = 0;

        timer.add_task(Duration::from_millis(METRICS_FLUSH_INTERVAL), ());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_traits::DATA_CFS;
    #[cfg(feature = "prost-codec")]
    use kvproto::cdcpb::event::Event as Event_oneof_event;
    use kvproto::errorpb::Error as ErrorHeader;
    use kvproto::kvrpcpb::Context;
    use raftstore::errors::Error as RaftStoreError;
    use raftstore::store::msg::CasualMessage;
    use std::collections::BTreeMap;
    use std::fmt::Display;
    use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender};
    use tempfile::TempDir;
    use test_raftstore::MockRaftStoreRouter;
    use test_raftstore::TestPdClient;
    use tikv::storage::kv::Engine;
    use tikv::storage::mvcc::tests::*;
    use tikv::storage::TestEngineBuilder;
    use tikv_util::collections::HashSet;
    use tikv_util::mpsc::batch;
    use tikv_util::worker::{dummy_scheduler, Builder as WorkerBuilder, Worker};

    struct ReceiverRunnable<T> {
        tx: Sender<T>,
    }

    impl<T: Display> Runnable<T> for ReceiverRunnable<T> {
        fn run(&mut self, task: T) {
            self.tx.send(task).unwrap();
        }
    }

    fn new_receiver_worker<T: Display + Send + 'static>() -> (Worker<T>, Receiver<T>) {
        let (tx, rx) = channel();
        let runnable = ReceiverRunnable { tx };
        let mut worker = WorkerBuilder::new("test-receiver-worker").create();
        worker.start(runnable).unwrap();
        (worker, rx)
    }

    fn mock_initializer() -> (Worker<Task>, ThreadPool, Initializer, Receiver<Task>) {
        let (receiver_worker, rx) = new_receiver_worker();

        let pool = Builder::new()
            .name_prefix("test-initializer-worker")
            .pool_size(4)
            .build();
        let downstream_state = DownstreamState::new();
        downstream_state.set_normal();

        let initializer = Initializer {
            sched: receiver_worker.scheduler(),

            region_id: 1,
            observe_id: ObserveID::new(),
            downstream_id: DownstreamID::new(),
            downstream_state,
            conn_id: ConnID::new(),
            checkpoint_ts: 1.into(),
            batch_size: 1,

            build_resolver: true,
        };

        (receiver_worker, pool, initializer, rx)
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

        let mut expected_locks = BTreeMap::<TimeStamp, HashSet<Vec<u8>>>::new();

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
            expected_locks.entry(ts).or_default().insert(k.to_vec());
        }

        let region = Region::default();
        let snap = engine.snapshot(&Context::default()).unwrap();

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
        initializer.downstream_state.set_stopped();
        initializer.async_incremental_scan(snap, region);

        loop {
            let task = rx.recv_timeout(Duration::from_secs(1));
            match task {
                Ok(t) => panic!("unepxected task {} received", t),
                Err(RecvTimeoutError::Timeout) => break,
                Err(e) => panic!("unexpected err {:?}", e),
            }
        }

        worker.stop().unwrap().join().unwrap();
    }

    #[test]
    fn test_raftstore_is_busy() {
        let (task_sched, task_rx) = dummy_scheduler();
        let raft_router = MockRaftStoreRouter::new();
        let observer = CdcObserver::new(task_sched.clone());
        let pd_client = Arc::new(TestPdClient::new(0, true));
        let mut ep = Endpoint::new(pd_client, task_sched, raft_router.clone(), observer);
        let (tx, _rx) = batch::unbounded(1);

        // Fill the channel.
        let _raft_rx = raft_router.add_region(1 /* region id */, 1 /* cap */);
        loop {
            if let Err(RaftStoreError::Transport(_)) =
                raft_router.casual_send(1, CasualMessage::ClearRegionSize)
            {
                break;
            }
        }
        // Make sure channel is full.
        raft_router
            .casual_send(1, CasualMessage::ClearRegionSize)
            .unwrap_err();

        let conn = Conn::new(tx);
        let conn_id = conn.get_id();
        ep.run(Task::OpenConn { conn });
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new("".to_string(), region_epoch, 0);
        ep.run(Task::Register {
            request: req,
            downstream,
            conn_id,
        });
        assert_eq!(ep.capture_regions.len(), 1);

        for _ in 0..5 {
            if let Ok(Some(Task::Deregister(Deregister::Downstream { err, .. }))) =
                task_rx.recv_timeout(Duration::from_secs(1))
            {
                if let Some(Error::Request(err)) = err {
                    assert!(!err.has_server_is_busy());
                }
            }
        }
    }

    #[test]
    fn test_deregister() {
        let (task_sched, _task_rx) = dummy_scheduler();
        let raft_router = MockRaftStoreRouter::new();
        let _raft_rx = raft_router.add_region(1 /* region id */, 100 /* cap */);
        let observer = CdcObserver::new(task_sched.clone());
        let pd_client = Arc::new(TestPdClient::new(0, true));
        let mut ep = Endpoint::new(pd_client, task_sched, raft_router, observer);
        let (tx, rx) = batch::unbounded(1);

        let conn = Conn::new(tx);
        let conn_id = conn.get_id();
        ep.run(Task::OpenConn { conn });
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new("".to_string(), region_epoch.clone(), 0);
        let downstream_id = downstream.get_id();
        ep.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
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
        let (_, mut change_data_event) = rx.recv_timeout(Duration::from_millis(500)).unwrap();
        let event = change_data_event.event.take().unwrap();
        match event {
            Event_oneof_event::Error(err) => assert!(err.has_not_leader()),
            _ => panic!("unknown event"),
        }
        assert_eq!(ep.capture_regions.len(), 0);

        let downstream = Downstream::new("".to_string(), region_epoch.clone(), 0);
        let new_downstream_id = downstream.get_id();
        ep.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
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
        let (_, mut change_data_event) = rx.recv_timeout(Duration::from_millis(500)).unwrap();
        let event = change_data_event.event.take().unwrap();
        match event {
            Event_oneof_event::Error(err) => assert!(err.has_not_leader()),
            _ => panic!("unknown event"),
        }
        assert_eq!(ep.capture_regions.len(), 0);

        // Stale deregister should be filtered.
        let downstream = Downstream::new("".to_string(), region_epoch, 0);
        ep.run(Task::Register {
            request: req,
            downstream,
            conn_id,
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
            _ => panic!("unknown event"),
        }
        assert_eq!(ep.capture_regions.len(), 1);
    }
}
