// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use engine_rocks::RocksEngine;
use futures::future::{lazy, Future};
use kvproto::cdcpb::*;
use kvproto::metapb::Region;
use pd_client::PdClient;
use raftstore::coprocessor::CmdBatch;
use raftstore::store::fsm::ChangeCmd;
use raftstore::store::msg::{Callback, CasualMessage, ReadResponse};
use raftstore::store::transport::CasualRouter;
use resolved_ts::Resolver;
use tikv::storage::kv::Snapshot;
use tikv::storage::mvcc::{DeltaScanner, ScannerBuilder};
use tikv::storage::txn::TxnEntry;
use tikv::storage::txn::TxnEntryScanner;
use tikv_util::collections::HashMap;
use tikv_util::timer::SteadyTimer;
use tikv_util::worker::{Runnable, ScheduleError, Scheduler};
use tokio_threadpool::{Builder, Sender as PoolSender, ThreadPool};
use txn_types::{Key, Lock, TimeStamp};

use crate::delegate::{Delegate, Downstream, DownstreamID};
use crate::service::{Conn, ConnID};
use crate::{CdcObserver, Error, Result};

pub enum Task {
    Register {
        request: ChangeDataRequest,
        downstream: Downstream,
        conn_id: ConnID,
    },
    Deregister {
        region_id: Option<u64>,
        downstream_id: Option<DownstreamID>,
        conn_id: Option<ConnID>,
        err: Option<Error>,
    },
    OpenConn {
        conn: Conn,
    },
    MultiBatch {
        multi: Vec<CmdBatch>,
    },
    MinTS {
        min_ts: TimeStamp,
    },
    ResolverReady {
        region_id: u64,
        region: Region,
        resolver: Resolver,
    },
    IncrementalScan {
        region_id: u64,
        downstream_id: DownstreamID,
        entries: Vec<Option<TxnEntry>>,
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
                .field("register request", request)
                .field("request", request)
                .field("id", &downstream.get_id())
                .field("conn_id", conn_id)
                .finish(),
            Task::Deregister {
                ref region_id,
                ref downstream_id,
                ref conn_id,
                ref err,
            } => de
                .field("deregister", &"")
                .field("region_id", region_id)
                .field("err", err)
                .field("downstream_id", downstream_id)
                .field("conn_id", conn_id)
                .finish(),
            Task::OpenConn { ref conn } => de.field("conn_id", &conn.get_id()).finish(),
            Task::MultiBatch { multi } => de.field("multibatch", &multi.len()).finish(),
            Task::MinTS { ref min_ts } => de.field("min_ts", min_ts).finish(),
            Task::ResolverReady { ref region_id, .. } => de.field("region_id", region_id).finish(),
            Task::IncrementalScan {
                ref region_id,
                ref downstream_id,
                ref entries,
            } => de
                .field("region_id", &region_id)
                .field("downstream", &downstream_id)
                .field("scan_entries", &entries.len())
                .finish(),
            Task::Validate(region_id, _) => de.field("region_id", &region_id).finish(),
        }
    }
}

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

    workers: ThreadPool,
}

impl<T: CasualRouter<RocksEngine>> Endpoint<T> {
    pub fn new(
        pd_client: Arc<dyn PdClient>,
        scheduler: Scheduler<Task>,
        raft_router: T,
        observer: CdcObserver,
    ) -> Endpoint<T> {
        let workers = Builder::new().name_prefix("cdcwkr").pool_size(4).build();
        let ep = Endpoint {
            capture_regions: HashMap::default(),
            connections: HashMap::default(),
            scheduler,
            pd_client,
            timer: SteadyTimer::default(),
            workers,
            raft_router,
            observer,
            scan_batch_size: 1024,
            min_ts_interval: Duration::from_secs(10),
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

    fn on_deregister(
        &mut self,
        region_id: Option<u64>,
        id: Option<DownstreamID>,
        conn_id: Option<ConnID>,
        err: Option<Error>,
    ) {
        info!("cdc deregister region";
            "region_id" => region_id,
            "downstream_id" => ?id,
            "conn_id" => ?conn_id,
            "error" => ?err);
        match (id, err, conn_id) {
            (Some(id), err, Some(conn_id)) => {
                // The peer wants to deregister
                let region_id = region_id.unwrap();
                let mut is_last = false;
                if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
                    is_last = delegate.unsubscribe(id, err);
                }
                if let Some(conn) = self.connections.get_mut(&conn_id) {
                    if let Some(downstream_id) = conn.downstream_id(region_id) {
                        if downstream_id == id {
                            conn.unsubscribe(region_id);
                        }
                    }
                }
                if is_last {
                    self.capture_regions.remove(&region_id);
                    // Do not continue to observe the events of the region.
                    self.observer.unsubscribe_region(region_id);
                }
            }
            (None, Some(err), None) => {
                // Something went wrong, deregister all downstreams of the region.
                let region_id = region_id.unwrap();
                if let Some(mut delegate) = self.capture_regions.remove(&region_id) {
                    delegate.stop(err);
                }
                self.connections
                    .iter_mut()
                    .for_each(|(_, conn)| conn.unsubscribe(region_id));
                // Do not continue to observe the events of the region.
                self.observer.unsubscribe_region(region_id);
            }
            (None, None, Some(conn_id)) => {
                // The connection is closed, deregister all downstreams of the connection.
                if let Some(conn) = self.connections.remove(&conn_id) {
                    conn.take_downstreams()
                        .into_iter()
                        .for_each(|(region_id, downstream_id)| {
                            if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
                                if delegate.unsubscribe(downstream_id, None) {
                                    self.capture_regions.remove(&region_id);
                                    // Do not continue to observe the events of the region.
                                    self.observer.unsubscribe_region(region_id);
                                }
                            }
                        });
                }
            }
            _ => unreachable!(),
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
                error!("register for a nonexistent connection"; "region_id" => region_id, "conn_id" => ?conn_id);
                return;
            }
        };
        downstream.set_sink(conn.get_sink());
        if !conn.subscribe(request.get_region_id(), downstream.get_id()) {
            downstream.sink_duplicate_error(request.get_region_id());
            return;
        }

        info!("cdc register region"; "region_id" => region_id, "conn_id" => ?conn.get_id(), "downstream_id" => ?downstream.get_id());
        let mut enabled = None;
        let delegate = self.capture_regions.entry(region_id).or_insert_with(|| {
            let d = Delegate::new(region_id);
            enabled = Some(d.enabled());
            d
        });

        let downstream_id = downstream.get_id();
        let checkpoint_ts = request.checkpoint_ts;
        let sched = self.scheduler.clone();
        let workers = self.workers.sender().clone();
        let batch_size = self.scan_batch_size;

        let init = Initializer {
            workers,
            sched,
            region_id,
            downstream_id,
            conn_id,
            checkpoint_ts: checkpoint_ts.into(),
            batch_size,
            build_resolver: enabled.is_some(),
        };
        if !delegate.subscribe(downstream) {
            conn.unsubscribe(request.get_region_id());
            return;
        }
        let change_cmd = if let Some(enabled) = enabled {
            // The region has never been registered.
            // Subscribe the change events of the region.
            self.observer.subscribe_region(region_id);

            ChangeCmd::RegisterObserver {
                region_id,
                region_epoch: request.take_region_epoch(),
                enabled,
            }
        } else {
            ChangeCmd::Snapshot {
                region_id,
                region_epoch: request.take_region_epoch(),
            }
        };
        if let Err(e) = self.raft_router.send(
            region_id,
            CasualMessage::CaptureChange {
                cmd: change_cmd,
                callback: Callback::Read(Box::new(move |resp: ReadResponse<_>| {
                    init.on_change_cmd(resp);
                })),
            },
        ) {
            error!("cdc send capture change cmd failed"; "region_id" => region_id, "error" => ?e);
            let deregister = Task::Deregister {
                region_id: Some(region_id),
                downstream_id: Some(downstream_id),
                conn_id: Some(conn_id),
                err: Some(Error::Request(e.into())),
            };
            self.scheduler.schedule(deregister).unwrap();
        }
    }

    pub fn on_multi_batch(&mut self, multi: Vec<CmdBatch>) {
        for batch in multi {
            let region_id = batch.region_id;
            if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
                if delegate.has_failed() {
                    // Skip the batch if the delegate has failed.
                    continue;
                }
                if let Err(e) = delegate.on_batch(batch) {
                    assert!(delegate.has_failed());
                    // Delegate has error, deregister the corresponding region.
                    if let Err(e) = self.scheduler.schedule(Task::Deregister {
                        region_id: Some(region_id),
                        downstream_id: None,
                        conn_id: None,
                        err: Some(e),
                    }) {
                        error!("schedule cdc task failed"; "error" => ?e);
                    }
                }
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

    fn on_region_ready(&mut self, region_id: u64, resolver: Resolver, region: Region) {
        if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
            if let Err(e) = delegate.on_region_ready(resolver, region) {
                assert!(delegate.has_failed());
                // Delegate has error, deregister the corresponding region.
                if let Err(e) = self.scheduler.schedule(Task::Deregister {
                    region_id: Some(region_id),
                    downstream_id: None,
                    conn_id: None,
                    err: Some(e),
                }) {
                    error!("schedule cdc task failed"; "error" => ?e);
                }
            }
        } else {
            warn!("region not found on region ready (finish building resolver)"; "region_id" => region_id);
        }
    }

    fn on_min_ts(&mut self, min_ts: TimeStamp) {
        for delegate in self.capture_regions.values_mut() {
            delegate.on_min_ts(min_ts);
        }
        self.register_min_ts_event();
    }

    fn register_min_ts_event(&self) {
        let timeout = self.timer.delay(self.min_ts_interval);
        let tso = self.pd_client.get_tso();
        let scheduler = self.scheduler.clone();
        let fut = tso.join(timeout.map_err(|_| unreachable!())).then(
            move |tso: pd_client::Result<(TimeStamp, ())>| {
                // Ignore get tso errors since we will retry every `min_ts_interval`.
                let (min_ts, _) = tso.unwrap_or((TimeStamp::default(), ()));
                match scheduler.schedule(Task::MinTS { min_ts }) {
                    Ok(_) | Err(ScheduleError::Stopped(_)) => Ok(()),
                    // Must schedule `MinTS` event otherwise resolved ts can not
                    // advance normally.
                    err => panic!(
                        "failed to schedule min_ts event, min_ts: {}, error: {:?}",
                        min_ts, err
                    ),
                }
            },
        );
        self.pd_client.spawn(Box::new(fut) as _);
    }

    fn on_open_conn(&mut self, conn: Conn) {
        self.connections.insert(conn.get_id(), conn);
    }

    fn flush_all(&self) {
        self.connections.iter().for_each(|(_, conn)| conn.flush());
    }
}

struct Initializer {
    workers: PoolSender,
    sched: Scheduler<Task>,

    region_id: u64,
    downstream_id: DownstreamID,
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
            let deregister = Task::Deregister {
                region_id: Some(self.region_id),
                downstream_id: Some(self.downstream_id),
                conn_id: Some(self.conn_id),
                err: Some(Error::Request(err)),
            };
            self.sched.schedule(deregister).unwrap();
        }
    }

    fn async_incremental_scan<S: Snapshot + 'static>(&self, snap: S, region: Region) {
        let downstream_id = self.downstream_id;
        let conn_id = self.conn_id;
        let sched = self.sched.clone();
        let batch_size = self.batch_size;
        let checkpoint_ts = self.checkpoint_ts;
        let build_resolver = self.build_resolver;
        info!("async incremental scan";
            "region_id" => region.get_id(),
            "downstream_id" => ?downstream_id);

        // spawn the task to a thread pool.
        // TODO: Add a cancellation mechanism so that the scanning can be canceled if it doesn't
        // finish when the region is deregistered.
        let region_id = region.get_id();
        self.workers
            .spawn(lazy(move || {
                let mut resolver = if build_resolver {
                    Some(Resolver::new())
                } else {
                    None
                };

                fail_point!("cdc_incremental_scan_start");

                // Time range: (checkpoint_ts, current]
                let current = TimeStamp::max();
                let mut scanner = ScannerBuilder::new(snap, current, false)
                    .range(None, None)
                    .build_delta_scanner(checkpoint_ts)
                    .unwrap();
                let mut done = false;
                while !done {
                    let entries =
                        match Self::scan_batch(&mut scanner, batch_size, resolver.as_mut()) {
                            Ok(res) => res,
                            Err(e) => {
                                error!("cdc scan entries failed"; "error" => ?e);
                                // TODO: record in metrics.
                                if let Err(e) = sched.schedule(Task::Deregister {
                                    region_id: Some(region_id),
                                    downstream_id: Some(downstream_id),
                                    conn_id: Some(conn_id),
                                    err: Some(e),
                                }) {
                                    error!("schedule task failed"; "error" => ?e);
                                }
                                return Ok(());
                            }
                        };
                    // If the last element is None, it means scanning is finished.
                    if let Some(None) = entries.last() {
                        done = true;
                    }
                    debug!("cdc scan entries"; "len" => entries.len());
                    fail_point!("before_schedule_incremental_scan");
                    let scanned = Task::IncrementalScan {
                        region_id,
                        downstream_id,
                        entries,
                    };
                    if let Err(e) = sched.schedule(scanned) {
                        error!("schedule task failed"; "error" => ?e);
                        return Ok(());
                    }
                }

                if let Some(resolver) = resolver {
                    Self::finish_building_resolver(resolver, region, sched);
                }

                Ok(())
            }))
            .unwrap();
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
                    resolver.track_lock(lock.ts, key);
                }
            }
        }

        Ok(entries)
    }

    fn finish_building_resolver(mut resolver: Resolver, region: Region, sched: Scheduler<Task>) {
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
                "lock_count" => resolver.locks().len()
            );
        }

        info!("schedule resolver ready"; "region_id" => region.get_id());
        if let Err(e) = sched.schedule(Task::ResolverReady {
            region_id: region.get_id(),
            resolver,
            region,
        }) {
            error!("schedule task failed"; "error" => ?e);
        }
    }
}

impl<T: CasualRouter<RocksEngine>> Runnable<Task> for Endpoint<T> {
    fn run(&mut self, task: Task) {
        debug!("run cdc task"; "task" => %task);
        match task {
            Task::MinTS { min_ts } => self.on_min_ts(min_ts),
            Task::Register {
                request,
                downstream,
                conn_id,
            } => self.on_register(request, downstream, conn_id),
            Task::ResolverReady {
                region_id,
                resolver,
                region,
            } => self.on_region_ready(region_id, resolver, region),
            Task::Deregister {
                region_id,
                downstream_id,
                conn_id,
                err,
            } => self.on_deregister(region_id, downstream_id, conn_id, err),
            Task::IncrementalScan {
                region_id,
                downstream_id,
                entries,
            } => {
                self.on_incremental_scan(region_id, downstream_id, entries);
            }
            Task::MultiBatch { multi } => self.on_multi_batch(multi),
            Task::OpenConn { conn } => self.on_open_conn(conn),
            Task::Validate(region_id, validate) => {
                validate(self.capture_regions.get(&region_id));
            }
        }
        self.flush_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_traits::DATA_CFS;
    use kvproto::errorpb::Error as ErrorHeader;
    use kvproto::kvrpcpb::Context;
    use std::collections::BTreeMap;
    use std::fmt::Display;
    use std::sync::mpsc::{channel, sync_channel, Receiver, RecvTimeoutError, Sender};
    use tempfile::TempDir;
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

        let initializer = Initializer {
            workers: pool.sender().clone(),
            sched: receiver_worker.scheduler(),

            region_id: 1,
            downstream_id: DownstreamID::new(),
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
        initializer.async_incremental_scan(snap, region);

        loop {
            let task = rx.recv_timeout(Duration::from_secs(1));
            match task {
                Ok(Task::IncrementalScan { .. }) => continue,
                Ok(t) => panic!("unepxected task {} received", t),
                Err(RecvTimeoutError::Timeout) => break,
                Err(e) => panic!("unexpected err {:?}", e),
            }
        }

        worker.stop().unwrap().join().unwrap();
    }

    #[test]
    fn test_deregister() {
        let (task_sched, _task_rx) = dummy_scheduler();
        let (raft_tx, _raft_rx) = sync_channel::<(u64, CasualMessage<RocksEngine>)>(16);
        let observer = CdcObserver::new(task_sched.clone());
        let pd_client = Arc::new(TestPdClient::new(0, true));
        let mut ep = Endpoint::new(pd_client, task_sched, raft_tx, observer);
        let (tx, rx) = batch::unbounded(1);

        let conn = Conn::new(tx);
        let conn_id = conn.get_id();
        ep.run(Task::OpenConn { conn });
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new("".to_string(), region_epoch.clone());
        let downstream_id = downstream.get_id();
        ep.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
        });
        assert_eq!(ep.capture_regions.len(), 1);

        let mut err_header = ErrorHeader::default();
        err_header.set_not_leader(Default::default());
        ep.run(Task::Deregister {
            region_id: Some(1),
            downstream_id: Some(downstream_id),
            conn_id: Some(conn_id),
            err: Some(Error::Request(err_header.clone())),
        });
        let (_, mut change_data_event) = rx.recv_timeout(Duration::from_millis(500)).unwrap();
        let event = change_data_event.event.take().unwrap();
        match event {
            Event_oneof_event::Error(err) => assert!(err.has_not_leader()),
            _ => panic!("unknown event"),
        }
        assert_eq!(ep.capture_regions.len(), 0);

        let downstream = Downstream::new("".to_string(), region_epoch);
        let downstream_id1 = downstream.get_id();
        ep.run(Task::Register {
            request: req,
            downstream,
            conn_id,
        });
        assert_eq!(ep.capture_regions.len(), 1);

        ep.run(Task::Deregister {
            region_id: Some(1),
            downstream_id: Some(downstream_id),
            conn_id: Some(conn_id),
            err: Some(Error::Request(err_header.clone())),
        });
        assert!(rx.recv_timeout(Duration::from_millis(200)).is_err());
        assert_eq!(ep.capture_regions.len(), 1);

        ep.run(Task::Deregister {
            region_id: Some(1),
            downstream_id: Some(downstream_id1),
            conn_id: Some(conn_id),
            err: Some(Error::Request(err_header)),
        });
        let (_, mut change_data_event) = rx.recv_timeout(Duration::from_millis(500)).unwrap();
        let event = change_data_event.event.take().unwrap();
        match event {
            Event_oneof_event::Error(err) => assert!(err.has_not_leader()),
            _ => panic!("unknown event"),
        }
        assert_eq!(ep.capture_regions.len(), 0);
    }
}
