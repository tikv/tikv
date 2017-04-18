// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::mpsc::Sender;
use std::boxed::Box;
use std::net::SocketAddr;
use mio::{Handler, EventLoop, EventLoopConfig};
use grpc::server::GrpcServerConf;
use kvproto::tikvpb_grpc::*;
use util::worker::{Stopped, Worker};
use util::worker::FutureWorker;
use util::transport::SendCh;
use util::{HashMap, HashSet};
use storage::Storage;
use raftstore::store::{SnapshotStatusMsg, SnapManager};
use raft::SnapshotStatus;

use super::{Msg, ConnData};
use super::{Result, Config};
use super::handle::Handle;
use super::raft_client::{SendTask, SendRunner};
use super::transport::RaftStoreRouter;
use super::resolve::StoreAddrResolver;
use super::snap::{Task as SnapTask, Runner as SnapHandler};
use super::metrics::*;

pub fn create_event_loop<T, S>(config: &Config) -> Result<EventLoop<Server<T, S>>>
    where T: RaftStoreRouter,
          S: StoreAddrResolver
{
    let mut loop_config = EventLoopConfig::new();
    loop_config.notify_capacity(config.notify_capacity);
    loop_config.messages_per_tick(config.messages_per_tick);
    let el = try!(EventLoop::configured(loop_config));
    Ok(el)
}

// A helper structure to bundle all senders for messages to raftstore.
pub struct ServerChannel<T: RaftStoreRouter + 'static> {
    pub raft_router: T,
    pub snapshot_status_sender: Sender<SnapshotStatusMsg>,
}

pub struct Server<T: RaftStoreRouter + 'static, S: StoreAddrResolver> {
    sendch: SendCh<Msg>,

    grpc_server: Option<TiKVAsyncServer>,
    local_addr: SocketAddr,

    // store id -> addr
    // This is for communicating with other raft stores.
    store_addrs: HashMap<u64, SocketAddr>,
    store_resolving: HashSet<u64>,

    ch: ServerChannel<T>,

    snap_mgr: SnapManager,
    snap_worker: Worker<SnapTask>,

    raft_msg_worker: FutureWorker<SendTask>,

    resolver: S,
}

impl<T: RaftStoreRouter, S: StoreAddrResolver> Server<T, S> {
    // Create a server with already initialized engines.
    // Now some tests use 127.0.0.1:0 but we need real listening
    // address in Node before creating the Server, so we first
    // create the listener outer, get the real listening address for
    // Node and then pass it here.
    pub fn new(event_loop: &mut EventLoop<Self>,
               cfg: &Config,
               storage: Storage,
               ch: ServerChannel<T>,
               resolver: S,
               snap_mgr: SnapManager)
               -> Result<Server<T, S>> {
        let sendch = SendCh::new(event_loop.channel(), "raft-server");
        let snap_worker = Worker::new("snap-handler");
        let raft_msg_worker = FutureWorker::new("raft-msg-worker");

        let mut h = Handle::new(storage,
                                cfg.end_point_concurrency,
                                ch.raft_router.clone(),
                                snap_worker.scheduler());
        try!(h.run());
        let mut conf = GrpcServerConf::default();
        conf.http.no_delay = Some(true);
        conf.http.thread_name = Some("grpc-server".to_owned());
        let grpc_server = TiKVAsyncServer::new(cfg.addr.to_owned(), conf, h);
        let addr = grpc_server.local_addr().to_owned();

        let svr = Server {
            sendch: sendch,
            grpc_server: Some(grpc_server),
            local_addr: addr,
            store_addrs: HashMap::default(),
            store_resolving: HashSet::default(),
            ch: ch,
            snap_mgr: snap_mgr,
            snap_worker: snap_worker,
            raft_msg_worker: raft_msg_worker,
            resolver: resolver,
        };

        Ok(svr)
    }

    pub fn run(&mut self, event_loop: &mut EventLoop<Self>) -> Result<()> {
        let ch = self.get_sendch();
        let snap_runner = SnapHandler::new(self.snap_mgr.clone(), self.ch.raft_router.clone(), ch);
        box_try!(self.snap_worker.start(snap_runner));
        box_try!(self.raft_msg_worker.start(SendRunner::default()));

        info!("TiKV is ready to serve");

        try!(event_loop.run(self));
        Ok(())
    }

    pub fn get_sendch(&self) -> SendCh<Msg> {
        self.sendch.clone()
    }

    // Return listening address, this may only be used for outer test
    // to get the real address because we may use "127.0.0.1:0"
    // in test to avoid port conflict.
    pub fn listening_addr(&self) -> SocketAddr {
        self.local_addr
    }

    fn write_data(&mut self, addr: SocketAddr, data: ConnData) {
        if let Err(e) = self.raft_msg_worker.schedule(SendTask {
            addr: addr,
            msg: data.msg,
        }) {
            error!("send raft msg err {:?}", e);
        }
    }

    fn resolve_store(&mut self, store_id: u64, data: ConnData) {
        let ch = self.sendch.clone();
        let cb = box move |r| {
            if let Err(e) = ch.send(Msg::ResolveResult {
                store_id: store_id,
                sock_addr: r,
                data: data,
            }) {
                error!("send store sock msg err {:?}", e);
            }
        };
        if let Err(e) = self.resolver.resolve(store_id, cb) {
            error!("try to resolve err {:?}", e);
        }
    }

    fn report_unreachable(&self, data: ConnData) {
        let region_id = data.msg.get_region_id();
        let to_peer_id = data.msg.get_to_peer().get_id();
        let to_store_id = data.msg.get_to_peer().get_store_id();

        if let Err(e) = self.ch.raft_router.report_unreachable(region_id, to_peer_id, to_store_id) {
            error!("report peer {} unreachable for region {} failed {:?}",
                   to_peer_id,
                   region_id,
                   e);
        }
    }

    fn send_store(&mut self, store_id: u64, data: ConnData) {
        if data.is_snapshot() {
            RESOLVE_STORE_COUNTER.with_label_values(&["snap"]).inc();
            return self.resolve_store(store_id, data);
        }

        // check the corresponding token for store.
        if let Some(addr) = self.store_addrs.get(&store_id).cloned() {
            return self.write_data(addr, data);
        }

        // No connection, try to resolve it.
        if self.store_resolving.contains(&store_id) {
            RESOLVE_STORE_COUNTER.with_label_values(&["resolving"]).inc();
            // If we are resolving the address, drop the message here.
            debug!("store {} address is being resolved, drop msg {}",
                   store_id,
                   data);
            self.report_unreachable(data);
            return;
        }

        debug!("begin to resolve store {} address", store_id);
        RESOLVE_STORE_COUNTER.with_label_values(&["store"]).inc();
        self.store_resolving.insert(store_id);
        self.resolve_store(store_id, data);
    }

    fn on_resolve_result(&mut self, store_id: u64, sock_addr: Result<SocketAddr>, data: ConnData) {
        if !data.is_snapshot() {
            // clear resolving.
            self.store_resolving.remove(&store_id);
        }

        if let Err(e) = sock_addr {
            RESOLVE_STORE_COUNTER.with_label_values(&["failed"]).inc();
            debug!("resolve store {} address failed {:?}", store_id, e);
            return self.report_unreachable(data);
        }

        RESOLVE_STORE_COUNTER.with_label_values(&["success"]).inc();
        let sock_addr = sock_addr.unwrap();
        info!("resolve store {} address ok, addr {}", store_id, sock_addr);
        self.store_addrs.insert(store_id, sock_addr);

        if data.is_snapshot() {
            return self.send_snapshot_sock(sock_addr, data);
        }

        self.write_data(sock_addr, data)
    }

    fn new_snapshot_reporter(&self, data: &ConnData) -> SnapshotReporter {
        let region_id = data.msg.get_region_id();
        let to_peer_id = data.msg.get_to_peer().get_id();
        let to_store_id = data.msg.get_to_peer().get_store_id();

        SnapshotReporter {
            snapshot_status_sender: self.ch.snapshot_status_sender.clone(),
            region_id: region_id,
            to_peer_id: to_peer_id,
            to_store_id: to_store_id,
        }
    }

    fn send_snapshot_sock(&mut self, sock_addr: SocketAddr, data: ConnData) {
        let rep = self.new_snapshot_reporter(&data);
        let cb = box move |res: Result<()>| {
            if res.is_err() {
                rep.report(SnapshotStatus::Failure);
            } else {
                rep.report(SnapshotStatus::Finish);
            }
        };
        if let Err(Stopped(SnapTask::SendTo { cb, .. })) = self.snap_worker
            .schedule(SnapTask::SendTo {
                addr: sock_addr,
                data: data,
                cb: cb,
            }) {
            error!("channel is closed, failed to schedule snapshot to {}",
                   sock_addr);
            cb(Err(box_err!("failed to schedule snapshot")));
        }
    }
}

impl<T: RaftStoreRouter, S: StoreAddrResolver> Handler for Server<T, S> {
    type Timeout = Msg;
    type Message = Msg;

    fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: Msg) {
        match msg {
            Msg::Quit => event_loop.shutdown(),
            Msg::SendStore { store_id, data } => self.send_store(store_id, data),
            Msg::ResolveResult { store_id, sock_addr, data } => {
                self.on_resolve_result(store_id, sock_addr, data)
            }
            Msg::CloseConn { .. } => {}
        }
    }

    fn interrupted(&mut self, _: &mut EventLoop<Self>) {
        // To be able to be attached by gdb, we should not shutdown.
        // TODO: find a grace way to shutdown.
        // event_loop.shutdown();
    }

    fn tick(&mut self, el: &mut EventLoop<Self>) {
        // tick is called in the end of the loop, so if we notify to quit,
        // we will quit the server here.
        // TODO: handle quit server if event_loop is_running() returns false.
        if !el.is_running() {
            self.snap_worker.stop();
            self.raft_msg_worker.stop();
            self.grpc_server.take();
        }
    }
}

struct SnapshotReporter {
    snapshot_status_sender: Sender<SnapshotStatusMsg>,
    region_id: u64,
    to_peer_id: u64,
    to_store_id: u64,
}

impl SnapshotReporter {
    pub fn report(&self, status: SnapshotStatus) {
        debug!("send snapshot to {} for {} {:?}",
               self.to_peer_id,
               self.region_id,
               status);

        if status == SnapshotStatus::Failure {
            let store = self.to_store_id.to_string();
            REPORT_FAILURE_MSG_COUNTER.with_label_values(&["snapshot", &*store]).inc();
        };

        if let Err(e) = self.snapshot_status_sender.send(SnapshotStatusMsg {
            region_id: self.region_id,
            to_peer_id: self.to_peer_id,
            status: status,
        }) {
            error!("report snapshot to peer {} in store {} with region {} err {:?}",
                   self.to_peer_id,
                   self.to_store_id,
                   self.region_id,
                   e);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::sync::{Arc, Mutex};
    use std::sync::mpsc::{self, Sender};
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use super::super::{Msg, ConnData, Result, Config};
    use super::super::transport::RaftStoreRouter;
    use super::super::resolve::{StoreAddrResolver, Callback as ResolveCallback};
    use storage::Storage;
    use kvproto::raft_serverpb::RaftMessage;
    use raftstore::Result as RaftStoreResult;
    use raftstore::store::Msg as StoreMsg;

    struct MockResolver {
        addr: Arc<Mutex<Option<SocketAddr>>>,
    }

    impl StoreAddrResolver for MockResolver {
        fn resolve(&self, _: u64, cb: ResolveCallback) -> Result<()> {
            cb.call_box((self.addr.lock().unwrap().ok_or(box_err!("not set")),));
            Ok(())
        }
    }

    #[derive(Clone)]
    struct TestRaftStoreRouter {
        tx: Sender<usize>,
        report_unreachable_count: Arc<AtomicUsize>,
    }

    impl TestRaftStoreRouter {
        fn new(tx: Sender<usize>) -> TestRaftStoreRouter {
            TestRaftStoreRouter {
                tx: tx,
                report_unreachable_count: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl RaftStoreRouter for TestRaftStoreRouter {
        fn send(&self, _: StoreMsg) -> RaftStoreResult<()> {
            self.tx.send(1).unwrap();
            Ok(())
        }

        fn try_send(&self, _: StoreMsg) -> RaftStoreResult<()> {
            self.tx.send(1).unwrap();
            Ok(())
        }

        fn report_unreachable(&self, _: u64, _: u64, _: u64) -> RaftStoreResult<()> {
            let count = self.report_unreachable_count.clone();
            count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn test_peer_resolve() {
        let mut cfg = Config::new();
        cfg.addr = "127.0.0.1:0".to_owned();
        let mut event_loop = create_event_loop(&cfg).unwrap();
        let mut storage = Storage::new(&cfg.storage).unwrap();
        storage.start(&cfg.storage).unwrap();

        let (tx, rx) = mpsc::channel();
        let router = TestRaftStoreRouter::new(tx);
        let report_unreachable_count = router.report_unreachable_count.clone();

        let (snapshot_status_sender, _) = mpsc::channel();

        let ch = ServerChannel {
            raft_router: router,
            snapshot_status_sender: snapshot_status_sender,
        };
        let addr = Arc::new(Mutex::new(None));
        let mut server =
            Server::new(&mut event_loop,
                        &cfg,
                        storage,
                        ch,
                        MockResolver { addr: addr.clone() },
                        SnapManager::new("", None, cfg.raft_store.use_sst_file_snapshot))
                .unwrap();
        *addr.lock().unwrap() = Some(server.listening_addr());

        for i in 0..10 {
            if i % 2 == 1 {
                server.report_unreachable(ConnData::new(0, RaftMessage::new()));
            }
            assert_eq!(report_unreachable_count.load(Ordering::SeqCst), (i + 1) / 2);
        }

        let ch = server.get_sendch();
        let h = thread::spawn(move || {
            server.run(&mut event_loop).unwrap();
        });

        ch.try_send(Msg::SendStore {
                store_id: 1,
                data: ConnData::new(0, RaftMessage::new()),
            })
            .unwrap();

        rx.recv().unwrap();

        ch.try_send(Msg::Quit).unwrap();
        h.join().unwrap();
    }
}
