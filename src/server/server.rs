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

use std::collections::HashSet;
use std::sync::mpsc::Sender;
use std::boxed::Box;
use std::net::SocketAddr;
use std::thread::Builder;

use mio::{Token, Handler, EventLoop, EventLoopBuilder, EventSet, PollOpt};
use mio::tcp::{TcpListener, TcpStream};

use super::{Msg, ConnData};
use super::conn::StoreConnKey;
use super::{Result, Config};
use util::worker::{Stopped, Worker};
use util::transport::SendCh;
use storage::Storage;
use raftstore::store::{SnapshotStatusMsg, SnapManager};
use super::kv::StoreHandler;
use super::coprocessor::{EndPointHost, EndPointTask};
use super::transport::RaftStoreRouter;
use super::resolve::StoreAddrResolver;
use super::snap::{Task as SnapTask, Runner as SnapHandler};
use raft::SnapshotStatus;
use super::metrics::*;
use super::conn_loop::{ConnHandler, ClientLoop, create_client_event_loop};
pub use super::conn_loop::ServerChannel;

const SERVER_TOKEN: Token = Token(1);
const FIRST_CUSTOM_TOKEN: Token = Token(1024);
const DEFAULT_COPROCESSOR_BATCH: usize = 50;
const DEFAULT_CLIENT_CONN_LOOP_NUM: usize = 3;

pub fn create_event_loop<T, S>(config: &Config) -> Result<EventLoop<Server<T, S>>>
    where T: RaftStoreRouter,
          S: StoreAddrResolver
{
    let mut builder = EventLoopBuilder::new();
    builder.notify_capacity(config.notify_capacity);
    builder.messages_per_tick(config.messages_per_tick);
    let el = try!(builder.build());
    Ok(el)
}

pub fn bind(addr: &str) -> Result<TcpListener> {
    let laddr = try!(addr.parse());
    let listener = try!(TcpListener::bind(&laddr));
    Ok(listener)
}


pub struct Server<T: RaftStoreRouter + 'static, S: StoreAddrResolver> {
    listener: TcpListener,
    conn_token_counter: usize,
    sendch: SendCh<Msg>,

    store_resolving: HashSet<u64>,

    ch: ServerChannel<T>,

    store: StoreHandler,
    end_point_worker: Worker<EndPointTask>,

    snap_mgr: SnapManager,
    snap_worker: Worker<SnapTask>,

    resolver: S,

    cfg: Config,

    conn_handler: ConnHandler<T>,
    client_loop_channels: Vec<SendCh<Msg>>,
}

impl<T: RaftStoreRouter, S: StoreAddrResolver> Server<T, S> {
    // Create a server with already initialized engines.
    // Now some tests use 127.0.0.1:0 but we need real listening
    // address in Node before creating the Server, so we first
    // create the listener outer, get the real listening address for
    // Node and then pass it here.
    pub fn new(event_loop: &mut EventLoop<Self>,
               cfg: &Config,
               listener: TcpListener,
               storage: Storage,
               ch: ServerChannel<T>,
               resolver: S,
               snap_mgr: SnapManager)
               -> Result<Server<T, S>> {
        try!(event_loop.register(&listener,
                                 SERVER_TOKEN,
                                 EventSet::readable(),
                                 PollOpt::edge()));

        let sendch = SendCh::new(event_loop.channel(), "raft-server");
        let store_handler = StoreHandler::new(storage);
        let end_point_worker = Worker::new("end-point-worker");
        let snap_worker = Worker::new("snap-handler");

        let conn_handler = ConnHandler::new(sendch.clone(),
                                            cfg,
                                            store_handler.clone(),
                                            ch.clone(),
                                            end_point_worker.scheduler(),
                                            snap_worker.scheduler());


        let svr = Server {
            listener: listener,
            sendch: sendch,
            conn_token_counter: FIRST_CUSTOM_TOKEN.as_usize(),
            store_resolving: HashSet::new(),
            ch: ch,
            store: store_handler,
            end_point_worker: end_point_worker,
            snap_mgr: snap_mgr,
            snap_worker: snap_worker,
            resolver: resolver,
            cfg: cfg.clone(),
            client_loop_channels: vec![],
            conn_handler: conn_handler,
        };

        Ok(svr)
    }

    pub fn run(&mut self, event_loop: &mut EventLoop<Self>) -> Result<()> {
        let end_point = EndPointHost::new(self.store.engine(),
                                          self.end_point_worker.scheduler(),
                                          self.cfg.end_point_concurrency);
        box_try!(self.end_point_worker.start_batch(end_point, DEFAULT_COPROCESSOR_BATCH));

        let snap_runner = SnapHandler::new(self.snap_mgr.clone(), self.ch.raft_router.clone());
        box_try!(self.snap_worker.start(snap_runner));

        try!(self.run_client_loops());

        info!("TiKV is ready to serve");

        try!(event_loop.run(self));
        Ok(())
    }

    fn run_client_loops(&mut self) -> Result<()> {
        for _ in 0..DEFAULT_CLIENT_CONN_LOOP_NUM {
            let mut event_loop = try!(create_client_event_loop(&self.cfg));
            let mut client_loop = ClientLoop::new(&mut event_loop,
                                                  &self.cfg,
                                                  self.store.clone(),
                                                  self.ch.clone(),
                                                  self.end_point_worker.scheduler(),
                                                  self.snap_worker.scheduler());

            let ch = client_loop.get_sendch();

            let _ = try!(Builder::new()
                .name("client-conn-loop".to_owned())
                .spawn(move || if let Err(e) = event_loop.run(&mut client_loop) {
                    panic!("run client loop failed: {}", e);
                }));

            self.client_loop_channels.push(ch);
        }

        Ok(())
    }


    pub fn get_sendch(&self) -> SendCh<Msg> {
        self.sendch.clone()
    }

    // Return listening address, this may only be used for outer test
    // to get the real address because we may use "127.0.0.1:0"
    // in test to avoid port conflict.
    pub fn listening_addr(&self) -> Result<SocketAddr> {
        let addr = try!(self.listener.local_addr());
        Ok(addr)
    }

    fn gen_token(&mut self) -> Token {
        let new_token = Token(self.conn_token_counter);
        self.conn_token_counter += 1;
        new_token
    }

    fn on_accpet(&mut self, _: &mut EventLoop<Self>) {
        loop {
            // For edge trigger, we must accept all connections until None.
            let sock = match self.listener.accept() {
                Err(e) => {
                    error!("accept error: {:?}", e);
                    return;
                }
                Ok(None) => {
                    debug!("no connection, accept later.");
                    return;
                }
                Ok(Some((sock, addr))) => {
                    debug!("accept conn {}", addr);
                    sock
                }
            };

            let new_token = self.gen_token();
            let index = new_token.as_usize() % DEFAULT_CLIENT_CONN_LOOP_NUM;
            let ch = self.client_loop_channels[index].clone();
            if let Err(e) = ch.try_send(Msg::Connect {
                token: new_token,
                sock: sock,
            }) {
                error!("register conn err {:?}", e);
            }
        }
    }

    fn try_connect(&mut self,
                   event_loop: &mut EventLoop<Self>,
                   sock_addr: SocketAddr,
                   conn_key: StoreConnKey)
                   -> Result<Token> {
        let sock = try!(TcpStream::connect(&sock_addr));
        let new_token = self.gen_token();
        try!(self.conn_handler
            .add_new_conn(event_loop, new_token, sock, Some(conn_key)));
        Ok(new_token)
    }

    fn connect_store(&mut self,
                     event_loop: &mut EventLoop<Self>,
                     conn_key: StoreConnKey,
                     sock_addr: SocketAddr)
                     -> Result<Token> {
        let store_id = conn_key.store_id;
        // We may already create the connection before.
        if let Some(token) = self.conn_handler.store_tokens.get(&conn_key).cloned() {
            debug!("token already exists for store {}, reuse", store_id);
            return Ok(token);
        }

        let token = try!(self.try_connect(event_loop, sock_addr, conn_key));
        self.conn_handler.store_tokens.insert(conn_key, token);
        Ok(token)
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
        if !data.msg.has_raft() {
            return;
        }

        let region_id = data.msg.get_raft().get_region_id();
        let to_peer_id = data.msg.get_raft().get_to_peer().get_id();
        let to_store_id = data.msg.get_raft().get_to_peer().get_store_id();

        if let Err(e) = self.ch.raft_router.report_unreachable(region_id, to_peer_id, to_store_id) {
            error!("report peer {} unreachable for region {} failed {:?}",
                   to_peer_id,
                   region_id,
                   e);
        }
    }

    fn send_store(&mut self, event_loop: &mut EventLoop<Self>, store_id: u64, data: ConnData) {
        if data.is_snapshot() {
            RESOLVE_STORE_COUNTER.with_label_values(&["snap"]).inc();
            return self.resolve_store(store_id, data);
        }

        let conn_key = StoreConnKey::new(store_id, data.get_region_id());
        // check the corresponding token for store.
        if let Some(token) = self.conn_handler.store_tokens.get(&conn_key).cloned() {
            return self.conn_handler.write_data(event_loop, token, data);
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

    fn on_resolve_failed(&mut self, store_id: u64, sock_addr: Result<SocketAddr>, data: ConnData) {
        let e = sock_addr.unwrap_err();
        debug!("resolve store {} address failed {:?}", store_id, e);

        self.report_unreachable(data)
    }

    fn on_resolve_result(&mut self,
                         event_loop: &mut EventLoop<Self>,
                         store_id: u64,
                         sock_addr: Result<SocketAddr>,
                         data: ConnData) {
        if !data.is_snapshot() {
            // clear resolving.
            self.store_resolving.remove(&store_id);
        }

        if sock_addr.is_err() {
            RESOLVE_STORE_COUNTER.with_label_values(&["failed"]).inc();
            return self.on_resolve_failed(store_id, sock_addr, data);
        }

        RESOLVE_STORE_COUNTER.with_label_values(&["success"]).inc();
        let sock_addr = sock_addr.unwrap();
        info!("resolve store {} address ok, addr {}", store_id, sock_addr);

        if data.is_snapshot() {
            return self.send_snapshot_sock(sock_addr, data);
        }

        let conn_key = StoreConnKey::new(store_id, data.get_region_id());
        let token = match self.connect_store(event_loop, conn_key, sock_addr) {
            Ok(token) => token,
            Err(e) => {
                self.report_unreachable(data);
                error!("connect store {} err {:?}", store_id, e);
                return;
            }
        };

        self.conn_handler.write_data(event_loop, token, data)
    }

    fn new_snapshot_reporter(&self, data: &ConnData) -> SnapshotReporter {
        let region_id = data.msg.get_raft().get_region_id();
        let to_peer_id = data.msg.get_raft().get_to_peer().get_id();
        let to_store_id = data.msg.get_raft().get_to_peer().get_store_id();

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
            if let Err(_) = res {
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

    fn ready(&mut self, event_loop: &mut EventLoop<Self>, token: Token, events: EventSet) {
        if token == SERVER_TOKEN {
            return self.on_accpet(event_loop);
        }

        self.conn_handler.on_ready(event_loop, token, events);
    }

    fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: Msg) {
        match msg {
            Msg::Quit => event_loop.shutdown(),
            Msg::WriteData { token, data } => self.conn_handler.write_data(event_loop, token, data),
            Msg::SendStore { store_id, data } => self.send_store(event_loop, store_id, data),
            Msg::ResolveResult { store_id, sock_addr, data } => {
                self.on_resolve_result(event_loop, store_id, sock_addr, data)
            }
            Msg::CloseConn { token } => self.conn_handler.remove_conn(event_loop, token),
            _ => error!("invalid msg {:?}", msg),
        }
    }

    fn timeout(&mut self, _: &mut EventLoop<Self>, _: Msg) {
        // nothing to do now.
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
            for ch in self.client_loop_channels.drain(..) {
                if let Err(e) = ch.try_send(Msg::Quit) {
                    error!("failed to stop client loop: {:?}", e);
                }

                self.end_point_worker.stop();
                self.snap_worker.stop();
                if let Err(e) = self.store.stop() {
                    error!("failed to stop store: {:?}", e);
                }
            }
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
    use std::sync::Arc;
    use std::sync::mpsc::{self, Sender};
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use mio::tcp::TcpListener;

    use super::*;
    use super::super::{Msg, ConnData, Result, Config};
    use super::super::transport::RaftStoreRouter;
    use super::super::resolve::{StoreAddrResolver, Callback as ResolveCallback};
    use storage::Storage;
    use kvproto::msgpb::{Message, MessageType};
    use kvproto::raft_serverpb::RaftMessage;
    use raftstore::Result as RaftStoreResult;
    use raftstore::store::{self, Msg as StoreMsg};

    struct MockResolver {
        addr: SocketAddr,
    }

    impl StoreAddrResolver for MockResolver {
        fn resolve(&self, _: u64, cb: ResolveCallback) -> Result<()> {
            cb.call_box((Ok(self.addr),));
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
        let addr = "127.0.0.1:0".parse().unwrap();
        let listener = TcpListener::bind(&addr).unwrap();

        let resolver = MockResolver { addr: listener.local_addr().unwrap() };

        let cfg = Config::new();
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
        let mut server = Server::new(&mut event_loop,
                                     &cfg,
                                     listener,
                                     storage,
                                     ch,
                                     resolver,
                                     store::new_snap_mgr("", None))
            .unwrap();

        for i in 0..10 {
            let mut msg = Message::new();
            if i % 2 == 1 {
                msg.set_raft(RaftMessage::new());
            }
            server.report_unreachable(ConnData::new(0, msg));
            assert_eq!(report_unreachable_count.load(Ordering::SeqCst), (i + 1) / 2);
        }

        let ch = server.get_sendch();
        let h = thread::spawn(move || {
            server.run(&mut event_loop).unwrap();
        });

        let mut msg = Message::new();
        msg.set_msg_type(MessageType::Raft);

        ch.try_send(Msg::SendStore {
                store_id: 1,
                data: ConnData::new(0, msg),
            })
            .unwrap();

        rx.recv().unwrap();

        ch.try_send(Msg::Quit).unwrap();
        h.join().unwrap();
    }
}
