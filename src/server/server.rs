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

use std::collections::{HashMap, HashSet};
use std::option::Option;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::boxed::Box;
use std::net::SocketAddr;

use mio::{Token, Handler, EventLoop, EventLoopBuilder, EventSet, PollOpt};
use mio::tcp::{TcpListener, TcpStream};

use kvproto::raft_cmdpb::RaftCmdRequest;
use kvproto::msgpb::{MessageType, Message};
use super::{Msg, SendCh, ConnData};
use super::conn::Conn;
use super::{Result, OnResponse};
use util::HandyRwLock;
use util::worker::Worker;
use storage::Storage;
use raftstore::store::SnapManager;
use super::kv::StoreHandler;
use super::coprocessor::{RequestTask, EndPointHost};
use super::transport::RaftStoreRouter;
use super::resolve::StoreAddrResolver;
use super::snap::{Task as SnapTask, Runner as SnapHandler};
use raft::SnapshotStatus;

const SERVER_TOKEN: Token = Token(1);
const FIRST_CUSTOM_TOKEN: Token = Token(1024);
const DEFAULT_COPROCESSOR_BATCH: usize = 50;
const DEFAULT_NOTIFY_CAPACITY: usize = 40960;

pub fn create_event_loop<T, S>() -> Result<EventLoop<Server<T, S>>>
    where T: RaftStoreRouter,
          S: StoreAddrResolver
{
    let mut builder = EventLoopBuilder::new();
    builder.notify_capacity(DEFAULT_NOTIFY_CAPACITY);
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
    // We use HashMap instead of common use mio slab to avoid token reusing.
    // In our raft server, a client with token 1 sends a raft command, we will
    // propose this command, execute it then send the response to the client with
    // token 1. But before the response, the client connection is broken and another
    // new client connects, mio slab may reuse the token 1 for it. So the subsequent
    // response will be sent to the new client.
    // To avoid this, we use the HashMap instead and can guarantee the token id is
    // unique and can't be reused.
    conns: HashMap<Token, Conn>,
    conn_token_counter: usize,
    sendch: SendCh,

    // store id -> Token
    // This is for communicating with other raft stores.
    store_tokens: HashMap<u64, Token>,
    store_resolving: HashSet<u64>,

    raft_router: Arc<RwLock<T>>,

    store: StoreHandler,
    end_point_worker: Worker<RequestTask>,

    snap_mgr: SnapManager,
    snap_worker: Worker<SnapTask>,

    resolver: S,
}

impl<T: RaftStoreRouter, S: StoreAddrResolver> Server<T, S> {
    // Create a server with already initialized engines.
    // Now some tests use 127.0.0.1:0 but we need real listening
    // address in Node before creating the Server, so we first
    // create the listener outer, get the real listening address for
    // Node and then pass it here.
    pub fn new(event_loop: &mut EventLoop<Self>,
               listener: TcpListener,
               storage: Storage,
               raft_router: Arc<RwLock<T>>,
               resolver: S,
               snap_mgr: SnapManager)
               -> Result<Server<T, S>> {
        try!(event_loop.register(&listener,
                                 SERVER_TOKEN,
                                 EventSet::readable(),
                                 PollOpt::edge()));

        let sendch = SendCh::new(event_loop.channel());
        let store_handler = StoreHandler::new(storage);
        let end_point_worker = Worker::new("end-point-worker");
        let snap_worker = Worker::new("snap-handler");

        let svr = Server {
            listener: listener,
            sendch: sendch,
            conns: HashMap::new(),
            conn_token_counter: FIRST_CUSTOM_TOKEN.as_usize(),
            store_tokens: HashMap::new(),
            store_resolving: HashSet::new(),
            raft_router: raft_router,
            store: store_handler,
            end_point_worker: end_point_worker,
            snap_mgr: snap_mgr,
            snap_worker: snap_worker,
            resolver: resolver,
        };

        Ok(svr)
    }

    pub fn run(&mut self, event_loop: &mut EventLoop<Self>) -> Result<()> {
        let end_point = EndPointHost::new(self.store.engine());
        box_try!(self.end_point_worker.start_batch(end_point, DEFAULT_COPROCESSOR_BATCH));

        let snap_runner = SnapHandler::new(self.snap_mgr.clone(), self.raft_router.clone());
        box_try!(self.snap_worker.start(snap_runner));

        try!(event_loop.run(self));
        Ok(())
    }

    pub fn get_sendch(&self) -> SendCh {
        self.sendch.clone()
    }

    // Return listening address, this may only be used for outer test
    // to get the real address because we may use "127.0.0.1:0"
    // in test to avoid port conflict.
    pub fn listening_addr(&self) -> Result<SocketAddr> {
        let addr = try!(self.listener.local_addr());
        Ok(addr)
    }

    fn remove_conn(&mut self, event_loop: &mut EventLoop<Self>, token: Token) {
        let conn = self.conns.remove(&token);
        match conn {
            Some(mut conn) => {
                debug!("remove connection token {:?}", token);
                // if connected to remote store, remove this too.
                if let Some(store_id) = conn.store_id {
                    warn!("remove store connection for store {} with token {:?}",
                          store_id,
                          token);
                    self.store_tokens.remove(&store_id);
                }

                if let Err(e) = event_loop.deregister(&conn.sock) {
                    error!("deregister conn err {:?}", e);
                }

                conn.close();
            }
            None => {
                debug!("missing connection for token {}", token.as_usize());
            }
        }
    }

    fn add_new_conn(&mut self,
                    event_loop: &mut EventLoop<Self>,
                    sock: TcpStream,
                    store_id: Option<u64>)
                    -> Result<Token> {
        let new_token = Token(self.conn_token_counter);
        self.conn_token_counter += 1;

        // TODO: check conn max capacity.

        try!(sock.set_nodelay(true));

        try!(event_loop.register(&sock,
                                 new_token,
                                 EventSet::readable() | EventSet::hup(),
                                 PollOpt::edge()));

        let conn = Conn::new(sock, new_token, store_id, self.snap_worker.scheduler());
        self.conns.insert(new_token, conn);
        debug!("register conn {:?}", new_token);

        Ok(new_token)
    }

    fn on_conn_readable(&mut self, event_loop: &mut EventLoop<Self>, token: Token) -> Result<()> {
        let msgs = try!(match self.conns.get_mut(&token) {
            None => {
                debug!("missing conn for token {:?}", token);
                return Ok(());
            }
            Some(conn) => conn.on_readable(event_loop),
        });

        if msgs.is_empty() {
            // Read no message, no need to handle.
            return Ok(());
        }

        for msg in msgs {
            try!(self.on_conn_msg(token, msg))
        }

        Ok(())
    }

    fn on_conn_msg(&mut self, token: Token, data: ConnData) -> Result<()> {
        let msg_id = data.msg_id;
        let mut msg = data.msg;

        let msg_type = msg.get_msg_type();
        match msg_type {
            MessageType::Raft => {
                try!(self.raft_router.rl().send_raft_msg(msg.take_raft()));
                Ok(())
            }
            MessageType::Cmd => self.on_raft_command(msg.take_cmd_req(), token, msg_id),
            MessageType::KvReq => {
                let req = msg.take_kv_req();
                debug!("notify Request token[{:?}] msg_id[{}] type[{:?}]",
                       token,
                       msg_id,
                       req.get_field_type());
                let on_resp = self.make_response_cb(token, msg_id);
                self.store.on_request(req, on_resp)
            }
            MessageType::CopReq => {
                let on_resp = self.make_response_cb(token, msg_id);
                box_try!(self.end_point_worker
                    .schedule(RequestTask::new(msg.take_cop_req(), on_resp)));
                Ok(())
            }
            _ => {
                Err(box_err!("unsupported message {:?} for token {:?} with msg id {}",
                             msg_type,
                             token,
                             msg_id))
            }
        }
    }

    fn on_raft_command(&mut self, msg: RaftCmdRequest, token: Token, msg_id: u64) -> Result<()> {
        trace!("handle raft command {:?}", msg);
        let on_resp = self.make_response_cb(token, msg_id);
        let cb = box move |resp| {
            let mut resp_msg = Message::new();
            resp_msg.set_msg_type(MessageType::CmdResp);
            resp_msg.set_cmd_resp(resp);

            on_resp.call_box((resp_msg,));
            Ok(())
        };

        try!(self.raft_router.rl().send_command(msg, cb));

        Ok(())
    }

    fn on_readable(&mut self, event_loop: &mut EventLoop<Self>, token: Token) {
        match token {
            SERVER_TOKEN => {
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

                    if let Err(e) = self.add_new_conn(event_loop, sock, None) {
                        error!("register conn err {:?}", e);
                    }
                }
            }
            token => {
                if let Err(e) = self.on_conn_readable(event_loop, token) {
                    debug!("handle read conn for token {:?} err {:?}, remove", token, e);
                    self.remove_conn(event_loop, token);
                }
            }

        }
    }

    fn on_writable(&mut self, event_loop: &mut EventLoop<Self>, token: Token) {
        let res = match self.conns.get_mut(&token) {
            None => {
                debug!("missing conn for token {:?}", token);
                return;
            }
            Some(conn) => conn.on_writable(event_loop),
        };

        if let Err(e) = res {
            debug!("handle write conn err {:?}, remove", e);
            self.remove_conn(event_loop, token);
        }
    }

    fn write_data(&mut self, event_loop: &mut EventLoop<Self>, token: Token, data: ConnData) {
        let res = match self.conns.get_mut(&token) {
            None => {
                debug!("missing conn for token {:?}", token);
                return;
            }
            Some(conn) => conn.append_write_buf(event_loop, data),
        };

        if let Err(e) = res {
            debug!("handle write data err {:?}, remove", e);
            self.remove_conn(event_loop, token);
        }
    }

    fn try_connect(&mut self,
                   event_loop: &mut EventLoop<Self>,
                   sock_addr: SocketAddr,
                   store_id_opt: Option<u64>)
                   -> Result<Token> {
        let sock = try!(TcpStream::connect(&sock_addr));
        let token = try!(self.add_new_conn(event_loop, sock, store_id_opt));
        Ok(token)
    }

    fn connect_store(&mut self,
                     event_loop: &mut EventLoop<Self>,
                     store_id: u64,
                     sock_addr: SocketAddr)
                     -> Result<Token> {
        // We may already create the connection before.
        if let Some(token) = self.store_tokens.get(&store_id).cloned() {
            debug!("token already exists for store {}, reuse", store_id);
            return Ok(token);
        }

        let token = try!(self.try_connect(event_loop, sock_addr, Some(store_id)));
        self.store_tokens.insert(store_id, token);
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
        if data.msg.has_raft() {
            return;
        }

        let region_id = data.msg.get_raft().get_region_id();
        let to_peer_id = data.msg.get_raft().get_to_peer().get_id();

        if let Err(e) = self.raft_router.rl().report_unreachable(region_id, to_peer_id) {
            error!("report peer {} unreachable for region {} failed {:?}",
                   to_peer_id,
                   region_id,
                   e);
        }
    }

    fn send_store(&mut self, event_loop: &mut EventLoop<Self>, store_id: u64, data: ConnData) {
        if data.is_snapshot() {
            return self.resolve_store(store_id, data);
        }

        // check the corresponding token for store.
        if let Some(token) = self.store_tokens.get(&store_id).cloned() {
            return self.write_data(event_loop, token, data);
        }

        // No connection, try to resolve it.
        if self.store_resolving.contains(&store_id) {
            // If we are resolving the address, drop the message here.
            debug!("store {} address is being resolved, drop msg {}",
                   store_id,
                   data);
            self.report_unreachable(data);
            return;
        }

        info!("begin to resolve store {} address", store_id);
        self.store_resolving.insert(store_id);
        self.resolve_store(store_id, data);
    }

    fn on_resolve_failed(&mut self, store_id: u64, sock_addr: Result<SocketAddr>, data: ConnData) {
        let e = sock_addr.unwrap_err();
        warn!("resolve store {} address failed {:?}", store_id, e);

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
            return self.on_resolve_failed(store_id, sock_addr, data);
        }

        let sock_addr = sock_addr.unwrap();
        info!("resolve store {} address ok, addr {}", store_id, sock_addr);

        if data.is_snapshot() {
            return self.send_snapshot_sock(sock_addr, data);
        }

        let token = match self.connect_store(event_loop, store_id, sock_addr) {
            Ok(token) => token,
            Err(e) => {
                self.report_unreachable(data);
                error!("connect store {} err {:?}", store_id, e);
                return;
            }
        };

        self.write_data(event_loop, token, data)
    }

    fn new_snapshot_reporter(&self, data: &ConnData) -> SnapshotReporter<T> {
        let region_id = data.msg.get_raft().get_region_id();
        let to_peer_id = data.msg.get_raft().get_to_peer().get_id();

        SnapshotReporter {
            router: self.raft_router.clone(),
            region_id: region_id,
            to_peer_id: to_peer_id,
            reported: AtomicBool::new(false),
        }
    }

    fn send_snapshot_sock(&mut self, sock_addr: SocketAddr, data: ConnData) {
        let reporter = Arc::new(self.new_snapshot_reporter(&data));
        let rep = reporter.clone();
        let cb = box move |res: Result<()>| {
            if let Err(_) = res {
                rep.report(SnapshotStatus::Failure);
            } else {
                rep.report(SnapshotStatus::Finish);
            }
        };
        if let Err(e) = self.snap_worker.schedule(SnapTask::SendTo {
            addr: sock_addr,
            data: data,
            cb: cb,
        }) {
            error!("failed to schedule snapshot: {:?}", e);
            reporter.report(SnapshotStatus::Failure);
        }
    }

    fn make_response_cb(&mut self, token: Token, msg_id: u64) -> OnResponse {
        let ch = self.sendch.clone();
        box move |res: Message| {
            let tp = res.get_msg_type();
            if let Err(e) = ch.send(Msg::WriteData {
                token: token,
                data: ConnData::new(msg_id, res),
            }) {
                error!("send {:?} resp failed with token {:?}, msg id {}, err {:?}",
                       tp,
                       token,
                       msg_id,
                       e);
            }
        }
    }
}

impl<T: RaftStoreRouter, S: StoreAddrResolver> Handler for Server<T, S> {
    type Timeout = Msg;
    type Message = Msg;

    fn ready(&mut self, event_loop: &mut EventLoop<Self>, token: Token, events: EventSet) {
        if events.is_error() {
            self.remove_conn(event_loop, token);
            return;
        }

        if events.is_readable() {
            self.on_readable(event_loop, token);
        }

        if events.is_writable() {
            self.on_writable(event_loop, token);
        }

        if events.is_hup() {
            self.remove_conn(event_loop, token);
        }
    }

    fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: Msg) {
        match msg {
            Msg::Quit => event_loop.shutdown(),
            Msg::WriteData { token, data } => self.write_data(event_loop, token, data),
            Msg::SendStore { store_id, data } => self.send_store(event_loop, store_id, data),
            Msg::ResolveResult { store_id, sock_addr, data } => {
                self.on_resolve_result(event_loop, store_id, sock_addr, data)
            }
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
            if let Err(e) = self.end_point_worker.stop() {
                error!("failed to stop end point: {:?}", e);
            }
            if let Err(e) = self.store.stop() {
                error!("failed to stop store: {:?}", e);
            }
        }
    }
}

struct SnapshotReporter<T: RaftStoreRouter + 'static> {
    router: Arc<RwLock<T>>,
    region_id: u64,
    to_peer_id: u64,

    reported: AtomicBool,
}

impl<T: RaftStoreRouter + 'static> SnapshotReporter<T> {
    pub fn report(&self, status: SnapshotStatus) {
        // return directly if already reported.
        if self.reported.compare_and_swap(false, true, Ordering::Relaxed) {
            return;
        }

        debug!("send snapshot to {} for {} {:?}",
               self.to_peer_id,
               self.region_id,
               status);


        if let Err(e) = self.router
            .rl()
            .report_snapshot(self.region_id, self.to_peer_id, status) {
            error!("report snapshot to peer {} with region {} err {:?}",
                   self.to_peer_id,
                   self.region_id,
                   e);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::sync::{Arc, RwLock, Mutex};
    use std::sync::mpsc::{self, Sender};
    use std::net::SocketAddr;

    use mio::tcp::TcpListener;

    use super::*;
    use super::super::{Msg, ConnData, Result};
    use super::super::transport::RaftStoreRouter;
    use super::super::resolve::{StoreAddrResolver, Callback as ResolveCallback};
    use storage::{Storage, Dsn};
    use kvproto::msgpb::{Message, MessageType};
    use raftstore::Result as RaftStoreResult;
    use kvproto::raft_serverpb::RaftMessage;
    use raftstore::store::{self, Callback};
    use kvproto::raft_cmdpb::RaftCmdRequest;
    use raft::SnapshotStatus;
    use storage::engine::TEMP_DIR;

    struct MockResolver {
        addr: SocketAddr,
    }

    impl StoreAddrResolver for MockResolver {
        fn resolve(&self, _: u64, cb: ResolveCallback) -> Result<()> {
            cb.call_box((Ok(self.addr),));
            Ok(())
        }
    }

    struct TestRaftStoreRouter {
        tx: Mutex<Sender<usize>>,
    }

    impl RaftStoreRouter for TestRaftStoreRouter {
        fn send_raft_msg(&self, _: RaftMessage) -> RaftStoreResult<()> {
            self.tx.lock().unwrap().send(1).unwrap();
            Ok(())
        }

        fn send_command(&self, _: RaftCmdRequest, _: Callback) -> RaftStoreResult<()> {
            self.tx.lock().unwrap().send(1).unwrap();
            Ok(())
        }

        fn report_snapshot(&self, _: u64, _: u64, _: SnapshotStatus) -> RaftStoreResult<()> {
            unimplemented!();
        }

        fn report_unreachable(&self, _: u64, _: u64) -> RaftStoreResult<()> {
            unimplemented!();
        }
    }

    #[test]
    fn test_peer_resolve() {
        let addr = "127.0.0.1:0".parse().unwrap();
        let listener = TcpListener::bind(&addr).unwrap();

        let resolver = MockResolver { addr: listener.local_addr().unwrap() };

        let mut event_loop = create_event_loop().unwrap();
        let (tx, rx) = mpsc::channel();
        let mut server =
            Server::new(&mut event_loop,
                        listener,
                        Storage::new(Dsn::RocksDBPath(TEMP_DIR)).unwrap(),
                        Arc::new(RwLock::new(TestRaftStoreRouter { tx: Mutex::new(tx) })),
                        resolver,
                        store::new_snap_mgr(""))
                .unwrap();

        let ch = server.get_sendch();
        let h = thread::spawn(move || {
            event_loop.run(&mut server).unwrap();
        });

        let mut msg = Message::new();
        msg.set_msg_type(MessageType::Raft);

        ch.send(Msg::SendStore {
                store_id: 1,
                data: ConnData::new(0, msg),
            })
            .unwrap();

        rx.recv().unwrap();

        ch.send(Msg::Quit).unwrap();
        h.join().unwrap();
    }
}
