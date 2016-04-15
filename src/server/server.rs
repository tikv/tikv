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

use std::collections::HashMap;
use std::option::Option;
use std::sync::{Arc, RwLock};
use std::boxed::Box;
use std::net::SocketAddr;

use mio::{Token, Handler, EventLoop, EventSet, PollOpt};
use mio::tcp::{TcpListener, TcpStream};

use raftstore::store::{cmd_resp, Transport};
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse};
use kvproto::msgpb::{MessageType, Message};
use super::{Msg, SendCh, ConnData};
use super::conn::Conn;
use super::Result;
use util::HandyRwLock;
use storage::Storage;
use super::kv::StoreHandler;
use super::coprocessor::RegionEndPoint;
use super::transport::RaftStoreRouter;
use super::resolve::Resolver;

const SERVER_TOKEN: Token = Token(1);
const FIRST_CUSTOM_TOKEN: Token = Token(1024);

pub fn create_event_loop<T: RaftStoreRouter>() -> Result<EventLoop<Server<T>>> {
    let event_loop = try!(EventLoop::new());
    Ok(event_loop)
}

pub fn bind(addr: &str) -> Result<TcpListener> {
    let laddr = try!(addr.parse());
    let listener = try!(TcpListener::bind(&laddr));
    Ok(listener)
}

pub struct Server<T: RaftStoreRouter> {
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

    // addr -> Token
    peers: HashMap<String, Token>,

    raft_router: Arc<RwLock<T>>,

    store: StoreHandler,
    end_point: RegionEndPoint,

    resolver: Resolver,
}

impl<T: RaftStoreRouter> Server<T> {
    // Create a server with already initialized engines.
    // Now some tests use 127.0.0.1:0 but we need real listening
    // address in Node before creating the Server, so we first
    // create the listener outer, get the real listening address for
    // Node and then pass it here.
    pub fn new(event_loop: &mut EventLoop<Self>,
               listener: TcpListener,
               storage: Storage,
               raft_router: Arc<RwLock<T>>)
               -> Result<Server<T>> {
        try!(event_loop.register(&listener,
                                 SERVER_TOKEN,
                                 EventSet::readable(),
                                 PollOpt::edge()));

        let sendch = SendCh::new(event_loop.channel());
        let engine = storage.get_engine();
        let store_handler = StoreHandler::new(storage, sendch.clone());
        let end_point = RegionEndPoint::new(engine, sendch.clone());

        let resolver = try!(Resolver::new());

        let svr = Server {
            listener: listener,
            sendch: sendch,
            conns: HashMap::new(),
            conn_token_counter: FIRST_CUSTOM_TOKEN.as_usize(),
            peers: HashMap::new(),
            raft_router: raft_router,
            store: store_handler,
            end_point: end_point,
            resolver: resolver,
        };

        Ok(svr)
    }

    pub fn run(&mut self, event_loop: &mut EventLoop<Self>) -> Result<()> {
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
            Some(conn) => {
                // if connected to remote peer, remove this too.
                if let Some(addr) = conn.peer_addr {
                    self.peers.remove(&addr);
                }

                if let Err(e) = event_loop.deregister(&conn.sock) {
                    error!("deregister conn err {:?}", e);
                }
            }
            None => {
                warn!("missing connection for token {}", token.as_usize());
            }
        }
    }

    fn add_new_conn(&mut self,
                    event_loop: &mut EventLoop<Self>,
                    sock: TcpStream,
                    peer_addr: Option<String>)
                    -> Result<Token> {
        let new_token = Token(self.conn_token_counter);
        self.conn_token_counter += 1;

        // TODO: check conn max capacity.

        try!(sock.set_nodelay(true));

        try!(event_loop.register(&sock,
                                 new_token,
                                 EventSet::readable() | EventSet::hup(),
                                 PollOpt::edge()));

        let conn = Conn::new(sock, new_token, peer_addr);
        self.conns.insert(new_token, conn);

        Ok(new_token)
    }

    fn on_conn_readable(&mut self, event_loop: &mut EventLoop<Self>, token: Token) -> Result<()> {
        let msgs = try!(match self.conns.get_mut(&token) {
            None => {
                warn!("missing conn for token {:?}", token);
                return Ok(());
            }
            Some(conn) => conn.read(event_loop),
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
                if let Err(e) = self.raft_router.rl().send_raft_msg(msg.take_raft()) {
                    // Should we return error to let outer close this connection later?
                    error!("send raft message for token {:?} with msg id {} err {:?}",
                           token,
                           msg_id,
                           e);
                }
                Ok(())
            }
            MessageType::Cmd => self.on_raft_command(msg.take_cmd_req(), token, msg_id),
            MessageType::KvReq => self.store.on_request(msg.take_kv_req(), token, msg_id),
            MessageType::CopReq => self.end_point.on_request(msg.take_cop_req(), token, msg_id),
            _ => {
                Err(box_err!("unsupported message {:?} for token {:?} with msg id {}",
                             msg_type,
                             token,
                             msg_id))
            }
        }
    }

    fn on_raft_command(&mut self, msg: RaftCmdRequest, token: Token, msg_id: u64) -> Result<()> {
        debug!("handle raft command {:?}", msg);
        let ch = self.sendch.clone();
        let cb = box move |resp| {
            send_raft_cmd_resp(ch, token, msg_id, resp);
            Ok(())
        };

        let uuid = msg.get_header().get_uuid().to_vec();
        if let Err(e) = self.raft_router.rl().send_command(msg, cb) {
            // send error, reply an error response.
            warn!("send command for token {:?} with msg id {} err {:?}",
                  token,
                  msg_id,
                  e);

            let mut resp = cmd_resp::message_error(e);
            resp.mut_header().set_uuid(uuid);
            send_raft_cmd_resp(self.sendch.clone(), token, msg_id, resp)
        }

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
                    warn!("handle read conn for token {:?} err {:?}, remove", token, e);
                    self.remove_conn(event_loop, token);
                }
            }

        }
    }

    fn on_writable(&mut self, event_loop: &mut EventLoop<Self>, token: Token) {
        let res = match self.conns.get_mut(&token) {
            None => {
                warn!("missing conn for token {:?}", token);
                return;
            }
            Some(conn) => conn.write(event_loop),
        };

        if let Err(e) = res {
            warn!("handle write conn err {:?}, remove", e);
            self.remove_conn(event_loop, token);
        }
    }

    fn write_data(&mut self, event_loop: &mut EventLoop<Self>, token: Token, data: ConnData) {
        let res = match self.conns.get_mut(&token) {
            None => {
                warn!("missing conn for token {:?}", token);
                return;
            }
            Some(conn) => conn.append_write_buf(event_loop, data),
        };

        if let Err(e) = res {
            warn!("handle write data err {:?}, remove", e);
            self.remove_conn(event_loop, token);
        }
    }

    fn connect_peer(&mut self,
                    event_loop: &mut EventLoop<Self>,
                    sock_addr: SocketAddr,
                    peer: &str)
                    -> Result<Token> {
        let sock = try!(TcpStream::connect(&sock_addr));
        let token = try!(self.add_new_conn(event_loop, sock, Some(peer.to_owned())));
        self.peers.insert(peer.to_owned(), token);
        Ok(token)
    }

    fn resolve_peer(&mut self, peer: String, data: ConnData) {
        // If the address is host:port, resolving the host's IP may
        // block the event loop, so we should use an asynchronous way
        // to resolve the host address and then send again.
        let peer_addr = peer.clone();
        let ch = self.sendch.clone();
        let cb = box move |r| {
            if let Err(e) = r {
                error!("resolve peer {} err {:?}", peer, e);
                return;
            }

            let sock = r.unwrap();
            if let Err(e) = ch.send(Msg::SendPeerSock {
                sock_addr: sock,
                peer: peer,
                data: data,
            }) {
                error!("send peer sock msg err {:?}", e);
            }
        };
        if let Err(e) = self.resolver.resolve(peer_addr, cb) {
            error!("try to resolve err {:?}", e);
        }
    }

    fn send_peer(&mut self, event_loop: &mut EventLoop<Self>, addr: String, data: ConnData) {
        // check the corresponding token for peer address.
        if let Some(token) = self.peers.get(&addr).cloned() {
            return self.write_data(event_loop, token, data);
        }

        // If parse ok, the addr is ip:port.
        if let Ok(peer_addr) = (&addr).parse() {
            return self.send_peer_sock(event_loop, peer_addr, addr, data);
        }

        // The addr is not ip:port, we should resolve it first.
        self.resolve_peer(addr, data);
    }

    fn send_peer_sock(&mut self,
                      event_loop: &mut EventLoop<Self>,
                      sock_addr: SocketAddr,
                      peer: String,
                      data: ConnData) {
        let token = match self.connect_peer(event_loop, sock_addr, &peer) {
            Ok(token) => token,
            Err(e) => {
                error!("connect peer {} err {:?}", peer, e);
                return;
            }
        };

        self.write_data(event_loop, token, data)
    }
}

impl<T: RaftStoreRouter> Handler for Server<T> {
    type Timeout = Msg;
    type Message = Msg;

    fn ready(&mut self, event_loop: &mut EventLoop<Self>, token: Token, events: EventSet) {
        if events.is_hup() || events.is_error() {
            self.remove_conn(event_loop, token);
            return;
        }

        if events.is_readable() {
            self.on_readable(event_loop, token);
        }

        if events.is_writable() {
            self.on_writable(event_loop, token);
        }
    }

    fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: Msg) {
        match msg {
            Msg::Quit => event_loop.shutdown(),
            Msg::WriteData { token, data } => self.write_data(event_loop, token, data),
            Msg::SendPeer { peer, data } => self.send_peer(event_loop, peer, data),
            Msg::SendPeerSock { sock_addr, peer, data } => {
                self.send_peer_sock(event_loop, sock_addr, peer, data)
            }
        }
    }

    fn timeout(&mut self, _: &mut EventLoop<Self>, _: Msg) {
        // nothing to do now.
    }

    fn interrupted(&mut self, event_loop: &mut EventLoop<Self>) {
        event_loop.shutdown();
    }

    fn tick(&mut self, el: &mut EventLoop<Self>) {
        // tick is called in the end of the loop, so if we notify to quit,
        // we will quit the server here.
        // TODO: handle quit server if event_loop is_running() returns false.
        if !el.is_running() {
            self.end_point.stop();
        }
    }
}

fn send_raft_cmd_resp(ch: SendCh, token: Token, msg_id: u64, resp: RaftCmdResponse) {
    let mut resp_msg = Message::new();
    resp_msg.set_msg_type(MessageType::CmdResp);
    resp_msg.set_cmd_resp(resp);
    if let Err(e) = ch.send(Msg::WriteData {
        token: token,
        data: ConnData::new(msg_id, resp_msg),
    }) {
        error!("send raft cmd resp failed with token {:?}, msg id {}, err {:?}",
               token,
               msg_id,
               e);
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::sync::{Arc, RwLock, Mutex};
    use std::sync::mpsc::{self, Sender};

    use mio::tcp::TcpListener;

    use super::*;
    use super::super::{Msg, ConnData};
    use super::super::transport::RaftStoreRouter;
    use storage::{Storage, Dsn};
    use kvproto::msgpb::{Message, MessageType};
    use raftstore::Result as RaftStoreResult;
    use kvproto::raft_serverpb::RaftMessage;
    use raftstore::store::Callback;
    use kvproto::raft_cmdpb::RaftCmdRequest;

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
    }

    #[test]
    fn test_peer_resolve() {
        let addr = "127.0.0.1:0".parse().unwrap();
        let listener = TcpListener::bind(&addr).unwrap();

        let port = listener.local_addr().unwrap().port();

        let mut event_loop = create_event_loop().unwrap();
        let (tx, rx) = mpsc::channel();
        let mut server = Server::new(&mut event_loop,
                                     listener,
                                     Storage::new(Dsn::Memory).unwrap(),
                                     Arc::new(RwLock::new(TestRaftStoreRouter {
                                         tx: Mutex::new(tx),
                                     })))
                             .unwrap();

        let ch = server.get_sendch();
        let h = thread::spawn(move || {
            event_loop.run(&mut server).unwrap();
        });

        let mut msg = Message::new();
        msg.set_msg_type(MessageType::Raft);

        ch.send(Msg::SendPeer {
              peer: format!("localhost:{}", port),
              data: ConnData::new(0, msg),
          })
          .unwrap();

        rx.recv().unwrap();

        ch.send(Msg::Quit).unwrap();
        h.join().unwrap();
    }
}
