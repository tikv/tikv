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
use std::sync::mpsc::Sender;
use std::boxed::Box;

use mio::{Token, Handler, EventLoop, EventLoopBuilder, EventSet, PollOpt};
use mio::tcp::TcpStream;

use kvproto::raft_cmdpb::RaftCmdRequest;
use kvproto::msgpb::{MessageType, Message};
use super::{Msg, ConnData};
use super::conn::{Conn, StoreConnKey};
use super::{Result, OnResponse, Config};
use util::worker::Scheduler;
use util::transport::SendCh;
use raftstore::store::SnapshotStatusMsg;
use super::kv::StoreHandler;
use super::coprocessor::{RequestTask, EndPointTask};
use super::transport::RaftStoreRouter;
use super::snap::Task as SnapTask;
use util::sockopt::SocketOpt;
use super::metrics::*;

pub fn create_client_event_loop<T>(config: &Config) -> Result<EventLoop<ClientLoop<T>>>
    where T: RaftStoreRouter
{
    let mut builder = EventLoopBuilder::new();
    builder.notify_capacity(config.notify_capacity);
    builder.messages_per_tick(config.messages_per_tick);
    let el = try!(builder.build());
    Ok(el)
}

// A helper structure to bundle all senders for messages to raftstore.
#[derive(Clone)]
pub struct ServerChannel<T: RaftStoreRouter + 'static> {
    pub raft_router: T,
    pub snapshot_status_sender: Sender<SnapshotStatusMsg>,
}

// `ConnHandler` is used to handle connection events in event loop.
pub struct ConnHandler<T: RaftStoreRouter + 'static> {
    // We use HashMap instead of common use mio slab to avoid token reusing.
    // In our raft server, a client with token 1 sends a raft command, we will
    // propose this command, execute it then send the response to the client with
    // token 1. But before the response, the client connection is broken and another
    // new client connects, mio slab may reuse the token 1 for it. So the subsequent
    // response will be sent to the new client.
    // To avoid this, we use the HashMap instead and can guarantee the token id is
    // unique and can't be reused.
    conns: HashMap<Token, Conn>,
    sendch: SendCh<Msg>,

    // store id -> Token
    // This is for communicating with other raft stores.
    pub store_tokens: HashMap<StoreConnKey, Token>,

    ch: ServerChannel<T>,

    store: StoreHandler,
    end_point_scheduler: Scheduler<EndPointTask>,

    snap_scheduler: Scheduler<SnapTask>,

    cfg: Config,
}

impl<T: RaftStoreRouter> ConnHandler<T> {
    // Create a server with already initialized engines.
    // Now some tests use 127.0.0.1:0 but we need real listening
    // address in Node before creating the Server, so we first
    // create the listener outer, get the real listening address for
    // Node and then pass it here.
    pub fn new(sendch: SendCh<Msg>,
               cfg: &Config,
               storage: StoreHandler,
               ch: ServerChannel<T>,
               end_point_scheduler: Scheduler<EndPointTask>,
               snap_scheduler: Scheduler<SnapTask>)
               -> ConnHandler<T> {
        ConnHandler {
            sendch: sendch,
            conns: HashMap::new(),
            store_tokens: HashMap::new(),
            ch: ch,
            store: storage,
            end_point_scheduler: end_point_scheduler,
            snap_scheduler: snap_scheduler,
            cfg: cfg.clone(),
        }
    }

    #[inline]
    pub fn get_sendch(&self) -> SendCh<Msg> {
        self.sendch.clone()
    }

    pub fn remove_conn<H: Handler>(&mut self, event_loop: &mut EventLoop<H>, token: Token) {
        let conn = self.conns.remove(&token);
        match conn {
            Some(mut conn) => {
                CONNECTION_GAUGE.dec();
                debug!("remove connection token {:?}", token);
                // if connected to remote store, remove this too.
                if let Some(conn_key) = conn.conn_key {
                    warn!("remove store connection for store {} with token {:?}",
                          conn_key.store_id,
                          token);
                    self.store_tokens.remove(&conn_key);
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

    pub fn add_new_conn<H: Handler>(&mut self,
                                    event_loop: &mut EventLoop<H>,
                                    new_token: Token,
                                    sock: TcpStream,
                                    conn_key: Option<StoreConnKey>)
                                    -> Result<()> {
        // TODO: check conn max capacity.

        try!(sock.set_nodelay(true));
        try!(sock.set_send_buffer_size(self.cfg.send_buffer_size));
        try!(sock.set_recv_buffer_size(self.cfg.recv_buffer_size));

        try!(event_loop.register(&sock,
                                 new_token,
                                 EventSet::readable() | EventSet::hup(),
                                 PollOpt::edge()));

        let conn = Conn::new(sock,
                             new_token,
                             conn_key,
                             self.snap_scheduler.clone(),
                             self.sendch.clone());
        self.conns.insert(new_token, conn);
        debug!("register conn {:?}", new_token);
        CONNECTION_GAUGE.inc();

        Ok(())
    }

    fn on_conn_readable<H: Handler>(&mut self,
                                    event_loop: &mut EventLoop<H>,
                                    token: Token)
                                    -> Result<()> {
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
                RECV_MSG_COUNTER.with_label_values(&["raft"]).inc();
                try!(self.ch.raft_router.send_raft_msg(msg.take_raft()));
                Ok(())
            }
            MessageType::Cmd => {
                RECV_MSG_COUNTER.with_label_values(&["cmd"]).inc();
                self.on_raft_command(msg.take_cmd_req(), token, msg_id)
            }
            MessageType::KvReq => {
                RECV_MSG_COUNTER.with_label_values(&["kv"]).inc();
                let req = msg.take_kv_req();
                debug!("notify Request token[{:?}] msg_id[{}] type[{:?}]",
                       token,
                       msg_id,
                       req.get_field_type());
                let on_resp = self.make_response_cb(token, msg_id);
                self.store.on_request(req, on_resp)
            }
            MessageType::CopReq => {
                RECV_MSG_COUNTER.with_label_values(&["coprocessor"]).inc();
                let on_resp = self.make_response_cb(token, msg_id);
                let req = RequestTask::new(msg.take_cop_req(), on_resp);
                box_try!(self.end_point_scheduler.schedule(EndPointTask::Request(req)));
                Ok(())
            }
            _ => {
                RECV_MSG_COUNTER.with_label_values(&["invalid"]).inc();
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
        };

        try!(self.ch.raft_router.send_command(msg, cb));

        Ok(())
    }

    pub fn on_readable<H: Handler>(&mut self, event_loop: &mut EventLoop<H>, token: Token) {
        if let Err(e) = self.on_conn_readable(event_loop, token) {
            debug!("handle read conn for token {:?} err {:?}, remove", token, e);
            self.remove_conn(event_loop, token);
        }
    }

    pub fn on_writable<H: Handler>(&mut self, event_loop: &mut EventLoop<H>, token: Token) {
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

    pub fn write_data<H: Handler>(&mut self,
                                  event_loop: &mut EventLoop<H>,
                                  token: Token,
                                  data: ConnData) {
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

    pub fn on_ready<H: Handler>(&mut self,
                                event_loop: &mut EventLoop<H>,
                                token: Token,
                                events: EventSet) {
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
}

// `ClientLoop` is used to handle client connection events.
pub struct ClientLoop<T: RaftStoreRouter + 'static> {
    pub h: ConnHandler<T>,
}

impl<T: RaftStoreRouter> ClientLoop<T> {
    pub fn new(event_loop: &mut EventLoop<Self>,
               cfg: &Config,
               storage: StoreHandler,
               ch: ServerChannel<T>,
               end_point_scheduler: Scheduler<EndPointTask>,
               snap_scheduler: Scheduler<SnapTask>)
               -> ClientLoop<T> {
        let sendch = SendCh::new(event_loop.channel(), "raft-client-conn-loop");
        let h = ConnHandler::new(sendch,
                                 cfg,
                                 storage,
                                 ch,
                                 end_point_scheduler,
                                 snap_scheduler);
        ClientLoop { h: h }
    }

    #[inline]
    pub fn get_sendch(&self) -> SendCh<Msg> {
        self.h.get_sendch()
    }
}

impl<T: RaftStoreRouter> Handler for ClientLoop<T> {
    type Timeout = Msg;
    type Message = Msg;

    fn ready(&mut self, event_loop: &mut EventLoop<Self>, token: Token, events: EventSet) {
        self.h.on_ready(event_loop, token, events)
    }

    fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: Msg) {
        match msg {
            Msg::Quit => event_loop.shutdown(),
            Msg::WriteData { token, data } => self.h.write_data(event_loop, token, data),
            Msg::CloseConn { token } => self.h.remove_conn(event_loop, token),
            Msg::Connect { token, sock } => {
                if let Err(e) = self.h.add_new_conn(event_loop, token, sock, None) {
                    error!("register conn err {:?}", e);
                }
            }
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

    fn tick(&mut self, _: &mut EventLoop<Self>) {
        // tick is called in the end of the loop, so if we notify to quit,
        // we will quit the server here.
        // TODO: handle quit server if event_loop is_running() returns false.
    }
}
