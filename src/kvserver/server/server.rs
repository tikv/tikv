#![allow(unused_must_use)]
use std::collections::HashMap;
use std::boxed::Box;
use std::io::{Read, Write};

use mio::{self, Token, EventLoop, EventSet, PollOpt};
use mio::tcp::TcpListener;
use protobuf::core::Message;
use protobuf::RepeatedField;

use proto::kvrpcpb;
use proto::kvrpcpb::{CmdGetRequest, CmdGetResponse, CmdScanRequest, CmdScanResponse,
                     CmdPrewriteRequest, CmdPrewriteResponse, CmdCommitRequest, CmdCommitResponse,
                     Request, Response, MessageType};
use util;
use util::codec::rpc::{encode_msg, MSG_HEADER_LEN};
use storage;
use storage::{Key, Storage, Value, KvPair};

use super::conn::Conn;

pub type Result<T> = ::std::result::Result<T, ServerError>;

pub const SERVER_TOKEN: Token = Token(0);

quick_error! {
    #[derive(Debug)]
    pub enum ServerError {
        Storage(err: storage::Error) {
            from()
            cause(err)
            description(err.description())
        }
        FormatError(desc: String) {
            description(desc)
        }
    }
}

pub struct Server {
    pub listener: TcpListener,
    pub conns: HashMap<Token, Conn>,
    pub token_counter: usize,
    store: Storage,
}

impl Server {
    pub fn new(listener: TcpListener,
               conns: HashMap<Token, Conn>,
               tc: usize,
               store: Storage)
               -> Server {
        return Server {
            listener: listener,
            conns: conns,
            token_counter: tc,
            store: store,
        };
    }
    pub fn handle_get(&mut self,
                      msg: &Request,
                      msg_id: u64,
                      token: Token,
                      event_loop: &mut EventLoop<Server>)
                      -> Result<()> {
        if !msg.has_cmd_get_req() {
            error!("Msg doesn't containe a CmdGetRequest");
            return Err(ServerError::FormatError("Msg doesn't containe a CmdGetRequest"
                                                    .to_string()));
        }
        let cmd_get_req: &CmdGetRequest = msg.get_cmd_get_req();
        let key = cmd_get_req.get_key().iter().cloned().collect();
        let sender = event_loop.channel();
        match self.store.async_get(key,
                                   cmd_get_req.get_version(),
                                   Box::new(move |v: ::std::result::Result<Option<Value>,
                                                                             storage::Error>| {
                                       let resp: Response = Server::cmd_get_done(v);
                                       let queue_msg: QueueMessage = QueueMessage::Response(token,
                                                                                            msg_id,
                                                                                            resp);
                                       sender.send(queue_msg).map_err(|e| error!("{:?}", e));
                                   })) {
            Ok(()) => Ok(()),
            Err(why) => return Err(ServerError::Storage(why)),
        }
    }
    pub fn handle_scan(&mut self,
                       msg: &Request,
                       msg_id: u64,
                       token: Token,
                       event_loop: &mut EventLoop<Server>)
                       -> Result<()> {
        if !msg.has_cmd_scan_req() {
            error!("Msg doesn't containe a CmdScanRequest");
            return Err(ServerError::FormatError("Msg doesn't containe a CmdScanRequest"
                                                    .to_string()));
        }
        let cmd_scan_req: &CmdScanRequest = msg.get_cmd_scan_req();
        let sender = event_loop.channel();
        // convert [u8] to Vec[u8]
        let start_key: Key = cmd_scan_req.get_key().iter().cloned().collect();
        debug!("start_key [{:?}]", start_key);
        match self.store
                  .async_scan(start_key,
                              cmd_scan_req.get_limit() as usize,
                              cmd_scan_req.get_version(),
                              Box::new(move |kvs: ::std::result::Result<Vec<KvPair>,
                                                                          storage::Error>| {
                                  let resp: Response = Server::cmd_scan_done(kvs);
                                  let queue_msg: QueueMessage = QueueMessage::Response(token,
                                                                                       msg_id,
                                                                                       resp);
                                  sender.send(queue_msg).map_err(|e| error!("{:?}", e));
                              })) {
            Ok(()) => Ok(()),
            Err(why) => return Err(ServerError::Storage(why)),
        }
    }
    pub fn handle_prewrite(&mut self,
                           msg: &Request,
                           msg_id: u64,
                           token: Token,
                           event_loop: &mut EventLoop<Server>)
                           -> Result<()> {
        if !msg.has_cmd_prewrite_req() {
            error!("Msg doesn't containe a CmdPrewriteRequest");
            return Err(ServerError::FormatError("Msg doesn't containe a CmdPrewriteRequest"
                                                    .to_string()));
        }
        let cmd_prewrite_req: &CmdPrewriteRequest = msg.get_cmd_prewrite_req();
        let sender = event_loop.channel();
        let puts: Vec<_> = cmd_prewrite_req.get_puts()
                                           .iter()
                                           .map(|kv| {
                                               (util::array_to_vec(kv.get_key()),
                                                util::array_to_vec(kv.get_value()))
                                           })
                                           .collect();
        let deletes: Vec<_> = cmd_prewrite_req.get_dels().iter().cloned().collect();
        let locks: Vec<_> = cmd_prewrite_req.get_locks().iter().cloned().collect();
        match self.store
                  .async_prewrite(puts,
                                  deletes,
                                  locks,
                                  cmd_prewrite_req.get_start_version(),
                                  Box::new(move |r: ::std::result::Result<(),
                                                                            storage::Error>| {
                                      let resp: Response = Server::cmd_prewrite_done(r);
                                      let queue_msg: QueueMessage = QueueMessage::Response(token,
                                                                                           msg_id,
                                                                                           resp);
                                      sender.send(queue_msg).map_err(|e| error!("{:?}", e));
                                  })) {
            Ok(()) => Ok(()),
            Err(why) => return Err(ServerError::Storage(why)),
        }
    }
    pub fn handle_commit(&mut self,
                         msg: &Request,
                         msg_id: u64,
                         token: Token,
                         event_loop: &mut EventLoop<Server>)
                         -> Result<()> {
        if !msg.has_cmd_commit_req() {
            error!("Msg doesn't containe a CmdCommitRequest");
            return Err(ServerError::FormatError("Msg doesn't containe a CmdCommitRequest"
                                                    .to_string()));
        }
        let cmd_commit_req: &CmdCommitRequest = msg.get_cmd_commit_req();
        let sender = event_loop.channel();
        match self.store
                  .async_commit(cmd_commit_req.get_start_version(),
                                cmd_commit_req.get_commit_version(),
                                Box::new(move |r: ::std::result::Result<(), storage::Error>| {
                                    let resp: Response = Server::cmd_commit_done(r);
                                    let queue_msg: QueueMessage = QueueMessage::Response(token,
                                                                                         msg_id,
                                                                                         resp);
                                    // [TODO]: retry
                                    sender.send(queue_msg).map_err(|e| error!("{:?}", e));
                                })) {
            Ok(()) => Ok(()),
            Err(why) => Err(ServerError::Storage(why)),
        }
    }
    fn cmd_get_done(r: ::std::result::Result<Option<Value>, storage::Error>) -> Response {
        let mut resp: Response = Response::new();
        let mut cmd_get_resp: CmdGetResponse = CmdGetResponse::new();
        cmd_get_resp.set_ok(r.is_ok());
        match r {
            Ok(v) => {
                match v {
                    Some(val) => cmd_get_resp.set_value(val),
                    None => cmd_get_resp.set_value(Vec::new()),
                }
                cmd_get_resp.set_ok(true);
            }
            Err(_) => {}
        }
        resp.set_field_type(MessageType::CmdGet);
        resp.set_cmd_get_resp(cmd_get_resp);
        resp
    }
    fn cmd_scan_done(kvs: ::std::result::Result<Vec<KvPair>, storage::Error>) -> Response {
        let mut resp: Response = Response::new();
        let mut cmd_scan_resp: CmdScanResponse = CmdScanResponse::new();
        cmd_scan_resp.set_ok(kvs.is_ok());
        match kvs {
            Ok(v) => {
                // convert storage::KvPair to kvrpcpb::KvPair
                let mut new_kvs: Vec<kvrpcpb::KvPair> = Vec::new();
                for x in &v {
                    let mut new_kv: kvrpcpb::KvPair = kvrpcpb::KvPair::new();
                    new_kv.set_key(x.0.clone());
                    new_kv.set_value(x.1.clone());
                    new_kvs.push(new_kv);
                }
                cmd_scan_resp.set_results(RepeatedField::from_vec(new_kvs));
            }
            Err(_) => {}
        }
        resp.set_field_type(MessageType::CmdScan);
        resp.set_cmd_scan_resp(cmd_scan_resp);
        resp
    }
    fn cmd_prewrite_done(r: ::std::result::Result<(), storage::Error>) -> Response {
        let mut resp: Response = Response::new();
        let mut cmd_prewrite_resp: CmdPrewriteResponse = CmdPrewriteResponse::new();
        cmd_prewrite_resp.set_ok(r.is_ok());
        resp.set_field_type(MessageType::CmdPrewrite);
        resp.set_cmd_prewrite_resp(cmd_prewrite_resp);
        resp
    }
    fn cmd_commit_done(r: ::std::result::Result<(), storage::Error>) -> Response {
        let mut resp: Response = Response::new();
        let mut cmd_commit_resp: CmdCommitResponse = CmdCommitResponse::new();
        cmd_commit_resp.set_ok(r.is_ok());
        resp.set_field_type(MessageType::CmdCommit);
        resp.set_cmd_commit_resp(cmd_commit_resp);
        resp
    }
}

pub enum QueueMessage {
    // Request(token, msg_id, kvrpc_request)
    Request(Token, u64, Request),
    // Request(token, msg_id, kvrpc_response)
    Response(Token, u64, Response),
    Quit,
}

impl mio::Handler for Server {
    type Timeout = ();
    type Message = QueueMessage;

    fn ready(&mut self, event_loop: &mut EventLoop<Server>, token: Token, events: EventSet) {
        if events.is_hup() || events.is_error() {
            self.conns.remove(&token);
            return;
        }

        if events.is_readable() {
            match token {
                SERVER_TOKEN => {
                    let sock = match self.listener.accept() {
                        // Some(sock, addr)
                        Ok(Some((sock, _))) => sock,
                        Ok(None) => unreachable!(),
                        Err(e) => {
                            error!("accept error: {}", e);
                            return;
                        }
                    };

                    let new_token = Token(self.token_counter);
                    match sock.set_nodelay(true) {
                        Ok(_) => {}
                        Err(e) => error!("set nodeplay failed {:?}", e),
                    }
                    self.conns.insert(new_token, Conn::new(sock));
                    self.token_counter += 1;

                    event_loop.register(&self.conns[&new_token].sock,
                                        new_token,
                                        EventSet::readable(),
                                        PollOpt::level() | PollOpt::oneshot())
                              .unwrap();

                }
                token => {
                    let mut conn: &mut Conn = match self.conns.get_mut(&token) {
                        Some(c) => c,
                        None => {
                            error!("Get connection failed token[{}]", token.0);
                            return;
                        }
                    };
                    conn.read(token, event_loop);
                    event_loop.reregister(&conn.sock,
                                          token,
                                          conn.interest,
                                          PollOpt::level() | PollOpt::oneshot())
                              .unwrap();
                }
            }
        }

        if events.is_writable() {
            let mut conn: &mut Conn = match self.conns.get_mut(&token) {
                Some(c) => c,
                None => {
                    error!("Get connection failed token[{}]", token.0);
                    return;
                }
            };
            conn.write();
            event_loop.reregister(&conn.sock,
                                  token,
                                  conn.interest,
                                  PollOpt::level() | PollOpt::oneshot())
                      .unwrap();
        }
    }

    fn notify(&mut self, event_loop: &mut EventLoop<Server>, msg: QueueMessage) {
        match msg {
            QueueMessage::Request(token, msg_id, req) => {
                debug!("notify Request token[{}] msg_id[{}] type[{:?}]",
                       token.0,
                       msg_id,
                       req.get_field_type());
                match req.get_field_type() {
                    MessageType::CmdGet => {
                        if let Err(e) = self.handle_get(&req, msg_id, token, event_loop) {
                            error!("Some error occur err[{:?}]", e);
                        };
                    }
                    MessageType::CmdScan => {
                        if let Err(e) = self.handle_scan(&req, msg_id, token, event_loop) {
                            error!("Some error occur err[{:?}]", e);
                        }
                    }
                    MessageType::CmdPrewrite => {
                        if let Err(e) = self.handle_prewrite(&req, msg_id, token, event_loop) {
                            error!("Some error occur err[{:?}]", e);
                        }
                    }
                    MessageType::CmdCommit => {
                        if let Err(e) = self.handle_commit(&req, msg_id, token, event_loop) {
                            error!("Some error occur err[{:?}]", e);
                        }
                    }
                }
            }
            QueueMessage::Response(token, msg_id, resp) => {
                debug!("notify Response token[{}] msg_id[{}] type[{:?}]",
                       token.0,
                       msg_id,
                       resp.get_field_type());
                let mut conn: &mut Conn = match self.conns.get_mut(&token) {
                    Some(c) => c,
                    None => {
                        error!("Get connection failed token[{}]", token.0);
                        return;
                    }
                };
                conn.res.clear();
                let resp_len: usize = MSG_HEADER_LEN + resp.compute_size() as usize;
                if conn.res.capacity() < resp_len {
                    conn.res = Vec::with_capacity(resp_len);
                }
                encode_msg(&mut conn.res, msg_id, &resp).unwrap();
                conn.interest.insert(EventSet::writable());
                event_loop.reregister(&conn.sock,
                                      token,
                                      conn.interest,
                                      PollOpt::level() | PollOpt::oneshot())
                          .unwrap();
            }
            QueueMessage::Quit => event_loop.shutdown(),
        }
    }
}
