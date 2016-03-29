use std::collections::HashMap;
use std::boxed::Box;
use std::io::{Read, Write};

use mio::{self, Token, EventLoop, EventSet, PollOpt, Sender};
use mio::tcp::{TcpListener, TcpStream};
use protobuf::RepeatedField;

use kvproto::kvrpcpb::{CmdGetResponse, CmdScanResponse, CmdPrewriteResponse, CmdCommitResponse,
                       CmdCleanupResponse, CmdRollbackThenGetResponse, CmdCommitThenGetResponse,
                       Request, Response, MessageType, Item, ResultType, ResultType_Type,
                       LockInfo, Operator};
use storage::{Storage, Key, Value, KvPair, KvContext, Mutation, MaybeLocked, MaybeComitted,
              MaybeRolledback, Callback};
use storage::Result as StorageResult;
use storage::Error as StorageError;
use storage::EngineError;

use super::conn::Conn;
use super::{Result, ServerError};

// Token(1) instead of Token(0)
// See here: https://github.com/hjr3/mob/blob/multi-echo-blog-post/src/main.rs#L115
pub const SERVER_TOKEN: Token = Token(1);
const FIRST_CUSTOM_TOKEN: Token = Token(1024);

pub struct Server {
    pub listener: TcpListener,
    pub conns: HashMap<Token, Conn>,
    pub token_counter: usize,
    store: Storage,
}

impl Server {
    pub fn new(listener: TcpListener, conns: HashMap<Token, Conn>, store: Storage) -> Server {
        Server {
            listener: listener,
            conns: conns,
            token_counter: FIRST_CUSTOM_TOKEN.as_usize(),
            store: store,
        }
    }

    pub fn handle_get(&mut self,
                      mut msg: Request,
                      msg_id: u64,
                      token: Token,
                      event_loop: &mut EventLoop<Server>)
                      -> Result<()> {
        if !msg.has_cmd_get_req() {
            format_err!("Msg doesn't contain a CmdGetRequest");
        }
        let mut cmd_get_req = msg.take_cmd_get_req();
        let mut key_address = cmd_get_req.take_key_address();
        let key = key_address.take_key();
        let ctx = KvContext::new(key_address.get_region_id(), key_address.take_peer());
        let sender = event_loop.channel();
        let cb = Server::make_cb(Server::cmd_get_done, sender, token, msg_id);
        self.store
            .async_get(ctx, Key::from_raw(key), cmd_get_req.get_version(), cb)
            .map_err(ServerError::Storage)
    }

    pub fn handle_scan(&mut self,
                       mut msg: Request,
                       msg_id: u64,
                       token: Token,
                       event_loop: &mut EventLoop<Server>)
                       -> Result<()> {
        if !msg.has_cmd_scan_req() {
            format_err!("Msg doesn't contain a CmdScanRequest");
        }
        let mut cmd_scan_req = msg.take_cmd_scan_req();
        let sender = event_loop.channel();
        let mut start_key_addresss = cmd_scan_req.take_key_address();
        let start_key = start_key_addresss.take_key();
        let ctx = KvContext::new(start_key_addresss.get_region_id(),
                                 start_key_addresss.take_peer());
        debug!("start_key [{:?}]", start_key);
        let cb = Server::make_cb(Server::cmd_scan_done, sender, token, msg_id);
        self.store
            .async_scan(ctx,
                        Key::from_raw(start_key),
                        cmd_scan_req.get_limit() as usize,
                        cmd_scan_req.get_version(),
                        cb)
            .map_err(ServerError::Storage)
    }

    pub fn handle_prewrite(&mut self,
                           mut msg: Request,
                           msg_id: u64,
                           token: Token,
                           event_loop: &mut EventLoop<Server>)
                           -> Result<()> {
        if !msg.has_cmd_prewrite_req() {
            format_err!("Msg doesn't contain a CmdPrewriteRequest");
        }
        let mut req = msg.take_cmd_prewrite_req();
        let sender = event_loop.channel();
        let mutations = req.take_mutations()
                           .into_iter()
                           .map(|mut x| {
                               match x.get_op() {
                                   Operator::OpPut => {
                                       Mutation::Put((Key::from_raw(x.take_key()), x.take_value()))
                                   }
                                   Operator::OpDel => Mutation::Delete(Key::from_raw(x.take_key())),
                                   Operator::OpLock => Mutation::Lock(Key::from_raw(x.take_key())),
                               }
                           })
                           .collect();
        let ctx = {
            let mut key_address = req.take_key_address();
            KvContext::new(key_address.get_region_id(), key_address.take_peer())
        };
        let cb = Server::make_cb(Server::cmd_prewrite_done, sender, token, msg_id);
        self.store
            .async_prewrite(ctx,
                            mutations,
                            req.get_primary_lock().to_vec(),
                            req.get_start_version(),
                            cb)
            .map_err(ServerError::Storage)
    }

    pub fn handle_commit(&mut self,
                         mut msg: Request,
                         msg_id: u64,
                         token: Token,
                         event_loop: &mut EventLoop<Server>)
                         -> Result<()> {
        if !msg.has_cmd_commit_req() {
            format_err!("Msg doesn't contain a CmdCommitRequest");
        }
        let mut req = msg.take_cmd_commit_req();
        let sender = event_loop.channel();
        let cb = Server::make_cb(Server::cmd_commit_done, sender, token, msg_id);
        let ctx = {
            let mut first = req.get_keys_address()[0].clone();
            KvContext::new(first.get_region_id(), first.take_peer())
        };
        let keys = req.take_keys_address()
                      .into_iter()
                      .map(|mut x| Key::from_raw(x.take_key()))
                      .collect();
        self.store
            .async_commit(ctx,
                          keys,
                          req.get_start_version(),
                          req.get_commit_version(),
                          cb)
            .map_err(ServerError::Storage)
    }

    pub fn handle_cleanup(&mut self,
                          mut msg: Request,
                          msg_id: u64,
                          token: Token,
                          event_loop: &mut EventLoop<Server>)
                          -> Result<()> {
        if !msg.has_cmd_cleanup_req() {
            format_err!("Msg doesn't contain a CmdCleanupRequest");
        }
        let mut req = msg.take_cmd_cleanup_req();
        let sender = event_loop.channel();
        let cb = Server::make_cb(Server::cmd_cleanup_done, sender, token, msg_id);
        let mut key_address = req.take_key_address();
        let key = key_address.take_key();
        let ctx = KvContext::new(key_address.get_region_id(), key_address.take_peer());
        self.store
            .async_cleanup(ctx, Key::from_raw(key), req.get_start_version(), cb)
            .map_err(ServerError::Storage)
    }

    pub fn handle_commit_then_get(&mut self,
                                  mut msg: Request,
                                  msg_id: u64,
                                  token: Token,
                                  event_loop: &mut EventLoop<Server>)
                                  -> Result<()> {
        if !msg.has_cmd_commit_get_req() {
            format_err!("Msg doesn't contain a CmdCommitThenGetRequest");
        }
        let sender = event_loop.channel();
        let cb = Server::make_cb(Server::cmd_commit_get_done, sender, token, msg_id);
        let mut req = msg.take_cmd_commit_get_req();
        let mut key_address = req.take_key_address();
        let key = key_address.take_key();
        let ctx = KvContext::new(key_address.get_region_id(), key_address.take_peer());
        self.store
            .async_commit_then_get(ctx,
                                   Key::from_raw(key),
                                   req.get_lock_version(),
                                   req.get_commit_version(),
                                   req.get_get_version(),
                                   cb)
            .map_err(ServerError::Storage)
    }

    pub fn handle_rollback_then_get(&mut self,
                                    mut msg: Request,
                                    msg_id: u64,
                                    token: Token,
                                    event_loop: &mut EventLoop<Server>)
                                    -> Result<()> {
        if !msg.has_cmd_rb_get_req() {
            format_err!("Msg doesn't contain a CmdRollbackThenGetRequest");
        }
        let mut req = msg.take_cmd_rb_get_req();
        let sender = event_loop.channel();
        let cb = Server::make_cb(Server::cmd_rollback_get_done, sender, token, msg_id);
        let mut key_address = req.take_key_address();
        let key = key_address.take_key();
        let ctx = KvContext::new(key_address.get_region_id(), key_address.take_peer());
        self.store
            .async_rollback_then_get(ctx, Key::from_raw(key), req.get_lock_version(), cb)
            .map_err(ServerError::Storage)
    }

    fn cmd_get_done(r: StorageResult<Option<Value>>) -> Response {
        let mut resp = Response::new();
        let mut cmd_get_resp = CmdGetResponse::new();
        let mut res_type = ResultType::new();
        match r {
            Ok(opt) => {
                res_type.set_field_type(ResultType_Type::Ok);
                match opt {
                    Some(val) => cmd_get_resp.set_value(val),
                    None => cmd_get_resp.set_value(Vec::new()),
                }
            }
            Err(ref e) => {
                if let StorageError::Engine(EngineError::Request(ref err)) = *e {
                    if err.has_not_leader() {
                        res_type.set_field_type(ResultType_Type::NotLeader);
                        res_type.set_leader_info(err.get_not_leader().to_owned());
                    } else {
                        error!("{:?}", err);
                        res_type.set_field_type(ResultType_Type::Other);
                        res_type.set_msg(format!("engine error: {:?}", err));
                    }
                } else if r.is_locked() {
                    if let Some((_, primary, ts)) = r.get_lock() {
                        res_type.set_field_type(ResultType_Type::Locked);
                        let mut lock_info = LockInfo::new();
                        lock_info.set_primary_lock(primary);
                        lock_info.set_lock_version(ts);
                        res_type.set_lock_info(lock_info);
                    } else {
                        let err_str = "key is locked but primary info not found".to_owned();
                        error!("{}", err_str);
                        res_type.set_field_type(ResultType_Type::Other);
                        res_type.set_msg(err_str);
                    }
                } else {
                    let err_str = format!("storage error: {:?}", e);
                    error!("{}", err_str);
                    res_type.set_field_type(ResultType_Type::Retryable);
                    res_type.set_msg(err_str);
                }
            }
        }
        cmd_get_resp.set_res_type(res_type);
        resp.set_field_type(MessageType::CmdGet);
        resp.set_cmd_get_resp(cmd_get_resp);
        resp
    }

    fn cmd_scan_done(kvs: StorageResult<Vec<StorageResult<KvPair>>>) -> Response {
        let mut resp = Response::new();
        let mut cmd_scan_resp = CmdScanResponse::new();
        cmd_scan_resp.set_ok(kvs.is_ok());
        match kvs {
            Ok(kvs) => {
                // convert storage::KvPair to kvrpcpb::Item
                let mut new_kvs: Vec<Item> = Vec::new();
                for result in kvs {
                    let mut new_kv = Item::new();
                    let mut res_type = ResultType::new();
                    match result {
                        Ok((ref key, ref value)) => {
                            res_type.set_field_type(ResultType_Type::Ok);
                            new_kv.set_key(key.clone());
                            new_kv.set_value(value.clone());
                        }
                        Err(..) => {
                            if result.is_locked() {
                                if let Some((key, primary, ts)) = result.get_lock() {
                                    res_type.set_field_type(ResultType_Type::Locked);
                                    let mut lock_info = LockInfo::new();
                                    lock_info.set_primary_lock(primary);
                                    lock_info.set_lock_version(ts);
                                    res_type.set_lock_info(lock_info);
                                    new_kv.set_key(key);
                                }
                            } else {
                                res_type.set_field_type(ResultType_Type::Retryable);
                            }
                        }
                    }
                    new_kv.set_res_type(res_type);
                    new_kvs.push(new_kv);
                }
                cmd_scan_resp.set_results(RepeatedField::from_vec(new_kvs));
            }
            Err(e) => {
                error!("storage error: {:?}", e);
            }
        }
        resp.set_field_type(MessageType::CmdScan);
        resp.set_cmd_scan_resp(cmd_scan_resp);
        resp
    }

    fn cmd_prewrite_done(results: StorageResult<Vec<StorageResult<()>>>) -> Response {
        let mut resp = Response::new();
        let mut cmd_prewrite_resp = CmdPrewriteResponse::new();
        cmd_prewrite_resp.set_ok(results.is_ok());
        let mut items = Vec::new();
        match results {
            Ok(results) => {
                for result in results {
                    let mut item = Item::new();
                    let mut res_type = ResultType::new();
                    if result.is_ok() {
                        res_type.set_field_type(ResultType_Type::Ok);
                    } else if let Some((key, primary, ts)) = result.get_lock() {
                        // Actually items only contain locked item, so `ok` is always false.
                        res_type.set_field_type(ResultType_Type::Locked);
                        let mut lock_info = LockInfo::new();
                        lock_info.set_primary_lock(primary);
                        lock_info.set_lock_version(ts);
                        res_type.set_lock_info(lock_info);
                        item.set_key(key);
                    } else {
                        res_type.set_field_type(ResultType_Type::Retryable);
                    }
                    item.set_res_type(res_type);
                    items.push(item);
                }
            }
            Err(e) => {
                error!("storage error: {:?}", e);
            }
        }
        cmd_prewrite_resp.set_results(RepeatedField::from_vec(items));
        resp.set_field_type(MessageType::CmdPrewrite);
        resp.set_cmd_prewrite_resp(cmd_prewrite_resp);
        resp
    }

    fn cmd_commit_done(r: StorageResult<()>) -> Response {
        let mut resp = Response::new();
        let mut cmd_commit_resp = CmdCommitResponse::new();
        cmd_commit_resp.set_ok(r.is_ok());
        resp.set_field_type(MessageType::CmdCommit);
        resp.set_cmd_commit_resp(cmd_commit_resp);
        resp
    }

    fn cmd_cleanup_done(r: StorageResult<()>) -> Response {
        let mut resp = Response::new();
        let mut cmd_cleanup_resp = CmdCleanupResponse::new();
        let mut res_type = ResultType::new();
        if r.is_ok() {
            res_type.set_field_type(ResultType_Type::Ok);
        } else if r.is_committed() {
            res_type.set_field_type(ResultType_Type::Committed);
            if let Some(commit_ts) = r.get_commit() {
                cmd_cleanup_resp.set_commit_version(commit_ts);
            } else {
                warn!("commit_ts not found when is_committed.");
            }
        } else if r.is_rolledback() {
            res_type.set_field_type(ResultType_Type::Rolledback);
        } else {
            warn!("Cleanup other error {:?}", r.err());
            res_type.set_field_type(ResultType_Type::Retryable);
        }
        cmd_cleanup_resp.set_res_type(res_type);
        resp.set_field_type(MessageType::CmdCleanup);
        resp.set_cmd_cleanup_resp(cmd_cleanup_resp);
        resp
    }

    fn cmd_commit_get_done(r: StorageResult<Option<Value>>) -> Response {
        let mut resp = Response::new();
        let mut cmd_commit_get_resp = CmdCommitThenGetResponse::new();
        cmd_commit_get_resp.set_ok(r.is_ok());
        if let Ok(Some(val)) = r {
            cmd_commit_get_resp.set_value(val);
        }
        resp.set_field_type(MessageType::CmdCommitThenGet);
        resp.set_cmd_commit_get_resp(cmd_commit_get_resp);
        resp
    }

    fn cmd_rollback_get_done(r: StorageResult<Option<Value>>) -> Response {
        let mut resp = Response::new();
        let mut cmd_rollback_get_resp = CmdRollbackThenGetResponse::new();
        cmd_rollback_get_resp.set_ok(r.is_ok());
        if let Err(ref e) = r {
            error!("rb & get error: {}", e);
        }
        if let Ok(Some(val)) = r {
            cmd_rollback_get_resp.set_value(val);
        }
        resp.set_field_type(MessageType::CmdRollbackThenGet);
        resp.set_cmd_rb_get_resp(cmd_rollback_get_resp);
        resp
    }

    fn make_cb<T: 'static>(f: fn(StorageResult<T>) -> Response,
                           sender: Sender<QueueMessage>,
                           token: Token,
                           msg_id: u64)
                           -> Callback<T> {
        Box::new(move |r: StorageResult<T>| {
            let resp = f(r);
            let queue_msg = QueueMessage::Response(token, msg_id, resp);
            if let Err(e) = sender.send(queue_msg) {
                error!("{:?}", e);
            }
        })
    }

    fn add_new_conn(&mut self,
                    event_loop: &mut EventLoop<Server>,
                    sock: TcpStream)
                    -> Result<(Token)> {
        let new_token = Token(self.token_counter);
        let _ = sock.set_nodelay(true).map_err(|e| error!("set nodelay failed {:?}", e));
        self.conns.insert(new_token, Conn::new(sock, new_token));
        self.token_counter += 1;

        event_loop.register(&self.conns[&new_token].sock,
                            new_token,
                            EventSet::readable() | EventSet::hup(),
                            PollOpt::edge())
                  .unwrap();
        Ok(new_token)
    }

    fn remove_conn(&mut self, event_loop: &mut EventLoop<Server>, token: Token) {
        let conn = self.conns.remove(&token);
        match conn {
            Some(conn) => {
                let _ = event_loop.deregister(&conn.sock);
            }
            None => {
                warn!("missing connection for token {}", token.as_usize());
            }
        }
    }

    fn handle_server_readable(&mut self, event_loop: &mut EventLoop<Server>) {
        loop {
            let sock = match self.listener.accept() {
                // Some(sock, addr)
                Ok(Some((sock, _))) => sock,
                Ok(None) => {
                    debug!("no connection, accept later.");
                    return;
                }
                Err(e) => {
                    error!("accept error: {}", e);
                    return;
                }
            };
            let _ = self.add_new_conn(event_loop, sock);
        }
    }

    fn handle_conn_readable(&mut self, event_loop: &mut EventLoop<Server>, token: Token) {
        let mut conn = match self.conns.get_mut(&token) {
            Some(c) => c,
            None => {
                error!("Get connection failed token[{}]", token.0);
                return;
            }
        };
        if let Err(e) = conn.read(event_loop) {
            error!("read failed with {:?}", e);
        }
    }

    fn handle_writable(&mut self, event_loop: &mut EventLoop<Server>, token: Token) {
        let mut conn = match self.conns.get_mut(&token) {
            Some(c) => c,
            None => {
                error!("Get connection failed token[{}]", token.0);
                return;
            }
        };
        if let Err(e) = conn.write(event_loop) {
            error!("write failed with {:?}", e);
        }
    }

    fn handle_request(&mut self,
                      event_loop: &mut EventLoop<Server>,
                      token: Token,
                      msg_id: u64,
                      req: Request) {
        debug!("notify Request token[{}] msg_id[{}] type[{:?}]",
               token.0,
               msg_id,
               req.get_field_type());
        if let Err(e) = match req.get_field_type() {
            MessageType::CmdGet => self.handle_get(req, msg_id, token, event_loop),
            MessageType::CmdScan => self.handle_scan(req, msg_id, token, event_loop),
            MessageType::CmdPrewrite => self.handle_prewrite(req, msg_id, token, event_loop),
            MessageType::CmdCommit => self.handle_commit(req, msg_id, token, event_loop),
            MessageType::CmdCleanup => self.handle_cleanup(req, msg_id, token, event_loop),
            MessageType::CmdCommitThenGet => {
                self.handle_commit_then_get(req, msg_id, token, event_loop)
            }
            MessageType::CmdRollbackThenGet => {
                self.handle_rollback_then_get(req, msg_id, token, event_loop)
            }
        } {
            error!("Some error occur err[{:?}]", e);
        }
    }

    fn handle_response(&mut self,
                       event_loop: &mut EventLoop<Server>,
                       token: Token,
                       msg_id: u64,
                       resp: Response) {

        debug!("notify Response token[{}] msg_id[{}] type[{:?}]",
               token.0,
               msg_id,
               resp.get_field_type());
        let mut conn = match self.conns.get_mut(&token) {
            Some(c) => c,
            None => {
                error!("Get connection failed token[{}]", token.0);
                return;
            }
        };
        let _ = conn.append_write_buf(event_loop, msg_id, resp);
    }
}

#[allow(dead_code)]
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
            self.remove_conn(event_loop, token);
            return;
        }

        if events.is_readable() {
            match token {
                SERVER_TOKEN => {
                    self.handle_server_readable(event_loop);
                }
                token => {
                    self.handle_conn_readable(event_loop, token);
                }
            }
        }

        if events.is_writable() {
            self.handle_writable(event_loop, token);
        }
    }

    fn notify(&mut self, event_loop: &mut EventLoop<Server>, msg: QueueMessage) {
        match msg {
            QueueMessage::Request(token, msg_id, req) => {
                self.handle_request(event_loop, token, msg_id, req);
            }
            QueueMessage::Response(token, msg_id, resp) => {
                self.handle_response(event_loop, token, msg_id, resp);
            }
            QueueMessage::Quit => event_loop.shutdown(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use mio::EventLoop;

    use kvproto::kvrpcpb::*;
    use kvproto::errorpb::NotLeader;
    use storage::{self, Value, Storage, Dsn, txn, mvcc, engine};
    use storage::Error;
    use storage::Result as StorageResult;
    use super::*;

    #[test]
    fn test_quit() {
        use std::collections::HashMap;
        use mio::tcp::TcpListener;
        let mut event_loop = EventLoop::new().unwrap();
        let sender = event_loop.channel();
        let h = thread::spawn(move || {
            let l = TcpListener::bind(&"127.0.0.1:64321".parse().unwrap()).unwrap();
            let store = Storage::new(Dsn::Memory).unwrap();
            let mut srv = Server::new(l, HashMap::new(), store);
            event_loop.run(&mut srv).unwrap();
        });
        // Without this thread will be hang.
        let _ = sender.send(QueueMessage::Quit);
        h.join().unwrap();
    }

    #[test]
    fn test_get_done_none() {
        let actual_resp = Server::cmd_get_done(Ok(None));
        let mut exp_resp = Response::new();
        let mut exp_cmd_resp = CmdGetResponse::new();
        exp_cmd_resp.set_res_type(make_res_type(ResultType_Type::Ok));
        exp_cmd_resp.set_value(Vec::new());
        exp_resp.set_field_type(MessageType::CmdGet);
        exp_resp.set_cmd_get_resp(exp_cmd_resp);
        assert_eq!(exp_resp, actual_resp);
    }

    #[test]
    fn test_get_done_some() {
        let storage_val = vec![0x0; 0x8];
        let actual_resp = Server::cmd_get_done(Ok(Some(storage_val)));
        let mut exp_resp = Response::new();
        let mut exp_cmd_resp = CmdGetResponse::new();
        exp_cmd_resp.set_res_type(make_res_type(ResultType_Type::Ok));
        exp_cmd_resp.set_value(vec![0x0; 0x8]);
        exp_resp.set_field_type(MessageType::CmdGet);
        exp_resp.set_cmd_get_resp(exp_cmd_resp);
        assert_eq!(exp_resp, actual_resp);
    }

    #[test]
    // #[should_panic]
    fn test_get_done_error() {
        let actual_resp = Server::cmd_get_done(Err(Error::other("error")));
        let mut exp_resp = Response::new();
        let mut exp_cmd_resp = CmdGetResponse::new();
        let mut res_type = make_res_type(ResultType_Type::Retryable);
        res_type.set_msg("storage error: Other(Any)".to_owned());
        exp_cmd_resp.set_res_type(res_type);
        exp_resp.set_field_type(MessageType::CmdGet);
        exp_resp.set_cmd_get_resp(exp_cmd_resp);
        assert_eq!(exp_resp, actual_resp);
    }

    #[test]
    fn test_scan_done_empty() {
        let actual_resp = Server::cmd_scan_done(Ok(Vec::new()));
        let mut exp_resp = Response::new();
        let mut exp_cmd_resp = CmdScanResponse::new();
        exp_cmd_resp.set_ok(true);
        exp_resp.set_field_type(MessageType::CmdScan);
        exp_resp.set_cmd_scan_resp(exp_cmd_resp);
        assert_eq!(exp_resp, actual_resp);
    }

    #[test]
    fn test_scan_done_some() {
        let k0 = vec![0x0, 0x0];
        let v0 = vec![0xff, 0xff];
        let k1 = vec![0x0, 0x1];
        let v1 = vec![0xff, 0xfe];
        let kvs = vec![Ok((k0.clone(), v0.clone())), Ok((k1.clone(), v1.clone()))];
        let actual_resp = Server::cmd_scan_done(Ok(kvs));
        assert_eq!(MessageType::CmdScan, actual_resp.get_field_type());
        let actual_cmd_resp = actual_resp.get_cmd_scan_resp();
        assert_eq!(true, actual_cmd_resp.get_ok());
        let actual_kvs = actual_cmd_resp.get_results();
        assert_eq!(2, actual_kvs.len());
        assert_eq!(make_res_type(ResultType_Type::Ok),
                   *actual_kvs[0].get_res_type());
        assert_eq!(k0, actual_kvs[0].get_key());
        assert_eq!(v0, actual_kvs[0].get_value());
        assert_eq!(make_res_type(ResultType_Type::Ok),
                   *actual_kvs[1].get_res_type());
        assert_eq!(k1, actual_kvs[1].get_key());
        assert_eq!(v1, actual_kvs[1].get_value());
    }

    #[test]
    fn test_scan_done_lock() {
        use kvproto::kvrpcpb::LockInfo;
        let k0 = vec![0x0, 0x0];
        let v0 = vec![0xff, 0xff];
        let k1 = vec![0x0, 0x1];
        let k1_primary = k0.clone();
        let k1_ts = 10000;
        let kvs = vec![Ok((k0.clone(), v0.clone())),
                       make_lock_error(k1.clone(), k1_primary.clone(), k1_ts)];
        let actual_resp = Server::cmd_scan_done(Ok(kvs));
        assert_eq!(MessageType::CmdScan, actual_resp.get_field_type());
        let actual_cmd_resp = actual_resp.get_cmd_scan_resp();
        assert_eq!(true, actual_cmd_resp.get_ok());
        let actual_kvs = actual_cmd_resp.get_results();
        assert_eq!(2, actual_kvs.len());
        assert_eq!(make_res_type(ResultType_Type::Ok),
                   *actual_kvs[0].get_res_type());
        assert_eq!(k0, actual_kvs[0].get_key());
        assert_eq!(v0, actual_kvs[0].get_value());
        let mut exp_res_type1 = make_res_type(ResultType_Type::Locked);
        let mut lock_info1 = LockInfo::new();
        lock_info1.set_primary_lock(k1_primary.clone());
        lock_info1.set_lock_version(k1_ts);
        exp_res_type1.set_lock_info(lock_info1);
        assert_eq!(exp_res_type1, *actual_kvs[1].get_res_type());
        assert_eq!(k1, actual_kvs[1].get_key());
        assert_eq!(k1_primary.clone(),
                   actual_kvs[1].get_res_type().get_lock_info().get_primary_lock());
        assert_eq!(k1_ts,
                   actual_kvs[1].get_res_type().get_lock_info().get_lock_version());
    }

    #[test]
    fn test_prewrite_done_ok() {
        let actual_resp = Server::cmd_prewrite_done(Ok(Vec::new()));
        assert_eq!(MessageType::CmdPrewrite, actual_resp.get_field_type());
        assert_eq!(true, actual_resp.get_cmd_prewrite_resp().get_ok());
    }

    #[test]
    fn test_prewrite_done_err() {
        let err = Error::other("prewrite error");
        let actual_resp = Server::cmd_prewrite_done(Err(err));
        assert_eq!(MessageType::CmdPrewrite, actual_resp.get_field_type());
        assert_eq!(false, actual_resp.get_cmd_prewrite_resp().get_ok());
    }

    #[test]
    fn test_commit_done_ok() {
        let actual_resp = Server::cmd_commit_done(Ok(()));
        assert_eq!(MessageType::CmdCommit, actual_resp.get_field_type());
        assert_eq!(true, actual_resp.get_cmd_commit_resp().get_ok());
    }

    #[test]
    fn test_commit_done_err() {
        let err = Error::other("commit error");
        let actual_resp = Server::cmd_commit_done(Err(err));
        assert_eq!(MessageType::CmdCommit, actual_resp.get_field_type());
        assert_eq!(false, actual_resp.get_cmd_commit_resp().get_ok());
    }

    #[test]
    fn test_cleanup_done_ok() {
        let actual_resp = Server::cmd_cleanup_done(Ok(()));
        assert_eq!(MessageType::CmdCleanup, actual_resp.get_field_type());
        assert_eq!(make_res_type(ResultType_Type::Ok),
                   *actual_resp.get_cmd_cleanup_resp().get_res_type());
    }

    #[test]
    fn test_cleanup_done_err() {
        let err = Error::other("cleanup error");
        let actual_resp = Server::cmd_cleanup_done(Err(err));
        assert_eq!(MessageType::CmdCleanup, actual_resp.get_field_type());
        assert_eq!(make_res_type(ResultType_Type::Retryable),
                   *actual_resp.get_cmd_cleanup_resp().get_res_type());
    }

    #[test]
    fn test_not_leader() {
        use kvproto::errorpb::NotLeader;
        let mut leader_info = NotLeader::new();
        leader_info.set_region_id(1);
        let storage_res: StorageResult<Option<Value>> =
            make_not_leader_error(leader_info.to_owned());
        let actual_resp = Server::cmd_get_done(storage_res);
        assert_eq!(MessageType::CmdGet, actual_resp.get_field_type());
        let mut exp_res_type = make_res_type(ResultType_Type::NotLeader);
        exp_res_type.set_leader_info(leader_info.to_owned());
        assert_eq!(exp_res_type, *actual_resp.get_cmd_get_resp().get_res_type());
    }

    fn make_lock_error<T>(key: Vec<u8>, primary: Vec<u8>, ts: u64) -> StorageResult<T> {
        Err(mvcc::Error::KeyIsLocked {
            key: key,
            primary: primary,
            ts: ts,
        })
            .map_err(txn::Error::from)
            .map_err(storage::Error::from)
    }

    fn make_not_leader_error<T>(leader_info: NotLeader) -> StorageResult<T> {
        use kvproto::errorpb::Error;
        let mut err = Error::new();
        err.set_not_leader(leader_info);
        Err(engine::Error::Request(err)).map_err(storage::Error::from)
    }

    fn make_res_type(tp: ResultType_Type) -> ResultType {
        let mut res_type = ResultType::new();
        res_type.set_field_type(tp);
        res_type
    }
}
