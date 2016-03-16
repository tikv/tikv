use std::collections::HashMap;
use std::boxed::Box;
use std::io::{Read, Write};

use mio::{self, Token, EventLoop, EventSet, PollOpt, Sender};
use mio::tcp::{TcpListener, TcpStream};
use protobuf::RepeatedField;

use kvproto::kvrpcpb::{CmdGetRequest, CmdGetResponse, CmdScanRequest, CmdScanResponse,
                       CmdPrewriteRequest, CmdPrewriteResponse, CmdCommitRequest,
                       CmdCommitResponse, CmdCleanupRequest, CmdCleanupResponse,
                       CmdCleanupResponse_ResultType, CmdRollbackThenGetRequest,
                       CmdRollbackThenGetResponse, CmdCommitThenGetRequest,
                       CmdPrewriteResponse_Item, CmdScanResponse_Item, CmdCommitThenGetResponse,
                       Request, Response, MessageType, ResultType};
use storage::{Key, Storage, Value, KvPair, Mutation, MaybeLocked, MaybeComitted, MaybeRolledback,
              Callback};
use storage::Result as ResultStorage;

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
                      msg: &Request,
                      msg_id: u64,
                      token: Token,
                      event_loop: &mut EventLoop<Server>)
                      -> Result<()> {
        if !msg.has_cmd_get_req() {
            format_err!("Msg doesn't contain a CmdGetRequest");
        }
        let cmd_get_req: &CmdGetRequest = msg.get_cmd_get_req();
        let key = cmd_get_req.get_key().to_vec();
        let sender = event_loop.channel();
        let cb = Server::make_cb::<Option<Value>>(Server::cmd_get_done, sender, token, msg_id);
        self.store
            .async_get(key, cmd_get_req.get_version(), cb)
            .map_err(ServerError::Storage)
    }

    pub fn handle_scan(&mut self,
                       msg: &Request,
                       msg_id: u64,
                       token: Token,
                       event_loop: &mut EventLoop<Server>)
                       -> Result<()> {
        if !msg.has_cmd_scan_req() {
            format_err!("Msg doesn't contain a CmdScanRequest");
        }
        let cmd_scan_req: &CmdScanRequest = msg.get_cmd_scan_req();
        let sender = event_loop.channel();
        // convert [u8] to Vec[u8]
        let start_key: Key = cmd_scan_req.get_key().to_vec();
        debug!("start_key [{:?}]", start_key);
        let cb = Server::make_cb::<Vec<ResultStorage<KvPair>>>(Server::cmd_scan_done,
                                                               sender,
                                                               token,
                                                               msg_id);
        self.store
            .async_scan(start_key,
                        cmd_scan_req.get_limit() as usize,
                        cmd_scan_req.get_version(),
                        cb)
            .map_err(ServerError::Storage)
    }

    pub fn handle_prewrite(&mut self,
                           msg: &Request,
                           msg_id: u64,
                           token: Token,
                           event_loop: &mut EventLoop<Server>)
                           -> Result<()> {
        if !msg.has_cmd_prewrite_req() {
            format_err!("Msg doesn't contain a CmdPrewriteRequest");
        }
        let cmd_prewrite_req: &CmdPrewriteRequest = msg.get_cmd_prewrite_req();
        let sender = event_loop.channel();
        let mut mutations = vec![];
        mutations.extend(cmd_prewrite_req.get_puts().iter().map(|kv| {
            Mutation::Put((kv.get_key().to_vec(), kv.get_value().to_vec()))
        }));
        mutations.extend(cmd_prewrite_req.get_dels()
                                         .iter()
                                         .map(|k| Mutation::Delete(k.to_owned())));
        mutations.extend(cmd_prewrite_req.get_locks()
                                         .iter()
                                         .map(|k| Mutation::Lock(k.to_owned())));
        let cb = Server::make_cb::<Vec<ResultStorage<()>>>(Server::cmd_prewrite_done,
                                                           sender,
                                                           token,
                                                           msg_id);
        self.store
            .async_prewrite(mutations,
                            cmd_prewrite_req.get_primary_lock().to_vec(),
                            cmd_prewrite_req.get_start_version(),
                            cb)
            .map_err(ServerError::Storage)
    }

    pub fn handle_commit(&mut self,
                         msg: &Request,
                         msg_id: u64,
                         token: Token,
                         event_loop: &mut EventLoop<Server>)
                         -> Result<()> {
        if !msg.has_cmd_commit_req() {
            format_err!("Msg doesn't contain a CmdCommitRequest");
        }
        let cmd_commit_req: &CmdCommitRequest = msg.get_cmd_commit_req();
        let sender = event_loop.channel();
        let cb = Server::make_cb::<()>(Server::cmd_commit_done, sender, token, msg_id);
        self.store
            .async_commit(cmd_commit_req.get_keys().to_vec(),
                          cmd_commit_req.get_start_version(),
                          cmd_commit_req.get_commit_version(),
                          cb)
            .map_err(ServerError::Storage)
    }

    pub fn handle_cleanup(&mut self,
                          msg: &Request,
                          msg_id: u64,
                          token: Token,
                          event_loop: &mut EventLoop<Server>)
                          -> Result<()> {
        if !msg.has_cmd_cleanup_req() {
            format_err!("Msg doesn't contain a CmdCleanupRequest");
        }
        let cmd_cleanup_req: &CmdCleanupRequest = msg.get_cmd_cleanup_req();
        let sender = event_loop.channel();
        let cb = Server::make_cb::<()>(Server::cmd_cleanup_done, sender, token, msg_id);
        self.store
            .async_cleanup(cmd_cleanup_req.get_key().to_vec(),
                           cmd_cleanup_req.get_start_version(),
                           cb)
            .map_err(ServerError::Storage)
    }

    pub fn handle_commit_then_get(&mut self,
                                  msg: &Request,
                                  msg_id: u64,
                                  token: Token,
                                  event_loop: &mut EventLoop<Server>)
                                  -> Result<()> {
        if !msg.has_cmd_commit_get_req() {
            format_err!("Msg doesn't contain a CmdCommitThenGetRequest");
        }
        let cmd_commit_get_req: &CmdCommitThenGetRequest = msg.get_cmd_commit_get_req();
        let sender = event_loop.channel();
        let cb = Server::make_cb::<Option<Value>>(Server::cmd_commit_get_done,
                                                  sender,
                                                  token,
                                                  msg_id);
        self.store
            .async_commit_then_get(cmd_commit_get_req.get_key().to_vec(),
                                   cmd_commit_get_req.get_lock_version(),
                                   cmd_commit_get_req.get_commit_version(),
                                   cmd_commit_get_req.get_get_version(),
                                   cb)
            .map_err(ServerError::Storage)
    }

    pub fn handle_rollback_then_get(&mut self,
                                    msg: &Request,
                                    msg_id: u64,
                                    token: Token,
                                    event_loop: &mut EventLoop<Server>)
                                    -> Result<()> {
        if !msg.has_cmd_rb_get_req() {
            format_err!("Msg doesn't contain a CmdRollbackThenGetRequest");
        }
        let cmd_rollback_get_req: &CmdRollbackThenGetRequest = msg.get_cmd_rb_get_req();
        let sender = event_loop.channel();
        let cb = Server::make_cb::<Option<Value>>(Server::cmd_rollback_get_done,
                                                  sender,
                                                  token,
                                                  msg_id);
        self.store
            .async_rollback_then_get(cmd_rollback_get_req.get_key().to_vec(),
                                     cmd_rollback_get_req.get_lock_version(),
                                     cb)
            .map_err(ServerError::Storage)
    }

    fn cmd_get_done(r: ResultStorage<Option<Value>>) -> Response {
        let mut resp: Response = Response::new();
        let mut cmd_get_resp: CmdGetResponse = CmdGetResponse::new();
        match r {
            Ok(opt) => {
                cmd_get_resp.set_res_type(ResultType::Ok);
                match opt {
                    Some(val) => cmd_get_resp.set_value(val),
                    None => cmd_get_resp.set_value(Vec::new()),
                }
            }
            Err(ref e) => {
                if r.is_locked() {
                    cmd_get_resp.set_res_type(ResultType::Locked);
                    if let Some((_, primary, ts)) = r.get_lock() {
                        cmd_get_resp.set_primary_lock(primary);
                        cmd_get_resp.set_lock_version(ts);
                    } else {
                        error!("key is locked but primary info not found");
                    }
                } else {
                    cmd_get_resp.set_res_type(ResultType::Retryable);
                    error!("storage error: {:?}", e);
                }
            }
        }
        resp.set_field_type(MessageType::CmdGet);
        resp.set_cmd_get_resp(cmd_get_resp);
        resp
    }

    fn cmd_scan_done(kvs: ResultStorage<Vec<ResultStorage<KvPair>>>) -> Response {
        let mut resp: Response = Response::new();
        let mut cmd_scan_resp: CmdScanResponse = CmdScanResponse::new();
        cmd_scan_resp.set_ok(kvs.is_ok());
        match kvs {
            Ok(kvs) => {
                // convert storage::KvPair to kvrpcpb::CmdScanResponse_Item
                let mut new_kvs: Vec<CmdScanResponse_Item> = Vec::new();
                for result in kvs {
                    let mut new_kv: CmdScanResponse_Item = CmdScanResponse_Item::new();
                    match result {
                        Ok((ref key, ref value)) => {
                            new_kv.set_res_type(ResultType::Ok);
                            new_kv.set_key(key.clone());
                            new_kv.set_value(value.clone());
                        }
                        Err(..) => {
                            if result.is_locked() {
                                if let Some((key, primary, ts)) = result.get_lock() {
                                    new_kv.set_res_type(ResultType::Locked);
                                    new_kv.set_key(key);
                                    new_kv.set_primary_lock(primary);
                                    new_kv.set_lock_version(ts);
                                }
                            } else {
                                new_kv.set_res_type(ResultType::Retryable);
                            }
                        }
                    }
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

    fn cmd_prewrite_done(results: ResultStorage<Vec<ResultStorage<()>>>) -> Response {
        let mut resp: Response = Response::new();
        let mut cmd_prewrite_resp: CmdPrewriteResponse = CmdPrewriteResponse::new();
        cmd_prewrite_resp.set_ok(results.is_ok());
        let mut items: Vec<CmdPrewriteResponse_Item> = Vec::new();
        match results {
            Ok(results) => {
                for result in results {
                    let mut item = CmdPrewriteResponse_Item::new();
                    if result.is_ok() {
                        item.set_res_type(ResultType::Ok);
                    } else if let Some((key, primary, ts)) = result.get_lock() {
                        // Actually items only contain locked item, so `ok` is always false.
                        item.set_res_type(ResultType::Locked);
                        item.set_key(key);
                        item.set_primary_lock(primary);
                        item.set_lock_version(ts);
                    } else {
                        item.set_res_type(ResultType::Retryable);
                    }
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

    fn cmd_commit_done(r: ResultStorage<()>) -> Response {
        let mut resp: Response = Response::new();
        let mut cmd_commit_resp: CmdCommitResponse = CmdCommitResponse::new();
        cmd_commit_resp.set_ok(r.is_ok());
        resp.set_field_type(MessageType::CmdCommit);
        resp.set_cmd_commit_resp(cmd_commit_resp);
        resp
    }

    fn cmd_cleanup_done(r: ResultStorage<()>) -> Response {
        let mut resp: Response = Response::new();
        let mut cmd_cleanup_resp: CmdCleanupResponse = CmdCleanupResponse::new();
        if r.is_ok() {
            cmd_cleanup_resp.set_res_type(CmdCleanupResponse_ResultType::Ok);
        } else if r.is_committed() {
            cmd_cleanup_resp.set_res_type(CmdCleanupResponse_ResultType::Committed);
            if let Some(commit_ts) = r.get_commit() {
                cmd_cleanup_resp.set_commit_version(commit_ts);
            } else {
                warn!("commit_ts not found when is_committed.");
            }
        } else if r.is_rolledback() {
            cmd_cleanup_resp.set_res_type(CmdCleanupResponse_ResultType::Rollbacked);
        } else {
            warn!("Cleanup other error {:?}", r.err());
            cmd_cleanup_resp.set_res_type(CmdCleanupResponse_ResultType::Retryable);
        }
        resp.set_field_type(MessageType::CmdCleanup);
        resp.set_cmd_cleanup_resp(cmd_cleanup_resp);
        resp
    }

    fn cmd_commit_get_done(r: ResultStorage<Option<Value>>) -> Response {
        let mut resp: Response = Response::new();
        let mut cmd_commit_get_resp: CmdCommitThenGetResponse = CmdCommitThenGetResponse::new();
        cmd_commit_get_resp.set_ok(r.is_ok());
        if let Ok(Some(val)) = r {
            cmd_commit_get_resp.set_value(val);
        }
        resp.set_field_type(MessageType::CmdCommitThenGet);
        resp.set_cmd_commit_get_resp(cmd_commit_get_resp);
        resp
    }

    fn cmd_rollback_get_done(r: ResultStorage<Option<Value>>) -> Response {
        let mut resp: Response = Response::new();
        let mut cmd_rollback_get_resp: CmdRollbackThenGetResponse =
            CmdRollbackThenGetResponse::new();
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

    fn make_cb<T: 'static>(f: fn(ResultStorage<T>) -> Response,
                           sender: Sender<QueueMessage>,
                           token: Token,
                           msg_id: u64)
                           -> Callback<T> {
        Box::new(move |r: ResultStorage<T>| {
            let resp: Response = f(r);
            let queue_msg: QueueMessage = QueueMessage::Response(token, msg_id, resp);
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
        let mut conn: &mut Conn = match self.conns.get_mut(&token) {
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
        let mut conn: &mut Conn = match self.conns.get_mut(&token) {
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
            MessageType::CmdGet => self.handle_get(&req, msg_id, token, event_loop),
            MessageType::CmdScan => self.handle_scan(&req, msg_id, token, event_loop),
            MessageType::CmdPrewrite => self.handle_prewrite(&req, msg_id, token, event_loop),
            MessageType::CmdCommit => self.handle_commit(&req, msg_id, token, event_loop),
            MessageType::CmdCleanup => self.handle_cleanup(&req, msg_id, token, event_loop),
            MessageType::CmdCommitThenGet => {
                self.handle_commit_then_get(&req, msg_id, token, event_loop)
            }
            MessageType::CmdRollbackThenGet => {
                self.handle_rollback_then_get(&req, msg_id, token, event_loop)
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
        let mut conn: &mut Conn = match self.conns.get_mut(&token) {
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
    use storage::{self, Key, Value, Storage, Dsn, txn, mvcc};
    use storage::Error::Other;
    use storage::KvPair as StorageKV;
    use storage::Result as ResultStorage;
    use super::*;

    #[test]
    fn test_quit() {
        use std::collections::HashMap;
        use mio::tcp::TcpListener;
        let mut event_loop = EventLoop::new().unwrap();
        let sender = event_loop.channel();
        let h = thread::spawn(move || {
            let l: TcpListener = TcpListener::bind(&"127.0.0.1:64321".parse().unwrap()).unwrap();
            let store: Storage = Storage::new(Dsn::Memory).unwrap();
            let mut srv: Server = Server::new(l, HashMap::new(), store);
            event_loop.run(&mut srv).unwrap();
        });
        // Without this thread will be hang.
        let _ = sender.send(QueueMessage::Quit);
        h.join().unwrap();
    }

    #[test]
    fn test_get_done_none() {
        let actual_resp: Response = Server::cmd_get_done(Ok(None));
        let mut exp_resp: Response = Response::new();
        let mut exp_cmd_resp: CmdGetResponse = CmdGetResponse::new();
        exp_cmd_resp.set_res_type(ResultType::Ok);
        exp_cmd_resp.set_value(Vec::new());
        exp_resp.set_field_type(MessageType::CmdGet);
        exp_resp.set_cmd_get_resp(exp_cmd_resp);
        assert_eq!(exp_resp, actual_resp);
    }

    #[test]
    fn test_get_done_some() {
        let storage_val: Vec<_> = vec![0u8; 8];
        let actual_resp: Response = Server::cmd_get_done(Ok(Some(storage_val)));
        let mut exp_resp: Response = Response::new();
        let mut exp_cmd_resp: CmdGetResponse = CmdGetResponse::new();
        exp_cmd_resp.set_res_type(ResultType::Ok);
        exp_cmd_resp.set_value(vec![0u8; 8]);
        exp_resp.set_field_type(MessageType::CmdGet);
        exp_resp.set_cmd_get_resp(exp_cmd_resp);
        assert_eq!(exp_resp, actual_resp);
    }

    #[test]
    // #[should_panic]
    fn test_get_done_error() {
        let actual_resp: Response = Server::cmd_get_done(Err(Other(Box::new("error"))));
        let mut exp_resp: Response = Response::new();
        let mut exp_cmd_resp: CmdGetResponse = CmdGetResponse::new();
        exp_cmd_resp.set_res_type(ResultType::Retryable);
        exp_resp.set_field_type(MessageType::CmdGet);
        exp_resp.set_cmd_get_resp(exp_cmd_resp);
        assert_eq!(exp_resp, actual_resp);
    }

    #[test]
    fn test_scan_done_empty() {
        let actual_resp: Response = Server::cmd_scan_done(Ok(Vec::new()));
        let mut exp_resp: Response = Response::new();
        let mut exp_cmd_resp: CmdScanResponse = CmdScanResponse::new();
        exp_cmd_resp.set_ok(true);
        exp_resp.set_field_type(MessageType::CmdScan);
        exp_resp.set_cmd_scan_resp(exp_cmd_resp);
        assert_eq!(exp_resp, actual_resp);
    }

    #[test]
    fn test_scan_done_some() {
        let k0: Key = vec![0u8, 0u8];
        let v0: Value = vec![255u8, 255u8];
        let k1: Key = vec![0u8, 1u8];
        let v1: Value = vec![255u8, 254u8];
        let kvs: Vec<ResultStorage<StorageKV>> = vec![Ok((k0.clone(), v0.clone())),
                                                      Ok((k1.clone(), v1.clone()))];
        let actual_resp: Response = Server::cmd_scan_done(Ok(kvs));
        assert_eq!(MessageType::CmdScan, actual_resp.get_field_type());
        let actual_cmd_resp: &CmdScanResponse = actual_resp.get_cmd_scan_resp();
        assert_eq!(true, actual_cmd_resp.get_ok());
        let actual_kvs = actual_cmd_resp.get_results();
        assert_eq!(2, actual_kvs.len());
        assert_eq!(ResultType::Ok, actual_kvs[0].get_res_type());
        assert_eq!(k0, actual_kvs[0].get_key());
        assert_eq!(v0, actual_kvs[0].get_value());
        assert_eq!(ResultType::Ok, actual_kvs[1].get_res_type());
        assert_eq!(k1, actual_kvs[1].get_key());
        assert_eq!(v1, actual_kvs[1].get_value());
    }

    #[test]
    fn test_scan_done_lock() {
        let k0: Key = vec![0u8, 0u8];
        let v0: Value = vec![255u8, 255u8];
        let k1: Key = vec![0u8, 1u8];
        let k1_primary: Key = k0.clone();
        let k1_ts: u64 = 10000;
        let kvs: Vec<ResultStorage<StorageKV>> = vec![Ok((k0.clone(), v0.clone())),
                                                      make_lock_error(k1.clone(),
                                                                      k1_primary.clone(),
                                                                      k1_ts)];
        let actual_resp: Response = Server::cmd_scan_done(Ok(kvs));
        assert_eq!(MessageType::CmdScan, actual_resp.get_field_type());
        let actual_cmd_resp: &CmdScanResponse = actual_resp.get_cmd_scan_resp();
        assert_eq!(true, actual_cmd_resp.get_ok());
        let actual_kvs = actual_cmd_resp.get_results();
        assert_eq!(2, actual_kvs.len());
        assert_eq!(ResultType::Ok, actual_kvs[0].get_res_type());
        assert_eq!(k0, actual_kvs[0].get_key());
        assert_eq!(v0, actual_kvs[0].get_value());
        assert_eq!(ResultType::Locked, actual_kvs[1].get_res_type());
        assert_eq!(k1, actual_kvs[1].get_key());
        assert_eq!(k1_primary, actual_kvs[1].get_primary_lock());
        assert_eq!(k1_ts, actual_kvs[1].get_lock_version());
    }

    #[test]
    fn test_prewrite_done_ok() {
        let actual_resp: Response = Server::cmd_prewrite_done(Ok(Vec::new()));
        assert_eq!(MessageType::CmdPrewrite, actual_resp.get_field_type());
        assert_eq!(true, actual_resp.get_cmd_prewrite_resp().get_ok());
    }

    #[test]
    fn test_prewrite_done_err() {
        let err = Other(Box::new("prewrite error"));
        let actual_resp: Response = Server::cmd_prewrite_done(Err(err));
        assert_eq!(MessageType::CmdPrewrite, actual_resp.get_field_type());
        assert_eq!(false, actual_resp.get_cmd_prewrite_resp().get_ok());
    }

    #[test]
    fn test_commit_done_ok() {
        let actual_resp: Response = Server::cmd_commit_done(Ok(()));
        assert_eq!(MessageType::CmdCommit, actual_resp.get_field_type());
        assert_eq!(true, actual_resp.get_cmd_commit_resp().get_ok());
    }

    #[test]
    fn test_commit_done_err() {
        let err = Other(Box::new("commit error"));
        let actual_resp: Response = Server::cmd_commit_done(Err(err));
        assert_eq!(MessageType::CmdCommit, actual_resp.get_field_type());
        assert_eq!(false, actual_resp.get_cmd_commit_resp().get_ok());
    }

    #[test]
    fn test_cleanup_done_ok() {
        let actual_resp: Response = Server::cmd_cleanup_done(Ok(()));
        assert_eq!(MessageType::CmdCleanup, actual_resp.get_field_type());
        assert_eq!(CmdCleanupResponse_ResultType::Ok,
                   actual_resp.get_cmd_cleanup_resp().get_res_type());
    }

    #[test]
    fn test_cleanup_done_err() {
        let err = Other(Box::new("cleanup error"));
        let actual_resp: Response = Server::cmd_cleanup_done(Err(err));
        assert_eq!(MessageType::CmdCleanup, actual_resp.get_field_type());
        assert_eq!(CmdCleanupResponse_ResultType::Retryable,
                   actual_resp.get_cmd_cleanup_resp().get_res_type());
    }

    fn make_lock_error<T>(key: Key, primary: Key, ts: u64) -> ResultStorage<T> {
        Err(mvcc::Error::KeyIsLocked {
            key: key,
            primary: primary,
            ts: ts,
        })
            .map_err(txn::Error::from)
            .map_err(storage::Error::from)
    }
}
