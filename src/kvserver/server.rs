use std::collections::HashMap;
use std::boxed::Box;
use std::io::{Read, Write};
use std::thread;

use mio::{self, Token, EventLoop, EventSet, PollOpt};
use mio::tcp::{TcpListener, TcpStream};
use protobuf::core::Message;
use protobuf::RepeatedField;

use proto::kvrpcpb;
use proto::kvrpcpb::{CmdGetRequest, CmdGetResponse, CmdScanRequest, CmdScanResponse,
                     CmdPrewriteRequest, CmdPrewriteResponse, CmdCommitRequest, CmdCommitResponse,
                     Request, Response, MessageType};
use util::codec::rpc::{encode_msg, decode_msg, MSG_HEADER_LEN};
use storage;
use storage::{Key, Storage, Value, KvPair};

use util;

const SERVER_TOKEN: Token = Token(0);

quick_error! {
    #[derive(Debug)]
    pub enum ServerError {
        //Notify(err: NotifyError<Message>) {
            //from()
            //cause(err)
            //description(err.description())
        //}
        Storage(err: storage::Error) {
            from()
            cause(err)
            description(err.description())
        }
    }
}

pub type Result<T> = ::std::result::Result<T, ServerError>;

pub trait Dispatcher {
    fn dispatch(&mut self, m: Request) -> Result<()>;
}

struct Client {
    sock: TcpStream,
    interest: EventSet,

    res: Vec<u8>,
}

impl Client {
    fn new(sock: TcpStream) -> Client {
        Client {
            sock: sock,
            interest: EventSet::readable(),
            res: Vec::with_capacity(1024),
        }
    }

    fn write(&mut self) {
        if self.sock.write(&self.res).is_err() {
            // [TODO]: add error log
        }
        self.interest.remove(EventSet::writable());
        self.interest.insert(EventSet::readable());
    }

    fn read(&mut self, token: Token, event_loop: &mut EventLoop<Server>) {
        // only test here
        let mut m = Request::new();
        let msg_id = decode_msg(&mut self.sock, &mut m).unwrap();

        let sender = event_loop.channel();
        let queue_msg: QueueMessage = QueueMessage::Request(token, msg_id, m);
        thread::spawn(move || {
            let _ = sender.send(queue_msg);
        });
        self.interest.remove(EventSet::readable());
    }
}

struct Server {
    listener: TcpListener,
    clients: HashMap<Token, Client>,
    token_counter: usize,
    store: Storage,
}

impl Server {
    pub fn handle_get(&mut self,
                      msg: &Request,
                      msg_id: u64,
                      token: Token,
                      event_loop: &mut EventLoop<Server>)
                      -> Result<()> {
        // if !msg.has_cmd_get_req() {
        // // [TODO]: return error
        // }
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
                                       thread::spawn(move || {
                                           let _ = sender.send(queue_msg);
                                       });
                                   })) {
            Ok(()) => {}
            Err(why) => return Err(ServerError::Storage(why)),
        }
        Ok(())
    }
    pub fn handle_scan(&mut self,
                       msg: &Request,
                       msg_id: u64,
                       token: Token,
                       event_loop: &mut EventLoop<Server>)
                       -> Result<()> {
        // if !msg.has_cmd_scan_req() {
        // / [TODO]: return error
        // }
        let cmd_scan_req: &CmdScanRequest = msg.get_cmd_scan_req();
        let sender = event_loop.channel();
        // convert [u8] to Vec[u8]
        let start_key: Key = cmd_scan_req.get_key().iter().cloned().collect();
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
                                  thread::spawn(move || {
                                      let _ = sender.send(queue_msg);
                                  });
                              })) {
            Ok(()) => {}
            Err(why) => return Err(ServerError::Storage(why)),
        }
        Ok(())
    }
    pub fn handle_prewrite(&mut self,
                           msg: &Request,
                           msg_id: u64,
                           token: Token,
                           event_loop: &mut EventLoop<Server>)
                           -> Result<()> {
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
                                      thread::spawn(move || {
                                          let _ = sender.send(queue_msg);
                                      });
                                  })) {
            Ok(()) => {}
            Err(why) => return Err(ServerError::Storage(why)),
        }
        Ok(())
    }
    pub fn handle_commit(&mut self,
                         msg: &Request,
                         msg_id: u64,
                         token: Token,
                         event_loop: &mut EventLoop<Server>)
                         -> Result<()> {
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
                                    thread::spawn(move || {
                                        let _ = sender.send(queue_msg);
                                    });
                                })) {
            Ok(()) => {}
            Err(why) => return Err(ServerError::Storage(why)),
        }
        Ok(())
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

enum QueueMessage {
    // Request(token, msg_id, kvrpc_request)
    Request(Token, u64, Request),
    // Request(token, msg_id, kvrpc_response)
    Response(Token, u64, Response),
}

impl mio::Handler for Server {
    type Timeout = ();
    type Message = QueueMessage;

    fn ready(&mut self, event_loop: &mut EventLoop<Server>, token: Token, events: EventSet) {
        if events.is_hup() || events.is_error() {
            self.clients.remove(&token);
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
                    if sock.set_nodelay(true).is_err() {
                        error!("set nodeplay failed");
                    }
                    self.clients.insert(new_token, Client::new(sock));
                    self.token_counter += 1;

                    event_loop.register(&self.clients[&new_token].sock,
                                        new_token,
                                        EventSet::readable(),
                                        PollOpt::edge() | PollOpt::oneshot())
                              .unwrap();

                }
                token => {
                    let mut client = self.clients.get_mut(&token).unwrap();
                    client.read(token, event_loop);
                    event_loop.reregister(&client.sock,
                                          token,
                                          client.interest,
                                          PollOpt::edge() | PollOpt::oneshot())
                              .unwrap();
                }
            }
        }

        if events.is_writable() {
            let mut client = self.clients.get_mut(&token).unwrap();
            client.write();
            event_loop.reregister(&client.sock,
                                  token,
                                  client.interest,
                                  PollOpt::edge() | PollOpt::oneshot())
                      .unwrap();
        }
    }

    fn notify(&mut self, event_loop: &mut EventLoop<Server>, msg: QueueMessage) {
        match msg {
            QueueMessage::Request(token, msg_id, m) => {
                match m.get_field_type() {
                    MessageType::CmdGet => {
                        if self.handle_get(&m, msg_id, token, event_loop).is_err() {
                            // [TODO]: error log
                        }
                    }
                    MessageType::CmdScan => {
                        if self.handle_scan(&m, msg_id, token, event_loop).is_err() {
                            // [TODO]: error log
                        }
                    }
                    MessageType::CmdPrewrite => {
                        if self.handle_prewrite(&m, msg_id, token, event_loop).is_err() {
                            // [TODO]: error log
                        }
                    }
                    MessageType::CmdCommit => {
                        if self.handle_commit(&m, msg_id, token, event_loop).is_err() {
                            // [TODO]: error log
                        }
                    }
                }
            }
            QueueMessage::Response(token, msg_id, resp) => {
                let mut client: &mut Client = self.clients.get_mut(&token).unwrap();
                client.res.clear();
                let resp_len: usize = MSG_HEADER_LEN + resp.compute_size() as usize;
                if client.res.capacity() < resp_len {
                    client.res = Vec::with_capacity(resp_len);
                }
                encode_msg(&mut client.res, msg_id, &resp).unwrap();
                client.interest.insert(EventSet::writable());
                event_loop.reregister(&client.sock,
                                      token,
                                      client.interest,
                                      PollOpt::edge() | PollOpt::oneshot())
                          .unwrap();
            }
        }
    }
}

pub fn run(addr: &str, store: Storage) {
    let laddr = addr.parse().unwrap();
    let listener = TcpListener::bind(&laddr).unwrap();

    let mut event_loop = EventLoop::new().unwrap();

    event_loop.register(&listener,
                        SERVER_TOKEN,
                        EventSet::readable(),
                        PollOpt::edge())
              .unwrap();

    let mut server = Server {
        listener: listener,
        token_counter: 1,
        clients: HashMap::new(),
        store: store,
    };
    event_loop.run(&mut server).unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::{Storage, Dsn};

    #[test]
    fn test_recv() {
        let store: Storage = Storage::new(Dsn::Memory).unwrap();
        run("127.0.0.1:61234", store);
    }
}
