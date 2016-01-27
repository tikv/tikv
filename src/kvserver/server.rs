use std::collections::HashMap;
use std::boxed::Box;
use std::rc::Rc;
use std::cell::RefCell;
use std::io::{Read, Write};
use std::thread;

use mio::{self, Token, EventLoop, EventSet, PollOpt, TryWrite, TryRead, NotifyError};
use mio::tcp::{TcpListener, TcpStream};
use protobuf;
use protobuf::core::Message;
use protobuf::RepeatedField;
use bytes::{MutBuf, ByteBuf, MutByteBuf};

use proto::kvrpcpb;
use proto::kvrpcpb::{CmdGetRequest, CmdGetResponse, CmdScanRequest, CmdScanResponse,
                     CmdCommitRequest, CmdCommitResponse, Request, Response, MessageType};
use util::codec::{self, encode_msg, decode_msg, MSG_HEADER_LEN};
use storage;
use storage::{Key, Storage, Value, KvPair, Callback};

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
        let mut queueMsg: QueueMessage = QueueMessage::Request(token, msg_id, m);
        thread::spawn(move || {
            let _ = sender.send(queueMsg);
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
                                       let mut queueMsg: QueueMessage =
                                           QueueMessage::Response(token, msg_id, resp);
                                       thread::spawn(move || {
                                           let _ = sender.send(queueMsg);
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
        let start_key = cmd_scan_req.get_key().iter().cloned().collect();
        let sender = event_loop.channel();
        match self.store
                  .async_scan(start_key,
                              cmd_scan_req.get_limit() as usize,
                              cmd_scan_req.get_version(),
                              Box::new(move |kvs: ::std::result::Result<Vec<KvPair>,
                                                                          storage::Error>| {
                                  let resp: Response = Server::cmd_scan_done(kvs);
                                  let mut queueMsg: QueueMessage = QueueMessage::Response(token,
                                                                                          msg_id,
                                                                                          resp);
                                  thread::spawn(move || {
                                      let _ = sender.send(queueMsg);
                                  });
                              })) {
            Ok(()) => {}
            Err(why) => return Err(ServerError::Storage(why)),
        }
        Ok(())
    }
    pub fn handle_commit(&mut self, msg: &Request) -> Result<Response> {
        unimplemented!();
    }
    fn cmd_get_done(r: ::std::result::Result<Option<Value>, storage::Error>) -> Response {
        let mut resp: Response = Response::new();
        let mut cmd_get_resp: CmdGetResponse = CmdGetResponse::new();
        match r {
            Ok(v) => {
                match v {
                    Some(val) => cmd_get_resp.set_value(val),
                    None => cmd_get_resp.set_value(Vec::new()),
                }
                cmd_get_resp.set_ok(true);
            }
            Err(e) => {
                cmd_get_resp.set_ok(false);
            }
        }
        resp.set_field_type(MessageType::CmdGet);
        resp.set_cmd_get_resp(cmd_get_resp);
        resp
    }
    fn cmd_scan_done(kvs: ::std::result::Result<Vec<KvPair>, storage::Error>) -> Response {
        let mut resp: Response = Response::new();
        let mut cmd_scan_resp: CmdScanResponse = CmdScanResponse::new();
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
                cmd_scan_resp.set_ok(true);
            }
            Err(e) => {
                cmd_scan_resp.set_ok(false);
            }
        }
        resp.set_field_type(MessageType::CmdScan);
        resp.set_cmd_scan_resp(cmd_scan_resp);
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
                        Ok(Some((sock, addr))) => sock,
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
                    MessageType::CmdCommit => {
                        if self.handle_commit(&m).is_err() {
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
