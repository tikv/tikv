use std::collections::HashMap;
use std::result::Result;
use std::boxed::Box;
use std::error::Error;
use std::rc::Rc;
use std::io::{Read, Write};

use mio::{self, Token, EventLoop, EventSet, PollOpt, TryWrite, TryRead};
use mio::tcp::{TcpListener, TcpStream};
use protobuf;
use bytes::{MutBuf, ByteBuf, MutByteBuf};

use raft::raftpb::Message;
use util::codec::{self, encode_msg, decode_msg};

const SERVER_TOKEN: Token = Token(0);

pub trait Dispatcher {
    fn Dispatch<M: protobuf::Message>(&self, m: M) -> Result<M, Box<Error + Send + Sync>>;
}

struct Client<D: Dispatcher> {
    sock: TcpStream,
    interest: EventSet,
    d: Rc<D>,

    res: MutByteBuf,
}

impl<D: Dispatcher> Client<D> {
    fn new(sock: TcpStream, d: Rc<D>) -> Client<D> {
        Client {
            sock: sock,
            interest: EventSet::readable(),
            d: d,
            res: ByteBuf::mut_with_capacity(0),
        }
    }

    fn write(&mut self) {
        self.sock.write(self.res.bytes()).unwrap();
        self.interest.remove(EventSet::writable());
        self.interest.insert(EventSet::readable());
    }

    fn read(&mut self) {
        // only test here
        let mut m = Message::new();
        let msg_id = decode_msg(&mut self.sock, &mut m).unwrap();

        let res = self.d.Dispatch(m).unwrap();
        self.res.clear();
        encode_msg(&mut self.res, msg_id, &res).unwrap();

        self.interest.remove(EventSet::readable());
        self.interest.insert(EventSet::writable());
    }
}



struct Server<D: Dispatcher> {
    listener: TcpListener,
    clients: HashMap<Token, Client<D>>,
    token_counter: usize,
    d: Rc<D>,
}

impl<D: Dispatcher> mio::Handler for Server<D> {
    type Timeout = ();
    type Message = ();

    fn ready(&mut self, event_loop: &mut EventLoop<Server<D>>, token: Token, events: EventSet) {
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
                    let d = self.d.clone();
                    self.clients.insert(new_token, Client::new(sock, d));
                    self.token_counter += 1;

                    event_loop.register(&self.clients[&new_token].sock,
                                        new_token,
                                        EventSet::readable(),
                                        PollOpt::edge() | PollOpt::oneshot())
                              .unwrap();

                }
                token => {
                    let mut client = self.clients.get_mut(&token).unwrap();
                    client.read();
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
}

fn run<D: Dispatcher>(addr: &str, d: D) {
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
        d: Rc::new(d),
    };

    event_loop.run(&mut server).unwrap();
}
