#![allow(dead_code)]
#![allow(unused_must_use)]

use std::collections::HashMap;
use std::option::Option;

use mio::{Token, Handler, EventLoop, EventSet, PollOpt};
use mio::tcp::{TcpListener, TcpStream};
use mio::util::Slab;

use raftserver::{Result, other};
use super::{Msg, SendCh, ConnData};
use super::conn::Conn;
use super::handler::ServerHandler;
use super::config::Config;

const SERVER_TOKEN: Token = Token(1);
const FIRST_CUSTOM_TOKEN: Token = Token(1024);
const INVALID_TOKEN: Token = Token(0);

pub struct Server<T: ServerHandler> {
    cfg: Config,

    listener: TcpListener,
    conns: Slab<Conn>,
    sendch: SendCh,

    peers: HashMap<String, Token>,
    handler: T,

    event_loop: Option<EventLoop<Server<T>>>,
}

impl<T: ServerHandler> Server<T> {
    pub fn new(cfg: Config, h: T) -> Result<Server<T>> {
        let addr = try!((&cfg.addr).parse());
        let listener = try!(TcpListener::bind(&addr));

        // create a event loop;
        let mut event_loop = try!(EventLoop::new());
        try!(event_loop.register(&listener,
                                 SERVER_TOKEN,
                                 EventSet::readable(),
                                 PollOpt::edge()));

        let sendch = SendCh::new(event_loop.channel());

        let max_conn_capacity = cfg.max_conn_capacity;

        Ok(Server {
            cfg: cfg,
            handler: h,
            listener: listener,
            sendch: sendch,
            conns: Slab::new_starting_at(FIRST_CUSTOM_TOKEN, max_conn_capacity),
            peers: HashMap::new(),
            event_loop: Some(event_loop),
        })
    }

    pub fn run(&mut self) -> Result<()> {
        let mut event_loop = self.event_loop.take().unwrap();
        try!(event_loop.run(self));
        Ok(())
    }

    pub fn get_sendch(&self) -> SendCh {
        self.sendch.clone()
    }

    fn remove_conn(&mut self, event_loop: &mut EventLoop<Server<T>>, token: Token) {
        let conn = self.conns.remove(token);
        match conn {
            Some(conn) => {
                // if connected to remote peer, remove this too.
                if let Some(addr) = conn.peer_addr {
                    self.peers.remove(&addr);
                }

                event_loop.deregister(&conn.sock);
            }
            None => {
                warn!("missing connection for token {}", token.as_usize());
            }
        }
    }

    fn add_new_conn(&mut self,
                    event_loop: &mut EventLoop<Server<T>>,
                    sock: TcpStream,
                    peer_addr: Option<String>)
                    -> Result<Token> {
        try!(sock.set_nodelay(true));

        // TODO: now slab crate doesn't support insert_with_opt, we should use it instead later.
        self.conns
            .insert_with(|new_token: Token| -> Conn {
                // TODO: if insert_with_opt used, we will return None for register error.
                // Now, just panic for this.
                event_loop.register(&sock,
                                    new_token,
                                    EventSet::readable() | EventSet::hup(),
                                    PollOpt::edge())
                          .unwrap();

                Conn::new(sock, new_token, peer_addr)
            })
            .ok_or_else(|| other("add new connection failed"))
    }

    fn handle_readeable(&mut self, event_loop: &mut EventLoop<Server<T>>, token: Token) {
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

                    self.add_new_conn(event_loop, sock, None)
                        .map_err(|e| {
                            error!("register conn err {:?}", e);
                        });
                }
            }
            token => {
                let msgs = match self.conns.get_mut(token) {
                    None => {
                        warn!("missing conn for token {:?}", token);
                        return;
                    }
                    Some(conn) => conn.read(event_loop),
                };

                msgs.and_then(|msgs| {
                        if msgs.is_empty() {
                            return Ok(msgs);
                        }

                        self.handler.handle_read_data(&self.sendch, token, msgs)
                    })
                    .and_then(|res| {
                        if res.is_empty() {
                            return Ok(());
                        }

                        // append to write buffer here, no need using sender to notify.
                        if let Some(conn) = self.conns.get_mut(token) {
                            for data in res {
                                try!(conn.append_write_buf(event_loop, data));
                            }
                        }
                        Ok(())
                    })
                    .map_err(|e| {
                        warn!("handle read conn err {:?}, remove", e);
                        self.remove_conn(event_loop, token);
                    });
            }

        }
    }

    fn handle_writable(&mut self, event_loop: &mut EventLoop<Server<T>>, token: Token) {
        let res = match self.conns.get_mut(token) {
            None => {
                warn!("missing conn for token {:?}", token);
                return;
            }
            Some(conn) => conn.write(event_loop),
        };

        res.map_err(|e| {
            warn!("handle write conn err {:?}, remove", e);
            self.remove_conn(event_loop, token);
        });
    }

    fn handle_writedata(&mut self,
                        event_loop: &mut EventLoop<Server<T>>,
                        token: Token,
                        data: ConnData) {
        let res = match self.conns.get_mut(token) {
            None => {
                warn!("missing conn for token {:?}", token);
                return;
            }
            Some(conn) => conn.append_write_buf(event_loop, data),
        };

        res.map_err(|e| {
            warn!("handle write data err {:?}, remove", e);
            self.remove_conn(event_loop, token);
        });
    }

    fn connect_peer(&mut self, event_loop: &mut EventLoop<Server<T>>, addr: &str) -> Result<Token> {
        let peer_addr = try!(addr.parse());
        let sock = try!(TcpStream::connect(&peer_addr));
        let token = try!(self.add_new_conn(event_loop, sock, Some(addr.to_string())));
        self.peers.insert(addr.to_owned(), token);
        Ok(token)
    }

    fn handle_sendpeer(&mut self,
                       event_loop: &mut EventLoop<Server<T>>,
                       addr: String,
                       data: ConnData) {
        // check the corresponding token for peer address.
        let mut token = self.peers.get(&addr).map_or(INVALID_TOKEN, |t| *t);

        if token == INVALID_TOKEN {
            match self.connect_peer(event_loop, &addr) {
                Err(e) => {
                    error!("connect {:?} err {:?}", addr, e);
                    return;
                }
                Ok(new_token) => token = new_token,
            }
        }

        self.handle_writedata(event_loop, token, data);
    }
}

impl<T: ServerHandler> Handler for Server<T> {
    type Timeout = Msg;
    type Message = Msg;

    fn ready(&mut self, event_loop: &mut EventLoop<Server<T>>, token: Token, events: EventSet) {
        if events.is_hup() || events.is_error() {
            self.remove_conn(event_loop, token);
            return;
        }

        if events.is_readable() {
            self.handle_readeable(event_loop, token);
        }

        if events.is_writable() {
            self.handle_writable(event_loop, token);
        }
    }

    fn notify(&mut self, event_loop: &mut EventLoop<Server<T>>, msg: Msg) {
        match msg {
            Msg::Quit => event_loop.shutdown(),
            Msg::WriteData{token, data} => self.handle_writedata(event_loop, token, data),
            Msg::SendPeer{addr, data} => self.handle_sendpeer(event_loop, addr, data),
            _ => panic!("unexpected msg"),
        }
    }

    fn timeout(&mut self, _: &mut EventLoop<Server<T>>, _: Msg) {
        // nothing to do now.
    }

    fn interrupted(&mut self, event_loop: &mut EventLoop<Server<T>>) {
        event_loop.shutdown();
    }

    fn tick(&mut self, event_loop: &mut EventLoop<Server<T>>) {
        if event_loop.is_running() {
            return;
        }

        // tick is called in the end of the loop, so if we notify to quit,
        // we will quit the server here.
        info!("begin to quit server......");
        self.handler.handle_quit();
        info!("quit server over");
    }
}
