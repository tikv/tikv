#![allow(dead_code)]
#![allow(unused_must_use)]

use std::collections::HashMap;

use mio::{Token, Handler, EventLoop, EventSet, PollOpt};
use mio::tcp::{TcpListener, TcpStream};

use raftserver::Result;
use super::{SERVER_TOKEN, FIRST_CUSTOM_TOKEN, DEFAULT_BASE_TICK_MS, INVALID_TOKEN};
use super::{Msg, Sender, ConnData, TimerMsg};
use super::conn::Conn;
use super::handler::ServerHandler;

pub struct Server<T: ServerHandler> {
    pub listener: TcpListener,
    pub conns: HashMap<Token, Conn>,
    pub token_counter: usize,
    pub sender: Sender,

    peers: HashMap<String, Token>,
    handler: T,
}

impl<T: ServerHandler> Server<T> {
    pub fn new(h: T, l: TcpListener, sender: Sender) -> Server<T> {
        Server {
            handler: h,
            listener: l,
            sender: sender,
            conns: HashMap::new(),
            peers: HashMap::new(),
            token_counter: FIRST_CUSTOM_TOKEN.as_usize(),
        }
    }

    pub fn register_tick(&mut self, event_loop: &mut EventLoop<Server<T>>) -> Result<()> {
        let token = Msg::Tick;
        // must ok, maybe check error later.
        event_loop.timeout_ms(token, DEFAULT_BASE_TICK_MS);
        Ok(())
    }

    fn register_timer(&mut self,
                      event_loop: &mut EventLoop<Server<T>>,
                      delay: u64,
                      msg: TimerMsg) {
        let token = Msg::Timer {
            delay: delay,
            msg: msg,
        };
        event_loop.timeout_ms(token, delay);
    }

    fn remove_conn(&mut self, event_loop: &mut EventLoop<Server<T>>, token: Token) {
        let conn = self.conns.remove(&token);
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
                    -> Result<(Token)> {
        let new_token = Token(self.token_counter);
        self.token_counter += 1;

        try!(sock.set_nodelay(true));

        try!(event_loop.register(&sock,
                                 new_token,
                                 EventSet::readable() | EventSet::hup(),
                                 PollOpt::edge()));

        let conn = Conn::new(sock, new_token, peer_addr);

        self.conns.insert(new_token, conn);

        Ok(new_token)
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
                let msgs = match self.conns.get_mut(&token) {
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

                        self.handler.handle_read_data(&self.sender, token, msgs)
                    })
                    .and_then(|res| {
                        if res.is_empty() {
                            return Ok(());
                        }

                        // append to write buffer here, no need using sender to notify.
                        if let Some(conn) = self.conns.get_mut(&token) {
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
        let res = match self.conns.get_mut(&token) {
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
        let res = match self.conns.get_mut(&token) {
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

    fn handle_tick(&mut self, event_loop: &mut EventLoop<Server<T>>) {
        self.handler
            .handle_tick(&self.sender)
            .map_err(|e| warn!("handle tick err {:?}", e));

        self.register_tick(event_loop);
    }

    fn handle_timer(&mut self, _: &mut EventLoop<Server<T>>, msg: TimerMsg) {
        self.handler
            .handle_timer(&self.sender, msg)
            .map_err(|e| warn!("handle timer err {:?}", e));
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
            Msg::Timer{delay, msg} => self.register_timer(event_loop, delay, msg),
            Msg::SendPeer{addr, data} => self.handle_sendpeer(event_loop, addr, data),
            _ => panic!("unexpected msg"),
        }
    }

    fn timeout(&mut self, event_loop: &mut EventLoop<Server<T>>, msg: Msg) {
        match msg {
            Msg::Tick => self.handle_tick(event_loop),
            Msg::Timer{msg, ..} => self.handle_timer(event_loop, msg),
            _ => panic!("unexpected msg"),
        }
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
