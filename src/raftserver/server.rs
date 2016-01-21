#![allow(dead_code)]
#![allow(unused_must_use)]

use std::collections::HashMap;

use mio::{Token, Handler, EventLoop, EventSet, PollOpt};
use mio::tcp::TcpListener;

use raftserver::{SERVER_TOKEN, FIRST_CUSTOM_TOKEN, DEFAULT_BASE_TICK_MS};
use raftserver::{Msg, Sender, Result, ConnData, TimerMsg};
use raftserver::conn::Conn;
use raftserver::handler::ServerHandler;

pub struct Server<T: ServerHandler> {
    pub listener: TcpListener,
    pub conns: HashMap<Token, Conn>,
    pub token_counter: usize,
    pub sender: Sender,

    handler: T,
}

impl<T: ServerHandler> Server<T> {
    pub fn new(h: T, l: TcpListener, sender: Sender) -> Server<T> {
        Server {
            handler: h,
            listener: l,
            sender: sender,
            conns: HashMap::new(),
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

    fn handle_error(&mut self, event_loop: &mut EventLoop<Server<T>>, token: Token) {
        let conn = self.conns.remove(&token);
        match conn {
            Some(conn) => {
                // need deregister here?
                event_loop.deregister(&conn.sock);
            }
            None => {
                warn!("missing connection for token {}", token.as_usize());
            }
        }

    }

    fn handle_readeable(&mut self, event_loop: &mut EventLoop<Server<T>>, token: Token) {
        match token {
            SERVER_TOKEN => {
                // let Server(ref mut server) = *self;
                // Accept and drop the socket immediately, this will close
                // the socket and notify the client of the EOF.
                let sock = match self.listener.accept() {
                    Err(e) => {
                        error!("accept error: {:?}", e);
                        return;
                    }
                    Ok(None) => {
                        debug!("listener is not ready, try later");
                        // TODO: check error later.
                        return;
                    }
                    Ok(Some((sock, addr))) => {
                        debug!("accept conn {}", addr);
                        sock
                    }
                };

                let new_token = Token(self.token_counter);
                self.token_counter += 1;

                sock.set_nodelay(true).map_err(|e| {
                    error!("connection set no delay err {}", e);
                });

                let conn = Conn::new(sock, new_token);

                self.conns.insert(new_token, conn);

                event_loop.register(&self.conns[&new_token].sock,
                                    new_token,
                                    EventSet::readable(),
                                    PollOpt::edge() | PollOpt::oneshot())
                          .map_err(|e| {
                              error!("register conn err {:?}", e);
                          });

            }
            token => {
                let msgs;
                match self.conns.get_mut(&token) {
                    None => {
                        warn!("missing conn for token {:?}", token);
                        return;
                    }
                    Some(conn) => msgs = conn.read(event_loop),
                }

                msgs.and_then(|msgs| self.handler.handle_read_data(&self.sender, token, msgs))
                    .and_then(|res| {
                        if res.len() == 0 {
                            return Ok(());
                        }

                        // append to write buffer here, no need using sender to notify.
                        if let Some(conn) = self.conns.get_mut(&token) {
                            for data in res {
                                conn.append_write_buf(data);
                            }
                            try!(conn.reregister_writeable(event_loop));
                        }
                        Ok(())
                    })
                    .map_err(|e| warn!("handle read conn err {:?}", e));
            }

        }
    }

    fn handle_writeable(&mut self, event_loop: &mut EventLoop<Server<T>>, token: Token) {
        match self.conns.get_mut(&token) {
            None => {
                warn!("missing conn for token {:?}", token);
                return;
            }
            Some(conn) => conn.write(event_loop),
        };
    }

    fn handle_writedata(&mut self,
                        event_loop: &mut EventLoop<Server<T>>,
                        token: Token,
                        data: ConnData) {
        if let Some(conn) = self.conns.get_mut(&token) {
            conn.append_write_buf(data);
            conn.reregister_writeable(event_loop);
        }
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
}

impl<T: ServerHandler> Handler for Server<T> {
    type Timeout = Msg;
    type Message = Msg;

    fn ready(&mut self, event_loop: &mut EventLoop<Server<T>>, token: Token, events: EventSet) {
        if events.is_hup() | events.is_error() {
            self.handle_error(event_loop, token);
            return;
        }

        if events.is_readable() {
            self.handle_readeable(event_loop, token);
        }

        if events.is_writable() {
            self.handle_writeable(event_loop, token);
        }
    }

    fn notify(&mut self, event_loop: &mut EventLoop<Server<T>>, msg: Msg) {
        match msg {
            Msg::Quit => event_loop.shutdown(),
            Msg::WriteData{token, data} => self.handle_writedata(event_loop, token, data),
            Msg::Timer{delay, msg} => self.register_timer(event_loop, delay, msg),
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
}
