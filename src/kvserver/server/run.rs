use std::collections::HashMap;

use mio::{Token, EventLoop, EventSet, PollOpt};
use mio::tcp::TcpListener;

use storage::Storage;
use super::server::{Server, SERVER_TOKEN};
use super::conn::Conn;

pub fn run(addr: &str, store: Storage) {
    let laddr = addr.parse().unwrap();
    let listener = TcpListener::bind(&laddr).unwrap();
    let mut event_loop = EventLoop::new().unwrap();
    event_loop.register(&listener,
                        SERVER_TOKEN,
                        EventSet::readable() | EventSet::hup(),
                        PollOpt::edge())
              .unwrap();

    let conns: HashMap<Token, Conn> = HashMap::new();
    let mut server = Server::new(listener, conns, 1, store);
    event_loop.run(&mut server).unwrap();
}
