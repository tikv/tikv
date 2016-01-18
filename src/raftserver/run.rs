#![allow(dead_code)]

use std::option::Option;

use mio::{EventLoop, EventSet, PollOpt};
use mio::tcp::TcpListener;

use raftserver::{Result, Sender, SERVER_TOKEN};
use raftserver::server::Server;
use raftserver::handler::ServerHandler;

pub struct Runner<T: ServerHandler> {
    sender: Sender,
    listener: Option<TcpListener>,
    event_loop: EventLoop<Server<T>>,
}

impl<T: ServerHandler> Runner<T> {
    // Starts a new runner with listening address.
    fn start(addr: &str) -> Result<(Runner<T>)> {
        let addr = try!(addr.parse());
        let listener = try!(TcpListener::bind(&addr));

        // create a event loop;
        let mut event_loop = try!(EventLoop::new());
        try!(event_loop.register(&listener,
                                 SERVER_TOKEN,
                                 EventSet::readable(),
                                 PollOpt::edge() | PollOpt::oneshot()));

        let sender = Sender::new(event_loop.channel());
        Ok(Runner {
            sender: sender,
            event_loop: event_loop,
            listener: Some(listener),
        })
    }

    fn get_sender(&self) -> Sender {
        self.sender.clone()
    }

    fn run(&mut self, h: T) -> Result<()> {
        let mut server = Server::new(h, self.listener.take().unwrap(), self.sender.clone());
        try!(self.event_loop.run(&mut server));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(unused_imports)]

    use std::default::Default;
    use std::thread;
    use std::sync;
    use std::time::Duration;
    use std::net::TcpStream;
    use std::io::prelude::*;

    use super::*;
    use raftserver::*;
    use util::codec;
    use raftserver::handler::ServerHandler;
    use raftserver::server::Server;

    struct BaseHandler;

    impl ServerHandler for BaseHandler {}

    #[test]
    fn test_base() {
        let addr = "127.0.0.1:0";
        let mut r = Runner::start(addr).unwrap();

        let sender = r.get_sender();
        thread::spawn(move || {
            thread::sleep(Duration::new(1, 0));
            sender.kill().unwrap();
        });

        let h = BaseHandler;
        r.run(h).unwrap();
    }

    #[test]
    fn test_conn() {
        let addr = "127.0.0.1:12345";
        let mut r = Runner::start(addr).unwrap();

        let sender = r.get_sender();

        thread::spawn(move || {
            let h = BaseHandler;
            r.run(h).unwrap();
        });

        thread::sleep(Duration::new(0, 500 * 1000000));
        let mut conn = TcpStream::connect("127.0.0.1:12345").unwrap();

        for i in 1..10 {
            let mut data = vec![];
            codec::encode_data(&mut data, i as u64, "hello world".as_bytes()).unwrap();
            for i in data.clone() {
                conn.write(&[i; 1]).unwrap();
                conn.flush().unwrap();
            }

            let mut buf = vec![0;data.len()];
            conn.read(&mut buf).unwrap();

            assert_eq!(data, buf);
        }

        sender.kill().unwrap();
    }
}
