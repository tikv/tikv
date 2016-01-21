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
    // Create a runner with listening address.
    fn new(addr: &str) -> Result<(Runner<T>)> {
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
        try!(server.register_tick(&mut self.event_loop));
        try!(self.event_loop.run(&mut server));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(unused_imports)]
    #![allow(unused_variables)]
    #![allow(dead_code)]

    use std::default::Default;
    use std::thread;
    use std::sync::{self, Arc, Mutex};
    use std::time::Duration;
    use std::net::TcpStream;
    use std::io::{Read, Write};
    use std::vec::Vec;
    use std::string::String;

    use bytes::{Buf, ByteBuf};
    use mio::{EventLoop, Token};

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
        let mut r = Runner::new(addr).unwrap();

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
        let mut r = Runner::new(addr).unwrap();

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

    struct TickHandler {
        n: Arc<Mutex<u64>>,
    }

    impl ServerHandler for TickHandler {
        fn handle_tick(&mut self, sender: &Sender) -> Result<()> {
            let mut v = self.n.lock().unwrap();
            *v += 1;
            Ok(())
        }
    }

    #[test]
    fn test_tick() {
        let addr = "127.0.0.1:0";
        let mut r = Runner::new(addr).unwrap();

        let sender = r.get_sender();
        thread::spawn(move || {
            thread::sleep(Duration::new(1, 0));
            sender.kill().unwrap();
        });

        let n = Arc::new(Mutex::new(1));
        let h = TickHandler { n: n.clone() };
        r.run(h).unwrap();

        let n = n.lock().unwrap();
        assert!(*n > 1);
    }

    struct TimerHandler {
        n: Arc<Mutex<u64>>,
    }

    impl ServerHandler for TimerHandler {
        fn handle_timer(&mut self, sender: &Sender, msg: TimerMsg) -> Result<()> {
            let mut v = self.n.lock().unwrap();
            *v = 0;
            Ok(())
        }
    }

    #[test]
    fn test_timer() {
        let addr = "127.0.0.1:0";
        let mut r = Runner::new(addr).unwrap();

        let sender = r.get_sender();
        let n = Arc::new(Mutex::new(1));
        let h = TimerHandler { n: n.clone() };
        sender.timeout_ms(100, TimerMsg::None)
              .unwrap();

        thread::spawn(move || {
            thread::sleep(Duration::new(1, 0));
            sender.kill().unwrap();
        });

        r.run(h).unwrap();

        let n = n.lock().unwrap();
        assert_eq!(*n, 0);
    }

    struct PeerHandler;

    impl ServerHandler for PeerHandler {
        fn handle_read_data(&mut self,
                            sender: &Sender,
                            token: Token,
                            msgs: Vec<ConnData>)
                            -> Result<(Vec<ConnData>)> {
            let mut res = vec![];
            for msg in msgs {
                let buf = Buf::bytes(&msg.data);
                if buf == String::from("ping").as_bytes() {
                    res.push(ConnData::from_string(msg.msg_id, "pong"));
                }
            }

            Ok(res)
        }
    }

    fn start_peer(addr: &str) -> (Sender, thread::JoinHandle<(())>) {
        let mut r = Runner::new(addr).unwrap();
        let s = r.get_sender();
        let t = thread::spawn(move || {
            let h = PeerHandler;

            r.run(h).unwrap();

            ()
        });

        (s, t)
    }

    #[test]
    fn test_peer() {
        let addr1 = "127.0.0.1:24680";
        let addr2 = "127.0.0.1:24681";

        let (s1, h1) = start_peer(addr1);
        let (s2, h2) = start_peer(addr2);

        s1.send_peer(addr2.to_string(), ConnData::from_string(1, "ping")).unwrap();
        s2.send_peer(addr1.to_string(), ConnData::from_string(2, "ping")).unwrap();
        s1.send_peer(addr1.to_string(), ConnData::from_string(3, "ping")).unwrap();
        s2.send_peer(addr2.to_string(), ConnData::from_string(4, "ping")).unwrap();

        thread::sleep(Duration::from_secs(1));

        s1.kill().unwrap();
        s2.kill().unwrap();

        h1.join().unwrap();
        h2.join().unwrap();
    }
}
