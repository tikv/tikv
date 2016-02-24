#[cfg(test)]
mod tests {
    // TODO: remove whole tests later.
    // Following tests are just for studying mio, now we will
    // implement our own raft server logic, so they are no need.
    // Later we will add another tests for server.

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

    use super::super::*;
    use raftserver::Result;
    use util::codec::rpc;

    fn new_server<T: ServerHandler>(addr: &str, h: T) -> Server {
        let cfg = Config { addr: addr.to_owned(), ..Default::default() };

        Server::new(addr, h).unwrap()
    }

    struct BaseHandler;

    impl ServerHandler for BaseHandler {}

    #[test]
    fn test_base() {
        let addr = "127.0.0.1:0";
        let h = BaseHandler;
        let mut r = new_server(addr, h);

        let sender = r.get_sendch();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(500));
            sender.kill().unwrap();
        });

        r.run().unwrap();
    }

    #[test]
    fn test_conn() {
        let addr = "127.0.0.1:12345";
        let h = BaseHandler;
        let mut r = new_server(addr, h);

        let sender = r.get_sendch();

        thread::spawn(move || {
            r.run().unwrap();
        });

        thread::sleep(Duration::from_millis(500));
        let mut conn = TcpStream::connect("127.0.0.1:12345").unwrap();

        for i in 1..10 {
            let mut data = vec![];
            rpc::encode_data(&mut data, i as u64, b"hello world").unwrap();
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

    struct TimerHandler {
        n: Arc<Mutex<u64>>,
    }

    impl ServerHandler for TimerHandler {
        fn handle_timer(&mut self, sender: &SendCh, msg: TimerMsg) -> Result<()> {
            let mut v = self.n.lock().unwrap();
            *v = 0;
            Ok(())
        }
    }

    #[test]
    fn test_timer() {
        let addr = "127.0.0.1:0";
        let n = Arc::new(Mutex::new(1));
        let h = TimerHandler { n: n.clone() };
        let mut r = new_server(addr, h);

        let sender = r.get_sendch();
        sender.timeout_ms(100, TimerMsg::None)
              .unwrap();

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(500));
            sender.kill().unwrap();
        });

        r.run().unwrap();

        let n = n.lock().unwrap();
        assert_eq!(*n, 0);
    }

    struct PeerHandler {
        cnt: Arc<Mutex<(u64, u64)>>,
    }

    impl ServerHandler for PeerHandler {
        fn handle_read_data(&mut self,
                            sender: &SendCh,
                            token: Token,
                            msgs: Vec<ConnData>)
                            -> Result<(Vec<ConnData>)> {
            let mut res = vec![];
            for msg in msgs {
                let mut cnt = self.cnt.lock().unwrap();
                let buf = Buf::bytes(&msg.data);
                if buf == String::from("ping").as_bytes() {
                    (*cnt).0 += 1;
                    res.push(ConnData::from_string(msg.msg_id, "pong"));
                } else {
                    (*cnt).1 += 1;
                }
            }

            Ok(res)
        }
    }

    fn start_peer(addr: &str, cnt: Arc<Mutex<(u64, u64)>>) -> (SendCh, thread::JoinHandle<(())>) {
        let h = PeerHandler { cnt: cnt };
        let mut r = new_server(addr, h);
        let s = r.get_sendch();
        let t = thread::spawn(move || {
            r.run().unwrap();

            ()
        });

        (s, t)
    }

    #[test]
    fn test_peer() {
        let addr1 = "127.0.0.1:24680";
        let addr2 = "127.0.0.1:24681";

        let cnt1 = Arc::new(Mutex::new((0, 0)));
        let cnt2 = Arc::new(Mutex::new((0, 0)));

        let (s1, h1) = start_peer(addr1, cnt1.clone());
        let (s2, h2) = start_peer(addr2, cnt2.clone());

        s1.send_peer(addr2.to_owned(), ConnData::from_string(1, "ping")).unwrap();
        s2.send_peer(addr1.to_owned(), ConnData::from_string(2, "ping")).unwrap();
        s1.send_peer(addr1.to_owned(), ConnData::from_string(3, "ping")).unwrap();
        s2.send_peer(addr2.to_owned(), ConnData::from_string(4, "ping")).unwrap();

        thread::sleep(Duration::from_secs(1));

        s1.kill().unwrap();
        s2.kill().unwrap();

        h1.join().unwrap();
        h2.join().unwrap();

        let r1 = *(cnt1.lock().unwrap());
        let r2 = *(cnt2.lock().unwrap());

        assert_eq!(r1, r2);
        assert_eq!(r1, (2, 2));
    }

    struct QuitHandler {
        n: Arc<Mutex<u64>>,
    }

    impl ServerHandler for QuitHandler {
        fn handle_quit(&mut self) {
            let mut v = self.n.lock().unwrap();
            *v = 0;
        }
    }

    #[test]
    fn test_quit() {
        let addr = "127.0.0.1:0";
        let n = Arc::new(Mutex::new(1));
        let h = QuitHandler { n: n.clone() };
        let mut r = new_server(addr, h);

        let sender = r.get_sendch();

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(500));
            sender.kill().unwrap();
        });

        r.run().unwrap();

        let n = n.lock().unwrap();
        assert_eq!(*n, 0);
    }
}
