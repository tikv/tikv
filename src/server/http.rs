// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::boxed::{Box, FnBox};
use std::io::ErrorKind::WouldBlock;
use std::io::{Read, Write};
use std::convert::AsMut;

header! { (XRequestGuid, "X-Request-Guid") => [String] }

use kvproto::msgpb;

use super::{Result, Error};

pub type OnResponseResult = Result<Option<msgpb::Message>>;
pub type OnResponse = Box<FnBox(OnResponseResult) + Send>;

pub struct Body {
    pos: usize,
    data: Vec<u8>,
}

impl Body {
    pub fn read_from<T: Read>(&mut self, r: &mut T) -> Result<()> {
        debug!("try to read body, read pos: {}, total {}",
               self.pos,
               self.data.len());

        if self.pos >= self.data.len() {
            return Ok(());
        }

        match r.read(&mut self.data[self.pos..]) {
            Ok(0) => Err(box_err!("remote has closed the connection")),
            Ok(n) => {
                self.pos += n;
                Ok(())
            }
            Err(e) => {
                if e.kind() == WouldBlock {
                    Ok(())
                } else {
                    Err(Error::Io(e))
                }
            }
        }
    }

    pub fn write_to<T: Write>(&mut self, w: &mut T) -> Result<()> {
        debug!("try to write body, write pos: {}, total {}",
               self.pos,
               self.data.len());

        if self.pos >= self.data.len() {
            return Ok(());
        }

        match w.write(&self.data[self.pos..]) {
            Ok(0) => Err(box_err!("can't write ZERO data")),
            Ok(n) => {
                self.pos += n;
                Ok(())
            }
            Err(e) => {
                if e.kind() == WouldBlock {
                    Ok(())
                } else {
                    Err(Error::Io(e))
                }
            }
        }
    }

    pub fn remaining(&self) -> usize {
        if self.pos >= self.data.len() {
            return 0;
        }

        self.data.len() - self.pos
    }

    pub fn reset(&mut self, size: usize) {
        self.pos = 0;
        self.data.resize(size, 0);
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}

impl Default for Body {
    fn default() -> Body {
        Body {
            pos: 0,
            data: vec![],
        }
    }
}

impl AsMut<Vec<u8>> for Body {
    fn as_mut(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }
}


#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;
    use std::sync::{Arc, RwLock};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    use super::*;
    use super::super::http_client::*;
    use super::super::http_server::*;
    use super::super::http_transport::*;
    use super::super::errors::Error;
    use super::super::Result;
    use super::super::transport::RaftStoreRouter;
    use raftstore::store::Transport;
    use kvproto::raft_serverpb::RaftMessage;
    use raftstore::store::Callback;
    use kvproto::raft_cmdpb::RaftCmdRequest;
    use raftstore::Result as RaftStoreResult;
    use raft::SnapshotStatus;
    use util::HandyRwLock;

    use kvproto::msgpb::{Message, MessageType};

    struct TestServerHandler;

    impl ServerHandler for TestServerHandler {
        fn on_request(&mut self, msg: Message, cb: OnResponse) {
            cb.call_box((Ok(Some(msg)),))
        }
    }

    #[test]
    fn test_http() {
        // use util;
        // util::init_log(util::LogLevelFilter::Debug).unwrap();

        let mut s = Server::new(TestServerHandler);
        let listening = s.http(&"127.0.0.1:0".parse().unwrap()).unwrap();

        let addr = listening.addr;
        let url: Url = format!("http://{}{}", addr, V1_MSG_PATH).parse().unwrap();

        let mut msg = Message::new();
        msg.set_msg_type(MessageType::Raft);

        let c = Client::new().unwrap();
        for _ in 0..2 {
            let (tx, rx) = mpsc::channel();
            c.post_message(url.clone(),
                           msg.clone(),
                           box move |res| {
                               tx.send(res).unwrap();
                           })
             .unwrap();

            let msg1 = rx.recv().unwrap().unwrap().unwrap();
            assert!(msg1.get_msg_type() == MessageType::Raft);
        }


        let msg1 = c.post_message_timeout(url.clone(), msg.clone(), Duration::from_secs(3))
                    .unwrap()
                    .unwrap();
        assert!(msg1.get_msg_type() == MessageType::Raft);

        // Send to invalid url
        let url = format!("http://{}/invalid", addr).parse().unwrap();
        let res = c.post_message_timeout(url, msg.clone(), Duration::from_secs(3));
        // Must invalid HTTP status code error, not 200.
        if let Err(Error::HttpResponse(..)) = res {
            // nothing to do
        } else {
            assert!(false, format!("must invalid http response, but {:?}", res));
        }


        c.close();

        listening.close();
    }

    struct TestStoreAddrPeeker {
        addr: String,
    }

    impl StoreAddrPeeker for TestStoreAddrPeeker {
        fn get_address(&mut self, _: u64) -> Result<&str> {
            Ok(&self.addr)
        }
    }

    struct TestRaftStoreRouter {
        unreachable_cnt: AtomicUsize,
    }

    impl RaftStoreRouter for TestRaftStoreRouter {
        fn send_raft_msg(&self, _: RaftMessage) -> RaftStoreResult<()> {
            Ok(())
        }

        fn send_command(&self, _: RaftCmdRequest, _: Callback) -> RaftStoreResult<()> {
            Ok(())
        }

        fn report_snapshot(&self, _: u64, _: u64, _: SnapshotStatus) {}

        fn report_unreachable(&self, _: u64, _: u64) {
            self.unreachable_cnt.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct TestTransportServerHandler {
        tx: mpsc::Sender<Message>,
    }

    impl ServerHandler for TestTransportServerHandler {
        fn on_request(&mut self, msg: Message, cb: OnResponse) {
            if msg.get_raft().get_region_id() % 2 == 0 {
                cb.call_box((Err(box_err!("test unreachable")),));
            } else {
                cb.call_box((Ok(None),));
            }

            self.tx.send(msg).unwrap();
        }
    }

    #[test]
    fn test_http_transport() {
        let (tx, rx) = mpsc::channel();
        let mut s = Server::new(TestTransportServerHandler { tx: tx });
        let listening = s.http(&"127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listening.addr;

        let peeker = TestStoreAddrPeeker { addr: format!("http://{}", addr) };

        let client = Client::new().unwrap();
        let router = Arc::new(RwLock::new(TestRaftStoreRouter {
            unreachable_cnt: AtomicUsize::new(0),
        }));
        let transport = HttpTransport::new(client.clone(), peeker, router.clone()).unwrap();

        let mut msg = RaftMessage::new();

        let num = 4;
        for i in 0..num {
            msg.set_region_id(i as u64);
            transport.send(msg.clone()).unwrap();
        }

        for i in 0..num {
            let msg = rx.recv().unwrap();
            assert_eq!(msg.get_raft().get_region_id(), i as u64);
        }

        thread::sleep(Duration::from_millis(200));

        client.close();
        listening.close();

        assert_eq!(router.rl().unreachable_cnt.load(Ordering::SeqCst), num / 2);
    }
}
