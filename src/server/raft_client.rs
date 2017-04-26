// Copyright 2017 PingCAP, Inc.
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

use std::fmt;
use std::net::SocketAddr;

use futures::{future, Future, Stream};
use futures::sync::mpsc;
use tokio_core::reactor::Handle;
use grpc::error::GrpcError;
use kvproto::raft_serverpb::RaftMessage;
use kvproto::tikvpb_grpc::{TiKVAsync, TiKVAsyncClient};

use util::worker::FutureRunnable;
use util::{HashMap, TryInsertWith};
use super::Result;

// SendTask delivers a raft message to other stores.
pub struct SendTask {
    pub addr: SocketAddr,
    pub msg: RaftMessage,
}

impl fmt::Display for SendTask {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "send raft message to {:?}", self.addr)
    }
}

struct Conn {
    _client: TiKVAsyncClient,
    stream: mpsc::UnboundedSender<RaftMessage>,
}

impl Conn {
    fn new(addr: SocketAddr, handle: &Handle) -> Result<Conn> {
        let host = format!("{}", addr.ip());
        let client = box_try!(TiKVAsyncClient::new(&*host, addr.port(), false, Default::default()));
        let (tx, rx) = mpsc::unbounded();
        handle.spawn(client.Raft(box rx.map_err(|_| GrpcError::Other("canceled")))
            .then(|_| future::ok(())));
        Ok(Conn {
            _client: client,
            stream: tx,
        })
    }
}

// SendRunner is used for sending raft messages to other stores.
pub struct SendRunner {
    conns: HashMap<SocketAddr, Conn>,
}

impl Default for SendRunner {
    fn default() -> SendRunner {
        SendRunner { conns: HashMap::default() }
    }
}

impl SendRunner {
    fn get_conn(&mut self, addr: SocketAddr, handle: &Handle) -> Result<&Conn> {
        let conn = try!(self.conns.entry(addr).or_try_insert_with(|| Conn::new(addr, handle)));
        Ok(conn)
    }

    fn send(&mut self, t: SendTask, handle: &Handle) -> Result<()> {
        let conn = try!(self.get_conn(t.addr, handle));
        box_try!(conn.stream.send(t.msg));
        Ok(())
    }
}

impl FutureRunnable<SendTask> for SendRunner {
    fn run(&mut self, t: SendTask, handle: &Handle) {
        let addr = t.addr;
        if let Err(e) = self.send(t, handle) {
            error!("send raft message error: {:?}", e);
            self.conns.remove(&addr);
        }
    }
}
