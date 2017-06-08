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

use std::sync::Arc;
use std::net::SocketAddr;

use futures::sync::mpsc::{self, UnboundedSender};
use futures::sync::oneshot::{self, Sender};
use futures::{Future, Sink, Stream};
use grpc::{Environment, ChannelBuilder, WriteFlags};
use kvproto::raft_serverpb::RaftMessage;
use kvproto::tikvpb_grpc::TikvClient;

use util::collections::HashMap;
use super::{Error, Result};

struct Conn {
    _client: TikvClient,
    stream: UnboundedSender<(RaftMessage, WriteFlags)>,
    _close: Sender<()>,
}

impl Conn {
    fn new(env: Arc<Environment>, addr: SocketAddr) -> Conn {
        info!("server: new connection with tikv endpoint: {}", addr);

        let channel = ChannelBuilder::new(env).connect(&format!("{}", addr));
        let client = TikvClient::new(channel);
        let (tx, rx) = mpsc::unbounded();
        let (tx_close, rx_close) = oneshot::channel();
        let (sink, _) = client.raft();
        client.spawn(rx_close.map_err(|_| ())
            .select(sink.sink_map_err(Error::from)
                .send_all(rx.map_err(|_| Error::Sink))
                .map(|_| ())
                .map_err(move |e| warn!("send raftmessage to {} failed: {:?}", addr, e)))
            .map(|_| ())
            .map_err(|_| ()));
        Conn {
            _client: client,
            stream: tx,
            _close: tx_close,
        }
    }
}

/// `RaftClient` is used for sending raft messages to other stores.
pub struct RaftClient {
    env: Arc<Environment>,
    conns: HashMap<SocketAddr, Conn>,
    pub addrs: HashMap<u64, SocketAddr>,
}

impl RaftClient {
    pub fn new(env: Arc<Environment>) -> RaftClient {
        RaftClient {
            env: env,
            conns: HashMap::default(),
            addrs: HashMap::default(),
        }
    }

    fn get_conn(&mut self, addr: SocketAddr) -> &Conn {
        let env = self.env.clone();
        self.conns
            .entry(addr)
            .or_insert_with(|| Conn::new(env, addr))
    }

    pub fn send(&mut self, store_id: u64, addr: SocketAddr, msg: RaftMessage) -> Result<()> {
        let res = {
            let conn = self.get_conn(addr);
            UnboundedSender::send(&conn.stream, (msg, WriteFlags::default()))
        };
        if let Err(e) = res {
            warn!("server: drop conn with tikv endpoint {} error: {:?}",
                  addr,
                  e);
            self.addrs.remove(&store_id);
            self.conns.remove(&addr);
            return Err(box_err!(e));
        }
        Ok(())
    }
}

impl Drop for RaftClient {
    fn drop(&mut self) {
        // Drop conns here to make sure all streams are dropped before Environment.
        self.conns.clear();
    }
}
