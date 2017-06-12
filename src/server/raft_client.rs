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

const MAX_GRPC_RECV_MSG_LEN: usize = 10 * 1024 * 1024;
const MAX_GRPC_SEND_MSG_LEN: usize = 10 * 1024 * 1024;

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

        let channel = ChannelBuilder::new(env)
            .max_receive_message_len(MAX_GRPC_RECV_MSG_LEN)
            .max_send_message_len(MAX_GRPC_SEND_MSG_LEN)
            .connect(&format!("{}", addr));
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
    conns: HashMap<(SocketAddr, usize), Conn>,
    conn_size: usize,
    conn_index: usize,
}

impl RaftClient {
    pub fn new(env: Arc<Environment>, conn_size: usize) -> RaftClient {
        RaftClient {
            env: env,
            conns: HashMap::default(),
            conn_size: conn_size,
            conn_index: 0,
        }
    }

    fn get_conn(&mut self, addr: SocketAddr, index: usize) -> &Conn {
        let env = self.env.clone();
        self.conns
            .entry((addr, index))
            .or_insert_with(|| Conn::new(env, addr))
    }

    pub fn send(&mut self, addr: SocketAddr, msg: RaftMessage) -> Result<()> {
        self.conn_index = (self.conn_index + 1) % self.conn_size;
        let index = self.conn_index;
        let res = {
            let conn = self.get_conn(addr, index);
            UnboundedSender::send(&conn.stream, (msg, WriteFlags::default()))
        };
        if let Err(e) = res {
            warn!("server: drop conn with tikv endpoint {} error: {:?}",
                  addr,
                  e);
            self.conns.remove(&(addr, index));
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
