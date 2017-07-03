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
use super::{Error, Result, Config};

struct Conn {
    _client: TikvClient,
    stream: UnboundedSender<(RaftMessage, WriteFlags)>,
    _close: Sender<()>,
    active: bool,
}

impl Conn {
    fn new(env: Arc<Environment>, addr: SocketAddr, cfg: &Config) -> Conn {
        info!("server: new connection with tikv endpoint: {}", addr);

        let channel = ChannelBuilder::new(env)
            .stream_initial_window_size(cfg.grpc_stream_initial_window_size)
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
            active: false,
        }
    }
}

/// `RaftClient` is used for sending raft messages to other stores.
pub struct RaftClient {
    env: Arc<Environment>,
    conns: HashMap<(SocketAddr, usize), Conn>,
    pub addrs: HashMap<u64, SocketAddr>,
    cfg: Config,
}

impl RaftClient {
    pub fn new(env: Arc<Environment>, cfg: Config) -> RaftClient {
        RaftClient {
            env: env,
            conns: HashMap::default(),
            addrs: HashMap::default(),
            cfg: cfg,
        }
    }

    fn get_conn(&mut self, addr: SocketAddr, index: usize) -> &mut Conn {
        let env = self.env.clone();
        let cfg = self.cfg.clone();
        self.conns
            .entry((addr, index))
            .or_insert_with(|| Conn::new(env, addr, &cfg))
    }

    pub fn send(&mut self, store_id: u64, addr: SocketAddr, msg: RaftMessage) -> Result<()> {
        let index = msg.get_region_id() as usize % self.cfg.grpc_raft_conn_num;
        let res = {
            let conn = self.get_conn(addr, index);
            UnboundedSender::send(&conn.stream, (msg, WriteFlags::default()))
        };
        if let Err(e) = res {
            warn!("server: drop conn with tikv endpoint {} error: {:?}",
                  addr,
                  e);
            self.conns.remove(&(addr, index));
            self.addrs.remove(&store_id);
            return Err(box_err!(e));
        }
        Ok(())
    }


    pub fn flush(&mut self) {
        for conn in self.conns.values_mut() {
            if conn.active {
                conn.active = false;
                if let Err(e) = UnboundedSender::send(&conn.stream,
                                                      (RaftMessage::new(), WriteFlags::default())) {
                    error!("flush conn error {:?}", e);
                }
            }
        }
    }
}

impl Drop for RaftClient {
    fn drop(&mut self) {
        // Drop conns here to make sure all streams are dropped before Environment.
        self.conns.clear();
    }
}
