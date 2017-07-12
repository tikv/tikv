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
use futures::{Future, Sink, Stream, stream};
use grpc::{Environment, ChannelBuilder, WriteFlags};
use kvproto::raft_serverpb::RaftMessage;
use kvproto::tikvpb_grpc::TikvClient;

const MAX_GRPC_RECV_MSG_LEN: usize = 10 * 1024 * 1024;
const MAX_GRPC_SEND_MSG_LEN: usize = 10 * 1024 * 1024;
const INITIAL_BUFFER_CAP: usize = 1024;

use util::collections::HashMap;
use super::{Error, Result, Config};

struct Conn {
    stream: UnboundedSender<Vec<(RaftMessage, WriteFlags)>>,
    buffer: Option<Vec<(RaftMessage, WriteFlags)>>,
    store_id: u64,

    _client: TikvClient,
    _close: Sender<()>,
}

impl Conn {
    fn new(env: Arc<Environment>, addr: SocketAddr, cfg: &Config, store_id: u64) -> Conn {
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
                .send_all(rx.map(|msgs: Vec<(RaftMessage, WriteFlags)>| {
                        stream::iter::<_, _, ()>(msgs.into_iter().map(Ok))
                    })
                    .flatten()
                    .map_err(|_| Error::Sink))
                .map(|_| ())
                .map_err(move |e| warn!("send raftmessage to {} failed: {:?}", addr, e)))
            .map(|_| ())
            .map_err(|_| ()));
        Conn {
            stream: tx,
            buffer: Some(Vec::with_capacity(INITIAL_BUFFER_CAP)),
            store_id: store_id,

            _client: client,
            _close: tx_close,
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

    fn get_conn(&mut self, addr: SocketAddr, region_id: u64, store_id: u64) -> &mut Conn {
        let env = self.env.clone();
        let cfg = self.cfg.clone();
        let index = region_id as usize % cfg.grpc_raft_conn_num;
        self.conns
            .entry((addr, index))
            .or_insert_with(|| Conn::new(env, addr, &cfg, store_id))
    }

    pub fn send(&mut self, store_id: u64, addr: SocketAddr, msg: RaftMessage) -> Result<()> {
        let mut conn = self.get_conn(addr, msg.region_id, store_id);
        conn.buffer.as_mut().unwrap().push((msg, WriteFlags::default().buffer_hint(true)));
        Ok(())
    }


    pub fn flush(&mut self) {
        let mut dead_conns = Vec::new();
        self.conns.retain(|&mut (addr, _), ref mut conn| {
            let msgs = if conn.buffer.as_ref().unwrap().is_empty() {
                // Avoid leaking dead and empty buffer conns.
                vec![(RaftMessage::new(), WriteFlags::default())]
            } else {
                let mut msgs = conn.buffer.take().unwrap();
                msgs.last_mut().unwrap().1 = WriteFlags::default();
                msgs
            };

            if let Err(e) = UnboundedSender::send(&conn.stream, msgs) {
                error!("server: drop conn with tikv endpoint {} flush conn error: {:?}",
                       addr,
                       e);
                dead_conns.push(conn.store_id);
                return false;
            }

            conn.buffer = Some(Vec::with_capacity(INITIAL_BUFFER_CAP));
            true
        });

        for key in dead_conns {
            self.addrs.remove(&key);
        }
    }
}

impl Drop for RaftClient {
    fn drop(&mut self) {
        // Drop conns here to make sure all streams are dropped before Environment.
        self.conns.clear();
    }
}
