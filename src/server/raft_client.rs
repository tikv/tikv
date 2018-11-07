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

use std::ffi::CString;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;

use futures::sync::mpsc::{self, UnboundedSender};
use futures::sync::oneshot::{self, Sender};
use futures::{stream, Future, Sink, Stream};
use grpc::{ChannelBuilder, Environment, WriteFlags};
use kvproto::raft_serverpb::RaftMessage;
use kvproto::tikvpb_grpc::TikvClient;

use super::metrics::*;
use super::{Config, Error, Result};
use util::collections::HashMap;
use util::security::SecurityManager;

const MAX_GRPC_RECV_MSG_LEN: i32 = 10 * 1024 * 1024;
const MAX_GRPC_SEND_MSG_LEN: i32 = 10 * 1024 * 1024;
const PRESERVED_MSG_BUFFER_COUNT: usize = 1024;

static CONN_ID: AtomicI32 = AtomicI32::new(0);

struct Conn {
    stream: UnboundedSender<Vec<(RaftMessage, WriteFlags)>>,
    buffer: Option<Vec<(RaftMessage, WriteFlags)>>,
    store_id: u64,
    alive: Arc<AtomicBool>,

    _client: TikvClient,
    _close: Sender<()>,
}

impl Conn {
    fn new(
        env: Arc<Environment>,
        addr: &str,
        cfg: &Config,
        security_mgr: &SecurityManager,
        store_id: u64,
    ) -> Conn {
        info!("server: new connection with tikv endpoint: {}", addr);

        let alive = Arc::new(AtomicBool::new(true));
        let alive1 = Arc::clone(&alive);
        let cb = ChannelBuilder::new(env)
            .stream_initial_window_size(cfg.grpc_stream_initial_window_size.0 as i32)
            .max_receive_message_len(MAX_GRPC_RECV_MSG_LEN)
            .max_send_message_len(MAX_GRPC_SEND_MSG_LEN)
            .keepalive_time(cfg.grpc_keepalive_time.0)
            .keepalive_timeout(cfg.grpc_keepalive_timeout.0)
            .default_compression_algorithm(cfg.grpc_compression_algorithm())
            // hack: so it's different args, grpc will always create a new connection.
            .raw_cfg_int(
                CString::new("random id").unwrap(),
                CONN_ID.fetch_add(1, Ordering::SeqCst),
            );
        let channel = security_mgr.connect(cb, addr);
        let client = TikvClient::new(channel);
        let (tx, rx) = mpsc::unbounded();
        let (tx_close, rx_close) = oneshot::channel();
        let (sink, _) = client.raft().unwrap();
        let addr = addr.to_owned();
        client.spawn(
            rx_close
                .map_err(|_| ())
                .select(
                    sink.sink_map_err(Error::from)
                        .send_all(rx.map(stream::iter_ok).flatten().map_err(|()| Error::Sink))
                        .then(move |r| {
                            alive.store(false, Ordering::SeqCst);
                            r
                        })
                        .map(|_| ())
                        .map_err(move |e| {
                            let store = store_id.to_string();
                            REPORT_FAILURE_MSG_COUNTER
                                .with_label_values(&["unreachable", &*store])
                                .inc();
                            warn!("send raftmessage to {} failed: {:?}", addr, e);
                        }),
                )
                .map(|_| ())
                .map_err(|_| ()),
        );
        Conn {
            stream: tx,
            buffer: Some(Vec::with_capacity(PRESERVED_MSG_BUFFER_COUNT)),
            store_id,
            alive: alive1,

            _client: client,
            _close: tx_close,
        }
    }
}

/// `RaftClient` is used for sending raft messages to other stores.
pub struct RaftClient {
    env: Arc<Environment>,
    conns: HashMap<(String, usize), Conn>,
    pub addrs: HashMap<u64, String>,
    cfg: Arc<Config>,
    security_mgr: Arc<SecurityManager>,
}

impl RaftClient {
    pub fn new(
        env: Arc<Environment>,
        cfg: Arc<Config>,
        security_mgr: Arc<SecurityManager>,
    ) -> RaftClient {
        RaftClient {
            env,
            conns: HashMap::default(),
            addrs: HashMap::default(),
            cfg,
            security_mgr,
        }
    }

    fn get_conn(&mut self, addr: &str, region_id: u64, store_id: u64) -> &mut Conn {
        let index = region_id as usize % self.cfg.grpc_raft_conn_num;
        let cfg = &self.cfg;
        let security_mgr = &self.security_mgr;
        let env = &self.env;
        // TODO: avoid to_owned
        self.conns
            .entry((addr.to_owned(), index))
            .or_insert_with(|| Conn::new(Arc::clone(env), addr, cfg, security_mgr, store_id))
    }

    pub fn send(&mut self, store_id: u64, addr: &str, msg: RaftMessage) -> Result<()> {
        let conn = self.get_conn(addr, msg.region_id, store_id);
        conn.buffer
            .as_mut()
            .unwrap()
            .push((msg, WriteFlags::default().buffer_hint(true)));
        Ok(())
    }

    pub fn flush(&mut self) {
        let addrs = &mut self.addrs;
        let mut counter: u64 = 0;
        self.conns.retain(|&(ref addr, _), conn| {
            let store_id = conn.store_id;
            if !conn.alive.load(Ordering::SeqCst) {
                if let Some(addr_current) = addrs.remove(&store_id) {
                    if addr_current != *addr {
                        addrs.insert(store_id, addr_current);
                    }
                }
                return false;
            }

            if conn.buffer.as_ref().unwrap().is_empty() {
                return true;
            }

            counter += 1;
            let mut msgs = conn.buffer.take().unwrap();
            msgs.last_mut().unwrap().1 = WriteFlags::default();
            if let Err(e) = conn.stream.unbounded_send(msgs) {
                error!(
                    "server: drop conn with tikv endpoint {} flush conn error: {:?}",
                    addr, e
                );

                if let Some(addr_current) = addrs.remove(&store_id) {
                    if addr_current != *addr {
                        addrs.insert(store_id, addr_current);
                    }
                }
                return false;
            }

            conn.buffer = Some(Vec::with_capacity(PRESERVED_MSG_BUFFER_COUNT));
            true
        });

        if counter > 0 {
            RAFT_MESSAGE_FLUSH_COUNTER.inc_by(counter as i64);
        }
    }
}

impl Drop for RaftClient {
    fn drop(&mut self) {
        // Drop conns here to make sure all streams are dropped before Environment.
        self.conns.clear();
    }
}
