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
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam::sync::AtomicOption;
use futures::sync::oneshot::{self, Sender};
use futures::{future, stream, Future, Poll, Sink, Stream};
use grpc::{ChannelBuilder, Environment, Error as GrpcError, RpcStatus, RpcStatusCode, WriteFlags};
use kvproto::raft_serverpb::RaftMessage;
use kvproto::tikvpb::BatchRaftMessage;
use kvproto::tikvpb_grpc::TikvClient;
use protobuf::RepeatedField;
use tokio::runtime::Runtime;
use tokio::timer::Delay;

use super::metrics::*;
use super::{Config, Result};
use util::collections::{HashMap, HashMapEntry};
use util::mpsc2::{batch_unbounded, Sender as BatchSender};
use util::security::SecurityManager;

const MAX_GRPC_RECV_MSG_LEN: i32 = 10 * 1024 * 1024;
const MAX_GRPC_SEND_MSG_LEN: i32 = 10 * 1024 * 1024;

static CONN_ID: AtomicI32 = AtomicI32::new(0);

struct Conn {
    stream: BatchSender<RaftMessage>,
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

        let (tx_close, rx_close) = oneshot::channel();
        let addr = addr.to_owned();

        let client = TikvClient::new(channel);
        let (tx, rx) = batch_unbounded::<RaftMessage>();

        let rx_for_batch_raft = Arc::new(AtomicOption::new());
        rx_for_batch_raft.swap(rx, Ordering::SeqCst);
        let rx_for_raft = Arc::clone(&rx_for_batch_raft);

        let (batch_sink, batch_receiver) = client.batch_raft().unwrap();
        let (sink, receiver) = client.raft().unwrap();

        let batch_send_or_fallback = batch_sink
            .send_all(Reusable::new(rx_for_batch_raft).map(move |v| {
                let mut batch_msgs = BatchRaftMessage::new();
                batch_msgs.set_msgs(RepeatedField::from(v));
                (batch_msgs, WriteFlags::default().buffer_hint(false))
            }))
            .then(|result| match result {
                Ok(_) => box future::ok(()) as Box<Future<Item = (), Error = GrpcError> + Send>,
                Err(GrpcError::RpcFinished(Some(RpcStatus { status, .. })))
                    if status == RpcStatusCode::Unimplemented =>
                {
                    // fallback batch_raft to raft call.
                    let msgs = Reusable::new(rx_for_raft)
                        .map(|msgs| {
                            let len = msgs.len();
                            let grpc_msgs = msgs.into_iter().enumerate().map(move |(i, v)| {
                                if i < len - 1 {
                                    (v, WriteFlags::default().buffer_hint(true))
                                } else {
                                    (v, WriteFlags::default())
                                }
                            });
                            stream::iter_ok::<_, GrpcError>(grpc_msgs)
                        })
                        .flatten();
                    box sink.send_all(msgs).map(|_| ()).then(move |r| {
                        drop(receiver);
                        r
                    })
                }
                Err(e) => box future::err(e),
            })
            .then(move |r| {
                drop(batch_receiver);
                r
            });

        client.spawn(
            rx_close
                .map_err(|_| ())
                .select(
                    batch_send_or_fallback
                        .map_err(move |e| {
                            REPORT_FAILURE_MSG_COUNTER
                                .with_label_values(&["unreachable", &*store_id.to_string()])
                                .inc();
                            warn!("send raftmessage to {} failed: {:?}", addr, e);
                        })
                )
                .then(|_| Ok(())),
        );

        Conn {
            stream: tx,
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
    in_heavy_load: Arc<(AtomicUsize, AtomicUsize)>,
    helper_runtime: Arc<Runtime>,
}

impl RaftClient {
    pub fn new(
        env: Arc<Environment>,
        cfg: Arc<Config>,
        security_mgr: Arc<SecurityManager>,
        in_heavy_load: Arc<(AtomicUsize, AtomicUsize)>,
        helper_runtime: Arc<Runtime>,
    ) -> RaftClient {
        RaftClient {
            env,
            conns: HashMap::default(),
            addrs: HashMap::default(),
            cfg,
            security_mgr,
            in_heavy_load,
            helper_runtime,
        }
    }

    fn get_conn(&mut self, addr: &str, region_id: u64, store_id: u64) -> &mut Conn {
        let index = region_id as usize % self.cfg.grpc_raft_conn_num;
        match self.conns.entry((addr.to_owned(), index)) {
            HashMapEntry::Occupied(e) => return e.into_mut(),
            HashMapEntry::Vacant(e) => {
                let conn = Conn::new(
                    Arc::clone(&self.env),
                    addr,
                    &self.cfg,
                    &self.security_mgr,
                    store_id,
                );
                return e.insert(conn);
            }
        }
    }

    pub fn send(&mut self, store_id: u64, addr: &str, msg: RaftMessage) -> Result<()> {
        if let Err(_) = self
            .get_conn(addr, msg.region_id, store_id)
            .stream
            .send(msg)
        {
            if let Some(current_addr) = self.addrs.remove(&store_id) {
                if current_addr != *addr {
                    self.addrs.insert(store_id, current_addr);
                }
            }
        }
        Ok(())
    }

    pub fn flush(&mut self) {
        let mut counter = 0;
        for conn in self.conns.values_mut() {
            if let Some(notifier) = conn.stream.get_notifier() {
                if self.in_heavy_load.1.load(Ordering::SeqCst) > 160 {
                    self.helper_runtime.executor().spawn(
                        Delay::new(Instant::now() + Duration::from_millis(2))
                            .map_err(|_| ())
                            .inspect(move |_| notifier.notify()),
                    );
                } else {
                    notifier.notify();
                    counter += 1;
                }
            }
        }
        RAFT_MESSAGE_FLUSH_COUNTER.inc_by(counter as i64);
    }
}

impl Drop for RaftClient {
    fn drop(&mut self) {
        // Drop conns here to make sure all streams are dropped before Environment.
        self.conns.clear();
    }
}

// ReusableReceiver is for fallback batch_raft call to raft call.
struct Reusable<T> {
    lock: Arc<AtomicOption<T>>,
    t: Option<T>,
}

impl<T> Reusable<T> {
    fn new(t: Arc<AtomicOption<T>>) -> Self {
        Reusable { lock: t, t: None }
    }
}

impl<T: Stream> Stream for Reusable<T> {
    type Item = T::Item;
    type Error = GrpcError;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if self.t.is_none() {
            self.t = self.lock.take(Ordering::SeqCst);
        }
        let t = self.t.as_mut().unwrap();
        t.poll().map_err(|_| GrpcError::RpcFinished(None))
    }
}

impl<T> Drop for Reusable<T> {
    fn drop(&mut self) {
        if let Some(t) = self.t.take() {
            self.lock.swap(t, Ordering::SeqCst);
        }
    }
}
