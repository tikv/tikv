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

use std::collections::VecDeque;
use std::collections::hash_map::Entry;
use std::ffi::CString;
use std::fmt::{self, Display, Formatter};
use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};

use futures::future::{self, Shared, SharedError};
use futures::sync::mpsc::{self, UnboundedSender};
use futures::sync::oneshot::{self, Sender};
use futures::{stream, Async, Future, Poll, Sink, Stream};
use futures_cpupool::{Builder, CpuPool};
use grpc::{ChannelBuilder, Environment, WriteFlags};
use kvproto::raft_serverpb::RaftMessage;
use kvproto::tikvpb_grpc::TikvClient;

use super::metrics::*;
use super::resolve::{ResolveResult, StoreAddrResolver};
use super::{Config, Error, Result};
use util::collections::HashMap;
use util::security::SecurityManager;

const MAX_GRPC_RECV_MSG_LEN: usize = 10 * 1024 * 1024;
const MAX_GRPC_SEND_MSG_LEN: usize = 10 * 1024 * 1024;
// `VecDeque` adds another slot for giving capacity.
const PRESERVED_MSG_BUFFER_COUNT: usize = 1024 - 1;
// TODO: make it configurable and become a limit on size instead of count.
const MSG_BUFFER_LIMIT: usize = 102400 - 1;

static CONN_ID: AtomicUsize = ATOMIC_USIZE_INIT;

struct ConnectionBuilder {
    store_id: u64,
    addr: Address,
    buffer: Option<VecDeque<(RaftMessage, WriteFlags)>>,
}

impl ConnectionBuilder {
    pub fn new(store_id: u64, addr: Address) -> ConnectionBuilder {
        ConnectionBuilder {
            store_id,
            addr,
            buffer: None,
        }
    }

    pub fn buffer(mut self, buffer: VecDeque<(RaftMessage, WriteFlags)>) -> Self {
        self.buffer = Some(buffer);
        self
    }

    pub fn connect(
        self,
        env: Arc<Environment>,
        cfg: &Config,
        security_mgr: Arc<SecurityManager>,
        pool: &CpuPool,
    ) -> Conn {
        let cb = ChannelBuilder::new(env)
        .stream_initial_window_size(cfg.grpc_stream_initial_window_size.0 as usize)
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
        let (tx, rx) = mpsc::unbounded();
        let (tx_close, rx_close) = oneshot::channel();
        let addr = self.addr;
        let buffer = self.buffer
            .unwrap_or_else(|| VecDeque::with_capacity(PRESERVED_MSG_BUFFER_COUNT));
        let version = addr.version;
        let store_id = self.store_id;
        let f = rx_close
            .map_err(|_| ())
            // It's OK to ignore address resolve error, as log
            // is printed during resolve.
            .select(addr.map_err(|_| ()).and_then(move |addr| {
                info!(
                    "new connection to store {}({}) {}",
                    store_id, version, addr
                );

                let channel = security_mgr.connect(cb, &addr);
                let client = TikvClient::new(channel);
                let (sink, resp) = client.raft().unwrap();
                let (mut tx_close, rx_close) = oneshot::channel::<()>();
                client.spawn(
                    sink.sink_map_err(Error::from)
                        .send_all(rx.map(stream::iter_ok).flatten().map_err(|()| Error::Sink))
                        .map(|_| ())
                        .join(resp.map_err(Error::from))
                        .map_err(move |e| {
                            let store = store_id.to_string();
                            REPORT_FAILURE_MSG_COUNTER
                                .with_label_values(&["unreachable", &store])
                                .inc();
                            // TODO: should report to raftstore.
                            warn!(
                                "send raftmessage to [{}] {} failed: {:?}",
                                store_id, addr, e
                            );
                        })
                        .then(|_| {
                            drop(rx_close);
                            Ok(())
                        }),
                );
                future::poll_fn(move || tx_close.poll_cancel()).then(|_| {
                    drop(client);
                    Ok(())
                })
            }));

        pool.spawn(f).forget();

        Conn {
            stream: tx,
            buffer,
            store_id,
            version,
            closed: tx_close,
        }
    }
}

struct Conn {
    stream: UnboundedSender<Vec<(RaftMessage, WriteFlags)>>,
    buffer: VecDeque<(RaftMessage, WriteFlags)>,
    store_id: u64,
    version: usize,
    closed: Sender<()>,
}

#[derive(Clone)]
pub struct Address {
    pub f: Shared<ResolveResult>,
    version: usize,
}

impl Display for Address {
    #[inline]
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        match self.f.peek() {
            Some(Ok(addr)) => write!(formatter, "({}){:?}", self.version, addr),
            None => write!(formatter, "({})resolving...", self.version),
            Some(Err(_)) => write!(formatter, "({})resolve failed", self.version),
        }
    }
}

impl Future for Address {
    type Item = String;
    type Error = SharedError<String>;

    #[inline]
    fn poll(&mut self) -> Poll<String, Self::Error> {
        let addr = try_ready!(self.f.poll());
        Ok(Async::Ready(String::clone(&addr)))
    }
}

struct ClientContext<S> {
    env: Arc<Environment>,
    cfg: Arc<Config>,
    security_mgr: Arc<SecurityManager>,
    // TODO: remove arc.
    resolver: Arc<S>,
    addrs: HashMap<u64, Address>,
    addr_version: usize,
}

impl<S: StoreAddrResolver> ClientContext<S> {
    #[inline]
    pub fn addr(&mut self, store_id: u64) -> Address {
        self.addrs
            .get(&store_id)
            .cloned()
            .unwrap_or_else(|| self.refresh_address(store_id))
    }

    fn refresh_address(&mut self, store_id: u64) -> Address {
        self.addr_version = self.addr_version.overflowing_add(1).0;
        let addr = Address {
            f: self.resolver.resolve(store_id).shared(),
            version: self.addr_version,
        };
        self.addrs.insert(store_id, Address::clone(&addr));
        addr
    }

    #[inline]
    fn connect(&mut self, pool: &CpuPool, store_id: u64) -> Conn {
        let addr = self.addrs
            .get(&store_id)
            .cloned()
            .unwrap_or_else(|| self.refresh_address(store_id));
        ConnectionBuilder::new(store_id, addr).connect(
            Arc::clone(&self.env),
            &self.cfg,
            Arc::clone(&self.security_mgr),
            pool,
        )
    }

    #[inline]
    fn reconnect_if_necessary(&mut self, pool: &CpuPool, conn: &mut Conn) {
        if !conn.closed.is_canceled() {
            return;
        }

        debug!("reconnecting to store {}", conn.store_id);

        let addr = self.addrs
            .get(&conn.store_id)
            .filter(|addr| addr.version != conn.version)
            .cloned()
            .unwrap_or_else(|| self.refresh_address(conn.store_id));
        *conn = ConnectionBuilder::new(conn.store_id, addr)
            .buffer(mem::replace(&mut conn.buffer, VecDeque::new()))
            .connect(
                Arc::clone(&self.env),
                &self.cfg,
                Arc::clone(&self.security_mgr),
                pool,
            );
    }
}

/// `RaftClient` is used for sending raft messages to other stores.
pub struct RaftClient<S> {
    ctx: ClientContext<S>,
    conns: HashMap<(u64, usize), Conn>,
    pool: CpuPool,
}

impl<S: StoreAddrResolver> RaftClient<S> {
    pub fn new(
        env: Arc<Environment>,
        cfg: Arc<Config>,
        security_mgr: Arc<SecurityManager>,
        resolver: Arc<S>,
    ) -> RaftClient<S> {
        RaftClient {
            ctx: ClientContext {
                env,
                cfg,
                security_mgr,
                resolver,
                addrs: HashMap::default(),
                addr_version: 0,
            },
            conns: HashMap::default(),
            pool: Builder::new()
                .name_prefix("raftclient")
                .pool_size(1)
                .create(),
        }
    }

    fn conn_mut(&mut self, region_id: u64, store_id: u64) -> &mut Conn {
        let index = region_id as usize % self.ctx.cfg.grpc_raft_conn_num;
        match self.conns.entry((store_id, index)) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                let conn = self.ctx.connect(&self.pool, store_id);
                e.insert(conn)
            }
        }
    }

    #[inline]
    pub fn addr(&mut self, store_id: u64) -> Address {
        self.ctx.addr(store_id)
    }

    // TODO: limit the sending messages.
    pub fn send(&mut self, store_id: u64, msg: RaftMessage) -> Result<()> {
        let conn = self.conn_mut(msg.region_id, store_id);
        let msg = (msg, WriteFlags::default().buffer_hint(true));
        if conn.buffer.len() < MSG_BUFFER_LIMIT {
            conn.buffer.push_back(msg);
        } else {
            // TODO: record metrics.
            conn.buffer.pop_front();
            conn.buffer.push_back(msg);
        }
        Ok(())
    }

    pub fn flush(&mut self) {
        let ctx = &mut self.ctx;
        let pool = &self.pool;
        let mut counter: u64 = 0;
        for (&(store_id, _), conn) in &mut self.conns {
            if conn.buffer.is_empty() {
                continue;
            }

            ctx.reconnect_if_necessary(pool, conn);

            counter += 1;
            let mut msgs: Vec<_> = conn.buffer.drain(..).collect();
            msgs.last_mut().unwrap().1 = WriteFlags::default();

            // It's safe to ignored send failure, as it will be reconnected automatically
            // next time.
            if let Err(e) = conn.stream.unbounded_send(msgs) {
                debug!(
                    "ignore send failure to store {}({}): {:?}",
                    store_id, conn.version, e
                );
            }
            if conn.buffer.capacity() > PRESERVED_MSG_BUFFER_COUNT {
                conn.buffer = VecDeque::with_capacity(PRESERVED_MSG_BUFFER_COUNT);
            }
        }

        if counter > 0 {
            RAFT_MESSAGE_FLUSH_COUNTER.inc_by(counter as i64);
        }
    }
}

impl<P> Drop for RaftClient<P> {
    fn drop(&mut self) {
        // Drop conns here to make sure all streams are dropped before Environment.
        self.conns.clear();
    }
}
