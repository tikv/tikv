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
use std::ffi::CString;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex};

use super::metrics::*;
use super::{Config, Result};
use futures::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures::sync::oneshot::{self, Sender};
use futures::{try_ready, Async, AsyncSink, Future, Poll, Sink, Stream};
use grpc::{
    ChannelBuilder, ClientCStreamSender, Environment, Error as GrpcError, RpcStatus, RpcStatusCode,
    WriteFlags,
};
use kvproto::raft_serverpb::RaftMessage;
use kvproto::tikvpb_grpc::TikvClient;
use util::collections::HashMap;
use util::security::SecurityManager;

const MAX_GRPC_RECV_MSG_LEN: i32 = 10 * 1024 * 1024;
const MAX_GRPC_SEND_MSG_LEN: i32 = 10 * 1024 * 1024;
const PRESERVED_MSG_BUFFER_COUNT: usize = 1024;

static CONN_ID: AtomicI32 = AtomicI32::new(0);

struct Conn {
    retry_conn_count: usize,
    stream: Option<UnboundedSender<VecDeque<(RaftMessage, WriteFlags)>>>,
    stream_receiver: Arc<Mutex<Option<UnboundedReceiver<VecDeque<(RaftMessage, WriteFlags)>>>>>,

    buffer: Option<VecDeque<(RaftMessage, WriteFlags)>>,
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
        retry_count: usize,
        reconnect: Option<(
            UnboundedSender<VecDeque<(RaftMessage, WriteFlags)>>,
            UnboundedReceiver<VecDeque<(RaftMessage, WriteFlags)>>,
        )>,
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
        let (tx, rx) = reconnect.unwrap_or_else(mpsc::unbounded);
        let rx_holder = Arc::new(Mutex::new(None));
        let rx_holder_copy = rx_holder.clone();

        let (tx_close, rx_close) = oneshot::channel();
        let (sink, receiver) = client.raft().unwrap();
        let addr = addr.to_owned();
        let addr1 = addr.clone();
        client.spawn(
            rx_close
                .map_err(|_| ())
                .select(
                    RaftMessageForwarder::new(sink, rx)
                        .then(move |sink_r| {
                            let sink_e = sink_r.err().map(|(e, rx)| {
                                *rx_holder_copy.lock().unwrap() = Some(rx);
                                e
                            });
                            alive.store(false, Ordering::SeqCst);
                            receiver.then(move |recv_r| {
                                check_rpc_result("raft", &addr1, sink_e, recv_r.err())
                            })
                        })
                        .map_err(move |e| {
                            let store = store_id.to_string();
                            REPORT_FAILURE_MSG_COUNTER
                                .with_label_values(&["unreachable", &*store])
                                .inc();
                        }),
                )
                .map(|_| ())
                .map_err(|_| ()),
        );
        Conn {
            retry_conn_count: retry_count,
            stream: Some(tx),
            stream_receiver: rx_holder,
            buffer: Some(VecDeque::with_capacity(PRESERVED_MSG_BUFFER_COUNT)),
            store_id,
            alive: alive1,

            _client: client,
            _close: tx_close,
        }
    }
}

struct RaftMessageForwarder {
    sender: ClientCStreamSender<RaftMessage>,
    stream: Option<UnboundedReceiver<VecDeque<(RaftMessage, WriteFlags)>>>,
    buffered: Option<VecDeque<(RaftMessage, WriteFlags)>>,
}

impl RaftMessageForwarder {
    fn new(
        sender: ClientCStreamSender<RaftMessage>,
        stream: UnboundedReceiver<VecDeque<(RaftMessage, WriteFlags)>>,
    ) -> RaftMessageForwarder {
        RaftMessageForwarder {
            sender,
            stream: Some(stream),
            buffered: None,
        }
    }

    fn handle_grpc_error(&mut self, e: GrpcError) -> <Self as Future>::Error {
        (e, self.stream.take().unwrap())
    }

    fn try_start_send(
        &mut self,
        mut queue: VecDeque<(RaftMessage, WriteFlags)>,
    ) -> Poll<(), <Self as Future>::Error> {
        debug_assert!(self.buffered.is_none());
        while let Some(item) = queue.pop_front() {
            if let AsyncSink::NotReady(item) = self
                .sender
                .start_send(item)
                .map_err(|e| self.handle_grpc_error(e))?
            {
                queue.push_front(item);
                self.buffered = Some(queue);
                return Ok(Async::NotReady);
            }
        }
        Ok(Async::Ready(()))
    }

    fn close_sender(&mut self) -> Poll<(), <Self as Future>::Error> {
        self.sender.close().map_err(|e| self.handle_grpc_error(e))
    }

    fn poll_sender(&mut self) -> Poll<(), <Self as Future>::Error> {
        self.sender
            .poll_complete()
            .map_err(|e| self.handle_grpc_error(e))
    }
}

impl Future for RaftMessageForwarder {
    type Item = ();
    type Error = (
        GrpcError,
        UnboundedReceiver<VecDeque<(RaftMessage, WriteFlags)>>,
    );

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Some(item) = self.buffered.take() {
            try_ready!(self.try_start_send(item))
        }

        loop {
            match self
                .stream
                .as_mut()
                .unwrap()
                .poll()
                .map_err(|_| unreachable!())?
            {
                Async::Ready(Some(queue)) => try_ready!(self.try_start_send(queue)),
                Async::Ready(None) => {
                    try_ready!(self.close_sender());
                    return Ok(Async::Ready(()));
                }
                Async::NotReady => {
                    try_ready!(self.poll_sender());
                    return Ok(Async::NotReady);
                }
            }
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
            .or_insert_with(|| {
                Conn::new(Arc::clone(env), addr, cfg, security_mgr, store_id, 0, None)
            })
    }

    pub fn send(&mut self, store_id: u64, addr: &str, msg: RaftMessage) -> Result<()> {
        let conn = self.get_conn(addr, msg.region_id, store_id);
        conn.buffer
            .as_mut()
            .unwrap()
            .push_back((msg, WriteFlags::default().buffer_hint(true)));
        Ok(())
    }

    pub fn flush(&mut self) {
        let addrs = &mut self.addrs;
        let mut counter: u64 = 0;
        let mut broken_conns = Vec::new();
        self.conns.retain(|&(ref addr, index), conn| {
            let store_id = conn.store_id;
            if !conn.alive.load(Ordering::SeqCst) {
                let mut retry_conn = false;
                if let Some(addr_current) = addrs.remove(&store_id) {
                    if addr_current != *addr {
                        addrs.insert(store_id, addr_current);
                    } else if conn.retry_conn_count < 10 {
                        // Only do fast reconnect 10 times.
                        addrs.insert(store_id, addr_current);
                        retry_conn = true;
                    } else {
                        info!("[{} -> {}] is stale", store_id, addr);
                    }
                }
                if retry_conn {
                    if let Some(rx) = conn.stream_receiver.lock().unwrap().take() {
                        // The associated GRPC connection is broken, will reconnect.
                        let tx = conn.stream.take().unwrap();
                        let retry = conn.retry_conn_count + 1;
                        broken_conns.push((store_id, addr.to_owned(), index, retry, tx, rx));
                    }
                }
                return false;
            }

            if !conn.buffer.as_ref().unwrap().is_empty() {
                counter += 1;
                let mut msgs = conn.buffer.take().unwrap();
                msgs.back_mut().unwrap().1 = WriteFlags::default();
                conn.stream.as_mut().unwrap().unbounded_send(msgs).unwrap();
                conn.buffer = Some(VecDeque::with_capacity(PRESERVED_MSG_BUFFER_COUNT));
            }
            true
        });

        // Fast reconnect to some stores without re-resolve it.
        for (store_id, addr, index, retry, tx, rx) in broken_conns {
            let env = self.env.clone();
            let cfg = &self.cfg;
            let security_mgr = &self.security_mgr;
            let reconn = Some((tx, rx));
            let conn = Conn::new(env, &addr, cfg, security_mgr, store_id, retry, reconn);
            info!("fast reconnect to {}, retry times {}", addr, retry);
            assert!(self.conns.insert((addr, index), conn).is_none());
        }

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

fn grpc_error_is_unimplemented(e: &GrpcError) -> bool {
    if let GrpcError::RpcFailure(RpcStatus { ref status, .. }) = e {
        let x = *status == RpcStatusCode::Unimplemented;
        return x;
    }
    false
}

fn check_rpc_result(
    rpc: &str,
    addr: &str,
    sink_e: Option<GrpcError>,
    recv_e: Option<GrpcError>,
) -> ::std::result::Result<(), bool> {
    if sink_e.is_none() && recv_e.is_none() {
        return Ok(());
    }
    warn!(
        "RPC {} to {} fail, sink_err: {:?}, err: {:?}",
        rpc, addr, sink_e, recv_e
    );
    recv_e.map_or(Ok(()), |e| Err(grpc_error_is_unimplemented(&e)))
}
