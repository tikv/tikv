// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::ffi::CString;
use std::i64;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use super::load_statistics::ThreadLoad;
use super::metrics::*;
use super::{Config, Result};
use crate::server::transport::RaftStoreRouter;
use crossbeam::channel::SendError;
use futures::{future, stream, Future, Poll, Sink, Stream};
use grpcio::{
    ChannelBuilder, Environment, Error as GrpcError, RpcStatus, RpcStatusCode, WriteFlags,
};
use kvproto::raft_serverpb::RaftMessage;
use kvproto::tikvpb::{BatchRaftMessage, TikvClient};
use tikv_util::collections::{HashMap, HashMapEntry};
use tikv_util::mpsc::batch::{self, Sender as BatchSender};
use tikv_util::security::SecurityManager;
use tikv_util::timer::GLOBAL_TIMER_HANDLE;
use tokio_timer::timer::Handle;

const MAX_GRPC_RECV_MSG_LEN: i32 = 10 * 1024 * 1024;
const MAX_GRPC_SEND_MSG_LEN: i32 = 10 * 1024 * 1024;

const RAFT_MSG_MAX_BATCH_SIZE: usize = 128;
const RAFT_MSG_NOTIFY_SIZE: usize = 8;

static CONN_ID: AtomicI32 = AtomicI32::new(0);

struct Conn {
    stream: BatchSender<RaftMessage>,
    _client: TikvClient,
}

impl Conn {
    fn new<T: RaftStoreRouter + 'static>(
        env: Arc<Environment>,
        router: T,
        addr: &str,
        cfg: &Config,
        security_mgr: &SecurityManager,
        store_id: u64,
    ) -> Conn {
        info!("server: new connection with tikv endpoint"; "addr" => addr);

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
        let client1 = TikvClient::new(channel);
        let client2 = client1.clone();

        let (tx, rx) = batch::unbounded::<RaftMessage>(RAFT_MSG_NOTIFY_SIZE);
        let rx = batch::BatchReceiver::new(rx, RAFT_MSG_MAX_BATCH_SIZE, Vec::new, |v, e| v.push(e));
        // Use a mutex to make compiler happy.
        let rx1 = Arc::new(Mutex::new(rx));
        let rx2 = Arc::clone(&rx1);

        let (batch_sink, batch_receiver) = client1.batch_raft().unwrap();
        let batch_send_or_fallback = batch_sink
            .send_all(Reusable(rx1).map(move |v| {
                let mut batch_msgs = BatchRaftMessage::default();
                batch_msgs.set_msgs(v.into());
                (batch_msgs, WriteFlags::default().buffer_hint(false))
            }))
            .then(move |r| {
                drop(batch_receiver);
                match r {
                    Ok(_) => {
                        info!("batch_raft RPC finished success");
                        Box::new(future::ok(()))
                            as Box<dyn Future<Item = (), Error = GrpcError> + Send>
                    }
                    Err(GrpcError::RpcFinished(Some(RpcStatus { status, .. })))
                        if status == RpcStatusCode::UNIMPLEMENTED =>
                    {
                        // Fallback to raft RPC.
                        warn!("batch_raft fail, fallback to raft");
                        let (sink, receiver) = client2.raft().unwrap();
                        let msgs = Reusable(rx2)
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
                        Box::new(sink.send_all(msgs).map(|_| ()).then(move |r| {
                            drop(receiver);
                            match r {
                                Ok(_) => info!("raft RPC finished success"),
                                Err(ref e) => warn!("raft RPC finished fail"; "err" => ?e),
                            };
                            r
                        }))
                    }
                    Err(e) => {
                        warn!("batch_raft RPC finished fail"; "err" => ?e);
                        Box::new(future::err(e))
                    }
                }
            });

        let addr = addr.to_owned();
        client1.spawn(
            batch_send_or_fallback
                .map_err(move |e| {
                    REPORT_FAILURE_MSG_COUNTER
                        .with_label_values(&["unreachable", &*store_id.to_string()])
                        .inc();
                    router.broadcast_unreachable(store_id);
                    warn!("batch_raft/raft RPC finally fail"; "to_addr" => addr, "err" => ?e);
                })
                .map(|_| ()),
        );

        Conn {
            stream: tx,
            _client: client1,
        }
    }
}

/// `RaftClient` is used for sending raft messages to other stores.
pub struct RaftClient<T: 'static> {
    env: Arc<Environment>,
    router: Mutex<T>,
    conns: HashMap<(String, usize), Conn>,
    pub addrs: HashMap<u64, String>,
    cfg: Arc<Config>,
    security_mgr: Arc<SecurityManager>,

    // To access CPU load of gRPC threads.
    grpc_thread_load: Arc<ThreadLoad>,
    // When message senders want to delay the notification to the gRPC client,
    // it can put a tokio_timer::Delay to the runtime.
    stats_pool: tokio_threadpool::Sender,
    timer: Handle,
}

impl<T: RaftStoreRouter> RaftClient<T> {
    pub fn new(
        env: Arc<Environment>,
        cfg: Arc<Config>,
        security_mgr: Arc<SecurityManager>,
        router: T,
        grpc_thread_load: Arc<ThreadLoad>,
        stats_pool: tokio_threadpool::Sender,
    ) -> RaftClient<T> {
        RaftClient {
            env,
            router: Mutex::new(router),
            conns: HashMap::default(),
            addrs: HashMap::default(),
            cfg,
            security_mgr,
            grpc_thread_load,
            stats_pool,
            timer: GLOBAL_TIMER_HANDLE.clone(),
        }
    }

    fn get_conn(&mut self, addr: &str, region_id: u64, store_id: u64) -> &mut Conn {
        let index = region_id as usize % self.cfg.grpc_raft_conn_num;
        match self.conns.entry((addr.to_owned(), index)) {
            HashMapEntry::Occupied(e) => e.into_mut(),
            HashMapEntry::Vacant(e) => {
                let conn = Conn::new(
                    Arc::clone(&self.env),
                    self.router.lock().unwrap().clone(),
                    addr,
                    &self.cfg,
                    &self.security_mgr,
                    store_id,
                );
                e.insert(conn)
            }
        }
    }

    pub fn send(&mut self, store_id: u64, addr: &str, msg: RaftMessage) -> Result<()> {
        if let Err(SendError(msg)) = self
            .get_conn(addr, msg.region_id, store_id)
            .stream
            .send(msg)
        {
            warn!("send to {} fail, the gRPC connection could be broken", addr);
            let index = msg.region_id as usize % self.cfg.grpc_raft_conn_num;
            self.conns.remove(&(addr.to_owned(), index));

            if let Some(current_addr) = self.addrs.remove(&store_id) {
                if current_addr != *addr {
                    self.addrs.insert(store_id, current_addr);
                }
            }
            return Err(box_err!("RaftClient send fail"));
        }
        Ok(())
    }

    pub fn flush(&mut self) {
        let (mut counter, mut delay_counter) = (0, 0);
        for conn in self.conns.values_mut() {
            if conn.stream.is_empty() {
                continue;
            }
            if let Some(notifier) = conn.stream.get_notifier() {
                if !self.grpc_thread_load.in_heavy_load() {
                    notifier.notify();
                    counter += 1;
                    continue;
                }
                let wait = self.cfg.heavy_load_wait_duration.0;
                let _ = self.stats_pool.spawn(
                    self.timer
                        .delay(Instant::now() + wait)
                        .map_err(|_| warn!("RaftClient delay flush error"))
                        .inspect(move |_| notifier.notify()),
                );
            }
            delay_counter += 1;
        }
        RAFT_MESSAGE_FLUSH_COUNTER.inc_by(i64::from(counter));
        RAFT_MESSAGE_DELAY_FLUSH_COUNTER.inc_by(i64::from(delay_counter));
    }
}

// Reusable is for fallback batch_raft call to raft call.
struct Reusable<T>(Arc<Mutex<T>>);
impl<T: Stream> Stream for Reusable<T> {
    type Item = T::Item;
    type Error = GrpcError;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut t = self.0.lock().unwrap();
        t.poll().map_err(|_| GrpcError::RpcFinished(None))
    }
}
