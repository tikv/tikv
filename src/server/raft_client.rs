// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::ffi::CString;
use std::i64;
use std::mem;
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use super::load_statistics::ThreadLoad;
use super::metrics::*;
use super::{Config, Result};
use crate::server::transport::RaftStoreRouter;
use futures::{future, stream, Future, Poll, Sink, Stream};
use grpcio::{
    ChannelBuilder, Environment, Error as GrpcError, RpcStatus, RpcStatusCode, WriteFlags,
};
use kvproto::raft_serverpb::RaftMessage;
use kvproto::tikvpb::BatchRaftMessage;
use kvproto::tikvpb_grpc::TikvClient;
use protobuf::RepeatedField;
use tikv_alloc::trace::{MemoryTrace, MemoryTraceProvider};
use tikv_util::collections::{HashMap, HashMapEntry};
use tikv_util::mpsc::batch::{self, BatchCollector, Sender as BatchSender};
use tikv_util::security::SecurityManager;
use tikv_util::timer::GLOBAL_TIMER_HANDLE;
use tokio_timer::timer::Handle;

// When merge raft messages into a batch message, leave a buffer.
const GRPC_SEND_MSG_BUF: usize = 64 * 1024;

const RAFT_MSG_MAX_BATCH_SIZE: usize = 128;
const RAFT_MSG_NOTIFY_SIZE: usize = 8;

static CONN_ID: AtomicI32 = AtomicI32::new(0);

pub struct CachedSizeMessage {
    msg: RaftMessage,
    size: usize,
}

impl CachedSizeMessage {
    fn new(msg: RaftMessage) -> CachedSizeMessage {
        let mut msg_size = msg.start_key.len() + msg.end_key.len();
        for entry in msg.get_message().get_entries() {
            msg_size += entry.data.len();
        }
        CachedSizeMessage {
            msg,
            size: msg_size,
        }
    }
}

struct Conn {
    stream: BatchSender<CachedSizeMessage>,
    trace: RaftClientMemoryTrace,
    _client: TikvClient,
    pending_msg_len: Arc<AtomicUsize>,
}

impl Conn {
    fn new<T: RaftStoreRouter + 'static>(
        env: Arc<Environment>,
        router: T,
        addr: &str,
        cfg: &Config,
        security_mgr: &SecurityManager,
        store_id: u64,
        trace: RaftClientMemoryTrace,
    ) -> Conn {
        info!("server: new connection with tikv endpoint"; "addr" => addr);

        let cb = ChannelBuilder::new(env)
            .stream_initial_window_size(cfg.grpc_stream_initial_window_size.0 as i32)
            .max_send_message_len(cfg.max_grpc_send_msg_len)
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

        let pending_msg_len = trace.new_conn_trace();
        let (tx, rx) = batch::unbounded::<CachedSizeMessage>(RAFT_MSG_NOTIFY_SIZE);
        let rx = batch::BatchReceiver::new(
            rx,
            RAFT_MSG_MAX_BATCH_SIZE,
            Vec::new,
            RaftMsgCollector::new(pending_msg_len.clone(), cfg.max_grpc_send_msg_len as usize),
        );

        // Use a mutex to make compiler happy.
        let rx1 = Arc::new(Mutex::new(rx));
        let rx2 = Arc::clone(&rx1);
        let (addr1, addr2) = (addr.to_owned(), addr.to_owned());

        let (batch_sink, batch_receiver) = client1.batch_raft().unwrap();
        let batch_send_or_fallback = batch_sink
            .send_all(Reusable(rx1).map(move |v| {
                let mut batch_msgs = BatchRaftMessage::new();
                batch_msgs.set_msgs(RepeatedField::from(v));
                (batch_msgs, WriteFlags::default().buffer_hint(false))
            }))
            .then(move |sink_r| {
                batch_receiver.then(move |recv_r| {
                    check_rpc_result("batch_raft", &addr1, sink_r.err(), recv_r.err())
                })
            })
            .or_else(move |fallback| {
                if !fallback {
                    return Box::new(future::err(false))
                        as Box<dyn Future<Item = _, Error = bool> + Send>;
                }
                // Fallback to raft RPC.
                warn!("batch_raft is unimplemented, fallback to raft");
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
                Box::new(sink.send_all(msgs).then(move |sink_r| {
                    receiver.then(move |recv_r| {
                        check_rpc_result("raft", &addr2, sink_r.err(), recv_r.err())
                    })
                }))
            });

        client1.spawn(
            batch_send_or_fallback
                .map_err(move |_| {
                    REPORT_FAILURE_MSG_COUNTER
                        .with_label_values(&["unreachable", &*store_id.to_string()])
                        .inc();
                    router.broadcast_unreachable(store_id);
                })
                .map(|_| ()),
        );

        pending_msg_len.fetch_add(mem::size_of::<Conn>(), Ordering::Relaxed);

        Conn {
            stream: tx,
            pending_msg_len,
            trace,
            _client: client1,
        }
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.trace.remove(&self.pending_msg_len);
    }
}

#[derive(Default, Clone)]
struct RaftClientMemoryTrace {
    mem_sizes: Arc<Mutex<Vec<Arc<AtomicUsize>>>>,
}

impl RaftClientMemoryTrace {
    fn new_conn_trace(&self) -> Arc<AtomicUsize> {
        let s = Arc::new(AtomicUsize::new(0));
        self.mem_sizes.lock().unwrap().push(s.clone());
        s
    }

    fn remove(&self, s: &Arc<AtomicUsize>) {
        let mut sizes = self.mem_sizes.lock().unwrap();
        let pos = sizes.iter().position(|i| Arc::ptr_eq(i, s)).unwrap();
        sizes.swap_remove(pos);
    }
}

impl MemoryTraceProvider for RaftClientMemoryTrace {
    fn trace(&mut self, dump: &mut MemoryTrace) {
        let sizes = self.mem_sizes.lock().unwrap();
        let sum = sizes.iter().map(|s| s.load(Ordering::Relaxed)).sum();
        drop(sizes);
        let s = dump.add_sub_trace("RaftClient");
        s.set_size(sum);
        dump.add_size(sum);
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
    trace: RaftClientMemoryTrace,
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
        let trace = RaftClientMemoryTrace::default();
        tikv_alloc::trace::register_provider(Box::new(trace.clone()));
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
            trace,
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
                    self.trace.clone(),
                );
                e.insert(conn)
            }
        }
    }

    pub fn send(&mut self, store_id: u64, addr: &str, msg: RaftMessage) -> Result<()> {
        let region_id = msg.region_id;
        let msg = CachedSizeMessage::new(msg);
        let size = msg.size;

        {
            let conn = self.get_conn(addr, region_id, store_id);
            if conn.stream.send(msg).is_ok() {
                conn.pending_msg_len.fetch_add(size, Ordering::Relaxed);
                return Ok(());
            }
        }

        warn!("send to {} fail, the gRPC connection could be broken", addr);
        let index = region_id as usize % self.cfg.grpc_raft_conn_num;
        self.conns.remove(&(addr.to_owned(), index));

        if let Some(current_addr) = self.addrs.remove(&store_id) {
            if current_addr != *addr {
                self.addrs.insert(store_id, current_addr);
            }
        }
        Err(box_err!("RaftClient send fail"))
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

// Collect raft messages into a vector so that we can merge them into one message later.
struct RaftMsgCollector {
    pending_msg_len: Arc<AtomicUsize>,
    size: usize,
    limit: usize,
}

impl RaftMsgCollector {
    fn new(pending_msg_len: Arc<AtomicUsize>, limit: usize) -> Self {
        Self {
            size: 0,
            limit,
            pending_msg_len,
        }
    }
}

impl BatchCollector<Vec<RaftMessage>, CachedSizeMessage> for RaftMsgCollector {
    fn collect(
        &mut self,
        v: &mut Vec<RaftMessage>,
        e: CachedSizeMessage,
    ) -> Option<CachedSizeMessage> {
        if self.size > 0 && self.size + e.size + GRPC_SEND_MSG_BUF >= self.limit as usize {
            self.pending_msg_len.fetch_sub(self.size, Ordering::Relaxed);
            self.size = 0;
            return Some(e);
        }
        self.size += e.size;
        v.push(e.msg);
        None
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
) -> std::result::Result<(), bool> {
    if sink_e.is_none() && recv_e.is_none() {
        return Ok(());
    }
    warn!( "RPC {} fail", rpc; "to_addr" => addr, "sink_err" => ?sink_e, "err" => ?recv_e);
    recv_e.map_or(Ok(()), |e| Err(grpc_error_is_unimplemented(&e)))
}
