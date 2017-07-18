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

use std::thread;
use std::str::FromStr;
use std::time::{Instant, Duration};
use std::net::{SocketAddr, IpAddr};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Sender as StdSender, Receiver as StdReceiver, channel as std_channel};

use test::Bencher;
use kvproto::tikvpb_grpc::*;
use kvproto::coprocessor::{Request, Response};
use kvproto::raft_serverpb::{RaftMessage, Done, SnapshotChunk};
use kvproto::kvrpcpb::*;

use futures::{Future, Stream, Sink, future, stream};
use futures::sync::mpsc::{channel as future_channel, Sender as FutureSender};
use futures::sync::oneshot::{self, Sender as OneShotSender};
use grpc::{WriteFlags, Server as GrpcServer, ServerBuilder, Environment, ChannelBuilder,
           RpcContext, UnarySink, RequestStream, ClientStreamingSink};

// Default settings used in TiKV.
const DEFAULT_GRPC_CONCURRENCY: usize = 4;
const DEFAULT_GRPC_CONCURRENT_STREAM: usize = 1024;
const DEFAULT_GRPC_RAFT_CONN_NUM: usize = 10;
const DEFAULT_GRPC_STREAM_INITIAL_WINDOW_SIZE: usize = 2 * 1024 * 1024;
const MAX_GRPC_RECV_MSG_LEN: usize = 10 * 1024 * 1024;
const MAX_GRPC_SEND_MSG_LEN: usize = 128 * 1024 * 1024;

// TODO: it's only suitable for streaming benchmarks.
struct Inner {
    name: String,
    counter: Arc<AtomicUsize>,
    // useful in bench.iter()
    forwarder: Option<StdSender<()>>,
}

#[derive(Clone)]
struct BenchTikvHandler {
    running: Arc<Mutex<Option<Inner>>>,
}

impl BenchTikvHandler {
    fn init_recording(&self,
                      name: String,
                      counter: Arc<AtomicUsize>,
                      forwarder: StdSender<()>)
                      -> Result<(), String> {
        let mut running = self.running.lock().unwrap();
        if running.is_some() {
            return Err(running.as_ref().unwrap().name.clone());
        }

        *running = Some(Inner {
            name: name,
            counter: counter,
            forwarder: Some(forwarder),
        });
        Ok(())
    }

    fn start_recording(&self) -> Result<(), String> {
        let running = self.running.lock().unwrap();
        if running.is_none() {
            return Err("not initialize".to_owned());
        }

        running.as_ref().unwrap().counter.store(1, Ordering::Release);
        Ok(())
    }

    fn stop_recording(&self) -> Result<usize, String> {
        let mut running = self.running.lock().unwrap();
        if running.is_none() {
            return Err("not initialize".to_owned());
        }
        let case = running.take().unwrap();
        let count = case.counter.swap(0, Ordering::Acquire);
        Ok(count)
    }
}

impl Tikv for BenchTikvHandler {
    fn raft(&self,
            ctx: RpcContext,
            stream: RequestStream<RaftMessage>,
            _: ClientStreamingSink<Done>) {
        let mut running = self.running.lock().unwrap();
        let mut inner = running.as_mut().unwrap();
        let counter = inner.counter.clone();
        let forwarder = inner.forwarder.take().unwrap();

        ctx.spawn(stream.for_each(move |_| {
                if 0 != counter.load(Ordering::Acquire) {
                    counter.fetch_add(1, Ordering::Release);
                    let _ = forwarder.send(());
                };
                future::ok(())
            })
            .map_err(|_| ())
            .then(|_| future::ok::<_, ()>(())));
    }

    fn kv_get(&self, _: RpcContext, _: GetRequest, _: UnarySink<GetResponse>) {
        unimplemented!()
    }

    fn kv_scan(&self, _: RpcContext, _: ScanRequest, _: UnarySink<ScanResponse>) {
        unimplemented!()
    }

    fn kv_prewrite(&self, _: RpcContext, _: PrewriteRequest, _: UnarySink<PrewriteResponse>) {
        unimplemented!()
    }

    fn kv_commit(&self, _: RpcContext, _: CommitRequest, _: UnarySink<CommitResponse>) {
        unimplemented!()
    }

    fn kv_import(&self, _: RpcContext, _: ImportRequest, _: UnarySink<ImportResponse>) {
        unimplemented!()
    }

    fn kv_cleanup(&self, _: RpcContext, _: CleanupRequest, _: UnarySink<CleanupResponse>) {
        unimplemented!()
    }

    fn kv_batch_get(&self, _: RpcContext, _: BatchGetRequest, _: UnarySink<BatchGetResponse>) {
        unimplemented!()
    }

    fn kv_batch_rollback(&self,
                         _: RpcContext,
                         _: BatchRollbackRequest,
                         _: UnarySink<BatchRollbackResponse>) {
        unimplemented!()
    }

    fn kv_scan_lock(&self, _: RpcContext, _: ScanLockRequest, _: UnarySink<ScanLockResponse>) {
        unimplemented!()
    }

    fn kv_resolve_lock(&self,
                       _: RpcContext,
                       _: ResolveLockRequest,
                       _: UnarySink<ResolveLockResponse>) {
        unimplemented!()
    }

    fn kv_gc(&self, _: RpcContext, _: GCRequest, _: UnarySink<GCResponse>) {
        unimplemented!()
    }

    fn raw_get(&self, _: RpcContext, _: RawGetRequest, _: UnarySink<RawGetResponse>) {
        unimplemented!()
    }

    fn raw_put(&self, _: RpcContext, _: RawPutRequest, _: UnarySink<RawPutResponse>) {
        unimplemented!()
    }

    fn raw_delete(&self, _: RpcContext, _: RawDeleteRequest, _: UnarySink<RawDeleteResponse>) {
        unimplemented!()
    }

    fn coprocessor(&self, _: RpcContext, _: Request, _: UnarySink<Response>) {
        unimplemented!()
    }

    fn snapshot(&self,
                _: RpcContext,
                _: RequestStream<SnapshotChunk>,
                _: ClientStreamingSink<Done>) {
        unimplemented!()
    }

    fn raw_scan(&self, _: RpcContext, _: RawScanRequest, _: UnarySink<RawScanResponse>) {
        unimplemented!()
    }

    fn mvcc_get_by_key(&self,
                       _: RpcContext,
                       _: MvccGetByKeyRequest,
                       _: UnarySink<MvccGetByKeyResponse>) {
        unimplemented!()
    }

    fn mvcc_get_by_start_ts(&self,
                            _: RpcContext,
                            _: MvccGetByStartTsRequest,
                            _: UnarySink<MvccGetByStartTsResponse>) {
        unimplemented!()
    }
}

/// A mock TiKV server for benching purpose, all `GrpcServer`
/// configuration MUST be consist with the real server in
/// `src/server/server.rs`.
// TODO: a better way for handling the cfgs.
pub struct BenchTikvServer {
    server: GrpcServer,
    local_addr: SocketAddr,
    handler: BenchTikvHandler,

    // Coupled with forwarder in `BenchTikvHandler`.
    rx: Option<StdReceiver<()>>,
}

impl BenchTikvServer {
    pub fn new(env: Arc<Environment>) -> BenchTikvServer {
        let h = BenchTikvHandler {
            running: Arc::new(Mutex::new(None)),
        };

        let channel_args = ChannelBuilder::new(env.clone())
            .stream_initial_window_size(DEFAULT_GRPC_STREAM_INITIAL_WINDOW_SIZE)
            .max_concurrent_stream(DEFAULT_GRPC_CONCURRENT_STREAM)
            .max_receive_message_len(MAX_GRPC_RECV_MSG_LEN)
            .max_send_message_len(MAX_GRPC_SEND_MSG_LEN)
            .build_args();

        let grpc_server = ServerBuilder::new(env.clone())
            .register_service(create_tikv(h.clone()))
            .bind("127.0.0.1", 0)
            .channel_args(channel_args)
            .build()
            .unwrap();

        let addr = {
            let (ref host, port) = grpc_server.bind_addrs()[0];
            SocketAddr::new(IpAddr::from_str(host).unwrap(), port as u16)
        };

        BenchTikvServer {
            server: grpc_server,
            local_addr: addr,
            handler: h,
            rx: None,
        }
    }

    pub fn start(&mut self) {
        self.server.start();
    }

    // TODO: addrs.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr.clone()
    }

    pub fn init_recording(&mut self, name: String) -> Result<(), String> {
        let (tx, rx) = std_channel();
        let counter = Arc::new(AtomicUsize::new(0));

        self.rx = Some(rx);
        self.handler.init_recording(name, counter, tx)
    }

    pub fn start_recording(&self) -> Result<(), String> {
        self.handler.start_recording()
    }

    pub fn stop_recording(&mut self) -> Result<usize, String> {
        self.rx.take();
        self.handler.stop_recording()
    }

    pub fn recv(&self) -> Result<(), String> {
        if self.rx.is_some() {
            self.rx.as_ref().unwrap().recv().map_err(|_| "channel has hung up".to_owned())
        } else {
            Err("Benchmark is not ready".to_owned())
        }
    }
}

// Imitate Conn in raft_clinet.rs
pub struct BenchTikvClient {
    stream: FutureSender<(RaftMessage, WriteFlags)>,
    _client: TikvClient,
    _close: OneShotSender<()>,
}

impl BenchTikvClient {
    pub fn new(env: Arc<Environment>, addr: SocketAddr) -> BenchTikvClient {
        let channel = ChannelBuilder::new(env)
            .stream_initial_window_size(DEFAULT_GRPC_STREAM_INITIAL_WINDOW_SIZE)
            .max_receive_message_len(MAX_GRPC_RECV_MSG_LEN)
            .max_send_message_len(MAX_GRPC_SEND_MSG_LEN)
            .connect(&format!("{}", addr));
        let client = TikvClient::new(channel);
        let (tx, rx) = future_channel(DEFAULT_GRPC_CONCURRENT_STREAM);
        let (tx_close, rx_close) = oneshot::channel();
        let (sink, _) = client.raft();
        client.spawn(rx_close.map_err(|_| ())
            .select(sink.sink_map_err(|_| ())
                .send_all(rx.map_err(|_| ()))
                .map(|_| ())
                .map_err(|_| ()))
            .map(|_| ())
            .map_err(|_| ()));

        BenchTikvClient {
            stream: tx,
            _client: client,
            _close: tx_close,
        }
    }

    pub fn raft_sink(&self) -> FutureSender<(RaftMessage, WriteFlags)> {
        self.stream.clone()
    }
}

pub fn new_bench_server() -> (Arc<Environment>, BenchTikvServer) {
    let env = Arc::new(Environment::new(DEFAULT_GRPC_CONCURRENCY));
    let mut server = BenchTikvServer::new(env.clone());
    server.start();

    // Sleep awhile for making sure server is ready.
    thread::sleep(Duration::from_secs(1));

    (env, server)
}

// TODO: Embedding perf.
// TODO: Stabilize result.
#[bench]
fn bench_raft_rpc(b: &mut Bencher) {
    let (env, mut server) = new_bench_server();
    let addr = server.local_addr();
    let client = BenchTikvClient::new(env, addr);

    server.init_recording("raft_client_streaming".to_owned()).unwrap();

    let sink = client.raft_sink();
    thread::spawn(move || {
        // // TODO: calc the precise count of msgs for ecah eventloop tick.
        let count = 1000;
        let buffered = vec![Ok((RaftMessage::new(), WriteFlags::default().buffer_hint(true)))];
        let flusher = vec![Ok((RaftMessage::new(), WriteFlags::default()))];
        let msgs = buffered.into_iter().cycle().take(count).chain(flusher);
        let source = stream::iter(msgs.cycle());

        let bench_raft = sink.send_all(source);
        let _ = bench_raft.wait();
    });

    // Warming up about 5 seconds.
    thread::sleep(Duration::from_secs(5));

    let start = Instant::now();
    server.start_recording().unwrap();
    b.iter(|| {
        server.recv().unwrap();
    });
    let duration = start.elapsed();
    let count = server.stop_recording().unwrap();

    // FIXME: do not ignore the extra nanoseconds.
    let qps = count as u64 / duration.as_secs();
    printf!("QPS: {:?}", qps);

    drop(client);
    drop(server);
}
