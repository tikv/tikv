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
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};

use kvproto::tikvpb_grpc::*;
use kvproto::coprocessor::{Request, Response};
use kvproto::raft_serverpb::{RaftMessage, Done, SnapshotChunk};
use kvproto::kvrpcpb::*;
use kvproto::metapb::*;

use futures::{Future, Stream, future, stream};
use futures::sync::oneshot;
use grpc::{Server as GrpcServer, ServerBuilder, Environment, ChannelBuilder, RpcContext,
           UnarySink, RequestStream, ClientStreamingSink};
use crossbeam::sync::MsQueue;

use tikv::server::{RaftClient, Config};

use super::print_result;

// Copy from std::time;
const NANOS_PER_SEC: u32 = 1_000_000_000;

struct Inner {
    start: AtomicBool,
    counter: AtomicUsize,
    // useful in bench.iter()
    forwarder: Arc<MsQueue<()>>,
}

#[derive(Clone)]
struct BenchTikvHandler {
    inner: Arc<Inner>,
}

impl BenchTikvHandler {
    fn new(forwarder: Arc<MsQueue<()>>) -> Self {
        BenchTikvHandler {
            inner: Arc::new(Inner {
                start: AtomicBool::new(false),
                counter: AtomicUsize::new(0),
                forwarder: forwarder,
            }),
        }
    }

    fn stop_recording(&self) -> usize {
        self.inner.start.store(false, Ordering::Release);
        self.inner.counter.load(Ordering::Acquire)
    }

    fn start_recording(&self) {
        self.inner.start.store(true, Ordering::Release);
    }
}

impl Tikv for BenchTikvHandler {
    fn raft(&self,
            ctx: RpcContext,
            stream: RequestStream<RaftMessage>,
            _: ClientStreamingSink<Done>) {
        let inner = self.inner.clone();

        ctx.spawn(stream.for_each(move |_| {
                if inner.start.load(Ordering::Acquire) {
                    inner.counter.fetch_add(1, Ordering::Release);
                    inner.forwarder.push(());
                }
                future::ok(())
            })
            .map_err(|_| ())
            .then(|_| future::ok::<_, ()>(())));
    }

    fn kv_get(&self, ctx: RpcContext, _: GetRequest, sink: UnarySink<GetResponse>) {
        let inner = self.inner.clone();
        let mut resp = GetResponse::new();
        resp.set_value(b"something".to_vec());

        ctx.spawn(sink.success(resp)
            .then(move |_| {
                if inner.start.load(Ordering::Acquire) {
                    inner.counter.fetch_add(1, Ordering::AcqRel);
                    inner.forwarder.push(());
                }
                future::ok::<_, ()>(())
            }));
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

    fn kv_delete_range(&self,
                       _: RpcContext,
                       _: DeleteRangeRequest,
                       _: UnarySink<DeleteRangeResponse>) {
        unimplemented!()
    }
}

// Default settings used in TiKV.
const DEFAULT_GRPC_CONCURRENCY: usize = 4;
const DEFAULT_GRPC_CONCURRENT_STREAM: usize = 1024;
const DEFAULT_GRPC_STREAM_INITIAL_WINDOW_SIZE: usize = 2 * 1024 * 1024;
const SERVER_MAX_GRPC_RECV_MSG_LEN: usize = 10 * 1024 * 1024;
const SERVER_MAX_GRPC_SEND_MSG_LEN: usize = 128 * 1024 * 1024;
const CLIENT_MAX_GRPC_RECV_MSG_LEN: usize = 10 * 1024 * 1024;
const CLIENT_MAX_GRPC_SEND_MSG_LEN: usize = 10 * 1024 * 1024;

/// A mock `TiKV` server for benching purpose, all `GrpcServer`
/// configuration MUST be consist with the real server in
/// `src/server/server.rs`.
pub struct BenchTikvServer {
    server: GrpcServer,
    local_addr: SocketAddr,
    handler: BenchTikvHandler,

    // Coupled with forwarder in `BenchTikvHandler`.
    rx: Option<Arc<MsQueue<()>>>,
}

impl BenchTikvServer {
    pub fn new(env: Arc<Environment>) -> BenchTikvServer {
        let channel_args = ChannelBuilder::new(env.clone())
            .stream_initial_window_size(DEFAULT_GRPC_STREAM_INITIAL_WINDOW_SIZE)
            .max_concurrent_stream(DEFAULT_GRPC_CONCURRENT_STREAM)
            .max_receive_message_len(SERVER_MAX_GRPC_RECV_MSG_LEN)
            .max_send_message_len(SERVER_MAX_GRPC_SEND_MSG_LEN)
            .build_args();

        let queue = Arc::new(MsQueue::new());
        let h = BenchTikvHandler::new(queue.clone());

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
            rx: Some(queue),
        }
    }

    pub fn start(&mut self) {
        self.server.start();
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn start_recording(&self) {
        self.handler.start_recording()
    }

    pub fn stop_recording(&mut self) -> usize {
        self.rx.take();
        self.handler.stop_recording()
    }

    pub fn recv(&self) -> Result<(), String> {
        if let Some(ref rx) = self.rx {
            rx.pop();
            Ok(())
        } else {
            Err("Benchmark is not running".to_owned())
        }
    }
}

fn new_bench_server() -> (Arc<Environment>, BenchTikvServer) {
    let env = Arc::new(Environment::new(DEFAULT_GRPC_CONCURRENCY));
    let mut server = BenchTikvServer::new(env.clone());
    server.start();

    // Sleep awhile for making sure server is ready.
    thread::sleep(Duration::from_secs(1));

    (env, server)
}

fn run_bench<F, C>(name: &'static str, run: F, clean: C)
    where F: FnOnce(Arc<Environment>, &mut BenchTikvServer),
          C: FnOnce()
{
    printf!("benching RPC on {}\t...", name);

    let (env, mut server) = new_bench_server();

    run(env, &mut server);

    // Warming up about 5 seconds.
    thread::sleep(Duration::from_secs(5));

    let start = Instant::now();
    server.start_recording();
    let result = bench!{
        server.recv().unwrap()
    };
    let count = server.stop_recording();
    let duration = start.elapsed();

    let qps = count as f64 /
              (duration.as_secs() as f64 + duration.subsec_nanos() as f64 / NANOS_PER_SEC as f64);
    printf!("\tQPS: {:.1}", qps);
    print_result(result);

    clean();
    drop(server)
}

fn bench_kv_get_rpc() {
    let name = "kv_get_unary";
    let (tx_close, rx_close) = oneshot::channel();
    let clean = move || {
        drop(tx_close);
    };
    let run = move |env: Arc<Environment>, server: &mut BenchTikvServer| {
        let mut get = GetRequest::new();
        let mut ctx = Context::new();
        ctx.set_region_id(1);
        let mut epoch = RegionEpoch::new();
        epoch.set_conf_ver(1);
        epoch.set_version(1);
        ctx.set_region_epoch(epoch);
        let mut peer = Peer::new();
        peer.set_id(1);
        peer.set_store_id(1);
        ctx.set_peer(peer);
        ctx.set_term(1);
        get.set_context(ctx);

        get.set_key(b"key".to_vec());
        get.set_version(1);
        let sample = vec![Ok(get)];

        // Those settings are used by RaftClient.
        let channel = ChannelBuilder::new(env)
            .stream_initial_window_size(DEFAULT_GRPC_STREAM_INITIAL_WINDOW_SIZE)
            .max_receive_message_len(CLIENT_MAX_GRPC_RECV_MSG_LEN)
            .max_send_message_len(CLIENT_MAX_GRPC_SEND_MSG_LEN)
            .connect(&format!("{}", server.local_addr()));
        let client = TikvClient::new(channel);

        thread::spawn(move || {
            // If the relax_duration is too short, may cause oom.
            let mut count = 0;
            let relax = 1024;
            let relax_duration = 10;

            let _ = rx_close.select(stream::iter(sample.into_iter().cycle()).for_each(|req| {
                    count += 1;
                    if count % relax == 0 {
                        thread::sleep(Duration::from_millis(relax_duration));
                    }
                    let req = client.kv_get_async(req).map(|_| ()).map_err(|_| ());
                    client.spawn(req);
                    Ok(())
                }))
                .wait();
        });
    };

    run_bench(name, run, clean);
}

fn bench_raft_rpc() {
    let name = "raft_client_streaming";
    let quit = Arc::new(AtomicBool::new(false));
    let quit1 = quit.clone();
    let clean = move || {
        quit1.store(true, Ordering::Release);
    };
    let run = move |env: Arc<Environment>, server: &mut BenchTikvServer| {
        let mut sample = RaftMessage::new();

        let mut epoch = RegionEpoch::new();
        epoch.set_conf_ver(1);
        epoch.set_version(1);
        sample.set_region_epoch(epoch);

        let mut peer = Peer::new();
        peer.set_id(1);
        peer.set_store_id(1);
        sample.set_from_peer(peer.clone());
        sample.set_to_peer(peer);

        sample.set_start_key(b"aaaa".to_vec());
        sample.set_end_key(b"bbbb".to_vec());

        let addr = server.local_addr();
        let mut client = RaftClient::new(env, Config::default());

        thread::spawn(move || {
            // TODO: calc the precise duration for every eventloop tick.
            //       if the flush_duration is too short, may cause oom.
            let batch_size = 1024;
            let flush_duration = 1;
            let mut count = 0;
            loop {
                if quit.load(Ordering::Acquire) {
                    return;
                }
                // Differen region id for different Conn.
                let mut req = sample.clone();
                req.set_region_id(count);
                let _ = client.send(1, addr, req);
                count += 1;
                if count % batch_size == 0 {
                    client.flush();
                    thread::sleep(Duration::from_millis(flush_duration));
                }
            }
        });
    };

    run_bench(name, run, clean);
}

pub fn bench_rpc() {
    bench_raft_rpc();
    bench_kv_get_rpc();
}
