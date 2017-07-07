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
use std::time::Duration;
use std::net::{SocketAddr, IpAddr};
use std::sync::Arc;
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
use grpc::{WriteFlags, Server as GrpcServer, ServerBuilder, Environment,
           ChannelBuilder, RpcContext, UnarySink, RequestStream, ClientStreamingSink};

// Default settings used in TiKV.
const DEFAULT_GRPC_CONCURRENCY: usize = 4;
const DEFAULT_GRPC_CONCURRENT_STREAM: usize = 1024;
const DEFAULT_GRPC_RAFT_CONN_NUM: usize = 10;
const DEFAULT_GRPC_STREAM_INITIAL_WINDOW_SIZE: usize = 2 * 1024 * 1024;
const MAX_GRPC_RECV_MSG_LEN: usize = 10 * 1024 * 1024;
const MAX_GRPC_SEND_MSG_LEN: usize = 128 * 1024 * 1024;

#[derive(Clone)]
struct BenchTikvHandler {
    benching: Arc<AtomicUsize>,
    raft_tx: StdSender<()>,
}

impl Tikv for BenchTikvHandler {
    fn raft(&self,
            ctx: RpcContext,
            stream: RequestStream<RaftMessage>,
            _: ClientStreamingSink<Done>) {
        let raft_tx = self.raft_tx.clone();
        let benching = self.benching.clone();
        ctx.spawn(stream.for_each(move |_| {
                if 0 != benching.load(Ordering::SeqCst) {
                    raft_tx.send(()).unwrap();
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
}

// TODO: Import from TiKV
pub struct BenchTikvServer {
    server: GrpcServer,
    local_addr: SocketAddr,
    handler: BenchTikvHandler,
    raft_rx: StdReceiver<()>,
}

impl BenchTikvServer {
    pub fn new(env: Arc<Environment>) -> BenchTikvServer {
        let (tx, rx) = std_channel();
        let h = BenchTikvHandler {
            benching: Arc::new(AtomicUsize::new(0)),
            raft_tx: tx,
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

        let svr = BenchTikvServer {
            server: grpc_server,
            local_addr: addr,
            handler: h,
            raft_rx: rx,
        };

        svr
    }

    pub fn start(&mut self) {
        self.server.start();
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr.clone()
    }

    pub fn start_benching(&self) -> Arc<AtomicUsize> {
        self.handler.benching.clone()
    }

    pub fn recv(&self) -> Result<(), &'static str> {
        if 0 != self.handler.benching.load(Ordering::SeqCst) {
            self.raft_rx.recv().map_err(|_| "channel has hung up")
        } else {
            Err("Benchmark is not ready")
        }
    }
}

// TODO: Import from TiKV
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

// TODO: make it configurable.
// Plan:
//   1. [x] Make it work
//   2. [ ] RaftClient::new takes a pre-configured TikvClient.
//   3. [ ] tikv::server::Server::new takes a pre configured GrpcServer.
fn new_client_and_server() -> (BenchTikvClient, BenchTikvServer) {
    let env = Arc::new(Environment::new(DEFAULT_GRPC_CONCURRENCY));
    let mut server = BenchTikvServer::new(env.clone());
    server.start();

    // Sleep awhile for making sure server is raedy.
    thread::sleep(Duration::from_secs(1));

    let addr = server.local_addr();
    let client = BenchTikvClient::new(env, addr);

    (client, server)
}

// TODO: Proper shutdown client and server.
// TODO: Report QPS.
// TODO: Embedding perf.
#[bench]
fn bench_raft_rpc(b: &mut Bencher) {
    let (client, server) = new_client_and_server();
    let benching = server.start_benching();
    let sink = client.raft_sink();

    thread::spawn(move || {
        let msgs = vec![Ok((RaftMessage::new(), WriteFlags::default().buffer_hint(true)))];

        let bench_raft = sink.send_all(stream::iter(msgs.into_iter().cycle()));

        bench_raft.wait().unwrap();
    });

    // Warming up about 5 sec.
    thread::sleep(Duration::from_secs(5));
    benching.store(1, Ordering::SeqCst);
    b.iter(|| {
        server.recv().unwrap();
    })
}
