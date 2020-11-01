// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;
use std::{thread, time};

use engine_rocks::RocksEngine;
use futures::{FutureExt, StreamExt, TryStreamExt};
use grpcio::{
    ClientStreamingSink, Environment, RequestStream, RpcContext, RpcStatus, RpcStatusCode, Server,
};
use kvproto::raft_serverpb::{Done, RaftMessage};
use kvproto::tikvpb::BatchRaftMessage;
use raft::eraftpb::Entry;
use raftstore::router::{RaftStoreBlackHole, RaftStoreRouter};
use security::{SecurityConfig, SecurityManager};
use tikv::server::resolve::Callback;
use tikv::server::{
    self, Config, ConnectionBuilder, RaftClient, StoreAddrResolver, TestRaftStoreRouter,
};
use tikv_util::worker::LazyWorker;

use super::{mock_kv_service, MockKv, MockKvService};

#[derive(Clone)]
pub struct StaticResolver {
    port: u16,
}

impl StoreAddrResolver for StaticResolver {
    fn resolve(&self, _store_id: u64, cb: Callback) -> server::Result<()> {
        cb(Ok(format!("localhost:{}", self.port)));
        Ok(())
    }
}

pub fn get_raft_client_with_router<R>(
    router: R,
    port: u16,
) -> RaftClient<StaticResolver, R, RocksEngine>
where
    R: RaftStoreRouter<RocksEngine> + Unpin + 'static,
{
    let env = Arc::new(Environment::new(2));
    let cfg = Arc::new(Config::default());
    let security_mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());
    let resolver = StaticResolver { port };
    let worker = LazyWorker::new("test-raftclient");
    let builder =
        ConnectionBuilder::new(env, cfg, security_mgr, resolver, router, worker.scheduler());
    RaftClient::new(builder)
}

pub fn get_raft_client(port: u16) -> RaftClient<StaticResolver, RaftStoreBlackHole, RocksEngine> {
    get_raft_client_with_router(RaftStoreBlackHole, port)
}

#[derive(Clone)]
struct MockKvForRaft {
    msg_count: Arc<AtomicUsize>,
    batch_msg_count: Arc<AtomicUsize>,
    allow_batch: bool,
}

impl MockKvForRaft {
    fn new(
        msg_count: Arc<AtomicUsize>,
        batch_msg_count: Arc<AtomicUsize>,
        allow_batch: bool,
    ) -> Self {
        MockKvForRaft {
            msg_count,
            batch_msg_count,
            allow_batch,
        }
    }
}

impl MockKvService for MockKvForRaft {
    fn raft(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<RaftMessage>,
        sink: ClientStreamingSink<Done>,
    ) {
        let counter = Arc::clone(&self.msg_count);
        ctx.spawn(async move {
            stream
                .for_each(move |_| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    futures::future::ready(())
                })
                .await;
            drop(sink);
        });
    }

    fn batch_raft(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<BatchRaftMessage>,
        sink: ClientStreamingSink<Done>,
    ) {
        if !self.allow_batch {
            let status = RpcStatus::new(RpcStatusCode::UNIMPLEMENTED, None);
            ctx.spawn(sink.fail(status).map(|_| ()));
            return;
        }
        let msg_count = Arc::clone(&self.msg_count);
        let batch_msg_count = Arc::clone(&self.batch_msg_count);
        ctx.spawn(async move {
            stream
                .try_for_each(move |msgs| {
                    batch_msg_count.fetch_add(1, Ordering::SeqCst);
                    msg_count.fetch_add(msgs.msgs.len(), Ordering::SeqCst);
                    futures::future::ok(())
                })
                .await
                .unwrap();
            drop(sink);
        });
    }
}

#[test]
fn test_batch_raft_fallback() {
    let msg_count = Arc::new(AtomicUsize::new(0));
    let batch_msg_count = Arc::new(AtomicUsize::new(0));
    let service = MockKvForRaft::new(Arc::clone(&msg_count), Arc::clone(&batch_msg_count), false);
    let (mock_server, port) = create_mock_server(service, 60000, 60100).unwrap();

    let mut raft_client = get_raft_client(port);
    (0..100).for_each(|_| {
        raft_client.send(RaftMessage::default()).unwrap();
        thread::sleep(time::Duration::from_millis(10));
        raft_client.flush();
    });

    assert!(msg_count.load(Ordering::SeqCst) > 0);
    assert_eq!(batch_msg_count.load(Ordering::SeqCst), 0);
    drop(mock_server)
}

#[test]
// Test raft_client auto reconnect to servers after connection break.
fn test_raft_client_reconnect() {
    let msg_count = Arc::new(AtomicUsize::new(0));
    let batch_msg_count = Arc::new(AtomicUsize::new(0));
    let service = MockKvForRaft::new(Arc::clone(&msg_count), Arc::clone(&batch_msg_count), true);
    let (mut mock_server, port) = create_mock_server(service, 60100, 60200).unwrap();

    let (tx, rx) = mpsc::channel();
    let (significant_msg_sender, _significant_msg_receiver) = mpsc::channel();
    let router = TestRaftStoreRouter::new(tx, significant_msg_sender);
    let mut raft_client = get_raft_client_with_router(router, port);
    (0..50).for_each(|_| raft_client.send(RaftMessage::default()).unwrap());
    raft_client.flush();

    check_msg_count(500, &msg_count, 50);

    // `send` should be pending after the mock server stopped.
    mock_server.shutdown();
    drop(mock_server);

    rx.recv_timeout(Duration::from_secs(3)).unwrap();

    for _ in 0..100 {
        raft_client.send(RaftMessage::default()).unwrap();
    }
    raft_client.flush();
    rx.recv_timeout(Duration::from_secs(3)).unwrap();

    // `send` should success after the mock server restarted.
    let service = MockKvForRaft::new(Arc::clone(&msg_count), batch_msg_count, true);
    let mock_server = create_mock_server_on(service, port);
    (0..50).for_each(|_| raft_client.send(RaftMessage::default()).unwrap());
    raft_client.flush();

    check_msg_count(3000, &msg_count, 100);

    drop(mock_server);
}

#[test]
fn test_batch_size_limit() {
    let msg_count = Arc::new(AtomicUsize::new(0));
    let batch_msg_count = Arc::new(AtomicUsize::new(0));
    let service = MockKvForRaft::new(Arc::clone(&msg_count), Arc::clone(&batch_msg_count), true);
    let (mock_server, port) = create_mock_server(service, 60200, 60300).unwrap();

    let mut raft_client = get_raft_client(port);

    // `send` should success.
    for _ in 0..10 {
        // 5M per RaftMessage.
        let mut raft_m = RaftMessage::default();
        for _ in 0..(5 * 1024) {
            let mut e = Entry::default();
            e.set_data(vec![b'a'; 1024]);
            raft_m.mut_message().mut_entries().push(e);
        }
        raft_client.send(raft_m).unwrap();
    }
    raft_client.flush();

    check_msg_count(500, &msg_count, 10);
    // The final received message count should be 10 exactly.
    drop(raft_client);
    drop(mock_server);
    assert_eq!(msg_count.load(Ordering::SeqCst), 10);
}

// Try to create a mock server with `service`. The server will be binded wiht a random
// port chosen between [`min_port`, `max_port`]. Return `None` if no port is available.
fn create_mock_server<T>(service: T, min_port: u16, max_port: u16) -> Option<(Server, u16)>
where
    T: MockKvService + Clone + Send + 'static,
{
    for port in min_port..max_port {
        let kv = MockKv(service.clone());
        let mut mock_server = match mock_kv_service(kv, "localhost", port) {
            Ok(s) => s,
            Err(_) => continue,
        };
        mock_server.start();
        return Some((mock_server, port));
    }
    None
}

// Try to create a mock server with `service` and bind it with `port`.
// Return `None` is the port is unavailable.
fn create_mock_server_on<T>(service: T, port: u16) -> Option<Server>
where
    T: MockKvService + Clone + Send + 'static,
{
    let mut mock_server = match mock_kv_service(MockKv(service), "localhost", port) {
        Ok(s) => s,
        Err(_) => return None,
    };
    mock_server.start();
    Some(mock_server)
}

fn check_msg_count(max_delay_ms: u64, count: &AtomicUsize, expected: usize) {
    let mut got = 0;
    for _delay_ms in 0..max_delay_ms / 10 {
        got = count.load(Ordering::SeqCst);
        if got >= expected {
            return;
        }
        thread::sleep(time::Duration::from_millis(10));
    }
    panic!("check_msg_count wants {}, gets {}", expected, got);
}
