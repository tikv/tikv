// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc,
    },
    thread, time,
    time::Duration,
};

use engine_rocks::RocksEngine;
use futures::{FutureExt, StreamExt, TryStreamExt};
use grpcio::{
    ClientStreamingSink, Environment, RequestStream, RpcContext, RpcStatus, RpcStatusCode, Server,
};
use kvproto::{
    metapb,
    raft_serverpb::{Done, RaftMessage},
    tikvpb::BatchRaftMessage,
};
use raft::eraftpb::Entry;
use raftstore::{
    errors::DiscardReason,
    router::{RaftStoreBlackHole, RaftStoreRouter},
};
use tikv::server::{
    self, load_statistics::ThreadLoadPool, resolve, resolve::Callback, Config, ConnectionBuilder,
    RaftClient, StoreAddrResolver, TestRaftStoreRouter,
};
use tikv_util::{
    config::VersionTrack,
    worker::{Builder as WorkerBuilder, LazyWorker},
};

use super::*;

#[derive(Clone)]
pub struct StaticResolver {
    port: u16,
}

impl StaticResolver {
    fn new(port: u16) -> StaticResolver {
        StaticResolver { port }
    }
}

impl StoreAddrResolver for StaticResolver {
    fn resolve(&self, _store_id: u64, cb: Callback) -> server::Result<()> {
        cb(Ok(format!("localhost:{}", self.port)));
        Ok(())
    }
}

fn get_raft_client<R, T>(router: R, resolver: T) -> RaftClient<T, R, RocksEngine>
where
    R: RaftStoreRouter<RocksEngine> + Unpin + 'static,
    T: StoreAddrResolver + 'static,
{
    let env = Arc::new(Environment::new(2));
    let cfg = Arc::new(VersionTrack::new(Config::default()));
    let security_mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());
    let worker = LazyWorker::new("test-raftclient");
    let loads = Arc::new(ThreadLoadPool::with_threshold(1000));
    let builder = ConnectionBuilder::new(
        env,
        cfg,
        security_mgr,
        resolver,
        router,
        worker.scheduler(),
        loads,
    );
    RaftClient::new(builder)
}

fn get_raft_client_by_port(
    port: u16,
) -> RaftClient<StaticResolver, RaftStoreBlackHole, RocksEngine> {
    get_raft_client(RaftStoreBlackHole, StaticResolver::new(port))
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

impl Tikv for MockKvForRaft {
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
            let status = RpcStatus::new(RpcStatusCode::UNIMPLEMENTED);
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

    let mut raft_client = get_raft_client_by_port(port);
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
    let mut raft_client = get_raft_client(router, StaticResolver::new(port));
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

    let mut raft_client = get_raft_client_by_port(port);

    // `send` should success.
    for _ in 0..10 {
        // 5M per RaftMessage.
        let mut raft_m = RaftMessage::default();
        for _ in 0..(5 * 1024) {
            let mut e = Entry::default();
            e.set_data(vec![b'a'; 1024].into());
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

/// In edge case that the estimated size may be inaccurate, we need to ensure connection
/// will not be broken in this case.
#[test]
fn test_batch_size_edge_limit() {
    let msg_count = Arc::new(AtomicUsize::new(0));
    let batch_msg_count = Arc::new(AtomicUsize::new(0));
    let service = MockKvForRaft::new(Arc::clone(&msg_count), Arc::clone(&batch_msg_count), true);
    let (mock_server, port) = create_mock_server(service, 60200, 60300).unwrap();

    let mut raft_client = get_raft_client_by_port(port);

    // Put them in buffer so sibling messages will be likely be batched during sending.
    let mut msgs = Vec::with_capacity(5);
    for _ in 0..5 {
        let mut raft_m = RaftMessage::default();
        // Magic number, this can make estimated size about 4940000, hence two messages will be
        // batched together, but the total size will be way largher than 10MiB as there are many
        // indexes and terms.
        for _ in 0..38000 {
            let mut e = Entry::default();
            e.set_term(1);
            e.set_index(256);
            e.set_data(vec![b'a'; 130].into());
            raft_m.mut_message().mut_entries().push(e);
        }
        msgs.push(raft_m);
    }
    for m in msgs {
        raft_client.send(m).unwrap();
    }
    raft_client.flush();

    check_msg_count(10000, &msg_count, 5);
    // The final received message count should be 5 exactly.
    drop(raft_client);
    drop(mock_server);
    assert_eq!(msg_count.load(Ordering::SeqCst), 5);
}

// Try to create a mock server with `service`. The server will be binded wiht a random
// port chosen between [`min_port`, `max_port`]. Return `None` if no port is available.
fn create_mock_server<T>(service: T, min_port: u16, max_port: u16) -> Option<(Server, u16)>
where
    T: Tikv + Clone + Send + 'static,
{
    for port in min_port..max_port {
        let kv = service.clone();
        let mut mock_server = match tikv_service(kv, "localhost", port) {
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
    T: Tikv + Clone + Send + 'static,
{
    let mut mock_server = match tikv_service(service, "localhost", port) {
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

/// Check if raft client can add tombstone stores in block list.
#[test]
fn test_tombstone_block_list() {
    let pd_server = test_pd::Server::new(1);
    let eps = pd_server.bind_addrs();
    let pd_client = Arc::new(test_pd::util::new_client(eps, None));
    let bg_worker = WorkerBuilder::new(thd_name!("background"))
        .thread_count(2)
        .create();
    let resolver =
        resolve::new_resolver::<_, _, RocksEngine>(pd_client, &bg_worker, RaftStoreBlackHole).0;

    let msg_count = Arc::new(AtomicUsize::new(0));
    let batch_msg_count = Arc::new(AtomicUsize::new(0));
    let service = MockKvForRaft::new(Arc::clone(&msg_count), Arc::clone(&batch_msg_count), true);
    let (_mock_server, port) = create_mock_server(service, 60200, 60300).unwrap();

    let mut raft_client = get_raft_client(RaftStoreBlackHole, resolver);

    let mut store1 = metapb::Store::default();
    store1.set_id(1);
    store1.set_address(format!("127.0.0.1:{}", port));
    pd_server.default_handler().add_store(store1.clone());

    // `send` should success.
    for _ in 0..10 {
        // 5M per RaftMessage.
        let mut raft_m = RaftMessage::default();
        raft_m.mut_to_peer().set_store_id(1);
        for _ in 0..(5 * 1024) {
            let mut e = Entry::default();
            e.set_data(vec![b'a'; 1024].into());
            raft_m.mut_message().mut_entries().push(e);
        }
        raft_client.send(raft_m).unwrap();
    }
    raft_client.flush();

    check_msg_count(500, &msg_count, 10);

    let mut store2 = metapb::Store::default();
    store2.set_id(2);
    store2.set_address(store1.get_address().to_owned());
    store2.set_state(metapb::StoreState::Tombstone);
    pd_server.default_handler().add_store(store2);
    let mut message = RaftMessage::default();
    message.mut_to_peer().set_store_id(2);
    // First message should be OK.
    raft_client.send(message.clone()).unwrap();
    // Wait some time for the resolve result.
    thread::sleep(time::Duration::from_millis(50));
    // Second message should fail as the store should be added to block list.
    assert_eq!(
        DiscardReason::Disconnected,
        raft_client.send(message).unwrap_err()
    );
}

#[test]
fn test_store_allowlist() {
    let pd_server = test_pd::Server::new(1);
    let eps = pd_server.bind_addrs();
    let pd_client = Arc::new(test_pd::util::new_client(eps, None));
    let bg_worker = WorkerBuilder::new(thd_name!("background"))
        .thread_count(2)
        .create();
    let resolver =
        resolve::new_resolver::<_, _, RocksEngine>(pd_client, &bg_worker, RaftStoreBlackHole).0;
    let mut raft_client = get_raft_client(RaftStoreBlackHole, resolver);

    let msg_count1 = Arc::new(AtomicUsize::new(0));
    let batch_msg_count1 = Arc::new(AtomicUsize::new(0));
    let service1 = MockKvForRaft::new(Arc::clone(&msg_count1), Arc::clone(&batch_msg_count1), true);
    let (_mock_server1, port1) = create_mock_server(service1, 60200, 60300).unwrap();

    let msg_count2 = Arc::new(AtomicUsize::new(0));
    let batch_msg_count2 = Arc::new(AtomicUsize::new(0));
    let service2 = MockKvForRaft::new(Arc::clone(&msg_count2), Arc::clone(&batch_msg_count2), true);
    let (_mock_server2, port2) = create_mock_server(service2, 60300, 60400).unwrap();

    let mut store1 = metapb::Store::default();
    store1.set_id(1);
    store1.set_address(format!("127.0.0.1:{}", port1));
    pd_server.default_handler().add_store(store1.clone());

    let mut store2 = metapb::Store::default();
    store2.set_id(2);
    store2.set_address(format!("127.0.0.1:{}", port2));
    pd_server.default_handler().add_store(store2.clone());

    for _ in 0..10 {
        let mut raft_m = RaftMessage::default();
        raft_m.mut_to_peer().set_store_id(1);
        raft_client.send(raft_m).unwrap();
    }
    raft_client.flush();
    check_msg_count(500, &msg_count1, 10);

    raft_client.set_store_allowlist(vec![2, 3]);
    for _ in 0..3 {
        let mut raft_m = RaftMessage::default();
        raft_m.mut_to_peer().set_store_id(1);
        assert!(raft_client.send(raft_m).is_err());
    }
    for _ in 0..5 {
        let mut raft_m = RaftMessage::default();
        raft_m.mut_to_peer().set_store_id(2);
        raft_client.send(raft_m).unwrap();
    }
    raft_client.flush();
    check_msg_count(500, &msg_count1, 10);
    check_msg_count(500, &msg_count2, 5);
}
