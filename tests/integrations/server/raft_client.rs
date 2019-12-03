// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::{thread, time};

use futures::{Future, Stream};
use grpcio::{
    ClientStreamingSink, Environment, RequestStream, RpcContext, RpcStatus, RpcStatusCode, Server,
};
use kvproto::raft_serverpb::{Done, RaftMessage};
use kvproto::tikvpb::BatchRaftMessage;
use tikv::raftstore::router::RaftStoreBlackHole;
use tikv::server::{load_statistics::ThreadLoad, Config, RaftClient};
use tikv_util::security::{SecurityConfig, SecurityManager};

use super::{mock_kv_service, MockKv, MockKvService};

pub fn get_raft_client(pool: &tokio_threadpool::ThreadPool) -> RaftClient<RaftStoreBlackHole> {
    let env = Arc::new(Environment::new(2));
    let cfg = Arc::new(Config::default());
    let security_mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());
    let grpc_thread_load = Arc::new(ThreadLoad::with_threshold(1000));
    RaftClient::new(
        env,
        cfg,
        security_mgr,
        RaftStoreBlackHole,
        grpc_thread_load,
        pool.sender().clone(),
    )
}

#[test]
fn test_batch_raft_fallback() {
    #[derive(Clone)]
    struct MockKvForRaft(Arc<AtomicUsize>);

    impl MockKvService for MockKvForRaft {
        fn raft(
            &mut self,
            ctx: RpcContext<'_>,
            stream: RequestStream<RaftMessage>,
            sink: ClientStreamingSink<Done>,
        ) {
            let counter = Arc::clone(&self.0);
            ctx.spawn(
                stream
                    .for_each(move |_| {
                        counter.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                    .map_err(|_| drop(sink)),
            );
        }

        fn batch_raft(
            &mut self,
            ctx: RpcContext<'_>,
            _stream: RequestStream<BatchRaftMessage>,
            sink: ClientStreamingSink<Done>,
        ) {
            let status = RpcStatus::new(RpcStatusCode::UNIMPLEMENTED, None);
            ctx.spawn(sink.fail(status).map_err(|_| ()));
        }
    }

    let pool = tokio_threadpool::Builder::new().pool_size(1).build();
    let mut raft_client = get_raft_client(&pool);
    let counter = Arc::new(AtomicUsize::new(0));

    let service = MockKvForRaft(Arc::clone(&counter));
    let (mock_server, port) = create_mock_server(service, 60000, 60100).unwrap();

    let addr = format!("localhost:{}", port);
    (0..100).for_each(|_| {
        raft_client.send(1, &addr, RaftMessage::default()).unwrap();
        thread::sleep(time::Duration::from_millis(10));
        raft_client.flush();
    });

    assert!(counter.load(Ordering::SeqCst) > 0);
    pool.shutdown().wait().unwrap();
    drop(mock_server)
}

#[test]
// Test raft_client auto reconnect to servers after connection break.
fn test_raft_client_reconnect() {
    #[derive(Clone)]
    struct MockKvForRaft(Arc<AtomicUsize>);

    impl MockKvService for MockKvForRaft {
        fn batch_raft(
            &mut self,
            ctx: RpcContext<'_>,
            stream: RequestStream<BatchRaftMessage>,
            sink: ClientStreamingSink<Done>,
        ) {
            let counter = Arc::clone(&self.0);
            ctx.spawn(
                stream
                    .for_each(move |msgs| {
                        let len = msgs.msgs.len();
                        counter.fetch_add(len, Ordering::SeqCst);
                        Ok(())
                    })
                    .map_err(|_| drop(sink)),
            );
        }
    }

    let pool = tokio_threadpool::Builder::new().pool_size(1).build();
    let mut raft_client = get_raft_client(&pool);
    let counter = Arc::new(AtomicUsize::new(0));

    let service = MockKvForRaft(Arc::clone(&counter));
    let (mock_server, port) = create_mock_server(service, 50000, 50300).unwrap();
    let addr = format!("localhost:{}", port);

    // `send` should success.
    (0..50).for_each(|_| raft_client.send(1, &addr, RaftMessage::default()).unwrap());
    raft_client.flush();

    check_f(300, || counter.load(Ordering::SeqCst) == 50);

    // `send` should fail after the mock server stopped.
    drop(mock_server);

    let send = |_| {
        thread::sleep(time::Duration::from_millis(10));
        raft_client.send(1, &addr, RaftMessage::default())
    };
    assert!((0..100).map(send).collect::<Result<(), _>>().is_err());

    // `send` should success after the mock server restarted.
    let service = MockKvForRaft(Arc::clone(&counter));
    let mock_server = create_mock_server_on(service, port);
    (0..50).for_each(|_| raft_client.send(1, &addr, RaftMessage::default()).unwrap());
    raft_client.flush();

    check_f(300, || counter.load(Ordering::SeqCst) == 100);
    assert_eq!(counter.load(Ordering::SeqCst), 100);

    drop(mock_server);
    pool.shutdown().wait().unwrap();
}

// Try to create a mock server with `service`. The server will be binded wiht a random
// port chosen between [`min_port`, `max_port`]. Return `None` if no port is available.
fn create_mock_server<T>(service: T, min_port: u16, max_port: u16) -> Option<(Server, u16)>
where
    T: MockKvService + Clone + Send + 'static,
{
    for port in min_port..=max_port {
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

fn check_f<F: Fn() -> bool>(max_delay_ms: u64, f: F) {
    for _delay_ms in 0..max_delay_ms / 10 {
        if f() {
            return;
        }
        thread::sleep(time::Duration::from_millis(10));
    }
    panic!("raftclient flush time out");
}
