// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::{thread, time};

use futures::{Future, Stream};
use grpcio::*;
use kvproto::raft_serverpb::{Done, RaftMessage};
use kvproto::tikvpb::BatchRaftMessage;
use tikv::server::transport::RaftStoreBlackHole;
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
            let status = RpcStatus::new(RpcStatusCode::Unimplemented, None);
            ctx.spawn(sink.fail(status).map_err(|_| ()));
        }
    }

    let pool = tokio_threadpool::Builder::new().pool_size(1).build();
    let mut raft_client = get_raft_client(&pool);
    let counter = Arc::new(AtomicUsize::new(0));

    // Try to bind the mock server on a TCP port, and then do test.
    for i in 0..100 {
        let kv_service = MockKv(MockKvForRaft(Arc::clone(&counter)));
        let port = 60000 + i;
        let mut mock_server = match mock_kv_service(kv_service, "localhost", port) {
            Ok(s) => s,
            Err(_) => continue,
        };
        mock_server.start();

        let addr = format!("localhost:{}", port);
        (0..100).for_each(|_| {
            raft_client.send(1, &addr, RaftMessage::new()).unwrap();
            thread::sleep(time::Duration::from_millis(10));
            raft_client.flush();
        });

        assert!(counter.load(Ordering::SeqCst) > 0);
        break;
    }
    pool.shutdown().wait().unwrap();
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

    // Try to bind the mock server on a Tcp port and return the mock server and addr,
    // try up to 300 connections, otherwise cause panic.
    fn create_server_and_build(counter: Arc<AtomicUsize>) -> (Server, String) {
        for i in 0..300 {
            let kv_service = MockKv(MockKvForRaft(Arc::clone(&counter)));
            let port = 50000 + i;
            let mock_server = match mock_kv_service(kv_service, "localhost", port) {
                Ok(s) => s,
                Err(_) => continue,
            };
            return (mock_server, format!("localhost:{}", port));
        }
        panic!("server connect failed");
    }

    let pool = tokio_threadpool::Builder::new().pool_size(1).build();
    let mut raft_client = get_raft_client(&pool);
    let counter = Arc::new(AtomicUsize::new(0));

    // Try to get mock server and Tcp port, and then do test.
    let (mut mock_server, addr) = create_server_and_build(Arc::clone(&counter));
    mock_server.start();

    (0..50).for_each(|_| {
        raft_client.send(1, &addr, RaftMessage::new()).unwrap();
        raft_client.flush();
    });

    thread::sleep(time::Duration::from_millis(100));
    assert_eq!(counter.load(Ordering::SeqCst), 50);
    drop(mock_server);

    (50..100).for_each(|_| {
        raft_client.send(1, &addr, RaftMessage::new()).unwrap();
        raft_client.flush();
    });

    // Try to rebuild mock server and bind the new mock server on a TCP port,
    // and then do test.
    let (mut mock_server, addr) = create_server_and_build(Arc::clone(&counter));
    mock_server.start();

    (100..150).for_each(|_| {
        raft_client.send(1, &addr, RaftMessage::new()).unwrap();
        raft_client.flush();
    });

    thread::sleep(time::Duration::from_millis(100));
    assert_eq!(counter.load(Ordering::SeqCst), 99);

    pool.shutdown().wait().unwrap();
}
