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
fn test_batch_raft_server_drop() {
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
            stream: RequestStream<BatchRaftMessage>,
            sink: ClientStreamingSink<Done>,
        ) {
            // let status = RpcStatus::new(RpcStatusCode::Unimplemented, None);
            // ctx.spawn(sink.fail(status).map_err(|_| ()));

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

    // Try to bind the mock server on a TCP port, and then do test.
    for i in 0..100 {
        let kv_service = MockKv(MockKvForRaft(Arc::clone(&counter)));
        let port = 50000 + i;
        let mut mock_server = match mock_kv_service(kv_service, "localhost", port) {
            Ok(s) => s,
            Err(_) => continue,
        };
        println!("port is {}",port);
        mock_server.start();

        let addr = format!("localhost:{}", port);

        (0..50).for_each(|_| {
            raft_client.send(1, &addr, RaftMessage::new()).unwrap();
           // if let Err(err) = raft_client.send(1, &addr, RaftMessage::new()).unwrap() {
           //     println!("{:?}",err), 
           // };
            thread::sleep(time::Duration::from_millis(10));
            raft_client.flush();
        });

        println!("counter {:?}",counter.load(Ordering::SeqCst));

       // thread::sleep(time::Duration::from_millis(1000));
        println!("drop the server");
        drop(mock_server);
        (50..100).for_each(|_| {
             raft_client.send(1, &addr, RaftMessage::new()).unwrap();
           // if let Err(err) = raft_client.send(1, &addr, RaftMessage::new()).unwrap() {
           //     println!("{:?}",err),
           // };
            thread::sleep(time::Duration::from_millis(10));
            raft_client.flush();
        });

        println!("counter {:?}",counter.load(Ordering::SeqCst));
        thread::sleep(time::Duration::from_millis(1000));
        println!("start server begin");
        
        for i in 0..100 {
            let kv_service = MockKv(MockKvForRaft(Arc::clone(&counter)));
            let port = 50000 + i;
            let mut mock_server = match mock_kv_service(kv_service, "localhost", port) {
                Ok(s) => s,
                Err(_) => continue,
            };
            println!("port is {}",port);
            mock_server.start();

            let addr = format!("localhost:{}", port);
            (100..150).for_each(| n | {
                       raft_client.send(1, &addr, RaftMessage::new()).unwrap();
                      // if let Err(err) = raft_client.send(1, &addr, RaftMessage::new()).unwrap() {
                      //      println!("{:?}",err),
                      // };
                       thread::sleep(time::Duration::from_millis(10));
                       raft_client.flush();
                       println!(" {} counter {}",n,counter.load(Ordering::SeqCst));
             });
             println!("counter {:?}",counter.load(Ordering::SeqCst));

             assert!(counter.load(Ordering::SeqCst) > 50);
             break;
        }
        assert!(counter.load(Ordering::SeqCst) >98);
        break;
    }
 pool.shutdown().wait().unwrap();
}

#[test]
fn test_batch_raft_network_isolation() {
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
            stream: RequestStream<BatchRaftMessage>,
            sink: ClientStreamingSink<Done>,
        ) {
            // let status = RpcStatus::new(RpcStatusCode::Unimplemented, None);
            // ctx.spawn(sink.fail(status).map_err(|_| ()));

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

    // Try to bind the mock server on a TCP port, and then do test.
    for i in 0..100 {
        let kv_service = MockKv(MockKvForRaft(Arc::clone(&counter)));
        let port = 60000 + i;
        let mut mock_server = match mock_kv_service(kv_service, "localhost", port) {
            Ok(s) => s,
            Err(_) => continue,
        };
        println!("port is {}",port);
        mock_server.start();

        let addr = format!("localhost:{}", port);

        (0..50).for_each(|_| {
            raft_client.send(1, &addr, RaftMessage::new()).unwrap();
            thread::sleep(time::Duration::from_millis(10));
            raft_client.flush();
        });

        println!("counter {:?}",counter.load(Ordering::SeqCst));
        println!("please close the port {}",port);
        // Write 60 seconds
        // Shut down the port during this time, simulate network isolation
        thread::sleep(time::Duration::from_secs(60));
        println!("work go on"); 
        (50..100).for_each(|_| {
             raft_client.send(1, &addr, RaftMessage::new()).unwrap(); 
            thread::sleep(time::Duration::from_millis(10));
            raft_client.flush();
        }); 

        // Counter must be 50.
        assert!(counter.load(Ordering::SeqCst) > 49);
        break;
    }
    pool.shutdown().wait().unwrap();
}
