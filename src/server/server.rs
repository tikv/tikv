// Copyright 2016 PingCAP, Inc.
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

use std::sync::{Arc, RwLock};
use std::sync::mpsc::Sender;
use std::boxed::Box;
use std::net::{SocketAddr, IpAddr};
use std::str::FromStr;

use grpc::{Server as GrpcServer, ServerBuilder, Environment, ChannelBuilder};
use kvproto::tikvpb_grpc::*;
use util::worker::Worker;
use storage::Storage;
use raftstore::store::{SnapshotStatusMsg, SnapManager};

use super::{Result, Config};
use super::coprocessor::{EndPointHost, EndPointTask};
use super::grpc_service::Service;
use super::transport::{RaftStoreRouter, ServerTransport};
use super::resolve::StoreAddrResolver;
use super::snap::{Task as SnapTask, Runner as SnapHandler};
use super::raft_client::RaftClient;

const DEFAULT_COPROCESSOR_BATCH: usize = 50;

pub struct Server<T: RaftStoreRouter + 'static, S: StoreAddrResolver + 'static> {
    env: Arc<Environment>,
    // Grpc server.
    grpc_server: GrpcServer,
    local_addr: SocketAddr,
    // Transport.
    trans: ServerTransport<T, S>,
    raft_router: T,
    // The kv storage.
    storage: Storage,
    // For handling coprocessor requests.
    end_point_worker: Worker<EndPointTask>,
    // For sending/receiving snapshots.
    snap_mgr: SnapManager,
    snap_worker: Worker<SnapTask>,
}

impl<T: RaftStoreRouter, S: StoreAddrResolver + 'static> Server<T, S> {
    pub fn new(cfg: &Config,
               storage: Storage,
               raft_router: T,
               snapshot_status_sender: Sender<SnapshotStatusMsg>,
               resolver: S,
               snap_mgr: SnapManager)
               -> Result<Server<T, S>> {
        let env = Arc::new(Environment::new(cfg.grpc_concurrency));
        let raft_client = Arc::new(RwLock::new(RaftClient::new(env.clone())));
        let end_point_worker = Worker::new("end-point-worker");
        let snap_worker = Worker::new("snap-handler");

        let h = Service::new(storage.clone(),
                             end_point_worker.scheduler(),
                             raft_router.clone(),
                             snap_worker.scheduler());
        let addr = try!(SocketAddr::from_str(&cfg.addr));
        let ip = format!("{}", addr.ip());
        let channel_args = ChannelBuilder::new(env.clone())
            .max_concurrent_stream(cfg.grpc_concurrent_stream)
            .build_args();
        let grpc_server = try!(ServerBuilder::new(env.clone())
            .register_service(create_tikv(h))
            .bind(ip, addr.port())
            .channel_args(channel_args)
            .build());

        let addr = {
            let (ref host, port) = grpc_server.bind_addrs()[0];
            SocketAddr::new(try!(IpAddr::from_str(host)), port as u16)
        };

        let trans = ServerTransport::new(raft_client,
                                         snap_worker.scheduler(),
                                         raft_router.clone(),
                                         snapshot_status_sender,
                                         resolver);

        let svr = Server {
            env: env.clone(),
            grpc_server: grpc_server,
            local_addr: addr,
            trans: trans,
            raft_router: raft_router,
            storage: storage,
            end_point_worker: end_point_worker,
            snap_mgr: snap_mgr,
            snap_worker: snap_worker,
        };

        Ok(svr)
    }

    pub fn transport(&self) -> ServerTransport<T, S> {
        self.trans.clone()
    }

    pub fn start(&mut self, cfg: &Config) -> Result<()> {
        let end_point = EndPointHost::new(self.storage.get_engine(),
                                          self.end_point_worker.scheduler(),
                                          cfg.end_point_concurrency,
                                          cfg.end_point_txn_concurrency_on_busy,
                                          cfg.end_point_small_txn_tasks_limit);
        box_try!(self.end_point_worker.start_batch(end_point, DEFAULT_COPROCESSOR_BATCH));
        let snap_runner = SnapHandler::new(self.env.clone(),
                                           self.snap_mgr.clone(),
                                           self.raft_router.clone());
        box_try!(self.snap_worker.start(snap_runner));
        self.grpc_server.start();
        info!("TiKV is ready to serve");
        Ok(())
    }

    pub fn stop(&mut self) -> Result<()> {
        self.end_point_worker.stop();
        self.snap_worker.stop();
        if let Err(e) = self.storage.stop() {
            error!("failed to stop store: {:?}", e);
        }
        self.grpc_server.shutdown();
        Ok(())
    }

    // Return listening address, this may only be used for outer test
    // to get the real address because we may use "127.0.0.1:0"
    // in test to avoid port conflict.
    pub fn listening_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::sync::{Arc, Mutex};
    use std::sync::mpsc::{self, Sender};
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use super::super::{Result, Config};
    use super::super::transport::RaftStoreRouter;
    use super::super::resolve::{StoreAddrResolver, Callback as ResolveCallback};
    use storage::Storage;
    use kvproto::raft_serverpb::RaftMessage;
    use raftstore::Result as RaftStoreResult;
    use raftstore::store::Msg as StoreMsg;
    use raftstore::store::transport::Transport;

    struct MockResolver {
        addr: Arc<Mutex<Option<SocketAddr>>>,
    }

    impl StoreAddrResolver for MockResolver {
        fn resolve(&self, _: u64, cb: ResolveCallback) -> Result<()> {
            cb.call_box((self.addr.lock().unwrap().ok_or(box_err!("not set")),));
            Ok(())
        }
    }

    #[derive(Clone)]
    struct TestRaftStoreRouter {
        tx: Sender<usize>,
        report_unreachable_count: Arc<AtomicUsize>,
    }

    impl TestRaftStoreRouter {
        fn new(tx: Sender<usize>) -> TestRaftStoreRouter {
            TestRaftStoreRouter {
                tx: tx,
                report_unreachable_count: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl RaftStoreRouter for TestRaftStoreRouter {
        fn send(&self, _: StoreMsg) -> RaftStoreResult<()> {
            self.tx.send(1).unwrap();
            Ok(())
        }

        fn try_send(&self, _: StoreMsg) -> RaftStoreResult<()> {
            self.tx.send(1).unwrap();
            Ok(())
        }

        fn report_unreachable(&self, _: u64, _: u64, _: u64) -> RaftStoreResult<()> {
            let count = self.report_unreachable_count.clone();
            count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn test_peer_resolve() {
        let mut cfg = Config::new();
        cfg.addr = "127.0.0.1:0".to_owned();

        let mut storage = Storage::new(&cfg.storage).unwrap();
        storage.start(&cfg.storage).unwrap();

        let (tx, rx) = mpsc::channel();
        let router = TestRaftStoreRouter::new(tx);
        let report_unreachable_count = router.report_unreachable_count.clone();
        let (snapshot_status_sender, _) = mpsc::channel();

        let addr = Arc::new(Mutex::new(None));
        let mut server =
            Server::new(&cfg,
                        storage,
                        router,
                        snapshot_status_sender,
                        MockResolver { addr: addr.clone() },
                        SnapManager::new("", None, cfg.raft_store.use_sst_file_snapshot))
                .unwrap();
        *addr.lock().unwrap() = Some(server.listening_addr());

        server.start(&cfg).unwrap();

        let trans = server.transport();
        for i in 0..10 {
            if i % 2 == 1 {
                trans.report_unreachable(RaftMessage::new());
            }
            assert_eq!(report_unreachable_count.load(Ordering::SeqCst), (i + 1) / 2);
        }
        trans.send(RaftMessage::new());
        rx.recv().unwrap();
        server.stop().unwrap();
    }
}
