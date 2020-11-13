// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::i32;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::compat::Stream01CompatExt;
use futures::stream::StreamExt;
use grpcio::{ChannelBuilder, Environment, ResourceQuota, Server as GrpcServer, ServerBuilder};
use kvproto::tikvpb::*;
use tokio::runtime::{Builder as RuntimeBuilder, Handle as RuntimeHandle, Runtime};
use tokio_timer::timer::Handle;

use crate::coprocessor::Endpoint;
use crate::server::gc_worker::GcWorker;
use crate::storage::lock_manager::LockManager;
use crate::storage::{Engine, Storage};
use engine_rocks::RocksEngine;
use raftstore::router::{RaftPeerRouter, RaftStoreRouter};
use raftstore::store::SnapManager;
use security::SecurityManager;
use tikv_util::timer::GLOBAL_TIMER_HANDLE;
use tikv_util::worker::{LazyWorker, Worker};
use tikv_util::Either;

use super::load_statistics::*;
use super::raft_client::{ConnectionBuilder, RaftClient};
use super::resolve::StoreAddrResolver;
use super::service::*;
use super::snap::{Runner as SnapHandler, Task as SnapTask};
use super::transport::ServerTransport;
use super::{Config, Result};
use crate::read_pool::ReadPool;

const LOAD_STATISTICS_SLOTS: usize = 4;
const LOAD_STATISTICS_INTERVAL: Duration = Duration::from_millis(100);
pub const GRPC_THREAD_PREFIX: &str = "grpc-server";
pub const READPOOL_NORMAL_THREAD_PREFIX: &str = "store-read-norm";
pub const STATS_THREAD_PREFIX: &str = "transport-stats";

/// The TiKV server
///
/// It hosts various internal components, including gRPC, the raftstore router
/// and a snapshot worker.
pub struct Server<S: StoreAddrResolver + 'static> {
    env: Arc<Environment>,
    /// A GrpcServer builder or a GrpcServer.
    ///
    /// If the listening port is configured, the server will be started lazily.
    builder_or_server: Option<Either<ServerBuilder, GrpcServer>>,
    local_addr: SocketAddr,
    // Transport.
    trans: ServerTransport<S>,
    raft_router: Box<dyn RaftPeerRouter>,
    // For sending/receiving snapshots.
    snap_mgr: SnapManager,
    snap_worker: LazyWorker<SnapTask>,

    // Currently load statistics is done in the thread.
    stats_pool: Option<Runtime>,
    grpc_thread_load: Arc<ThreadLoad>,
    yatp_read_pool: Option<ReadPool>,
    readpool_normal_thread_load: Arc<ThreadLoad>,
    debug_thread_pool: Arc<Runtime>,
    timer: Handle,
}

impl<S: StoreAddrResolver + 'static> Server<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new<E: Engine, L: LockManager, T: RaftStoreRouter<RocksEngine> + Unpin>(
        cfg: &Arc<Config>,
        security_mgr: &Arc<SecurityManager>,
        storage: Storage<E, L>,
        cop: Endpoint<E>,
        raft_router: T,
        resolver: S,
        snap_mgr: SnapManager,
        gc_worker: GcWorker<E, T>,
        env: Arc<Environment>,
        yatp_read_pool: Option<ReadPool>,
        debug_thread_pool: Arc<Runtime>,
    ) -> Result<Self> {
        // A helper thread (or pool) for transport layer.
        let stats_pool = if cfg.stats_concurrency > 0 {
            Some(
                RuntimeBuilder::new()
                    .threaded_scheduler()
                    .thread_name(STATS_THREAD_PREFIX)
                    .core_threads(cfg.stats_concurrency)
                    .build()
                    .unwrap(),
            )
        } else {
            None
        };
        let grpc_thread_load = Arc::new(ThreadLoad::with_threshold(cfg.heavy_load_threshold));
        let readpool_normal_thread_load =
            Arc::new(ThreadLoad::with_threshold(cfg.heavy_load_threshold));

        let snap_worker = Worker::new("snap-handler");
        let lazy_worker = snap_worker.lazy_build("snap-handler");

        let kv_service = KvService::new(
            storage,
            gc_worker,
            cop,
            raft_router.clone(),
            lazy_worker.scheduler(),
            Arc::clone(&grpc_thread_load),
            Arc::clone(&readpool_normal_thread_load),
            cfg.enable_request_batch,
            security_mgr.clone(),
        );

        let addr = SocketAddr::from_str(&cfg.addr)?;
        let ip = format!("{}", addr.ip());
        let mem_quota = ResourceQuota::new(Some("ServerMemQuota"))
            .resize_memory(cfg.grpc_memory_pool_quota.0 as usize);
        let channel_args = ChannelBuilder::new(Arc::clone(&env))
            .stream_initial_window_size(cfg.grpc_stream_initial_window_size.0 as i32)
            .max_concurrent_stream(cfg.grpc_concurrent_stream)
            .max_receive_message_len(-1)
            .set_resource_quota(mem_quota)
            .max_send_message_len(-1)
            .http2_max_ping_strikes(i32::MAX) // For pings without data from clients.
            .keepalive_time(cfg.grpc_keepalive_time.into())
            .keepalive_timeout(cfg.grpc_keepalive_timeout.into())
            .build_args();
        let builder = {
            let mut sb = ServerBuilder::new(Arc::clone(&env))
                .channel_args(channel_args)
                .register_service(create_tikv(kv_service));
            sb = security_mgr.bind(sb, &ip, addr.port());
            Either::Left(sb)
        };

        let conn_builder = ConnectionBuilder::new(
            env.clone(),
            cfg.clone(),
            security_mgr.clone(),
            resolver,
            Box::new(raft_router.clone()),
            lazy_worker.scheduler(),
        );
        let raft_client = RaftClient::new(conn_builder);

        let trans = ServerTransport::new(raft_client);

        let svr = Server {
            env: Arc::clone(&env),
            builder_or_server: Some(builder),
            local_addr: addr,
            trans,
            raft_router: Box::new(raft_router),
            snap_mgr,
            snap_worker: lazy_worker,
            stats_pool,
            grpc_thread_load,
            yatp_read_pool,
            readpool_normal_thread_load,
            debug_thread_pool,
            timer: GLOBAL_TIMER_HANDLE.clone(),
        };

        Ok(svr)
    }

    pub fn get_debug_thread_pool(&self) -> &RuntimeHandle {
        self.debug_thread_pool.handle()
    }

    pub fn transport(&self) -> ServerTransport<S> {
        self.trans.clone()
    }

    /// Register a gRPC service.
    /// Register after starting, it fails and returns the service.
    pub fn register_service(&mut self, svc: grpcio::Service) -> Option<grpcio::Service> {
        match self.builder_or_server.take() {
            Some(Either::Left(mut builder)) => {
                builder = builder.register_service(svc);
                self.builder_or_server = Some(Either::Left(builder));
                None
            }
            Some(server) => {
                self.builder_or_server = Some(server);
                Some(svc)
            }
            None => Some(svc),
        }
    }

    /// Build gRPC server and bind to address.
    pub fn build_and_bind(&mut self) -> Result<SocketAddr> {
        let sb = self.builder_or_server.take().unwrap().left().unwrap();
        let server = sb.build()?;
        let (host, port) = server.bind_addrs().next().unwrap();
        let addr = SocketAddr::new(IpAddr::from_str(host)?, port);
        self.local_addr = addr;
        self.builder_or_server = Some(Either::Right(server));
        Ok(addr)
    }

    /// Starts the TiKV server.
    /// Notice: Make sure call `build_and_bind` first.
    pub fn start(&mut self, cfg: Arc<Config>, security_mgr: Arc<SecurityManager>) -> Result<()> {
        let snap_runner = SnapHandler::new(
            Arc::clone(&self.env),
            self.snap_mgr.clone(),
            self.raft_router.clone_box(),
            security_mgr,
            Arc::clone(&cfg),
        );
        self.snap_worker.start(snap_runner);

        let mut grpc_server = self.builder_or_server.take().unwrap().right().unwrap();
        info!("listening on addr"; "addr" => &self.local_addr);
        grpc_server.start();
        self.builder_or_server = Some(Either::Right(grpc_server));

        let mut grpc_load_stats = {
            let tl = Arc::clone(&self.grpc_thread_load);
            ThreadLoadStatistics::new(LOAD_STATISTICS_SLOTS, GRPC_THREAD_PREFIX, tl)
        };
        let mut readpool_normal_load_stats = {
            let tl = Arc::clone(&self.readpool_normal_thread_load);
            ThreadLoadStatistics::new(LOAD_STATISTICS_SLOTS, READPOOL_NORMAL_THREAD_PREFIX, tl)
        };
        let mut delay = self
            .timer
            .interval(Instant::now(), LOAD_STATISTICS_INTERVAL)
            .compat();
        if let Some(ref p) = self.stats_pool {
            p.spawn(async move {
                while let Some(Ok(i)) = delay.next().await {
                    grpc_load_stats.record(i);
                    readpool_normal_load_stats.record(i);
                }
            });
        };

        info!("TiKV is ready to serve");
        Ok(())
    }

    /// Stops the TiKV server.
    pub fn stop(&mut self) -> Result<()> {
        self.snap_worker.stop();
        if let Some(Either::Right(mut server)) = self.builder_or_server.take() {
            server.shutdown();
        }
        if let Some(pool) = self.stats_pool.take() {
            let _ = pool.shutdown_background();
        }
        let _ = self.yatp_read_pool.take();
        Ok(())
    }

    // Return listening address, this may only be used for outer test
    // to get the real address because we may use "127.0.0.1:0"
    // in test to avoid port conflict.
    pub fn listening_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

#[cfg(any(test, feature = "testexport"))]
pub mod test_router {
    use std::sync::mpsc::*;

    use super::*;

    use raftstore::store::*;
    use raftstore::Result as RaftStoreResult;

    use engine_rocks::RocksSnapshot;
    use engine_traits::{KvEngine, Snapshot};
    use kvproto::raft_serverpb::RaftMessage;
    use raft::SnapshotStatus;
    use raftstore::router::RaftPeerRouter;

    #[derive(Clone)]
    pub struct TestRaftStoreRouter {
        tx: Sender<usize>,
        significant_msg_sender: Sender<SignificantMsg<RocksSnapshot>>,
    }

    impl TestRaftStoreRouter {
        pub fn new(
            tx: Sender<usize>,
            significant_msg_sender: Sender<SignificantMsg<RocksSnapshot>>,
        ) -> TestRaftStoreRouter {
            TestRaftStoreRouter {
                tx,
                significant_msg_sender,
            }
        }
    }

    impl StoreRouter for TestRaftStoreRouter {
        fn send(&self, _: StoreMsg) -> RaftStoreResult<()> {
            self.tx.send(1).unwrap();
            Ok(())
        }
    }

    impl<S: Snapshot> ProposalRouter<S> for TestRaftStoreRouter {
        fn send(
            &self,
            _: RaftCommand<S>,
        ) -> std::result::Result<(), crossbeam::channel::TrySendError<RaftCommand<S>>> {
            self.tx.send(1).unwrap();
            Ok(())
        }
    }

    impl<EK: KvEngine> CasualRouter<EK> for TestRaftStoreRouter {
        fn send(&self, _: u64, _: CasualMessage<EK>) -> RaftStoreResult<()> {
            self.tx.send(1).unwrap();
            Ok(())
        }
    }

    impl RaftPeerRouter for TestRaftStoreRouter {
        fn send_raft_msg(&self, _: RaftMessage) -> RaftStoreResult<()> {
            self.tx.send(1).unwrap();
            Ok(())
        }

        fn clone_box(&self) -> Box<dyn RaftPeerRouter> {
            Box::new(self.clone())
        }

        fn report_snapshot_status(
            &self,
            region_id: u64,
            to_peer_id: u64,
            status: SnapshotStatus,
        ) -> RaftStoreResult<()> {
            let msg = SignificantMsg::SnapshotStatus {
                region_id,
                to_peer_id,
                status,
            };
            self.significant_send(region_id, msg)
        }

        fn report_unreachable(&self, region_id: u64, to_peer_id: u64) -> RaftStoreResult<()> {
            self.significant_send(
                region_id,
                SignificantMsg::Unreachable {
                    region_id,
                    to_peer_id,
                },
            )
        }
    }

    impl RaftStoreRouter<RocksEngine> for TestRaftStoreRouter {
        fn significant_send(
            &self,
            _: u64,
            msg: SignificantMsg<RocksSnapshot>,
        ) -> RaftStoreResult<()> {
            self.significant_msg_sender.send(msg).unwrap();
            Ok(())
        }

        fn broadcast_normal(&self, _: impl FnMut() -> PeerMsg<RocksEngine>) {
            self.tx.send(1).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::*;
    use std::sync::*;
    use std::time::Duration;

    use super::*;

    use super::super::resolve::{Callback as ResolveCallback, StoreAddrResolver};
    use super::super::{Config, Result};
    use crate::config::CoprReadPoolConfig;
    use crate::coprocessor::{self, readpool_impl};
    use crate::server::TestRaftStoreRouter;
    use crate::storage::TestStorageBuilder;
    use grpcio::EnvBuilder;
    use raftstore::store::transport::Transport;
    use raftstore::store::*;

    use crate::storage::lock_manager::DummyLockManager;
    use engine_rocks::{PerfLevel, RocksSnapshot};
    use kvproto::raft_serverpb::RaftMessage;
    use security::SecurityConfig;
    use tokio::runtime::Builder as TokioBuilder;

    #[derive(Clone)]
    struct MockResolver {
        quick_fail: Arc<AtomicBool>,
        addr: Arc<Mutex<Option<String>>>,
    }

    impl StoreAddrResolver for MockResolver {
        fn resolve(&self, _: u64, cb: ResolveCallback) -> Result<()> {
            if self.quick_fail.load(Ordering::SeqCst) {
                return Err(box_err!("quick fail"));
            }
            let addr = self.addr.lock().unwrap();
            cb(addr
                .as_ref()
                .map(|s| s.to_owned())
                .ok_or(box_err!("not set")));
            Ok(())
        }
    }

    fn is_unreachable_to(
        msg: &SignificantMsg<RocksSnapshot>,
        region_id: u64,
        to_peer_id: u64,
    ) -> bool {
        if let SignificantMsg::Unreachable {
            region_id: r_id,
            to_peer_id: p_id,
        } = *msg
        {
            region_id == r_id && to_peer_id == p_id
        } else {
            false
        }
    }

    // if this failed, unset the environmental variables 'http_proxy' and 'https_proxy', and retry.
    #[test]
    fn test_peer_resolve() {
        let mut cfg = Config::default();
        cfg.addr = "127.0.0.1:0".to_owned();

        let storage = TestStorageBuilder::new(DummyLockManager {})
            .build()
            .unwrap();

        let (tx, rx) = mpsc::channel();
        let (significant_msg_sender, significant_msg_receiver) = mpsc::channel();
        let router = TestRaftStoreRouter::new(tx, significant_msg_sender);
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(1)
                .name_prefix(thd_name!(GRPC_THREAD_PREFIX))
                .build(),
        );

        let mut gc_worker = GcWorker::new(
            storage.get_engine(),
            router.clone(),
            Default::default(),
            Default::default(),
        );
        gc_worker.start().unwrap();

        let quick_fail = Arc::new(AtomicBool::new(false));
        let cfg = Arc::new(cfg);
        let security_mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());

        let cop_read_pool = ReadPool::from(readpool_impl::build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            storage.get_engine(),
        ));
        let cop = coprocessor::Endpoint::new(
            &cfg,
            cop_read_pool.handle(),
            storage.get_concurrency_manager(),
            PerfLevel::EnableCount,
        );
        let debug_thread_pool = Arc::new(
            TokioBuilder::new()
                .threaded_scheduler()
                .thread_name(thd_name!("debugger"))
                .core_threads(1)
                .build()
                .unwrap(),
        );
        let addr = Arc::new(Mutex::new(None));
        let mut server = Server::new(
            &cfg,
            &security_mgr,
            storage,
            cop,
            router.clone(),
            MockResolver {
                quick_fail: Arc::clone(&quick_fail),
                addr: Arc::clone(&addr),
            },
            SnapManager::new(""),
            gc_worker,
            env,
            None,
            debug_thread_pool,
        )
        .unwrap();

        server.build_and_bind().unwrap();
        server.start(cfg, security_mgr).unwrap();

        let mut trans = server.transport();
        router.report_unreachable(0, 0).unwrap();
        let mut resp = significant_msg_receiver.try_recv().unwrap();
        assert!(is_unreachable_to(&resp, 0, 0), "{:?}", resp);

        let mut msg = RaftMessage::default();
        msg.set_region_id(1);
        trans.send(msg.clone()).unwrap();
        trans.flush();
        resp = significant_msg_receiver
            .recv_timeout(Duration::from_secs(3))
            .unwrap();
        assert!(is_unreachable_to(&resp, 1, 0), "{:?}", resp);

        *addr.lock().unwrap() = Some(format!("{}", server.listening_addr()));

        trans.send(msg.clone()).unwrap();
        trans.flush();
        assert!(rx.recv_timeout(Duration::from_secs(5)).is_ok());

        msg.mut_to_peer().set_store_id(2);
        msg.set_region_id(2);
        quick_fail.store(true, Ordering::SeqCst);
        trans.send(msg).unwrap();
        trans.flush();
        resp = significant_msg_receiver
            .recv_timeout(Duration::from_secs(3))
            .unwrap();
        assert!(is_unreachable_to(&resp, 2, 0), "{:?}", resp);
        server.stop().unwrap();
    }
}
