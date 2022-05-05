// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::i32;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use api_version::KvFormat;
use futures::compat::Stream01CompatExt;
use futures::stream::StreamExt;
use grpcio::{ChannelBuilder, Environment, ResourceQuota, Server as GrpcServer, ServerBuilder};
use grpcio_health::{create_health, HealthService, ServingStatus};
use kvproto::tikvpb::*;
use tokio::runtime::{Builder as RuntimeBuilder, Handle as RuntimeHandle, Runtime};
use tokio_timer::timer::Handle;

use crate::coprocessor::Endpoint;
use crate::coprocessor_v2;
use crate::server::gc_worker::GcWorker;
use crate::server::Proxy;
use crate::storage::lock_manager::LockManager;
use crate::storage::{Engine, Storage};
use raftstore::router::RaftStoreRouter;
use raftstore::store::{CheckLeaderTask, SnapManager};
use security::SecurityManager;
use tikv_util::config::VersionTrack;
use tikv_util::sys::{get_global_memory_usage, record_global_memory_usage};
use tikv_util::timer::GLOBAL_TIMER_HANDLE;
use tikv_util::worker::{LazyWorker, Scheduler, Worker};
use tikv_util::Either;

use super::load_statistics::*;
use super::metrics::{MEMORY_USAGE_GAUGE, SERVER_INFO_GAUGE_VEC};
use super::raft_client::{ConnectionBuilder, RaftClient};
use super::resolve::StoreAddrResolver;
use super::service::*;
use super::snap::{Runner as SnapHandler, Task as SnapTask};
use super::transport::ServerTransport;
use super::{Config, Error, Result};
use crate::read_pool::ReadPool;

const LOAD_STATISTICS_SLOTS: usize = 4;
const LOAD_STATISTICS_INTERVAL: Duration = Duration::from_millis(100);
const MEMORY_USAGE_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
pub const GRPC_THREAD_PREFIX: &str = "grpc-server";
pub const READPOOL_NORMAL_THREAD_PREFIX: &str = "store-read-norm";
pub const STATS_THREAD_PREFIX: &str = "transport-stats";

/// The TiKV server
///
/// It hosts various internal components, including gRPC, the raftstore router
/// and a snapshot worker.
pub struct Server<T: RaftStoreRouter<E::Local> + 'static, S: StoreAddrResolver + 'static, E: Engine>
{
    env: Arc<Environment>,
    /// A GrpcServer builder or a GrpcServer.
    ///
    /// If the listening port is configured, the server will be started lazily.
    builder_or_server: Option<Either<ServerBuilder, GrpcServer>>,
    local_addr: SocketAddr,
    // Transport.
    trans: ServerTransport<T, S, E::Local>,
    raft_router: T,
    // For sending/receiving snapshots.
    snap_mgr: SnapManager,
    snap_worker: LazyWorker<SnapTask>,

    // Currently load statistics is done in the thread.
    stats_pool: Option<Runtime>,
    grpc_thread_load: Arc<ThreadLoadPool>,
    yatp_read_pool: Option<ReadPool>,
    debug_thread_pool: Arc<Runtime>,
    health_service: HealthService,
    timer: Handle,
}

impl<T: RaftStoreRouter<E::Local> + Unpin, S: StoreAddrResolver + 'static, E: Engine>
    Server<T, S, E>
{
    #[allow(clippy::too_many_arguments)]
    pub fn new<L: LockManager, F: KvFormat>(
        store_id: u64,
        cfg: &Arc<VersionTrack<Config>>,
        security_mgr: &Arc<SecurityManager>,
        storage: Storage<E, L, F>,
        copr: Endpoint<E>,
        copr_v2: coprocessor_v2::Endpoint,
        raft_router: T,
        resolver: S,
        snap_mgr: SnapManager,
        gc_worker: GcWorker<E, T>,
        check_leader_scheduler: Scheduler<CheckLeaderTask>,
        env: Arc<Environment>,
        yatp_read_pool: Option<ReadPool>,
        debug_thread_pool: Arc<Runtime>,
        health_service: HealthService,
    ) -> Result<Self> {
        // A helper thread (or pool) for transport layer.
        let stats_pool = if cfg.value().stats_concurrency > 0 {
            Some(
                RuntimeBuilder::new_multi_thread()
                    .thread_name(STATS_THREAD_PREFIX)
                    .worker_threads(cfg.value().stats_concurrency)
                    .build()
                    .unwrap(),
            )
        } else {
            None
        };
        let grpc_thread_load = Arc::new(ThreadLoadPool::with_threshold(
            cfg.value().heavy_load_threshold,
        ));

        let snap_worker = Worker::new("snap-handler");
        let lazy_worker = snap_worker.lazy_build("snap-handler");

        let proxy = Proxy::new(security_mgr.clone(), &env, Arc::new(cfg.value().clone()));
        let kv_service = KvService::new(
            store_id,
            storage,
            gc_worker,
            copr,
            copr_v2,
            raft_router.clone(),
            lazy_worker.scheduler(),
            check_leader_scheduler,
            Arc::clone(&grpc_thread_load),
            cfg.value().enable_request_batch,
            proxy,
            cfg.value().reject_messages_on_memory_ratio,
        );

        let addr = SocketAddr::from_str(&cfg.value().addr)?;
        let ip = format!("{}", addr.ip());
        let mem_quota = ResourceQuota::new(Some("ServerMemQuota"))
            .resize_memory(cfg.value().grpc_memory_pool_quota.0 as usize);
        let channel_args = ChannelBuilder::new(Arc::clone(&env))
            .stream_initial_window_size(cfg.value().grpc_stream_initial_window_size.0 as i32)
            .max_concurrent_stream(cfg.value().grpc_concurrent_stream)
            .max_receive_message_len(-1)
            .set_resource_quota(mem_quota)
            .max_send_message_len(-1)
            .http2_max_ping_strikes(i32::MAX) // For pings without data from clients.
            .keepalive_time(cfg.value().grpc_keepalive_time.into())
            .keepalive_timeout(cfg.value().grpc_keepalive_timeout.into())
            .build_args();

        let builder = {
            let mut sb = ServerBuilder::new(Arc::clone(&env))
                .channel_args(channel_args)
                .register_service(create_tikv(kv_service))
                .register_service(create_health(health_service.clone()));
            sb = security_mgr.bind(sb, &ip, addr.port());
            Either::Left(sb)
        };

        let conn_builder = ConnectionBuilder::new(
            env.clone(),
            Arc::new(cfg.value().clone()),
            security_mgr.clone(),
            resolver,
            raft_router.clone(),
            lazy_worker.scheduler(),
            grpc_thread_load.clone(),
        );
        let raft_client = RaftClient::new(conn_builder);

        let trans = ServerTransport::new(raft_client);
        health_service.set_serving_status("", ServingStatus::NotServing);

        let svr = Server {
            env: Arc::clone(&env),
            builder_or_server: Some(builder),
            local_addr: addr,
            trans,
            raft_router,
            snap_mgr,
            snap_worker: lazy_worker,
            stats_pool,
            grpc_thread_load,
            yatp_read_pool,
            debug_thread_pool,
            health_service,
            timer: GLOBAL_TIMER_HANDLE.clone(),
        };

        Ok(svr)
    }

    pub fn get_debug_thread_pool(&self) -> &RuntimeHandle {
        self.debug_thread_pool.handle()
    }

    pub fn get_snap_worker_scheduler(&self) -> Scheduler<SnapTask> {
        self.snap_worker.scheduler()
    }

    pub fn transport(&self) -> ServerTransport<T, S, E::Local> {
        self.trans.clone()
    }

    pub fn env(&self) -> Arc<Environment> {
        self.env.clone()
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
    pub fn start(
        &mut self,
        cfg: Arc<VersionTrack<Config>>,
        security_mgr: Arc<SecurityManager>,
    ) -> Result<()> {
        let snap_runner = SnapHandler::new(
            Arc::clone(&self.env),
            self.snap_mgr.clone(),
            self.raft_router.clone(),
            security_mgr,
            Arc::clone(&cfg),
        );
        self.snap_worker.start(snap_runner);

        let mut grpc_server = self.builder_or_server.take().unwrap().right().unwrap();
        info!("listening on addr"; "addr" => &self.local_addr);
        grpc_server.start();
        self.builder_or_server = Some(Either::Right(grpc_server));

        // Note this should be called only after grpc server is started.
        let mut grpc_load_stats = {
            let tl = Arc::clone(&self.grpc_thread_load);
            ThreadLoadStatistics::new(LOAD_STATISTICS_SLOTS, GRPC_THREAD_PREFIX, tl)
        };
        if let Some(ref p) = self.stats_pool {
            let mut delay = self
                .timer
                .interval(Instant::now(), LOAD_STATISTICS_INTERVAL)
                .compat();
            p.spawn(async move {
                while let Some(Ok(i)) = delay.next().await {
                    grpc_load_stats.record(i);
                }
            });
            let mut delay = self
                .timer
                .interval(Instant::now(), MEMORY_USAGE_REFRESH_INTERVAL)
                .compat();
            p.spawn(async move {
                while let Some(Ok(_)) = delay.next().await {
                    record_global_memory_usage();
                    MEMORY_USAGE_GAUGE.set(get_global_memory_usage() as i64);
                }
            });
        };

        let startup_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Other(box_err!("Clock may have gone backwards")))?
            .as_secs();

        SERVER_INFO_GAUGE_VEC
            .with_label_values(&[
                &("v".to_owned() + env!("CARGO_PKG_VERSION")),
                option_env!("TIKV_BUILD_GIT_HASH").unwrap_or("None"),
            ])
            .set(startup_ts as i64);
        self.health_service
            .set_serving_status("", ServingStatus::Serving);

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
        self.health_service.shutdown();
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

    use engine_rocks::{RocksEngine, RocksSnapshot};
    use engine_traits::{KvEngine, Snapshot};
    use kvproto::raft_serverpb::RaftMessage;

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

    impl StoreRouter<RocksEngine> for TestRaftStoreRouter {
        fn send(&self, _: StoreMsg<RocksEngine>) -> RaftStoreResult<()> {
            let _ = self.tx.send(1);
            Ok(())
        }
    }

    impl<S: Snapshot> ProposalRouter<S> for TestRaftStoreRouter {
        fn send(
            &self,
            _: RaftCommand<S>,
        ) -> std::result::Result<(), crossbeam::channel::TrySendError<RaftCommand<S>>> {
            let _ = self.tx.send(1);
            Ok(())
        }
    }

    impl<EK: KvEngine> CasualRouter<EK> for TestRaftStoreRouter {
        fn send(&self, _: u64, _: CasualMessage<EK>) -> RaftStoreResult<()> {
            let _ = self.tx.send(1);
            Ok(())
        }
    }

    impl SignificantRouter<RocksEngine> for TestRaftStoreRouter {
        fn significant_send(
            &self,
            _: u64,
            msg: SignificantMsg<RocksSnapshot>,
        ) -> RaftStoreResult<()> {
            let _ = self.significant_msg_sender.send(msg);
            Ok(())
        }
    }

    impl RaftStoreRouter<RocksEngine> for TestRaftStoreRouter {
        fn send_raft_msg(&self, _: RaftMessage) -> RaftStoreResult<()> {
            let _ = self.tx.send(1);
            Ok(())
        }

        fn broadcast_normal(&self, _: impl FnMut() -> PeerMsg<RocksEngine>) {
            let _ = self.tx.send(1);
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
    use crate::storage::lock_manager::DummyLockManager;
    use crate::storage::TestStorageBuilderApiV1;
    use engine_rocks::{PerfLevel, RocksSnapshot};
    use grpcio::EnvBuilder;
    use kvproto::raft_serverpb::RaftMessage;
    use raftstore::store::transport::Transport;
    use raftstore::store::*;
    use resource_metering::ResourceTagFactory;
    use security::SecurityConfig;
    use tikv_util::quota_limiter::QuotaLimiter;
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
        let cfg = Config {
            addr: "127.0.0.1:0".to_owned(),
            ..Default::default()
        };

        let storage = TestStorageBuilderApiV1::new(DummyLockManager)
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

        let (tx, _rx) = mpsc::channel();
        let mut gc_worker = GcWorker::new(
            storage.get_engine(),
            router.clone(),
            tx,
            Default::default(),
            Default::default(),
        );
        gc_worker.start().unwrap();

        let quick_fail = Arc::new(AtomicBool::new(false));
        let cfg = Arc::new(VersionTrack::new(cfg));
        let security_mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());

        let cop_read_pool = ReadPool::from(readpool_impl::build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            storage.get_engine(),
        ));
        let copr = coprocessor::Endpoint::new(
            &cfg.value().clone(),
            cop_read_pool.handle(),
            storage.get_concurrency_manager(),
            PerfLevel::EnableCount,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
        );
        let copr_v2 = coprocessor_v2::Endpoint::new(&coprocessor_v2::Config::default());
        let debug_thread_pool = Arc::new(
            TokioBuilder::new_multi_thread()
                .thread_name(thd_name!("debugger"))
                .worker_threads(1)
                .build()
                .unwrap(),
        );
        let mock_store_id = 5;
        let addr = Arc::new(Mutex::new(None));
        let (check_leader_scheduler, _) = tikv_util::worker::dummy_scheduler();
        let mut server = Server::new(
            mock_store_id,
            &cfg,
            &security_mgr,
            storage,
            copr,
            copr_v2,
            router.clone(),
            MockResolver {
                quick_fail: Arc::clone(&quick_fail),
                addr: Arc::clone(&addr),
            },
            SnapManager::new(""),
            gc_worker,
            check_leader_scheduler,
            env,
            None,
            debug_thread_pool,
            HealthService::default(),
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
