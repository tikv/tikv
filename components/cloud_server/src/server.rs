// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use futures::channel::oneshot::Canceled;
use std::error::Error as StdError;
use std::io::Error as IoError;
use std::net::AddrParseError;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{i32, result};

use api_version::KvFormat;
use futures::compat::Stream01CompatExt;
use futures::stream::StreamExt;
use grpcio::{ChannelBuilder, Environment, ResourceQuota, Server as GrpcServer, ServerBuilder};
use grpcio_health::{create_health, HealthService, ServingStatus};
use kvproto::tikvpb::*;
use tokio::runtime::{Builder as RuntimeBuilder, Handle as RuntimeHandle, Runtime};
use tokio_timer::timer::Handle;

use rfstore::router::RaftStoreRouter;
use security::SecurityManager;
use tikv::coprocessor::Endpoint;
use tikv::coprocessor_v2;
use tikv::server::{Config, Proxy};
use tikv::storage::lock_manager::LockManager;
use tikv::storage::Storage;
use tikv_util::config::VersionTrack;
use tikv_util::sys::{get_global_memory_usage, record_global_memory_usage};
use tikv_util::timer::GLOBAL_TIMER_HANDLE;
use tikv_util::Either;

use super::raft_client::{ConnectionBuilder, RaftClient};
use tikv::server::load_statistics::*;
use tikv::server::metrics::{MEMORY_USAGE_GAUGE, SERVER_INFO_GAUGE_VEC};

use super::transport::ServerTransport;
use crate::service::KvService;
use crate::RaftKv;
use tikv::read_pool::ReadPool;
use tikv::server::resolve::StoreAddrResolver;

use engine_traits::Error as EngineTraitError;
use grpcio::Error as GrpcError;
use hyper::Error as HttpError;
use openssl::error::ErrorStack as OpenSSLError;
use pd_client::Error as PdError;
use protobuf::ProtobufError;
use rfstore::Error as RaftServerError;
use thiserror::Error;
use tikv::storage::kv::Error as EngineError;
use tikv::storage::Error as StorageError;
use tikv_util::codec::Error as CodecError;

const LOAD_STATISTICS_SLOTS: usize = 4;
const LOAD_STATISTICS_INTERVAL: Duration = Duration::from_millis(100);
const MEMORY_USAGE_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
pub const GRPC_THREAD_PREFIX: &str = "grpc-server";
pub const READPOOL_NORMAL_THREAD_PREFIX: &str = "store-read-norm";
pub const STATS_THREAD_PREFIX: &str = "transport-stats";

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0:?}")]
    Other(#[from] Box<dyn StdError + Sync + Send>),

    // Following is for From other errors.
    #[error("{0:?}")]
    Io(#[from] IoError),

    #[error("{0}")]
    Protobuf(#[from] ProtobufError),

    #[error("{0:?}")]
    Grpc(#[from] GrpcError),

    #[error("{0:?}")]
    Codec(#[from] CodecError),

    #[error("{0:?}")]
    AddrParse(#[from] AddrParseError),

    #[error("{0:?}")]
    RaftServer(#[from] RaftServerError),

    #[error("{0:?}")]
    Engine(#[from] EngineError),

    #[error("{0:?}")]
    EngineTrait(#[from] EngineTraitError),

    #[error("{0:?}")]
    Storage(#[from] StorageError),

    #[error("{0:?}")]
    Pd(#[from] PdError),

    #[error("failed to poll from mpsc receiver")]
    Sink,

    #[error("{0:?}")]
    RecvError(#[from] Canceled),

    #[error("{0:?}")]
    Http(#[from] HttpError),

    #[error("{0:?}")]
    OpenSSL(#[from] OpenSSLError),
}

pub type Result<T> = result::Result<T, Error>;

/// The TiKV server
///
/// It hosts various internal components, including gRPC, the raftstore router
/// and a snapshot worker.
pub struct Server<T: RaftStoreRouter + 'static, S: StoreAddrResolver + 'static> {
    env: Arc<Environment>,
    /// A GrpcServer builder or a GrpcServer.
    ///
    /// If the listening port is configured, the server will be started lazily.
    builder_or_server: Option<Either<ServerBuilder, GrpcServer>>,
    local_addr: SocketAddr,
    // Transport.
    trans: ServerTransport<T, S>,
    raft_router: T,

    // Currently load statistics is done in the thread.
    stats_pool: Option<Runtime>,
    grpc_thread_load: Arc<ThreadLoadPool>,
    yatp_read_pool: Option<ReadPool>,
    debug_thread_pool: Arc<Runtime>,
    health_service: HealthService,
    timer: Handle,
}

impl<T: RaftStoreRouter + Unpin, S: StoreAddrResolver + 'static> Server<T, S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new<L: LockManager, F: KvFormat>(
        store_id: u64,
        cfg: &Arc<VersionTrack<Config>>,
        security_mgr: &Arc<SecurityManager>,
        storage: Storage<RaftKv, L, F>,
        copr: Endpoint<RaftKv>,
        copr_v2: coprocessor_v2::Endpoint,
        raft_router: T,
        resolver: S,
        env: Arc<Environment>,
        yatp_read_pool: Option<ReadPool>,
        debug_thread_pool: Arc<Runtime>,
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

        let proxy = Proxy::new(security_mgr.clone(), &env, Arc::new(cfg.value().clone()));
        let kv_service = KvService::new(
            store_id,
            storage,
            copr,
            copr_v2,
            raft_router.clone(),
            Arc::clone(&grpc_thread_load),
            cfg.value().enable_request_batch,
            proxy,
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
        let health_service = HealthService::default();
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

    pub fn transport(&self) -> ServerTransport<T, S> {
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
        let mut grpc_server = self.builder_or_server.take().unwrap().right().unwrap();
        info!("listening on addr"; "addr" => &self.local_addr);
        grpc_server.start();
        self.builder_or_server = Some(Either::Right(grpc_server));

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
        if let Some(Either::Right(mut server)) = self.builder_or_server.take() {
            server.shutdown();
        }
        if let Some(pool) = self.stats_pool.take() {
            let _ = pool.shutdown_background();
        }
        let _ = self.yatp_read_pool.take();
        self.health_service
            .set_serving_status("", ServingStatus::NotServing);
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

    use rfstore::store::*;

    use kvproto::raft_serverpb::RaftMessage;

    #[derive(Clone)]
    pub struct TestRaftStoreRouter {
        tx: Sender<usize>,
        significant_msg_sender: Sender<SignificantMsg>,
    }

    impl TestRaftStoreRouter {
        pub fn new(
            tx: Sender<usize>,
            significant_msg_sender: Sender<SignificantMsg>,
        ) -> TestRaftStoreRouter {
            TestRaftStoreRouter {
                tx,
                significant_msg_sender,
            }
        }
    }

    impl StoreRouter for TestRaftStoreRouter {
        fn send(&self, _: StoreMsg) {
            let _ = self.tx.send(1);
        }
    }

    impl ProposalRouter for TestRaftStoreRouter {
        fn send(&self, _: RaftCommand) {
            let _ = self.tx.send(1);
        }
    }

    impl CasualRouter for TestRaftStoreRouter {
        fn send(&self, _: u64, _: CasualMessage) {
            let _ = self.tx.send(1);
        }
    }

    impl RaftStoreRouter for TestRaftStoreRouter {
        fn send_raft_msg(&self, _: RaftMessage) {
            let _ = self.tx.send(1);
        }

        fn significant_send(&self, _: u64, msg: SignificantMsg) {
            let _ = self.significant_msg_sender.send(msg);
        }
    }
}
