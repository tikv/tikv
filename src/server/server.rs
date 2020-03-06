// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::i32;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{Future, Stream};
use grpcio::{
    ChannelBuilder, EnvBuilder, Environment, ResourceQuota, Server as GrpcServer, ServerBuilder,
};
use kvproto::tikvpb::*;
use tokio_threadpool::{Builder as ThreadPoolBuilder, ThreadPool};
use tokio_timer::timer::Handle;

use crate::coprocessor::Endpoint;
use crate::server::gc_worker::GcWorker;
use crate::storage::lock_manager::LockManager;
use crate::storage::{Engine, Storage};
use raftstore::router::RaftStoreRouter;
use raftstore::store::SnapManager;
use tikv_util::security::SecurityManager;
use tikv_util::timer::GLOBAL_TIMER_HANDLE;
use tikv_util::worker::Worker;
use tikv_util::Either;

use super::load_statistics::*;
use super::raft_client2::RaftStreamPool;
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
    // For sending/receiving snapshots.
    snap_mgr: SnapManager,
    snap_worker: Worker<SnapTask>,

    // Currently load statistics is done in the thread.
    stats_pool: Option<ThreadPool>,
    grpc_thread_load: Arc<ThreadLoad>,
    yatp_read_pool: Option<ReadPool>,
    readpool_normal_concurrency: usize,
    readpool_normal_thread_load: Arc<ThreadLoad>,
    timer: Handle,
}

impl<T: RaftStoreRouter, S: StoreAddrResolver + 'static> Server<T, S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new<E: Engine, L: LockManager>(
        cfg: &Arc<Config>,
        security_mgr: &Arc<SecurityManager>,
        storage: Storage<E, L>,
        cop: Endpoint<E>,
        raft_router: T,
        resolver: S,
        snap_mgr: SnapManager,
        gc_worker: GcWorker<E>,
        yatp_read_pool: Option<ReadPool>,
    ) -> Result<Self> {
        // A helper thread (or pool) for transport layer.
        let stats_pool = if cfg.stats_concurrency > 0 {
            Some(
                ThreadPoolBuilder::new()
                    .pool_size(cfg.stats_concurrency)
                    .name_prefix(STATS_THREAD_PREFIX)
                    .build(),
            )
        } else {
            None
        };
        let grpc_thread_load = Arc::new(ThreadLoad::with_threshold(cfg.heavy_load_threshold));
        let readpool_normal_concurrency = storage.readpool_normal_concurrency();
        let readpool_normal_thread_load =
            Arc::new(ThreadLoad::with_threshold(cfg.heavy_load_threshold));

        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(cfg.grpc_concurrency)
                .name_prefix(thd_name!(GRPC_THREAD_PREFIX))
                .build(),
        );
        let snap_worker = Worker::new("snap-handler");

        let kv_service = KvService::new(
            storage,
            gc_worker,
            cop,
            raft_router.clone(),
            snap_worker.scheduler(),
            Arc::clone(&grpc_thread_load),
            Arc::clone(&readpool_normal_thread_load),
            cfg.enable_request_batch,
            if cfg.enable_request_batch && cfg.request_batch_enable_cross_command {
                Some(Duration::from(cfg.request_batch_wait_duration))
            } else {
                None
            },
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

        let stream_pool = RaftStreamPool::new(
            env.clone(),
            cfg.clone(),
            security_mgr.clone(),
            resolver,
            raft_router.clone(),
            snap_worker.scheduler(),
        );
        let trans = ServerTransport::new(stream_pool);

        let svr = Server {
            env: Arc::clone(&env),
            builder_or_server: Some(builder),
            local_addr: addr,
            trans,
            raft_router,
            snap_mgr,
            snap_worker,
            stats_pool,
            grpc_thread_load,
            yatp_read_pool,
            readpool_normal_concurrency,
            readpool_normal_thread_load,
            timer: GLOBAL_TIMER_HANDLE.clone(),
        };

        Ok(svr)
    }

    pub fn transport(&self) -> ServerTransport<T, S> {
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
        let (ref host, port) = server.bind_addrs()[0];
        let addr = SocketAddr::new(IpAddr::from_str(host)?, port as u16);
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
            self.raft_router.clone(),
            security_mgr,
            Arc::clone(&cfg),
        );
        box_try!(self.snap_worker.start(snap_runner));

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
            let mut stats =
                ThreadLoadStatistics::new(LOAD_STATISTICS_SLOTS, READPOOL_NORMAL_THREAD_PREFIX, tl);
            stats.set_thread_target(self.readpool_normal_concurrency);
            stats
        };
        if let Some(ref p) = self.stats_pool {
            p.spawn(
                self.timer
                    .interval(Instant::now(), LOAD_STATISTICS_INTERVAL)
                    .map_err(|_| ())
                    .for_each(move |i| {
                        grpc_load_stats.record(i);
                        readpool_normal_load_stats.record(i);
                        Ok(())
                    }),
            )
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
            let _ = pool.shutdown_now().wait();
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::*;
    use std::sync::mpsc::*;
    use std::sync::*;
    use std::time::Duration;

    use super::*;

    use super::super::resolve::{Callback as ResolveCallback, StoreAddrResolver};
    use super::super::{Config, Result};
    use crate::config::CoprReadPoolConfig;
    use crate::coprocessor::{self, readpool_impl};
    use crate::storage::TestStorageBuilder;
    use raftstore::store::transport::Transport;
    use raftstore::store::*;
    use raftstore::Result as RaftStoreResult;

    use engine_rocks::RocksEngine;
    use kvproto::raft_cmdpb::RaftCmdRequest;
    use kvproto::raft_serverpb::RaftMessage;
    use tikv_util::security::SecurityConfig;

    #[allow(clippy::type_complexity)]
    #[derive(Clone)]
    struct MockResolver {
        quick_fail: Arc<AtomicBool>,
        addr: Arc<Mutex<Option<String>>>,
        delegate: Arc<Mutex<Option<Sender<(u64, ResolveCallback)>>>>,
    }

    impl StoreAddrResolver for MockResolver {
        fn resolve(&self, store_id: u64, cb: ResolveCallback) -> Result<()> {
            if self.quick_fail.load(Ordering::SeqCst) {
                return Err(box_err!("quick fail"));
            }
            if let Some(d) = self.delegate.lock().unwrap().as_ref() {
                let _ = d.send((store_id, cb));
                return Ok(());
            }
            let addr = self.addr.lock().unwrap();
            cb(addr
                .as_ref()
                .map(|s| s.to_owned())
                .ok_or(box_err!("not set")));
            Ok(())
        }
    }

    struct TestRaftStoreReceiver {
        rx: Receiver<RaftMessage>,
        significant_msg_receiver: Receiver<SignificantMsg>,
    }

    #[derive(Clone)]
    struct TestRaftStoreRouter {
        tx: Sender<RaftMessage>,
        significant_msg_sender: Sender<SignificantMsg>,
    }

    impl RaftStoreRouter for TestRaftStoreRouter {
        fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()> {
            self.tx.send(msg).unwrap();
            Ok(())
        }

        fn send_command(&self, _: RaftCmdRequest, _: Callback<RocksEngine>) -> RaftStoreResult<()> {
            unimplemented!()
        }

        fn significant_send(&self, _: u64, msg: SignificantMsg) -> RaftStoreResult<()> {
            self.significant_msg_sender.send(msg).unwrap();
            Ok(())
        }

        fn casual_send(&self, _: u64, _: CasualMessage<RocksEngine>) -> RaftStoreResult<()> {
            unimplemented!()
        }

        fn broadcast_unreachable(&self, _: u64) {}
    }

    fn is_unreachable_to(msg: &SignificantMsg, region_id: u64, to_peer_id: u64) -> bool {
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

    fn build_server() -> (
        TestRaftStoreReceiver,
        MockResolver,
        Server<TestRaftStoreRouter, MockResolver>,
    ) {
        let mut cfg = Config::default();
        cfg.addr = "127.0.0.1:0".to_owned();

        let storage = TestStorageBuilder::new().build().unwrap();
        let mut gc_worker =
            GcWorker::new(storage.get_engine(), None, None, None, Default::default());
        gc_worker.start().unwrap();

        let (tx, rx) = mpsc::channel();
        let (significant_msg_sender, significant_msg_receiver) = mpsc::channel();
        let router = TestRaftStoreRouter {
            tx,
            significant_msg_sender,
        };

        let quick_fail = Arc::new(AtomicBool::new(false));
        let cfg = Arc::new(cfg);
        let security_mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());

        let cop_read_pool = ReadPool::from(readpool_impl::build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            storage.get_engine(),
        ));
        let cop = coprocessor::Endpoint::new(&cfg, cop_read_pool.handle());

        let addr = Arc::new(Mutex::new(None));
        let resolver = MockResolver {
            quick_fail,
            addr,
            delegate: Arc::default(),
        };
        let mut server = Server::new(
            &cfg,
            &security_mgr,
            storage,
            cop,
            router,
            resolver.clone(),
            SnapManager::new("", None),
            gc_worker,
            None,
        )
        .unwrap();

        server.build_and_bind().unwrap();
        server.start(cfg, security_mgr).unwrap();

        (
            TestRaftStoreReceiver {
                rx,
                significant_msg_receiver,
            },
            resolver,
            server,
        )
    }

    // if this failed, unset the environmental variables 'http_proxy' and 'https_proxy', and retry.
    #[test]
    fn test_peer_resolve() {
        let (receiver, resolver, mut server) = build_server();
        let mut trans = server.transport();

        let mut msg = RaftMessage::default();
        msg.set_region_id(1);
        trans.send(msg.clone()).unwrap();
        trans.flush();
        let mut resp = receiver
            .significant_msg_receiver
            .recv_timeout(Duration::from_secs(3))
            .unwrap();
        assert!(is_unreachable_to(&resp, 1, 0), "{:?}", resp);

        *resolver.addr.lock().unwrap() = Some(format!("{}", server.listening_addr()));

        trans.send(msg.clone()).unwrap();
        trans.flush();
        assert!(receiver.rx.recv_timeout(Duration::from_secs(5)).is_ok());

        msg.mut_to_peer().set_store_id(2);
        msg.set_region_id(2);
        resolver.quick_fail.store(true, Ordering::SeqCst);
        trans.send(msg).unwrap();
        trans.flush();
        resp = receiver
            .significant_msg_receiver
            .recv_timeout(Duration::from_secs(3))
            .unwrap();
        assert!(is_unreachable_to(&resp, 2, 0), "{:?}", resp);
        server.stop().unwrap();
    }

    #[test]
    fn test_buffer_full() {
        let (receiver, resolver, mut server) = build_server();
        let mut trans = server.transport();
        *resolver.addr.lock().unwrap() = Some(format!("{}", server.listening_addr()));

        let mut msg = RaftMessage::default();
        msg.set_region_id(1);
        trans.send(msg.clone()).unwrap();
        let mut ans_msg = receiver.rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(ans_msg, msg);

        // When it fits in buffer, it should not report error.
        for id in 0..4096 {
            let mut msg = msg.clone();
            msg.mut_to_peer().set_id(id);
            trans.send(msg).unwrap();
        }
        // And it should not flush.
        assert!(receiver
            .rx
            .recv_timeout(Duration::from_millis(100))
            .is_err());
        msg.mut_to_peer().set_id(4096);
        assert!(trans.send(msg.clone()).is_err());

        // When it can't fit in buffer, it should flush automatically.
        for i in 0..4096 {
            ans_msg = receiver
                .rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap_or_else(|_| panic!("failed to fetch {}", i));
            assert_eq!(i, ans_msg.get_to_peer().get_id());
        }
        assert!(receiver
            .rx
            .recv_timeout(Duration::from_millis(100))
            .is_err());

        msg.mut_to_peer().set_id(4097);
        trans.send(msg).unwrap();
        assert!(receiver
            .rx
            .recv_timeout(Duration::from_millis(100))
            .is_err());
        trans.flush();
        msg = receiver.rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(msg.get_to_peer().get_id(), 4097);
        server.stop().unwrap();
    }

    #[test]
    fn test_reconnect() {
        let (receiver1, resolver1, mut server1) = build_server();
        let (receiver2, _resolver2, mut server2) = build_server();
        let mut trans = server1.transport();

        let (tx, rx) = mpsc::channel();
        *resolver1.delegate.lock().unwrap() = Some(tx);
        let mut msg = RaftMessage::default();
        msg.set_region_id(1);
        trans.send(msg.clone()).unwrap();
        trans.flush();
        receiver2
            .rx
            .recv_timeout(Duration::from_millis(300))
            .unwrap_err();

        let (_, cb) = rx.recv_timeout(Duration::from_millis(300)).unwrap();
        cb(Ok(format!("{}", server2.listening_addr())));
        let mut ans_msg = receiver2
            .rx
            .recv_timeout(Duration::from_millis(300))
            .unwrap();
        assert_eq!(ans_msg, msg);

        for i in 0..10 {
            msg.mut_to_peer().set_id(i);
            trans.send(msg.clone()).unwrap();
        }
        trans.flush();
        for i in 0..10 {
            ans_msg = receiver2
                .rx
                .recv_timeout(Duration::from_millis(300))
                .unwrap_or_else(|_| panic!("failed to fetch {}", i));
            assert_eq!(i, ans_msg.get_to_peer().get_id());
        }
        assert!(receiver2
            .rx
            .recv_timeout(Duration::from_millis(100))
            .is_err());

        server2.stop().unwrap();
        let (_, cb) = rx.recv_timeout(Duration::from_millis(300)).unwrap();
        msg.mut_to_peer().set_id(5000);
        trans.send(msg.clone()).unwrap();
        trans.flush();
        cb(Ok(format!("{}", server1.listening_addr())));
        ans_msg = receiver1.rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(ans_msg, msg);
        server1.stop().unwrap();
    }
}
