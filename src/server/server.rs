// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::i32;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use engine::Engines;
use futures::{Future, Stream};
use grpcio::{
    ChannelBuilder, EnvBuilder, Environment, ResourceQuota, Server as GrpcServer, ServerBuilder,
};
use kvproto::debugpb_grpc::create_debug;
use kvproto::import_sstpb_grpc::create_import_sst;
use kvproto::tikvpb_grpc::*;
use tokio_threadpool::{Builder as ThreadPoolBuilder, ThreadPool};
use tokio_timer::timer::Handle;

use crate::server::lock_manager::deadlock::Service as DeadlockService;
use kvproto::deadlock_grpc::create_deadlock;

use crate::coprocessor::Endpoint;
use crate::import::ImportSSTService;
use crate::raftstore::store::SnapManager;
use crate::storage::lock_manager::LockMgr;
use crate::storage::{Engine, Storage};
use tikv_util::security::SecurityManager;
use tikv_util::timer::GLOBAL_TIMER_HANDLE;
use tikv_util::worker::Worker;
use tikv_util::Either;

use super::load_statistics::*;
use super::raft_client2::RaftStreamPool;
use super::resolve::StoreAddrResolver;
use super::service::*;
use super::snap::{Runner as SnapHandler, Task as SnapTask};
use super::transport::{RaftStoreRouter, ServerTransport};
use super::{Config, Result};

const LOAD_STATISTICS_SLOTS: usize = 4;
const LOAD_STATISTICS_INTERVAL: Duration = Duration::from_millis(100);
const MAX_GRPC_RECV_MSG_LEN: i32 = 10 * 1024 * 1024;
pub const GRPC_THREAD_PREFIX: &str = "grpc-server";
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
    thread_load: Arc<ThreadLoad>,
    timer: Handle,
}

impl<T: RaftStoreRouter, S: StoreAddrResolver + 'static> Server<T, S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new<E: Engine, L: LockMgr>(
        cfg: &Arc<Config>,
        security_mgr: &Arc<SecurityManager>,
        storage: Storage<E, L>,
        cop: Endpoint<E>,
        raft_router: T,
        resolver: S,
        snap_mgr: SnapManager,
        debug_engines: Option<Engines>,
        import_service: Option<ImportSSTService<T>>,
        deadlock_service: Option<DeadlockService>,
    ) -> Result<Self> {
        // A helper thread (or pool) for transport layer.
        let stats_pool = ThreadPoolBuilder::new()
            .pool_size(cfg.stats_concurrency)
            .name_prefix(STATS_THREAD_PREFIX)
            .build();
        let thread_load = Arc::new(ThreadLoad::with_threshold(cfg.heavy_load_threshold));

        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(cfg.grpc_concurrency)
                .name_prefix(thd_name!(GRPC_THREAD_PREFIX))
                .build(),
        );
        let snap_worker = Worker::new("snap-handler");

        let gc_worker = storage.gc_worker.clone();
        let kv_service = KvService::new(
            storage,
            cop,
            raft_router.clone(),
            snap_worker.scheduler(),
            Arc::clone(&thread_load),
        );

        let mut addr = SocketAddr::from_str(&cfg.addr)?;
        let ip = format!("{}", addr.ip());
        let mem_quota = ResourceQuota::new(Some("ServerMemQuota"))
            .resize_memory(cfg.grpc_memory_pool_quota.0 as usize);
        let channel_args = ChannelBuilder::new(Arc::clone(&env))
            .stream_initial_window_size(cfg.grpc_stream_initial_window_size.0 as i32)
            .max_concurrent_stream(cfg.grpc_concurrent_stream)
            .max_receive_message_len(MAX_GRPC_RECV_MSG_LEN)
            .resource_quota(mem_quota)
            .max_send_message_len(-1)
            .http2_max_ping_strikes(i32::MAX) // For pings without data from clients.
            .build_args();
        let builder_or_server = {
            let mut sb = ServerBuilder::new(Arc::clone(&env))
                .channel_args(channel_args)
                .register_service(create_tikv(kv_service));
            sb = security_mgr.bind(sb, &ip, addr.port());
            if let Some(engines) = debug_engines {
                let debug_service = DebugService::new(engines, raft_router.clone(), gc_worker);
                sb = sb.register_service(create_debug(debug_service));
            }
            if let Some(service) = import_service {
                sb = sb.register_service(create_import_sst(service));
            }
            if let Some(service) = deadlock_service {
                sb = sb.register_service(create_deadlock(service));
            }
            // When port is 0, it has to be binded now to get a valid address, which
            // is then reported to PD before the server is up. 0 is usually used in tests.
            if addr.port() == 0 {
                let server = sb.build()?;
                let (ref host, port) = server.bind_addrs()[0];
                addr = SocketAddr::new(IpAddr::from_str(host)?, port as u16);
                Either::Right(server)
            } else {
                Either::Left(sb)
            }
        };

        info!("listening on addr"; "addr" => addr);

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
            builder_or_server: Some(builder_or_server),
            local_addr: addr,
            trans,
            raft_router,
            snap_mgr,
            snap_worker,
            stats_pool: Some(stats_pool),
            thread_load,
            timer: GLOBAL_TIMER_HANDLE.clone(),
        };

        Ok(svr)
    }

    pub fn transport(&self) -> ServerTransport<T, S> {
        self.trans.clone()
    }

    /// Starts the TiKV server.
    pub fn start(&mut self, cfg: Arc<Config>, security_mgr: Arc<SecurityManager>) -> Result<()> {
        let snap_runner = SnapHandler::new(
            Arc::clone(&self.env),
            self.snap_mgr.clone(),
            self.raft_router.clone(),
            security_mgr,
            Arc::clone(&cfg),
        );
        box_try!(self.snap_worker.start(snap_runner));
        let builder_or_server = self.builder_or_server.take().unwrap();
        let mut grpc_server = match builder_or_server {
            Either::Left(builder) => builder.build()?,
            Either::Right(server) => server,
        };
        info!("listening on addr"; "addr" => &self.local_addr);
        grpc_server.start();
        self.builder_or_server = Some(Either::Right(grpc_server));

        let mut load_stats = {
            let tl = Arc::clone(&self.thread_load);
            ThreadLoadStatistics::new(LOAD_STATISTICS_SLOTS, GRPC_THREAD_PREFIX, tl)
        };
        self.stats_pool.as_ref().unwrap().spawn(
            self.timer
                .interval(Instant::now(), LOAD_STATISTICS_INTERVAL)
                .map_err(|_| ())
                .for_each(move |i| {
                    load_stats.record(i);
                    Ok(())
                }),
        );

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
    use super::super::transport::RaftStoreRouter;
    use super::super::{Config, Result};
    use crate::coprocessor;
    use crate::raftstore::store::transport::Transport;
    use crate::raftstore::store::*;
    use crate::raftstore::Result as RaftStoreResult;
    use crate::storage::TestStorageBuilder;

    use crate::coprocessor::readpool_impl;
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

        fn send_command(&self, _: RaftCmdRequest, _: Callback) -> RaftStoreResult<()> {
            unimplemented!()
        }

        fn significant_send(&self, _: u64, msg: SignificantMsg) -> RaftStoreResult<()> {
            self.significant_msg_sender.send(msg).unwrap();
            Ok(())
        }

        fn casual_send(&self, _: u64, _: CasualMessage) -> RaftStoreResult<()> {
            unimplemented!()
        }

        fn broadcast_unreachable(&self, _: u64) {}
    }

    fn is_unreachable_to(msg: &SignificantMsg, region_id: u64, to_peer_id: u64) -> bool {
        *msg == SignificantMsg::Unreachable {
            region_id,
            to_peer_id,
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

        let (tx, rx) = mpsc::channel();
        let (significant_msg_sender, significant_msg_receiver) = mpsc::channel();
        let router = TestRaftStoreRouter {
            tx,
            significant_msg_sender,
        };

        let quick_fail = Arc::new(AtomicBool::new(false));
        let cfg = Arc::new(cfg);
        let security_mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());

        let cop_read_pool = readpool_impl::build_read_pool_for_test(storage.get_engine());
        let cop = coprocessor::Endpoint::new(&cfg, cop_read_pool);

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
            None,
            None,
            None,
        )
        .unwrap();

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

        let mut msg = RaftMessage::new();
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
        trans.send(msg.clone()).unwrap();
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

        let mut msg = RaftMessage::new();
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
        let mut msg = RaftMessage::new();
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
