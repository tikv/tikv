// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::Write,
    path::Path,
    str::FromStr,
    sync::{
        mpsc::{self},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

use collections::HashMap;
use encryption_export::{
    data_key_manager_from_config, DataKeyManager, FileConfig, MasterKeyConfig,
};
use engine_rocks::{
    config::BlobRunMode, RocksCompactedEvent, RocksEngine, RocksSnapshot, RocksStatistics,
};
use engine_test::raft::RaftTestEngine;
use engine_traits::{
    CfName, CfNamesExt, Engines, Iterable, KvEngine, Peekable, RaftEngineDebug, RaftEngineReadOnly,
    CF_DEFAULT, CF_RAFT, CF_WRITE,
};
use fail::fail_point;
use file_system::IoRateLimiter;
use futures::{executor::block_on, future::BoxFuture, StreamExt};
use grpcio::{ChannelBuilder, Environment};
use hybrid_engine::HybridEngine;
use kvproto::{
    encryptionpb::EncryptionMethod,
    kvrpcpb::{PrewriteRequestPessimisticAction::*, *},
    metapb::{self, RegionEpoch},
    raft_cmdpb::{
        AdminCmdType, AdminRequest, ChangePeerRequest, ChangePeerV2Request, CmdType,
        RaftCmdRequest, RaftCmdResponse, Request, StatusCmdType, StatusRequest,
    },
    raft_serverpb::{
        PeerState, RaftApplyState, RaftLocalState, RaftTruncatedState, RegionLocalState,
    },
    tikvpb::TikvClient,
};
use pd_client::PdClient;
use protobuf::RepeatedField;
use raft::eraftpb::ConfChangeType;
use raftstore::{
    store::{fsm::RaftRouter, *},
    RaftRouterCompactedEventSender, Result,
};
use rand::{seq::SliceRandom, RngCore};
use region_cache_memory_engine::{RangeCacheEngineContext, RangeCacheMemoryEngine};
use server::common::{ConfiguredRaftEngine, KvEngineBuilder};
use tempfile::TempDir;
use test_pd_client::TestPdClient;
use test_util::eventually;
use tikv::{
    config::*,
    server::KvEngineFactoryBuilder,
    storage::{
        kv::{SnapContext, SnapshotExt},
        point_key_range, Engine, Snapshot,
    },
};
pub use tikv_util::store::{find_peer, new_learner_peer, new_peer};
use tikv_util::{
    config::*,
    escape,
    mpsc::future,
    time::{Instant, ThreadReadId},
    worker::LazyWorker,
    HandyRwLock,
};
use txn_types::Key;

use crate::{Cluster, Config, KvEngineWithRocks, RawEngine, ServerCluster, Simulator};

pub type HybridEngineImpl = HybridEngine<RocksEngine, RangeCacheMemoryEngine>;

pub fn must_get<EK: KvEngine>(
    engine: &impl RawEngine<EK>,
    cf: &str,
    key: &[u8],
    value: Option<&[u8]>,
) {
    for _ in 1..300 {
        let res = engine.get_value_cf(cf, &keys::data_key(key)).unwrap();
        if let (Some(value), Some(res)) = (value, res.as_ref()) {
            assert_eq!(value, &res[..]);
            return;
        }
        if value.is_none() && res.is_none() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    debug!("last try to get {}", log_wrappers::hex_encode_upper(key));
    let res = engine.get_value_cf(cf, &keys::data_key(key)).unwrap();
    if value == res.as_ref().map(|r| r.as_ref()) {
        return;
    }
    panic!(
        "can't get value {:?} for key {}, actual={:?}",
        value.map(escape),
        log_wrappers::hex_encode_upper(key),
        res
    )
}

pub fn eventually_get_equal<EK: KvEngine>(engine: &impl RawEngine<EK>, key: &[u8], value: &[u8]) {
    eventually(
        Duration::from_millis(100),
        Duration::from_millis(2000),
        || {
            let res = engine
                .get_value_cf("default", &keys::data_key(key))
                .unwrap();
            if let Some(res) = res.as_ref() {
                value == &res[..]
            } else {
                false
            }
        },
    );
}

pub fn must_get_equal<EK: KvEngine>(engine: &impl RawEngine<EK>, key: &[u8], value: &[u8]) {
    must_get(engine, "default", key, Some(value));
}

pub fn must_get_none<EK: KvEngine>(engine: &impl RawEngine<EK>, key: &[u8]) {
    must_get(engine, "default", key, None);
}

pub fn must_get_cf_equal<EK: KvEngine>(
    engine: &impl RawEngine<EK>,
    cf: &str,
    key: &[u8],
    value: &[u8],
) {
    must_get(engine, cf, key, Some(value));
}

pub fn must_get_cf_none<EK: KvEngine>(engine: &impl RawEngine<EK>, cf: &str, key: &[u8]) {
    must_get(engine, cf, key, None);
}

pub fn must_region_cleared(engine: &Engines<RocksEngine, RaftTestEngine>, region: &metapb::Region) {
    let id = region.get_id();
    let state_key = keys::region_state_key(id);
    let state: RegionLocalState = engine.kv.get_msg_cf(CF_RAFT, &state_key).unwrap().unwrap();
    assert_eq!(state.get_state(), PeerState::Tombstone, "{:?}", state);
    let start_key = keys::data_key(region.get_start_key());
    let end_key = keys::data_key(region.get_end_key());
    for cf in engine.kv.cf_names() {
        engine
            .kv
            .scan(cf, &start_key, &end_key, false, |k, v| {
                panic!(
                    "[region {}] unexpected ({:?}, {:?}) in cf {:?}",
                    id, k, v, cf
                );
            })
            .unwrap();
    }

    engine
        .raft
        .scan_entries(id, |_| panic!("[region {}] unexpected entry", id))
        .unwrap();

    let state: Option<RaftLocalState> = engine.raft.get_raft_state(id).unwrap();
    assert!(
        state.is_none(),
        "[region {}] raft state key should be removed: {:?}",
        id,
        state
    );
}

lazy_static! {
    pub static ref TEST_CONFIG: TikvConfig = {
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let common_test_cfg = manifest_dir.join("src/common-test.toml");
        let mut cfg = TikvConfig::from_file(&common_test_cfg, None).unwrap_or_else(|e| {
            panic!(
                "invalid auto generated configuration file {}, err {}",
                manifest_dir.display(),
                e
            );
        });
        // To speed up leader transfer.
        cfg.raft_store.allow_unsafe_vote_after_start = true;
        cfg
    };
}

pub fn new_tikv_config(cluster_id: u64) -> TikvConfig {
    let mut cfg = TEST_CONFIG.clone();
    cfg.server.cluster_id = cluster_id;
    cfg
}

pub fn new_tikv_config_with_api_ver(cluster_id: u64, api_ver: ApiVersion) -> TikvConfig {
    let mut cfg = TEST_CONFIG.clone();
    cfg.server.cluster_id = cluster_id;
    cfg.storage.set_api_version(api_ver);
    cfg.raft_store.pd_report_min_resolved_ts_interval = config(ReadableDuration::secs(1));
    cfg
}

fn config(interval: ReadableDuration) -> ReadableDuration {
    fail_point!("mock_min_resolved_ts_interval", |_| {
        ReadableDuration::millis(50)
    });
    fail_point!("mock_min_resolved_ts_interval_disable", |_| {
        ReadableDuration::millis(0)
    });
    interval
}

// Create a base request.
pub fn new_base_request(region_id: u64, epoch: RegionEpoch, read_quorum: bool) -> RaftCmdRequest {
    let mut req = RaftCmdRequest::default();
    req.mut_header().set_region_id(region_id);
    req.mut_header().set_region_epoch(epoch);
    req.mut_header().set_read_quorum(read_quorum);
    req
}

pub fn new_request(
    region_id: u64,
    epoch: RegionEpoch,
    requests: Vec<Request>,
    read_quorum: bool,
) -> RaftCmdRequest {
    let mut req = new_base_request(region_id, epoch, read_quorum);
    req.set_requests(requests.into());
    req
}

pub fn new_put_cmd(key: &[u8], value: &[u8]) -> Request {
    let mut cmd = Request::default();
    cmd.set_cmd_type(CmdType::Put);
    cmd.mut_put().set_key(key.to_vec());
    cmd.mut_put().set_value(value.to_vec());
    cmd
}

pub fn new_put_cf_cmd(cf: &str, key: &[u8], value: &[u8]) -> Request {
    let mut cmd = Request::default();
    cmd.set_cmd_type(CmdType::Put);
    cmd.mut_put().set_key(key.to_vec());
    cmd.mut_put().set_value(value.to_vec());
    cmd.mut_put().set_cf(cf.to_string());
    cmd
}

pub fn new_get_cmd(key: &[u8]) -> Request {
    let mut cmd = Request::default();
    cmd.set_cmd_type(CmdType::Get);
    cmd.mut_get().set_key(key.to_vec());
    cmd
}

pub fn new_snap_cmd() -> Request {
    let mut cmd = Request::default();
    cmd.set_cmd_type(CmdType::Snap);
    cmd
}

pub fn new_read_index_cmd() -> Request {
    let mut cmd = Request::default();
    cmd.set_cmd_type(CmdType::ReadIndex);
    cmd
}

pub fn new_get_cf_cmd(cf: &str, key: &[u8]) -> Request {
    let mut cmd = Request::default();
    cmd.set_cmd_type(CmdType::Get);
    cmd.mut_get().set_key(key.to_vec());
    cmd.mut_get().set_cf(cf.to_string());
    cmd
}

pub fn new_delete_cmd(cf: &str, key: &[u8]) -> Request {
    let mut cmd = Request::default();
    cmd.set_cmd_type(CmdType::Delete);
    cmd.mut_delete().set_key(key.to_vec());
    cmd.mut_delete().set_cf(cf.to_string());
    cmd
}

pub fn new_delete_range_cmd(cf: &str, start: &[u8], end: &[u8]) -> Request {
    let mut cmd = Request::default();
    cmd.set_cmd_type(CmdType::DeleteRange);
    cmd.mut_delete_range().set_start_key(start.to_vec());
    cmd.mut_delete_range().set_end_key(end.to_vec());
    cmd.mut_delete_range().set_cf(cf.to_string());
    cmd
}

pub fn new_status_request(
    region_id: u64,
    peer: metapb::Peer,
    request: StatusRequest,
) -> RaftCmdRequest {
    let mut req = new_base_request(region_id, RegionEpoch::default(), false);
    req.mut_header().set_peer(peer);
    req.set_status_request(request);
    req
}

pub fn new_region_detail_cmd() -> StatusRequest {
    let mut cmd = StatusRequest::default();
    cmd.set_cmd_type(StatusCmdType::RegionDetail);
    cmd
}

pub fn new_region_leader_cmd() -> StatusRequest {
    let mut cmd = StatusRequest::default();
    cmd.set_cmd_type(StatusCmdType::RegionLeader);
    cmd
}

pub fn new_admin_request(
    region_id: u64,
    epoch: &RegionEpoch,
    request: AdminRequest,
) -> RaftCmdRequest {
    let mut req = new_base_request(region_id, epoch.clone(), false);
    req.set_admin_request(request);
    req
}

pub fn new_change_peer_request(change_type: ConfChangeType, peer: metapb::Peer) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::ChangePeer);
    req.mut_change_peer().set_change_type(change_type);
    req.mut_change_peer().set_peer(peer);
    req
}

pub fn new_change_peer_v2_request(changes: Vec<ChangePeerRequest>) -> AdminRequest {
    let mut cp = ChangePeerV2Request::default();
    cp.set_changes(changes.into());
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::ChangePeerV2);
    req.set_change_peer_v2(cp);
    req
}

pub fn new_compact_log_request(index: u64, term: u64) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::CompactLog);
    req.mut_compact_log().set_compact_index(index);
    req.mut_compact_log().set_compact_term(term);
    req
}

pub fn new_transfer_leader_cmd(peer: metapb::Peer) -> AdminRequest {
    let mut cmd = AdminRequest::default();
    cmd.set_cmd_type(AdminCmdType::TransferLeader);
    cmd.mut_transfer_leader().set_peer(peer);
    cmd
}

pub fn new_prepare_merge(target_region: metapb::Region) -> AdminRequest {
    let mut cmd = AdminRequest::default();
    cmd.set_cmd_type(AdminCmdType::PrepareMerge);
    cmd.mut_prepare_merge().set_target(target_region);
    cmd
}

pub fn new_store(store_id: u64, addr: String) -> metapb::Store {
    let mut store = metapb::Store::default();
    store.set_id(store_id);
    store.set_address(addr);

    store
}

pub fn sleep_ms(ms: u64) {
    thread::sleep(Duration::from_millis(ms));
}

pub fn sleep_until_election_triggered(cfg: &Config) {
    let election_timeout = cfg.raft_store.raft_base_tick_interval.as_millis()
        * cfg.raft_store.raft_election_timeout_ticks as u64;
    sleep_ms(3u64 * election_timeout);
}

pub fn is_error_response(resp: &RaftCmdResponse) -> bool {
    resp.get_header().has_error()
}

#[derive(Default)]
struct CallbackLeakDetector {
    called: bool,
}

impl Drop for CallbackLeakDetector {
    fn drop(&mut self) {
        if self.called {
            return;
        }

        debug!("before capture");
        let bt = backtrace::Backtrace::new();
        warn!("callback is dropped"; "backtrace" => ?bt);
    }
}

pub fn check_raft_cmd_request(cmd: &RaftCmdRequest) -> bool {
    let mut is_read = cmd.has_status_request();
    let mut is_write = cmd.has_admin_request();
    for req in cmd.get_requests() {
        match req.get_cmd_type() {
            CmdType::Get | CmdType::Snap | CmdType::ReadIndex => is_read = true,
            CmdType::Put | CmdType::Delete | CmdType::DeleteRange | CmdType::IngestSst => {
                is_write = true
            }
            CmdType::Invalid | CmdType::Prewrite => panic!("Invalid RaftCmdRequest: {:?}", cmd),
        }
    }
    assert!(is_read ^ is_write, "Invalid RaftCmdRequest: {:?}", cmd);
    is_read
}

pub fn make_cb_rocks(
    cmd: &RaftCmdRequest,
) -> (Callback<RocksSnapshot>, future::Receiver<RaftCmdResponse>) {
    make_cb::<RocksEngine>(cmd)
}

pub fn make_cb<EK: KvEngine>(
    cmd: &RaftCmdRequest,
) -> (Callback<EK::Snapshot>, future::Receiver<RaftCmdResponse>) {
    let is_read = check_raft_cmd_request(cmd);
    let (tx, rx) = future::bounded(1, future::WakePolicy::Immediately);
    let mut detector = CallbackLeakDetector::default();
    let cb = if is_read {
        Callback::read(Box::new(move |resp: ReadResponse<EK::Snapshot>| {
            detector.called = true;
            // we don't care error actually.
            let _ = tx.send(resp.response);
        }))
    } else {
        Callback::write(Box::new(move |resp: WriteResponse| {
            detector.called = true;
            // we don't care error actually.
            let _ = tx.send(resp.response);
        }))
    };
    (cb, rx)
}

pub fn make_cb_ext<EK: KvEngine>(
    cmd: &RaftCmdRequest,
    proposed: Option<ExtCallback>,
    committed: Option<ExtCallback>,
) -> (Callback<EK::Snapshot>, future::Receiver<RaftCmdResponse>) {
    let (cb, receiver) = make_cb::<EK>(cmd);
    if let Callback::Write { cb, .. } = cb {
        (Callback::write_ext(cb, proposed, committed), receiver)
    } else {
        (cb, receiver)
    }
}

// Issue a read request on the specified peer.
pub fn read_on_peer<EK: KvEngineWithRocks, T: Simulator<EK>>(
    cluster: &mut Cluster<EK, T>,
    peer: metapb::Peer,
    region: metapb::Region,
    key: &[u8],
    read_quorum: bool,
    timeout: Duration,
) -> Result<RaftCmdResponse> {
    let mut request = new_request(
        region.get_id(),
        region.get_region_epoch().clone(),
        vec![new_get_cmd(key)],
        read_quorum,
    );
    request.mut_header().set_peer(peer);
    cluster.read(None, None, request, timeout)
}

pub fn async_read_on_peer<EK: KvEngineWithRocks, T: Simulator<EK>>(
    cluster: &mut Cluster<EK, T>,
    peer: metapb::Peer,
    region: metapb::Region,
    key: &[u8],
    read_quorum: bool,
    replica_read: bool,
) -> BoxFuture<'static, RaftCmdResponse> {
    let node_id = peer.get_store_id();
    let mut request = new_request(
        region.get_id(),
        region.get_region_epoch().clone(),
        vec![new_get_cmd(key)],
        read_quorum,
    );
    request.mut_header().set_peer(peer);
    request.mut_header().set_replica_read(replica_read);
    let (tx, mut rx) = future::bounded(1, future::WakePolicy::Immediately);
    let cb = Callback::read(Box::new(move |resp| drop(tx.send(resp.response))));
    cluster
        .sim
        .wl()
        .async_read(None, node_id, None, request, cb);
    Box::pin(async move {
        let fut = rx.next();
        fut.await.unwrap()
    })
}

pub fn batch_read_on_peer<EK: KvEngineWithRocks, T: Simulator<EK>>(
    cluster: &mut Cluster<EK, T>,
    requests: &[(metapb::Peer, metapb::Region)],
) -> Vec<ReadResponse<EK::Snapshot>> {
    let batch_id = Some(ThreadReadId::new());
    let (tx, rx) = mpsc::sync_channel(3);
    let mut results = vec![];
    let mut len = 0;
    for (peer, region) in requests {
        let node_id = peer.get_store_id();
        let mut request = new_request(
            region.get_id(),
            region.get_region_epoch().clone(),
            vec![new_snap_cmd()],
            false,
        );
        request.mut_header().set_peer(peer.clone());
        let t = tx.clone();
        let cb = Callback::read(Box::new(move |resp| {
            t.send((len, resp)).unwrap();
        }));
        cluster
            .sim
            .wl()
            .async_read(None, node_id, batch_id.clone(), request, cb);
        len += 1;
    }
    while results.len() < len {
        results.push(rx.recv_timeout(Duration::from_secs(1)).unwrap());
    }
    results.sort_by_key(|resp| resp.0);
    results.into_iter().map(|resp| resp.1).collect()
}

pub fn read_index_on_peer<EK: KvEngineWithRocks, T: Simulator<EK>>(
    cluster: &mut Cluster<EK, T>,
    peer: metapb::Peer,
    region: metapb::Region,
    read_quorum: bool,
    timeout: Duration,
) -> Result<RaftCmdResponse> {
    let mut request = new_request(
        region.get_id(),
        region.get_region_epoch().clone(),
        vec![new_read_index_cmd()],
        read_quorum,
    );
    request.mut_header().set_peer(peer);
    cluster.read(None, None, request, timeout)
}

pub fn async_read_index_on_peer<EK: KvEngineWithRocks, T: Simulator<EK>>(
    cluster: &mut Cluster<EK, T>,
    peer: metapb::Peer,
    region: metapb::Region,
    key: &[u8],
    read_quorum: bool,
) -> BoxFuture<'static, RaftCmdResponse> {
    let node_id = peer.get_store_id();
    let mut cmd = new_read_index_cmd();
    cmd.mut_read_index().set_start_ts(u64::MAX);
    cmd.mut_read_index()
        .mut_key_ranges()
        .push(point_key_range(Key::from_raw(key)));
    let mut request = new_request(
        region.get_id(),
        region.get_region_epoch().clone(),
        vec![cmd],
        read_quorum,
    );
    request.mut_header().set_peer(peer);
    let (tx, mut rx) = future::bounded(1, future::WakePolicy::Immediately);
    let cb = Callback::read(Box::new(move |resp| drop(tx.send(resp.response))));
    cluster
        .sim
        .wl()
        .async_read(None, node_id, None, request, cb);
    Box::pin(async move {
        let fut = rx.next();
        fut.await.unwrap()
    })
}

pub fn async_command_on_node<EK: KvEngineWithRocks, T: Simulator<EK>>(
    cluster: &mut Cluster<EK, T>,
    node_id: u64,
    request: RaftCmdRequest,
) -> BoxFuture<'static, RaftCmdResponse> {
    let (cb, mut rx) = make_cb::<EK>(&request);
    cluster
        .sim
        .rl()
        .async_command_on_node(node_id, request, cb)
        .unwrap();
    Box::pin(async move {
        let fut = rx.next();
        fut.await.unwrap()
    })
}

pub fn must_get_value(resp: &RaftCmdResponse) -> Vec<u8> {
    if resp.get_header().has_error() {
        panic!("failed to read {:?}", resp);
    }
    assert_eq!(resp.get_responses().len(), 1);
    assert_eq!(resp.get_responses()[0].get_cmd_type(), CmdType::Get);
    assert!(resp.get_responses()[0].has_get());
    resp.get_responses()[0].get_get().get_value().to_vec()
}

pub fn must_read_on_peer<EK: KvEngineWithRocks, T: Simulator<EK>>(
    cluster: &mut Cluster<EK, T>,
    peer: metapb::Peer,
    region: metapb::Region,
    key: &[u8],
    value: &[u8],
) {
    let timeout = Duration::from_secs(5);
    match read_on_peer(cluster, peer, region, key, false, timeout) {
        Ok(ref resp) if value == must_get_value(resp).as_slice() => (),
        other => panic!(
            "read key {}, expect value {:?}, got {:?}",
            log_wrappers::hex_encode_upper(key),
            value,
            other
        ),
    }
}

pub fn must_error_read_on_peer<EK: KvEngineWithRocks, T: Simulator<EK>>(
    cluster: &mut Cluster<EK, T>,
    peer: metapb::Peer,
    region: metapb::Region,
    key: &[u8],
    timeout: Duration,
) {
    if let Ok(mut resp) = read_on_peer(cluster, peer, region, key, false, timeout) {
        if !resp.get_header().has_error() {
            let value = resp.mut_responses()[0].mut_get().take_value();
            panic!(
                "key {}, expect error but got {}",
                log_wrappers::hex_encode_upper(key),
                escape(&value)
            );
        }
    }
}

#[track_caller]
pub fn must_contains_error(resp: &RaftCmdResponse, msg: &str) {
    let header = resp.get_header();
    assert!(header.has_error());
    let err_msg = header.get_error().get_message();
    assert!(err_msg.contains(msg), "{:?}", resp);
}

pub fn create_test_engine<EK>(
    // TODO: pass it in for all cases.
    router: Option<RaftRouter<EK, RaftTestEngine>>,
    limiter: Option<Arc<IoRateLimiter>>,
    cfg: &Config,
) -> (
    Engines<EK, RaftTestEngine>,
    Option<Arc<DataKeyManager>>,
    TempDir,
    LazyWorker<String>,
    Arc<RocksStatistics>,
    Option<Arc<RocksStatistics>>,
)
where
    EK: KvEngine<DiskEngine = RocksEngine, CompactedEvent = RocksCompactedEvent> + KvEngineBuilder,
{
    let dir = test_util::temp_dir("test_cluster", cfg.prefer_mem);
    let mut cfg = cfg.clone();
    cfg.storage.data_dir = dir.path().to_str().unwrap().to_string();
    cfg.raft_store.raftdb_path = cfg.infer_raft_db_path(None).unwrap();
    cfg.raft_engine.mut_config().dir = cfg.infer_raft_engine_path(None).unwrap();
    let key_manager =
        data_key_manager_from_config(&cfg.security.encryption, dir.path().to_str().unwrap())
            .unwrap()
            .map(Arc::new);
    let cache = cfg.storage.block_cache.build_shared_cache();
    let env = cfg
        .build_shared_rocks_env(key_manager.clone(), limiter)
        .unwrap();

    let sst_worker = LazyWorker::new("sst-recovery");
    let scheduler = sst_worker.scheduler();

    let (raft_engine, raft_statistics) = RaftTestEngine::build(&cfg, &env, &key_manager, &cache);

    let mut builder = KvEngineFactoryBuilder::new(env, &cfg, cache, key_manager.clone())
        .sst_recovery_sender(Some(scheduler));
    if let Some(router) = router {
        builder = builder.compaction_event_sender(Arc::new(RaftRouterCompactedEventSender {
            router: Mutex::new(router),
        }));
    }
    let factory = builder.build();
    let disk_engine = factory.create_shared_db(dir.path()).unwrap();
    let config = Arc::new(VersionTrack::new(cfg.tikv.range_cache_engine.clone()));
    let kv_engine: EK = KvEngineBuilder::build(
        RangeCacheEngineContext::new(config),
        disk_engine,
        None,
        None,
    );
    let engines = Engines::new(kv_engine, raft_engine);
    (
        engines,
        key_manager,
        dir,
        sst_worker,
        factory.rocks_statistics(),
        raft_statistics,
    )
}

pub fn configure_for_request_snapshot(config: &mut Config) {
    // We don't want to generate snapshots due to compact log.
    config.raft_store.raft_log_gc_threshold = 1000;
    config.raft_store.raft_log_gc_count_limit = Some(1000);
    config.raft_store.raft_log_gc_size_limit = Some(ReadableSize::mb(20));
}

pub fn configure_for_hibernate(config: &mut Config) {
    // Uses long check interval to make leader keep sleeping during tests.
    config.raft_store.abnormal_leader_missing_duration = ReadableDuration::secs(20);
    config.raft_store.max_leader_missing_duration = ReadableDuration::secs(40);
    config.raft_store.peer_stale_state_check_interval = ReadableDuration::secs(10);
}

pub fn configure_for_snapshot(config: &mut Config) {
    // Truncate the log quickly so that we can force sending snapshot.
    config.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(20);
    config.raft_store.raft_log_gc_count_limit = Some(2);
    config.raft_store.merge_max_log_gap = 1;
    config.raft_store.snap_mgr_gc_tick_interval = ReadableDuration::millis(50);
}

pub fn configure_for_merge(config: &mut Config) {
    // Avoid log compaction which will prevent merge.
    config.raft_store.raft_log_gc_threshold = 1000;
    config.raft_store.raft_log_gc_count_limit = Some(1000);
    config.raft_store.raft_log_gc_size_limit = Some(ReadableSize::mb(20));
    // Make merge check resume quickly.
    config.raft_store.merge_check_tick_interval = ReadableDuration::millis(100);
    // When isolated, follower relies on stale check tick to detect failure leader,
    // choose a smaller number to make it recover faster.
    config.raft_store.peer_stale_state_check_interval = ReadableDuration::millis(500);
}

pub fn ignore_merge_target_integrity(config: &mut Config, pd_client: &TestPdClient) {
    config.raft_store.dev_assert = false;
    pd_client.ignore_merge_target_integrity();
}

pub fn configure_for_lease_read(
    cfg: &mut Config,
    base_tick_ms: Option<u64>,
    election_ticks: Option<usize>,
) -> Duration {
    if let Some(base_tick_ms) = base_tick_ms {
        cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(base_tick_ms);
    }
    let base_tick_interval = cfg.raft_store.raft_base_tick_interval.0;
    if let Some(election_ticks) = election_ticks {
        cfg.raft_store.raft_election_timeout_ticks = election_ticks;
    }
    let election_ticks = cfg.raft_store.raft_election_timeout_ticks as u32;
    let election_timeout = base_tick_interval * election_ticks;
    // Adjust max leader lease.
    cfg.raft_store.raft_store_max_leader_lease =
        ReadableDuration(election_timeout - base_tick_interval);
    // Use large peer check interval, abnormal and max leader missing duration to
    // make a valid config, that is election timeout x 2 < peer stale state
    // check < abnormal < max leader missing duration.
    cfg.raft_store.peer_stale_state_check_interval = ReadableDuration(election_timeout * 3);
    cfg.raft_store.abnormal_leader_missing_duration = ReadableDuration(election_timeout * 4);
    cfg.raft_store.max_leader_missing_duration = ReadableDuration(election_timeout * 5);

    election_timeout
}

pub fn configure_for_enable_titan<EK: KvEngineWithRocks, T: Simulator<EK>>(
    cluster: &mut Cluster<EK, T>,
    min_blob_size: ReadableSize,
) {
    cluster.cfg.rocksdb.titan.enabled = Some(true);
    cluster.cfg.rocksdb.titan.purge_obsolete_files_period = ReadableDuration::secs(1);
    cluster.cfg.rocksdb.titan.max_background_gc = 10;
    cluster.cfg.rocksdb.defaultcf.titan.min_blob_size = Some(min_blob_size);
    cluster.cfg.rocksdb.defaultcf.titan.blob_run_mode = BlobRunMode::Normal;
    cluster.cfg.rocksdb.defaultcf.titan.min_gc_batch_size = ReadableSize::kb(0);
}

pub fn configure_for_disable_titan<EK: KvEngineWithRocks, T: Simulator<EK>>(
    cluster: &mut Cluster<EK, T>,
) {
    cluster.cfg.rocksdb.titan.enabled = Some(false);
}

pub fn configure_for_encryption<EK: KvEngineWithRocks, T: Simulator<EK>>(
    cluster: &mut Cluster<EK, T>,
) {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let master_key_file = manifest_dir.join("src/master-key.data");

    let cfg = &mut cluster.cfg.security.encryption;
    cfg.data_encryption_method = EncryptionMethod::Aes128Ctr;
    cfg.data_key_rotation_period = ReadableDuration(Duration::from_millis(100));
    cfg.master_key = MasterKeyConfig::File {
        config: FileConfig {
            path: master_key_file.to_str().unwrap().to_owned(),
        },
    }
}

pub fn configure_for_causal_ts<EK: KvEngineWithRocks, T: Simulator<EK>>(
    cluster: &mut Cluster<EK, T>,
    renew_interval: &str,
    renew_batch_min_size: u32,
) {
    let cfg = &mut cluster.cfg.causal_ts;
    cfg.renew_interval = ReadableDuration::from_str(renew_interval).unwrap();
    cfg.renew_batch_min_size = renew_batch_min_size;
}

/// Keep putting random kvs until specified size limit is reached.
pub fn put_till_size<EK: KvEngineWithRocks, T: Simulator<EK>>(
    cluster: &mut Cluster<EK, T>,
    limit: u64,
    range: &mut dyn Iterator<Item = u64>,
) -> Vec<u8> {
    put_cf_till_size(cluster, CF_DEFAULT, limit, range)
}

pub fn put_till_count<EK: KvEngineWithRocks, T: Simulator<EK>>(
    cluster: &mut Cluster<EK, T>,
    limit: u64,
    range: &mut dyn Iterator<Item = u64>,
) -> Vec<u8> {
    put_cf_till_count(cluster, CF_WRITE, limit, range)
}

pub fn put_cf_till_size<EK: KvEngineWithRocks, T: Simulator<EK>>(
    cluster: &mut Cluster<EK, T>,
    cf: &'static str,
    limit: u64,
    range: &mut dyn Iterator<Item = u64>,
) -> Vec<u8> {
    assert!(limit > 0);
    let mut len = 0;
    let mut rng = rand::thread_rng();
    let mut key = String::new();
    let mut value = vec![0; 64];
    while len < limit {
        let batch_size = std::cmp::min(1024, limit - len);
        let mut reqs = vec![];
        for _ in 0..batch_size / 74 + 1 {
            key.clear();
            let key_id = range.next().unwrap();
            write!(key, "{:09}", key_id).unwrap();
            rng.fill_bytes(&mut value);
            // plus 1 for the extra encoding prefix
            len += key.len() as u64 + 1;
            len += value.len() as u64;
            reqs.push(new_put_cf_cmd(cf, key.as_bytes(), &value));
        }
        cluster.batch_put(key.as_bytes(), reqs).unwrap();
        // Approximate size of memtable is inaccurate for small data,
        // we flush it to SST so we can use the size properties instead.
        cluster.must_flush_cf(cf, true);
    }
    key.into_bytes()
}

pub fn put_cf_till_count<EK: KvEngineWithRocks, T: Simulator<EK>>(
    cluster: &mut Cluster<EK, T>,
    cf: &'static str,
    limit: u64,
    range: &mut dyn Iterator<Item = u64>,
) -> Vec<u8> {
    assert!(limit > 0);
    let mut len = 0;
    let mut rng = rand::thread_rng();
    let mut key = String::new();
    let mut value = vec![0; 64];
    while len < limit {
        let batch_size = std::cmp::min(5, limit - len);
        let mut reqs = vec![];
        for _ in 0..batch_size {
            key.clear();
            let key_id = range.next().unwrap();
            write!(key, "{:09}", key_id).unwrap();
            rng.fill_bytes(&mut value);
            reqs.push(new_put_cf_cmd(cf, key.as_bytes(), &value));
        }
        len += batch_size;
        cluster.batch_put(key.as_bytes(), reqs).unwrap();
        // Approximate size of memtable is inaccurate for small data,
        // we flush it to SST so we can use the size properties instead.
        cluster.must_flush_cf(cf, true);
    }
    key.into_bytes()
}

pub fn new_mutation(op: Op, k: &[u8], v: &[u8]) -> Mutation {
    let mut mutation = Mutation::default();
    mutation.set_op(op);
    mutation.set_key(k.to_vec());
    mutation.set_value(v.to_vec());
    mutation
}

pub fn must_kv_write(
    pd_client: &TestPdClient,
    client: &TikvClient,
    ctx: Context,
    kvs: Vec<Mutation>,
    pk: Vec<u8>,
) -> u64 {
    let keys: Vec<_> = kvs.iter().map(|m| m.get_key().to_vec()).collect();
    let start_ts = block_on(pd_client.get_tso()).unwrap();
    must_kv_prewrite(client, ctx.clone(), kvs, pk, start_ts.into_inner());
    let commit_ts = block_on(pd_client.get_tso()).unwrap();
    must_kv_commit(
        client,
        ctx,
        keys,
        start_ts.into_inner(),
        commit_ts.into_inner(),
        commit_ts.into_inner(),
    );
    commit_ts.into_inner()
}

pub fn must_kv_read_equal(client: &TikvClient, ctx: Context, key: Vec<u8>, val: Vec<u8>, ts: u64) {
    let mut get_req = GetRequest::default();
    get_req.set_context(ctx);
    get_req.set_key(key);
    get_req.set_version(ts);

    for _ in 1..250 {
        let mut get_resp = client.kv_get(&get_req).unwrap();
        if get_resp.has_region_error() || get_resp.has_error() || get_resp.get_not_found() {
            thread::sleep(Duration::from_millis(20));
        } else if get_resp.take_value() == val {
            return;
        }
    }

    // Last try
    let mut get_resp = client.kv_get(&get_req).unwrap();
    assert!(
        !get_resp.has_region_error(),
        "{:?}",
        get_resp.get_region_error()
    );
    assert!(!get_resp.has_error(), "{:?}", get_resp.get_error());
    assert!(!get_resp.get_not_found());
    assert_eq!(get_resp.take_value(), val);
}

pub fn must_kv_read_not_found(client: &TikvClient, ctx: Context, key: Vec<u8>, ts: u64) {
    let mut get_req = GetRequest::default();
    get_req.set_context(ctx);
    get_req.set_key(key);
    get_req.set_version(ts);

    for _ in 1..250 {
        let get_resp = client.kv_get(&get_req).unwrap();
        if get_resp.has_region_error() || get_resp.has_error() {
            thread::sleep(Duration::from_millis(20));
        } else if get_resp.get_not_found() {
            return;
        }
    }

    // Last try
    let get_resp = client.kv_get(&get_req).unwrap();
    assert!(
        !get_resp.has_region_error(),
        "{:?}",
        get_resp.get_region_error()
    );
    assert!(!get_resp.has_error(), "{:?}", get_resp.get_error());
    assert!(get_resp.get_not_found());
}

pub fn write_and_read_key(
    client: &TikvClient,
    ctx: &Context,
    ts: &mut u64,
    k: Vec<u8>,
    v: Vec<u8>,
) {
    // Prewrite
    let prewrite_start_version = *ts + 1;
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(k.clone());
    mutation.set_value(v.clone());
    must_kv_prewrite(
        client,
        ctx.clone(),
        vec![mutation],
        k.clone(),
        prewrite_start_version,
    );
    // Commit
    let commit_version = *ts + 2;
    must_kv_commit(
        client,
        ctx.clone(),
        vec![k.clone()],
        prewrite_start_version,
        commit_version,
        commit_version,
    );
    // Get
    *ts += 3;
    must_kv_read_equal(client, ctx.clone(), k, v, *ts);
}

pub fn kv_read(client: &TikvClient, ctx: Context, key: Vec<u8>, ts: u64) -> GetResponse {
    let mut get_req = GetRequest::default();
    get_req.set_context(ctx);
    get_req.set_key(key);
    get_req.set_version(ts);
    client.kv_get(&get_req).unwrap()
}

pub fn kv_batch_read(
    client: &TikvClient,
    ctx: Context,
    keys: Vec<Vec<u8>>,
    ts: u64,
) -> BatchGetResponse {
    let mut batch_get_req = BatchGetRequest::default();
    batch_get_req.set_context(ctx);
    batch_get_req.set_keys(RepeatedField::from(keys));
    batch_get_req.set_version(ts);
    client.kv_batch_get(&batch_get_req).unwrap()
}

pub fn must_kv_prewrite_with(
    client: &TikvClient,
    ctx: Context,
    muts: Vec<Mutation>,
    pessimistic_actions: Vec<PrewriteRequestPessimisticAction>,
    pk: Vec<u8>,
    ts: u64,
    for_update_ts: u64,
    use_async_commit: bool,
    try_one_pc: bool,
) {
    let mut prewrite_req = PrewriteRequest::default();
    prewrite_req.set_context(ctx);
    if for_update_ts != 0 {
        prewrite_req.pessimistic_actions = pessimistic_actions;
    }
    prewrite_req.set_mutations(muts.into_iter().collect());
    prewrite_req.primary_lock = pk;
    prewrite_req.start_version = ts;
    prewrite_req.lock_ttl = 3000;
    prewrite_req.for_update_ts = for_update_ts;
    prewrite_req.min_commit_ts = prewrite_req.start_version + 1;
    prewrite_req.use_async_commit = use_async_commit;
    prewrite_req.try_one_pc = try_one_pc;
    let prewrite_resp = client.kv_prewrite(&prewrite_req).unwrap();
    assert!(
        !prewrite_resp.has_region_error(),
        "{:?}",
        prewrite_resp.get_region_error()
    );
    assert!(
        prewrite_resp.errors.is_empty(),
        "{:?}",
        prewrite_resp.get_errors()
    );
}

pub fn try_kv_prewrite_with(
    client: &TikvClient,
    ctx: Context,
    muts: Vec<Mutation>,
    pessimistic_actions: Vec<PrewriteRequestPessimisticAction>,
    pk: Vec<u8>,
    ts: u64,
    for_update_ts: u64,
    use_async_commit: bool,
    try_one_pc: bool,
) -> PrewriteResponse {
    try_kv_prewrite_with_impl(
        client,
        ctx,
        muts,
        pessimistic_actions,
        pk,
        ts,
        for_update_ts,
        use_async_commit,
        try_one_pc,
    )
    .unwrap()
}

pub fn try_kv_prewrite_with_impl(
    client: &TikvClient,
    ctx: Context,
    muts: Vec<Mutation>,
    pessimistic_actions: Vec<PrewriteRequestPessimisticAction>,
    pk: Vec<u8>,
    ts: u64,
    for_update_ts: u64,
    use_async_commit: bool,
    try_one_pc: bool,
) -> grpcio::Result<PrewriteResponse> {
    let mut prewrite_req = PrewriteRequest::default();
    prewrite_req.set_context(ctx);
    if for_update_ts != 0 {
        prewrite_req.pessimistic_actions = pessimistic_actions;
    }
    prewrite_req.set_mutations(muts.into_iter().collect());
    prewrite_req.primary_lock = pk;
    prewrite_req.start_version = ts;
    prewrite_req.lock_ttl = 3000;
    prewrite_req.for_update_ts = for_update_ts;
    prewrite_req.min_commit_ts = prewrite_req.start_version + 1;
    prewrite_req.use_async_commit = use_async_commit;
    prewrite_req.try_one_pc = try_one_pc;
    client.kv_prewrite(&prewrite_req)
}

pub fn try_kv_prewrite(
    client: &TikvClient,
    ctx: Context,
    muts: Vec<Mutation>,
    pk: Vec<u8>,
    ts: u64,
) -> PrewriteResponse {
    try_kv_prewrite_with(client, ctx, muts, vec![], pk, ts, 0, false, false)
}

pub fn try_kv_prewrite_pessimistic(
    client: &TikvClient,
    ctx: Context,
    muts: Vec<Mutation>,
    pk: Vec<u8>,
    ts: u64,
) -> PrewriteResponse {
    let len = muts.len();
    try_kv_prewrite_with(
        client,
        ctx,
        muts,
        vec![DoPessimisticCheck; len],
        pk,
        ts,
        ts,
        false,
        false,
    )
}

pub fn must_kv_prewrite(
    client: &TikvClient,
    ctx: Context,
    muts: Vec<Mutation>,
    pk: Vec<u8>,
    ts: u64,
) {
    must_kv_prewrite_with(client, ctx, muts, vec![], pk, ts, 0, false, false)
}

pub fn must_kv_prewrite_pessimistic(
    client: &TikvClient,
    ctx: Context,
    muts: Vec<Mutation>,
    pk: Vec<u8>,
    ts: u64,
) {
    let len = muts.len();
    must_kv_prewrite_with(
        client,
        ctx,
        muts,
        vec![DoPessimisticCheck; len],
        pk,
        ts,
        ts,
        false,
        false,
    )
}

pub fn must_kv_commit(
    client: &TikvClient,
    ctx: Context,
    keys: Vec<Vec<u8>>,
    start_ts: u64,
    commit_ts: u64,
    expect_commit_ts: u64,
) {
    let mut commit_req = CommitRequest::default();
    commit_req.set_context(ctx);
    commit_req.start_version = start_ts;
    commit_req.set_keys(keys.into_iter().collect());
    commit_req.commit_version = commit_ts;
    let commit_resp = client.kv_commit(&commit_req).unwrap();
    assert!(
        !commit_resp.has_region_error(),
        "{:?}",
        commit_resp.get_region_error()
    );
    assert!(!commit_resp.has_error(), "{:?}", commit_resp.get_error());
    assert_eq!(commit_resp.get_commit_version(), expect_commit_ts);
}

pub fn must_kv_rollback(client: &TikvClient, ctx: Context, keys: Vec<Vec<u8>>, start_ts: u64) {
    let mut rollback_req = BatchRollbackRequest::default();
    rollback_req.set_context(ctx);
    rollback_req.start_version = start_ts;
    rollback_req.set_keys(keys.into_iter().collect());
    let rollback_req = client.kv_batch_rollback(&rollback_req).unwrap();
    assert!(
        !rollback_req.has_region_error(),
        "{:?}",
        rollback_req.get_region_error()
    );
}

pub fn kv_pessimistic_lock(
    client: &TikvClient,
    ctx: Context,
    keys: Vec<Vec<u8>>,
    ts: u64,
    for_update_ts: u64,
    return_values: bool,
) -> PessimisticLockResponse {
    kv_pessimistic_lock_with_ttl(client, ctx, keys, ts, for_update_ts, return_values, 20)
}

pub fn kv_pessimistic_lock_resumable(
    client: &TikvClient,
    ctx: Context,
    keys: Vec<Vec<u8>>,
    ts: u64,
    for_update_ts: u64,
    wait_timeout: Option<i64>,
    return_values: bool,
    check_existence: bool,
) -> PessimisticLockResponse {
    let mut req = PessimisticLockRequest::default();
    req.set_context(ctx);
    let primary = keys[0].clone();
    let mut mutations = vec![];
    for key in keys {
        let mut mutation = Mutation::default();
        mutation.set_op(Op::PessimisticLock);
        mutation.set_key(key);
        mutations.push(mutation);
    }
    req.set_mutations(mutations.into());
    req.primary_lock = primary;
    req.start_version = ts;
    req.for_update_ts = for_update_ts;
    req.lock_ttl = 20;
    req.is_first_lock = false;
    req.wait_timeout = wait_timeout.unwrap_or(-1);
    req.set_wake_up_mode(PessimisticLockWakeUpMode::WakeUpModeForceLock);
    req.return_values = return_values;
    req.check_existence = check_existence;
    client.kv_pessimistic_lock(&req).unwrap()
}

pub fn kv_pessimistic_lock_with_ttl(
    client: &TikvClient,
    ctx: Context,
    keys: Vec<Vec<u8>>,
    ts: u64,
    for_update_ts: u64,
    return_values: bool,
    ttl: u64,
) -> PessimisticLockResponse {
    let mut req = PessimisticLockRequest::default();
    req.set_context(ctx);
    let primary = keys[0].clone();
    let mut mutations = vec![];
    for key in keys {
        let mut mutation = Mutation::default();
        mutation.set_op(Op::PessimisticLock);
        mutation.set_key(key);
        mutations.push(mutation);
    }
    req.set_mutations(mutations.into());
    req.primary_lock = primary;
    req.start_version = ts;
    req.for_update_ts = for_update_ts;
    req.lock_ttl = ttl;
    req.is_first_lock = false;
    req.return_values = return_values;
    client.kv_pessimistic_lock(&req).unwrap()
}

pub fn must_kv_pessimistic_lock(client: &TikvClient, ctx: Context, key: Vec<u8>, ts: u64) {
    let resp = kv_pessimistic_lock(client, ctx, vec![key], ts, ts, false);
    assert!(!resp.has_region_error(), "{:?}", resp.get_region_error());
    assert!(resp.errors.is_empty(), "{:?}", resp.get_errors());
}

pub fn must_kv_pessimistic_rollback(
    client: &TikvClient,
    ctx: Context,
    key: Vec<u8>,
    ts: u64,
    for_update_ts: u64,
) {
    let mut req = PessimisticRollbackRequest::default();
    req.set_context(ctx);
    req.set_keys(vec![key].into_iter().collect());
    req.start_version = ts;
    req.for_update_ts = for_update_ts;
    let resp = client.kv_pessimistic_rollback(&req).unwrap();
    assert!(!resp.has_region_error(), "{:?}", resp.get_region_error());
    assert!(resp.errors.is_empty(), "{:?}", resp.get_errors());
}

pub fn must_kv_pessimistic_rollback_with_scan_first(
    client: &TikvClient,
    ctx: Context,
    ts: u64,
    for_update_ts: u64,
) {
    let mut req = PessimisticRollbackRequest::default();
    req.set_context(ctx);
    req.start_version = ts;
    req.for_update_ts = for_update_ts;
    let resp = client.kv_pessimistic_rollback(&req).unwrap();
    assert!(!resp.has_region_error(), "{:?}", resp.get_region_error());
    assert!(resp.errors.is_empty(), "{:?}", resp.get_errors());
}

pub fn must_check_txn_status(
    client: &TikvClient,
    ctx: Context,
    key: &[u8],
    lock_ts: u64,
    caller_start_ts: u64,
    current_ts: u64,
) -> CheckTxnStatusResponse {
    let mut req = CheckTxnStatusRequest::default();
    req.set_context(ctx);
    req.set_primary_key(key.to_vec());
    req.set_lock_ts(lock_ts);
    req.set_caller_start_ts(caller_start_ts);
    req.set_current_ts(current_ts);

    let resp = client.kv_check_txn_status(&req).unwrap();
    assert!(!resp.has_region_error(), "{:?}", resp.get_region_error());
    assert!(resp.error.is_none(), "{:?}", resp.get_error());
    resp
}

pub fn must_kv_have_locks(
    client: &TikvClient,
    ctx: Context,
    ts: u64,
    start_key: &[u8],
    end_key: &[u8],
    expected_locks: &[(
        // key
        &[u8],
        Op,
        // start_ts
        u64,
        // for_update_ts
        u64,
    )],
) {
    let mut req = ScanLockRequest::default();
    req.set_context(ctx);
    req.set_limit(100);
    req.set_start_key(start_key.to_vec());
    req.set_end_key(end_key.to_vec());
    req.set_max_version(ts);
    let resp = client.kv_scan_lock(&req).unwrap();
    assert!(!resp.has_region_error(), "{:?}", resp.get_region_error());
    assert!(resp.error.is_none(), "{:?}", resp.get_error());

    assert_eq!(
        resp.locks.len(),
        expected_locks.len(),
        "lock count not match, expected: {:?}; got: {:?}",
        expected_locks,
        resp.locks
    );

    for (lock_info, (expected_key, expected_op, expected_start_ts, expected_for_update_ts)) in
        resp.locks.into_iter().zip(expected_locks.iter())
    {
        assert_eq!(lock_info.get_key(), *expected_key);
        assert_eq!(lock_info.get_lock_type(), *expected_op);
        assert_eq!(lock_info.get_lock_version(), *expected_start_ts);
        assert_eq!(lock_info.get_lock_for_update_ts(), *expected_for_update_ts);
    }
}

/// Scan scan_limit number of locks within [start_key, end_key), the returned
/// lock number should equal the input expected_cnt.
pub fn must_lock_cnt(
    client: &TikvClient,
    ctx: Context,
    ts: u64,
    start_key: &[u8],
    end_key: &[u8],
    lock_type: Op,
    expected_cnt: usize,
    scan_limit: usize,
) {
    let mut req = ScanLockRequest::default();
    req.set_context(ctx);
    req.set_limit(scan_limit as u32);
    req.set_start_key(start_key.to_vec());
    req.set_end_key(end_key.to_vec());
    req.set_max_version(ts);
    let resp = client.kv_scan_lock(&req).unwrap();
    assert!(!resp.has_region_error(), "{:?}", resp.get_region_error());
    assert!(resp.error.is_none(), "{:?}", resp.get_error());

    let lock_cnt = resp
        .locks
        .iter()
        .filter(|lock_info| lock_info.get_lock_type() == lock_type)
        .count();

    assert_eq!(
        lock_cnt,
        expected_cnt,
        "lock count not match, expected: {:?}; got: {:?}",
        expected_cnt,
        resp.locks.len()
    );
}

pub fn get_tso(pd_client: &TestPdClient) -> u64 {
    block_on(pd_client.get_tso()).unwrap().into_inner()
}

pub fn get_raft_msg_or_default<M: protobuf::Message + Default>(
    engines: &Engines<RocksEngine, RaftTestEngine>,
    key: &[u8],
) -> M {
    engines
        .kv
        .get_msg_cf(CF_RAFT, key)
        .unwrap()
        .unwrap_or_default()
}

pub fn check_compacted(
    all_engines: &HashMap<u64, Engines<RocksEngine, RaftTestEngine>>,
    before_states: &HashMap<u64, RaftTruncatedState>,
    compact_count: u64,
    must_compacted: bool,
) -> bool {
    // Every peer must have compacted logs, so the truncate log state index/term
    // must > than before.
    let mut compacted_idx = HashMap::default();

    for (&id, engines) in all_engines {
        let mut state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        let after_state = state.take_truncated_state();

        let before_state = &before_states[&id];
        let idx = after_state.get_index();
        let term = after_state.get_term();
        if idx == before_state.get_index() || term == before_state.get_term() {
            if must_compacted {
                panic!(
                    "Should be compacted, but Raft truncated state is not updated: {} state={:?}",
                    id, before_state
                );
            }
            return false;
        }
        if idx - before_state.get_index() < compact_count {
            if must_compacted {
                panic!(
                    "Should be compacted, but compact count is too small: {} {}<{}",
                    id,
                    idx - before_state.get_index(),
                    compact_count
                );
            }
            return false;
        }
        assert!(term > before_state.get_term());
        compacted_idx.insert(id, idx);
    }

    // wait for actual deletion.
    sleep_ms(250);

    for (id, engines) in all_engines {
        for i in 0..compacted_idx[id] {
            if engines.raft.get_entry(1, i).unwrap().is_some() {
                if must_compacted {
                    panic!("Should be compacted, but found entry: {} {}", id, i);
                }
                return false;
            }
        }
    }
    true
}

pub fn must_raw_put(client: &TikvClient, ctx: Context, key: Vec<u8>, value: Vec<u8>) {
    let mut put_req = RawPutRequest::default();
    put_req.set_context(ctx);
    put_req.key = key;
    put_req.value = value;

    let retryable = |err: &kvproto::errorpb::Error| -> bool { err.has_max_timestamp_not_synced() };
    let start = Instant::now_coarse();
    loop {
        let put_resp = client.raw_put(&put_req).unwrap();
        if put_resp.has_region_error() {
            let err = put_resp.get_region_error();
            if retryable(err) && start.saturating_elapsed() < Duration::from_secs(5) {
                debug!("must_raw_put meet region error"; "err" => ?err);
                sleep_ms(100);
                continue;
            }
            panic!(
                "must_raw_put meet region error: {:?}, ctx: {:?}, key: {}, value {}",
                err,
                put_req.get_context(),
                tikv_util::escape(&put_req.key),
                tikv_util::escape(&put_req.value),
            );
        }
        assert!(
            put_resp.get_error().is_empty(),
            "must_raw_put meet error: {:?}",
            put_resp.get_error()
        );
        return;
    }
}

pub fn must_raw_get(client: &TikvClient, ctx: Context, key: Vec<u8>) -> Option<Vec<u8>> {
    let mut get_req = RawGetRequest::default();
    get_req.set_context(ctx);
    get_req.key = key;
    let get_resp = client.raw_get(&get_req).unwrap();
    assert!(
        !get_resp.has_region_error(),
        "{:?}",
        get_resp.get_region_error()
    );
    assert!(
        get_resp.get_error().is_empty(),
        "{:?}",
        get_resp.get_error()
    );
    if get_resp.not_found {
        None
    } else {
        Some(get_resp.value)
    }
}

pub fn must_prepare_flashback(client: &TikvClient, ctx: Context, version: u64, start_ts: u64) {
    let mut prepare_req = PrepareFlashbackToVersionRequest::default();
    prepare_req.set_context(ctx);
    prepare_req.set_start_ts(start_ts);
    prepare_req.set_version(version);
    prepare_req.set_start_key(b"a".to_vec());
    prepare_req.set_end_key(b"z".to_vec());
    client
        .kv_prepare_flashback_to_version(&prepare_req)
        .unwrap();
}

pub fn must_finish_flashback(
    client: &TikvClient,
    ctx: Context,
    version: u64,
    start_ts: u64,
    commit_ts: u64,
) {
    let mut req = FlashbackToVersionRequest::default();
    req.set_context(ctx);
    req.set_start_ts(start_ts);
    req.set_commit_ts(commit_ts);
    req.set_version(version);
    req.set_start_key(b"a".to_vec());
    req.set_end_key(b"z".to_vec());
    let resp = client.kv_flashback_to_version(&req).unwrap();
    assert!(!resp.has_region_error());
    assert!(resp.get_error().is_empty());
}

pub fn must_flashback_to_version(
    client: &TikvClient,
    ctx: Context,
    version: u64,
    start_ts: u64,
    commit_ts: u64,
) {
    must_prepare_flashback(client, ctx.clone(), version, start_ts);
    must_finish_flashback(client, ctx, version, start_ts, commit_ts);
}

// A helpful wrapper to make the test logic clear
pub struct PeerClient {
    pub cli: TikvClient,
    pub ctx: Context,
}

impl PeerClient {
    pub fn new<EK: KvEngineWithRocks>(
        cluster: &Cluster<EK, ServerCluster<EK>>,
        region_id: u64,
        peer: metapb::Peer,
    ) -> PeerClient {
        let cli = {
            let env = Arc::new(Environment::new(1));
            let channel =
                ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(peer.get_store_id()));
            TikvClient::new(channel)
        };
        let ctx = {
            let epoch = cluster.get_region_epoch(region_id);
            let mut ctx = Context::default();
            ctx.set_region_id(region_id);
            ctx.set_peer(peer);
            ctx.set_region_epoch(epoch);
            ctx
        };
        PeerClient { cli, ctx }
    }

    pub fn kv_read(&self, key: Vec<u8>, ts: u64) -> GetResponse {
        kv_read(&self.cli, self.ctx.clone(), key, ts)
    }

    pub fn must_kv_read_equal(&self, key: Vec<u8>, val: Vec<u8>, ts: u64) {
        must_kv_read_equal(&self.cli, self.ctx.clone(), key, val, ts)
    }

    pub fn must_kv_write(&self, pd_client: &TestPdClient, kvs: Vec<Mutation>, pk: Vec<u8>) -> u64 {
        must_kv_write(pd_client, &self.cli, self.ctx.clone(), kvs, pk)
    }

    pub fn must_kv_prewrite(&self, muts: Vec<Mutation>, pk: Vec<u8>, ts: u64) {
        must_kv_prewrite(&self.cli, self.ctx.clone(), muts, pk, ts)
    }

    pub fn try_kv_prewrite(
        &self,
        muts: Vec<Mutation>,
        pk: Vec<u8>,
        ts: u64,
        opt: DiskFullOpt,
    ) -> PrewriteResponse {
        let mut ctx = self.ctx.clone();
        ctx.disk_full_opt = opt;
        try_kv_prewrite(&self.cli, ctx, muts, pk, ts)
    }

    pub fn must_kv_prewrite_async_commit(&self, muts: Vec<Mutation>, pk: Vec<u8>, ts: u64) {
        must_kv_prewrite_with(
            &self.cli,
            self.ctx.clone(),
            muts,
            vec![],
            pk,
            ts,
            0,
            true,
            false,
        )
    }

    pub fn must_kv_prewrite_one_pc(&self, muts: Vec<Mutation>, pk: Vec<u8>, ts: u64) {
        must_kv_prewrite_with(
            &self.cli,
            self.ctx.clone(),
            muts,
            vec![],
            pk,
            ts,
            0,
            false,
            true,
        )
    }

    pub fn must_kv_commit(&self, keys: Vec<Vec<u8>>, start_ts: u64, commit_ts: u64) {
        must_kv_commit(
            &self.cli,
            self.ctx.clone(),
            keys,
            start_ts,
            commit_ts,
            commit_ts,
        )
    }

    pub fn must_kv_rollback(&self, keys: Vec<Vec<u8>>, start_ts: u64) {
        must_kv_rollback(&self.cli, self.ctx.clone(), keys, start_ts)
    }

    pub fn must_kv_pessimistic_lock(&self, key: Vec<u8>, ts: u64) {
        must_kv_pessimistic_lock(&self.cli, self.ctx.clone(), key, ts)
    }

    pub fn must_kv_pessimistic_rollback(&self, key: Vec<u8>, ts: u64) {
        must_kv_pessimistic_rollback(&self.cli, self.ctx.clone(), key, ts, ts)
    }
}

pub fn peer_on_store(region: &metapb::Region, store_id: u64) -> metapb::Peer {
    region
        .get_peers()
        .iter()
        .find(|p| p.get_store_id() == store_id)
        .unwrap()
        .clone()
}

pub fn wait_for_synced<EK: KvEngineWithRocks>(
    cluster: &mut Cluster<EK, ServerCluster<EK>>,
    node_id: u64,
    region_id: u64,
) {
    let mut storage = cluster
        .sim
        .read()
        .unwrap()
        .storages
        .get(&node_id)
        .unwrap()
        .clone();
    let leader = cluster.leader_of_region(region_id).unwrap();
    let epoch = cluster.get_region_epoch(region_id);
    let mut ctx = Context::default();
    ctx.set_region_id(region_id);
    ctx.set_peer(leader);
    ctx.set_region_epoch(epoch);
    let snap_ctx = SnapContext {
        pb_ctx: &ctx,
        ..Default::default()
    };
    let snapshot = storage.snapshot(snap_ctx).unwrap();
    let txn_ext = snapshot.txn_ext.clone().unwrap();
    for retry in 0..10 {
        if txn_ext.is_max_ts_synced() {
            break;
        }
        thread::sleep(Duration::from_millis(1 << retry));
    }
    assert!(snapshot.ext().is_max_ts_synced());
}

pub fn test_delete_range<EK: KvEngineWithRocks, T: Simulator<EK>>(
    cluster: &mut Cluster<EK, T>,
    cf: CfName,
) {
    let data_set: Vec<_> = (1..500)
        .map(|i| {
            (
                format!("key{:08}", i).into_bytes(),
                format!("value{}", i).into_bytes(),
            )
        })
        .collect();
    for kvs in data_set.chunks(50) {
        let requests = kvs.iter().map(|(k, v)| new_put_cf_cmd(cf, k, v)).collect();
        // key9 is always the last region.
        cluster.batch_put(b"key9", requests).unwrap();
    }

    // delete_range request with notify_only set should not actually delete data.
    cluster.must_notify_delete_range_cf(cf, b"", b"");

    let mut rng = rand::thread_rng();
    for _ in 0..50 {
        let (k, v) = data_set.choose(&mut rng).unwrap();
        assert_eq!(cluster.get_cf(cf, k).unwrap(), *v);
    }

    // Empty keys means the whole range.
    cluster.must_delete_range_cf(cf, b"", b"");

    for _ in 0..50 {
        let k = &data_set.choose(&mut rng).unwrap().0;
        assert!(cluster.get_cf(cf, k).is_none());
    }
}

pub fn put_with_timeout<EK: KvEngineWithRocks, T: Simulator<EK>>(
    cluster: &mut Cluster<EK, T>,
    node_id: u64,
    key: &[u8],
    value: &[u8],
    timeout: Duration,
) -> Result<RaftCmdResponse> {
    let mut region = cluster.get_region(key);
    let region_id = region.get_id();
    let req = new_request(
        region_id,
        region.take_region_epoch(),
        vec![new_put_cf_cmd(CF_DEFAULT, key, value)],
        false,
    );
    cluster.call_command_on_node(node_id, req, timeout)
}

pub fn wait_down_peers<EK: KvEngineWithRocks, T: Simulator<EK>>(
    cluster: &Cluster<EK, T>,
    count: u64,
    peer: Option<u64>,
) {
    let mut peers = cluster.get_down_peers();
    for _ in 1..1000 {
        if peers.len() == count as usize && peer.as_ref().map_or(true, |p| peers.contains_key(p)) {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
        peers = cluster.get_down_peers();
    }
    panic!(
        "got {:?}, want {} peers which should include {:?}",
        peers, count, peer
    );
}
