// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::Write,
    path::Path,
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};

use collections::HashMap;
use encryption_export::{
    data_key_manager_from_config, DataKeyManager, FileConfig, MasterKeyConfig,
};
use engine_rocks::{config::BlobRunMode, raw::DB, Compat, RocksEngine, RocksSnapshot};
use engine_test::raft::RaftTestEngine;
use engine_traits::{
    Engines, Iterable, Peekable, RaftEngineDebug, RaftEngineReadOnly, TabletFactory, ALL_CFS,
    CF_DEFAULT, CF_RAFT,
};
use file_system::IORateLimiter;
use futures::executor::block_on;
use grpcio::{ChannelBuilder, Environment};
use kvproto::{
    encryptionpb::EncryptionMethod,
    kvrpcpb::*,
    metapb::{self, RegionEpoch},
    pdpb::{
        ChangePeer, ChangePeerV2, CheckPolicy, Merge, RegionHeartbeatResponse, SplitRegion,
        TransferLeader,
    },
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
use raft::eraftpb::ConfChangeType;
pub use raftstore::store::util::{find_peer, new_learner_peer, new_peer};
use raftstore::{
    store::{fsm::RaftRouter, *},
    Result,
};
use rand::RngCore;
use server::server::ConfiguredRaftEngine;
use tempfile::TempDir;
use tikv::{config::*, server::KvEngineFactoryBuilder, storage::point_key_range};
use tikv_util::{config::*, escape, time::ThreadReadId, worker::LazyWorker, HandyRwLock};
use txn_types::Key;

use crate::{Cluster, Config, ServerCluster, Simulator, TestPdClient};

pub fn must_get(engine: &Arc<DB>, cf: &str, key: &[u8], value: Option<&[u8]>) {
    for _ in 1..300 {
        let res = engine.c().get_value_cf(cf, &keys::data_key(key)).unwrap();
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
    let res = engine.c().get_value_cf(cf, &keys::data_key(key)).unwrap();
    if value.is_none() && res.is_none()
        || value.is_some() && res.is_some() && value.unwrap() == &*res.unwrap()
    {
        return;
    }
    panic!(
        "can't get value {:?} for key {}",
        value.map(escape),
        log_wrappers::hex_encode_upper(key)
    )
}

pub fn must_get_equal(engine: &Arc<DB>, key: &[u8], value: &[u8]) {
    must_get(engine, "default", key, Some(value));
}

pub fn must_get_none(engine: &Arc<DB>, key: &[u8]) {
    must_get(engine, "default", key, None);
}

pub fn must_get_cf_equal(engine: &Arc<DB>, cf: &str, key: &[u8], value: &[u8]) {
    must_get(engine, cf, key, Some(value));
}

pub fn must_get_cf_none(engine: &Arc<DB>, cf: &str, key: &[u8]) {
    must_get(engine, cf, key, None);
}

pub fn must_region_cleared(engine: &Engines<RocksEngine, RaftTestEngine>, region: &metapb::Region) {
    let id = region.get_id();
    let state_key = keys::region_state_key(id);
    let state: RegionLocalState = engine.kv.get_msg_cf(CF_RAFT, &state_key).unwrap().unwrap();
    assert_eq!(state.get_state(), PeerState::Tombstone, "{:?}", state);
    let start_key = keys::data_key(region.get_start_key());
    let end_key = keys::data_key(region.get_end_key());
    for cf in ALL_CFS {
        engine
            .kv
            .scan_cf(cf, &start_key, &end_key, false, |k, v| {
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
    static ref TEST_CONFIG: TiKvConfig = {
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let common_test_cfg = manifest_dir.join("src/common-test.toml");
        TiKvConfig::from_file(&common_test_cfg, None).unwrap_or_else(|e| {
            panic!(
                "invalid auto generated configuration file {}, err {}",
                manifest_dir.display(),
                e
            );
        })
    };
}

pub fn new_tikv_config(cluster_id: u64) -> TiKvConfig {
    let mut cfg = TEST_CONFIG.clone();
    cfg.server.cluster_id = cluster_id;
    cfg
}

pub fn new_tikv_config_with_api_ver(cluster_id: u64, api_ver: ApiVersion) -> TiKvConfig {
    let mut cfg = TEST_CONFIG.clone();
    cfg.server.cluster_id = cluster_id;
    cfg.storage.set_api_version(api_ver);
    cfg
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

#[allow(dead_code)]
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

pub fn new_pd_change_peer(
    change_type: ConfChangeType,
    peer: metapb::Peer,
) -> RegionHeartbeatResponse {
    let mut change_peer = ChangePeer::default();
    change_peer.set_change_type(change_type);
    change_peer.set_peer(peer);

    let mut resp = RegionHeartbeatResponse::default();
    resp.set_change_peer(change_peer);
    resp
}

pub fn new_pd_change_peer_v2(changes: Vec<ChangePeer>) -> RegionHeartbeatResponse {
    let mut change_peer = ChangePeerV2::default();
    change_peer.set_changes(changes.into());

    let mut resp = RegionHeartbeatResponse::default();
    resp.set_change_peer_v2(change_peer);
    resp
}

pub fn new_split_region(policy: CheckPolicy, keys: Vec<Vec<u8>>) -> RegionHeartbeatResponse {
    let mut split_region = SplitRegion::default();
    split_region.set_policy(policy);
    split_region.set_keys(keys.into());
    let mut resp = RegionHeartbeatResponse::default();
    resp.set_split_region(split_region);
    resp
}

pub fn new_pd_transfer_leader(
    peer: metapb::Peer,
    peers: Vec<metapb::Peer>,
) -> RegionHeartbeatResponse {
    let mut transfer_leader = TransferLeader::default();
    transfer_leader.set_peer(peer);
    transfer_leader.set_peers(peers.into());

    let mut resp = RegionHeartbeatResponse::default();
    resp.set_transfer_leader(transfer_leader);
    resp
}

pub fn new_pd_merge_region(target_region: metapb::Region) -> RegionHeartbeatResponse {
    let mut merge = Merge::default();
    merge.set_target(target_region);

    let mut resp = RegionHeartbeatResponse::default();
    resp.set_merge(merge);
    resp
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

pub fn make_cb(cmd: &RaftCmdRequest) -> (Callback<RocksSnapshot>, mpsc::Receiver<RaftCmdResponse>) {
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

    let (tx, rx) = mpsc::channel();
    let mut detector = CallbackLeakDetector::default();
    let cb = if is_read {
        Callback::Read(Box::new(move |resp: ReadResponse<RocksSnapshot>| {
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

pub fn make_cb_ext(
    cmd: &RaftCmdRequest,
    proposed: Option<ExtCallback>,
    committed: Option<ExtCallback>,
) -> (Callback<RocksSnapshot>, mpsc::Receiver<RaftCmdResponse>) {
    let (cb, receiver) = make_cb(cmd);
    if let Callback::Write { cb, .. } = cb {
        (Callback::write_ext(cb, proposed, committed), receiver)
    } else {
        (cb, receiver)
    }
}

// Issue a read request on the specified peer.
pub fn read_on_peer<T: Simulator>(
    cluster: &mut Cluster<T>,
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
    cluster.read(None, request, timeout)
}

pub fn async_read_on_peer<T: Simulator>(
    cluster: &mut Cluster<T>,
    peer: metapb::Peer,
    region: metapb::Region,
    key: &[u8],
    read_quorum: bool,
    replica_read: bool,
) -> mpsc::Receiver<RaftCmdResponse> {
    let node_id = peer.get_store_id();
    let mut request = new_request(
        region.get_id(),
        region.get_region_epoch().clone(),
        vec![new_get_cmd(key)],
        read_quorum,
    );
    request.mut_header().set_peer(peer);
    request.mut_header().set_replica_read(replica_read);
    let (tx, rx) = mpsc::sync_channel(1);
    let cb = Callback::Read(Box::new(move |resp| drop(tx.send(resp.response))));
    cluster.sim.wl().async_read(node_id, None, request, cb);
    rx
}

pub fn batch_read_on_peer<T: Simulator>(
    cluster: &mut Cluster<T>,
    requests: &[(metapb::Peer, metapb::Region)],
) -> Vec<ReadResponse<RocksSnapshot>> {
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
        let cb = Callback::Read(Box::new(move |resp| {
            t.send((len, resp)).unwrap();
        }));
        cluster
            .sim
            .wl()
            .async_read(node_id, batch_id.clone(), request, cb);
        len += 1;
    }
    while results.len() < len {
        results.push(rx.recv_timeout(Duration::from_secs(1)).unwrap());
    }
    results.sort_by_key(|resp| resp.0);
    results.into_iter().map(|resp| resp.1).collect()
}

pub fn read_index_on_peer<T: Simulator>(
    cluster: &mut Cluster<T>,
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
    cluster.read(None, request, timeout)
}

pub fn async_read_index_on_peer<T: Simulator>(
    cluster: &mut Cluster<T>,
    peer: metapb::Peer,
    region: metapb::Region,
    key: &[u8],
    read_quorum: bool,
) -> mpsc::Receiver<RaftCmdResponse> {
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
    let (tx, rx) = mpsc::sync_channel(1);
    let cb = Callback::Read(Box::new(move |resp| drop(tx.send(resp.response))));
    cluster.sim.wl().async_read(node_id, None, request, cb);
    rx
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

pub fn must_read_on_peer<T: Simulator>(
    cluster: &mut Cluster<T>,
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

pub fn must_error_read_on_peer<T: Simulator>(
    cluster: &mut Cluster<T>,
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

pub fn must_contains_error(resp: &RaftCmdResponse, msg: &str) {
    let header = resp.get_header();
    assert!(header.has_error());
    let err_msg = header.get_error().get_message();
    assert!(err_msg.contains(msg), "{:?}", resp);
}

pub fn create_test_engine(
    // TODO: pass it in for all cases.
    router: Option<RaftRouter<RocksEngine, RaftTestEngine>>,
    limiter: Option<Arc<IORateLimiter>>,
    cfg: &Config,
) -> (
    Engines<RocksEngine, RaftTestEngine>,
    Option<Arc<DataKeyManager>>,
    TempDir,
    LazyWorker<String>,
) {
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

    let raft_engine = RaftTestEngine::build(&cfg, &env, &key_manager, &cache);

    let mut builder =
        KvEngineFactoryBuilder::new(env, &cfg, dir.path()).sst_recovery_sender(Some(scheduler));
    if let Some(cache) = cache {
        builder = builder.block_cache(cache);
    }
    if let Some(router) = router {
        builder = builder.compaction_filter_router(router);
    }
    let factory = builder.build();
    let engine = factory.create_tablet().unwrap();
    let engines = Engines::new(engine, raft_engine);
    (engines, key_manager, dir, sst_worker)
}

pub fn configure_for_request_snapshot<T: Simulator>(cluster: &mut Cluster<T>) {
    // We don't want to generate snapshots due to compact log.
    cluster.cfg.raft_store.raft_log_gc_threshold = 1000;
    cluster.cfg.raft_store.raft_log_gc_count_limit = 1000;
    cluster.cfg.raft_store.raft_log_gc_size_limit = ReadableSize::mb(20);
}

pub fn configure_for_hibernate<T: Simulator>(cluster: &mut Cluster<T>) {
    // Uses long check interval to make leader keep sleeping during tests.
    cluster.cfg.raft_store.abnormal_leader_missing_duration = ReadableDuration::secs(20);
    cluster.cfg.raft_store.max_leader_missing_duration = ReadableDuration::secs(40);
    cluster.cfg.raft_store.peer_stale_state_check_interval = ReadableDuration::secs(10);
}

pub fn configure_for_snapshot<T: Simulator>(cluster: &mut Cluster<T>) {
    // Truncate the log quickly so that we can force sending snapshot.
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(20);
    cluster.cfg.raft_store.raft_log_gc_count_limit = 2;
    cluster.cfg.raft_store.merge_max_log_gap = 1;
    cluster.cfg.raft_store.snap_mgr_gc_tick_interval = ReadableDuration::millis(50);
}

pub fn configure_for_merge<T: Simulator>(cluster: &mut Cluster<T>) {
    // Avoid log compaction which will prevent merge.
    cluster.cfg.raft_store.raft_log_gc_threshold = 1000;
    cluster.cfg.raft_store.raft_log_gc_count_limit = 1000;
    cluster.cfg.raft_store.raft_log_gc_size_limit = ReadableSize::mb(20);
    // Make merge check resume quickly.
    cluster.cfg.raft_store.merge_check_tick_interval = ReadableDuration::millis(100);
    // When isolated, follower relies on stale check tick to detect failure leader,
    // choose a smaller number to make it recover faster.
    cluster.cfg.raft_store.peer_stale_state_check_interval = ReadableDuration::millis(500);
}

pub fn ignore_merge_target_integrity<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.cfg.raft_store.dev_assert = false;
    cluster.pd_client.ignore_merge_target_integrity();
}

pub fn configure_for_lease_read<T: Simulator>(
    cluster: &mut Cluster<T>,
    base_tick_ms: Option<u64>,
    election_ticks: Option<usize>,
) -> Duration {
    if let Some(base_tick_ms) = base_tick_ms {
        cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(base_tick_ms);
    }
    let base_tick_interval = cluster.cfg.raft_store.raft_base_tick_interval.0;
    if let Some(election_ticks) = election_ticks {
        cluster.cfg.raft_store.raft_election_timeout_ticks = election_ticks;
    }
    let election_ticks = cluster.cfg.raft_store.raft_election_timeout_ticks as u32;
    let election_timeout = base_tick_interval * election_ticks;
    // Adjust max leader lease.
    cluster.cfg.raft_store.raft_store_max_leader_lease =
        ReadableDuration(election_timeout - base_tick_interval);
    // Use large peer check interval, abnormal and max leader missing duration to make a valid config,
    // that is election timeout x 2 < peer stale state check < abnormal < max leader missing duration.
    cluster.cfg.raft_store.peer_stale_state_check_interval = ReadableDuration(election_timeout * 3);
    cluster.cfg.raft_store.abnormal_leader_missing_duration =
        ReadableDuration(election_timeout * 4);
    cluster.cfg.raft_store.max_leader_missing_duration = ReadableDuration(election_timeout * 5);

    election_timeout
}

pub fn configure_for_enable_titan<T: Simulator>(
    cluster: &mut Cluster<T>,
    min_blob_size: ReadableSize,
) {
    cluster.cfg.rocksdb.titan.enabled = true;
    cluster.cfg.rocksdb.titan.purge_obsolete_files_period = ReadableDuration::secs(1);
    cluster.cfg.rocksdb.titan.max_background_gc = 10;
    cluster.cfg.rocksdb.defaultcf.titan.min_blob_size = min_blob_size;
    cluster.cfg.rocksdb.defaultcf.titan.blob_run_mode = BlobRunMode::Normal;
    cluster.cfg.rocksdb.defaultcf.titan.min_gc_batch_size = ReadableSize::kb(0);
}

pub fn configure_for_disable_titan<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.cfg.rocksdb.titan.enabled = false;
}

pub fn configure_for_encryption<T: Simulator>(cluster: &mut Cluster<T>) {
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

/// Keep putting random kvs until specified size limit is reached.
pub fn put_till_size<T: Simulator>(
    cluster: &mut Cluster<T>,
    limit: u64,
    range: &mut dyn Iterator<Item = u64>,
) -> Vec<u8> {
    put_cf_till_size(cluster, CF_DEFAULT, limit, range)
}

pub fn put_cf_till_size<T: Simulator>(
    cluster: &mut Cluster<T>,
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

pub fn kv_read(client: &TikvClient, ctx: Context, key: Vec<u8>, ts: u64) -> GetResponse {
    let mut get_req = GetRequest::default();
    get_req.set_context(ctx);
    get_req.set_key(key);
    get_req.set_version(ts);
    client.kv_get(&get_req).unwrap()
}

pub fn must_kv_prewrite_with(
    client: &TikvClient,
    ctx: Context,
    muts: Vec<Mutation>,
    pk: Vec<u8>,
    ts: u64,
    for_update_ts: u64,
    use_async_commit: bool,
    try_one_pc: bool,
) {
    let mut prewrite_req = PrewriteRequest::default();
    prewrite_req.set_context(ctx);
    if for_update_ts != 0 {
        prewrite_req.is_pessimistic_lock = vec![true; muts.len()];
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

// Disk full test interface.
pub fn try_kv_prewrite_with(
    client: &TikvClient,
    ctx: Context,
    muts: Vec<Mutation>,
    pk: Vec<u8>,
    ts: u64,
    for_update_ts: u64,
    use_async_commit: bool,
    try_one_pc: bool,
) -> PrewriteResponse {
    let mut prewrite_req = PrewriteRequest::default();
    prewrite_req.set_context(ctx);
    if for_update_ts != 0 {
        prewrite_req.is_pessimistic_lock = vec![true; muts.len()];
    }
    prewrite_req.set_mutations(muts.into_iter().collect());
    prewrite_req.primary_lock = pk;
    prewrite_req.start_version = ts;
    prewrite_req.lock_ttl = 3000;
    prewrite_req.for_update_ts = for_update_ts;
    prewrite_req.min_commit_ts = prewrite_req.start_version + 1;
    prewrite_req.use_async_commit = use_async_commit;
    prewrite_req.try_one_pc = try_one_pc;
    client.kv_prewrite(&prewrite_req).unwrap()
}

pub fn try_kv_prewrite(
    client: &TikvClient,
    ctx: Context,
    muts: Vec<Mutation>,
    pk: Vec<u8>,
    ts: u64,
) -> PrewriteResponse {
    try_kv_prewrite_with(client, ctx, muts, pk, ts, 0, false, false)
}

pub fn try_kv_prewrite_pessimistic(
    client: &TikvClient,
    ctx: Context,
    muts: Vec<Mutation>,
    pk: Vec<u8>,
    ts: u64,
) -> PrewriteResponse {
    try_kv_prewrite_with(client, ctx, muts, pk, ts, ts, false, false)
}

pub fn must_kv_prewrite(
    client: &TikvClient,
    ctx: Context,
    muts: Vec<Mutation>,
    pk: Vec<u8>,
    ts: u64,
) {
    must_kv_prewrite_with(client, ctx, muts, pk, ts, 0, false, false)
}

pub fn must_kv_prewrite_pessimistic(
    client: &TikvClient,
    ctx: Context,
    muts: Vec<Mutation>,
    pk: Vec<u8>,
    ts: u64,
) {
    must_kv_prewrite_with(client, ctx, muts, pk, ts, ts, false, false)
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

pub fn must_kv_pessimistic_rollback(client: &TikvClient, ctx: Context, key: Vec<u8>, ts: u64) {
    let mut req = PessimisticRollbackRequest::default();
    req.set_context(ctx);
    req.set_keys(vec![key].into_iter().collect());
    req.start_version = ts;
    req.for_update_ts = ts;
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

pub fn must_physical_scan_lock(
    client: &TikvClient,
    ctx: Context,
    max_ts: u64,
    start_key: &[u8],
    limit: usize,
) -> Vec<LockInfo> {
    let mut req = PhysicalScanLockRequest::default();
    req.set_context(ctx);
    req.set_max_ts(max_ts);
    req.set_start_key(start_key.to_owned());
    req.set_limit(limit as _);
    let mut resp = client.physical_scan_lock(&req).unwrap();
    resp.take_locks().into()
}

pub fn register_lock_observer(client: &TikvClient, max_ts: u64) -> RegisterLockObserverResponse {
    let mut req = RegisterLockObserverRequest::default();
    req.set_max_ts(max_ts);
    client.register_lock_observer(&req).unwrap()
}

pub fn must_register_lock_observer(client: &TikvClient, max_ts: u64) {
    let resp = register_lock_observer(client, max_ts);
    assert!(resp.get_error().is_empty(), "{:?}", resp.get_error());
}

pub fn check_lock_observer(client: &TikvClient, max_ts: u64) -> CheckLockObserverResponse {
    let mut req = CheckLockObserverRequest::default();
    req.set_max_ts(max_ts);
    client.check_lock_observer(&req).unwrap()
}

pub fn must_check_lock_observer(client: &TikvClient, max_ts: u64, clean: bool) -> Vec<LockInfo> {
    let mut resp = check_lock_observer(client, max_ts);
    assert!(resp.get_error().is_empty(), "{:?}", resp.get_error());
    assert_eq!(resp.get_is_clean(), clean);
    resp.take_locks().into()
}

pub fn remove_lock_observer(client: &TikvClient, max_ts: u64) -> RemoveLockObserverResponse {
    let mut req = RemoveLockObserverRequest::default();
    req.set_max_ts(max_ts);
    client.remove_lock_observer(&req).unwrap()
}

pub fn must_remove_lock_observer(client: &TikvClient, max_ts: u64) {
    let resp = remove_lock_observer(client, max_ts);
    assert!(resp.get_error().is_empty(), "{:?}", resp.get_error());
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
    // Every peer must have compacted logs, so the truncate log state index/term must > than before.
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

// A helpful wrapper to make the test logic clear
pub struct PeerClient {
    pub cli: TikvClient,
    pub ctx: Context,
}

impl PeerClient {
    pub fn new(cluster: &Cluster<ServerCluster>, region_id: u64, peer: metapb::Peer) -> PeerClient {
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
        must_kv_prewrite_with(&self.cli, self.ctx.clone(), muts, pk, ts, 0, true, false)
    }

    pub fn must_kv_prewrite_one_pc(&self, muts: Vec<Mutation>, pk: Vec<u8>, ts: u64) {
        must_kv_prewrite_with(&self.cli, self.ctx.clone(), muts, pk, ts, 0, false, true)
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
        must_kv_pessimistic_rollback(&self.cli, self.ctx.clone(), key, ts)
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
