// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;
use std::{thread, u64};

use rand::RngCore;
use tempfile::{Builder, TempDir};

use kvproto::metapb::{self, RegionEpoch};
use kvproto::pdpb::{
    ChangePeer, CheckPolicy, Merge, RegionHeartbeatResponse, SplitRegion, TransferLeader,
};
use kvproto::raft_cmdpb::{AdminCmdType, CmdType, StatusCmdType};
use kvproto::raft_cmdpb::{AdminRequest, RaftCmdRequest, RaftCmdResponse, Request, StatusRequest};
use kvproto::raft_serverpb::{PeerState, RaftLocalState, RegionLocalState};
use raft::eraftpb::ConfChangeType;

use engine::rocks::util::config::BlobRunMode;
use engine::rocks::{CompactionJobInfo, DB};
use engine::*;
use engine_rocks::CompactionListener;
use engine_rocks::RocksEngine;
use raftstore::store::fsm::RaftRouter;
use raftstore::store::*;
use raftstore::Result;
use tikv::config::*;
use tikv::server::{lock_manager::Config as PessimisticTxnConfig, Config as ServerConfig};
use tikv::storage::config::{Config as StorageConfig, DEFAULT_ROCKSDB_SUB_DIR};
use tikv_util::config::*;
use tikv_util::{escape, HandyRwLock};

use super::*;

use engine_traits::{ALL_CFS, CF_DEFAULT, CF_RAFT};
pub use raftstore::store::util::{find_peer, new_learner_peer, new_peer};

pub fn must_get(engine: &Arc<DB>, cf: &str, key: &[u8], value: Option<&[u8]>) {
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
    debug!("last try to get {}", hex::encode_upper(key));
    let res = engine.get_value_cf(cf, &keys::data_key(key)).unwrap();
    if value.is_none() && res.is_none()
        || value.is_some() && res.is_some() && value.unwrap() == &*res.unwrap()
    {
        return;
    }
    panic!(
        "can't get value {:?} for key {}",
        value.map(escape),
        hex::encode_upper(key)
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

pub fn must_region_cleared(engine: &Engines, region: &metapb::Region) {
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
    let log_min_key = keys::raft_log_key(id, 0);
    let log_max_key = keys::raft_log_key(id, u64::MAX);
    engine
        .raft
        .scan(&log_min_key, &log_max_key, false, |k, v| {
            panic!("[region {}] unexpected log ({:?}, {:?})", id, k, v);
        })
        .unwrap();
    let state_key = keys::raft_state_key(id);
    let state: Option<RaftLocalState> = engine.raft.get_msg(&state_key).unwrap();
    assert!(
        state.is_none(),
        "[region {}] raft state key should be removed: {:?}",
        id,
        state
    );
}

pub fn new_store_cfg() -> Config {
    Config {
        sync_log: false,
        raft_base_tick_interval: ReadableDuration::millis(10),
        raft_heartbeat_ticks: 2,
        raft_election_timeout_ticks: 25,
        raft_log_gc_tick_interval: ReadableDuration::millis(100),
        raft_log_gc_threshold: 1,
        // Use a value of 3 seconds as max_leader_missing_duration just for test.
        // In production environment, the value of max_leader_missing_duration
        // should be configured far beyond the election timeout.
        max_leader_missing_duration: ReadableDuration::secs(2),
        // To make a valid config, use a value of 2 seconds as
        // abnormal_leader_missing_duration and set
        // peer_stale_state_check_interval to 1 second.
        abnormal_leader_missing_duration: ReadableDuration::millis(1500),
        peer_stale_state_check_interval: ReadableDuration::secs(1),
        pd_heartbeat_tick_interval: ReadableDuration::millis(20),
        region_split_check_diff: ReadableSize(10000),
        report_region_flow_interval: ReadableDuration::millis(100),
        raft_store_max_leader_lease: ReadableDuration::millis(250),
        raft_reject_transfer_leader_duration: ReadableDuration::secs(0),
        clean_stale_peer_delay: ReadableDuration::secs(0),
        allow_remove_leader: true,
        merge_check_tick_interval: ReadableDuration::millis(100),
        ..Config::default()
    }
}

pub fn new_server_config(cluster_id: u64) -> ServerConfig {
    ServerConfig {
        cluster_id,
        addr: "127.0.0.1:0".to_owned(),
        grpc_concurrency: 1,
        // Considering connection selection algo is involved, maybe
        // use 2 or larger value here?
        grpc_raft_conn_num: 1,
        // Disable stats concurrency. procinfo performs too bad without optimization,
        // disable it to save CPU for real tests.
        stats_concurrency: 0,
        ..ServerConfig::default()
    }
}

pub fn new_readpool_cfg() -> ReadPoolConfig {
    ReadPoolConfig {
        unify_read_pool: false,
        unified: UnifiedReadPoolConfig {
            min_thread_count: 1,
            max_thread_count: 1,
            ..UnifiedReadPoolConfig::default()
        },
        storage: StorageReadPoolConfig {
            high_concurrency: 1,
            normal_concurrency: 1,
            low_concurrency: 1,
            ..StorageReadPoolConfig::default()
        },
        coprocessor: CoprReadPoolConfig {
            high_concurrency: 1,
            normal_concurrency: 1,
            low_concurrency: 1,
            ..CoprReadPoolConfig::default()
        },
    }
}

pub fn new_pessimistic_txn_cfg() -> PessimisticTxnConfig {
    PessimisticTxnConfig {
        // Use a large value here since tests run slowly in CI.
        wait_for_lock_timeout: 3000,
        wake_up_delay_duration: 100,
        ..PessimisticTxnConfig::default()
    }
}

pub fn new_tikv_config(cluster_id: u64) -> TiKvConfig {
    TiKvConfig {
        storage: StorageConfig {
            scheduler_worker_pool_size: 1,
            scheduler_concurrency: 10,
            ..StorageConfig::default()
        },
        server: new_server_config(cluster_id),
        raft_store: new_store_cfg(),
        readpool: new_readpool_cfg(),
        pessimistic_txn: new_pessimistic_txn_cfg(),
        ..TiKvConfig::default()
    }
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

pub fn new_split_region(policy: CheckPolicy, keys: Vec<Vec<u8>>) -> RegionHeartbeatResponse {
    let mut split_region = SplitRegion::default();
    split_region.set_policy(policy);
    split_region.set_keys(keys.into());
    let mut resp = RegionHeartbeatResponse::default();
    resp.set_split_region(split_region);
    resp
}

pub fn new_pd_transfer_leader(peer: metapb::Peer) -> RegionHeartbeatResponse {
    let mut transfer_leader = TransferLeader::default();
    transfer_leader.set_peer(peer);

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

pub fn make_cb(cmd: &RaftCmdRequest) -> (Callback<RocksEngine>, mpsc::Receiver<RaftCmdResponse>) {
    let mut is_read;
    let mut is_write;
    is_read = cmd.has_status_request();
    is_write = cmd.has_admin_request();
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
    let cb = if is_read {
        Callback::Read(Box::new(move |resp: ReadResponse<RocksEngine>| {
            // we don't care error actually.
            let _ = tx.send(resp.response);
        }))
    } else {
        Callback::Write(Box::new(move |resp: WriteResponse| {
            // we don't care error actually.
            let _ = tx.send(resp.response);
        }))
    };
    (cb, rx)
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
    cluster.call_command(request, timeout)
}

pub fn async_read_on_peer<T: Simulator>(
    cluster: &mut Cluster<T>,
    peer: metapb::Peer,
    region: metapb::Region,
    key: &[u8],
    read_quorum: bool,
    replica_read: bool,
) -> mpsc::Receiver<RaftCmdResponse> {
    let node_id = peer.get_id();
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
    cluster
        .sim
        .wl()
        .async_command_on_node(node_id, request, cb)
        .unwrap();
    rx
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
    cluster.call_command(request, timeout)
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
            hex::encode_upper(key),
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
                hex::encode_upper(key),
                escape(&value)
            );
        }
    }
}

fn dummpy_filter(_: &CompactionJobInfo) -> bool {
    true
}

pub fn create_test_engine(
    engines: Option<Engines>,
    router: RaftRouter<RocksEngine>,
    cfg: &TiKvConfig,
) -> (Engines, Option<TempDir>) {
    // Create engine
    let mut path = None;
    let engines = match engines {
        Some(e) => e,
        None => {
            path = Some(Builder::new().prefix("test_cluster").tempdir().unwrap());
            let mut kv_db_opt = cfg.rocksdb.build_opt();
            let router = Mutex::new(router);
            let cmpacted_handler = Box::new(move |event| {
                router
                    .lock()
                    .unwrap()
                    .send_control(StoreMsg::CompactedEvent(event))
                    .unwrap();
            });
            kv_db_opt.add_event_listener(CompactionListener::new(
                cmpacted_handler,
                Some(dummpy_filter),
            ));
            let cache = cfg.storage.block_cache.build_shared_cache();
            let kv_cfs_opt = cfg.rocksdb.build_cf_opts(&cache);
            let kv_path = path.as_ref().unwrap().path().join(DEFAULT_ROCKSDB_SUB_DIR);
            let engine = Arc::new(
                rocks::util::new_engine_opt(kv_path.to_str().unwrap(), kv_db_opt, kv_cfs_opt)
                    .unwrap(),
            );
            let raft_path = path.as_ref().unwrap().path().join("raft");
            let raft_engine = Arc::new(
                rocks::util::new_engine(raft_path.to_str().unwrap(), None, &[CF_DEFAULT], None)
                    .unwrap(),
            );
            Engines::new(engine, raft_engine, cache.is_some())
        }
    };
    (engines, path)
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

pub fn configure_for_transfer_leader<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.cfg.raft_store.raft_reject_transfer_leader_duration = ReadableDuration::secs(1);
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
    cluster.cfg.raft_store.raft_store_max_leader_lease = ReadableDuration(election_timeout);
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
    let mut last_len = 0;
    let mut rng = rand::thread_rng();
    let mut key = vec![];
    while len < limit {
        let key_id = range.next().unwrap();
        let key_str = format!("{:09}", key_id);
        key = key_str.into_bytes();
        let mut value = vec![0; 64];
        rng.fill_bytes(&mut value);
        cluster.must_put_cf(cf, &key, &value);
        // plus 1 for the extra encoding prefix
        len += key.len() as u64 + 1;
        len += value.len() as u64;
        // Flush memtable to SST periodically, to make approximate size more accurate.
        if len - last_len >= 1000 {
            cluster.must_flush_cf(cf, true);
            last_len = len;
        }
    }
    // Approximate size of memtable is inaccurate for small data,
    // we flush it to SST so we can use the size properties instead.
    cluster.must_flush_cf(cf, true);
    key
}
