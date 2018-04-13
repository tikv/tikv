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

use std::sync::{mpsc, Arc};
use std::time::Duration;
use std::thread;
use std::path::Path;

use tempdir::TempDir;

use rocksdb::{CompactionJobInfo, DB};
use protobuf;

use kvproto::metapb::{self, RegionEpoch};
use kvproto::raft_cmdpb::{AdminRequest, RaftCmdRequest, RaftCmdResponse, Request, StatusRequest};
use kvproto::raft_cmdpb::{AdminCmdType, CmdType, StatusCmdType};
use kvproto::pdpb::{ChangePeer, Merge, RegionHeartbeatResponse, SplitRegion, TransferLeader};
use raft::eraftpb::ConfChangeType;

use tikv::raftstore::store::*;
use tikv::raftstore::{Error, Result};
use tikv::server::Config as ServerConfig;
use tikv::server::readpool::Config as ReadPoolInstanceConfig;
use tikv::storage::{Config as StorageConfig, CF_DEFAULT};
use tikv::util::escape;
use tikv::util::rocksdb::{self, CompactionListener};
use tikv::util::config::*;
use tikv::config::{ReadPoolConfig, TiKvConfig};
use tikv::util::transport::SendCh;
use tikv::raftstore::store::Msg as StoreMsg;

use super::cluster::{Cluster, Simulator};

pub use tikv::raftstore::store::util::{find_peer, new_learner_peer, new_peer};

pub const MAX_LEADER_LEASE: u64 = 250; // 250ms

pub fn must_get(engine: &Arc<DB>, cf: &str, key: &[u8], value: Option<&[u8]>) {
    for _ in 1..300 {
        let res = engine.get_value_cf(cf, &keys::data_key(key)).unwrap();
        if value.is_some() && res.is_some() {
            assert_eq!(value.unwrap(), &*res.unwrap());
            return;
        }
        if value.is_none() && res.is_none() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    debug!("last try to get {}", escape(key));
    let res = engine.get_value_cf(cf, &keys::data_key(key)).unwrap();
    if value.is_none() && res.is_none()
        || value.is_some() && res.is_some() && value.unwrap() == &*res.unwrap()
    {
        return;
    }
    panic!(
        "can't get value {:?} for key {:?}",
        value.map(escape),
        escape(key)
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
        max_leader_missing_duration: ReadableDuration::secs(3),
        // To make a valid config, use a value of 2 seconds as
        // abnormal_leader_missing_duration and set
        // peer_stale_state_check_interval to 1 second.
        abnormal_leader_missing_duration: ReadableDuration::secs(2),
        peer_stale_state_check_interval: ReadableDuration::secs(1),
        pd_heartbeat_tick_interval: ReadableDuration::millis(20),
        region_split_check_diff: ReadableSize(10000),
        report_region_flow_interval: ReadableDuration::millis(100),
        raft_store_max_leader_lease: ReadableDuration::millis(MAX_LEADER_LEASE),
        allow_remove_leader: true,
        ..Config::default()
    }
}

pub fn new_server_config(cluster_id: u64) -> ServerConfig {
    ServerConfig {
        cluster_id: cluster_id,
        addr: "127.0.0.1:0".to_owned(),
        grpc_concurrency: 1,
        // Considering connection selection algo is involved, maybe
        // use 2 or larger value here?
        grpc_raft_conn_num: 1,
        end_point_concurrency: 1,
        ..ServerConfig::default()
    }
}

pub fn new_readpool_cfg() -> ReadPoolConfig {
    ReadPoolConfig {
        storage: ReadPoolInstanceConfig::default_for_test(),
    }
}

pub fn new_tikv_config(cluster_id: u64) -> TiKvConfig {
    TiKvConfig {
        storage: StorageConfig {
            scheduler_worker_pool_size: 1,
            ..StorageConfig::default()
        },
        server: new_server_config(cluster_id),
        raft_store: new_store_cfg(),
        readpool: new_readpool_cfg(),
        ..TiKvConfig::default()
    }
}

// Create a base request.
pub fn new_base_request(region_id: u64, epoch: RegionEpoch, read_quorum: bool) -> RaftCmdRequest {
    let mut req = RaftCmdRequest::new();
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
    req.set_requests(protobuf::RepeatedField::from_vec(requests));
    req
}

pub fn new_put_cmd(key: &[u8], value: &[u8]) -> Request {
    let mut cmd = Request::new();
    cmd.set_cmd_type(CmdType::Put);
    cmd.mut_put().set_key(key.to_vec());
    cmd.mut_put().set_value(value.to_vec());
    cmd
}

pub fn new_put_cf_cmd(cf: &str, key: &[u8], value: &[u8]) -> Request {
    let mut cmd = Request::new();
    cmd.set_cmd_type(CmdType::Put);
    cmd.mut_put().set_key(key.to_vec());
    cmd.mut_put().set_value(value.to_vec());
    cmd.mut_put().set_cf(cf.to_string());
    cmd
}

pub fn new_get_cmd(key: &[u8]) -> Request {
    let mut cmd = Request::new();
    cmd.set_cmd_type(CmdType::Get);
    cmd.mut_get().set_key(key.to_vec());
    cmd
}

pub fn new_delete_cmd(cf: &str, key: &[u8]) -> Request {
    let mut cmd = Request::new();
    cmd.set_cmd_type(CmdType::Delete);
    cmd.mut_delete().set_key(key.to_vec());
    cmd.mut_delete().set_cf(cf.to_string());
    cmd
}

pub fn new_delete_range_cmd(cf: &str, start: &[u8], end: &[u8]) -> Request {
    let mut cmd = Request::new();
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
    let mut req = new_base_request(region_id, RegionEpoch::new(), false);
    req.mut_header().set_peer(peer);
    req.set_status_request(request);
    req
}

pub fn new_region_detail_cmd() -> StatusRequest {
    let mut cmd = StatusRequest::new();
    cmd.set_cmd_type(StatusCmdType::RegionDetail);
    cmd
}

pub fn new_region_leader_cmd() -> StatusRequest {
    let mut cmd = StatusRequest::new();
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
    let mut req = AdminRequest::new();
    req.set_cmd_type(AdminCmdType::ChangePeer);
    req.mut_change_peer().set_change_type(change_type);
    req.mut_change_peer().set_peer(peer);
    req
}

pub fn new_compact_log_request(index: u64, term: u64) -> AdminRequest {
    let mut req = AdminRequest::new();
    req.set_cmd_type(AdminCmdType::CompactLog);
    req.mut_compact_log().set_compact_index(index);
    req.mut_compact_log().set_compact_term(term);
    req
}

pub fn new_transfer_leader_cmd(peer: metapb::Peer) -> AdminRequest {
    let mut cmd = AdminRequest::new();
    cmd.set_cmd_type(AdminCmdType::TransferLeader);
    cmd.mut_transfer_leader().set_peer(peer);
    cmd
}

#[allow(dead_code)]
pub fn new_prepare_merge(target_region: metapb::Region) -> AdminRequest {
    let mut cmd = AdminRequest::new();
    cmd.set_cmd_type(AdminCmdType::PrepareMerge);
    cmd.mut_prepare_merge().set_target(target_region);
    cmd
}

pub fn new_store(store_id: u64, addr: String) -> metapb::Store {
    let mut store = metapb::Store::new();
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
    let mut change_peer = ChangePeer::new();
    change_peer.set_change_type(change_type);
    change_peer.set_peer(peer);

    let mut resp = RegionHeartbeatResponse::new();
    resp.set_change_peer(change_peer);
    resp
}

pub fn new_half_split_region() -> RegionHeartbeatResponse {
    let split_region = SplitRegion::new();
    let mut resp = RegionHeartbeatResponse::new();
    resp.set_split_region(split_region);
    resp
}

pub fn new_pd_transfer_leader(peer: metapb::Peer) -> RegionHeartbeatResponse {
    let mut transfer_leader = TransferLeader::new();
    transfer_leader.set_peer(peer);

    let mut resp = RegionHeartbeatResponse::new();
    resp.set_transfer_leader(transfer_leader);
    resp
}

pub fn new_pd_merge_region(target_region: metapb::Region) -> RegionHeartbeatResponse {
    let mut merge = Merge::new();
    merge.set_target(target_region);

    let mut resp = RegionHeartbeatResponse::new();
    resp.set_merge(merge);
    resp
}

pub fn make_cb(cmd: &RaftCmdRequest) -> (Callback, mpsc::Receiver<RaftCmdResponse>) {
    let mut is_read;
    let mut is_write;
    is_read = cmd.has_status_request();
    is_write = cmd.has_admin_request();
    for req in cmd.get_requests() {
        match req.get_cmd_type() {
            CmdType::Get | CmdType::Snap => is_read = true,
            CmdType::Put | CmdType::Delete | CmdType::DeleteRange | CmdType::IngestSST => {
                is_write = true
            }
            CmdType::Invalid | CmdType::Prewrite => panic!("Invalid RaftCmdRequest: {:?}", cmd),
        }
    }
    assert!(is_read ^ is_write, "Invalid RaftCmdRequest: {:?}", cmd);

    let (tx, rx) = mpsc::channel();
    let cb = if is_read {
        Callback::Read(Box::new(move |resp: ReadResponse| {
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
    timeout: Duration,
) -> Result<Vec<u8>> {
    let mut request = new_request(
        region.get_id(),
        region.get_region_epoch().clone(),
        vec![new_get_cmd(key)],
        false,
    );
    request.mut_header().set_peer(peer);
    let mut resp = cluster.call_command(request, timeout)?;
    if resp.get_header().has_error() {
        return Err(Error::Other(box_err!(
            resp.mut_header().take_error().take_message()
        )));
    }
    assert_eq!(resp.get_responses().len(), 1);
    assert_eq!(resp.get_responses()[0].get_cmd_type(), CmdType::Get);
    assert!(resp.get_responses()[0].has_get());
    Ok(resp.mut_responses()[0].mut_get().take_value())
}

pub fn must_read_on_peer<T: Simulator>(
    cluster: &mut Cluster<T>,
    peer: metapb::Peer,
    region: metapb::Region,
    key: &[u8],
    value: &[u8],
) {
    let timeout = Duration::from_secs(1);
    match read_on_peer(cluster, peer, region, key, timeout) {
        Ok(v) => if v != value {
            panic!(
                "read key {}, expect value {}, got {}",
                escape(key),
                escape(value),
                escape(&v)
            )
        },
        Err(e) => panic!("failed to read for key {}, err {:?}", escape(key), e),
    }
}

pub fn must_error_read_on_peer<T: Simulator>(
    cluster: &mut Cluster<T>,
    peer: metapb::Peer,
    region: metapb::Region,
    key: &[u8],
    timeout: Duration,
) {
    if let Ok(value) = read_on_peer(cluster, peer, region, key, timeout) {
        panic!(
            "key {}, expect error but got {}",
            escape(key),
            escape(&value)
        );
    }
}

fn dummpy_filter(_: &CompactionJobInfo) -> bool {
    true
}

pub fn create_test_engine(
    engines: Option<Engines>,
    tx: SendCh<StoreMsg>,
    cfg: &TiKvConfig,
) -> (Engines, Option<TempDir>) {
    // Create engine
    let mut path = None;
    let engines = match engines {
        Some(e) => e,
        None => {
            path = Some(TempDir::new("test_cluster").unwrap());
            let mut kv_db_opt = cfg.rocksdb.build_opt();
            let cmpacted_handler = box move |event| {
                tx.send(StoreMsg::CompactedEvent(event)).unwrap();
            };
            kv_db_opt.add_event_listener(CompactionListener::new(
                cmpacted_handler,
                Some(dummpy_filter),
            ));
            let kv_cfs_opt = cfg.rocksdb.build_cf_opts();
            let engine = Arc::new(
                rocksdb::new_engine_opt(
                    path.as_ref().unwrap().path().to_str().unwrap(),
                    kv_db_opt,
                    kv_cfs_opt,
                ).unwrap(),
            );
            let raft_path = path.as_ref().unwrap().path().join(Path::new("raft"));
            let raft_engine = Arc::new(
                rocksdb::new_engine(raft_path.to_str().unwrap(), &[CF_DEFAULT], None).unwrap(),
            );
            Engines::new(engine, raft_engine)
        }
    };
    (engines, path)
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
}
