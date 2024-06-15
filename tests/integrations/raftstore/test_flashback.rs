// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    thread::sleep,
    time::{Duration, Instant},
};

use engine_rocks::RocksEngine;
use futures::executor::block_on;
use kvproto::{
    errorpb::FlashbackInProgress,
    metapb,
    raft_cmdpb::{AdminCmdType, CmdType, RaftCmdRequest, RaftCmdResponse, Request},
    raft_serverpb::RegionLocalState,
};
use raftstore::store::{Callback, LocksStatus};
use test_raftstore::*;
use test_raftstore_macro::test_case;
use tikv::storage::kv::SnapContext;
use txn_types::{Key, LastChange, PessimisticLock, WriteBatchFlags};

const TEST_KEY: &[u8] = b"k1";
const TEST_VALUE: &[u8] = b"v1";

#[test_case(test_raftstore::new_server_cluster)]
#[test_case(test_raftstore_v2::new_server_cluster)]
fn test_flashback_with_in_memory_pessimistic_locks() {
    let mut cluster = new_cluster(0, 3);
    cluster.cfg.raft_store.raft_heartbeat_ticks = 20;
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));

    let region = cluster.get_region(TEST_KEY);
    // Write a pessimistic lock to the in-memory pessimistic lock table.
    {
        let snapshot = cluster.must_get_snapshot_of_region(region.get_id());
        let txn_ext = snapshot.txn_ext.unwrap();
        let mut pessimistic_locks = txn_ext.pessimistic_locks.write();
        assert!(pessimistic_locks.is_writable());
        pessimistic_locks
            .insert(vec![(
                Key::from_raw(TEST_KEY),
                PessimisticLock {
                    primary: TEST_KEY.to_vec().into_boxed_slice(),
                    start_ts: 10.into(),
                    ttl: 3000,
                    for_update_ts: 20.into(),
                    min_commit_ts: 30.into(),
                    last_change: LastChange::make_exist(5.into(), 3),
                    is_locked_with_conflict: false,
                },
            )])
            .unwrap();
        assert_eq!(pessimistic_locks.len(), 1);
    }
    // Prepare flashback.
    cluster.must_send_wait_flashback_msg(region.get_id(), AdminCmdType::PrepareFlashback);
    // Check the in-memory pessimistic lock table.
    {
        let snapshot = cluster.must_get_snapshot_of_region_with_ctx(
            region.get_id(),
            SnapContext {
                allowed_in_flashback: true,
                ..Default::default()
            },
        );
        let txn_ext = snapshot.txn_ext.unwrap();
        eventually_meet(
            Box::new(move || {
                let pessimistic_locks = txn_ext.pessimistic_locks.read();
                !pessimistic_locks.is_writable()
                    && pessimistic_locks.status == LocksStatus::IsInFlashback
                    && pessimistic_locks.is_empty()
            }),
            "pessimistic locks status should be LocksStatus::IsInFlashback",
        );
    }
    // Finish flashback.
    cluster.must_send_wait_flashback_msg(region.get_id(), AdminCmdType::FinishFlashback);
    // Check the in-memory pessimistic lock table.
    {
        let snapshot = cluster.must_get_snapshot_of_region(region.get_id());
        let txn_ext = snapshot.txn_ext.unwrap();
        eventually_meet(
            Box::new(move || {
                let pessimistic_locks = txn_ext.pessimistic_locks.read();
                pessimistic_locks.is_writable() && pessimistic_locks.is_empty()
            }),
            "pessimistic locks should be writable again",
        );
    }
}

fn eventually_meet(condition: Box<dyn Fn() -> bool>, purpose: &str) {
    for _ in 0..30 {
        if condition() {
            return;
        }
        sleep(Duration::from_millis(100));
    }
    panic!("condition never meet: {}", purpose);
}

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_allow_read_only_request() {
    let mut cluster = new_cluster(0, 3);
    configure_for_lease_read(&mut cluster.cfg, Some(50), Some(30));
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.must_put(TEST_KEY, TEST_VALUE);

    let mut region = cluster.get_region(TEST_KEY);
    let mut get_req = Request::default();
    get_req.set_cmd_type(CmdType::Get);
    // Get normally.
    let snap_resp = request(&mut cluster, &mut region.clone(), get_req.clone(), false);
    assert!(
        !snap_resp.get_header().has_error(),
        "{:?}",
        snap_resp.get_header()
    );
    // Get with flashback flag without in the flashback state.
    let snap_resp = request(&mut cluster, &mut region.clone(), get_req.clone(), true);
    assert!(
        !snap_resp.get_header().has_error(),
        "{:?}",
        snap_resp.get_header()
    );
    // Get with flashback flag with in the flashback state.
    cluster.must_send_wait_flashback_msg(region.get_id(), AdminCmdType::PrepareFlashback);
    let snap_resp = request(&mut cluster, &mut region.clone(), get_req.clone(), true);
    assert!(
        !snap_resp.get_header().has_error(),
        "{:?}",
        snap_resp.get_header()
    );
    // Get without flashback flag with in the flashback state.
    let snap_resp = request(&mut cluster, &mut region, get_req, false);
    assert!(
        snap_resp
            .get_header()
            .get_error()
            .has_flashback_in_progress(),
        "{:?}",
        snap_resp
    );
    // Finish flashback.
    cluster.must_send_wait_flashback_msg(region.get_id(), AdminCmdType::FinishFlashback);
}

#[cfg(feature = "failpoints")]
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_read_after_prepare_flashback() {
    let mut cluster = new_cluster(0, 3);
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));

    let region = cluster.get_region(TEST_KEY);
    fail::cfg("keep_peer_fsm_flashback_state_false", "return").unwrap();
    // Prepare flashback.
    cluster.must_send_wait_flashback_msg(region.get_id(), AdminCmdType::PrepareFlashback);
    // Read with flashback flag will succeed even the peer fsm does not updated its
    // `is_in_flashback` flag.
    must_request_with_flashback_flag(&mut cluster, &mut region.clone(), new_get_cmd(TEST_KEY));
    // Writing with flashback flag will succeed since the ApplyFSM owns the
    // latest `is_in_flashback` flag.
    must_request_with_flashback_flag(&mut cluster, &mut region.clone(), new_get_cmd(TEST_KEY));
    fail::remove("keep_peer_fsm_flashback_state_false");
    // Finish flashback.
    cluster.must_send_wait_flashback_msg(region.get_id(), AdminCmdType::FinishFlashback);
}

#[cfg(feature = "failpoints")]
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_prepare_flashback_after_split() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));

    let old_region = cluster.get_region(b"a");
    cluster.wait_applied_to_current_term(old_region.get_id(), Duration::from_secs(3));
    // Pause the apply to make sure the split cmd and prepare flashback cmd are in
    // the same batch.
    let on_handle_apply_fp = "on_handle_apply";
    fail::cfg(on_handle_apply_fp, "pause").unwrap();
    // Send the split msg.
    cluster.split_region(
        &old_region,
        b"b",
        Callback::write(Box::new(|resp| {
            if resp.response.get_header().has_error() {
                panic!("split failed: {:?}", resp.response.get_header().get_error());
            }
        })),
    );
    // Make sure the admin split cmd is ready.
    sleep(Duration::from_millis(100));
    // Send the prepare flashback msg.
    let resp = cluster.must_send_flashback_msg(old_region.get_id(), AdminCmdType::PrepareFlashback);
    // Remove the pause to make these two commands are in the same batch to apply.
    fail::remove(on_handle_apply_fp);
    let prepare_flashback_err = block_on(async {
        let resp = resp.await;
        resp.get_header().get_error().clone()
    });
    assert!(
        prepare_flashback_err.has_epoch_not_match(),
        "prepare flashback should fail with epoch not match, but got {:?}",
        prepare_flashback_err
    );
    // Check the region meta.
    let left_region = cluster.get_region(b"a");
    let right_region = cluster.get_region(b"b");
    assert!(left_region.get_id() != old_region.get_id());
    assert!(left_region.get_end_key() == right_region.get_start_key());
    assert!(
        left_region.get_region_epoch().get_version()
            == right_region.get_region_epoch().get_version()
    );
    must_check_flashback_state(&mut cluster, left_region.get_id(), 1, false);
    must_check_flashback_state(&mut cluster, right_region.get_id(), 1, false);
}

// #[cfg(feature = "failpoints")]
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_prepare_flashback_after_conf_change() {
    let mut cluster = new_node_cluster(0, 3);
    // Disable default max peer count check.
    cluster.pd_client.disable_default_operator();

    let region_id = cluster.run_conf_change();
    cluster.wait_applied_to_current_term(region_id, Duration::from_secs(3));
    // Pause the apply to make sure the conf change cmd and prepare flashback cmd
    // are in the same batch.
    let on_handle_apply_fp = "on_handle_apply";
    fail::cfg(on_handle_apply_fp, "pause").unwrap();
    // Send the conf change msg.
    let _ = cluster.async_add_peer(region_id, new_peer(2, 2)).unwrap();
    // Make sure the conf change cmd is ready.
    sleep(Duration::from_millis(100));
    // Send the prepare flashback msg.
    let resp = cluster.must_send_flashback_msg(region_id, AdminCmdType::PrepareFlashback);
    // Remove the pause to make these two commands are in the same batch to apply.
    fail::remove(on_handle_apply_fp);
    let prepare_flashback_err = block_on(async {
        let resp = resp.await;
        resp.get_header().get_error().clone()
    });
    assert!(
        prepare_flashback_err.has_epoch_not_match(),
        "prepare flashback should fail with epoch not match, but got {:?}",
        prepare_flashback_err
    );
    // Check the region meta.
    let region = cluster.get_region(b"a");
    assert!(region.get_id() == region_id);
    assert!(region.get_peers().len() == 2);
    must_check_flashback_state(&mut cluster, region_id, 1, false);
}

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_flashback_unprepared() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));

    let mut region = cluster.get_region(TEST_KEY);
    must_get_flashback_not_prepared_error(
        &mut cluster,
        &mut region,
        new_put_cmd(TEST_KEY, TEST_VALUE),
    );
}

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_flashback_for_schedule() {
    let mut cluster = new_cluster(0, 3);
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(2, 2));
    cluster.must_transfer_leader(1, new_peer(1, 1));

    // Prepare flashback.
    let region = cluster.get_region(TEST_KEY);
    cluster.must_send_wait_flashback_msg(region.get_id(), AdminCmdType::PrepareFlashback);
    // Make sure the schedule is disabled.
    let mut region = cluster.get_region(TEST_KEY);
    let admin_req = new_transfer_leader_cmd(new_peer(2, 2));
    let transfer_leader =
        new_admin_request(region.get_id(), &region.take_region_epoch(), admin_req);
    let resp = cluster
        .call_command_on_leader(transfer_leader, Duration::from_secs(3))
        .unwrap();
    assert_eq!(
        resp.get_header().get_error().get_flashback_in_progress(),
        &FlashbackInProgress {
            region_id: region.get_id(),
            ..Default::default()
        }
    );
    // Finish flashback.
    cluster.must_send_wait_flashback_msg(region.get_id(), AdminCmdType::FinishFlashback);
    // Transfer leader to (2, 2) should succeed.
    cluster.must_transfer_leader(1, new_peer(2, 2));
}

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_flashback_for_write() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));

    // Write without flashback flag.
    let mut region = cluster.get_region(TEST_KEY);
    must_request_without_flashback_flag(
        &mut cluster,
        &mut region.clone(),
        new_put_cmd(TEST_KEY, TEST_VALUE),
    );
    // Prepare flashback.
    cluster.must_send_wait_flashback_msg(region.get_id(), AdminCmdType::PrepareFlashback);
    // Write will be blocked
    must_get_flashback_in_progress_error(
        &mut cluster,
        &mut region.clone(),
        new_put_cmd(TEST_KEY, TEST_VALUE),
    );
    // Write with flashback flag will succeed.
    must_request_with_flashback_flag(
        &mut cluster,
        &mut region.clone(),
        new_put_cmd(TEST_KEY, TEST_VALUE),
    );
    cluster.must_send_wait_flashback_msg(region.get_id(), AdminCmdType::FinishFlashback);
    must_request_without_flashback_flag(
        &mut cluster,
        &mut region,
        new_put_cmd(TEST_KEY, TEST_VALUE),
    );
}

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_flashback_for_read() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));

    // Read without flashback flag.
    let mut region = cluster.get_region(TEST_KEY);
    must_request_without_flashback_flag(&mut cluster, &mut region.clone(), new_get_cmd(TEST_KEY));
    // Prepare flashback.
    cluster.must_send_wait_flashback_msg(region.get_id(), AdminCmdType::PrepareFlashback);
    // Read will be blocked.
    must_get_flashback_in_progress_error(&mut cluster, &mut region.clone(), new_get_cmd(TEST_KEY));
    // Read with flashback flag will succeed.
    must_request_with_flashback_flag(&mut cluster, &mut region.clone(), new_get_cmd(TEST_KEY));
    // Finish flashback.
    cluster.must_send_wait_flashback_msg(region.get_id(), AdminCmdType::FinishFlashback);
    must_request_without_flashback_flag(&mut cluster, &mut region, new_get_cmd(TEST_KEY));
}

// LocalReader will attempt to renew the lease.
// However, when flashback is enabled, it will make the lease None and prevent
// renew lease.
#[test]
fn test_flashback_for_local_read() {
    let mut cluster = new_node_cluster(0, 3);
    let election_timeout = configure_for_lease_read(&mut cluster.cfg, Some(50), None);
    // Avoid triggering the log compaction in this test case.
    cluster.cfg.raft_store.raft_log_gc_threshold = 100;
    cluster.run();
    cluster.must_put(TEST_KEY, TEST_VALUE);
    let mut region = cluster.get_region(TEST_KEY);
    let store_id = 3;
    let peer = new_peer(store_id, 3);
    cluster.must_transfer_leader(region.get_id(), peer);

    // Check local read before prepare flashback
    let state = cluster.raft_local_state(region.get_id(), store_id);
    let last_index = state.get_last_index();
    // Make sure the leader transfer procedure timeouts.
    sleep(election_timeout * 2);
    must_request_without_flashback_flag(&mut cluster, &mut region.clone(), new_get_cmd(TEST_KEY));
    // Check the leader does a local read.
    let state = cluster.raft_local_state(region.get_id(), store_id);
    assert_eq!(state.get_last_index(), last_index);

    // Prepare flashback.
    cluster.must_send_wait_flashback_msg(region.get_id(), AdminCmdType::PrepareFlashback);
    // Check the leader does a local read.
    let state = cluster.raft_local_state(region.get_id(), store_id);
    assert_eq!(state.get_last_index(), last_index + 1);
    // Wait for apply_res to set leader lease.
    sleep_ms(500);
    // Read should fail.
    must_get_flashback_in_progress_error(&mut cluster, &mut region.clone(), new_get_cmd(TEST_KEY));
    // Wait for the leader's lease to expire to ensure that a renew lease interval
    // has elapsed.
    sleep(election_timeout * 2);
    // Read should fail.
    must_get_flashback_in_progress_error(&mut cluster, &mut region.clone(), new_get_cmd(TEST_KEY));
    // Also check read by propose was blocked
    let state = cluster.raft_local_state(region.get_id(), store_id);
    assert_eq!(state.get_last_index(), last_index + 1);
    // Finish flashback.
    cluster.must_send_wait_flashback_msg(region.get_id(), AdminCmdType::FinishFlashback);
    let state = cluster.raft_local_state(region.get_id(), store_id);
    assert_eq!(state.get_last_index(), last_index + 2);

    // Check local read after finish flashback
    let state = cluster.raft_local_state(region.get_id(), store_id);
    let last_index = state.get_last_index();
    // Make sure the leader transfer procedure timeouts.
    sleep(election_timeout * 2);
    must_request_without_flashback_flag(&mut cluster, &mut region.clone(), new_get_cmd(TEST_KEY));
    // Check the leader does a local read.
    let state = cluster.raft_local_state(region.get_id(), store_id);
    assert_eq!(state.get_last_index(), last_index);
    // A local read with flashback flag will not be blocked since it won't have any
    // side effects.
    must_request_with_flashback_flag(&mut cluster, &mut region, new_get_cmd(TEST_KEY));
}

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_flashback_for_status_cmd_as_region_detail() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.run();

    let leader = cluster.leader_of_region(1).unwrap();
    let region = cluster.get_region(TEST_KEY);
    cluster.must_send_wait_flashback_msg(region.get_id(), AdminCmdType::PrepareFlashback);

    let region_detail = cluster.region_detail(region.get_id(), leader.get_store_id());
    assert!(region_detail.has_region());
    let region = region_detail.get_region();
    assert_eq!(region.get_id(), 1);
    assert!(region.get_start_key().is_empty());
    assert!(region.get_end_key().is_empty());
    assert_eq!(region.get_peers().len(), 3);
    let epoch = region.get_region_epoch();
    assert_eq!(epoch.get_conf_ver(), 1);
    assert_eq!(epoch.get_version(), 1);

    assert!(region_detail.has_leader());
    assert_eq!(region_detail.get_leader(), &leader);
}

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_flashback_for_check_is_in_persist() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.run();

    cluster.must_transfer_leader(1, new_peer(2, 2));
    must_check_flashback_state(&mut cluster, 1, 2, false);

    // Prepare for flashback
    cluster.must_send_wait_flashback_msg(1, AdminCmdType::PrepareFlashback);
    must_check_flashback_state(&mut cluster, 1, 2, true);

    cluster.must_send_wait_flashback_msg(1, AdminCmdType::FinishFlashback);
    must_check_flashback_state(&mut cluster, 1, 2, false);
}

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_flashback_for_apply_snapshot() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_snapshot(&mut cluster.cfg);
    cluster.run();

    cluster.must_transfer_leader(1, new_peer(3, 3));
    cluster.must_transfer_leader(1, new_peer(1, 1));

    must_check_flashback_state(&mut cluster, 1, 1, false);
    must_check_flashback_state(&mut cluster, 1, 3, false);
    // Make store 3 isolated.
    cluster.add_send_filter(IsolationFilterFactory::new(3));
    // Write some data to trigger snapshot.
    let mut region = cluster.get_region(TEST_KEY);
    for _ in 0..10 {
        must_request_without_flashback_flag(
            &mut cluster,
            &mut region.clone(),
            new_put_cf_cmd("write", TEST_KEY, TEST_VALUE),
        )
    }
    // Prepare for flashback
    cluster.must_send_wait_flashback_msg(1, AdminCmdType::PrepareFlashback);
    must_check_flashback_state(&mut cluster, 1, 1, true);
    must_check_flashback_state(&mut cluster, 1, 3, false);
    // Add store 3 back.
    cluster.clear_send_filters();
    must_check_flashback_state(&mut cluster, 1, 1, true);
    must_check_flashback_state(&mut cluster, 1, 3, true);
    cluster.must_send_wait_flashback_msg(1, AdminCmdType::FinishFlashback);
    must_check_flashback_state(&mut cluster, 1, 1, false);
    must_check_flashback_state(&mut cluster, 1, 3, false);

    // Prepare for flashback
    cluster.must_send_wait_flashback_msg(1, AdminCmdType::PrepareFlashback);
    must_check_flashback_state(&mut cluster, 1, 1, true);
    must_check_flashback_state(&mut cluster, 1, 3, true);
    // Make store 3 isolated.
    cluster.add_send_filter(IsolationFilterFactory::new(3));
    // Write some flashback data to trigger snapshot.
    for _ in 0..10 {
        must_request_with_flashback_flag(
            &mut cluster,
            &mut region.clone(),
            new_put_cf_cmd("write", TEST_KEY, TEST_VALUE),
        )
    }
    // Finish flashback.
    cluster.must_send_wait_flashback_msg(1, AdminCmdType::FinishFlashback);
    must_check_flashback_state(&mut cluster, 1, 1, false);
    must_check_flashback_state(&mut cluster, 1, 3, true);
    // Wait for a while before adding store 3 back to make sure only it does not
    // receive the `FinishFlashback` message.
    sleep(Duration::from_secs(1));
    // Add store 3 back.
    cluster.clear_send_filters();
    must_check_flashback_state(&mut cluster, 1, 1, false);
    must_check_flashback_state(&mut cluster, 1, 3, false);
    // Make store 3 become leader.
    cluster.must_transfer_leader(region.get_id(), new_peer(3, 3));
    // Region should not in the flashback state.
    must_request_without_flashback_flag(
        &mut cluster,
        &mut region,
        new_put_cmd(TEST_KEY, TEST_VALUE),
    );
}

trait ClusterI {
    fn region_local_state(&self, region_id: u64, store_id: u64) -> RegionLocalState;
    fn query_leader(
        &self,
        store_id: u64,
        region_id: u64,
        timeout: Duration,
    ) -> Option<metapb::Peer>;
    fn call_command(
        &self,
        request: RaftCmdRequest,
        timeout: Duration,
    ) -> raftstore::Result<RaftCmdResponse>;
}

impl ClusterI for Cluster<RocksEngine, NodeCluster<RocksEngine>> {
    fn region_local_state(&self, region_id: u64, store_id: u64) -> RegionLocalState {
        Cluster::<RocksEngine, NodeCluster<RocksEngine>>::region_local_state(
            self, region_id, store_id,
        )
    }
    fn query_leader(
        &self,
        store_id: u64,
        region_id: u64,
        timeout: Duration,
    ) -> Option<metapb::Peer> {
        Cluster::<RocksEngine, NodeCluster<RocksEngine>>::query_leader(
            self, store_id, region_id, timeout,
        )
    }
    fn call_command(
        &self,
        request: RaftCmdRequest,
        timeout: Duration,
    ) -> raftstore::Result<RaftCmdResponse> {
        Cluster::<RocksEngine, NodeCluster<RocksEngine>>::call_command(self, request, timeout)
    }
}

type ClusterV2 =
    test_raftstore_v2::Cluster<test_raftstore_v2::NodeCluster<RocksEngine>, RocksEngine>;
impl ClusterI for ClusterV2 {
    fn region_local_state(&self, region_id: u64, store_id: u64) -> RegionLocalState {
        ClusterV2::region_local_state(self, region_id, store_id)
    }
    fn query_leader(
        &self,
        store_id: u64,
        region_id: u64,
        timeout: Duration,
    ) -> Option<metapb::Peer> {
        ClusterV2::query_leader(self, store_id, region_id, timeout)
    }
    fn call_command(
        &self,
        request: RaftCmdRequest,
        timeout: Duration,
    ) -> raftstore::Result<RaftCmdResponse> {
        ClusterV2::call_command(self, request, timeout)
    }
}

fn must_check_flashback_state<T: ClusterI>(
    cluster: &mut T,
    region_id: u64,
    store_id: u64,
    is_in_flashback: bool,
) {
    let mut now = Instant::now();
    let timeout = Duration::from_secs(3);
    let deadline = now + timeout;
    while now < deadline {
        let local_state = cluster.region_local_state(region_id, store_id);
        if local_state.get_region().get_is_in_flashback() == is_in_flashback {
            return;
        }
        sleep(Duration::from_millis(10));
        now = Instant::now();
    }
    panic!(
        "region {} on store {} flashback state unmatched, want: {}",
        region_id, store_id, is_in_flashback,
    );
}

fn request<T: ClusterI>(
    cluster: &mut T,
    region: &mut metapb::Region,
    req: Request,
    with_flashback_flag: bool,
) -> RaftCmdResponse {
    let mut cmd_req = new_request(
        region.get_id(),
        region.take_region_epoch(),
        vec![req],
        false,
    );
    let new_leader = cluster.query_leader(1, region.get_id(), Duration::from_secs(1));
    let header = cmd_req.mut_header();
    header.set_peer(new_leader.unwrap());
    if with_flashback_flag {
        header.set_flags(WriteBatchFlags::FLASHBACK.bits());
    }
    cluster
        .call_command(cmd_req, Duration::from_secs(3))
        .unwrap()
}

// Make sure the request could be executed with flashback flag.
fn must_request_with_flashback_flag<T: ClusterI>(
    cluster: &mut T,
    region: &mut metapb::Region,
    req: Request,
) {
    let resp = request(cluster, region, req, true);
    assert!(!resp.get_header().has_error(), "{:?}", resp);
}

fn must_get_flashback_not_prepared_error<T: ClusterI>(
    cluster: &mut T,
    region: &mut metapb::Region,
    req: Request,
) {
    let resp = request(cluster, region, req, true);
    assert!(resp.get_header().get_error().has_flashback_not_prepared());
}

// Make sure the request could be executed without flashback flag.
fn must_request_without_flashback_flag<T: ClusterI>(
    cluster: &mut T,
    region: &mut metapb::Region,
    req: Request,
) {
    let resp = request(cluster, region, req, false);
    assert!(!resp.get_header().has_error(), "{:?}", resp);
}

fn must_get_flashback_in_progress_error<T: ClusterI>(
    cluster: &mut T,
    region: &mut metapb::Region,
    req: Request,
) {
    let resp = request(cluster, region, req, false);
    assert!(resp.get_header().get_error().has_flashback_in_progress());
}
