// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    thread::sleep,
    time::{Duration, Instant},
};

use futures::{channel::oneshot, executor::block_on};
use kvproto::{
    errorpb::FlashbackInProgress,
    metapb,
    raft_cmdpb::{AdminCmdType, CmdType, Request},
};
use raftstore::store::Callback;
use test_raftstore::*;
use txn_types::WriteBatchFlags;

#[test]
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
    let (result_tx, result_rx) = oneshot::channel();
    cluster.must_send_flashback_msg(
        old_region.get_id(),
        AdminCmdType::PrepareFlashback,
        Callback::write(Box::new(move |resp| {
            if resp.response.get_header().has_error() {
                result_tx
                    .send(Some(resp.response.get_header().get_error().clone()))
                    .unwrap();
                return;
            }
            result_tx.send(None).unwrap();
        })),
    );
    // Remove the pause to make these two commands are in the same batch to apply.
    fail::remove(on_handle_apply_fp);
    let prepare_flashback_err = block_on(result_rx).unwrap().unwrap();
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

#[test]
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
    cluster.async_add_peer(region_id, new_peer(2, 2)).unwrap();
    // Make sure the conf change cmd is ready.
    sleep(Duration::from_millis(100));
    // Send the prepare flashback msg.
    let (result_tx, result_rx) = oneshot::channel();
    cluster.must_send_flashback_msg(
        region_id,
        AdminCmdType::PrepareFlashback,
        Callback::write(Box::new(move |resp| {
            if resp.response.get_header().has_error() {
                result_tx
                    .send(Some(resp.response.get_header().get_error().clone()))
                    .unwrap();
                return;
            }
            result_tx.send(None).unwrap();
        })),
    );
    // Remove the pause to make these two commands are in the same batch to apply.
    fail::remove(on_handle_apply_fp);
    let prepare_flashback_err = block_on(result_rx).unwrap().unwrap();
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

#[test]
fn test_flashback_unprepared() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.run();

    cluster.must_transfer_leader(1, new_peer(2, 2));
    cluster.must_transfer_leader(1, new_peer(1, 1));

    let mut region = cluster.get_region(b"k1");
    let mut cmd = Request::default();
    cmd.set_cmd_type(CmdType::Put);
    let mut req = new_request(
        region.get_id(),
        region.take_region_epoch(),
        vec![cmd],
        false,
    );
    let new_leader = cluster.query_leader(1, region.get_id(), Duration::from_secs(1));
    req.mut_header().set_peer(new_leader.unwrap());
    req.mut_header()
        .set_flags(WriteBatchFlags::FLASHBACK.bits());
    let resp = cluster.call_command(req, Duration::from_secs(3)).unwrap();
    assert!(resp.get_header().get_error().has_flashback_not_prepared());
}

#[test]
fn test_flashback_for_schedule() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.run();

    cluster.must_transfer_leader(1, new_peer(2, 2));
    cluster.must_transfer_leader(1, new_peer(1, 1));

    // Prepare for flashback
    let region = cluster.get_region(b"k1");
    cluster.must_send_wait_flashback_msg(region.get_id(), AdminCmdType::PrepareFlashback);

    // Verify the schedule is disabled.
    let mut region = cluster.get_region(b"k3");
    let admin_req = new_transfer_leader_cmd(new_peer(2, 2));
    let transfer_leader =
        new_admin_request(region.get_id(), &region.take_region_epoch(), admin_req);
    let resp = cluster
        .call_command_on_leader(transfer_leader, Duration::from_secs(3))
        .unwrap();
    let e = resp.get_header().get_error();
    assert_eq!(
        e.get_flashback_in_progress(),
        &FlashbackInProgress {
            region_id: region.get_id(),
            ..Default::default()
        }
    );

    cluster.must_send_wait_flashback_msg(region.get_id(), AdminCmdType::FinishFlashback);
    // Transfer leader to (2, 2) should succeed.
    cluster.must_transfer_leader(1, new_peer(2, 2));
}

#[test]
fn test_flashback_for_write() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));

    // Write for cluster
    let value = vec![1_u8; 8096];
    multi_do_cmd(&mut cluster, new_put_cf_cmd("write", b"k1", &value));

    // Prepare for flashback
    let region = cluster.get_region(b"k1");
    cluster.must_send_wait_flashback_msg(region.get_id(), AdminCmdType::PrepareFlashback);

    // Write will be blocked
    let value = vec![1_u8; 8096];
    must_get_error_flashback_in_progress(&mut cluster, &region, new_put_cmd(b"k1", &value));
    // Write with flashback flag will succeed
    must_do_cmd_with_flashback_flag(
        &mut cluster,
        &mut region.clone(),
        new_put_cmd(b"k1", &value),
    );

    cluster.must_send_wait_flashback_msg(region.get_id(), AdminCmdType::FinishFlashback);

    multi_do_cmd(&mut cluster, new_put_cf_cmd("write", b"k1", &value));
}

#[test]
fn test_flashback_for_read() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));

    // Write for cluster
    let value = vec![1_u8; 8096];
    multi_do_cmd(&mut cluster, new_put_cf_cmd("write", b"k1", &value));
    // read for cluster
    multi_do_cmd(&mut cluster, new_get_cf_cmd("write", b"k1"));

    // Prepare for flashback
    let region = cluster.get_region(b"k1");
    cluster.must_send_wait_flashback_msg(region.get_id(), AdminCmdType::PrepareFlashback);

    // read will be blocked
    must_get_error_flashback_in_progress(&mut cluster, &region, new_get_cf_cmd("write", b"k1"));

    // Verify the read can be executed if add flashback flag in request's
    // header.
    must_do_cmd_with_flashback_flag(
        &mut cluster,
        &mut region.clone(),
        new_get_cf_cmd("write", b"k1"),
    );

    cluster.must_send_wait_flashback_msg(region.get_id(), AdminCmdType::FinishFlashback);

    multi_do_cmd(&mut cluster, new_get_cf_cmd("write", b"k1"));
}

// LocalReader will attempt to renew the lease.
// However, when flashback is enabled, it will make the lease None and prevent
// renew lease.
#[test]
fn test_flashback_for_local_read() {
    let mut cluster = new_node_cluster(0, 3);
    let election_timeout = configure_for_lease_read(&mut cluster, Some(50), None);

    // Avoid triggering the log compaction in this test case.
    cluster.cfg.raft_store.raft_log_gc_threshold = 100;

    let store_id = 3;
    let peer = new_peer(store_id, 3);
    cluster.run();

    cluster.must_put(b"k1", b"v1");
    let region = cluster.get_region(b"k1");
    cluster.must_transfer_leader(region.get_id(), peer.clone());

    // Check local read before prepare flashback
    let state = cluster.raft_local_state(region.get_id(), store_id);
    let last_index = state.get_last_index();
    // Make sure the leader transfer procedure timeouts.
    sleep(election_timeout * 2);
    must_read_on_peer(&mut cluster, peer.clone(), region.clone(), b"k1", b"v1");
    // Check the leader does a local read.
    let state = cluster.raft_local_state(region.get_id(), store_id);
    assert_eq!(state.get_last_index(), last_index);

    // Prepare for flashback
    cluster.must_send_wait_flashback_msg(region.get_id(), AdminCmdType::PrepareFlashback);

    // Check the leader does a local read.
    let state = cluster.raft_local_state(region.get_id(), store_id);
    assert_eq!(state.get_last_index(), last_index + 1);
    // Wait for apply_res to set leader lease.
    sleep_ms(500);

    must_error_read_on_peer(
        &mut cluster,
        peer.clone(),
        region.clone(),
        b"k1",
        Duration::from_secs(1),
    );

    // Wait for the leader's lease to expire to ensure that a renew lease interval
    // has elapsed.
    sleep(election_timeout * 2);
    must_error_read_on_peer(
        &mut cluster,
        peer.clone(),
        region.clone(),
        b"k1",
        Duration::from_secs(1),
    );

    // Also check read by propose was blocked
    let state = cluster.raft_local_state(region.get_id(), store_id);
    assert_eq!(state.get_last_index(), last_index + 1);

    cluster.must_send_wait_flashback_msg(region.get_id(), AdminCmdType::FinishFlashback);

    let state = cluster.raft_local_state(region.get_id(), store_id);
    assert_eq!(state.get_last_index(), last_index + 2);

    // Check local read after finish flashback
    let state = cluster.raft_local_state(region.get_id(), store_id);
    let last_index = state.get_last_index();
    // Make sure the leader transfer procedure timeouts.
    sleep(election_timeout * 2);
    must_read_on_peer(&mut cluster, peer, region.clone(), b"k1", b"v1");

    // Check the leader does a local read.
    let state = cluster.raft_local_state(region.get_id(), store_id);
    assert_eq!(state.get_last_index(), last_index);
}

#[test]
fn test_flashback_for_status_cmd_as_region_detail() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.run();

    let leader = cluster.leader_of_region(1).unwrap();
    let region = cluster.get_region(b"k1");
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

#[test]
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

#[test]
fn test_flashback_for_apply_snapshot() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_snapshot(&mut cluster);
    cluster.run();

    cluster.must_transfer_leader(1, new_peer(3, 3));
    cluster.must_transfer_leader(1, new_peer(1, 1));

    must_check_flashback_state(&mut cluster, 1, 1, false);
    must_check_flashback_state(&mut cluster, 1, 3, false);

    // Make store 3 isolated.
    cluster.add_send_filter(IsolationFilterFactory::new(3));

    // Write some data to trigger snapshot.
    for i in 100..110 {
        let key = format!("k{}", i);
        let value = format!("v{}", i);
        cluster.must_put_cf("write", key.as_bytes(), value.as_bytes());
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
}

fn must_check_flashback_state(
    cluster: &mut Cluster<NodeCluster>,
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

fn multi_do_cmd<T: Simulator>(cluster: &mut Cluster<T>, cmd: Request) {
    for _ in 0..100 {
        let mut reqs = vec![];
        for _ in 0..100 {
            reqs.push(cmd.clone());
        }
        cluster.batch_put(b"k1", reqs).unwrap();
    }
}

fn must_do_cmd_with_flashback_flag<T: Simulator>(
    cluster: &mut Cluster<T>,
    region: &mut metapb::Region,
    cmd: Request,
) {
    // Verify the read can be executed if add flashback flag in request's
    // header.
    let mut req = new_request(
        region.get_id(),
        region.take_region_epoch(),
        vec![cmd],
        false,
    );
    let new_leader = cluster.query_leader(1, region.get_id(), Duration::from_secs(1));
    req.mut_header().set_peer(new_leader.unwrap());
    req.mut_header()
        .set_flags(WriteBatchFlags::FLASHBACK.bits());
    let resp = cluster.call_command(req, Duration::from_secs(3)).unwrap();
    assert!(!resp.get_header().has_error());
}

fn must_get_error_flashback_in_progress<T: Simulator>(
    cluster: &mut Cluster<T>,
    region: &metapb::Region,
    cmd: Request,
) {
    for _ in 0..100 {
        let mut reqs = vec![];
        for _ in 0..100 {
            reqs.push(cmd.clone());
        }
        match cluster.batch_put(b"k1", reqs) {
            Ok(_) => {}
            Err(e) => {
                assert_eq!(
                    e.get_flashback_in_progress(),
                    &FlashbackInProgress {
                        region_id: region.get_id(),
                        ..Default::default()
                    }
                );
            }
        }
    }
}
