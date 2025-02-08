// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{iter::FromIterator, sync::Arc, time::Duration};

use engine_traits::{Peekable, RaftEngineReadOnly, CF_RAFT};
use futures::executor::block_on;
use kvproto::{
    metapb, pdpb,
    raft_serverpb::{RaftApplyState, RaftLocalState},
};
use pd_client::PdClient;
use raft::eraftpb::MessageType;
use test_raftstore::*;
use test_raftstore_macro::test_case;
use tikv_util::{config::ReadableDuration, mpsc, store::find_peer, HandyRwLock};

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_unsafe_recovery_send_report() {
    let mut cluster = new_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();

    // Makes the leadership definite.
    let store2_peer = find_peer(&region, nodes[1]).unwrap().to_owned();
    cluster.must_transfer_leader(region.get_id(), store2_peer);
    cluster.put(b"random_key1", b"random_val1").unwrap();

    // Blocks the raft apply process on store 1 entirely.
    let (apply_triggered_tx, apply_triggered_rx) = mpsc::bounded::<()>(1);
    let (apply_released_tx, apply_released_rx) = mpsc::bounded::<()>(1);
    fail::cfg_callback("on_handle_apply_store_1", move || {
        let _ = apply_triggered_tx.send(());
        let _ = apply_released_rx.recv();
    })
    .unwrap();

    // Manually makes an update, and wait for the apply to be triggered, to
    // simulate "some entries are committed but not applied" scenario.
    cluster.put(b"random_key2", b"random_val2").unwrap();
    apply_triggered_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();

    // Makes the group lose its quorum.
    cluster.stop_node(nodes[1]);
    cluster.stop_node(nodes[2]);

    // Triggers the unsafe recovery store reporting process.
    let plan = pdpb::RecoveryPlan::default();
    pd_client.must_set_unsafe_recovery_plan(nodes[0], plan);
    cluster.must_send_store_heartbeat(nodes[0]);

    // No store report is sent, since there are peers have unapplied entries.
    for _ in 0..20 {
        assert_eq!(pd_client.must_get_store_report(nodes[0]), None);
        sleep_ms(100);
    }

    // Unblocks the apply process.
    drop(apply_released_tx);

    // Store reports are sent once the entries are applied.
    let mut store_report = None;
    for _ in 0..20 {
        store_report = pd_client.must_get_store_report(nodes[0]);
        if store_report.is_some() {
            break;
        }
        sleep_ms(100);
    }
    assert_ne!(store_report, None);
    fail::remove("on_handle_apply_store_1");
}

#[test_case(test_raftstore::new_node_cluster)]
// #[test_case(test_raftstore_v2::new_node_cluster)]
fn test_unsafe_recovery_timeout_abort() {
    let mut cluster = new_cluster(0, 3);
    cluster.cfg.raft_store.raft_election_timeout_ticks = 5;
    cluster.cfg.raft_store.raft_store_max_leader_lease = ReadableDuration::millis(40);
    cluster.cfg.raft_store.max_leader_missing_duration = ReadableDuration::millis(150);
    cluster.cfg.raft_store.abnormal_leader_missing_duration = ReadableDuration::millis(100);
    cluster.cfg.raft_store.peer_stale_state_check_interval = ReadableDuration::millis(100);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();

    // Makes the leadership definite.
    let store2_peer = find_peer(&region, nodes[1]).unwrap().to_owned();
    cluster.must_transfer_leader(region.get_id(), store2_peer);
    cluster.put(b"random_key1", b"random_val1").unwrap();

    // Blocks the raft apply process on store 1 entirely.
    let (apply_triggered_tx, apply_triggered_rx) = mpsc::bounded::<()>(1);
    let (apply_released_tx, apply_released_rx) = mpsc::bounded::<()>(1);
    fail::cfg_callback("on_handle_apply_store_1", move || {
        let _ = apply_triggered_tx.send(());
        let _ = apply_released_rx.recv();
    })
    .unwrap();

    // Manually makes an update, and wait for the apply to be triggered, to
    // simulate "some entries are committed but not applied" scenario.
    cluster.put(b"random_key2", b"random_val2").unwrap();
    apply_triggered_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();

    // Makes the group lose its quorum.
    cluster.stop_node(nodes[1]);
    cluster.stop_node(nodes[2]);

    // Triggers the unsafe recovery store reporting process.
    let plan = pdpb::RecoveryPlan::default();
    pd_client.must_set_unsafe_recovery_plan(nodes[0], plan);
    cluster.must_send_store_heartbeat(nodes[0]);

    // sleep for a while to trigger timeout
    fail::cfg("unsafe_recovery_state_timeout", "return").unwrap();
    sleep_ms(200);
    fail::remove("unsafe_recovery_state_timeout");

    // Unblocks the apply process.
    drop(apply_released_tx);

    // No store report is sent, cause the plan is aborted.
    for _ in 0..20 {
        assert_eq!(pd_client.must_get_store_report(nodes[0]), None);
        sleep_ms(100);
    }

    // resend the plan
    let plan = pdpb::RecoveryPlan::default();
    pd_client.must_set_unsafe_recovery_plan(nodes[0], plan);
    cluster.must_send_store_heartbeat(nodes[0]);

    // Store reports are sent once the entries are applied.
    let mut store_report = None;
    for _ in 0..20 {
        store_report = pd_client.must_get_store_report(nodes[0]);
        if store_report.is_some() {
            break;
        }
        sleep_ms(100);
    }
    assert_ne!(store_report, None);
    fail::remove("on_handle_apply_store_1");
}

#[test]
fn test_unsafe_recovery_execution_result_report() {
    let mut cluster = new_server_cluster(0, 3);
    // Prolong force leader time.
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();

    // Makes the leadership definite.
    let store2_peer = find_peer(&region, nodes[1]).unwrap().to_owned();
    cluster.must_transfer_leader(region.get_id(), store2_peer);
    cluster.put(b"random_key1", b"random_val1").unwrap();

    // Split the region into 2, and remove one of them, so that we can test both
    // region peer list update and region creation.
    pd_client.must_split_region(
        region,
        pdpb::CheckPolicy::Usekey,
        vec![b"random_key1".to_vec()],
    );
    let region1 = pd_client.get_region(b"random_key".as_ref()).unwrap();
    let region2 = pd_client.get_region(b"random_key1".as_ref()).unwrap();
    let region1_store0_peer = find_peer(&region1, nodes[0]).unwrap().to_owned();
    pd_client.must_remove_peer(region1.get_id(), region1_store0_peer);
    cluster.must_remove_region(nodes[0], region1.get_id());

    // Makes the group lose its quorum.
    cluster.stop_node(nodes[1]);
    cluster.stop_node(nodes[2]);
    {
        let put = new_put_cmd(b"k2", b"v2");
        let req = new_request(
            region2.get_id(),
            region2.get_region_epoch().clone(),
            vec![put],
            true,
        );
        // marjority is lost, can't propose command successfully.
        cluster
            .call_command_on_leader(req, Duration::from_millis(10))
            .unwrap_err();
    }

    cluster.must_enter_force_leader(region2.get_id(), nodes[0], vec![nodes[1], nodes[2]]);

    // Construct recovery plan.
    let mut plan = pdpb::RecoveryPlan::default();

    let to_be_removed: Vec<metapb::Peer> = region2
        .get_peers()
        .iter()
        .filter(|&peer| peer.get_store_id() != nodes[0])
        .cloned()
        .collect();
    let mut demote = pdpb::DemoteFailedVoters::default();
    demote.set_region_id(region2.get_id());
    demote.set_failed_voters(to_be_removed.into());
    plan.mut_demotes().push(demote);

    let mut create = metapb::Region::default();
    create.set_id(101);
    create.set_end_key(b"random_key1".to_vec());
    let mut peer = metapb::Peer::default();
    peer.set_id(102);
    peer.set_store_id(nodes[0]);
    create.mut_peers().push(peer);
    plan.mut_creates().push(create);

    // Blocks the raft apply process on store 1 entirely .
    let (apply_released_tx, apply_released_rx) = mpsc::bounded::<()>(1);
    fail::cfg_callback("on_handle_apply_store_1", move || {
        let _ = apply_released_rx.recv();
    })
    .unwrap();

    // Triggers the unsafe recovery plan execution.
    pd_client.must_set_unsafe_recovery_plan(nodes[0], plan);
    cluster.must_send_store_heartbeat(nodes[0]);

    // No store report is sent, since there are peers have unapplied entries.
    for _ in 0..20 {
        assert_eq!(pd_client.must_get_store_report(nodes[0]), None);
        sleep_ms(100);
    }

    // Unblocks the apply process.
    drop(apply_released_tx);

    // Store reports are sent once the entries are applied.
    let mut store_report = None;
    for _ in 0..20 {
        store_report = pd_client.must_get_store_report(nodes[0]);
        if store_report.is_some() {
            break;
        }
        sleep_ms(100);
    }
    assert_ne!(store_report, None);
    for peer_report in store_report.unwrap().get_peer_reports() {
        let region = peer_report.get_region_state().get_region();
        if region.get_id() == 101 {
            assert_eq!(region.get_end_key(), b"random_key1".to_vec());
        } else {
            assert_eq!(region.get_id(), region2.get_id());
            for peer in region.get_peers() {
                if peer.get_store_id() != nodes[0] {
                    assert_eq!(peer.get_role(), metapb::PeerRole::Learner);
                }
            }
        }
    }
    fail::remove("on_handle_apply_store_1");
}

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_unsafe_recovery_wait_for_snapshot_apply() {
    let mut cluster = new_cluster(0, 3);
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(8);
    cluster.cfg.raft_store.merge_max_log_gap = 3;
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(10);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();

    // Makes the leadership definite.
    let store0_peer = find_peer(&region, nodes[0]).unwrap().to_owned();
    cluster.must_transfer_leader(region.get_id(), store0_peer.clone());
    cluster.stop_node(nodes[1]);

    // Compact logs to force requesting snapshot after clearing send filters.
    let state = cluster.truncated_state(region.get_id(), store0_peer.get_store_id());
    // Write some data to trigger snapshot.
    for i in 100..150 {
        let key = format!("k{}", i);
        let value = format!("v{}", i);
        cluster.must_put(key.as_bytes(), value.as_bytes());
    }
    cluster.wait_log_truncated(
        region.get_id(),
        store0_peer.get_store_id(),
        state.get_index() + 40,
    );

    // Makes the group lose its quorum.
    cluster.stop_node(nodes[2]);

    // Blocks the raft snap apply process.
    let (apply_triggered_tx, apply_triggered_rx) = mpsc::bounded::<()>(1);
    let (apply_released_tx, apply_released_rx) = mpsc::bounded::<()>(1);
    fail::cfg_callback("region_apply_snap", move || {
        let _ = apply_triggered_tx.send(());
        let _ = apply_released_rx.recv();
    })
    .unwrap();

    cluster.run_node(nodes[1]).unwrap();

    apply_triggered_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();

    // Triggers the unsafe recovery store reporting process.
    let plan = pdpb::RecoveryPlan::default();
    pd_client.must_set_unsafe_recovery_plan(nodes[1], plan);
    cluster.must_send_store_heartbeat(nodes[1]);

    // No store report is sent, since there are peers have unapplied entries.
    for _ in 0..20 {
        assert_eq!(pd_client.must_get_store_report(nodes[1]), None);
        sleep_ms(100);
    }

    // Unblocks the snap apply process.
    drop(apply_released_tx);

    // Store reports are sent once the entries are applied.
    let mut store_report = None;
    for _ in 0..20 {
        store_report = pd_client.must_get_store_report(nodes[1]);
        if store_report.is_some() {
            break;
        }
        sleep_ms(100);
    }
    assert_ne!(store_report, None);

    fail::remove("region_apply_snap");
}

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_unsafe_recovery_demotion_reentrancy() {
    let mut cluster = new_cluster(0, 3);
    cluster.cfg.raft_store.raft_store_max_leader_lease = ReadableDuration::millis(40);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();

    // Makes the leadership definite.
    let store2_peer = find_peer(&region, nodes[2]).unwrap().to_owned();
    cluster.must_transfer_leader(region.get_id(), store2_peer);

    // Makes the group lose its quorum.
    cluster.stop_node(nodes[1]);
    cluster.stop_node(nodes[2]);
    {
        let put = new_put_cmd(b"k2", b"v2");
        let req = new_request(
            region.get_id(),
            region.get_region_epoch().clone(),
            vec![put],
            true,
        );
        // marjority is lost, can't propose command successfully.
        cluster
            .call_command_on_leader(req, Duration::from_millis(10))
            .unwrap_err();
    }

    cluster.must_enter_force_leader(region.get_id(), nodes[0], vec![nodes[1], nodes[2]]);

    // Construct recovery plan.
    let mut plan = pdpb::RecoveryPlan::default();

    let to_be_removed: Vec<metapb::Peer> = region
        .get_peers()
        .iter()
        .filter(|&peer| peer.get_store_id() != nodes[0])
        .cloned()
        .collect();
    let mut demote = pdpb::DemoteFailedVoters::default();
    demote.set_region_id(region.get_id());
    demote.set_failed_voters(to_be_removed.into());
    plan.mut_demotes().push(demote);

    // Blocks the raft apply process on store 1 entirely .
    let (apply_released_tx, apply_released_rx) = mpsc::bounded::<()>(1);
    fail::cfg_callback("on_handle_apply_store_1", move || {
        let _ = apply_released_rx.recv();
    })
    .unwrap();

    // Triggers the unsafe recovery plan execution.
    pd_client.must_set_unsafe_recovery_plan(nodes[0], plan.clone());
    cluster.must_send_store_heartbeat(nodes[0]);

    // No store report is sent, since there are peers have unapplied entries.
    for _ in 0..10 {
        assert_eq!(pd_client.must_get_store_report(nodes[0]), None);
        sleep_ms(100);
    }

    // Send the plan again.
    pd_client.must_set_unsafe_recovery_plan(nodes[0], plan);
    cluster.must_send_store_heartbeat(nodes[0]);

    // Unblocks the apply process.
    drop(apply_released_tx);

    let mut demoted = false;
    for _ in 0..10 {
        let region_in_pd = block_on(pd_client.get_region_by_id(region.get_id()))
            .unwrap()
            .unwrap();
        assert_eq!(region_in_pd.get_peers().len(), 3);
        demoted = region_in_pd
            .get_peers()
            .iter()
            .filter(|peer| peer.get_store_id() != nodes[0])
            .all(|peer| peer.get_role() == metapb::PeerRole::Learner);
        sleep_ms(100);
    }
    assert_eq!(demoted, true);
    fail::remove("on_handle_apply_store_1");
}

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_unsafe_recovery_rollback_merge() {
    let mut cluster = new_cluster(0, 3);
    cluster.cfg.raft_store.raft_store_max_leader_lease = ReadableDuration::millis(40);
    cluster.cfg.raft_store.merge_check_tick_interval = ReadableDuration::millis(20);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    for i in 0..10 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v");
    }

    // Block merge commit, let go of the merge prepare.
    fail::cfg("on_schedule_merge", "return()").unwrap();

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");

    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();

    // Makes the leadership definite.
    let left_peer_2 = find_peer(&left, nodes[2]).unwrap().to_owned();
    let right_peer_2 = find_peer(&right, nodes[2]).unwrap().to_owned();
    cluster.must_transfer_leader(left.get_id(), left_peer_2);
    cluster.must_transfer_leader(right.get_id(), right_peer_2);
    cluster.try_merge(left.get_id(), right.get_id());

    let right_peer_0 = find_peer(&right, nodes[0]).unwrap().to_owned();
    pd_client.must_remove_peer(right.get_id(), right_peer_0);
    cluster.must_remove_region(nodes[0], right.get_id());
    // Makes the group lose its quorum.
    cluster.stop_node(nodes[1]);
    cluster.stop_node(nodes[2]);
    fail::remove("on_schedule_merge");
    {
        let put = new_put_cmd(b"k2", b"v2");
        let req = new_request(
            region.get_id(),
            region.get_region_epoch().clone(),
            vec![put],
            true,
        );
        // marjority is lost, can't propose command successfully.
        cluster
            .call_command_on_leader(req, Duration::from_millis(10))
            .unwrap_err();
    }

    cluster.must_enter_force_leader(left.get_id(), nodes[0], vec![nodes[1], nodes[2]]);
    // Allow rollback merge to finish.
    sleep_ms(100);

    // Construct recovery plan.
    let mut plan = pdpb::RecoveryPlan::default();

    let left_demote_peers: Vec<metapb::Peer> = left
        .get_peers()
        .iter()
        .filter(|&peer| peer.get_store_id() != nodes[0])
        .cloned()
        .collect();
    let mut left_demote = pdpb::DemoteFailedVoters::default();
    left_demote.set_region_id(left.get_id());
    left_demote.set_failed_voters(left_demote_peers.into());
    plan.mut_demotes().push(left_demote);

    // Triggers the unsafe recovery plan execution.
    pd_client.must_set_unsafe_recovery_plan(nodes[0], plan.clone());
    cluster.must_send_store_heartbeat(nodes[0]);

    let mut store_report = None;
    for _ in 0..20 {
        store_report = pd_client.must_get_store_report(nodes[0]);
        if store_report.is_some() {
            break;
        }
        sleep_ms(100);
    }
    assert_ne!(store_report, None);
    // Demotion is done
    let mut demoted = false;
    for _ in 0..10 {
        let new_left = block_on(pd_client.get_region_by_id(left.get_id()))
            .unwrap()
            .unwrap();
        assert_eq!(new_left.get_peers().len(), 3);
        demoted = new_left
            .get_peers()
            .iter()
            .filter(|peer| peer.get_store_id() != nodes[0])
            .all(|peer| peer.get_role() == metapb::PeerRole::Learner);
        if demoted {
            break;
        }
        sleep_ms(100);
    }
    assert_eq!(demoted, true);

    fail::remove("on_schedule_merge_ret_err");
}

// Test the compatibility between apply before persist with unsafe recovery.
// Currently only raftstore supports this feature.
#[test]
fn test_unsafe_recovery_apply_before_persist() {
    let mut cluster = new_node_cluster(0, 5);
    cluster.cfg.raft_store.raft_store_max_leader_lease = ReadableDuration::millis(40);
    cluster.cfg.raft_store.store_io_pool_size = 1;
    cluster.run();
    assert_eq!(cluster.get_node_ids().len(), 5);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let region = pd_client.get_region(b"k1").unwrap();
    let peer_1 = find_peer(&region, 1).cloned().unwrap();

    cluster.must_transfer_leader(region.get_id(), peer_1);

    for i in 0..10 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v1");
    }

    let raft_before_save_on_store_1_fp = "raft_before_persist_on_store_1";
    // skip persist to simulate raft log persist lag but not block node restart.
    fail::cfg(raft_before_save_on_store_1_fp, "return").unwrap();

    for i in 10..20 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v2");
    }

    fn get_applied_index<T: Simulator>(cluster: &Cluster<T>, store_id: u64) -> u64 {
        let state: RaftApplyState = cluster.engines[&store_id]
            .kv
            .get_msg_cf(CF_RAFT, &keys::apply_state_key(1))
            .unwrap()
            .unwrap_or_default();
        state.applied_index
    }

    let mut catch_up = false;
    for _i in 0..20 {
        let applied1 = get_applied_index(&cluster, 1);
        let applied2 = get_applied_index(&cluster, 2);
        if applied1 == applied2 {
            catch_up = true;
            break;
        }
        sleep_ms(50);
    }
    assert!(catch_up);

    let send_filter = Box::new(
        RegionPacketFilter::new(region.get_id(), 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend),
    );
    cluster.sim.wl().add_recv_filter(2, send_filter);

    for i in 20..30 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v3");
    }
    must_get_none(&cluster.get_engine(2), "k20".as_bytes());
    let mut catch_up = false;
    for _i in 0..20 {
        let applied1 = get_applied_index(&cluster, 1);
        let applied3 = get_applied_index(&cluster, 3);
        if applied1 == applied3 {
            catch_up = true;
            break;
        }
        sleep_ms(50);
    }
    assert!(catch_up);

    let apply1 = get_applied_index(&cluster, 1);
    let apply2 = get_applied_index(&cluster, 2);
    let apply3 = get_applied_index(&cluster, 3);
    assert_eq!(apply1, apply3);
    assert_eq!(apply1, apply2 + 10);

    let get_committed_idx = |store_id: u64| {
        let state: RaftLocalState = cluster.engines[&store_id]
            .raft
            .get_raft_state(region.get_id())
            .unwrap()
            .unwrap();
        state.get_hard_state().commit
    };
    let commit1 = get_committed_idx(1);
    let commit2 = get_committed_idx(2);
    let commit3 = get_committed_idx(3);
    assert!(commit3 >= commit1 + 20);
    assert_eq!(commit3, commit2 + 10);
    assert_eq!(commit3, apply1);

    for i in [1, 3, 4, 5] {
        cluster.stop_node(i);
    }
    cluster.run_node(1).unwrap();

    cluster.sim.wl().clear_recv_filters(2);
    fail::remove(raft_before_save_on_store_1_fp);

    // Triggers the unsafe recovery store reporting process.
    let plan = pdpb::RecoveryPlan::default();
    for i in [1, 2] {
        pd_client.must_set_unsafe_recovery_plan(i, plan.clone());
        cluster.must_send_store_heartbeat(i);
    }
    // Store reports are sent once the entries are applied.
    let mut store_report = None;
    for _ in 0..20 {
        store_report = pd_client.must_get_store_report(1);
        if store_report.is_some() {
            break;
        }
        sleep_ms(20);
    }
    assert_ne!(store_report, None);

    cluster.must_enter_force_leader(region.get_id(), 1, vec![3, 4, 5]);
    // Allow rollback merge to finish.
    sleep_ms(100);

    // Construct recovery plan.
    let mut plan = pdpb::RecoveryPlan::default();

    let to_be_removed: Vec<metapb::Peer> = region
        .get_peers()
        .iter()
        .filter(|&peer| [3, 4, 5].contains(&peer.get_store_id()))
        .cloned()
        .collect();
    let mut demote = pdpb::DemoteFailedVoters::default();
    demote.set_region_id(region.get_id());
    demote.set_failed_voters(to_be_removed.into());
    plan.mut_demotes().push(demote);

    // Send the plan again.
    pd_client.must_set_unsafe_recovery_plan(1, plan);
    cluster.must_send_store_heartbeat(1);

    let mut demoted = false;
    for _ in 0..50 {
        let region_in_pd = block_on(pd_client.get_region_by_id(region.get_id()))
            .unwrap()
            .unwrap();
        assert_eq!(region_in_pd.get_peers().len(), 5);
        demoted = region_in_pd
            .get_peers()
            .iter()
            .filter(|peer| [3, 4, 5].contains(&peer.get_store_id()))
            .all(|peer| peer.get_role() == metapb::PeerRole::Learner);
        sleep_ms(100);
    }
    assert_eq!(demoted, true);

    // Test after recovery, the store 2 should also contain all the data.
    must_get_equal(&cluster.get_engine(2), b"k29", b"v3");
}
