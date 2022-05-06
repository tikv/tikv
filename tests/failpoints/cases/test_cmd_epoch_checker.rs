// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::mpsc::{self, TryRecvError},
    time::Duration,
};

use engine_rocks::RocksSnapshot;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse};
use raft::eraftpb::MessageType;
use raftstore::store::msg::*;
use test_raftstore::*;
use tikv_util::HandyRwLock;

struct CbReceivers {
    proposed: mpsc::Receiver<()>,
    committed: mpsc::Receiver<()>,
    applied: mpsc::Receiver<RaftCmdResponse>,
}

impl CbReceivers {
    fn assert_not_ready(&self) {
        sleep_ms(100);
        assert_eq!(self.proposed.try_recv().unwrap_err(), TryRecvError::Empty);
        assert_eq!(self.committed.try_recv().unwrap_err(), TryRecvError::Empty);
        assert_eq!(self.applied.try_recv().unwrap_err(), TryRecvError::Empty);
    }

    fn assert_ok(&self) {
        self.assert_applied_ok();
        // proposed and committed should be invoked before applied
        self.proposed.try_recv().unwrap();
        self.committed.try_recv().unwrap();
    }

    // When fails to propose, only applied callback will be invoked.
    fn assert_err(&self) {
        let resp = self.applied.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(resp.get_header().has_error(), "{:?}", resp);
        self.proposed.try_recv().unwrap_err();
        self.committed.try_recv().unwrap_err();
    }

    fn assert_applied_ok(&self) {
        let resp = self.applied.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(
            !resp.get_header().has_error(),
            "{:?}",
            resp.get_header().get_error()
        );
    }

    fn assert_proposed_ok(&self) {
        self.proposed.recv_timeout(Duration::from_secs(1)).unwrap();
    }
}

fn make_cb(cmd: &RaftCmdRequest) -> (Callback<RocksSnapshot>, CbReceivers) {
    let (proposed_tx, proposed_rx) = mpsc::channel();
    let (committed_tx, committed_rx) = mpsc::channel();
    let (cb, applied_rx) = make_cb_ext(
        cmd,
        Some(Box::new(move || proposed_tx.send(()).unwrap())),
        Some(Box::new(move || committed_tx.send(()).unwrap())),
    );
    (
        cb,
        CbReceivers {
            proposed: proposed_rx,
            committed: committed_rx,
            applied: applied_rx,
        },
    )
}

fn make_write_req(cluster: &mut Cluster<NodeCluster>, k: &[u8]) -> RaftCmdRequest {
    let r = cluster.get_region(k);
    let mut req = new_request(
        r.get_id(),
        r.get_region_epoch().clone(),
        vec![new_put_cmd(k, b"v")],
        false,
    );
    let leader = cluster.leader_of_region(r.get_id()).unwrap();
    req.mut_header().set_peer(leader);
    req
}

#[test]
fn test_reject_proposal_during_region_split() {
    let mut cluster = new_node_cluster(0, 3);
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.must_put(b"k", b"v");

    // Pause on applying so that region split is not finished.
    let fp = "apply_before_split";
    fail::cfg(fp, "pause").unwrap();

    // Try to split region.
    let (split_tx, split_rx) = mpsc::channel();
    let cb = Callback::Read(Box::new(move |resp: ReadResponse<RocksSnapshot>| {
        split_tx.send(resp.response).unwrap()
    }));
    let r = cluster.get_region(b"");
    cluster.split_region(&r, b"k", cb);
    split_rx
        .recv_timeout(Duration::from_millis(100))
        .unwrap_err();

    // Try to put a key.
    let propose_batch_raft_command_fp = "propose_batch_raft_command";
    let mut receivers = vec![];
    for i in 0..2 {
        if i == 1 {
            // Test another path of calling proposed callback.
            fail::cfg(propose_batch_raft_command_fp, "2*return").unwrap();
        }
        let write_req = make_write_req(&mut cluster, b"k1");
        let (cb, cb_receivers) = make_cb(&write_req);
        cluster
            .sim
            .rl()
            .async_command_on_node(1, write_req, cb)
            .unwrap();
        // The write request should be blocked until split is finished.
        cb_receivers.assert_not_ready();
        receivers.push(cb_receivers);
    }

    fail::remove(fp);
    // Split is finished.
    assert!(
        !split_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .get_header()
            .has_error()
    );

    // The write request fails due to epoch not match.
    for r in receivers {
        r.assert_err();
    }

    // New write request can succeed.
    let write_req = make_write_req(&mut cluster, b"k1");
    let (cb, cb_receivers) = make_cb(&write_req);
    cluster
        .sim
        .rl()
        .async_command_on_node(1, write_req, cb)
        .unwrap();
    cb_receivers.assert_ok();
}

#[test]
fn test_reject_proposal_during_region_merge() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.must_put(b"k", b"v");

    let r = cluster.get_region(b"");
    cluster.must_split(&r, b"k");
    // Let the new region catch up.
    cluster.must_put(b"a", b"v");
    cluster.must_put(b"k", b"v");

    let prepare_merge_fp = "apply_before_prepare_merge";
    let commit_merge_fp = "apply_before_commit_merge";

    // Pause on applying so that prepare-merge is not finished.
    fail::cfg(prepare_merge_fp, "pause").unwrap();
    // Try to merge region.
    let (merge_tx, merge_rx) = mpsc::channel();
    let cb = Callback::Read(Box::new(move |resp: ReadResponse<RocksSnapshot>| {
        merge_tx.send(resp.response).unwrap()
    }));
    let source = cluster.get_region(b"");
    let target = cluster.get_region(b"k");
    cluster.merge_region(source.get_id(), target.get_id(), cb);
    merge_rx
        .recv_timeout(Duration::from_millis(100))
        .unwrap_err();

    // Try to put a key on the source region.
    let propose_batch_raft_command_fp = "propose_batch_raft_command";
    let mut receivers = vec![];
    for i in 0..2 {
        if i == 1 {
            // Test another path of calling proposed callback.
            fail::cfg(propose_batch_raft_command_fp, "2*return").unwrap();
        }
        let write_req = make_write_req(&mut cluster, b"a");
        let (cb, cb_receivers) = make_cb(&write_req);
        cluster
            .sim
            .rl()
            .async_command_on_node(1, write_req, cb)
            .unwrap();
        // The write request should be blocked until prepare-merge is finished.
        cb_receivers.assert_not_ready();
        receivers.push(cb_receivers);
    }

    // Pause on the second phase of region merge.
    fail::cfg(commit_merge_fp, "pause").unwrap();

    // prepare-merge is finished.
    fail::remove(prepare_merge_fp);
    assert!(
        !merge_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .get_header()
            .has_error()
    );
    // The write request fails due to epoch not match.
    for r in receivers {
        r.assert_err();
    }

    // Write request is rejected because the source region is merging.
    // It's not handled by epoch checker now.
    for i in 0..2 {
        if i == 1 {
            // Test another path of calling proposed callback.
            fail::cfg(propose_batch_raft_command_fp, "2*return").unwrap();
        }
        let write_req = make_write_req(&mut cluster, b"a");
        let (cb, cb_receivers) = make_cb(&write_req);
        cluster
            .sim
            .rl()
            .async_command_on_node(1, write_req, cb)
            .unwrap();
        cb_receivers.assert_err();
    }

    // Try to put a key on the target region.
    let mut receivers = vec![];
    for i in 0..2 {
        if i == 1 {
            // Test another path of calling proposed callback.
            fail::cfg(propose_batch_raft_command_fp, "2*return").unwrap();
        }
        let write_req = make_write_req(&mut cluster, b"k");
        let (cb, cb_receivers) = make_cb(&write_req);
        cluster
            .sim
            .rl()
            .async_command_on_node(1, write_req, cb)
            .unwrap();
        // The write request should be blocked until commit-merge is finished.
        cb_receivers.assert_not_ready();
        receivers.push(cb_receivers);
    }

    // Wait for region merge done.
    fail::remove(commit_merge_fp);
    pd_client.check_merged_timeout(source.get_id(), Duration::from_secs(5));
    // The write request fails due to epoch not match.
    for r in receivers {
        r.assert_err();
    }

    // New write request can succeed.
    let write_req = make_write_req(&mut cluster, b"k");
    let (cb, cb_receivers) = make_cb(&write_req);
    cluster
        .sim
        .rl()
        .async_command_on_node(1, write_req, cb)
        .unwrap();
    cb_receivers.assert_ok();
}

#[test]
fn test_reject_proposal_during_rollback_region_merge() {
    let mut cluster = new_node_cluster(0, 2);
    configure_for_merge(&mut cluster);
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();
    cluster.run_conf_change();

    let r = cluster.get_region(b"");
    cluster.must_split(&r, b"k");

    // Don't enter the second phase of region merge.
    let schedule_merge_fp = "on_schedule_merge";
    fail::cfg(schedule_merge_fp, "return()").unwrap();

    let source = cluster.get_region(b"");
    let target = cluster.get_region(b"k");
    // The call is finished when prepare_merge is applied.
    cluster.must_try_merge(source.get_id(), target.get_id());

    // Add a peer to trigger rollback.
    pd_client.must_add_peer(target.get_id(), new_peer(2, 4));
    cluster.must_put(b"k", b"v");
    must_get_equal(&cluster.get_engine(1), b"k", b"v");

    // Pause on applying so that rolling back merge is not finished.
    let rollback_merge_fp = "apply_before_rollback_merge";
    fail::cfg(rollback_merge_fp, "pause").unwrap();
    fail::remove(schedule_merge_fp);
    sleep_ms(200);

    // Write request is rejected because the source region is merging.
    // It's not handled by epoch checker now.
    let propose_batch_raft_command_fp = "propose_batch_raft_command";
    for i in 0..2 {
        if i == 1 {
            // Test another path of calling proposed callback.
            fail::cfg(propose_batch_raft_command_fp, "2*return").unwrap();
        }
        let write_req = make_write_req(&mut cluster, b"a");
        let (cb, cb_receivers) = make_cb(&write_req);
        cluster
            .sim
            .rl()
            .async_command_on_node(1, write_req, cb)
            .unwrap();
        cb_receivers.assert_err();
    }

    fail::remove(rollback_merge_fp);
    // Make sure the rollback is done.
    cluster.must_put(b"a", b"v");

    // New write request can succeed.
    let write_req = make_write_req(&mut cluster, b"a");
    let (cb, cb_receivers) = make_cb(&write_req);
    cluster
        .sim
        .rl()
        .async_command_on_node(1, write_req, cb)
        .unwrap();
    cb_receivers.assert_ok();
}

#[test]
fn test_reject_proposal_during_leader_transfer() {
    let mut cluster = new_node_cluster(0, 2);
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();
    let r = cluster.run_conf_change();
    pd_client.must_add_peer(r, new_peer(2, 2));

    // Don't allow leader transfer succeed if it is actually triggered.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(r, 2)
            .msg_type(MessageType::MsgTimeoutNow)
            .direction(Direction::Recv),
    ));

    cluster.must_put(b"k", b"v");
    cluster.transfer_leader(r, new_peer(2, 2));
    // The leader can't change to transferring state immediately due to pre-transfer-leader
    // feature, so wait for a while.
    sleep_ms(100);
    assert_ne!(cluster.leader_of_region(r).unwrap(), new_peer(2, 2));

    let propose_batch_raft_command_fp = "propose_batch_raft_command";
    for i in 0..2 {
        if i == 1 {
            // Test another path of calling proposed callback.
            fail::cfg(propose_batch_raft_command_fp, "2*return").unwrap();
        }
        let write_req = make_write_req(&mut cluster, b"k");
        let (cb, cb_receivers) = make_cb(&write_req);
        cluster
            .sim
            .rl()
            .async_command_on_node(1, write_req, cb)
            .unwrap();
        cb_receivers.assert_err();
    }

    cluster.clear_send_filters();
}

#[test]
fn test_accept_proposal_during_conf_change() {
    let mut cluster = new_node_cluster(0, 2);
    cluster.pd_client.disable_default_operator();
    let r = cluster.run_conf_change();
    cluster.must_put(b"a", b"v");

    let conf_change_fp = "apply_on_conf_change_all_1";
    fail::cfg(conf_change_fp, "pause").unwrap();
    let add_peer_rx = cluster.async_add_peer(r, new_peer(2, 2)).unwrap();
    add_peer_rx
        .recv_timeout(Duration::from_millis(100))
        .unwrap_err();

    // Conf change doesn't affect proposals.
    let write_req = make_write_req(&mut cluster, b"k");
    let (cb, cb_receivers) = make_cb(&write_req);
    cluster
        .sim
        .rl()
        .async_command_on_node(1, write_req, cb)
        .unwrap();
    cb_receivers
        .committed
        .recv_timeout(Duration::from_millis(300))
        .unwrap();
    cb_receivers.proposed.try_recv().unwrap();

    fail::remove(conf_change_fp);
    assert!(
        !add_peer_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .get_header()
            .has_error()
    );
    assert!(
        !cb_receivers
            .applied
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .get_header()
            .has_error()
    );
    must_get_equal(&cluster.get_engine(2), b"k", b"v");
}

#[test]
fn test_not_invoke_committed_cb_when_fail_to_commit() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.must_put(b"k", b"v");

    // Partiton the leader and followers to let the leader fails to commit the proposal.
    cluster.partition(vec![1], vec![2, 3]);
    let write_req = make_write_req(&mut cluster, b"k1");
    let (cb, cb_receivers) = make_cb(&write_req);
    cluster
        .sim
        .rl()
        .async_command_on_node(1, write_req, cb)
        .unwrap();
    // Check the request is proposed but not committed.
    cb_receivers
        .committed
        .recv_timeout(Duration::from_millis(200))
        .unwrap_err();
    cb_receivers.proposed.try_recv().unwrap();

    // The election timeout is 250ms by default.
    let election_timeout = cluster.cfg.raft_store.raft_base_tick_interval.0
        * cluster.cfg.raft_store.raft_election_timeout_ticks as u32;
    std::thread::sleep(2 * election_timeout);

    // Make sure a new leader is elected and will discard the previous proposal when partition is
    // recovered.
    cluster.must_put(b"k2", b"v");
    cluster.clear_send_filters();

    let resp = cb_receivers
        .applied
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert!(resp.get_header().has_error(), "{:?}", resp);
    // The committed callback shouldn't be invoked.
    cb_receivers.committed.try_recv().unwrap_err();
}

#[test]
fn test_propose_before_transfer_leader() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.must_put(b"k", b"v");

    let propose_batch_raft_command_fp = "propose_batch_raft_command";
    fail::cfg(propose_batch_raft_command_fp, "return").unwrap();

    let write_req = make_write_req(&mut cluster, b"k1");
    let (cb, cb_receivers) = make_cb(&write_req);
    cluster
        .sim
        .rl()
        .async_command_on_node(1, write_req, cb)
        .unwrap();
    // Proposed cb is called.
    cb_receivers.assert_proposed_ok();

    cluster.must_transfer_leader(1, new_peer(2, 2));

    // Write request should succeed.
    cb_receivers.assert_applied_ok();
    must_get_equal(&cluster.get_engine(2), b"k1", b"v");
}

#[test]
fn test_propose_before_split_and_merge() {
    let mut cluster = new_node_cluster(0, 3);
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.must_put(b"k", b"v");

    let propose_batch_raft_command_fp = "propose_batch_raft_command";
    fail::cfg(propose_batch_raft_command_fp, "return").unwrap();

    let write_req = make_write_req(&mut cluster, b"k1");
    let (cb, cb_receivers) = make_cb(&write_req);
    cluster
        .sim
        .rl()
        .async_command_on_node(1, write_req, cb)
        .unwrap();
    // Proposed cb is called.
    cb_receivers.assert_proposed_ok();

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k2");

    cb_receivers.assert_applied_ok();
    must_get_equal(&cluster.get_engine(1), b"k1", b"v");

    let left = cluster.get_region(b"k1");
    let right = cluster.get_region(b"k2");
    let left_peer1 = find_peer(&left, 1).unwrap().to_owned();
    let right_peer2 = find_peer(&right, 2).unwrap().to_owned();

    cluster.must_transfer_leader(left.get_id(), left_peer1);
    cluster.must_transfer_leader(right.get_id(), right_peer2);

    let write_req = make_write_req(&mut cluster, b"k0");
    let (cb, cb_receivers) = make_cb(&write_req);
    cluster
        .sim
        .rl()
        .async_command_on_node(1, write_req, cb)
        .unwrap();
    // Proposed cb is called.
    cb_receivers.assert_proposed_ok();

    let write_req2 = make_write_req(&mut cluster, b"k2");
    let (cb2, cb_receivers2) = make_cb(&write_req2);
    cluster
        .sim
        .rl()
        .async_command_on_node(2, write_req2, cb2)
        .unwrap();
    // Proposed cb is called.
    cb_receivers2.assert_proposed_ok();

    pd_client.must_merge(left.get_id(), right.get_id());

    // Write request should succeed.
    cb_receivers.assert_applied_ok();
    must_get_equal(&cluster.get_engine(1), b"k0", b"v");

    cb_receivers2.assert_applied_ok();
    must_get_equal(&cluster.get_engine(2), b"k2", b"v");
}
