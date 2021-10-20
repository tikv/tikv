// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::disk_usage::DiskUsage;
use kvproto::kvrpcpb::{DiskFullOpt, Op};
use kvproto::metapb::Region;
use kvproto::raft_cmdpb::*;
use raft::eraftpb::MessageType;
use raftstore::store::msg::*;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use test_raftstore::*;

fn mocked() -> bool {
    true
}
fn assert_disk_full(resp: &RaftCmdResponse) {
    assert!(resp.get_header().get_error().has_disk_full());
}

fn disk_full_stores(resp: &RaftCmdResponse) -> Vec<u64> {
    let region_error = resp.get_header().get_error();
    assert!(region_error.has_disk_full());
    let mut stores = region_error.get_disk_full().get_store_id().to_vec();
    stores.sort_unstable();
    stores
}

fn get_fp(usage: DiskUsage, store_id: u64) -> String {
    match usage {
        DiskUsage::AlmostFull => format!("disk_almost_full_peer_{}", store_id),
        DiskUsage::AlreadyFull => format!("disk_already_full_peer_{}", store_id),
        _ => unreachable!(),
    }
}

fn ensure_disk_usage_is_reported<T: Simulator>(
    cluster: &mut Cluster<T>,
    peer_id: u64,
    store_id: u64,
    region: &Region,
) {
    let peer = new_peer(store_id, peer_id);
    let key = region.get_start_key();
    let ch = async_read_on_peer(cluster, peer, region.clone(), key, true, true);
    assert!(ch.recv_timeout(Duration::from_secs(1)).is_ok());
}

fn test_disk_full_leader_behaviors(usage: DiskUsage) {
    // Mocked in v5.2
    if !matches!(usage, DiskUsage::Normal) {
        return;
    }
    let mut cluster = new_node_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    cluster.run();

    // To ensure all replicas are not pending.
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    cluster.must_transfer_leader(1, new_peer(1, 1));
    fail::cfg(get_fp(usage, 1), "return").unwrap();

    // Test new normal proposals won't be allowed when disk is full.
    let old_last_index = cluster.raft_local_state(1, 1).last_index;
    let rx = cluster.async_put(b"k2", b"v2").unwrap();
    assert_disk_full(&rx.recv_timeout(Duration::from_secs(2)).unwrap());
    let new_last_index = cluster.raft_local_state(1, 1).last_index;
    assert_eq!(old_last_index, new_last_index);

    // Test split won't be allowed when disk is full.
    let old_last_index = cluster.raft_local_state(1, 1).last_index;
    let region = cluster.get_region(b"k1");
    let (tx, rx) = mpsc::sync_channel(1);
    cluster.split_region(
        &region,
        b"k1",
        Callback::write(Box::new(move |resp| tx.send(resp.response).unwrap())),
    );
    assert_disk_full(&rx.recv_timeout(Duration::from_secs(2)).unwrap());
    let new_last_index = cluster.raft_local_state(1, 1).last_index;
    assert_eq!(old_last_index, new_last_index);

    // Test transfer leader should be allowed.
    cluster.must_transfer_leader(1, new_peer(2, 2));

    // Transfer the leadership back to store 1.
    fail::remove(get_fp(usage, 1));
    cluster.must_transfer_leader(1, new_peer(1, 1));
    fail::cfg(get_fp(usage, 1), "return").unwrap();

    if matches!(usage, DiskUsage::AlmostFull) {
        // Test remove peer should be allowed.
        cluster.pd_client.must_remove_peer(1, new_peer(3, 3));
        must_get_none(&cluster.get_engine(3), b"k1");

        // Test add peer should be allowed.
        cluster.pd_client.must_add_peer(1, new_peer(3, 3));
        must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    }

    fail::remove(get_fp(usage, 1));
}

#[test]
fn test_disk_full_for_region_leader() {
    test_disk_full_leader_behaviors(DiskUsage::AlmostFull);
    test_disk_full_leader_behaviors(DiskUsage::AlreadyFull);
}

fn test_disk_full_follower_behaviors(usage: DiskUsage) {
    // Mocked in v5.2
    if !matches!(usage, DiskUsage::Normal) {
        return;
    }
    let mut cluster = new_node_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    cluster.run();

    // To ensure all replicas are not pending.
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    cluster.must_transfer_leader(1, new_peer(1, 1));
    fail::cfg(get_fp(usage, 2), "return").unwrap();

    // Test followers will reject pre-transfer-leader command.
    let epoch = cluster.get_region_epoch(1);
    let transfer = new_admin_request(1, &epoch, new_transfer_leader_cmd(new_peer(2, 2)));
    cluster
        .call_command_on_leader(transfer, Duration::from_secs(3))
        .unwrap();
    assert_eq!(cluster.leader_of_region(1).unwrap(), new_peer(1, 1));
    cluster.must_put(b"k2", b"v2");

    // Test leader shouldn't append entries to disk full followers.
    let old_last_index = cluster.raft_local_state(1, 2).last_index;
    cluster.must_put(b"k3", b"v3");
    let new_last_index = cluster.raft_local_state(1, 2).last_index;
    assert_eq!(old_last_index, new_last_index);
    must_get_none(&cluster.get_engine(2), b"k3");

    // Test followers will response votes when disk is full.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 1)
            .direction(Direction::Send)
            .msg_type(MessageType::MsgRequestVoteResponse),
    ));
    cluster.must_transfer_leader(1, new_peer(3, 3));

    fail::remove(get_fp(usage, 2));
}

#[test]
fn test_disk_full_for_region_follower() {
    test_disk_full_follower_behaviors(DiskUsage::AlmostFull);
    test_disk_full_follower_behaviors(DiskUsage::AlreadyFull);
}

fn test_disk_full_txn_behaviors(usage: DiskUsage) {
    // Mocked in v5.2
    if !matches!(usage, DiskUsage::Normal) {
        return;
    }
    let mut cluster = new_server_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    cluster.run();

    // To ensure all replicas are not pending.
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    cluster.must_transfer_leader(1, new_peer(1, 1));
    fail::cfg(get_fp(usage, 1), "return").unwrap();

    // Test normal prewrite is not allowed.
    let pd_client = cluster.pd_client.clone();
    let lead_client = PeerClient::new(&cluster, 1, new_peer(1, 1));
    let prewrite_ts = get_tso(&pd_client);
    let res = lead_client.try_kv_prewrite(
        vec![new_mutation(Op::Put, b"k3", b"v3")],
        b"k4".to_vec(),
        prewrite_ts,
        DiskFullOpt::NotAllowedOnFull,
    );
    assert!(res.get_region_error().has_disk_full());

    fail::remove(get_fp(usage, 1));
    let prewrite_ts = get_tso(&pd_client);
    lead_client.must_kv_prewrite(
        vec![new_mutation(Op::Put, b"k4", b"v4")],
        b"k4".to_vec(),
        prewrite_ts,
    );

    // Test commit is allowed.
    fail::cfg(get_fp(usage, 1), "return").unwrap();
    let commit_ts = get_tso(&pd_client);
    lead_client.must_kv_commit(vec![b"k4".to_vec()], prewrite_ts, commit_ts);
    lead_client.must_kv_read_equal(b"k4".to_vec(), b"v4".to_vec(), commit_ts);

    // Test prewrite is allowed with a special `DiskFullOpt` flag.
    let prewrite_ts = get_tso(&pd_client);
    let res = lead_client.try_kv_prewrite(
        vec![new_mutation(Op::Put, b"k5", b"v5")],
        b"k4".to_vec(),
        prewrite_ts,
        DiskFullOpt::AllowedOnAlmostFull,
    );
    assert!(!res.get_region_error().has_disk_full());
    let commit_ts = get_tso(&pd_client);
    lead_client.must_kv_commit(vec![b"k5".to_vec()], prewrite_ts, commit_ts);
    assert!(!res.get_region_error().has_disk_full());

    fail::remove(get_fp(usage, 1));
    let lead_client = PeerClient::new(&cluster, 1, new_peer(1, 1));
    let prewrite_ts = get_tso(&pd_client);
    lead_client.must_kv_prewrite(
        vec![new_mutation(Op::Put, b"k6", b"v6")],
        b"k6".to_vec(),
        prewrite_ts,
    );

    // Test rollback must be allowed.
    fail::cfg(get_fp(usage, 1), "return").unwrap();
    PeerClient::new(&cluster, 1, new_peer(1, 1))
        .must_kv_rollback(vec![b"k6".to_vec()], prewrite_ts);

    fail::remove(get_fp(usage, 1));
    let start_ts = get_tso(&pd_client);
    lead_client.must_kv_pessimistic_lock(b"k7".to_vec(), start_ts);

    // Test pessimistic commit is allowed.
    fail::cfg(get_fp(usage, 1), "return").unwrap();
    let res = lead_client.try_kv_prewrite(
        vec![new_mutation(Op::Put, b"k5", b"v5")],
        b"k4".to_vec(),
        start_ts,
        DiskFullOpt::AllowedOnAlmostFull,
    );
    assert!(!res.get_region_error().has_disk_full());
    lead_client.must_kv_commit(vec![b"k7".to_vec()], start_ts, get_tso(&pd_client));

    fail::remove(get_fp(usage, 1));
    let lock_ts = get_tso(&pd_client);
    lead_client.must_kv_pessimistic_lock(b"k8".to_vec(), lock_ts);

    // Test pessmistic rollback is allowed.
    fail::cfg(get_fp(usage, 1), "return").unwrap();
    lead_client.must_kv_pessimistic_rollback(b"k8".to_vec(), lock_ts);

    fail::remove(get_fp(usage, 1));
}

#[test]
fn test_disk_full_for_txn_operations() {
    test_disk_full_txn_behaviors(DiskUsage::AlmostFull);
}

#[test]
fn test_majority_disk_full() {
    // Mocked
    if mocked() {
        return;
    }
    let mut cluster = new_node_cluster(0, 3);
    // To ensure the thread has full store disk usage infomation.
    cluster.cfg.raft_store.store_batch_system.pool_size = 1;
    cluster.pd_client.disable_default_operator();
    cluster.run();

    // To ensure all replicas are not pending.
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    cluster.must_transfer_leader(1, new_peer(1, 1));
    let region = cluster.get_region(b"k1");
    let epoch = region.get_region_epoch().clone();

    // To ensure followers have reported disk usages to the leader.
    for i in 1..3 {
        fail::cfg(get_fp(DiskUsage::AlmostFull, i + 1), "return").unwrap();
        ensure_disk_usage_is_reported(&mut cluster, i + 1, i + 1, &region);
    }

    // Normal proposals will be rejected because of majority peers' disk full.
    let ch = cluster.async_put(b"k2", b"v2").unwrap();
    let resp = ch.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(disk_full_stores(&resp), vec![2, 3]);

    // Proposals with special `DiskFullOpt`s can be accepted even if all peers are disk full.
    fail::cfg(get_fp(DiskUsage::AlmostFull, 1), "return").unwrap();
    let reqs = vec![new_put_cmd(b"k3", b"v3")];
    let put = new_request(1, epoch.clone(), reqs, false);
    let mut opts = RaftCmdExtraOpts::default();
    opts.disk_full_opt = DiskFullOpt::AllowedOnAlmostFull;
    let ch = cluster.async_request_with_opts(put, opts).unwrap();
    let resp = ch.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(!resp.get_header().has_error());

    // Reset disk full status for peer 2 and 3. 2 follower reads must success because the leader
    // will continue to append entries to followers after the new disk usages are reported.
    for i in 1..3 {
        fail::remove(get_fp(DiskUsage::AlmostFull, i + 1));
        ensure_disk_usage_is_reported(&mut cluster, i + 1, i + 1, &region);
        must_get_equal(&cluster.get_engine(i + 1), b"k3", b"v3");
    }

    // To ensure followers have reported disk usages to the leader.
    for i in 1..3 {
        fail::cfg(get_fp(DiskUsage::AlreadyFull, i + 1), "return").unwrap();
        ensure_disk_usage_is_reported(&mut cluster, i + 1, i + 1, &region);
    }

    // Proposals with special `DiskFullOpt`s will still be rejected if majority peers are already
    // disk full.
    let reqs = vec![new_put_cmd(b"k3", b"v3")];
    let put = new_request(1, epoch.clone(), reqs, false);
    let mut opts = RaftCmdExtraOpts::default();
    opts.disk_full_opt = DiskFullOpt::AllowedOnAlmostFull;
    let ch = cluster.async_request_with_opts(put, opts).unwrap();
    let resp = ch.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(disk_full_stores(&resp), vec![2, 3]);

    // Peer 2 disk usage changes from already full to almost full.
    fail::remove(get_fp(DiskUsage::AlreadyFull, 2));
    fail::cfg(get_fp(DiskUsage::AlmostFull, 2), "return").unwrap();
    ensure_disk_usage_is_reported(&mut cluster, 2, 2, &region);

    // Configuration change should be alloed.
    cluster.pd_client.must_remove_peer(1, new_peer(2, 2));

    // After the last configuration change is applied, the raft group will be like
    // `[(1, DiskUsage::AlmostFull), (3, DiskUsage::AlreadyFull)]`. So no more proposals
    // should be allowed.
    let reqs = vec![new_put_cmd(b"k4", b"v4")];
    let put = new_request(1, epoch, reqs, false);
    let mut opts = RaftCmdExtraOpts::default();
    opts.disk_full_opt = DiskFullOpt::AllowedOnAlmostFull;
    let ch = cluster.async_request_with_opts(put, opts).unwrap();
    let resp = ch.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(disk_full_stores(&resp), vec![3]);

    for i in 0..3 {
        fail::remove(get_fp(DiskUsage::AlreadyFull, i + 1));
        fail::remove(get_fp(DiskUsage::AlmostFull, i + 1));
    }
}

#[test]
fn test_disk_full_followers_with_hibernate_regions() {
    // Mocked.
    if mocked() {
        return;
    }
    let mut cluster = new_node_cluster(0, 2);
    // To ensure the thread has full store disk usage infomation.
    cluster.cfg.raft_store.store_batch_system.pool_size = 1;
    cluster.pd_client.disable_default_operator();
    let _ = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");

    // Add a peer which is almost disk full should be allowed.
    fail::cfg(get_fp(DiskUsage::AlmostFull, 2), "return").unwrap();
    cluster.pd_client.must_add_peer(1, new_peer(2, 2));
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    let tick_dur = cluster.cfg.raft_store.raft_base_tick_interval.0;
    let election_timeout = cluster.cfg.raft_store.raft_election_timeout_ticks;

    thread::sleep(tick_dur * 2 * election_timeout as u32);
    fail::remove(get_fp(DiskUsage::AlmostFull, 2));
    thread::sleep(tick_dur * 2);

    // The leader should know peer 2's disk usage changes, because it's keeping to tick.
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(2), b"k2", b"v2");
}
