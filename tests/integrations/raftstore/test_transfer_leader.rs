// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, thread, time::Duration};

use engine_traits::CF_LOCK;
use kvproto::kvrpcpb::Context;
use raft::eraftpb::MessageType;
use raftstore::store::LocksStatus;
use test_raftstore::*;
use tikv::storage::{
    kv::{SnapContext, SnapshotExt},
    Engine, Snapshot,
};
use tikv_util::config::*;
use txn_types::{Key, PessimisticLock};

fn test_basic_transfer_leader<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.cfg.raft_store.raft_heartbeat_ticks = 20;
    let reserved_time = Duration::from_millis(
        cluster.cfg.raft_store.raft_base_tick_interval.as_millis()
            * cluster.cfg.raft_store.raft_heartbeat_ticks as u64,
    );
    cluster.run();

    // transfer leader to (2, 2) first to make address resolve happen early.
    cluster.must_transfer_leader(1, new_peer(2, 2));
    cluster.must_transfer_leader(1, new_peer(1, 1));

    let mut region = cluster.get_region(b"k3");

    // ensure follower has latest entries before transfer leader.
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    // check if transfer leader is fast enough.
    let leader = cluster.leader_of_region(1).unwrap();
    let admin_req = new_transfer_leader_cmd(new_peer(2, 2));
    let mut req = new_admin_request(1, region.get_region_epoch(), admin_req);
    req.mut_header().set_peer(leader);
    cluster.call_command(req, Duration::from_secs(3)).unwrap();
    thread::sleep(reserved_time);
    assert_eq!(
        cluster.query_leader(2, 1, Duration::from_secs(5)),
        Some(new_peer(2, 2))
    );

    let mut req = new_request(
        region.get_id(),
        region.take_region_epoch(),
        vec![new_put_cmd(b"k3", b"v3")],
        false,
    );
    req.mut_header().set_peer(new_peer(2, 2));
    // transfer leader to (1, 1)
    cluster.must_transfer_leader(1, new_peer(1, 1));
    // send request to old leader (2, 2) directly and verify it fails
    let resp = cluster.call_command(req, Duration::from_secs(5)).unwrap();
    assert!(resp.get_header().get_error().has_not_leader());
}

#[test]
fn test_server_basic_transfer_leader() {
    let mut cluster = new_server_cluster(0, 3);
    test_basic_transfer_leader(&mut cluster);
}

fn test_pd_transfer_leader<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    cluster.must_put(b"k", b"v");

    // call command on this leader directly, must successfully.
    let mut region = cluster.get_region(b"");
    let mut req = new_request(
        region.get_id(),
        region.take_region_epoch(),
        vec![new_get_cmd(b"k")],
        false,
    );

    for id in 1..4 {
        // select a new leader to transfer
        pd_client.transfer_leader(region.get_id(), new_peer(id, id), vec![]);

        for _ in 0..100 {
            // reset leader and wait transfer successfully.
            cluster.reset_leader_of_region(1);

            sleep_ms(20);

            if let Some(leader) = cluster.leader_of_region(1) {
                if leader.get_id() == id {
                    // make sure new leader apply an entry on its term
                    // so we can use its local reader safely
                    cluster.must_put(b"k1", b"v1");
                    break;
                }
            }
        }

        assert_eq!(cluster.leader_of_region(1), Some(new_peer(id, id)));
        req.mut_header().set_peer(new_peer(id, id));
        debug!("requesting {:?}", req);
        let resp = cluster
            .call_command(req.clone(), Duration::from_secs(5))
            .unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        assert_eq!(resp.get_responses()[0].get_get().get_value(), b"v");
    }
}

fn test_pd_transfer_leader_multi_target<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    cluster.add_send_filter(IsolationFilterFactory::new(3));
    cluster.must_put(b"k2", b"v2");
    // append index of the cluster will finally be 2, 2, 1
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
    must_get_equal(&cluster.get_engine(2), b"k2", b"v2");
    must_get_none(&cluster.get_engine(3), b"k2");

    // select multi target leaders to transfer
    // set `peer` to 3, make sure the new leader comes from `peers`
    let mut region = cluster.get_region(b"");
    pd_client.transfer_leader(
        region.get_id(),
        new_peer(3, 3),
        vec![new_peer(2, 2), new_peer(3, 3)],
    );

    for _ in 0..100 {
        // reset leader and wait transfer successfully.
        cluster.reset_leader_of_region(1);

        sleep_ms(20);

        if let Some(leader) = cluster.leader_of_region(1) {
            if leader.get_id() == 2 {
                break;
            }
        }
    }

    // call command on this leader directly, must successfully.
    let mut req = new_request(
        region.get_id(),
        region.take_region_epoch(),
        vec![new_get_cmd(b"k1")],
        false,
    );
    assert!(cluster.leader_of_region(1) == Some(new_peer(2, 2)));
    let leader_id = cluster.leader_of_region(1).unwrap().id;
    req.mut_header().set_peer(new_peer(leader_id, leader_id));
    let resp = cluster.call_command(req, Duration::from_secs(5)).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    assert_eq!(resp.get_responses()[0].get_get().get_value(), b"v1");
}

#[test]
fn test_server_pd_transfer_leader() {
    let mut cluster = new_server_cluster(0, 3);
    test_pd_transfer_leader(&mut cluster);
}

#[test]
fn test_server_pd_transfer_leader_multi_target() {
    let mut cluster = new_server_cluster(0, 3);
    test_pd_transfer_leader_multi_target(&mut cluster);
}

fn test_transfer_leader_during_snapshot<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_operator();
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(20);
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(2);
    cluster.cfg.raft_store.merge_max_log_gap = 1;

    let r1 = cluster.run_conf_change();
    pd_client.must_add_peer(r1, new_peer(2, 2));

    for i in 0..1024 {
        let key = format!("{:01024}", i);
        let value = format!("{:01024}", i);
        cluster.must_put(key.as_bytes(), value.as_bytes());
    }

    cluster.must_transfer_leader(r1, new_peer(1, 1));

    // hook transport and drop all snapshot packet, so follower's status
    // will stay at snapshot.
    cluster.add_send_filter(DefaultFilterFactory::<SnapshotFilter>::default());
    // don't allow leader transfer succeed if it is actually triggered.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 2)
            .msg_type(MessageType::MsgTimeoutNow)
            .direction(Direction::Recv),
    ));

    pd_client.must_add_peer(r1, new_peer(3, 3));
    // a just added peer needs wait a couple of ticks, it'll communicate with leader
    // before getting snapshot
    sleep_ms(1000);

    let epoch = cluster.get_region_epoch(1);
    let put = new_request(1, epoch, vec![new_put_cmd(b"k1", b"v1")], false);
    cluster.transfer_leader(r1, new_peer(2, 2));
    let resp = cluster.call_command_on_leader(put, Duration::from_secs(5));
    // if it's transferring leader, resp will timeout.
    assert!(resp.is_ok(), "{:?}", resp);
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
}

#[test]
fn test_server_transfer_leader_during_snapshot() {
    let mut cluster = new_server_cluster(0, 3);
    test_transfer_leader_during_snapshot(&mut cluster);
}

#[test]
fn test_sync_max_ts_after_leader_transfer() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.raft_heartbeat_ticks = 20;
    cluster.run();

    let cm = cluster.sim.read().unwrap().get_concurrency_manager(1);
    let storage = cluster
        .sim
        .read()
        .unwrap()
        .storages
        .get(&1)
        .unwrap()
        .clone();
    let wait_for_synced = |cluster: &mut Cluster<ServerCluster>| {
        let region_id = 1;
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
    };

    cluster.must_transfer_leader(1, new_peer(1, 1));
    wait_for_synced(&mut cluster);
    let max_ts = cm.max_ts();

    cluster.pd_client.trigger_tso_failure();
    // Transfer the leader out and back
    cluster.must_transfer_leader(1, new_peer(2, 2));
    cluster.must_transfer_leader(1, new_peer(1, 1));

    wait_for_synced(&mut cluster);
    let new_max_ts = cm.max_ts();
    assert!(new_max_ts > max_ts);
}

#[test]
fn test_propose_in_memory_pessimistic_locks() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.raft_heartbeat_ticks = 20;
    cluster.run();

    let region_id = 1;
    cluster.must_transfer_leader(1, new_peer(1, 1));

    let snapshot = cluster.must_get_snapshot_of_region(region_id);
    let txn_ext = snapshot.txn_ext.unwrap();
    let lock = PessimisticLock {
        primary: b"key".to_vec().into_boxed_slice(),
        start_ts: 10.into(),
        ttl: 3000,
        for_update_ts: 20.into(),
        min_commit_ts: 30.into(),
    };
    // Write a pessimistic lock to the in-memory pessimistic lock table.
    {
        let mut pessimistic_locks = txn_ext.pessimistic_locks.write();
        assert!(pessimistic_locks.is_writable());
        assert!(
            pessimistic_locks
                .insert(vec![(Key::from_raw(b"key"), lock.clone())])
                .is_ok()
        );
    }

    cluster.must_transfer_leader(1, new_peer(2, 2));

    // After the leader is transferred to store 2, we should be able to get the lock
    // in the lock CF.
    let snapshot = cluster.must_get_snapshot_of_region(region_id);
    let value = snapshot
        .get_cf(CF_LOCK, &Key::from_raw(b"key"))
        .unwrap()
        .unwrap();
    assert_eq!(value, lock.into_lock().to_bytes());
}

#[test]
fn test_memory_pessimistic_locks_status_after_transfer_leader_failure() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.raft_heartbeat_ticks = 20;
    cluster.cfg.raft_store.reactive_memory_lock_tick_interval = ReadableDuration::millis(200);
    cluster.cfg.raft_store.reactive_memory_lock_timeout_tick = 3;
    cluster.run();

    cluster.must_transfer_leader(1, new_peer(1, 1));

    let snapshot = cluster.must_get_snapshot_of_region(1);
    let txn_ext = snapshot.txn_ext.unwrap();
    let lock = PessimisticLock {
        primary: b"key".to_vec().into_boxed_slice(),
        start_ts: 10.into(),
        ttl: 3000,
        for_update_ts: 20.into(),
        min_commit_ts: 30.into(),
    };
    // Write a pessimistic lock to the in-memory pessimistic lock table.
    assert!(
        txn_ext
            .pessimistic_locks
            .write()
            .insert(vec![(Key::from_raw(b"key"), lock)])
            .is_ok()
    );

    // Make it fail to transfer leader
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 2)
            .msg_type(MessageType::MsgTimeoutNow)
            .direction(Direction::Recv),
    ));
    cluster.transfer_leader(1, new_peer(2, 2));

    thread::sleep(Duration::from_millis(200));
    // At first, the locks status will become invalid.
    assert_eq!(
        txn_ext.pessimistic_locks.read().status,
        LocksStatus::TransferringLeader
    );

    // After several ticks, in-memory pessimistic locks should become available again.
    thread::sleep(Duration::from_secs(1));
    assert_eq!(txn_ext.pessimistic_locks.read().status, LocksStatus::Normal);
    cluster.reset_leader_of_region(1);
    assert_eq!(cluster.leader_of_region(1).unwrap().get_store_id(), 1);
}
