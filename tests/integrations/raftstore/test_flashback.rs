// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use futures::executor::block_on;
use kvproto::metapb;
use test_raftstore::*;
use txn_types::WriteBatchFlags;

#[test]
fn test_flahsback_for_applied_index() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.run();

    // write for cluster.
    let value = vec![1_u8; 8096];
    multi_do_cmd(&mut cluster, new_put_cf_cmd("write", b"k1", &value));

    // prepare for flashback
    let region = cluster.get_region(b"k1");
    block_on(cluster.call_and_wait_prepare_flashback(region.get_id(), 1));

    let last_index = cluster
        .raft_local_state(region.get_id(), 1)
        .get_last_index();
    let appied_index = cluster.apply_state(region.get_id(), 1).get_applied_index();

    assert_eq!(last_index, appied_index);
}

#[test]
fn test_flashback_for_schedule() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.run();

    cluster.must_transfer_leader(1, new_peer(2, 2));
    cluster.must_transfer_leader(1, new_peer(1, 1));

    // prepare for flashback
    let region = cluster.get_region(b"k1");
    block_on(cluster.call_and_wait_prepare_flashback(region.get_id(), 1));

    // verify the schedule is unabled.
    let mut region = cluster.get_region(b"k3");
    let admin_req = new_transfer_leader_cmd(new_peer(2, 2));
    let mut transfer_leader =
        new_admin_request(region.get_id(), &region.take_region_epoch(), admin_req);
    transfer_leader.mut_header().set_peer(new_peer(1, 1));
    let resp = cluster
        .call_command_on_leader(transfer_leader, Duration::from_secs(3))
        .unwrap();
    let e = resp.get_header().get_error();
    assert_eq!(
        e.get_flashback_in_progress(),
        &kvproto::errorpb::FlashbackInProgress {
            region_id: region.get_id(),
            ..Default::default()
        }
    );

    // verify the schedule can be executed if add flashback flag in request's
    // header.
    let mut region = cluster.get_region(b"k3");
    let admin_req = new_transfer_leader_cmd(new_peer(2, 2));
    let mut transfer_leader =
        new_admin_request(region.get_id(), &region.take_region_epoch(), admin_req);
    transfer_leader.mut_header().set_peer(new_peer(1, 1));
    transfer_leader
        .mut_header()
        .set_flags(WriteBatchFlags::FLASHBACK.bits());
    let resp = cluster
        .call_command_on_leader(transfer_leader, Duration::from_secs(5))
        .unwrap();
    assert!(!resp.get_header().has_error());

    cluster.call_finish_flashback(region.get_id(), 1);
    // transfer leader to (1, 1)
    cluster.must_transfer_leader(1, new_peer(1, 1));
}

#[test]
fn test_flahsback_for_write() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.run();

    // write for cluster
    let value = vec![1_u8; 8096];
    multi_do_cmd(&mut cluster, new_put_cf_cmd("write", b"k1", &value));

    // prepare for flashback
    let region = cluster.get_region(b"k1");
    block_on(cluster.call_and_wait_prepare_flashback(region.get_id(), 1));

    // write will be blocked
    let value = vec![1_u8; 8096];
    must_get_error_flashback_in_progress(&mut cluster, &region, new_put_cmd(b"k1", &value));

    must_cmd_add_flashback_flag(
        &mut cluster,
        &mut region.clone(),
        new_put_cmd(b"k1", &value),
    );

    cluster.call_finish_flashback(region.get_id(), 1);

    multi_do_cmd(&mut cluster, new_put_cf_cmd("write", b"k1", &value));
}

#[test]
fn test_flahsback_for_read() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.run();

    // write for cluster
    let value = vec![1_u8; 8096];
    multi_do_cmd(&mut cluster, new_put_cf_cmd("write", b"k1", &value));
    // read for cluster
    multi_do_cmd(&mut cluster, new_get_cf_cmd("write", b"k1"));

    // prepare for flashback
    let region = cluster.get_region(b"k1");
    block_on(cluster.call_and_wait_prepare_flashback(region.get_id(), 1));

    // read will be blocked
    must_get_error_flashback_in_progress(&mut cluster, &region, new_get_cf_cmd("write", b"k1"));

    // verify the read can be executed if add flashback flag in request's
    // header.
    must_cmd_add_flashback_flag(
        &mut cluster,
        &mut region.clone(),
        new_get_cf_cmd("write", b"k1"),
    );

    cluster.call_finish_flashback(region.get_id(), 1);

    multi_do_cmd(&mut cluster, new_get_cf_cmd("write", b"k1"));
}

// LocalReader will attempt to renew the lease.
// However, when flashback is enabled, it will make the lease None and prevent
// renew lease.
#[test]
fn test_flahsback_for_local_read() {
    let mut cluster = new_node_cluster(0, 3);
    let election_timeout = configure_for_lease_read(&mut cluster, Some(50), None);

    // Avoid triggering the log compaction in this test case.
    cluster.cfg.raft_store.raft_log_gc_threshold = 100;

    let node_id = 3u64;
    let store_id = 3u64;
    let peer = new_peer(store_id, node_id);
    cluster.run();

    cluster.must_put(b"k1", b"v1");
    let region = cluster.get_region(b"k1");
    cluster.must_transfer_leader(region.get_id(), peer.clone());

    // check local read before prepare flashback
    let state = cluster.raft_local_state(region.get_id(), store_id);
    let last_index = state.get_last_index();
    // Make sure the leader transfer procedure timeouts.
    std::thread::sleep(election_timeout * 2);
    must_read_on_peer(&mut cluster, peer.clone(), region.clone(), b"k1", b"v1");
    // Check the leader does a local read.
    let state = cluster.raft_local_state(region.get_id(), store_id);
    assert_eq!(state.get_last_index(), last_index);

    // prepare for flashback
    block_on(cluster.call_and_wait_prepare_flashback(region.get_id(), store_id));

    must_error_read_on_peer(
        &mut cluster,
        peer.clone(),
        region.clone(),
        b"k1",
        Duration::from_secs(1),
    );

    // Wait for the leader's lease to expire to ensure that a renew lease interval
    // has elapsed.
    std::thread::sleep(election_timeout * 2);
    must_error_read_on_peer(
        &mut cluster,
        peer.clone(),
        region.clone(),
        b"k1",
        Duration::from_secs(1),
    );

    // Also check read by propose was blocked
    let state = cluster.raft_local_state(region.get_id(), store_id);
    assert_eq!(state.get_last_index(), last_index);

    cluster.call_finish_flashback(region.get_id(), store_id);

    // check local read after finish flashback
    let state = cluster.raft_local_state(region.get_id(), store_id);
    let last_index = state.get_last_index();
    // Make sure the leader transfer procedure timeouts.
    std::thread::sleep(election_timeout * 2);
    must_read_on_peer(&mut cluster, peer, region.clone(), b"k1", b"v1");

    // Check the leader does a local read.
    let state = cluster.raft_local_state(region.get_id(), store_id);
    assert_eq!(state.get_last_index(), last_index);
}

#[test]
fn test_flahsback_for_status_cmd_as_region_detail() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.run();

    let region = cluster.get_region(b"k1");
    block_on(cluster.call_and_wait_prepare_flashback(region.get_id(), 1));

    let leader = cluster.leader_of_region(1).unwrap();
    let region_detail = cluster.region_detail(1, 1);
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

fn multi_do_cmd<T: Simulator>(cluster: &mut Cluster<T>, cmd: kvproto::raft_cmdpb::Request) {
    for _ in 0..100 {
        let mut reqs = vec![];
        for _ in 0..100 {
            reqs.push(cmd.clone());
        }
        cluster.batch_put(b"k1", reqs).unwrap();
    }
}

fn must_cmd_add_flashback_flag<T: Simulator>(
    cluster: &mut Cluster<T>,
    region: &mut metapb::Region,
    cmd: kvproto::raft_cmdpb::Request,
) {
    // verify the read can be executed if add flashback flag in request's
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
    let resp = cluster.call_command(req, Duration::from_secs(5)).unwrap();
    assert!(!resp.get_header().has_error());
}

fn must_get_error_flashback_in_progress<T: Simulator>(
    cluster: &mut Cluster<T>,
    region: &metapb::Region,
    cmd: kvproto::raft_cmdpb::Request,
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
                    &kvproto::errorpb::FlashbackInProgress {
                        region_id: region.get_id(),
                        ..Default::default()
                    }
                );
            }
        }
    }
}
