// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{assert_matches::assert_matches, time::Duration};

use engine_traits::{Peekable, CF_DEFAULT};
use futures::executor::block_on;
use kvproto::{
    metapb,
    raft_cmdpb::{AdminCmdType, TransferLeaderRequest},
};
use raft::prelude::ConfChangeType;
use raftstore_v2::{
    router::{PeerMsg, PeerTick},
    SimpleWriteEncoder,
};
use tikv_util::store::new_peer;

use crate::cluster::Cluster;

fn put_data(
    region_id: u64,
    cluster: &mut Cluster,
    node_off: usize,
    node_off_for_verify: usize,
    key: &[u8],
) {
    let mut router = &mut cluster.routers[node_off];

    router.wait_applied_to_current_term(region_id, Duration::from_secs(3));

    // router.wait_applied_to_current_term(2, Duration::from_secs(3));
    let snap = router.stale_snapshot(region_id);
    assert_matches!(snap.get_value(key), Ok(None));

    let header = Box::new(router.new_request_for(region_id).take_header());
    let mut put = SimpleWriteEncoder::with_capacity(64);
    put.put(CF_DEFAULT, key, b"value");
    let (msg, mut sub) = PeerMsg::simple_write(header, put.encode());
    router.send(region_id, msg).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));
    cluster.dispatch(region_id, vec![]);
    assert!(block_on(sub.wait_proposed()));

    std::thread::sleep(std::time::Duration::from_millis(10));
    cluster.dispatch(region_id, vec![]);
    // triage send snapshot
    std::thread::sleep(std::time::Duration::from_millis(100));
    cluster.dispatch(region_id, vec![]);
    assert!(block_on(sub.wait_committed()));

    let resp = block_on(sub.result()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    router = &mut cluster.routers[node_off];
    let snap = router.stale_snapshot(region_id);
    assert_eq!(snap.get_value(key).unwrap().unwrap(), b"value");

    // Because of skip bcast commit, the data should not be applied yet.
    router = &mut cluster.routers[node_off_for_verify];
    let snap = router.stale_snapshot(region_id);
    assert_matches!(snap.get_value(key), Ok(None));
    // Trigger heartbeat explicitly to commit on follower.
    router = &mut cluster.routers[node_off];
    for _ in 0..2 {
        router
            .send(region_id, PeerMsg::Tick(PeerTick::Raft))
            .unwrap();
        router
            .send(region_id, PeerMsg::Tick(PeerTick::Raft))
            .unwrap();
    }
    cluster.dispatch(region_id, vec![]);
    std::thread::sleep(std::time::Duration::from_millis(100));
    router = &mut cluster.routers[node_off_for_verify];
    let snap = router.stale_snapshot(region_id);
    assert_eq!(snap.get_value(key).unwrap().unwrap(), b"value");
}

pub fn must_transfer_leader(
    cluster: &Cluster,
    region_id: u64,
    from_off: usize,
    to_off: usize,
    to_peer: metapb::Peer,
) {
    let router = &cluster.routers[from_off];
    let router2 = &cluster.routers[to_off];
    let mut req = router.new_request_for(region_id);
    let mut transfer_req = TransferLeaderRequest::default();
    transfer_req.set_peer(to_peer.clone());
    let admin_req = req.mut_admin_request();
    admin_req.set_cmd_type(AdminCmdType::TransferLeader);
    admin_req.set_transfer_leader(transfer_req);
    let resp = router.admin_command(region_id, req).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    cluster.dispatch(region_id, vec![]);

    let meta = router
        .must_query_debug_info(region_id, Duration::from_secs(3))
        .unwrap();
    assert_eq!(meta.raft_status.soft_state.leader_id, to_peer.id);
    let meta = router2
        .must_query_debug_info(region_id, Duration::from_secs(3))
        .unwrap();
    assert_eq!(meta.raft_status.soft_state.leader_id, to_peer.id);
}

#[test]
fn test_transfer_leader() {
    let mut cluster = Cluster::with_node_count(3, None);
    let region_id = 2;
    let router0 = &cluster.routers[0];

    let mut req = router0.new_request_for(region_id);
    let admin_req = req.mut_admin_request();
    admin_req.set_cmd_type(AdminCmdType::ChangePeer);
    admin_req
        .mut_change_peer()
        .set_change_type(ConfChangeType::AddNode);
    let store_id = cluster.node(1).id();
    let peer1 = new_peer(store_id, 10);
    admin_req.mut_change_peer().set_peer(peer1.clone());
    let req_clone = req.clone();
    let resp = router0.admin_command(region_id, req_clone).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    let epoch = req.get_header().get_region_epoch();
    let new_conf_ver = epoch.get_conf_ver() + 1;
    let leader_peer = req.get_header().get_peer().clone();
    let meta = router0
        .must_query_debug_info(region_id, Duration::from_secs(3))
        .unwrap();
    assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);
    assert_eq!(meta.region_state.peers, vec![leader_peer, peer1.clone()]);
    let peer0_id = meta.raft_status.id;

    // So heartbeat will create a learner.
    cluster.dispatch(region_id, vec![]);
    let router1 = &cluster.routers[1];
    let meta = router1
        .must_query_debug_info(region_id, Duration::from_secs(3))
        .unwrap();
    assert_eq!(peer0_id, meta.raft_status.soft_state.leader_id);
    assert_eq!(meta.raft_status.id, peer1.id, "{:?}", meta);
    assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);
    cluster.dispatch(region_id, vec![]);

    // Ensure follower has latest entries before transfer leader.
    put_data(region_id, &mut cluster, 0, 1, b"key1");

    // Perform transfer leader
    must_transfer_leader(&cluster, region_id, 0, 1, peer1);

    // Before transfer back to peer0, put some data again.
    put_data(region_id, &mut cluster, 1, 0, b"key2");

    // Perform transfer leader
    let store_id = cluster.node(0).id();
    must_transfer_leader(&cluster, region_id, 1, 0, new_peer(store_id, peer0_id));
}
