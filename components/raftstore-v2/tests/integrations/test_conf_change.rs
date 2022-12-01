// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{self, time::Duration};

use engine_traits::{OpenOptions, Peekable, TabletFactory};
use kvproto::raft_cmdpb::{AdminCmdType, CmdType, Request};
use raft::prelude::ConfChangeType;
use raftstore_v2::router::{PeerMsg, PeerTick};
use tikv_util::store::new_learner_peer;

use crate::cluster::Cluster;

#[test]
fn test_simple_change() {
    let cluster = Cluster::with_node_count(2, None);
    let region_id = 2;
    let router0 = cluster.router(0);
    let mut req = router0.new_request_for(2);
    let admin_req = req.mut_admin_request();
    admin_req.set_cmd_type(AdminCmdType::ChangePeer);
    admin_req
        .mut_change_peer()
        .set_change_type(ConfChangeType::AddLearnerNode);
    let store_id = cluster.node(1).id();
    let new_peer = new_learner_peer(store_id, 10);
    admin_req.mut_change_peer().set_peer(new_peer.clone());
    let resp = router0.command(2, req.clone()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    let epoch = req.get_header().get_region_epoch();
    let new_conf_ver = epoch.get_conf_ver() + 1;
    let leader_peer = req.get_header().get_peer().clone();
    let meta = router0
        .must_query_debug_info(2, Duration::from_secs(3))
        .unwrap();
    let match_index = meta.raft_apply.applied_index;
    assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);
    assert_eq!(meta.region_state.peers, vec![leader_peer, new_peer]);

    // So heartbeat will create a learner.
    cluster.dispatch(2, vec![]);
    let router1 = cluster.router(1);
    let meta = router1
        .must_query_debug_info(2, Duration::from_secs(3))
        .unwrap();
    assert_eq!(meta.raft_status.id, 10, "{:?}", meta);
    assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);
    assert_eq!(
        meta.raft_status.soft_state.leader_id,
        req.get_header().get_peer().get_id()
    );
    // Trigger the raft tick to replica the log to the learner and execute the
    // snapshot task.
    router0
        .send(region_id, PeerMsg::Tick(PeerTick::Raft))
        .unwrap();
    cluster.dispatch(region_id, vec![]);

    // write one kv after snapshot
    let (key, val) = (b"key", b"value");
    let mut write_req = router0.new_request_for(region_id);
    let mut put_req = Request::default();
    put_req.set_cmd_type(CmdType::Put);
    put_req.mut_put().set_key(key.to_vec());
    put_req.mut_put().set_value(val.to_vec());
    write_req.mut_requests().push(put_req);
    let (msg, _) = PeerMsg::raft_command(write_req.clone());
    router0.send(region_id, msg).unwrap();
    std::thread::sleep(Duration::from_millis(1000));
    cluster.dispatch(region_id, vec![]);

    let meta = router1
        .must_query_debug_info(region_id, Duration::from_secs(3))
        .unwrap();
    // the learner truncated index muse be equal the leader applied index and can
    // read the new written kv.
    assert_eq!(match_index, meta.raft_apply.truncated_state.index);
    assert!(meta.raft_apply.applied_index >= match_index);
    let tablet_factory = cluster.node(1).tablet_factory();
    let tablet = tablet_factory
        .open_tablet(region_id, None, OpenOptions::default().set_cache_only(true))
        .unwrap();
    assert_eq!(tablet.get_value(key).unwrap().unwrap(), val);

    req.mut_header()
        .mut_region_epoch()
        .set_conf_ver(new_conf_ver);
    req.mut_admin_request()
        .mut_change_peer()
        .set_change_type(ConfChangeType::RemoveNode);
    let resp = router0.command(2, req.clone()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    let epoch = req.get_header().get_region_epoch();
    let new_conf_ver = epoch.get_conf_ver() + 1;
    let leader_peer = req.get_header().get_peer().clone();
    let meta = router0
        .must_query_debug_info(2, Duration::from_secs(3))
        .unwrap();
    assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);
    assert_eq!(meta.region_state.peers, vec![leader_peer]);
    // TODO: check if the peer is removed once life trace is implemented or
    // snapshot is implemented.
}
