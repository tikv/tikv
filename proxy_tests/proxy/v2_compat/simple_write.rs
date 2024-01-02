// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Mutex;

use test_raftstore_v2::Simulator as S2;

use super::utils::*;
use crate::utils::v1::*;

#[test]
fn test_write_simple() {
    let mut cluster_v2 = test_raftstore_v2::new_node_cluster(1, 2);
    let (mut cluster_v1, _) = new_mock_cluster(1, 2);

    cluster_v1
        .cfg
        .server
        .labels
        .insert(String::from("engine"), String::from("tiflash"));
    cluster_v1.cfg.tikv.raft_store.enable_v2_compatible_learner = true;
    cluster_v1.pd_client.disable_default_operator();
    cluster_v2.pd_client.disable_default_operator();
    let r11 = cluster_v1.run_conf_change();
    let r21 = cluster_v2.run_conf_change();

    cluster_v1.must_put(b"k0", b"v0");
    cluster_v2.must_put(b"k0", b"v0");
    cluster_v1
        .pd_client
        .must_add_peer(r11, new_learner_peer(2, 10));
    cluster_v2
        .pd_client
        .must_add_peer(r21, new_learner_peer(2, 10));

    check_key(&cluster_v1, b"k0", b"v0", Some(true), None, None);
    let trans1 = Mutex::new(cluster_v1.sim.read().unwrap().get_router(2).unwrap());
    let trans2 = Mutex::new(cluster_v2.sim.read().unwrap().get_router(1).unwrap());

    let factory1 = ForwardFactoryV1 {
        node_id: 1,
        chain_send: Arc::new(move |m| {
            info!("send to trans2"; "msg" => ?m);
            let _ = trans2.lock().unwrap().send_raft_message(Box::new(m));
        }),
        keep_msg: true,
    };
    cluster_v1.add_send_filter(factory1);
    let factory2 = ForwardFactoryV2 {
        node_id: 2,
        chain_send: Arc::new(move |m| {
            info!("send to trans1"; "msg" => ?m);
            let _ = trans1.lock().unwrap().send_raft_message(m);
        }),
        keep_msg: true,
    };
    cluster_v2.add_send_filter(factory2);

    let filter11 = Box::new(
        RegionPacketFilter::new(r11, 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend)
            .msg_type(MessageType::MsgAppendResponse)
            .msg_type(MessageType::MsgSnapshot)
            .msg_type(MessageType::MsgHeartbeat)
            .msg_type(MessageType::MsgHeartbeatResponse),
    );
    cluster_v1.add_recv_filter_on_node(2, filter11);

    cluster_v2.must_put(b"k1", b"v1");
    assert_eq!(
        cluster_v2.must_get(b"k1").unwrap(),
        "v1".as_bytes().to_vec()
    );
    std::thread::sleep(std::time::Duration::from_millis(3000));
    let v = cluster_v1
        .get_engine(2)
        .get_value(b"zk1")
        .unwrap()
        .unwrap()
        .to_vec();
    assert_eq!(v, "v1".as_bytes().to_vec());

    cluster_v2.must_split(&cluster_v2.get_region(b"k1"), b"k1");

    std::thread::sleep(std::time::Duration::from_millis(3000));

    cluster_v1.shutdown();
    cluster_v2.shutdown();
}
