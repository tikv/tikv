// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use engine_rocks::RocksEngine;
use kvproto::metapb::Peer;
use kvproto::raft_serverpb::RegionLocalState;
use raft::eraftpb::ConfChangeType;
use test_raftstore::*;
use tikv::config::ConfigController;
use tikv::server::debug::{get_region_state, Debugger};

fn get_debugger<T: Simulator>(cluster: &mut Cluster<T>, store_id: u64) -> Debugger<RocksEngine> {
    let engines = cluster.engines.get(&store_id).unwrap().clone();
    let cfg_ctl = ConfigController::new(cluster.cfg.clone());
    Debugger::new(engines, cfg_ctl)
}

fn peers(state: &RegionLocalState) -> &[Peer] {
    state.get_region().get_peers()
}

fn conf_ver(state: &RegionLocalState) -> u64 {
    state.get_region().get_region_epoch().conf_ver
}

#[test]
fn test_unsafe_recover() {
    type Action = Box<dyn Fn(&TestPdClient, u64)>;
    let test_actions: Vec<(Action, Action, Action)> = vec![
        (
            Box::new(|pd_client: &TestPdClient, region_id: u64| {
                pd_client.must_add_peer(region_id, new_learner_peer(4, 4));
            }),
            Box::new(|pd_client: &TestPdClient, region_id: u64| {
                pd_client.must_add_peer(region_id, new_peer(4, 4));
            }),
            Box::new(|pd_client: &TestPdClient, region_id: u64| {
                pd_client.must_remove_peer(region_id, new_peer(4, 4));
            }),
        ),
        (
            Box::new(|pd_client: &TestPdClient, region_id: u64| {
                pd_client.must_joint_confchange(
                    region_id,
                    vec![(ConfChangeType::AddLearnerNode, new_learner_peer(4, 4))],
                );
                pd_client.must_leave_joint(region_id);
            }),
            Box::new(|pd_client: &TestPdClient, region_id: u64| {
                pd_client.must_joint_confchange(
                    region_id,
                    vec![(ConfChangeType::AddNode, new_peer(4, 4))],
                );
                pd_client.must_leave_joint(region_id);
            }),
            Box::new(|pd_client: &TestPdClient, region_id: u64| {
                pd_client.must_joint_confchange(
                    region_id,
                    vec![(ConfChangeType::RemoveNode, new_peer(4, 4))],
                );
                pd_client.must_leave_joint(region_id);
            }),
        ),
    ];

    for (add_learner, add_peer, remove_peer) in test_actions {
        let mut cluster = new_server_cluster(0, 4);
        let pd_client = cluster.pd_client.clone();
        pd_client.disable_default_operator();

        let region_id = cluster.run_conf_change();
        cluster.must_transfer_leader(region_id, new_peer(1, 1));

        cluster.must_put(b"key1", b"value1");
        for i in 2..=3 {
            pd_client.must_add_peer(region_id, new_peer(i, i));
            must_get_equal(&cluster.get_engine(i), b"key1", b"value1");
        }

        get_debugger(&mut cluster, 3)
            .remove_failed_stores(vec![4], None)
            .unwrap();

        // Test add learner removed by unsafe recover.
        add_learner(&pd_client, region_id);
        cluster.must_put(b"key2", b"value2");
        must_get_equal(&cluster.get_engine(3), b"key2", b"value2");

        let state1 = get_region_state(&cluster.get_engine(1), region_id);
        let state3 = get_region_state(&cluster.get_engine(3), region_id);
        assert_eq!(peers(&state1).len(), 4);
        assert_eq!(peers(&state3).len(), 3);
        assert_eq!(conf_ver(&state1), conf_ver(&state3));

        // Test add peer removed by unsafe recover.
        add_peer(&pd_client, region_id);
        cluster.must_put(b"key3", b"value3");
        must_get_equal(&cluster.get_engine(3), b"key3", b"value3");

        let state1 = get_region_state(&cluster.get_engine(1), region_id);
        let state3 = get_region_state(&cluster.get_engine(3), region_id);
        assert_eq!(peers(&state1).len(), 4);
        assert_eq!(peers(&state3).len(), 3);
        assert_eq!(conf_ver(&state1), conf_ver(&state3));

        // Test remove peer removed by unsafe recover.
        remove_peer(&pd_client, region_id);
        cluster.must_put(b"key4", b"value4");
        must_get_equal(&cluster.get_engine(3), b"key4", b"value4");

        let state1 = get_region_state(&cluster.get_engine(1), region_id);
        let state3 = get_region_state(&cluster.get_engine(3), region_id);
        assert_eq!(peers(&state1).len(), 3);
        assert_eq!(peers(&state3).len(), 3);
        assert_eq!(conf_ver(&state1), conf_ver(&state3));
    }
}
