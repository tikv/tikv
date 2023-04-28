// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use crate::utils::v1::*;
use new_mock_engine_store::{copy_data_from, copy_meta_from};

#[test]
fn test_add_delayed_started_learner_no_snapshot() {
    // fail::cfg("before_tiflash_check_double_write", "return").unwrap();
    // fail::cfg("before_tiflash_do_write", "return").unwrap();
    let (mut cluster, pd_client) = new_later_add_learner_cluster(
        |c: &mut Cluster<NodeCluster>| {
            c.must_put(b"k1", b"v1");
            check_key(c, b"k1", b"v1", Some(true), None, Some(vec![1]));
        },
        vec![4],
    );

    // Start Leader store 4.
    cluster
        .start_with(HashSet::from_iter(
            vec![0, 1, 2, 4].into_iter().map(|x| x as usize),
        ))
        .unwrap();

    must_put_and_check_key_with_generator(
        &mut cluster,
        |i: u64| (format!("k{}", i), (0..1024).map(|_| "X").collect()),
        10,
        20,
        Some(true),
        None,
        Some(vec![1, 2, 3, 4]),
    );
    cluster.must_transfer_leader(1, new_peer(1, 1));

    // Force a compact log, so the leader have to send snapshot if peer 5 not catch
    // up.
    {
        assert!(force_compact_log(&mut cluster, b"k1", None) > 15);
    }

    // Simulate 4 is lost, recover its data to node 5.
    cluster.stop_node(4);

    later_bootstrap_learner_peer(&mut cluster, vec![5], 1);
    // After that, we manually compose data, to avoid snapshot sending.
    recover_from_peer(&cluster, 4, 5, 1);

    cluster.must_put(b"m1", b"v1");
    // Add node 5 to cluster.
    pd_client.must_add_peer(1, new_learner_peer(5, 5));

    fail::cfg("apply_on_handle_snapshot_finish_1_1", "panic").unwrap();
    // Start store 5.
    cluster
        .start_with(HashSet::from_iter(
            vec![0, 1, 2, 3].into_iter().map(|x| x as usize),
        ))
        .unwrap();

    cluster.must_put(b"z1", b"v1");
    check_key(
        &cluster,
        b"z1",
        b"v1",
        Some(true),
        None,
        Some(vec![1, 2, 3, 5]),
    );

    // Check if every node has the correct configuation.
    let new_states = maybe_collect_states(&cluster.cluster_ext, 1, Some(vec![1, 2, 3, 5]));
    assert_eq!(new_states.len(), 4);
    for i in new_states.keys() {
        assert_eq!(
            new_states
                .get(i)
                .unwrap()
                .in_disk_region_state
                .get_region()
                .get_peers()
                .len(),
            1 + 2 /* AddPeer */ + 2 // Learner
        );
    }

    fail::remove("apply_on_handle_snapshot_finish_1_1");
    fail::remove("on_pre_persist_with_finish");
    cluster.shutdown();
    // fail::remove("before_tiflash_check_double_write");
    // fail::remove("before_tiflash_do_write");
}
