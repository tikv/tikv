// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.
use std::iter::FromIterator;

use collections::HashSet;

use crate::utils::v1::*;

/// This test is very important.
/// It make sure we can add learner peer for a store which is not started
/// actually.
/// We don't start the absent learner peer in this test.
#[test]
fn test_add_absent_learner_peer_by_simple() {
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);
    disable_auto_gen_compact_log(&mut cluster);
    // Disable default max peer count check.
    pd_client.disable_default_operator();

    let _ = cluster.run();
    cluster.must_put(b"k1", b"v1");
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1]));

    pd_client.must_add_peer(1, new_learner_peer(4, 4));

    cluster.must_put(b"k3", b"v3");
    check_key(&cluster, b"k3", b"v3", Some(true), None, None);
    let new_states = collect_all_states(&cluster.cluster_ext, 1);
    assert_eq!(new_states.len(), 3);
    for i in new_states.keys() {
        assert_eq!(
            new_states
                .get(i)
                .unwrap()
                .in_disk_region_state
                .get_region()
                .get_peers()
                .len(),
            3 + 1 // Learner
        );
    }

    cluster.shutdown();
}

/// This test is very important.
/// It make sure we can add learner peer for a store which is not started
/// actually.
/// We don't start the absent learner peer in this test.
#[test]
fn test_add_absent_learner_peer_by_joint() {
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);
    disable_auto_gen_compact_log(&mut cluster);
    // Disable default max peer count check.
    pd_client.disable_default_operator();

    let _ = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1]));

    pd_client.must_joint_confchange(
        1,
        vec![
            (ConfChangeType::AddNode, new_peer(2, 2)),
            (ConfChangeType::AddNode, new_peer(3, 3)),
            (ConfChangeType::AddLearnerNode, new_learner_peer(4, 4)),
            (ConfChangeType::AddLearnerNode, new_learner_peer(5, 5)),
        ],
    );
    assert!(pd_client.is_in_joint(1));
    pd_client.must_leave_joint(1);

    cluster.must_put(b"k3", b"v3");
    check_key(&cluster, b"k3", b"v3", Some(true), None, None);
    let new_states = collect_all_states(&cluster.cluster_ext, 1);
    assert_eq!(new_states.len(), 3);
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

    cluster.shutdown();
}

use engine_traits::{Engines, KvEngine, RaftEngine};
use raftstore::store::{write_initial_apply_state, write_initial_raft_state};

pub fn prepare_bootstrap_cluster_with(
    engines: &Engines<impl KvEngine, impl RaftEngine>,
    region: &metapb::Region,
) -> raftstore::Result<()> {
    let mut state = RegionLocalState::default();
    state.set_region(region.clone());

    let mut wb = engines.kv.write_batch();
    // box_try!(wb.put_msg(keys::PREPARE_BOOTSTRAP_KEY, region));
    box_try!(wb.put_msg_cf(CF_RAFT, &keys::region_state_key(region.get_id()), &state));
    write_initial_apply_state(&mut wb, region.get_id())?;
    wb.write()?;
    engines.sync_kv()?;

    let mut raft_wb = engines.raft.log_batch(1024);
    write_initial_raft_state(&mut raft_wb, region.get_id())?;
    box_try!(engines.raft.consume(&mut raft_wb, true));
    Ok(())
}

fn new_later_add_learner_cluster<F: Fn(&mut Cluster<NodeCluster>)>(
    initer: F,
    learner: Vec<u64>,
) -> (Cluster<NodeCluster>, Arc<TestPdClient>) {
    let (mut cluster, pd_client) = new_mock_cluster(0, 5);
    // Make sure we persist before generate snapshot.
    fail::cfg("on_pre_write_apply_state", "return").unwrap();

    cluster.cfg.mock_cfg.proxy_compat = false;
    disable_auto_gen_compact_log(&mut cluster);
    // Disable default max peer count check.
    pd_client.disable_default_operator();

    let _ = cluster.run_conf_change_no_start();
    let _ = cluster.start_with(HashSet::from_iter(
        vec![3, 4].into_iter().map(|x| x as usize),
    ));
    initer(&mut cluster);

    let mut peers = vec![
        (ConfChangeType::AddNode, new_peer(2, 2)),
        (ConfChangeType::AddNode, new_peer(3, 3)),
    ];
    let mut learner_peers: Vec<(ConfChangeType, kvproto::metapb::Peer)> = learner
        .iter()
        .map(|i| (ConfChangeType::AddLearnerNode, new_learner_peer(*i, *i)))
        .collect();
    peers.append(&mut learner_peers);
    pd_client.must_joint_confchange(1, peers);
    assert!(pd_client.is_in_joint(1));
    pd_client.must_leave_joint(1);

    (cluster, pd_client)
}

fn later_bootstrap_learner_peer(
    cluster: &mut Cluster<NodeCluster>,
    peers: Vec<u64>,
    already_learner_count: usize,
) {
    // Check if the voters has correct learner peer.
    let new_states = maybe_collect_states(&cluster.cluster_ext, 1, Some(vec![1, 2, 3]));
    assert_eq!(new_states.len(), 3);
    for i in new_states.keys() {
        assert_eq!(
            new_states
                .get(i)
                .unwrap()
                .in_disk_region_state
                .get_region()
                .get_peers()
                .len(),
            1 + 2 /* AddPeer */ + already_learner_count // Learner
        );
    }

    let region = new_states
        .get(&1)
        .unwrap()
        .in_disk_region_state
        .get_region();
    // Explicitly bootstrap region.
    for id in peers {
        let engines = cluster.get_engines(id);
        assert!(prepare_bootstrap_cluster_with(engines, region).is_ok());
    }
}

/// We start the absent learner peer in this test.
/// We don't try to reuse data from other learner peer.
#[test]
fn test_add_delayed_started_learner_by_joint() {
    let (mut cluster, _pd_client) = new_later_add_learner_cluster(
        |c: &mut Cluster<NodeCluster>| {
            c.must_put(b"k1", b"v1");
            check_key(c, b"k1", b"v1", Some(true), None, Some(vec![1]));
        },
        vec![4, 5],
    );

    cluster.must_put(b"k2", b"v2");
    check_key(
        &cluster,
        b"k2",
        b"v2",
        Some(true),
        None,
        Some(vec![1, 2, 3]),
    );

    later_bootstrap_learner_peer(&mut cluster, vec![4, 5], 2);
    cluster
        .start_with(HashSet::from_iter(
            vec![0, 1, 2].into_iter().map(|x| x as usize),
        ))
        .unwrap();

    cluster.must_put(b"k4", b"v4");
    check_key(&cluster, b"k4", b"v4", Some(true), None, None);

    let new_states = maybe_collect_states(&cluster.cluster_ext, 1, None);
    assert_eq!(new_states.len(), 5);
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

    fail::remove("on_pre_write_apply_state");
    cluster.shutdown();
}

use mock_engine_store::{copy_data_from, copy_meta_from};

fn recover_from_peer(cluster: &Cluster<NodeCluster>, from: u64, to: u64, region_id: u64) {
    let mut maybe_source_region = None;
    iter_ffi_helpers(
        cluster,
        Some(vec![from]),
        &mut |_, ffi: &mut FFIHelperSet| {
            let server = &mut ffi.engine_store_server;
            maybe_source_region = server.kvstore.get(&region_id).cloned();
        },
    );
    let source_region = maybe_source_region.unwrap();
    let mut new_region_meta = source_region.region.clone();
    new_region_meta.mut_peers().push(new_learner_peer(to, to));

    // Copy all node `from`'s data to node `to`
    iter_ffi_helpers(
        cluster,
        Some(vec![to]),
        &mut |id: u64, ffi: &mut FFIHelperSet| {
            let server = &mut ffi.engine_store_server;
            assert!(server.kvstore.get(&region_id).is_none());

            let new_region = make_new_region(Some(source_region.region.clone()), Some(id));
            server
                .kvstore
                .insert(source_region.region.get_id(), Box::new(new_region));
            if let Some(region) = server.kvstore.get_mut(&region_id) {
                let source_engines = cluster.get_engines(from);
                let target_engines = cluster.get_engines(to);
                copy_data_from(
                    source_engines,
                    target_engines,
                    source_region.as_ref(),
                    region.as_mut(),
                )
                .unwrap();
                copy_meta_from(
                    source_engines,
                    target_engines,
                    source_region.as_ref(),
                    region.as_mut(),
                    new_region_meta.clone(),
                    true,
                    true,
                    true,
                )
                .unwrap();
            } else {
                panic!("error");
            }
        },
    );
    {
        let prev_states = maybe_collect_states(&cluster.cluster_ext, region_id, None);
        assert_eq!(
            prev_states.get(&from).unwrap().in_disk_apply_state,
            prev_states.get(&to).unwrap().in_disk_apply_state
        );
    }
}

/// We start the absent learner peer in this test.
/// We try to reuse data from other learner peer.
/// We don't use a snapshot to initialize a peer.
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
    fail::remove("on_pre_write_apply_state");
    cluster.shutdown();
    // fail::remove("before_tiflash_check_double_write");
    // fail::remove("before_tiflash_do_write");
}

/// We start the absent learner peer in this test.
/// We try to reuse data from other learner peer.
/// We use a snapshot to initialize a peer.
#[test]
fn test_add_delayed_started_learner_snapshot() {
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

    // Simulate 4 is lost, recover its data to node 5.
    cluster.stop_node(4);

    // Force a compact log, so the leader have to send snapshot if peer 5 not catch
    // up.
    {
        must_put_and_check_key(&mut cluster, 21, 25, Some(true), None, Some(vec![1, 2, 3]));
        let prev_states = maybe_collect_states(&cluster.cluster_ext, 1, Some(vec![4]));
        assert!(
            force_compact_log(&mut cluster, b"k1", Some(vec![1, 2, 3]))
                > prev_states
                    .get(&4)
                    .unwrap()
                    .in_disk_apply_state
                    .get_applied_index()
        );
    }

    later_bootstrap_learner_peer(&mut cluster, vec![5], 1);
    // After that, we manually compose data, to avoid snapshot sending.
    recover_from_peer(&cluster, 4, 5, 1);
    // Add node 5 to cluster.
    pd_client.must_add_peer(1, new_learner_peer(5, 5));

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

    iter_ffi_helpers(
        &cluster,
        Some(vec![5]),
        &mut |_: u64, ffi: &mut FFIHelperSet| {
            (*ffi.engine_store_server).mutate_region_states(1, |e: &mut RegionStats| {
                assert_eq!(e.pre_handle_count.load(Ordering::SeqCst), 1);
            });
        },
    );

    fail::remove("on_pre_write_apply_state");
    cluster.shutdown();
}
