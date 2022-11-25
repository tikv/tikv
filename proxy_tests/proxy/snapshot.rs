// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use crate::proxy::*;

// This is a panic while panic test, which we can not handle.
// This double panic is due to:
// 1. check_applying_snap after apply_snap.
// 2. Drop in PeerFsm which leads to check_applying_snap.
// #[test]
#[should_panic]
fn test_delete_snapshot_after_apply() {
    let (mut cluster, pd_client) = new_mock_cluster_snap(0, 3);
    assert_eq!(cluster.cfg.proxy_cfg.raft_store.snap_handle_pool_size, 2);

    fail::cfg("apply_pending_snapshot", "return").unwrap();
    disable_auto_gen_compact_log(&mut cluster);

    // Disable default max peer count check.
    pd_client.disable_default_operator();
    let r1 = cluster.run_conf_change();

    let first_value = vec![0; 10240];
    // at least 4m data
    for i in 0..400 {
        let key = format!("{:03}", i);
        cluster.must_put(key.as_bytes(), &first_value);
    }
    let first_key: &[u8] = b"000";

    let eng_ids = cluster
        .engines
        .iter()
        .map(|e| e.0.to_owned())
        .collect::<Vec<_>>();
    tikv_util::info!("engine_2 is {}", eng_ids[1]);
    let engine_2 = cluster.get_engine(eng_ids[1]);
    must_get_none(&engine_2, first_key);
    // add peer (engine_2,engine_2) to region 1.

    fail::cfg("on_ob_pre_handle_snapshot_delete", "return").unwrap();
    pd_client.must_add_peer(r1, new_peer(eng_ids[1], eng_ids[1]));

    std::thread::sleep(std::time::Duration::from_millis(1000));
    // Note there is no region 1 on engine_2.
    let new_states = maybe_collect_states(&cluster, r1, None);
    assert!(new_states.get(&eng_ids[1]).is_none());

    // assert_eq!(new_states.get(&eng_ids[1]).unwrap().in_disk_region_state.
    // get_state(), kvproto::raft_serverpb::PeerState::Applying);

    fail::remove("apply_pending_snapshot");
    {
        let (key, value) = (b"k2", b"v2");
        cluster.must_put(key, value);
        check_key(
            &cluster,
            key,
            value,
            Some(true),
            None,
            Some(vec![eng_ids[1]]),
        );
        let engine_2 = cluster.get_engine(eng_ids[1]);
        // now snapshot must be applied on peer engine_2
        must_get_equal(&engine_2, first_key, first_value.as_slice());
    }

    fail::remove("apply_pending_snapshot");
    fail::remove("on_ob_pre_handle_snapshot_delete");
    cluster.shutdown();
}

#[test]
fn test_huge_multi_snapshot() {
    test_huge_snapshot(true)
}

#[test]
fn test_huge_normal_snapshot() {
    test_huge_snapshot(false)
}

fn test_huge_snapshot(is_multi: bool) {
    let (mut cluster, pd_client) = new_mock_cluster_snap(0, 3);
    assert_eq!(cluster.cfg.proxy_cfg.raft_store.snap_handle_pool_size, 2);

    fail::cfg("on_can_apply_snapshot", "return(true)").unwrap();
    disable_auto_gen_compact_log(&mut cluster);
    cluster.cfg.raft_store.max_snapshot_file_raw_size = if is_multi {
        ReadableSize(1024 * 1024)
    } else {
        ReadableSize(u64::MAX)
    };

    // Disable default max peer count check.
    pd_client.disable_default_operator();
    let r1 = cluster.run_conf_change();

    let first_value = vec![0; 10240];
    // at least 4m data
    for i in 0..400 {
        let key = format!("{:03}", i);
        cluster.must_put(key.as_bytes(), &first_value);
    }
    let first_key: &[u8] = b"000";

    let eng_ids = cluster
        .engines
        .iter()
        .map(|e| e.0.to_owned())
        .collect::<Vec<_>>();
    tikv_util::info!("engine_2 is {}", eng_ids[1]);
    let engine_2 = cluster.get_engine(eng_ids[1]);
    must_get_none(&engine_2, first_key);
    // add peer (engine_2,engine_2) to region 1.
    pd_client.must_add_peer(r1, new_peer(eng_ids[1], eng_ids[1]));

    {
        let (key, value) = (b"k2", b"v2");
        cluster.must_put(key, value);
        // we can get in memory, since snapshot is pre handled, though it is not
        // persisted
        check_key(
            &cluster,
            key,
            value,
            Some(true),
            None,
            Some(vec![eng_ids[1]]),
        );
        let engine_2 = cluster.get_engine(eng_ids[1]);
        // now snapshot must be applied on peer engine_2
        must_get_equal(&engine_2, first_key, first_value.as_slice());

        // engine 3 will not exec post apply snapshot.
        fail::cfg("on_ob_post_apply_snapshot", "pause").unwrap();

        tikv_util::info!("engine_3 is {}", eng_ids[2]);
        let engine_3 = cluster.get_engine(eng_ids[2]);
        must_get_none(&engine_3, first_key);
        pd_client.must_add_peer(r1, new_peer(eng_ids[2], eng_ids[2]));

        std::thread::sleep(std::time::Duration::from_millis(500));
        // We have not apply pre handled snapshot,
        // we can't be sure if it exists in only get from memory too, since pre handle
        // snapshot is async.
        must_get_none(&engine_3, first_key);
        fail::remove("on_ob_post_apply_snapshot");

        std::thread::sleep(std::time::Duration::from_millis(500));
        tikv_util::info!("put to engine_3");
        let (key, value) = (b"k3", b"v3");
        cluster.must_put(key, value);
        tikv_util::info!("check engine_3");
        check_key(&cluster, key, value, Some(true), None, None);
    }

    fail::remove("on_can_apply_snapshot");

    cluster.shutdown();
}

#[test]
fn test_concurrent_snapshot() {
    let (mut cluster, pd_client) = new_mock_cluster_snap(0, 3);
    assert_eq!(cluster.cfg.proxy_cfg.raft_store.snap_handle_pool_size, 2);
    disable_auto_gen_compact_log(&mut cluster);

    // Disable default max peer count check.
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");
    pd_client.must_add_peer(r1, new_peer(2, 2));
    // Force peer 2 to be followers all the way.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(r1, 2)
            .msg_type(MessageType::MsgRequestVote)
            .direction(Direction::Send),
    ));
    cluster.must_transfer_leader(r1, new_peer(1, 1));
    cluster.must_put(b"k3", b"v3");
    // Pile up snapshots of overlapped region ranges and deliver them all at once.
    let (tx, rx) = mpsc::channel();
    cluster
        .sim
        .wl()
        .add_recv_filter(3, Box::new(CollectSnapshotFilter::new(tx)));
    pd_client.must_add_peer(r1, new_peer(3, 3));
    // Ensure the snapshot of range ("", "") is sent and piled in filter.
    if let Err(e) = rx.recv_timeout(Duration::from_secs(1)) {
        panic!("the snapshot is not sent before split, e: {:?}", e);
    }

    // Occasionally fails.
    // let region1 = cluster.get_region(b"k1");
    // // Split the region range and then there should be another snapshot for the
    // split ranges. cluster.must_split(&region, b"k2");
    // check_key(&cluster, b"k3", b"v3", None, Some(true), Some(vec![3]));
    //
    // // Ensure the regions work after split.
    // cluster.must_put(b"k11", b"v11");
    // check_key(&cluster, b"k11", b"v11", Some(true), None, Some(vec![3]));
    // cluster.must_put(b"k4", b"v4");
    // check_key(&cluster, b"k4", b"v4", Some(true), None, Some(vec![3]));

    cluster.shutdown();
}

fn new_split_region_cluster(count: u64) -> (Cluster<NodeCluster>, Arc<TestPdClient>) {
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);
    // Disable raft log gc in this test case.
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::secs(60);
    // Disable default max peer count check.
    pd_client.disable_default_operator();

    let _ = cluster.run_conf_change();
    for i in 0..count {
        let k = format!("k{:0>4}", 2 * i + 1);
        let v = format!("v{}", 2 * i + 1);
        cluster.must_put(k.as_bytes(), v.as_bytes());
    }

    // k1 in [ , ]  splited by k2 -> (, k2] [k2, )
    // k3 in [k2, ) splited by k4 -> [k2, k4) [k4, )
    for i in 0..count {
        let k = format!("k{:0>4}", 2 * i + 1);
        let region = cluster.get_region(k.as_bytes());
        let sp = format!("k{:0>4}", 2 * i + 2);
        cluster.must_split(&region, sp.as_bytes());
    }

    (cluster, pd_client)
}

#[test]
fn test_prehandle_fail() {
    let (mut cluster, pd_client) = new_mock_cluster_snap(0, 3);
    assert_eq!(cluster.cfg.proxy_cfg.raft_store.snap_handle_pool_size, 2);

    // Disable raft log gc in this test case.
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::secs(60);

    // Disable default max peer count check.
    pd_client.disable_default_operator();
    let r1 = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");

    let eng_ids = cluster
        .engines
        .iter()
        .map(|e| e.0.to_owned())
        .collect::<Vec<_>>();
    // If we fail to call pre-handle snapshot, we can still handle it when apply
    // snapshot.
    fail::cfg("before_actually_pre_handle", "return").unwrap();
    pd_client.must_add_peer(r1, new_peer(eng_ids[1], eng_ids[1]));
    check_key(
        &cluster,
        b"k1",
        b"v1",
        Some(true),
        Some(true),
        Some(vec![eng_ids[1]]),
    );
    fail::remove("before_actually_pre_handle");

    // If we failed in apply snapshot(not panic), even if per_handle_snapshot is not
    // called.
    fail::cfg("on_ob_pre_handle_snapshot", "return").unwrap();
    check_key(
        &cluster,
        b"k1",
        b"v1",
        Some(false),
        Some(false),
        Some(vec![eng_ids[2]]),
    );
    pd_client.must_add_peer(r1, new_peer(eng_ids[2], eng_ids[2]));
    check_key(
        &cluster,
        b"k1",
        b"v1",
        Some(true),
        Some(true),
        Some(vec![eng_ids[2]]),
    );
    fail::remove("on_ob_pre_handle_snapshot");

    cluster.shutdown();
}

#[test]
fn test_split_merge() {
    let (mut cluster, pd_client) = new_mock_cluster_snap(0, 3);
    assert_eq!(cluster.cfg.proxy_cfg.raft_store.snap_handle_pool_size, 2);

    // Can always apply snapshot immediately
    fail::cfg("on_can_apply_snapshot", "return(true)").unwrap();
    cluster.cfg.raft_store.right_derive_when_split = true;

    // May fail if cluster.start, since node 2 is not in region1.peers(),
    // and node 2 has not bootstrap region1,
    // because region1 is not bootstrap if we only call cluster.start()
    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    check_key(&cluster, b"k1", b"v1", Some(true), None, None);
    check_key(&cluster, b"k3", b"v3", Some(true), None, None);

    let r1 = cluster.get_region(b"k1");
    let r3 = cluster.get_region(b"k3");
    assert_eq!(r1.get_id(), r3.get_id());

    cluster.must_split(&r1, b"k2");
    let r1_new = cluster.get_region(b"k1");
    let r3_new = cluster.get_region(b"k3");

    assert_eq!(r1.get_id(), r3_new.get_id());

    iter_ffi_helpers(&cluster, None, &mut |id: u64, _, ffi: &mut FFIHelperSet| {
        let server = &ffi.engine_store_server;
        if !server.kvstore.contains_key(&r1_new.get_id()) {
            panic!("node {} has no region {}", id, r1_new.get_id())
        }
        if !server.kvstore.contains_key(&r3_new.get_id()) {
            panic!("node {} has no region {}", id, r3_new.get_id())
        }
        // Region meta must equal
        assert_eq!(server.kvstore.get(&r1_new.get_id()).unwrap().region, r1_new);
        assert_eq!(server.kvstore.get(&r3_new.get_id()).unwrap().region, r3_new);

        // Can get from disk
        check_key(&cluster, b"k1", b"v1", None, Some(true), None);
        check_key(&cluster, b"k3", b"v3", None, Some(true), None);
        // TODO Region in memory data must not contradict, but now we do not
        // delete data
    });

    pd_client.must_merge(r1_new.get_id(), r3_new.get_id());
    let _r1_new2 = cluster.get_region(b"k1");
    let r3_new2 = cluster.get_region(b"k3");

    iter_ffi_helpers(&cluster, None, &mut |id: u64, _, ffi: &mut FFIHelperSet| {
        let server = &ffi.engine_store_server;

        // The left region is removed
        if server.kvstore.contains_key(&r1_new.get_id()) {
            panic!("node {} should has no region {}", id, r1_new.get_id())
        }
        if !server.kvstore.contains_key(&r3_new.get_id()) {
            panic!("node {} has no region {}", id, r3_new.get_id())
        }
        // Region meta must equal
        assert_eq!(
            server.kvstore.get(&r3_new2.get_id()).unwrap().region,
            r3_new2
        );

        // Can get from disk
        check_key(&cluster, b"k1", b"v1", None, Some(true), None);
        check_key(&cluster, b"k3", b"v3", None, Some(true), None);
        // TODO Region in memory data must not contradict, but now we do not delete data

        let origin_epoch = r3_new.get_region_epoch();
        let new_epoch = r3_new2.get_region_epoch();
        // PrepareMerge + CommitMerge, so it should be 2.
        assert_eq!(new_epoch.get_version(), origin_epoch.get_version() + 2);
        assert_eq!(new_epoch.get_conf_ver(), origin_epoch.get_conf_ver());
    });

    fail::remove("on_can_apply_snapshot");
    cluster.shutdown();
}

#[test]
fn test_basic_concurrent_snapshot() {
    let (mut cluster, pd_client) = new_mock_cluster_snap(0, 3);
    assert_eq!(cluster.cfg.proxy_cfg.raft_store.snap_handle_pool_size, 2);

    disable_auto_gen_compact_log(&mut cluster);

    // Disable default max peer count check.
    pd_client.disable_default_operator();

    let _ = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region1 = cluster.get_region(b"k1");
    cluster.must_split(&region1, b"k2");
    let r1 = cluster.get_region(b"k1").get_id();
    let r3 = cluster.get_region(b"k3").get_id();

    fail::cfg("before_actually_pre_handle", "sleep(1000)").unwrap();
    tikv_util::info!("region k1 {} k3 {}", r1, r3);
    let pending_count = cluster
        .engines
        .get(&2)
        .unwrap()
        .kv
        .pending_applies_count
        .clone();
    pd_client.add_peer(r1, new_peer(2, 2));
    pd_client.add_peer(r3, new_peer(2, 2));
    // handle_pending_applies will do nothing.
    fail::cfg("apply_pending_snapshot", "return").unwrap();
    // wait snapshot is generated.
    std::thread::sleep(std::time::Duration::from_millis(500));
    // Now, region k1 and k3 are not handled, since pre-handle process is not
    // finished. This is because `pending_applies_count` is not greater than
    // `snap_handle_pool_size`, So there are no `handle_pending_applies`
    // until `on_timeout`.

    fail::remove("apply_pending_snapshot");
    assert_eq!(pending_count.load(Ordering::SeqCst), 2);
    std::thread::sleep(std::time::Duration::from_millis(600));
    check_key(&cluster, b"k1", b"v1", None, Some(true), Some(vec![1, 2]));
    check_key(&cluster, b"k3", b"v3", None, Some(true), Some(vec![1, 2]));
    // Now, k1 and k3 are handled.
    assert_eq!(pending_count.load(Ordering::SeqCst), 0);

    fail::remove("before_actually_pre_handle");

    cluster.shutdown();
}

#[test]
fn test_many_concurrent_snapshot() {
    let c = 4;
    let (mut cluster, pd_client) = new_split_region_cluster(c);

    for i in 0..c {
        let k = format!("k{:0>4}", 2 * i + 1);
        let region_id = cluster.get_region(k.as_bytes()).get_id();
        pd_client.must_add_peer(region_id, new_peer(2, 2));
    }

    for i in 0..c {
        let k = format!("k{:0>4}", 2 * i + 1);
        let v = format!("v{}", 2 * i + 1);
        check_key(
            &cluster,
            k.as_bytes(),
            v.as_bytes(),
            Some(true),
            Some(true),
            Some(vec![2]),
        );
    }

    cluster.shutdown();
}
