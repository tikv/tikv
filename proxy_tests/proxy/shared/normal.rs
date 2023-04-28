// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use crate::utils::v1::*;

mod restart {
    use super::*;

    #[test]
    fn test_snap_append_restart() {
        let (mut cluster, pd_client) = new_mock_cluster(0, 3);

        disable_auto_gen_compact_log(&mut cluster);
        cluster.cfg.raft_store.max_snapshot_file_raw_size = ReadableSize(u64::MAX);

        // Disable default max peer count check.
        pd_client.disable_default_operator();
        let r1 = cluster.run_conf_change();

        let first_value = vec![0; 10240];
        for i in 0..10 {
            let key = format!("{:03}", i);
            cluster.must_put(key.as_bytes(), &first_value);
        }
        let first_key: &[u8] = b"000";

        let eng_ids = cluster
            .engines
            .iter()
            .map(|e| e.0.to_owned())
            .collect::<Vec<_>>();

        let engine_2 = cluster.get_engine(eng_ids[1]);
        pd_client.must_add_peer(r1, new_peer(eng_ids[1], eng_ids[1]));
        must_get_equal(engine_2, first_key, first_value.as_slice());

        fail::cfg("apply_pending_snapshot", "return").unwrap();
        tikv_util::info!("engine_3 is {}", eng_ids[2]);
        let engine_3 = cluster.get_engine(eng_ids[2]);
        must_get_none(engine_3, first_key);
        pd_client.must_add_peer(r1, new_peer(eng_ids[2], eng_ids[2]));

        std::thread::sleep(std::time::Duration::from_millis(1000));
        {
            let (key, value) = (b"k2", b"v2");
            cluster.must_put(key, value);
            check_key(
                &cluster,
                key,
                value,
                Some(true),
                None,
                Some(vec![eng_ids[0], eng_ids[1]]),
            );
            let new_states = maybe_collect_states(&cluster.cluster_ext, r1, None);
            // engine_3 has not applied snapshot.
            assert!(new_states.get(&eng_ids[2]).is_none());
            // engine_2 has applied snapshot.
            assert_eq!(
                new_states
                    .get(&eng_ids[1])
                    .unwrap()
                    .in_disk_region_state
                    .get_state(),
                PeerState::Normal
            );
        }

        stop_tiflash_node(&mut cluster, eng_ids[2]);
        restart_tiflash_node(&mut cluster, eng_ids[2]);

        fail::remove("apply_pending_snapshot");

        check_key(
            &cluster,
            first_key,
            &first_value,
            Some(true),
            None,
            Some(vec![eng_ids[2]]),
        );

        cluster.shutdown();
    }

    /// This test is currently not valid, since we can't abort in apply_snap by
    /// failpoint now.
    // #[test]
    fn test_snap_restart() {
        let (mut cluster, pd_client) = new_mock_cluster(0, 3);

        fail::cfg("on_can_apply_snapshot", "return(true)").unwrap();
        disable_auto_gen_compact_log(&mut cluster);
        cluster.cfg.raft_store.max_snapshot_file_raw_size = ReadableSize(u64::MAX);

        // Disable default max peer count check.
        pd_client.disable_default_operator();
        let r1 = cluster.run_conf_change();

        let first_value = vec![0; 10240];
        for i in 0..10 {
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
        // engine 2 will not exec post apply snapshot.
        fail::cfg("on_ob_pre_handle_snapshot", "return").unwrap();
        fail::cfg("on_ob_post_apply_snapshot", "return").unwrap();

        let engine_2 = cluster.get_engine(eng_ids[1]);
        must_get_none(engine_2, first_key);
        // add peer (engine_2,engine_2) to region 1.
        pd_client.must_add_peer(r1, new_peer(eng_ids[1], eng_ids[1]));

        check_key(&cluster, first_key, &first_value, Some(false), None, None);

        // If we wait here any longer, the snapshot can be applied...
        // So we have to disable this test.
        // std::thread::sleep(std::time::Duration::from_millis(2500));

        stop_tiflash_node(&mut cluster, eng_ids[1]);
        fail::remove("on_ob_pre_handle_snapshot");
        restart_tiflash_node(&mut cluster, eng_ids[1]);

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
        // now snapshot must be applied on peer engine_2
        check_key(
            &cluster,
            first_key,
            first_value.as_slice(),
            Some(true),
            None,
            Some(vec![eng_ids[1]]),
        );

        cluster.shutdown();
    }

    #[test]
    fn test_kv_restart() {
        // Test if a empty command can be observed when leadership changes.
        let (mut cluster, _pd_client) = new_mock_cluster(0, 3);

        // Disable AUTO generated compact log.
        disable_auto_gen_compact_log(&mut cluster);

        // We don't handle CompactLog at all.
        fail::cfg("try_flush_data", "return(0)").unwrap();
        let _ = cluster.run();

        cluster.must_put(b"k", b"v");
        let region = cluster.get_region(b"k");
        let region_id = region.get_id();
        for i in 0..10 {
            let k = format!("k{}", i);
            let v = format!("v{}", i);
            cluster.must_put(k.as_bytes(), v.as_bytes());
        }
        let prev_state = collect_all_states(&cluster.cluster_ext, region_id);
        let (compact_index, compact_term) = get_valid_compact_index(&prev_state);
        let compact_log = test_raftstore::new_compact_log_request(compact_index, compact_term);
        let req =
            test_raftstore::new_admin_request(region_id, region.get_region_epoch(), compact_log);
        fail::cfg("try_flush_data", "return(1)").unwrap();
        let _ = cluster
            .call_command_on_leader(req, Duration::from_secs(3))
            .unwrap();

        let eng_ids = cluster
            .engines
            .iter()
            .map(|e| e.0.to_owned())
            .collect::<Vec<_>>();

        for i in 0..10 {
            let k = format!("k{}", i);
            let v = format!("v{}", i);
            // Whatever already persisted or not, we won't loss data.
            check_key(
                &cluster,
                k.as_bytes(),
                v.as_bytes(),
                Some(true),
                Some(true),
                Some(vec![eng_ids[0]]),
            );
        }

        for i in 10..20 {
            let k = format!("k{}", i);
            let v = format!("v{}", i);
            cluster.must_put(k.as_bytes(), v.as_bytes());
        }

        for i in 10..20 {
            let k = format!("k{}", i);
            let v = format!("v{}", i);
            // Whatever already persisted or not, we won't loss data.
            check_key(
                &cluster,
                k.as_bytes(),
                v.as_bytes(),
                Some(true),
                Some(false),
                Some(vec![eng_ids[0]]),
            );
        }

        stop_tiflash_node(&mut cluster, eng_ids[0]);
        restart_tiflash_node(&mut cluster, eng_ids[0]);

        std::thread::sleep(std::time::Duration::from_millis(2000));

        for i in 0..20 {
            let k = format!("k{}", i);
            let v = format!("v{}", i);
            // Whatever already persisted or not, we won't loss data.
            check_key(
                &cluster,
                k.as_bytes(),
                v.as_bytes(),
                Some(true),
                None,
                Some(vec![eng_ids[0]]),
            );
        }

        fail::remove("try_flush_data");
        cluster.shutdown();
    }
}

mod persist {
    use super::*;

    #[test]
    fn test_persist_when_finish() {
        let (mut cluster, _pd_client) = new_mock_cluster(0, 3);
        disable_auto_gen_compact_log(&mut cluster);

        cluster.run();
        cluster.must_put(b"k0", b"v0");
        check_key(&cluster, b"k0", b"v0", Some(true), Some(false), None);
        let region_id = cluster.get_region(b"k0").get_id();

        let prev_states = collect_all_states(&cluster.cluster_ext, region_id);
        cluster.must_put(b"k1", b"v1");
        check_key(&cluster, b"k1", b"v1", Some(true), Some(false), None);
        let new_states = collect_all_states(&cluster.cluster_ext, region_id);
        must_altered_memory_apply_index(&prev_states, &new_states, 1);
        must_altered_disk_apply_index(&prev_states, &new_states, 0);

        fail::cfg("on_pre_write_apply_state", "return").unwrap();
        cluster.must_put(b"k2", b"v2");
        // Because we flush when batch ends.
        check_key(&cluster, b"k2", b"v2", Some(true), Some(false), None);

        // TODO(tiflash) wait `write_apply_state` in raftstore.
        std::thread::sleep(std::time::Duration::from_millis(1000));
        let prev_states = collect_all_states(&cluster.cluster_ext, region_id);
        cluster.must_put(b"k3", b"v3");
        // Because we flush when batch ends.
        check_key(&cluster, b"k3", b"v3", Some(true), Some(false), None);

        // TODO(tiflash) wait `write_apply_state` in raftstore.
        std::thread::sleep(std::time::Duration::from_millis(1000));
        let new_states = collect_all_states(&cluster.cluster_ext, region_id);
        must_apply_index_advanced_diff(&prev_states, &new_states, 0);
        fail::remove("on_pre_write_apply_state");
    }

    #[test]
    fn test_persist_when_merge() {
        let (mut cluster, pd_client) = new_mock_cluster_snap(0, 3);
        assert_eq!(cluster.cfg.proxy_cfg.raft_store.snap_handle_pool_size, 2);

        // disable_auto_gen_compact_log(&mut cluster);
        cluster.cfg.raft_store.right_derive_when_split = false;

        cluster.run();

        cluster.must_put(b"k1", b"v1");
        cluster.must_put(b"k3", b"v3");

        check_key(&cluster, b"k1", b"v1", Some(true), None, None);
        check_key(&cluster, b"k3", b"v3", Some(true), None, None);

        let r1 = cluster.get_region(b"k1");
        cluster.must_split(&r1, b"k2");
        let r3 = cluster.get_region(b"k3");

        std::thread::sleep(std::time::Duration::from_millis(1000));
        let prev_states = collect_all_states(&cluster.cluster_ext, r3.get_id());

        info!("start merge"; "from" => r1.get_id(), "to" => r3.get_id());
        pd_client.must_merge(r1.get_id(), r3.get_id());

        // TODO(tiflash) wait `write_apply_state` in raftstore.
        std::thread::sleep(std::time::Duration::from_millis(1000));
        let r3_new = cluster.get_region(b"k3");
        assert_eq!(r3_new.get_id(), r3.get_id());
        let new_states = collect_all_states(&cluster.cluster_ext, r3_new.get_id());
        // index 6 empty command
        // index 7 CommitMerge
        must_altered_disk_apply_index(&prev_states, &new_states, 2);
    }
}
