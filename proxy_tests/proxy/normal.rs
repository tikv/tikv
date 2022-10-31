// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    io::Write,
    ops::DerefMut,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{atomic::Ordering, mpsc, Arc},
};

use clap::{App, Arg};
use engine_store_ffi::{KVGetStatus, RaftStoreProxyFFI};
use engine_traits::{MiscExt, CF_DEFAULT, CF_LOCK, CF_WRITE};
use kvproto::{
    import_sstpb::SstMeta,
    metapb::RegionEpoch,
    raft_cmdpb::{CmdType, Request},
};
use new_mock_engine_store::{
    config::Config,
    mock_cluster::FFIHelperSet,
    node::NodeCluster,
    transport_simulate::{
        CloneFilterFactory, CollectSnapshotFilter, Direction, RegionPacketFilter,
    },
    Cluster, ProxyConfig, Simulator, TestPdClient,
};
use pd_client::PdClient;
use proxy_server::{
    config::{
        address_proxy_config, ensure_no_common_unrecognized_keys, get_last_config,
        memory_limit_for_cf, setup_default_tikv_config, validate_and_persist_config,
        TIFLASH_DEFAULT_ADVERTISE_LISTENING_ADDR, TIFLASH_DEFAULT_LISTENING_ADDR,
        TIFLASH_DEFAULT_STATUS_ADDR,
    },
    proxy::gen_tikv_config,
};
use raft::eraftpb::MessageType;
use sst_importer::SstImporter;
use tikv::config::{TikvConfig, LAST_CONFIG_FILE};
use tikv_util::{
    config::{ReadableDuration, ReadableSize},
    store::find_peer,
    sys::SysQuota,
    time::Duration,
    HandyRwLock,
};

use crate::proxy::*;

mod store {
    use super::*;
    #[test]
    fn test_store_stats() {
        let (mut cluster, pd_client) = new_mock_cluster(0, 1);

        let _ = cluster.run();

        for id in cluster.engines.keys() {
            let engine = cluster.get_tiflash_engine(*id);
            assert_eq!(
                engine.ffi_hub.as_ref().unwrap().get_store_stats().capacity,
                444444
            );
        }

        for id in cluster.engines.keys() {
            cluster.must_send_store_heartbeat(*id);
        }
        std::thread::sleep(std::time::Duration::from_millis(1000));
        // let resp = block_on(pd_client.store_heartbeat(Default::default(), None,
        // None)).unwrap();
        for id in cluster.engines.keys() {
            let store_stat = pd_client.get_store_stats(*id).unwrap();
            assert_eq!(store_stat.get_capacity(), 444444);
            assert_eq!(store_stat.get_available(), 333333);
        }
        // The same to mock-engine-store
        cluster.shutdown();
    }
}

mod region {
    use super::*;

    #[test]
    fn test_handle_destroy() {
        let (mut cluster, pd_client) = new_mock_cluster(0, 3);

        disable_auto_gen_compact_log(&mut cluster);

        // Disable default max peer count check.
        pd_client.disable_default_operator();

        cluster.run();
        cluster.must_put(b"k1", b"v1");
        let eng_ids = cluster
            .engines
            .iter()
            .map(|e| e.0.to_owned())
            .collect::<Vec<_>>();

        let region = cluster.get_region(b"k1");
        let region_id = region.get_id();
        let peer_1 = find_peer(&region, eng_ids[0]).cloned().unwrap();
        let peer_2 = find_peer(&region, eng_ids[1]).cloned().unwrap();
        cluster.must_transfer_leader(region_id, peer_1);

        iter_ffi_helpers(
            &cluster,
            Some(vec![eng_ids[1]]),
            &mut |_, _, ffi: &mut FFIHelperSet| {
                let server = &ffi.engine_store_server;
                assert!(server.kvstore.contains_key(&region_id));
            },
        );

        pd_client.must_remove_peer(region_id, peer_2);

        check_key(
            &cluster,
            b"k1",
            b"v2",
            Some(false),
            None,
            Some(vec![eng_ids[1]]),
        );

        std::thread::sleep(std::time::Duration::from_millis(100));
        // Region removed in server.
        iter_ffi_helpers(
            &cluster,
            Some(vec![eng_ids[1]]),
            &mut |_, _, ffi: &mut FFIHelperSet| {
                let server = &ffi.engine_store_server;
                assert!(!server.kvstore.contains_key(&region_id));
            },
        );

        cluster.shutdown();
    }

    #[test]
    fn test_get_region_local_state() {
        let (mut cluster, _pd_client) = new_mock_cluster(0, 3);

        cluster.run();

        let k = b"k1";
        let v = b"v1";
        cluster.must_put(k, v);
        check_key(&cluster, k, v, Some(true), None, None);
        let region_id = cluster.get_region(k).get_id();

        // Get RegionLocalState through ffi
        unsafe {
            iter_ffi_helpers(
                &cluster,
                None,
                &mut |_id: u64, _, ffi_set: &mut FFIHelperSet| {
                    let f = ffi_set.proxy_helper.fn_get_region_local_state.unwrap();
                    let mut state = kvproto::raft_serverpb::RegionLocalState::default();
                    let mut error_msg = new_mock_engine_store::RawCppStringPtrGuard::default();

                    assert_eq!(
                        f(
                            ffi_set.proxy_helper.proxy_ptr,
                            region_id,
                            &mut state as *mut _ as _,
                            error_msg.as_mut(),
                        ),
                        KVGetStatus::Ok
                    );
                    assert!(state.has_region());
                    assert_eq!(state.get_state(), kvproto::raft_serverpb::PeerState::Normal);
                    assert!(error_msg.as_ref().is_null());

                    let mut state = kvproto::raft_serverpb::RegionLocalState::default();
                    assert_eq!(
                        f(
                            ffi_set.proxy_helper.proxy_ptr,
                            0, // not exist
                            &mut state as *mut _ as _,
                            error_msg.as_mut(),
                        ),
                        KVGetStatus::NotFound
                    );
                    assert!(!state.has_region());
                    assert!(error_msg.as_ref().is_null());

                    ffi_set
                        .proxy
                        .get_value_cf("none_cf", "123".as_bytes(), |value| {
                            let msg = value.unwrap_err();
                            assert_eq!(msg, "Storage Engine Status { code: IoError, sub_code: None, sev: NoError, state: \"cf none_cf not found\" }");
                        });
                    ffi_set
                        .proxy
                        .get_value_cf("raft", "123".as_bytes(), |value| {
                            let res = value.unwrap();
                            assert!(res.is_none());
                        });

                    // If we have no kv engine.
                    ffi_set.proxy.set_kv_engine(None);
                    let res = ffi_set.proxy_helper.fn_get_region_local_state.unwrap()(
                        ffi_set.proxy_helper.proxy_ptr,
                        region_id,
                        &mut state as *mut _ as _,
                        error_msg.as_mut(),
                    );
                    assert_eq!(res, KVGetStatus::Error);
                    assert!(!error_msg.as_ref().is_null());
                    assert_eq!(
                        error_msg.as_str(),
                        "KV engine is not initialized".as_bytes()
                    );
                },
            );
        }

        cluster.shutdown();
    }
}

mod config {
    use futures::io::Read;
    use proxy_server::{proxy::gen_proxy_config, setup::overwrite_config_with_cmd_args};
    use tikv::server::DEFAULT_LISTENING_ADDR;

    use super::*;

    #[test]
    fn test_config() {
        // Test double read.
        let mut file = tempfile::NamedTempFile::new().unwrap();
        let text = "memory-usage-high-water=0.65\n[server]\nengine-addr=\"1.2.3.4:5\"\n[raftstore]\nsnap-handle-pool-size=4\n[nosense]\nfoo=2\n[rocksdb]\nmax-open-files = 111\nz=1";
        write!(file, "{}", text).unwrap();
        let path = file.path();

        let mut unrecognized_keys = Vec::new();
        let mut config = TikvConfig::from_file(path, Some(&mut unrecognized_keys)).unwrap();
        // Otherwise we have no default addr for TiKv.
        setup_default_tikv_config(&mut config);
        assert_eq!(config.memory_usage_high_water, 0.65);
        assert_eq!(config.rocksdb.max_open_files, 111);
        assert_eq!(config.server.addr, TIFLASH_DEFAULT_LISTENING_ADDR);
        assert_eq!(unrecognized_keys.len(), 3);

        let mut proxy_unrecognized_keys = Vec::new();
        let proxy_config =
            ProxyConfig::from_file(path, Some(&mut proxy_unrecognized_keys)).unwrap();
        assert_eq!(proxy_config.raft_store.snap_handle_pool_size, 4);
        assert_eq!(proxy_config.server.engine_addr, "1.2.3.4:5");
        assert!(proxy_unrecognized_keys.contains(&"memory-usage-high-water".to_string()));
        assert!(proxy_unrecognized_keys.contains(&"nosense".to_string()));
        let v1 = vec!["a.b", "b"]
            .iter()
            .map(|e| String::from(*e))
            .collect::<Vec<String>>();
        let v2 = vec!["a.b", "b.b", "c"]
            .iter()
            .map(|e| String::from(*e))
            .collect::<Vec<String>>();
        let unknown = ensure_no_common_unrecognized_keys(&v1, &v2);
        assert_eq!(unknown.is_err(), true);
        assert_eq!(unknown.unwrap_err(), "a.b, b.b");
        let unknown =
            ensure_no_common_unrecognized_keys(&proxy_unrecognized_keys, &unrecognized_keys);
        assert_eq!(unknown.is_err(), true);
        assert_eq!(unknown.unwrap_err(), "nosense, rocksdb.z");

        // Common config can be persisted.
        // Need run this test with ENGINE_LABEL_VALUE=tiflash, otherwise will fatal
        // exit.
        let _ = std::fs::remove_file(
            PathBuf::from_str(&config.storage.data_dir)
                .unwrap()
                .join(LAST_CONFIG_FILE),
        );
        validate_and_persist_config(&mut config, true);

        // Will not override ProxyConfig
        let proxy_config_new = ProxyConfig::from_file(path, None).unwrap();
        assert_eq!(proxy_config_new.raft_store.snap_handle_pool_size, 4);
    }

    #[test]
    fn test_validate_config() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        let text = "[raftstore.aaa]\nbbb=2\n[server]\nengine-addr=\"1.2.3.4:5\"\n[raftstore]\nsnap-handle-pool-size=4\nclean-stale-ranges-tick=9999\n[nosense]\nfoo=2\n[rocksdb]\nmax-open-files = 111\nz=1";
        write!(file, "{}", text).unwrap();
        let path = file.path();
        let tmp_store_folder = tempfile::TempDir::new().unwrap();
        let tmp_last_config_path = tmp_store_folder.path().join(LAST_CONFIG_FILE);
        std::fs::copy(path, tmp_last_config_path.as_path()).unwrap();
        get_last_config(tmp_store_folder.path().to_str().unwrap());

        let mut unrecognized_keys: Vec<String> = vec![];
        let mut config = TikvConfig::from_file(path, Some(&mut unrecognized_keys)).unwrap();
        assert_eq!(config.raft_store.clean_stale_ranges_tick, 9999);
        address_proxy_config(&mut config, &ProxyConfig::default());
        let clean_stale_ranges_tick =
            (10_000 / config.raft_store.region_worker_tick_interval.as_millis()) as usize;
        assert_eq!(
            config.raft_store.clean_stale_ranges_tick,
            clean_stale_ranges_tick
        );
    }

    /// We test here if we can use proxy's default value without given file.
    #[test]
    fn test_config_proxy_default1() {
        let args: Vec<&str> = vec![];
        let matches = App::new("RaftStore Proxy")
            .arg(
                Arg::with_name("config")
                    .short("C")
                    .long("config")
                    .value_name("FILE")
                    .help("Set the configuration file")
                    .takes_value(true),
            )
            .get_matches_from(args);
        let mut v: Vec<String> = vec![];
        let mut config = gen_tikv_config(&None, false, &mut v);
        let mut proxy_config = gen_proxy_config(&None, false, &mut v);
        overwrite_config_with_cmd_args(&mut config, &mut proxy_config, &matches);
        address_proxy_config(&mut config, &proxy_config);

        assert_eq!(config.server.addr, TIFLASH_DEFAULT_LISTENING_ADDR);
        assert_eq!(config.server.status_addr, TIFLASH_DEFAULT_STATUS_ADDR);
        assert_eq!(
            config.server.advertise_status_addr,
            TIFLASH_DEFAULT_ADVERTISE_LISTENING_ADDR
        );
        assert_eq!(
            config.raft_store.region_worker_tick_interval.as_millis(),
            500
        );
    }

    /// We test here if we can use proxy's default value with given file,
    /// but without given field.
    #[test]
    fn test_config_proxy_default2() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        let text = "z=4\n[rocksdb]\nmax-open-files=56\n";
        write!(file, "{}", text).unwrap();
        let path = file.path();
        let cpath = Some(path.as_os_str());
        let args = vec![format!("-C{}", path.to_str().unwrap())];
        let matches = App::new("RaftStore Proxy")
            .arg(
                Arg::with_name("config")
                    .short("C")
                    .long("config")
                    .value_name("FILE")
                    .help("Set the configuration file")
                    .takes_value(true),
            )
            .get_matches_from(args);
        let mut v: Vec<String> = vec![];
        let mut config = gen_tikv_config(&cpath, false, &mut v);
        let mut proxy_config = gen_proxy_config(&cpath, false, &mut v);
        overwrite_config_with_cmd_args(&mut config, &mut proxy_config, &matches);
        address_proxy_config(&mut config, &proxy_config);

        let total_mem = SysQuota::memory_limit_in_bytes();
        assert_eq!(config.rocksdb.max_open_files, 56);
        assert_eq!(config.server.addr, TIFLASH_DEFAULT_LISTENING_ADDR);
        assert_eq!(config.server.status_addr, TIFLASH_DEFAULT_STATUS_ADDR);
        assert_eq!(
            config.server.advertise_status_addr,
            TIFLASH_DEFAULT_ADVERTISE_LISTENING_ADDR
        );
        assert_eq!(
            config.raft_store.region_worker_tick_interval.as_millis(),
            500
        );
        assert_eq!(
            ProxyConfig::default()
                .raft_store
                .apply_low_priority_pool_size,
            config.raft_store.apply_batch_system.low_priority_pool_size
        );
        assert_eq!(
            config.raftdb.defaultcf.block_cache_size,
            memory_limit_for_cf(true, CF_DEFAULT, total_mem)
        );
        assert_eq!(
            config.rocksdb.defaultcf.block_cache_size,
            memory_limit_for_cf(false, CF_DEFAULT, total_mem)
        );
        assert_eq!(
            config.rocksdb.writecf.block_cache_size,
            memory_limit_for_cf(false, CF_WRITE, total_mem)
        );
        assert_eq!(
            config.rocksdb.lockcf.block_cache_size,
            memory_limit_for_cf(false, CF_LOCK, total_mem)
        );
        assert_eq!(config.storage.reserve_space, ReadableSize::gb(1));
    }

    /// We test if the engine-label is set properly.
    #[test]
    fn test_config_proxy_engine_label() {
        // case-1: If engine-label not specified in arguments, use default value.
        let args: Vec<&str> = vec![];
        let matches = App::new("RaftStore Proxy").get_matches_from(args);
        let mut v: Vec<String> = vec![];
        let mut config = gen_tikv_config(&None, false, &mut v);
        let mut proxy_config = gen_proxy_config(&None, false, &mut v);
        overwrite_config_with_cmd_args(&mut config, &mut proxy_config, &matches);
        address_proxy_config(&mut config, &proxy_config);
        const DEFAULT_ENGINE_LABEL_KEY: &str = "engine";

        assert_eq!(
            config
                .server
                .labels
                .get(DEFAULT_ENGINE_LABEL_KEY)
                .unwrap()
                .as_str(),
            option_env!("ENGINE_LABEL_VALUE").unwrap()
        );

        // case-2: If engine-label specified in arguments, use it as engine-label.
        const EXPECTED_ENGINE_LABEL: &str = "tiflash_compute";
        let args = vec![
            "test_config_proxy_default1",
            "--engine-label",
            EXPECTED_ENGINE_LABEL,
        ];
        let matches = App::new("RaftStore Proxy")
            .arg(
                Arg::with_name("engine-label")
                    .long("engine-label")
                    .help("Set engine label")
                    .required(true)
                    .takes_value(true),
            )
            .get_matches_from(args);
        overwrite_config_with_cmd_args(&mut config, &mut proxy_config, &matches);
        address_proxy_config(&mut config, &proxy_config);
        assert_eq!(
            config.server.labels.get(DEFAULT_ENGINE_LABEL_KEY).unwrap(),
            EXPECTED_ENGINE_LABEL
        );
    }

    #[test]
    fn test_config_proxy_overwrite() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write!(
            file,
            "
[raftstore]
apply-low-priority-pool-size = 41
        "
        )
        .unwrap();
        let path = file.path();

        let mut v: Vec<String> = vec![];
        let cpath = Some(path.as_os_str());
        let mut config = gen_tikv_config(&cpath, false, &mut v);
        let proxy_config = gen_proxy_config(&cpath, false, &mut v);
        address_proxy_config(&mut config, &proxy_config);

        // When raftstore.apply-low-priority-pool-size is specified, its value
        // should be used.
        assert_eq!(
            41,
            config.raft_store.apply_batch_system.low_priority_pool_size
        );
    }

    #[test]
    fn test_store_setup() {
        let (mut cluster, pd_client) = new_mock_cluster(0, 3);

        // Add label to cluster
        address_proxy_config(&mut cluster.cfg.tikv, &ProxyConfig::default());

        // Try to start this node, return after persisted some keys.
        let _ = cluster.start();
        let store_id = cluster.engines.keys().last().unwrap();
        let store = pd_client.get_store(*store_id).unwrap();
        println!("store {:?}", store);
        assert!(
            store
                .get_labels()
                .iter()
                .find(|&x| x.key == "engine" && x.value == "tiflash")
                .is_some()
        );
        cluster.shutdown();
    }
}

mod write {
    use super::*;
    #[test]
    fn test_interaction() {
        // TODO Maybe we should pick this test to TiKV.
        // This test is to check if empty entries can affect pre_exec and post_exec.
        let (mut cluster, _pd_client) = new_mock_cluster(0, 3);

        fail::cfg("try_flush_data", "return(0)").unwrap();
        let _ = cluster.run();

        cluster.must_put(b"k1", b"v1");
        let region = cluster.get_region(b"k1");
        let region_id = region.get_id();

        // Wait until all nodes have (k1, v1).
        check_key(&cluster, b"k1", b"v1", Some(true), None, None);

        let prev_states = collect_all_states(&cluster, region_id);
        let compact_log = test_raftstore::new_compact_log_request(100, 10);
        let req =
            test_raftstore::new_admin_request(region_id, region.get_region_epoch(), compact_log);
        let _ = cluster
            .call_command_on_leader(req.clone(), Duration::from_secs(3))
            .unwrap();

        // Empty result can also be handled by post_exec
        let mut retry = 0;
        let new_states = loop {
            let new_states = collect_all_states(&cluster, region_id);
            let mut ok = true;
            for i in prev_states.keys() {
                let old = prev_states.get(i).unwrap();
                let new = new_states.get(i).unwrap();
                if old.in_memory_apply_state == new.in_memory_apply_state
                    && old.in_memory_applied_term == new.in_memory_applied_term
                {
                    ok = false;
                    break;
                }
            }
            if ok {
                break new_states;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
            retry += 1;
            if retry >= 30 {
                panic!("states is not changed")
            }
        };

        for i in prev_states.keys() {
            let old = prev_states.get(i).unwrap();
            let new = new_states.get(i).unwrap();
            assert_ne!(old.in_memory_apply_state, new.in_memory_apply_state);
            assert_eq!(old.in_memory_applied_term, new.in_memory_applied_term);
            // An empty cmd will not cause persistence.
            assert_eq!(old.in_disk_apply_state, new.in_disk_apply_state);
        }

        cluster.must_put(b"k2", b"v2");
        // Wait until all nodes have (k2, v2).
        check_key(&cluster, b"k2", b"v2", Some(true), None, None);

        fail::cfg("on_empty_cmd_normal", "return").unwrap();
        let prev_states = collect_all_states(&cluster, region_id);
        let _ = cluster
            .call_command_on_leader(req, Duration::from_secs(3))
            .unwrap();

        std::thread::sleep(std::time::Duration::from_millis(400));
        let new_states = collect_all_states(&cluster, region_id);
        for i in prev_states.keys() {
            let old = prev_states.get(i).unwrap();
            let new = new_states.get(i).unwrap();
            assert_ne!(old.in_memory_apply_state, new.in_memory_apply_state);
            assert_eq!(old.in_memory_applied_term, new.in_memory_applied_term);
        }

        fail::remove("try_flush_data");
        fail::remove("on_empty_cmd_normal");
        cluster.shutdown();
    }

    #[test]
    fn test_leadership_change_filter() {
        test_leadership_change_impl(true);
    }

    #[test]
    fn test_leadership_change_no_persist() {
        test_leadership_change_impl(false);
    }

    fn test_leadership_change_impl(filter: bool) {
        // Test if a empty command can be observed when leadership changes.
        let (mut cluster, _pd_client) = new_mock_cluster(0, 3);

        disable_auto_gen_compact_log(&mut cluster);

        if filter {
            // We don't handle CompactLog at all.
            fail::cfg("try_flush_data", "return(0)").unwrap();
        } else {
            // We don't return Persist after handling CompactLog.
            fail::cfg("no_persist_compact_log", "return").unwrap();
        }
        // Do not handle empty cmd.
        fail::cfg("on_empty_cmd_normal", "return").unwrap();
        let _ = cluster.run();

        cluster.must_put(b"k1", b"v1");
        let region = cluster.get_region(b"k1");
        let region_id = region.get_id();

        let eng_ids = cluster
            .engines
            .iter()
            .map(|e| e.0.to_owned())
            .collect::<Vec<_>>();
        let peer_1 = find_peer(&region, eng_ids[0]).cloned().unwrap();
        let peer_2 = find_peer(&region, eng_ids[1]).cloned().unwrap();
        cluster.must_transfer_leader(region.get_id(), peer_1.clone());

        cluster.must_put(b"k2", b"v2");
        fail::cfg("on_empty_cmd_normal", "return").unwrap();

        // Wait until all nodes have (k2, v2), then transfer leader.
        check_key(&cluster, b"k2", b"v2", Some(true), None, None);
        if filter {
            // We should also filter normal kv, since a empty result can also be invoke
            // pose_exec.
            fail::cfg("on_post_exec_normal", "return(false)").unwrap();
        }
        let prev_states = collect_all_states(&cluster, region_id);
        cluster.must_transfer_leader(region.get_id(), peer_2.clone());

        // The states remain the same, since we don't observe empty cmd.
        let new_states = collect_all_states(&cluster, region_id);
        for i in prev_states.keys() {
            let old = prev_states.get(i).unwrap();
            let new = new_states.get(i).unwrap();
            if filter {
                // CompactLog can still change in-memory state, when exec in memory.
                assert_eq!(old.in_memory_apply_state, new.in_memory_apply_state);
                assert_eq!(old.in_memory_applied_term, new.in_memory_applied_term);
            }
            assert_eq!(old.in_disk_apply_state, new.in_disk_apply_state);
        }

        fail::remove("on_empty_cmd_normal");
        // We need forward empty cmd generated by leadership changing to TiFlash.
        cluster.must_transfer_leader(region.get_id(), peer_1.clone());
        std::thread::sleep(std::time::Duration::from_secs(1));

        let new_states = collect_all_states(&cluster, region_id);
        for i in prev_states.keys() {
            let old = prev_states.get(i).unwrap();
            let new = new_states.get(i).unwrap();
            assert_ne!(old.in_memory_apply_state, new.in_memory_apply_state);
            assert_ne!(old.in_memory_applied_term, new.in_memory_applied_term);
        }

        if filter {
            fail::remove("try_flush_data");
            fail::remove("on_post_exec_normal");
        } else {
            fail::remove("no_persist_compact_log");
        }
        cluster.shutdown();
    }

    #[test]
    fn test_kv_write_always_persist() {
        let (mut cluster, _pd_client) = new_mock_cluster(0, 3);

        let _ = cluster.run();

        cluster.must_put(b"k0", b"v0");
        let region_id = cluster.get_region(b"k0").get_id();

        let mut prev_states = collect_all_states(&cluster, region_id);
        // Always persist on every command
        fail::cfg("on_post_exec_normal_end", "return(true)").unwrap();
        for i in 1..20 {
            let k = format!("k{}", i);
            let v = format!("v{}", i);
            cluster.must_put(k.as_bytes(), v.as_bytes());

            // We can't always get kv from disk, even we commit everytime,
            // since they are filtered by engint_tiflash
            check_key(&cluster, k.as_bytes(), v.as_bytes(), Some(true), None, None);

            // This may happen after memory write data and before commit.
            // We must check if we already have in memory.
            check_apply_state(&cluster, region_id, &prev_states, Some(false), None);
            std::thread::sleep(std::time::Duration::from_millis(20));
            // However, advanced apply index will always persisted.
            let new_states = collect_all_states(&cluster, region_id);
            for id in cluster.engines.keys() {
                let p = &prev_states.get(id).unwrap().in_disk_apply_state;
                let n = &new_states.get(id).unwrap().in_disk_apply_state;
                assert_ne!(p, n);
            }
            prev_states = new_states;
        }
        fail::remove("on_post_exec_normal_end");
        cluster.shutdown();
    }

    #[test]
    fn test_kv_write() {
        let (mut cluster, _pd_client) = new_mock_cluster(0, 3);

        fail::cfg("on_post_exec_normal", "return(false)").unwrap();
        fail::cfg("on_post_exec_admin", "return(false)").unwrap();
        // Abandon CompactLog and previous flush.
        fail::cfg("try_flush_data", "return(0)").unwrap();

        let _ = cluster.run();

        for i in 0..10 {
            let k = format!("k{}", i);
            let v = format!("v{}", i);
            cluster.must_put(k.as_bytes(), v.as_bytes());
        }

        // Since we disable all observers, we can get nothing in either memory and disk.
        for i in 0..10 {
            let k = format!("k{}", i);
            let v = format!("v{}", i);
            check_key(
                &cluster,
                k.as_bytes(),
                v.as_bytes(),
                Some(false),
                Some(false),
                None,
            );
        }

        // We can read initial raft state, since we don't persist meta either.
        let r1 = cluster.get_region(b"k1").get_id();
        let prev_states = collect_all_states(&cluster, r1);

        fail::remove("on_post_exec_normal");
        fail::remove("on_post_exec_admin");
        for i in 10..20 {
            let k = format!("k{}", i);
            let v = format!("v{}", i);
            cluster.must_put(k.as_bytes(), v.as_bytes());
        }

        // Since we enable all observers, we can get in memory.
        // However, we get nothing in disk since we don't persist.
        for i in 10..20 {
            let k = format!("k{}", i);
            let v = format!("v{}", i);
            check_key(
                &cluster,
                k.as_bytes(),
                v.as_bytes(),
                Some(true),
                Some(false),
                None,
            );
        }

        let new_states = collect_all_states(&cluster, r1);
        for id in cluster.engines.keys() {
            assert_ne!(
                &prev_states.get(id).unwrap().in_memory_apply_state,
                &new_states.get(id).unwrap().in_memory_apply_state
            );
            assert_eq!(
                &prev_states.get(id).unwrap().in_disk_apply_state,
                &new_states.get(id).unwrap().in_disk_apply_state
            );
        }

        std::thread::sleep(std::time::Duration::from_millis(20));
        fail::remove("try_flush_data");

        let prev_states = collect_all_states(&cluster, r1);
        // Write more after we force persist when CompactLog.
        for i in 20..30 {
            let k = format!("k{}", i);
            let v = format!("v{}", i);
            cluster.must_put(k.as_bytes(), v.as_bytes());
        }

        // We can read from mock-store's memory, we are not sure if we can read from
        // disk, since there may be or may not be a CompactLog.
        for i in 11..30 {
            let k = format!("k{}", i);
            let v = format!("v{}", i);
            check_key(&cluster, k.as_bytes(), v.as_bytes(), Some(true), None, None);
        }

        // Force a compact log to persist.
        let region_r = cluster.get_region("k1".as_bytes());
        let region_id = region_r.get_id();
        let compact_log = test_raftstore::new_compact_log_request(1000, 100);
        let req =
            test_raftstore::new_admin_request(region_id, region_r.get_region_epoch(), compact_log);
        let res = cluster
            .call_command_on_leader(req, Duration::from_secs(3))
            .unwrap();
        assert!(res.get_header().has_error(), "{:?}", res);
        // This CompactLog is executed with an error. It will not trigger a compaction.
        // However, it can trigger a persistence.
        for i in 11..30 {
            let k = format!("k{}", i);
            let v = format!("v{}", i);
            check_key(
                &cluster,
                k.as_bytes(),
                v.as_bytes(),
                Some(true),
                Some(true),
                None,
            );
        }

        let new_states = collect_all_states(&cluster, r1);

        // apply_state is changed in memory, and persisted.
        for id in cluster.engines.keys() {
            assert_ne!(
                &prev_states.get(id).unwrap().in_memory_apply_state,
                &new_states.get(id).unwrap().in_memory_apply_state
            );
            assert_ne!(
                &prev_states.get(id).unwrap().in_disk_apply_state,
                &new_states.get(id).unwrap().in_disk_apply_state
            );
        }

        fail::remove("no_persist_compact_log");
        cluster.shutdown();
    }

    #[test]
    fn test_consistency_check() {
        // ComputeHash and VerifyHash shall be filtered.
        let (mut cluster, _pd_client) = new_mock_cluster(0, 2);

        cluster.run();

        cluster.must_put(b"k", b"v");
        let region = cluster.get_region("k".as_bytes());
        let region_id = region.get_id();

        let r = new_compute_hash_request();
        let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), r);
        let _ = cluster
            .call_command_on_leader(req, Duration::from_secs(3))
            .unwrap();

        let r = new_verify_hash_request(vec![7, 8, 9, 0], 1000);
        let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), r);
        let _ = cluster
            .call_command_on_leader(req, Duration::from_secs(3))
            .unwrap();

        cluster.must_put(b"k2", b"v2");
        cluster.shutdown();
    }

    #[test]
    fn test_old_compact_log() {
        // If we just return None for CompactLog, the region state in ApplyFsm will
        // change. Because there is no rollback in new implementation.
        // This is a ERROR state.
        let (mut cluster, _pd_client) = new_mock_cluster(0, 3);
        cluster.run();

        // We don't return Persist after handling CompactLog.
        fail::cfg("no_persist_compact_log", "return").unwrap();
        for i in 0..10 {
            let k = format!("k{}", i);
            let v = format!("v{}", i);
            cluster.must_put(k.as_bytes(), v.as_bytes());
        }

        for i in 0..10 {
            let k = format!("k{}", i);
            let v = format!("v{}", i);
            check_key(&cluster, k.as_bytes(), v.as_bytes(), Some(true), None, None);
        }

        let region = cluster.get_region(b"k1");
        let region_id = region.get_id();
        let prev_state = collect_all_states(&cluster, region_id);
        let (compact_index, compact_term) = get_valid_compact_index(&prev_state);
        let compact_log = test_raftstore::new_compact_log_request(compact_index, compact_term);
        let req =
            test_raftstore::new_admin_request(region_id, region.get_region_epoch(), compact_log);
        let _ = cluster
            .call_command_on_leader(req, Duration::from_secs(3))
            .unwrap();

        // Wait for state applys.
        std::thread::sleep(std::time::Duration::from_secs(2));

        let new_state = collect_all_states(&cluster, region_id);
        for i in prev_state.keys() {
            let old = prev_state.get(i).unwrap();
            let new = new_state.get(i).unwrap();
            assert_ne!(
                old.in_memory_apply_state.get_truncated_state(),
                new.in_memory_apply_state.get_truncated_state()
            );
            assert_eq!(
                old.in_disk_apply_state.get_truncated_state(),
                new.in_disk_apply_state.get_truncated_state()
            );
        }

        fail::remove("no_persist_compact_log");
        cluster.shutdown();
    }

    #[test]
    fn test_compact_log() {
        let (mut cluster, _pd_client) = new_mock_cluster(0, 3);

        disable_auto_gen_compact_log(&mut cluster);

        cluster.run();

        cluster.must_put(b"k", b"v");
        let region = cluster.get_region("k".as_bytes());
        let region_id = region.get_id();

        fail::cfg("on_empty_cmd_normal", "return").unwrap();
        fail::cfg("try_flush_data", "return(0)").unwrap();
        for i in 0..10 {
            let k = format!("k{}", i);
            let v = format!("v{}", i);
            cluster.must_put(k.as_bytes(), v.as_bytes());
        }
        for i in 0..10 {
            let k = format!("k{}", i);
            let v = format!("v{}", i);
            check_key(&cluster, k.as_bytes(), v.as_bytes(), Some(true), None, None);
        }

        std::thread::sleep(std::time::Duration::from_millis(500));
        let prev_state = collect_all_states(&cluster, region_id);

        let (compact_index, compact_term) = get_valid_compact_index(&prev_state);
        let compact_log = test_raftstore::new_compact_log_request(compact_index, compact_term);
        let req =
            test_raftstore::new_admin_request(region_id, region.get_region_epoch(), compact_log);
        let res = cluster
            .call_command_on_leader(req, Duration::from_secs(3))
            .unwrap();
        // compact index should less than applied index
        assert!(!res.get_header().has_error(), "{:?}", res);

        // TODO(tiflash) Make sure compact log is filtered successfully.
        // Can be abstract to a retry function.
        std::thread::sleep(std::time::Duration::from_millis(500));

        // CompactLog is filtered, because we can't flush data.
        // However, we can still observe apply index advanced
        let new_state = collect_all_states(&cluster, region_id);
        for i in prev_state.keys() {
            let old = prev_state.get(i).unwrap();
            let new = new_state.get(i).unwrap();
            assert_eq!(
                old.in_memory_apply_state.get_truncated_state(),
                new.in_memory_apply_state.get_truncated_state()
            );
            assert_eq!(
                old.in_disk_apply_state.get_truncated_state(),
                new.in_disk_apply_state.get_truncated_state()
            );
            assert_eq!(
                old.in_memory_apply_state.get_applied_index() + 1,
                new.in_memory_apply_state.get_applied_index()
            );
            // Persist is before.
            assert_eq!(
                old.in_disk_apply_state.get_applied_index(),
                new.in_disk_apply_state.get_applied_index()
            );
        }

        fail::remove("on_empty_cmd_normal");
        fail::remove("try_flush_data");

        let (compact_index, compact_term) = get_valid_compact_index(&new_state);
        let prev_state = new_state;
        let compact_log = test_raftstore::new_compact_log_request(compact_index, compact_term);
        let req =
            test_raftstore::new_admin_request(region_id, region.get_region_epoch(), compact_log);
        let res = cluster
            .call_command_on_leader(req, Duration::from_secs(3))
            .unwrap();
        assert!(!res.get_header().has_error(), "{:?}", res);

        cluster.must_put(b"kz", b"vz");
        check_key(&cluster, b"kz", b"vz", Some(true), None, None);

        // CompactLog is not filtered
        let new_state = collect_all_states(&cluster, region_id);
        for i in prev_state.keys() {
            let old = prev_state.get(i).unwrap();
            let new = new_state.get(i).unwrap();
            assert_ne!(
                old.in_memory_apply_state.get_truncated_state(),
                new.in_memory_apply_state.get_truncated_state()
            );
            assert_eq!(
                old.in_memory_apply_state.get_applied_index() + 2, // compact log + (kz,vz)
                new.in_memory_apply_state.get_applied_index()
            );
        }

        cluster.shutdown();
    }

    #[test]
    fn test_empty_cmd() {
        // Test if a empty command can be observed when leadership changes.
        let (mut cluster, _pd_client) = new_mock_cluster(0, 3);
        // Disable compact log
        cluster.cfg.raft_store.raft_log_gc_count_limit = Some(1000);
        cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(10000);
        cluster.cfg.raft_store.snap_apply_batch_size = ReadableSize(50000);
        cluster.cfg.raft_store.raft_log_gc_threshold = 1000;

        let _ = cluster.run();

        cluster.must_put(b"k1", b"v1");
        let region = cluster.get_region(b"k1");
        let region_id = region.get_id();
        let eng_ids = cluster
            .engines
            .iter()
            .map(|e| e.0.to_owned())
            .collect::<Vec<_>>();
        let peer_1 = find_peer(&region, eng_ids[0]).cloned().unwrap();
        let peer_2 = find_peer(&region, eng_ids[1]).cloned().unwrap();
        cluster.must_transfer_leader(region.get_id(), peer_1.clone());
        std::thread::sleep(std::time::Duration::from_secs(2));

        check_key(&cluster, b"k1", b"v1", Some(true), None, None);
        let prev_states = collect_all_states(&cluster, region_id);

        // We need forward empty cmd generated by leadership changing to TiFlash.
        cluster.must_transfer_leader(region.get_id(), peer_2.clone());
        std::thread::sleep(std::time::Duration::from_secs(2));

        let new_states = collect_all_states(&cluster, region_id);
        for i in prev_states.keys() {
            let old = prev_states.get(i).unwrap();
            let new = new_states.get(i).unwrap();
            assert_ne!(old.in_memory_apply_state, new.in_memory_apply_state);
            assert_ne!(old.in_memory_applied_term, new.in_memory_applied_term);
        }

        std::thread::sleep(std::time::Duration::from_secs(2));
        fail::cfg("on_empty_cmd_normal", "return").unwrap();

        let prev_states = new_states;
        cluster.must_transfer_leader(region.get_id(), peer_1.clone());
        std::thread::sleep(std::time::Duration::from_secs(2));

        let new_states = collect_all_states(&cluster, region_id);
        for i in prev_states.keys() {
            let old = prev_states.get(i).unwrap();
            let new = new_states.get(i).unwrap();
            assert_eq!(old.in_memory_apply_state, new.in_memory_apply_state);
            assert_eq!(old.in_memory_applied_term, new.in_memory_applied_term);
        }

        fail::remove("on_empty_cmd_normal");

        cluster.shutdown();
    }
}

mod ingest {
    use test_sst_importer::gen_sst_file_with_kvs;

    use super::*;

    pub fn new_ingest_sst_cmd(meta: SstMeta) -> Request {
        let mut cmd = Request::default();
        cmd.set_cmd_type(CmdType::IngestSst);
        cmd.mut_ingest_sst().set_sst(meta);
        cmd
    }

    pub fn create_tmp_importer(cfg: &Config, kv_path: &str) -> (PathBuf, Arc<SstImporter>) {
        let dir = Path::new(kv_path).join("import-sst");
        let importer = {
            Arc::new(
                SstImporter::new(&cfg.import, dir.clone(), None, cfg.storage.api_version())
                    .unwrap(),
            )
        };
        (dir, importer)
    }

    fn make_sst(
        cluster: &Cluster<NodeCluster>,
        region_id: u64,
        region_epoch: RegionEpoch,
        keys: Vec<String>,
    ) -> (PathBuf, SstMeta, PathBuf) {
        let path = cluster.engines.iter().last().unwrap().1.kv.path();
        let (import_dir, importer) = create_tmp_importer(&cluster.cfg, path);

        // Prepare data
        let mut kvs: Vec<(&[u8], &[u8])> = Vec::new();
        let mut keys = keys;
        keys.sort();
        for i in 0..keys.len() {
            kvs.push((keys[i].as_bytes(), b"2"));
        }

        // Make file
        let sst_path = import_dir.join("test.sst");
        let (mut meta, data) = gen_sst_file_with_kvs(&sst_path, &kvs);
        meta.set_region_id(region_id);
        meta.set_region_epoch(region_epoch);
        meta.set_cf_name("default".to_owned());
        let mut file = importer.create(&meta).unwrap();
        file.append(&data).unwrap();
        file.finish().unwrap();

        // copy file to save dir.
        let src = sst_path.clone();
        let dst = file.get_import_path().save.to_str().unwrap();
        let _ = std::fs::copy(src.clone(), dst);

        (file.get_import_path().save.clone(), meta, sst_path)
    }

    #[test]
    fn test_handle_ingest_sst() {
        let (mut cluster, _pd_client) = new_mock_cluster(0, 1);
        let _ = cluster.run();

        let key = "k";
        cluster.must_put(key.as_bytes(), b"v");
        let region = cluster.get_region(key.as_bytes());

        let (file, meta, sst_path) = make_sst(
            &cluster,
            region.get_id(),
            region.get_region_epoch().clone(),
            (0..100).map(|i| format!("k{}", i)).collect::<Vec<_>>(),
        );

        let req = new_ingest_sst_cmd(meta);
        let _ = cluster.request(
            key.as_bytes(),
            vec![req],
            false,
            Duration::from_secs(5),
            true,
        );

        check_key(&cluster, b"k66", b"2", Some(true), Some(true), None);

        assert!(sst_path.as_path().is_file());
        assert!(!file.as_path().is_file());
        std::fs::remove_file(sst_path.as_path()).unwrap();
        cluster.shutdown();
    }

    #[test]
    fn test_invalid_ingest_sst() {
        let (mut cluster, _pd_client) = new_mock_cluster(0, 1);

        let _ = cluster.run();

        let key = "k";
        cluster.must_put(key.as_bytes(), b"v");
        let region = cluster.get_region(key.as_bytes());

        let mut bad_epoch = RegionEpoch::default();
        bad_epoch.set_conf_ver(999);
        bad_epoch.set_version(999);
        let (file, meta, sst_path) = make_sst(
            &cluster,
            region.get_id(),
            bad_epoch,
            (0..100).map(|i| format!("k{}", i)).collect::<Vec<_>>(),
        );

        let req = new_ingest_sst_cmd(meta);
        let _ = cluster.request(
            key.as_bytes(),
            vec![req],
            false,
            Duration::from_secs(5),
            false,
        );
        check_key(&cluster, b"k66", b"2", Some(false), Some(false), None);

        assert!(sst_path.as_path().is_file());
        assert!(!file.as_path().is_file());
        std::fs::remove_file(sst_path.as_path()).unwrap();
        cluster.shutdown();
    }

    #[test]
    fn test_ingest_return_none() {
        let (mut cluster, _pd_client) = new_mock_cluster(0, 1);

        disable_auto_gen_compact_log(&mut cluster);

        let _ = cluster.run();

        cluster.must_put(b"k1", b"v1");
        cluster.must_put(b"k5", b"v5");
        let region = cluster.get_region(b"k1");
        cluster.must_split(&region, b"k5");
        let region1 = cluster.get_region(b"k1");
        let region5 = cluster.get_region(b"k5");
        assert_ne!(region1.get_id(), region5.get_id());

        fail::cfg("on_handle_ingest_sst_return", "return").unwrap();

        let prev_states1 = collect_all_states(&cluster, region1.get_id());
        let prev_states5 = collect_all_states(&cluster, region5.get_id());
        let (file1, meta1, sst_path1) = make_sst(
            &cluster,
            region1.get_id(),
            region1.get_region_epoch().clone(),
            (0..100).map(|i| format!("k1_{}", i)).collect::<Vec<_>>(),
        );
        assert!(sst_path1.as_path().is_file());

        let req = new_ingest_sst_cmd(meta1);
        let _ = cluster.request(b"k1", vec![req], false, Duration::from_secs(5), true);

        let (file5, meta5, _sst_path5) = make_sst(
            &cluster,
            region5.get_id(),
            region5.get_region_epoch().clone(),
            (0..100).map(|i| format!("k5_{}", i)).collect::<Vec<_>>(),
        );
        let req = new_ingest_sst_cmd(meta5);
        let _ = cluster.request(b"k5", vec![req], false, Duration::from_secs(5), true);

        check_key(&cluster, b"k1_66", b"2", Some(true), Some(false), None);
        check_key(&cluster, b"k5_66", b"2", Some(true), Some(false), None);

        let new_states1 = collect_all_states(&cluster, region1.get_id());
        let new_states5 = collect_all_states(&cluster, region5.get_id());
        for i in prev_states1.keys() {
            let old = prev_states1.get(i).unwrap();
            let new = new_states1.get(i).unwrap();
            assert_ne!(old.in_memory_apply_state, new.in_memory_apply_state);
            assert_eq!(old.in_memory_applied_term, new.in_memory_applied_term);
            assert_eq!(old.in_disk_apply_state, new.in_disk_apply_state);
        }
        for i in prev_states5.keys() {
            let old = prev_states5.get(i).unwrap();
            let new = new_states5.get(i).unwrap();
            assert_ne!(old.in_memory_apply_state, new.in_memory_apply_state);
            assert_eq!(old.in_memory_applied_term, new.in_memory_applied_term);
            assert_eq!(old.in_disk_apply_state, new.in_disk_apply_state);
        }
        let prev_states1 = new_states1;
        let prev_states5 = new_states5;
        // Not deleted
        assert!(file1.as_path().is_file());
        assert!(file5.as_path().is_file());
        fail::remove("on_handle_ingest_sst_return");

        let (file11, meta11, sst_path11) = make_sst(
            &cluster,
            region1.get_id(),
            region1.get_region_epoch().clone(),
            (200..300).map(|i| format!("k1_{}", i)).collect::<Vec<_>>(),
        );
        assert!(sst_path11.as_path().is_file());

        let req = new_ingest_sst_cmd(meta11);
        let _ = cluster.request(b"k1", vec![req], false, Duration::from_secs(5), true);

        check_key(&cluster, b"k1_222", b"2", Some(true), None, None);
        check_key(&cluster, b"k5_66", b"2", Some(false), None, None);

        let new_states1 = collect_all_states(&cluster, region1.get_id());
        let new_states5 = collect_all_states(&cluster, region5.get_id());
        // Region 1 is persisted.
        for i in prev_states1.keys() {
            let old = prev_states1.get(i).unwrap();
            let new = new_states1.get(i).unwrap();
            assert_ne!(old.in_memory_apply_state, new.in_memory_apply_state);
            assert_eq!(old.in_memory_applied_term, new.in_memory_applied_term);
            assert_ne!(old.in_disk_apply_state, new.in_disk_apply_state);
        }
        // Region 5 not persisted yet.
        for i in prev_states5.keys() {
            let old = prev_states5.get(i).unwrap();
            let new = new_states5.get(i).unwrap();
            assert_eq!(old.in_disk_apply_state, new.in_disk_apply_state);
        }
        // file1 and file11 for region 1 is deleted.
        assert!(!file1.as_path().is_file());
        assert!(!file11.as_path().is_file());
        assert!(file5.as_path().is_file());

        // ssp_path1/11/5 share one path.
        std::fs::remove_file(sst_path1.as_path()).unwrap();
        cluster.shutdown();
    }
}

mod restart {
    use super::*;

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
        must_get_none(&engine_2, first_key);
        // add peer (engine_2,engine_2) to region 1.
        pd_client.must_add_peer(r1, new_peer(eng_ids[1], eng_ids[1]));

        check_key(&cluster, first_key, &first_value, Some(false), None, None);

        // If we wait here any longer, the snapshot can be applied...
        // So we have to disable this test.
        // std::thread::sleep(std::time::Duration::from_millis(2500));

        info!("stop node {}", eng_ids[1]);
        cluster.stop_node(eng_ids[1]);
        {
            let lock = cluster.ffi_helper_set.lock();
            lock.unwrap()
                .deref_mut()
                .get_mut(&eng_ids[1])
                .unwrap()
                .engine_store_server
                .stop();
        }

        fail::remove("on_ob_pre_handle_snapshot");
        fail::remove("on_ob_post_apply_snapshot");
        info!("resume node {}", eng_ids[1]);
        {
            let lock = cluster.ffi_helper_set.lock();
            lock.unwrap()
                .deref_mut()
                .get_mut(&eng_ids[1])
                .unwrap()
                .engine_store_server
                .restore();
        }
        info!("restored node {}", eng_ids[1]);
        cluster.run_node(eng_ids[1]).unwrap();

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
        let prev_state = collect_all_states(&cluster, region_id);
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

        info!("stop node {}", eng_ids[0]);
        cluster.stop_node(eng_ids[0]);
        {
            let lock = cluster.ffi_helper_set.lock();
            lock.unwrap()
                .deref_mut()
                .get_mut(&eng_ids[0])
                .unwrap()
                .engine_store_server
                .stop();
        }

        info!("resume node {}", eng_ids[0]);
        {
            let lock = cluster.ffi_helper_set.lock();
            lock.unwrap()
                .deref_mut()
                .get_mut(&eng_ids[0])
                .unwrap()
                .engine_store_server
                .restore();
        }
        info!("restored node {}", eng_ids[0]);
        cluster.run_node(eng_ids[0]).unwrap();

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

mod snapshot {
    use super::*;

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

        let prev_states = collect_all_states(&cluster, region_id);
        cluster.must_put(b"k1", b"v1");
        check_key(&cluster, b"k1", b"v1", Some(true), Some(false), None);
        let new_states = collect_all_states(&cluster, region_id);
        for i in prev_states.keys() {
            let old = prev_states.get(i).unwrap();
            let new = new_states.get(i).unwrap();
            assert_eq!(
                old.in_memory_apply_state.get_applied_index() + 1,
                new.in_memory_apply_state.get_applied_index()
            );
            assert_eq!(
                old.in_disk_apply_state.get_applied_index(),
                new.in_disk_apply_state.get_applied_index()
            );
        }

        fail::cfg("on_pre_persist_with_finish", "return").unwrap();
        cluster.must_put(b"k2", b"v2");
        // Because we flush when batch ends.
        check_key(&cluster, b"k2", b"v2", Some(true), Some(false), None);

        // TODO(tiflash) wait `write_apply_state` in raftstore.
        std::thread::sleep(std::time::Duration::from_millis(1000));
        let prev_states = collect_all_states(&cluster, region_id);
        cluster.must_put(b"k3", b"v3");
        // Because we flush when batch ends.
        check_key(&cluster, b"k3", b"v3", Some(true), Some(false), None);

        // TODO(tiflash) wait `write_apply_state` in raftstore.
        std::thread::sleep(std::time::Duration::from_millis(1000));
        let new_states = collect_all_states(&cluster, region_id);
        for i in prev_states.keys() {
            let old = prev_states.get(i).unwrap();
            let new = new_states.get(i).unwrap();
            let gap = new.in_memory_apply_state.get_applied_index()
                - old.in_memory_apply_state.get_applied_index();
            let gap2 = new.in_disk_apply_state.get_applied_index()
                - old.in_disk_apply_state.get_applied_index();
            assert_eq!(gap, gap2);
        }
        fail::remove("on_pre_persist_with_finish");
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
        let prev_states = collect_all_states(&cluster, r3.get_id());

        info!("start merge"; "from" => r1.get_id(), "to" => r3.get_id());
        pd_client.must_merge(r1.get_id(), r3.get_id());

        // TODO(tiflash) wait `write_apply_state` in raftstore.
        std::thread::sleep(std::time::Duration::from_millis(1000));
        let r3_new = cluster.get_region(b"k3");
        assert_eq!(r3_new.get_id(), r3.get_id());
        let new_states = collect_all_states(&cluster, r3_new.get_id());
        // index 6 empty command
        // index 7 CommitMerge
        for i in prev_states.keys() {
            let old = prev_states.get(i).unwrap();
            let new = new_states.get(i).unwrap();
            let _gap = new.in_memory_apply_state.get_applied_index()
                - old.in_memory_apply_state.get_applied_index();
            let gap2 = new.in_disk_apply_state.get_applied_index()
                - old.in_disk_apply_state.get_applied_index();
            assert_eq!(gap2, 2);
        }
    }
}
