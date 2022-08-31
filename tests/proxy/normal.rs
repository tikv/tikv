// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    io::{self, Read, Write},
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc, Once, RwLock,
    },
};

use clap::{App, Arg, ArgMatches};
use engine_traits::{
    Error, ExternalSstFileInfo, Iterable, Iterator, MiscExt, Mutable, Peekable, Result, SeekKey,
    SstExt, SstReader, SstWriter, SstWriterBuilder, WriteBatch, WriteBatchExt, CF_DEFAULT, CF_LOCK,
    CF_RAFT, CF_WRITE,
};
use kvproto::{
    import_sstpb::SstMeta,
    metapb::RegionEpoch,
    raft_cmdpb::{AdminCmdType, AdminRequest, CmdType, Request},
    raft_serverpb::{RaftApplyState, RegionLocalState, StoreIdent},
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
        validate_and_persist_config,
    },
    proxy::{
        gen_tikv_config, setup_default_tikv_config, TIFLASH_DEFAULT_LISTENING_ADDR,
        TIFLASH_DEFAULT_STATUS_ADDR,
    },
    run::run_tikv_proxy,
};
use raft::eraftpb::MessageType;
use raftstore::{
    coprocessor::{ConsistencyCheckMethod, Coprocessor},
    engine_store_ffi,
    engine_store_ffi::{KVGetStatus, RaftStoreProxyFFI},
    store::util::find_peer,
};
use sst_importer::SstImporter;
pub use test_raftstore::{must_get_equal, must_get_none, new_peer};
use test_raftstore::{new_node_cluster, new_tikv_config};
use tikv::config::{TiKvConfig, LAST_CONFIG_FILE};
use tikv_util::{
    config::{LogFormat, ReadableDuration, ReadableSize},
    time::Duration,
    HandyRwLock,
};

use crate::proxy::*;

#[test]
fn test_config() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    let text = "memory-usage-high-water=0.65\n[server]\nengine-addr=\"1.2.3.4:5\"\n[raftstore]\nsnap-handle-pool-size=4\n[nosense]\nfoo=2\n[rocksdb]\nmax-open-files = 111\nz=1";
    write!(file, "{}", text).unwrap();
    let path = file.path();

    let mut unrecognized_keys = Vec::new();
    let mut config = TiKvConfig::from_file(path, Some(&mut unrecognized_keys)).unwrap();
    // Othersize we have no default addr for TiKv.
    setup_default_tikv_config(&mut config);
    assert_eq!(config.memory_usage_high_water, 0.65);
    assert_eq!(config.rocksdb.max_open_files, 111);
    assert_eq!(config.server.addr, TIFLASH_DEFAULT_LISTENING_ADDR);
    assert_eq!(unrecognized_keys.len(), 3);

    let mut proxy_unrecognized_keys = Vec::new();
    let proxy_config = ProxyConfig::from_file(path, Some(&mut proxy_unrecognized_keys)).unwrap();
    assert_eq!(proxy_config.raft_store.snap_handle_pool_size, 4);
    assert_eq!(proxy_config.server.engine_addr, "1.2.3.4:5");
    assert!(proxy_unrecognized_keys.contains(&"rocksdb".to_string()));
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
    let unknown = ensure_no_common_unrecognized_keys(&proxy_unrecognized_keys, &unrecognized_keys);
    assert_eq!(unknown.is_err(), true);
    assert_eq!(unknown.unwrap_err(), "nosense, rocksdb.z");

    // Need run this test with ENGINE_LABEL_VALUE=tiflash, otherwise will fatal exit.
    std::fs::remove_file(
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
    let text = "memory-usage-high-water=0.65\n[raftstore.aaa]\nbbb=2\n[server]\nengine-addr=\"1.2.3.4:5\"\n[raftstore]\nsnap-handle-pool-size=4\n[nosense]\nfoo=2\n[rocksdb]\nmax-open-files = 111\nz=1";
    write!(file, "{}", text).unwrap();
    let path = file.path();
    let tmp_store_folder = tempfile::TempDir::new().unwrap();
    let tmp_last_config_path = tmp_store_folder.path().join(LAST_CONFIG_FILE);
    std::fs::copy(path, tmp_last_config_path.as_path()).unwrap();
    std::fs::copy(path, "./last_ttikv.toml").unwrap();
    get_last_config(tmp_store_folder.path().to_str().unwrap());
}

#[test]
fn test_config_default_addr() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    let text = "memory-usage-high-water=0.65\nsnap-handle-pool-size=4\n[nosense]\nfoo=2\n[rocksdb]\nmax-open-files = 111\nz=1";
    write!(file, "{}", text).unwrap();
    let path = file.path();
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
    let c = format!("--config {}", path.to_str().unwrap());
    let mut v = vec![c];
    let config = gen_tikv_config(&matches, false, &mut v);
    assert_eq!(config.server.addr, TIFLASH_DEFAULT_LISTENING_ADDR);
    assert_eq!(config.server.status_addr, TIFLASH_DEFAULT_STATUS_ADDR);
    assert_eq!(
        config.server.advertise_status_addr,
        TIFLASH_DEFAULT_STATUS_ADDR
    );
}

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
    // let resp = block_on(pd_client.store_heartbeat(Default::default(), None, None)).unwrap();
    for id in cluster.engines.keys() {
        let store_stat = pd_client.get_store_stats(*id).unwrap();
        assert_eq!(store_stat.get_capacity(), 444444);
        assert_eq!(store_stat.get_available(), 333333);
    }
    // The same to mock-engine-store
    cluster.shutdown();
}

#[test]
fn test_store_setup() {
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);

    // Add label to cluster
    address_proxy_config(&mut cluster.cfg.tikv);

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

#[test]
fn test_interaction() {
    // TODO Maybe we should pick this test to TiKV.
    // This test is to check if empty entries can affect pre_exec and post_exec.
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);

    fail::cfg("try_flush_data", "return(0)").unwrap();
    let _ = cluster.run();

    cluster.must_put(b"k1", b"v1");
    let region = cluster.get_region(b"k1");
    let region_id = region.get_id();

    // Wait until all nodes have (k1, v1).
    check_key(&cluster, b"k1", b"v1", Some(true), None, None);

    let prev_states = collect_all_states(&cluster, region_id);
    let compact_log = test_raftstore::new_compact_log_request(100, 10);
    let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), compact_log);
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
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);

    // Disable AUTO generated compact log.
    // This will not totally disable, so we use some failpoints later.
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(1000);
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(10000);
    cluster.cfg.raft_store.snap_apply_batch_size = ReadableSize(50000);
    cluster.cfg.raft_store.raft_log_gc_threshold = 1000;

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
        // We should also filter normal kv, since a empty result can also be invoke pose_exec.
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
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);

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

    cluster.shutdown();
}

#[test]
fn test_kv_write() {
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);

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

    // We can read from mock-store's memory, we are not sure if we can read from disk,
    // since there may be or may not be a CompactLog.
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
    let (mut cluster, pd_client) = new_mock_cluster(0, 2);

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
    // If we just return None for CompactLog, the region state in ApplyFsm will change.
    // Because there is no rollback in new implementation.
    // This is a ERROR state.
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);
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
    let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), compact_log);
    let res = cluster
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

    cluster.shutdown();
}

#[test]
fn test_compact_log() {
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);

    // Disable auto compact log
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(1000);
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(10000);
    cluster.cfg.raft_store.snap_apply_batch_size = ReadableSize(50000);
    cluster.cfg.raft_store.raft_log_gc_threshold = 1000;

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
    let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), compact_log);
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
    let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), compact_log);
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

// TODO(tiflash) Test a KV will not be write twice by not only handle_put but also observer. When we fully enable engine_tiflash.

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
            SstImporter::new(&cfg.import, dir.clone(), None, cfg.storage.api_version()).unwrap(),
        )
    };
    (dir, importer)
}

mod ingest {
    use tempfile::TempDir;
    use test_sst_importer::gen_sst_file_with_kvs;
    use txn_types::TimeStamp;

    use super::*;

    fn make_sst(
        cluster: &Cluster<NodeCluster>,
        region_id: u64,
        region_epoch: RegionEpoch,
        keys: Vec<String>,
    ) -> (PathBuf, SstMeta, PathBuf) {
        let path = cluster.engines.iter().last().unwrap().1.kv.rocks.path();
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
        let dst = file.path.save.to_str().unwrap();
        std::fs::copy(src.clone(), dst);

        (file.path.save.clone(), meta, sst_path)
    }

    #[test]
    fn test_handle_ingest_sst() {
        let (mut cluster, pd_client) = new_mock_cluster(0, 1);
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
        let (mut cluster, pd_client) = new_mock_cluster(0, 1);

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
        let (mut cluster, pd_client) = new_mock_cluster(0, 1);

        // Disable auto compact log
        cluster.cfg.raft_store.raft_log_gc_count_limit = Some(1000);
        cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(10000);
        cluster.cfg.raft_store.snap_apply_batch_size = ReadableSize(50000);
        cluster.cfg.raft_store.raft_log_gc_threshold = 1000;

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

        let (file5, meta5, sst_path5) = make_sst(
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

#[test]
fn test_handle_destroy() {
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);

    // Disable raft log gc in this test case.
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::secs(60);

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
        b"k2",
        Some(false),
        None,
        Some(vec![eng_ids[1]]),
    );

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
fn test_empty_cmd() {
    // Test if a empty command can be observed when leadership changes.
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);
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
