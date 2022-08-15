// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    io::{self, Read, Write},
    ops::{Deref, DerefMut},
    path::Path,
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
    raft_cmdpb::{AdminCmdType, AdminRequest},
    raft_serverpb::{RaftApplyState, RegionLocalState, StoreIdent},
};
use new_mock_engine_store::{
    mock_cluster::FFIHelperSet,
    node::NodeCluster,
    transport_simulate::{
        CloneFilterFactory, CollectSnapshotFilter, Direction, RegionPacketFilter,
    },
    Cluster, ProxyConfig, Simulator, TestPdClient,
};
use pd_client::PdClient;
use proxy_server::{
    config::{address_proxy_config, ensure_no_common_unrecognized_keys},
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
use server::setup::validate_and_persist_config;
use sst_importer::SstImporter;
use test_raftstore::new_tikv_config;
pub use test_raftstore::{must_get_equal, must_get_none, new_peer};
use tikv::config::TiKvConfig;
use tikv_util::{
    config::{LogFormat, ReadableDuration, ReadableSize},
    time::Duration,
    HandyRwLock,
};

use crate::proxy::*;

#[test]
fn test_config() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    let text = "memory-usage-high-water=0.65\nsnap-handle-pool-size=4\n[nosense]\nfoo=2\n[rocksdb]\nmax-open-files = 111\nz=1";
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
    assert_eq!(proxy_config.snap_handle_pool_size, 4);
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
    server::setup::validate_and_persist_config(&mut config, true);

    // Will not override ProxyConfig
    let proxy_config_new = ProxyConfig::from_file(path, None).unwrap();
    assert_eq!(proxy_config_new.snap_handle_pool_size, 4);
}

#[test]
fn test_config_addr() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    let text = "memory-usage-high-water=0.65\nsnap-handle-pool-size=4\n[nosense]\nfoo=2\n[rocksdb]\nmax-open-files = 111\nz=1";
    write!(file, "{}", text).unwrap();
    let path = file.path();
    let mut args: Vec<&str> = vec![];
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
fn test_consistency_check() {
    // ComputeHash and VerifyHash shall be filtered.
    let (mut cluster, pd_client) = new_mock_cluster(0, 2);

    cluster.run();

    cluster.must_put(b"k", b"v");
    let region = cluster.get_region("k".as_bytes());
    let region_id = region.get_id();

    let r = new_verify_hash_request(vec![1, 2, 3, 4, 5, 6], 1000);
    let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), r);
    let res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();

    let r = new_verify_hash_request(vec![7, 8, 9, 0], 1000);
    let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), r);
    let res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();

    cluster.must_put(b"k2", b"v2");
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
