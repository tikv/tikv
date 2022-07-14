// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

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

use crate::proxy::new_mock_cluster;

#[test]
fn test_config() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    let text = "memory-usage-high-water=0.65\nsnap-handle-pool-size=4\n[nosense]\nfoo=2\n[rocksdb]\nmax-open-files = 111\nz=1";
    write!(file, "{}", text).unwrap();
    let path = file.path();

    let mut unrecognized_keys = Vec::new();
    let mut config = TiKvConfig::from_file(path, Some(&mut unrecognized_keys)).unwrap();
    assert_eq!(config.memory_usage_high_water, 0.65);
    assert_eq!(config.rocksdb.max_open_files, 111);
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
