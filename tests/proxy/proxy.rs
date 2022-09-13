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

use engine_store_ffi::{KVGetStatus, RaftStoreProxyFFI};
// use engine_store_ffi::config::{ensure_no_common_unrecognized_keys, ProxyConfig};
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
use proxy_server::config::{ensure_no_common_unrecognized_keys, validate_and_persist_config};
use raft::eraftpb::MessageType;
use raftstore::{
    coprocessor::{ConsistencyCheckMethod, Coprocessor},
    store::util::find_peer,
};
use sst_importer::SstImporter;
pub use test_raftstore::{must_get_equal, must_get_none, new_peer};
use tikv::config::TiKvConfig;
use tikv_util::{
    config::{LogFormat, ReadableDuration, ReadableSize},
    time::Duration,
    HandyRwLock,
};

// TODO Need refactor if moved to raft-engine
pub fn get_region_local_state(
    engine: &engine_rocks::RocksEngine,
    region_id: u64,
) -> RegionLocalState {
    let region_state_key = keys::region_state_key(region_id);
    let region_state = match engine.get_msg_cf::<RegionLocalState>(CF_RAFT, &region_state_key) {
        Ok(Some(s)) => s,
        _ => unreachable!(),
    };
    region_state
}

// TODO Need refactor if moved to raft-engine
pub fn get_apply_state(engine: &engine_rocks::RocksEngine, region_id: u64) -> RaftApplyState {
    let apply_state_key = keys::apply_state_key(region_id);
    let apply_state = match engine.get_msg_cf::<RaftApplyState>(CF_RAFT, &apply_state_key) {
        Ok(Some(s)) => s,
        _ => unreachable!(),
    };
    apply_state
}

pub fn new_compute_hash_request() -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::ComputeHash);
    req.mut_compute_hash()
        .set_context(vec![ConsistencyCheckMethod::Raw as u8]);
    req
}

pub fn new_verify_hash_request(hash: Vec<u8>, index: u64) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::VerifyHash);
    req.mut_verify_hash().set_hash(hash);
    req.mut_verify_hash().set_index(index);
    req
}

pub struct States {
    pub in_memory_apply_state: RaftApplyState,
    pub in_memory_applied_term: u64,
    pub in_disk_apply_state: RaftApplyState,
    pub in_disk_region_state: RegionLocalState,
    pub ident: StoreIdent,
}

pub fn iter_ffi_helpers(
    cluster: &Cluster<NodeCluster>,
    store_ids: Option<Vec<u64>>,
    f: &mut dyn FnMut(u64, &engine_rocks::RocksEngine, &mut FFIHelperSet) -> (),
) {
    let ids = match store_ids {
        Some(ids) => ids,
        None => cluster.engines.keys().map(|e| *e).collect::<Vec<_>>(),
    };
    for id in ids {
        let db = cluster.get_engine(id);
        let engine = engine_rocks::RocksEngine::from_db(db);
        let mut lock = cluster.ffi_helper_set.lock().unwrap();
        let ffiset = lock.get_mut(&id).unwrap();
        f(id, &engine, ffiset);
    }
}

pub fn collect_all_states(cluster: &Cluster<NodeCluster>, region_id: u64) -> HashMap<u64, States> {
    let mut prev_state: HashMap<u64, States> = HashMap::default();
    iter_ffi_helpers(
        cluster,
        None,
        &mut |id: u64, engine: &engine_rocks::RocksEngine, ffi: &mut FFIHelperSet| {
            let server = &ffi.engine_store_server;
            let region = server.kvstore.get(&region_id).unwrap();
            let ident = match engine.get_msg::<StoreIdent>(keys::STORE_IDENT_KEY) {
                Ok(Some(i)) => (i),
                _ => unreachable!(),
            };
            prev_state.insert(
                id,
                States {
                    in_memory_apply_state: region.apply_state.clone(),
                    in_memory_applied_term: region.applied_term,
                    in_disk_apply_state: get_apply_state(&engine, region_id),
                    in_disk_region_state: get_region_local_state(&engine, region_id),
                    ident,
                },
            );
        },
    );
    prev_state
}

pub fn new_mock_cluster(id: u64, count: usize) -> (Cluster<NodeCluster>, Arc<TestPdClient>) {
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut cluster = Cluster::new(id, count, sim, pd_client.clone(), ProxyConfig::default());
    // Compat new proxy
    cluster.cfg.proxy_compat = true;

    (cluster, pd_client)
}

pub fn must_get_mem(
    engine_store_server: &Box<new_mock_engine_store::EngineStoreServer>,
    region_id: u64,
    key: &[u8],
    value: Option<&[u8]>,
) {
    let last_res: Option<&Vec<u8>> = None;
    let cf = new_mock_engine_store::ffi_interfaces::ColumnFamilyType::Default;
    for _ in 1..300 {
        let res = engine_store_server.get_mem(region_id, cf, &key.to_vec());

        if let (Some(value), Some(last_res)) = (value, res) {
            assert_eq!(value, &last_res[..]);
            return;
        }
        if value.is_none() && last_res.is_none() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let s = std::str::from_utf8(key).unwrap_or("");
    panic!(
        "can't get mem value {:?} for key {}({}) in store {} cf {:?}, actual {:?}",
        value.map(tikv_util::escape),
        log_wrappers::hex_encode_upper(key),
        s,
        engine_store_server.id,
        cf,
        last_res,
    )
}

pub fn check_key(
    cluster: &Cluster<NodeCluster>,
    k: &[u8],
    v: &[u8],
    in_mem: Option<bool>,
    in_disk: Option<bool>,
    engines: Option<Vec<u64>>,
) {
    let region_id = cluster.get_region(k).get_id();
    let engine_keys = {
        match engines {
            Some(e) => e.to_vec(),
            None => cluster.engines.keys().map(|k| *k).collect::<Vec<u64>>(),
        }
    };
    for id in engine_keys {
        let engine = &cluster.get_engine(id);

        match in_disk {
            Some(b) => {
                if b {
                    must_get_equal(engine, k, v);
                } else {
                    must_get_none(engine, k);
                }
            }
            None => (),
        };
        match in_mem {
            Some(b) => {
                let lock = cluster.ffi_helper_set.lock().unwrap();
                let server = &lock.get(&id).unwrap().engine_store_server;
                if b {
                    must_get_mem(server, region_id, k, Some(v));
                } else {
                    must_get_mem(server, region_id, k, None);
                }
            }
            None => (),
        };
    }
}

pub fn check_apply_state(
    cluster: &Cluster<NodeCluster>,
    region_id: u64,
    prev_states: &HashMap<u64, States>,
    in_mem_eq: Option<bool>,
    in_disk_eq: Option<bool>,
) {
    let old = prev_states.get(&region_id).unwrap();
    for _ in 1..10 {
        let new_states = collect_all_states(&cluster, region_id);
        let new = new_states.get(&region_id).unwrap();
        if let Some(b) = in_mem_eq {
            if b && new.in_memory_applied_term == old.in_memory_applied_term
                && new.in_memory_apply_state == old.in_memory_apply_state
            {
                break;
            }
            if !b
                && (new.in_memory_applied_term != old.in_memory_applied_term
                    || new.in_memory_apply_state != old.in_memory_apply_state)
            {
                break;
            }
        }
        if let Some(b) = in_disk_eq {
            if b && new.in_disk_apply_state == old.in_disk_apply_state {
                break;
            }
            if !b && new.in_disk_apply_state != old.in_disk_apply_state {
                break;
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    let new_states = collect_all_states(&cluster, region_id);
    let new = new_states.get(&region_id).unwrap();
    if let Some(b) = in_mem_eq {
        if b {
            assert_eq!(new.in_memory_applied_term, old.in_memory_applied_term);
            assert_eq!(new.in_memory_apply_state, old.in_memory_apply_state);
        } else {
            assert_ne!(new.in_memory_apply_state, old.in_memory_apply_state);
        }
    }
    if let Some(b) = in_disk_eq {
        if b && new.in_disk_apply_state == old.in_disk_apply_state {
            assert_eq!(new.in_disk_apply_state, old.in_disk_apply_state);
        }
        if !b && new.in_disk_apply_state != old.in_disk_apply_state {
            assert_ne!(new.in_disk_apply_state, old.in_disk_apply_state);
        }
    }
}

pub fn get_valid_compact_index(states: &HashMap<u64, States>) -> (u64, u64) {
    states
        .iter()
        .map(|(_, s)| {
            (
                s.in_memory_apply_state.get_applied_index(),
                s.in_memory_applied_term,
            )
        })
        .min_by(|l, r| l.0.cmp(&r.0))
        .unwrap()
}

pub fn disable_auto_gen_compact_log(cluster: &mut Cluster<NodeCluster>) {
    // Disable AUTO generated compact log.
    // This will not totally disable, so we use some failpoints later.
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(1000);
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(10000);
    cluster.cfg.raft_store.snap_apply_batch_size = ReadableSize(50000);
    cluster.cfg.raft_store.raft_log_gc_threshold = 1000;
}

#[test]
fn test_kv_write() {
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);

    cluster.cfg.proxy_compat = false;
    // No persist will be triggered by CompactLog
    fail::cfg("no_persist_compact_log", "return").unwrap();
    let _ = cluster.run();

    cluster.must_put(b"k0", b"v0");
    // check_key(&cluster, b"k0", b"v0", Some(false), Some(false), None);

    // We can read initial raft state, since we don't persist meta either.
    let r1 = cluster.get_region(b"k0").get_id();
    let prev_states = collect_all_states(&mut cluster, r1);

    for i in 1..10 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        cluster.must_put(k.as_bytes(), v.as_bytes());
    }

    // Since we disable all observers, we can get nothing in either memory and disk.
    for i in 0..10 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        check_key(&cluster, k.as_bytes(), v.as_bytes(), Some(true), None, None);
    }

    let new_states = collect_all_states(&mut cluster, r1);
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

    debug!("now CompactLog can persist");
    fail::remove("no_persist_compact_log");

    let prev_states = collect_all_states(&mut cluster, r1);
    // Write more after we force persist when CompactLog.
    for i in 20..30 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        cluster.must_put(k.as_bytes(), v.as_bytes());
    }

    // We can read from mock-store's memory, we are not sure if we can read from disk,
    // since there may be or may not be a CompactLog.
    for i in 20..30 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        check_key(&cluster, k.as_bytes(), v.as_bytes(), Some(true), None, None);
    }

    // Force a compact log to persist.
    let region_r = cluster.get_region("k1".as_bytes());
    let region_id = region_r.get_id();
    let compact_log = test_raftstore::new_compact_log_request(100, 10);
    let req =
        test_raftstore::new_admin_request(region_id, region_r.get_region_epoch(), compact_log);
    let res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    assert!(res.get_header().has_error(), "{:?}", res);

    for i in 20..30 {
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

    let new_states = collect_all_states(&mut cluster, r1);

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
