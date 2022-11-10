// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

pub use std::{
    collections::HashMap,
    io::Write,
    ops::DerefMut,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{atomic::Ordering, mpsc, Arc, RwLock},
};

pub use engine_store_ffi::{KVGetStatus, RaftStoreProxyFFI};
pub use engine_traits::{MiscExt, CF_DEFAULT, CF_LOCK, CF_WRITE};
// use engine_store_ffi::config::{ensure_no_common_unrecognized_keys, ProxyConfig};
pub use engine_traits::{Peekable, CF_RAFT};
pub use kvproto::{
    import_sstpb::SstMeta,
    metapb,
    metapb::RegionEpoch,
    raft_cmdpb::{AdminCmdType, AdminRequest, CmdType, Request},
    raft_serverpb::{RaftApplyState, RegionLocalState, StoreIdent},
};
pub use new_mock_engine_store::{
    config::Config,
    mock_cluster::{new_put_cmd, new_request, FFIHelperSet},
    must_get_equal, must_get_none,
    node::NodeCluster,
    transport_simulate::{
        CloneFilterFactory, CollectSnapshotFilter, Direction, RegionPacketFilter,
    },
    Cluster, ProxyConfig, Simulator, TestPdClient,
};
pub use raftstore::coprocessor::ConsistencyCheckMethod;
pub use test_raftstore::new_peer;
pub use tikv_util::{
    config::{ReadableDuration, ReadableSize},
    store::find_peer,
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

#[derive(Debug)]
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
        let engine = cluster.get_engine(id);
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

pub fn new_mock_cluster_snap(id: u64, count: usize) -> (Cluster<NodeCluster>, Arc<TestPdClient>) {
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut proxy_config = ProxyConfig::default();
    proxy_config.raft_store.snap_handle_pool_size = 2;
    let mut cluster = Cluster::new(id, count, sim, pd_client.clone(), proxy_config);
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
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(100000);
    cluster.cfg.raft_store.snap_apply_batch_size = ReadableSize(500000);
    cluster.cfg.raft_store.raft_log_gc_threshold = 10000;
}

pub fn compare_states<F: Fn(&States, &States)>(
    prev_states: &HashMap<u64, States>,
    new_states: &HashMap<u64, States>,
    f: F,
) {
    for i in prev_states.keys() {
        let old = prev_states.get(i).unwrap();
        let new = new_states.get(i).unwrap();
        f(old, new);
    }
}

pub fn must_unaltered_memory_apply_term(
    prev_states: &HashMap<u64, States>,
    new_states: &HashMap<u64, States>,
) {
    let f = |old: &States, new: &States| {
        assert_eq!(old.in_memory_applied_term, new.in_memory_applied_term);
    };
    compare_states(prev_states, new_states, f);
}

pub fn must_altered_memory_apply_term(
    prev_states: &HashMap<u64, States>,
    new_states: &HashMap<u64, States>,
) {
    let f = |old: &States, new: &States| {
        assert_ne!(old.in_memory_applied_term, new.in_memory_applied_term);
    };
    compare_states(prev_states, new_states, f);
}

pub fn must_unaltered_memory_apply_state(
    prev_states: &HashMap<u64, States>,
    new_states: &HashMap<u64, States>,
) {
    let f = |old: &States, new: &States| {
        assert_eq!(old.in_memory_apply_state, new.in_memory_apply_state);
    };
    compare_states(prev_states, new_states, f);
}

pub fn must_altered_memory_apply_state(
    prev_states: &HashMap<u64, States>,
    new_states: &HashMap<u64, States>,
) {
    let f = |old: &States, new: &States| {
        assert_ne!(old.in_memory_apply_state, new.in_memory_apply_state);
    };
    compare_states(prev_states, new_states, f);
}

pub fn must_altered_memory_apply_index(
    prev_states: &HashMap<u64, States>,
    new_states: &HashMap<u64, States>,
    apply_index_advanced: u64,
) {
    let f = |old: &States, new: &States| {
        assert_eq!(
            old.in_memory_apply_state.get_applied_index() + apply_index_advanced,
            new.in_memory_apply_state.get_applied_index()
        );
    };
    compare_states(prev_states, new_states, f);
}

pub fn must_altered_disk_apply_index(
    prev_states: &HashMap<u64, States>,
    new_states: &HashMap<u64, States>,
    apply_index_advanced: u64,
) {
    let f = |old: &States, new: &States| {
        assert_eq!(
            old.in_disk_apply_state.get_applied_index() + apply_index_advanced,
            new.in_disk_apply_state.get_applied_index()
        );
    };
    compare_states(prev_states, new_states, f);
}

pub fn must_apply_index_advanced_diff(
    prev_states: &HashMap<u64, States>,
    new_states: &HashMap<u64, States>,
    memory_more_advanced: u64,
) {
    let f = |old: &States, new: &States| {
        let gap = new.in_memory_apply_state.get_applied_index()
            - old.in_memory_apply_state.get_applied_index();
        let gap2 = new.in_disk_apply_state.get_applied_index()
            - old.in_disk_apply_state.get_applied_index();
        assert_eq!(gap, gap2 + memory_more_advanced);
    };
    compare_states(prev_states, new_states, f);
}

pub fn must_unaltered_disk_apply_state(
    prev_states: &HashMap<u64, States>,
    new_states: &HashMap<u64, States>,
) {
    let f = |old: &States, new: &States| {
        assert_eq!(old.in_disk_apply_state, new.in_disk_apply_state);
    };
    compare_states(prev_states, new_states, f);
}

pub fn must_altered_disk_apply_state(
    prev_states: &HashMap<u64, States>,
    new_states: &HashMap<u64, States>,
) {
    let f = |old: &States, new: &States| {
        assert_ne!(old.in_disk_apply_state, new.in_disk_apply_state);
    };
    compare_states(prev_states, new_states, f);
}

pub fn must_altered_memory_truncated_state(
    prev_states: &HashMap<u64, States>,
    new_states: &HashMap<u64, States>,
) {
    let f = |old: &States, new: &States| {
        assert_ne!(
            old.in_memory_apply_state.get_truncated_state(),
            new.in_memory_apply_state.get_truncated_state()
        );
    };
    compare_states(prev_states, new_states, f);
}

pub fn must_unaltered_memory_truncated_state(
    prev_states: &HashMap<u64, States>,
    new_states: &HashMap<u64, States>,
) {
    let f = |old: &States, new: &States| {
        assert_eq!(
            old.in_memory_apply_state.get_truncated_state(),
            new.in_memory_apply_state.get_truncated_state()
        );
    };
    compare_states(prev_states, new_states, f);
}

pub fn must_altered_disk_truncated_state(
    prev_states: &HashMap<u64, States>,
    new_states: &HashMap<u64, States>,
) {
    let f = |old: &States, new: &States| {
        assert_ne!(
            old.in_disk_apply_state.get_truncated_state(),
            new.in_disk_apply_state.get_truncated_state()
        );
    };
    compare_states(prev_states, new_states, f);
}

pub fn must_unaltered_disk_truncated_state(
    prev_states: &HashMap<u64, States>,
    new_states: &HashMap<u64, States>,
) {
    let f = |old: &States, new: &States| {
        assert_eq!(
            old.in_disk_apply_state.get_truncated_state(),
            new.in_disk_apply_state.get_truncated_state()
        );
    };
    compare_states(prev_states, new_states, f);
}

// Must wait until all nodes satisfy cond given by `pref`.
pub fn must_wait_until_cond_states(
    cluster: &Cluster<NodeCluster>,
    region_id: u64,
    prev_states: &HashMap<u64, States>,
    pred: &dyn Fn(&States, &States) -> bool,
) -> HashMap<u64, States> {
    let mut retry = 0;
    loop {
        let new_states = collect_all_states(&cluster, region_id);
        let mut ok = true;
        for i in prev_states.keys() {
            let old = prev_states.get(i).unwrap();
            let new = new_states.get(i).unwrap();
            if !pred(old, new) {
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
            panic!("states not as expect after timeout")
        }
    }
}
