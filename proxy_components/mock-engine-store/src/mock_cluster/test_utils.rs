// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.
use std::{collections::HashMap, iter::FromIterator};

use collections::HashSet;
use engine_store_ffi::ffi::interfaces_ffi;
use engine_traits::Peekable;
use kvproto::raft_serverpb::{RaftApplyState, RaftLocalState, RegionLocalState, StoreIdent};
pub use test_raftstore::{
    must_get_equal, must_get_none, new_learner_peer, new_peer, new_put_cmd, new_request,
};
use tikv_util::error;

use super::cluster_ext::*;
pub use super::mixed_cluster::MixedCluster;
use crate::{
    general_get_apply_state, general_get_raft_local_state, general_get_region_local_state,
};

#[derive(Debug, Default)]
pub struct FlushedIndex {
    pub admin: u64,
    pub data: u64,
}

/// All in_memory_ state are recorded in EngineStore.
/// All in_disk_ state can somehow be fetched through KvEngine or RaftEngine.
#[derive(Debug)]
pub struct States {
    // In proxy's memory, may be smaller than in_disk_apply_state.
    pub in_memory_apply_state: RaftApplyState,
    pub in_memory_applied_term: u64,
    // In persistence of raft-engine.
    pub in_disk_apply_state: RaftApplyState,
    pub in_disk_region_state: RegionLocalState,
    pub in_disk_raft_state: RaftLocalState,
    // TODO maybe unused, we keep that since the persistence of admin.flushed is async.
    #[allow(unused_variables)]
    pub in_memory_flush_index: FlushedIndex,
    // The flush index in RaftEngine.
    pub in_disk_flush_index: FlushedIndex,
    // The flush index record in EngineStore.
    pub in_engine_store_flush_index: FlushedIndex,
    pub ident: StoreIdent,
}

pub fn iter_ffi_helpers(
    cluster: &impl MixedCluster,
    store_ids: Option<Vec<u64>>,
    f: &mut dyn FnMut(u64, &mut FFIHelperSet),
) {
    cluster.iter_ffi_helpers(store_ids, f);
}

pub fn maybe_collect_states(
    cluster_ext: &ClusterExt,
    region_id: u64,
    store_ids: Option<Vec<u64>>,
) -> HashMap<u64, States> {
    let mut prev_state: HashMap<u64, States> = HashMap::default();
    cluster_ext.iter_ffi_helpers(store_ids, &mut |id: u64, ffi: &mut FFIHelperSet| {
        let server = &ffi.engine_store_server;
        let raft_engine = &server.engines.as_ref().unwrap().raft;
        let ffi_engine = &server.engines.as_ref().unwrap().kv;
        if let Some(region) = server.kvstore.get(&region_id) {
            let ident = match ffi_engine.get_msg::<StoreIdent>(keys::STORE_IDENT_KEY) {
                Ok(Some(i)) => i,
                _ => unreachable!(),
            };
            let apply_state = general_get_apply_state(ffi_engine, region_id);
            let region_state = general_get_region_local_state(ffi_engine, region_id);
            let raft_state = general_get_raft_local_state(raft_engine, region_id);
            if apply_state.is_none() {
                return;
            }
            if region_state.is_none() {
                return;
            }
            if raft_state.is_none() {
                return;
            }
            prev_state.insert(
                id,
                States {
                    in_memory_apply_state: region.apply_state.clone(),
                    in_memory_applied_term: region.applied_term,
                    in_disk_apply_state: apply_state.unwrap(),
                    in_disk_region_state: region_state.unwrap(),
                    in_disk_raft_state: raft_state.unwrap(),
                    in_disk_flush_index: Default::default(),
                    in_memory_flush_index: Default::default(),
                    in_engine_store_flush_index: Default::default(),
                    ident,
                },
            );
        }
    });
    prev_state
}

pub fn collect_all_states(cluster_ext: &ClusterExt, region_id: u64) -> HashMap<u64, States> {
    let prev_state = maybe_collect_states(cluster_ext, region_id, None);
    assert_eq!(
        prev_state.len(),
        cluster_ext.ffi_helper_set.lock().expect("poison").len()
    );
    prev_state
}

pub fn must_get_mem(
    cluster_ext: &ClusterExt,
    node_id: u64,
    region_id: u64,
    key: &[u8],
    value: Option<&[u8]>,
) {
    let last_res: Option<&Vec<u8>> = None;
    let cf = interfaces_ffi::ColumnFamilyType::Default;
    for _ in 1..300 {
        let mut ok = false;
        {
            cluster_ext.iter_ffi_helpers(Some(vec![node_id]), &mut |_, ffi: &mut FFIHelperSet| {
                let server = &ffi.engine_store_server;
                // If the region not exists in the node, will return None.
                let res = server.get_mem(region_id, cf, &key.to_vec());
                if let (Some(value), Some(last_res)) = (value, res) {
                    assert_eq!(value, &last_res[..]);
                    ok = true;
                    return;
                }
                if value.is_none() && last_res.is_none() {
                    ok = true;
                }
            });
        }
        if ok {
            return;
        }

        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let s = std::str::from_utf8(key).unwrap_or("");
    let e = format!(
        "can't get mem value {:?} for key {}({}) in store {} cf {:?}, actual {:?}",
        value.map(tikv_util::escape),
        log_wrappers::hex_encode_upper(key),
        s,
        node_id,
        cf,
        last_res,
    );
    error!("{}", e);
    panic!("{}", e);
}

pub fn check_apply_state(
    cluster_ext: &ClusterExt,
    region_id: u64,
    prev_states: &HashMap<u64, States>,
    in_mem_eq: Option<bool>,
    in_disk_eq: Option<bool>,
) {
    let old = prev_states.get(&region_id).unwrap();
    for _ in 1..10 {
        let new_states = collect_all_states(cluster_ext, region_id);
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
    let new_states = collect_all_states(cluster_ext, region_id);
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

// Must wait until some node satisfy cond given by `pref`.
// If region not exists in store, consider pred is not satisfied.
pub fn must_wait_until_cond_node(
    cluster_ext: &ClusterExt,
    region_id: u64,
    store_ids: Option<Vec<u64>>,
    pred: &dyn Fn(&States) -> bool,
) -> HashMap<u64, States> {
    let mut retry = 0;
    loop {
        let new_states = maybe_collect_states(cluster_ext, region_id, store_ids.clone());
        let mut ok = true;
        if let Some(ref e) = store_ids {
            if e.len() == new_states.len() {
                for i in new_states.keys() {
                    if let Some(new) = new_states.get(i) {
                        if !pred(new) {
                            ok = false;
                            break;
                        }
                    } else {
                        ok = false;
                        break;
                    }
                }
            } else {
                // If region not exists in some store.
                ok = false;
            }
        }
        if ok {
            break new_states;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
        retry += 1;
        if retry >= 30 {
            panic!("states not as expect after timeout {:?}", new_states)
        }
    }
}

pub fn must_wait_until_cond_generic(
    cluster_ext: &ClusterExt,
    region_id: u64,
    store_ids: Option<Vec<u64>>,
    pred: &dyn Fn(&HashMap<u64, States>) -> bool,
) -> HashMap<u64, States> {
    let mut retry = 0;
    loop {
        let new_states = maybe_collect_states(cluster_ext, region_id, store_ids.clone());
        let ok = pred(&new_states);
        if ok {
            break new_states;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
        retry += 1;
        if retry >= 30 {
            panic!("states not as expect after timeout {:?}", new_states)
        }
    }
}

pub fn must_not_wait_until_cond_generic_for(
    cluster_ext: &ClusterExt,
    region_id: u64,
    store_ids: Option<Vec<u64>>,
    pred: &dyn Fn(&HashMap<u64, States>) -> bool,
    millis: u64,
) -> HashMap<u64, States> {
    let mut retry = 0;
    loop {
        let new_states = maybe_collect_states(cluster_ext, region_id, store_ids.clone());
        let fail = pred(&new_states);
        if fail {
            panic!("states should not be {:?}", new_states)
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
        retry += 1;
        if retry * 100 >= millis {
            break new_states;
        }
    }
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

pub fn check_state<F: Fn(&States)>(states: &HashMap<u64, States>, f: F) {
    for i in states.keys() {
        f(states.get(i).unwrap());
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
    cluster_ext: &ClusterExt,
    region_id: u64,
    prev_states: &HashMap<u64, States>,
    pred: &dyn Fn(&States, &States) -> bool,
) -> HashMap<u64, States> {
    let mut retry = 0;
    loop {
        let new_states = collect_all_states(cluster_ext, region_id);
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
            panic!("states not as expect after timeout {:?}", new_states)
        }
    }
}

pub fn get_valid_compact_index(states: &HashMap<u64, States>) -> (u64, u64) {
    get_valid_compact_index_by(states, None)
}

pub fn get_valid_compact_index_by(
    states: &HashMap<u64, States>,
    use_nodes: Option<Vec<u64>>,
) -> (u64, u64) {
    let set = use_nodes.map(|nodes| HashSet::from_iter(nodes.clone()));
    states
        .iter()
        .filter(|(k, _)| {
            if let Some(ref s) = set {
                return s.contains(k);
            }
            true
        })
        .map(|(_, s)| {
            (
                s.in_memory_apply_state.get_applied_index(),
                s.in_memory_applied_term,
            )
        })
        .min_by(|l, r| l.0.cmp(&r.0))
        .unwrap()
}
