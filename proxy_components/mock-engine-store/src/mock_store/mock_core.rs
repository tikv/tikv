// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use std::{collections::BTreeMap, sync::atomic::AtomicU64};

use engine_traits::RaftEngineDebug;

use super::common::*;

pub type RegionId = u64;
#[derive(Default, Clone)]
pub struct MockRegion {
    pub region: kvproto::metapb::Region,
    // Which peer is me?
    pub peer: kvproto::metapb::Peer,
    // in-memory data
    pub data: [BTreeMap<Vec<u8>, Vec<u8>>; 3],
    // If we a key is deleted, it will immediately be removed from data,
    // We will record the key in pending_delete, so we can delete it from disk when flushing.
    pub pending_delete: [HashSet<Vec<u8>>; 3],
    pub pending_write: [BTreeMap<Vec<u8>, Vec<u8>>; 3],
    pub apply_state: kvproto::raft_serverpb::RaftApplyState,
    pub applied_term: u64,
}

impl MockRegion {
    pub fn set_applied(&mut self, index: u64, term: u64) {
        self.apply_state.set_applied_index(index);
        self.applied_term = term;
    }

    pub fn new(meta: kvproto::metapb::Region) -> Self {
        MockRegion {
            region: meta,
            peer: Default::default(),
            data: Default::default(),
            pending_delete: Default::default(),
            pending_write: Default::default(),
            apply_state: Default::default(),
            applied_term: 0,
        }
    }
}

#[derive(Default)]
pub struct RegionStats {
    pub pre_handle_count: AtomicU64,
    // Count of call to `ffi_fast_add_peer`.
    pub fast_add_peer_count: AtomicU64,
    pub apply_snap_count: AtomicU64,
    // FAP is finished building. Whether succeed or not.
    pub finished_fast_add_peer_count: AtomicU64,
    pub started_fast_add_peers: std::sync::Mutex<HashSet<u64>>,
}

// In case of newly added cfs.
#[allow(unreachable_patterns)]
pub fn cf_to_name(cf: interfaces_ffi::ColumnFamilyType) -> &'static str {
    match cf {
        interfaces_ffi::ColumnFamilyType::Lock => CF_LOCK,
        interfaces_ffi::ColumnFamilyType::Write => CF_WRITE,
        interfaces_ffi::ColumnFamilyType::Default => CF_DEFAULT,
        _ => unreachable!(),
    }
}

pub fn write_kv_in_mem(region: &mut MockRegion, cf_index: usize, k: &[u8], v: &[u8]) {
    let data = &mut region.data[cf_index];
    let pending_delete = &mut region.pending_delete[cf_index];
    let pending_write = &mut region.pending_write[cf_index];
    pending_delete.remove(k);
    data.insert(k.to_vec(), v.to_vec());
    pending_write.insert(k.to_vec(), v.to_vec());
}

pub fn delete_kv_in_mem(region: &mut MockRegion, cf_index: usize, k: &[u8]) {
    let data = &mut region.data[cf_index];
    let pending_delete = &mut region.pending_delete[cf_index];
    pending_delete.insert(k.to_vec());
    data.remove(k);
}

// TODO adapt raftstore-v2
pub fn copy_meta_from<EK: engine_traits::KvEngine, ER: RaftEngine + RaftEngineDebug>(
    source_engines: &Engines<EK, ER>,
    target_engines: &Engines<EK, ER>,
    source: &MockRegion,
    target: &mut MockRegion,
    new_region_meta: kvproto::metapb::Region,
    copy_region_state: bool,
    copy_apply_state: bool,
    copy_raft_state: bool,
) -> raftstore::Result<()> {
    let region_id = source.region.get_id();
    let mut wb = target_engines.kv.write_batch();

    // Can't copy this key, otherwise will cause a bootstrap.
    // box_try!(wb.put_msg(keys::PREPARE_BOOTSTRAP_KEY, &source.region));

    // region local state
    if copy_region_state {
        let mut state = RegionLocalState::default();
        state.set_region(new_region_meta);
        box_try!(wb.put_msg_cf(CF_RAFT, &keys::region_state_key(region_id), &state));
    }

    // apply state
    if copy_apply_state {
        let apply_state: RaftApplyState =
            match general_get_apply_state(&source_engines.kv, region_id) {
                Some(x) => x,
                None => return Err(box_err!("bad RaftApplyState")),
            };
        wb.put_msg_cf(CF_RAFT, &keys::apply_state_key(region_id), &apply_state)?;
        target.apply_state = apply_state.clone();
        target.applied_term = source.applied_term;
    }

    wb.write()?;
    target_engines.sync_kv()?;

    let mut raft_wb = target_engines.raft.log_batch(1024);
    // raft state
    if copy_raft_state {
        let raft_state = match general_get_raft_local_state(&source_engines.raft, region_id) {
            Some(x) => x,
            None => return Err(box_err!("bad RaftLocalState")),
        };
        raft_wb.put_raft_state(region_id, &raft_state)?;
    };

    box_try!(target_engines.raft.consume(&mut raft_wb, true));
    Ok(())
}

// TODO adapt raftstore-v2
pub fn copy_data_from(
    source_engines: &Engines<impl KvEngine, impl RaftEngine + RaftEngineDebug>,
    target_engines: &Engines<impl KvEngine, impl RaftEngine>,
    source: &MockRegion,
    target: &mut MockRegion,
) -> raftstore::Result<()> {
    let region_id = source.region.get_id();

    // kv data in memory
    for cf in 0..3 {
        for (k, v) in &source.data[cf] {
            if (k.as_slice() >= source.region.get_start_key()
                || source.region.get_start_key().is_empty())
                && (k.as_slice() < source.region.get_end_key()
                    || source.region.get_end_key().is_empty())
            {
                debug!(
                    "copy_data_from {} to {} write to region {} {:?} {:?} S {:?} E {:?}",
                    source.peer.get_store_id(),
                    target.peer.get_store_id(),
                    region_id,
                    k,
                    v,
                    source.region.get_start_key(),
                    source.region.get_end_key()
                );
                write_kv_in_mem(target, cf, k.as_slice(), v.as_slice());
            } else {
                debug!(
                    "copy_data_from skip write to region {} {:?} {:?} S {:?} E {:?}",
                    region_id,
                    k,
                    v,
                    source.region.get_start_key(),
                    source.region.get_end_key()
                );
            }
        }
    }

    // raft log
    let mut raft_wb = target_engines.raft.log_batch(1024);
    let mut entries: Vec<raft::eraftpb::Entry> = Default::default();
    source_engines
        .raft
        .get_all_entries_to(region_id, &mut entries)
        .unwrap();
    debug!("copy raft log {:?}", entries);

    raft_wb.append(region_id, None, entries)?;
    target_engines.raft.consume(&mut raft_wb, true)?;
    Ok(())
}

// TODO Need refactor if moved to raft-engine
pub fn general_get_region_local_state<EK: engine_traits::KvEngine>(
    engine: &EK,
    region_id: u64,
) -> Option<RegionLocalState> {
    let region_state_key = keys::region_state_key(region_id);
    engine
        .get_msg_cf::<RegionLocalState>(CF_RAFT, &region_state_key)
        .unwrap_or(None)
}

// TODO Need refactor if moved to raft-engine
pub fn general_get_apply_state<EK: engine_traits::KvEngine>(
    engine: &EK,
    region_id: u64,
) -> Option<RaftApplyState> {
    let apply_state_key = keys::apply_state_key(region_id);
    engine
        .get_msg_cf::<RaftApplyState>(CF_RAFT, &apply_state_key)
        .unwrap_or(None)
}

pub fn general_get_raft_local_state<ER: engine_traits::RaftEngine>(
    raft_engine: &ER,
    region_id: u64,
) -> Option<RaftLocalState> {
    match raft_engine.get_raft_state(region_id) {
        Ok(Some(x)) => Some(x),
        _ => None,
    }
}

pub fn set_new_region_peer(new_region: &mut MockRegion, store_id: u64) {
    if let Some(peer) = new_region
        .region
        .get_peers()
        .iter()
        .find(|&peer| peer.get_store_id() == store_id)
    {
        new_region.peer = peer.clone();
    } else {
        // This happens when region is not found.
    }
}

pub fn make_new_region(
    maybe_from_region: Option<kvproto::metapb::Region>,
    maybe_store_id: Option<u64>,
) -> MockRegion {
    let mut region = MockRegion {
        region: maybe_from_region.unwrap_or_default(),
        ..Default::default()
    };
    if let Some(store_id) = maybe_store_id {
        set_new_region_peer(&mut region, store_id);
    }
    region
        .apply_state
        .mut_truncated_state()
        .set_index(raftstore::store::RAFT_INIT_LOG_INDEX);
    region
        .apply_state
        .mut_truncated_state()
        .set_term(raftstore::store::RAFT_INIT_LOG_TERM);
    region.set_applied(
        raftstore::store::RAFT_INIT_LOG_INDEX,
        raftstore::store::RAFT_INIT_LOG_TERM,
    );
    region
}
