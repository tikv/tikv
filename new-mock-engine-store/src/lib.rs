// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(slice_take)]
pub mod config;
pub mod mock_cluster;
pub mod mock_store;
pub mod node;
pub mod server;
pub mod transport_simulate;

pub use mock_store::*;

pub fn copy_meta_from<EK: engine_traits::KvEngine, ER: RaftEngine + engine_traits::Peekable>(
    source_engines: &Engines<EK, ER>,
    target_engines: &Engines<EK, ER>,
    source: &Region,
    target: &mut Region,
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
        let raft_state = match get_raft_local_state(&source_engines.raft, region_id) {
            Some(x) => x,
            None => return Err(box_err!("bad RaftLocalState")),
        };
        raft_wb.put_raft_state(region_id, &raft_state)?;
    };

    box_try!(target_engines.raft.consume(&mut raft_wb, true));
    Ok(())
}

pub fn copy_data_from(
    source_engines: &Engines<impl KvEngine, impl RaftEngine + engine_traits::Peekable>,
    target_engines: &Engines<impl KvEngine, impl RaftEngine>,
    source: &Region,
    target: &mut Region,
) -> raftstore::Result<()> {
    let region_id = source.region.get_id();

    // kv data in memory
    for cf in 0..3 {
        for (k, v) in &source.data[cf] {
            write_kv_in_mem(target, cf, k.as_slice(), v.as_slice());
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

    raft_wb.append(region_id, entries)?;
    box_try!(target_engines.raft.consume(&mut raft_wb, true));
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

pub fn get_region_local_state(
    engine: &engine_rocks::RocksEngine,
    region_id: u64,
) -> Option<RegionLocalState> {
    general_get_region_local_state(engine, region_id)
}

// TODO Need refactor if moved to raft-engine
pub fn get_apply_state(
    engine: &engine_rocks::RocksEngine,
    region_id: u64,
) -> Option<RaftApplyState> {
    general_get_apply_state(engine, region_id)
}

pub fn get_raft_local_state<ER: engine_traits::RaftEngine>(
    raft_engine: &ER,
    region_id: u64,
) -> Option<RaftLocalState> {
    match raft_engine.get_raft_state(region_id) {
        Ok(Some(x)) => Some(x),
        _ => None,
    }
}
