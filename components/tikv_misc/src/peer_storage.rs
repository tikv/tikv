use engine::CF_RAFT;
use engine::rocks;
use engine::rocks::DB;
use engine::Mutable;
use kvproto::metapb;
use kvproto::raft_serverpb::{
    MergeState, PeerState, RaftApplyState, RaftLocalState, RegionLocalState,
};
use super::keys;

pub const RAFT_INIT_LOG_TERM: u64 = 5;
pub const RAFT_INIT_LOG_INDEX: u64 = 5;
pub const JOB_STATUS_CANCELLING: usize = 2;

// When we bootstrap the region we must call this to initialize region local state first.
pub fn write_initial_raft_state<T: Mutable>(raft_wb: &T, region_id: u64) -> engine::Result<()> {
    let mut raft_state = RaftLocalState::new();
    raft_state.set_last_index(RAFT_INIT_LOG_INDEX);
    raft_state.mut_hard_state().set_term(RAFT_INIT_LOG_TERM);
    raft_state.mut_hard_state().set_commit(RAFT_INIT_LOG_INDEX);

    raft_wb.put_msg(&keys::raft_state_key(region_id), &raft_state)?;
    Ok(())
}

// When we bootstrap the region or handling split new region, we must
// call this to initialize region apply state first.
pub fn write_initial_apply_state<T: Mutable>(
    kv_engine: &DB,
    kv_wb: &T,
    region_id: u64,
) -> engine::Result<()> {
    let mut apply_state = RaftApplyState::new();
    apply_state.set_applied_index(RAFT_INIT_LOG_INDEX);
    apply_state
        .mut_truncated_state()
        .set_index(RAFT_INIT_LOG_INDEX);
    apply_state
        .mut_truncated_state()
        .set_term(RAFT_INIT_LOG_TERM);

    let handle = rocks::util::get_cf_handle(kv_engine, CF_RAFT)?;
    kv_wb.put_msg_cf(handle, &keys::apply_state_key(region_id), &apply_state)?;
    Ok(())
}

pub fn write_peer_state<T: Mutable>(
    kv_engine: &DB,
    kv_wb: &T,
    region: &metapb::Region,
    state: PeerState,
    merge_state: Option<MergeState>,
) -> engine::Result<()> {
    let region_id = region.get_id();
    let mut region_state = RegionLocalState::new();
    region_state.set_state(state);
    region_state.set_region(region.clone());
    if let Some(state) = merge_state {
        region_state.set_merge_state(state);
    }

    let handle = rocks::util::get_cf_handle(kv_engine, CF_RAFT)?;
    debug!(
        "writing merge state";
        "region_id" => region_id,
        "state" => ?region_state,
    );
    kv_wb.put_msg_cf(handle, &keys::region_state_key(region_id), &region_state)?;
    Ok(())
}
