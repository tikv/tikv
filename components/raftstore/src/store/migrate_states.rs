// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{
    Engines, KvEngine, Mutable, RaftEngine, RaftLogBatch, Result as EngineTraitsResult, WriteBatch,
    CF_RAFT,
};
use kvproto::raft_serverpb::{PeerState, RegionLocalState};
use protobuf::Message;
use tikv_util::{box_try, info};

use crate::{store::peer_storage::recover_from_applying_state, Result};

pub fn migrate_states_from_kvdb_to_raftdb<EK, ER>(engines: &Engines<EK, ER>) -> Result<()>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    info!("start to migrate states from kvdb to raftdb");
    let start_key = keys::REGION_META_MIN_KEY;
    let end_key = keys::REGION_META_MAX_KEY;
    let kv_engine = engines.kv.clone();
    let raft_engine = engines.raft.clone();
    let mut raft_wb = raft_engine.log_batch(0);

    let mut total_count = 0;
    let mut tombstone_count = 0;
    let mut applying_count = 0;
    kv_engine.scan(CF_RAFT, start_key, end_key, false, |key, value| {
        let (region_id, suffix) = box_try!(keys::decode_region_meta_key(key));
        if suffix != keys::REGION_STATE_SUFFIX {
            return Ok(true);
        }

        total_count += 1;

        let mut region_state = RegionLocalState::default();
        region_state.merge_from_bytes(value)?;

        match region_state.get_state() {
            PeerState::Normal | PeerState::Merging  => {
                let apply_state = kv_engine
                    .get_msg_cf(CF_RAFT, &keys::apply_state_key(region_id))
                    .unwrap()
                    .unwrap();
                raft_wb.put_apply_state(region_id, &apply_state).unwrap();
                info!("migrate apply state from kvdb to raftdb"; "region_id" => region_id, "apply_state" => ?apply_state);
            }
            PeerState::Applying => {
                recover_from_applying_state(engines, &mut raft_wb, region_id).unwrap();
                let apply_state = kv_engine
                    .get_msg_cf(CF_RAFT, &keys::apply_state_key(region_id))
                    .unwrap()
                    .unwrap();
                raft_wb.put_apply_state(region_id, &apply_state).unwrap();
                raft_wb.put_apply_snapshot_state(region_id, &region_state, &apply_state).unwrap();
                applying_count += 1;
            }
            PeerState::Tombstone => {
                tombstone_count += 1;
            }
            PeerState::Unavailable => {
                // TODO: need to handle this state?
            }
        }
        raft_wb.put_region_state(region_id, &region_state).unwrap();

        Ok(true)
    })?;

    raft_engine.consume(&mut raft_wb, true)?;

    info!("migrating states from kvdb to raftdb done";
        "total_count" => total_count,
        "tombstone_count" => tombstone_count,
        "applying_count" => applying_count,
    );

    Ok(())
}

pub fn migrate_states_from_raftdb_to_kvdb<EK, ER>(
    engines: &Engines<EK, ER>,
    delete_states: bool,
) -> Result<()>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    info!("start to migrate states from raftdb to kvdb");
    let kv_engine = engines.kv.clone();
    let raft_engine = engines.raft.clone();
    let mut raft_wb = engines.raft.log_batch(0);
    let mut kv_wb = engines.kv.write_batch();

    raft_engine
        .for_each_raft_group(&mut |region_id| {
            if let Some((snap_region_state, snap_apply_state)) =
                raft_engine.get_apply_snapshot_state(region_id)?
            {
                kv_wb.put_msg_cf(
                    CF_RAFT,
                    &keys::region_state_key(region_id),
                    &snap_region_state,
                )?;
                kv_wb.put_msg_cf(
                    CF_RAFT,
                    &keys::apply_state_key(region_id),
                    &snap_apply_state,
                )?;
                info!(
                    "migrate applying snapshot region from raftdb";
                    "region_id" => region_id,
                    "snap_region_state" => ?snap_region_state,
                    "snap_apply_state" => ?snap_apply_state,
                );
            } else {
                let region_state = raft_engine
                    .get_region_state(region_id)?
                    .unwrap_or_else(|| panic!("region state not found, region_id: {}", region_id));
                kv_wb.put_msg_cf(CF_RAFT, &keys::region_state_key(region_id), &region_state)?;
                let apply_state = if region_state.get_state() != PeerState::Tombstone {
                    let apply_state =
                        raft_engine.get_apply_state(region_id)?.unwrap_or_else(|| {
                            panic!("apply state not found, region_id: {}", region_id)
                        });
                    kv_wb.put_msg_cf(CF_RAFT, &keys::apply_state_key(region_id), &apply_state)?;
                    Some(apply_state)
                } else {
                    None
                };

                info!(
                    "migrate region from raftdb";
                    "region_id" => region_id,
                    "region_state" => ?region_state,
                    "apply_state" => ?apply_state,
                );
            }

            if delete_states {
                raft_wb.delete_apply_snapshot_state(region_id).unwrap();
                raft_wb.delete_apply_state(region_id).unwrap();
                raft_wb.delete_region_state(region_id).unwrap();
            }

            EngineTraitsResult::Ok(())
        })
        .unwrap();

    if !kv_wb.is_empty() {
        kv_wb.write().unwrap();
        kv_engine.flush_cf(CF_RAFT, true).unwrap();
    }
    if !raft_wb.is_empty() {
        raft_engine.consume(&mut raft_wb, true).unwrap();
    }
    info!("migrating states from kvdb to raftdb done");

    Ok(())
}

pub fn clear_states_in_raftdb<E: RaftEngine>(engine: &E) -> Result<()> {
    let mut raft_wb = engine.log_batch(0);
    engine
        .for_each_raft_group(&mut |region_id| {
            raft_wb.delete_apply_snapshot_state(region_id).unwrap();
            raft_wb.delete_apply_state(region_id).unwrap();
            raft_wb.delete_region_state(region_id).unwrap();

            EngineTraitsResult::Ok(())
        })
        .unwrap();
    if !raft_wb.is_empty() {
        engine.consume(&mut raft_wb, true).unwrap();
    }
    Ok(())
}
