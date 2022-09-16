// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{Engines, KvEngine, RaftEngine, RaftLogBatch, CF_RAFT};
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
                raft_wb.put_region_apply_snapshot_state(region_id, &region_state, &apply_state).unwrap();
                applying_count += 1;
            }
            PeerState::Tombstone => {
                tombstone_count += 1;
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

// pub fn migrate_states_from_raftdb_to_kvdb() {}
