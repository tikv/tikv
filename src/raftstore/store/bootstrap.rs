// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use super::engine::{Iterable, Mutable};
use super::keys;
use super::peer_storage::{write_initial_apply_state, write_initial_raft_state};
use super::util::{new_peer, Engines};
use crate::raftengine::LogBatch;
use crate::raftstore::Result;
use crate::storage::{CF_DEFAULT, CF_RAFT};
use crate::util::rocksdb_util;
use kvproto::metapb;
use kvproto::raft_serverpb::{RegionLocalState, StoreIdent};
use rocksdb::{Writable, WriteBatch, DB};

/// The initial region epoch version.
pub const INIT_EPOCH_VER: u64 = 1;
/// The initial region epoch conf_version.
pub const INIT_EPOCH_CONF_VER: u64 = 1;

pub fn initial_region(store_id: u64, region_id: u64, peer_id: u64) -> metapb::Region {
    let mut region = metapb::Region::new();
    region.set_id(region_id);
    region.set_start_key(keys::EMPTY_KEY.to_vec());
    region.set_end_key(keys::EMPTY_KEY.to_vec());
    region.mut_region_epoch().set_version(INIT_EPOCH_VER);
    region.mut_region_epoch().set_conf_ver(INIT_EPOCH_CONF_VER);
    region.mut_peers().push(new_peer(store_id, peer_id));
    region
}

// check no any data in range [start_key, end_key)
fn is_range_empty(engine: &DB, cf: &str, start_key: &[u8], end_key: &[u8]) -> Result<bool> {
    let mut count: u32 = 0;
    engine.scan_cf(cf, start_key, end_key, false, |_, _| {
        count += 1;
        Ok(false)
    })?;

    Ok(count == 0)
}

// Bootstrap the store, the DB for this store must be empty and has no data.
pub fn bootstrap_store(engines: &Engines, cluster_id: u64, store_id: u64) -> Result<()> {
    let mut ident = StoreIdent::new();

    if !is_range_empty(&engines.kv, CF_DEFAULT, keys::MIN_KEY, keys::MAX_KEY)? {
        return Err(box_err!("kv store is not empty and has already had data."));
    }

    if !engines.raft.is_empty() {
        return Err(box_err!(
            "raft store is not empty and has already had data."
        ));
    }

    ident.set_cluster_id(cluster_id);
    ident.set_store_id(store_id);

    engines.kv.put_msg(keys::STORE_IDENT_KEY, &ident)?;
    engines.kv.sync_wal()?;
    Ok(())
}

/// The first phase of bootstrap cluster
///
/// Write the first region meta and prepare state.
pub fn prepare_bootstrap_cluster(engines: &Engines, region: &metapb::Region) -> Result<()> {
    let mut state = RegionLocalState::new();
    state.set_region(region.clone());

    let wb = WriteBatch::new();
    wb.put_msg(keys::PREPARE_BOOTSTRAP_KEY, region)?;
    let handle = rocksdb_util::get_cf_handle(&engines.kv, CF_RAFT)?;
    wb.put_msg_cf(handle, &keys::region_state_key(region.get_id()), &state)?;
    write_initial_apply_state(&engines.kv, &wb, region.get_id())?;
    engines.kv.write(wb)?;
    engines.kv.sync_wal()?;

    let raft_wb = LogBatch::new();
    write_initial_raft_state(&raft_wb, region.get_id())?;
    engines.raft.write(raft_wb, true)?;
    Ok(())
}

// Clear first region meta and prepare key.
pub fn clear_prepare_bootstrap_cluster(engines: &Engines, region_id: u64) -> Result<()> {
    engines.raft.clean_region(region_id)?;

    let wb = WriteBatch::new();
    wb.delete(keys::PREPARE_BOOTSTRAP_KEY)?;
    // should clear raft initial state too.
    let handle = rocksdb_util::get_cf_handle(&engines.kv, CF_RAFT)?;
    wb.delete_cf(handle, &keys::region_state_key(region_id))?;
    wb.delete_cf(handle, &keys::apply_state_key(region_id))?;
    engines.kv.write(wb)?;
    engines.kv.sync_wal()?;
    Ok(())
}

// Clear prepare key
pub fn clear_prepare_bootstrap_key(engines: &Engines) -> Result<()> {
    engines.kv.delete(keys::PREPARE_BOOTSTRAP_KEY)?;
    engines.kv.sync_wal()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;
    use tempdir::TempDir;

    use super::*;
    use crate::raftengine::{Config as RaftEngineCfg, RaftEngine};
    use crate::raftstore::store::engine::Peekable;
    use crate::raftstore::store::{keys, Engines};
    use crate::storage::CF_DEFAULT;
    use crate::util::rocksdb_util;

    #[test]
    fn test_bootstrap() {
        let path = TempDir::new("var").unwrap();
        let raft_path = path.path().join(Path::new("raft"));
        let kv_engine = Arc::new(
            rocksdb_util::new_engine(
                path.path().to_str().unwrap(),
                None,
                &[CF_DEFAULT, CF_RAFT],
                None,
            )
            .unwrap(),
        );
        let mut raft_cfg = RaftEngineCfg::new();
        raft_cfg.dir = String::from(raft_path.to_str().unwrap());
        let raft_engine = Arc::new(RaftEngine::new(raft_cfg));
        let engines = Engines::new(Arc::clone(&kv_engine), Arc::clone(&raft_engine));
        let region = initial_region(1, 1, 1);

        assert!(bootstrap_store(&engines, 1, 1).is_ok());
        assert!(bootstrap_store(&engines, 1, 1).is_err());

        assert!(prepare_bootstrap_cluster(&engines, &region).is_ok());
        assert!(kv_engine
            .get_value(keys::PREPARE_BOOTSTRAP_KEY)
            .unwrap()
            .is_some());
        assert!(kv_engine
            .get_value_cf(CF_RAFT, &keys::region_state_key(1))
            .unwrap()
            .is_some());
        assert!(kv_engine
            .get_value_cf(CF_RAFT, &keys::apply_state_key(1))
            .unwrap()
            .is_some());
        assert!(raft_engine
            .get(1, &keys::raft_state_key(1))
            .unwrap()
            .is_some());

        assert!(clear_prepare_bootstrap_key(&engines).is_ok());
        assert!(clear_prepare_bootstrap_cluster(&engines, 1).is_ok());
        assert!(is_range_empty(
            &kv_engine,
            CF_RAFT,
            &keys::region_meta_prefix(1),
            &keys::region_meta_prefix(2)
        )
        .unwrap());

        assert!(raft_engine
            .fetch_all_entries_for_region(1)
            .unwrap()
            .is_empty());
    }
}
