// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use super::keys;
use super::peer_storage::{
    write_initial_apply_state, write_initial_raft_state, INIT_EPOCH_CONF_VER, INIT_EPOCH_VER,
};
use super::util::new_peer;
use crate::raftstore::Result;
use engine::rocks::Writable;
use engine::CF_DEFAULT;
use engine::{Engines, Iterable, Mutable, WriteBatch, DB};

use kvproto::metapb;
use kvproto::raft_serverpb::{RegionLocalState, StoreIdent};

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

/// Bootstrap the store, the DB for this store must be empty and has no data.
pub fn bootstrap_store(engines: &Engines, cluster_id: u64, store_id: u64) -> Result<()> {
    let mut ident = StoreIdent::new();

    if !is_range_empty(&engines.kv, CF_DEFAULT, keys::MIN_KEY, keys::MAX_KEY)? {
        return Err(box_err!("kv engine is not empty"));
    }

    if !is_range_empty(&engines.raft, CF_DEFAULT, keys::MIN_KEY, keys::MAX_KEY)? {
        return Err(box_err!("raft engine is not empty"));
    }

    ident.set_cluster_id(cluster_id);
    ident.set_store_id(store_id);

    box_try!(engines.raft.put_msg(keys::STORE_IDENT_KEY, &ident));
    engines.sync_raft()?;
    Ok(())
}

/// The first phase of bootstrapping cluster.
///
/// Write the first region meta and prepare state.
pub fn prepare_bootstrap_cluster(engines: &Engines, region: &metapb::Region) -> Result<()> {
    let mut state = RegionLocalState::new();
    state.set_region(region.clone());

    let raft_wb = WriteBatch::new();
    raft_wb.put_msg(keys::PREPARE_BOOTSTRAP_KEY, region)?;
    raft_wb.put_msg(&keys::region_state_key(region.get_id()), &state)?;
    write_initial_apply_state(&raft_wb, region.get_id())?;
    write_initial_raft_state(&raft_wb, region.get_id())?;
    engines.write_raft(&raft_wb)?;
    engines.sync_raft()?;
    Ok(())
}

// Clear first region meta and prepare key for bootstrapping cluster.
pub fn clear_prepare_bootstrap_cluster(engines: &Engines, region_id: u64) -> Result<()> {
    let wb = WriteBatch::new();
    box_try!(wb.delete(keys::PREPARE_BOOTSTRAP_KEY));
    // Clear raft initial state too.
    box_try!(wb.delete(&keys::raft_state_key(region_id)));
    box_try!(wb.delete(&keys::region_state_key(region_id)));
    box_try!(wb.delete(&keys::apply_state_key(region_id)));
    engines.write_raft(&wb)?;
    engines.sync_raft()?;
    Ok(())
}

// Clear prepare key
pub fn clear_prepare_bootstrap_key(engines: &Engines) -> Result<()> {
    box_try!(engines.raft.delete(keys::PREPARE_BOOTSTRAP_KEY));
    engines.sync_raft()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tempdir::TempDir;

    use super::*;
    use crate::raftstore::store::keys;
    use engine::rocks;
    use engine::Engines;
    use engine::Peekable;
    use engine::CF_DEFAULT;

    #[test]
    fn test_bootstrap() {
        let path = TempDir::new("var").unwrap();
        let raft_path = path.path().join("raft");
        let kv_engine = Arc::new(
            rocks::util::new_engine(path.path().to_str().unwrap(), None, &[CF_DEFAULT], None)
                .unwrap(),
        );
        let raft_engine = Arc::new(
            rocks::util::new_engine(raft_path.to_str().unwrap(), None, &[CF_DEFAULT], None)
                .unwrap(),
        );
        let region = initial_region(1, 1, 1);
        let shared_block_cache = false;
        let engines = Engines::new(kv_engine, raft_engine, shared_block_cache);

        assert!(bootstrap_store(&engines, 1, 1).is_ok());
        assert!(bootstrap_store(&engines, 1, 1).is_err());

        assert!(prepare_bootstrap_cluster(&engines, &region).is_ok());
        assert!(engines
            .raft
            .get_value(keys::PREPARE_BOOTSTRAP_KEY)
            .unwrap()
            .is_some());
        assert!(engines
            .raft
            .get_value(&keys::region_state_key(1))
            .unwrap()
            .is_some());
        assert!(engines
            .raft
            .get_value(&keys::apply_state_key(1))
            .unwrap()
            .is_some());
        assert!(engines
            .raft
            .get_value(&keys::raft_state_key(1))
            .unwrap()
            .is_some());

        assert!(clear_prepare_bootstrap_key(&engines).is_ok());
        assert!(clear_prepare_bootstrap_cluster(&engines, 1).is_ok());
        assert!(is_range_empty(
            &engines.raft,
            CF_DEFAULT,
            &keys::region_meta_prefix(1),
            &keys::region_meta_prefix(2)
        )
        .unwrap());
    }
}
