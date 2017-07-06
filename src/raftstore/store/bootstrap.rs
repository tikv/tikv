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

use rocksdb::{DB, Writable, WriteBatch};
use kvproto::raft_serverpb::{StoreIdent, RegionLocalState};
use kvproto::metapb;
use raftstore::Result;
use super::keys;
use super::engine::{Iterable, Mutable};
use super::peer_storage::write_initial_state;
use util::rocksdb;
use storage::{CF_DEFAULT, CF_RAFT};

const INIT_EPOCH_VER: u64 = 1;
const INIT_EPOCH_CONF_VER: u64 = 1;

// check no any data in range [start_key, end_key)
fn is_range_empty(engine: &DB, cf: &str, start_key: &[u8], end_key: &[u8]) -> Result<bool> {
    let mut count: u32 = 0;
    try!(engine.scan_cf(cf,
                        start_key,
                        end_key,
                        false,
                        &mut |_, _| {
                            count += 1;
                            Ok(false)
                        }));

    Ok(count == 0)
}

// Bootstrap the store, the DB for this store must be empty and has no data.
pub fn bootstrap_store(raft_engine: &DB, cluster_id: u64, store_id: u64) -> Result<()> {
    if !try!(is_range_empty(raft_engine, CF_DEFAULT, keys::MIN_KEY, keys::MAX_KEY)) {
        return Err(box_err!("raft store is not empty and has already had data."));
    }

    let ident_key = keys::store_ident_key();
    let mut ident = StoreIdent::new();
    ident.set_cluster_id(cluster_id);
    ident.set_store_id(store_id);

    raft_engine.put_msg(&ident_key, &ident)
}

// Write first region meta and prepare state.
pub fn write_prepare_bootstrap(raft_engine: &DB, region: &metapb::Region) -> Result<()> {
    let mut state = RegionLocalState::new();
    state.set_region(region.clone());

    let wb = WriteBatch::new();
    try!(wb.put_msg(&keys::region_state_key(region.get_id()), &state));
    try!(write_initial_state(raft_engine, &wb, region.get_id()));
    try!(wb.put_msg(&keys::prepare_bootstrap_key(), region));
    try!(raft_engine.write(wb));
    Ok(())
}

// Clear first region meta and prepare state.
pub fn clear_prepare_bootstrap(raft_engine: &DB, region_id: u64) -> Result<()> {
    let wb = WriteBatch::new();

    try!(wb.delete(&keys::region_state_key(region_id)));
    try!(wb.delete(&keys::prepare_bootstrap_key()));
    // should clear raft initial state too.
    let raft_cf = try!(rocksdb::get_cf_handle(raft_engine, CF_RAFT));
    try!(wb.delete_cf(raft_cf, &keys::raft_state_key(region_id)));
    try!(wb.delete_cf(raft_cf, &keys::apply_state_key(region_id)));

    try!(raft_engine.write(wb));
    Ok(())
}

// Clear prepare state
pub fn clear_prepare_bootstrap_state(raft_engine: &DB) -> Result<()> {
    try!(raft_engine.delete(&keys::prepare_bootstrap_key()));
    Ok(())
}

// Prepare bootstrap.
pub fn prepare_bootstrap(raft_engine: &DB,
                         store_id: u64,
                         region_id: u64,
                         peer_id: u64)
                         -> Result<metapb::Region> {
    let mut region = metapb::Region::new();
    region.set_id(region_id);
    region.set_start_key(keys::EMPTY_KEY.to_vec());
    region.set_end_key(keys::EMPTY_KEY.to_vec());
    region.mut_region_epoch().set_version(INIT_EPOCH_VER);
    region.mut_region_epoch().set_conf_ver(INIT_EPOCH_CONF_VER);

    let mut peer = metapb::Peer::new();
    peer.set_store_id(store_id);
    peer.set_id(peer_id);
    region.mut_peers().push(peer);

    try!(write_prepare_bootstrap(raft_engine, &region));

    Ok(region)
}


#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;
    use util::rocksdb;
    use raftstore::store::engine::Peekable;
    use raftstore::store::keys;
    use storage::{CF_DEFAULT, CF_RAFT};

    #[test]
    fn test_bootstrap() {
        let path = TempDir::new("var").unwrap();
        let raft_engine = rocksdb::new_engine(path.path().to_str().unwrap(), &[CF_RAFT]).unwrap();

        assert!(bootstrap_store(&raft_engine, 1, 1).is_ok());
        assert!(bootstrap_store(&raft_engine, 1, 1).is_err());

        assert!(prepare_bootstrap(&raft_engine, 1, 1, 1).is_ok());
        assert!(raft_engine.get_value(&keys::region_state_key(1)).unwrap().is_some());
        assert!(raft_engine.get_value(&keys::prepare_bootstrap_key()).unwrap().is_some());
        assert!(raft_engine.get_value_cf(CF_RAFT, &keys::raft_state_key(1)).unwrap().is_some());
        assert!(raft_engine.get_value_cf(CF_RAFT, &keys::apply_state_key(1)).unwrap().is_some());

        assert!(clear_prepare_bootstrap_state(&raft_engine).is_ok());
        assert!(clear_prepare_bootstrap(&raft_engine, 1).is_ok());
        assert!(is_range_empty(&raft_engine,
                               CF_DEFAULT,
                               &keys::region_meta_prefix(1),
                               &keys::region_meta_prefix(2))
            .unwrap());
        assert!(is_range_empty(&engine,
                               CF_RAFT,
                               &keys::region_raft_prefix(1),
                               &keys::region_raft_prefix(2))
            .unwrap());
    }
}
