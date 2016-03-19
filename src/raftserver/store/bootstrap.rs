
use std::sync::Arc;

use rocksdb::{DB, Writable};
use kvproto::raft_serverpb::StoreIdent;
use kvproto::metapb;
use raftserver::{Result, other};
use super::keys;
use super::engine::{Iterable, Mutable};

// Bootstrap the store, the DB for this store must be empty and has no data.
pub fn bootstrap_store(engine: Arc<DB>,
                       cluster_id: u64,
                       node_id: u64,
                       store_id: u64)
                       -> Result<()> {
    let mut ident = StoreIdent::new();

    let mut count: u32 = 0;
    try!(engine.scan(keys::MIN_KEY,
                     keys::MAX_KEY,
                     &mut |_, _| -> Result<(bool)> {
                         count += 1;
                         Ok(false)
                     }));

    if count > 0 {
        return Err(other("store is not empty and has already had data."));
    }

    let ident_key = keys::store_ident_key();

    ident.set_cluster_id(cluster_id);
    ident.set_node_id(node_id);
    ident.set_store_id(store_id);

    engine.put_msg(&ident_key, &ident)
}

// Write first region meta.
pub fn write_region(engine: &DB, region: &metapb::Region) -> Result<()> {
    try!(engine.put_msg(&keys::region_info_key(region.get_region_id()), region));
    Ok(())
}

// Clear first region meta.
pub fn clear_region(engine: &DB, region_id: u64) -> Result<()> {
    try!(engine.delete(&keys::region_info_key(region_id)));
    Ok(())
}

// Bootstrap first region.
pub fn bootstrap_region(engine: Arc<DB>,
                        node_id: u64,
                        store_id: u64,
                        region_id: u64,
                        peer_id: u64)
                        -> Result<metapb::Region> {
    let mut region = metapb::Region::new();
    region.set_region_id(region_id);
    region.set_start_key(keys::EMPTY_KEY.to_vec());
    region.set_end_key(keys::EMPTY_KEY.to_vec());
    region.mut_region_epoch().set_version(1);
    region.mut_region_epoch().set_conf_ver(1);

    let mut peer = metapb::Peer::new();
    peer.set_node_id(node_id);
    peer.set_store_id(store_id);
    peer.set_peer_id(peer_id);
    region.mut_peers().push(peer);

    try!(write_region(&engine, &region));

    Ok(region)
}
