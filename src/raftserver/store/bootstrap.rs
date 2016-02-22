
use std::sync::Arc;

use rocksdb::{DB, WriteBatch};
use proto::raft_serverpb::StoreIdent;
use proto::metapb;
use raftserver::{Result, other};
use super::keys;
use super::engine::{Retriever, Mutator};

// How to construct cluster?
//
// Use bootstrap_cluster to bootstrap cluster first, this function will create
// node 1 and store 1 for this cluster.
// When we start store, we must first check whether the store is bootstrapped or not
// using is_store_bootstrapped, if not, we must call bootstrap_store to bootstrap it first.

pub const BOOTSTRAP_FIRST_NODE_ID: u64 = 1;
pub const BOOTSTRAP_FIRST_STORE_ID: u64 = 1;
pub const BOOTSTRAP_FIRST_REGION_ID: u64 = 1;
pub const BOOTSTRAP_FIRST_PEER_ID: u64 = 1;

pub fn is_store_bootstrapped(engine: Arc<DB>, cluster_id: u64) -> Result<bool> {
    let res = try!(engine.get_msg::<StoreIdent>(&keys::store_ident_key()));
    match res {
        None => Ok(false),
        Some(ident) => {
            if ident.get_cluster_id() != cluster_id {
                return Err(other(format!("store is bootstrapped for cluster {} already",
                                         ident.get_cluster_id())));
            }
            Ok(true)
        }
    }
}

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

pub fn write_first_region(engine: &DB, region: &metapb::Region) -> Result<()> {
    let batch = WriteBatch::new();
    try!(batch.put_msg(&keys::region_info_key(region.get_start_key()), region));

    let meta2_key = keys::region_route_meta_key(region.get_end_key());
    try!(batch.put_msg(&meta2_key, region));

    let meta1_key = keys::region_route_meta_key(&meta2_key);
    try!(batch.put_msg(&meta1_key, region));

    try!(engine.write(batch));
    Ok(())
}

// Bootstrap first region, the region id must be 1 and start/end key is
// min_key/max_key. The first peer id is 1 too.
pub fn bootstrap_region(engine: Arc<DB>) -> Result<metapb::Region> {
    let mut region = metapb::Region::new();
    region.set_region_id(BOOTSTRAP_FIRST_REGION_ID);
    region.set_start_key(keys::MIN_KEY.to_vec());
    region.set_end_key(keys::MAX_KEY.to_vec());
    region.set_max_peer_id(BOOTSTRAP_FIRST_REGION_ID);

    let mut peer = metapb::Peer::new();
    peer.set_node_id(BOOTSTRAP_FIRST_NODE_ID);
    peer.set_store_id(BOOTSTRAP_FIRST_STORE_ID);
    peer.set_peer_id(BOOTSTRAP_FIRST_PEER_ID);
    region.mut_peers().push(peer);

    try!(write_first_region(&engine, &region));

    Ok(region)
}

// Bootstrap cluster, we must bootstrap the first store with node/store id both are 1,
// and first region.
pub fn bootstrap_cluster(engine: Arc<DB>, cluster_id: u64) -> Result<()> {
    try!(bootstrap_store(engine.clone(),
                         cluster_id,
                         BOOTSTRAP_FIRST_NODE_ID,
                         BOOTSTRAP_FIRST_STORE_ID));
    try!(bootstrap_region(engine.clone()));
    Ok(())
}
