
use std::sync::Arc;

use rocksdb::DB;
use proto::raft_serverpb::StoreIdent;
use proto::metapb;
use raftserver::Result;
use super::keys;
use super::engine::Mutator;

// TODO: implement bootstrap later.
// 1. bootstrap store.
// 2. bootstrap region 1.

pub fn bootstrap_store(engine: Arc<DB>, cluster_id: u64, meta: metapb::Store) -> Result<()> {
    let mut ident = StoreIdent::new();
    ident.set_cluster_id(cluster_id);
    ident.set_node_id(meta.get_node().get_node_id());
    ident.set_store_id(meta.get_store_id());

    engine.put_msg(&keys::store_ident_key(), &ident)
}
