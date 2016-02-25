use std::sync::Arc;
use std::vec::Vec;

use rocksdb::DB;

use proto::raft_serverpb::StoreIdent;
use raftserver::{Result, other};
use raftserver::store::{keys, Retriever};

const INVALID_NODE_ID: u64 = 0;

// TODO: use a help Node structure to manage all stores after we introduces pd later.

// Check All the stores bootstrapped already.
// All the stores must be in the same cluster and node.
// Return the corresponding StoreIdent for the Store in DB.
pub fn check_all_bootstrapped(cluster_id: u64, engines: &[Arc<DB>]) -> Result<Vec<StoreIdent>> {
    if engines.is_empty() {
        return Err(other("empty stores to check"));
    }

    let mut idents = vec![];
    let mut node_id = INVALID_NODE_ID;
    for engine in engines {
        let res = try!(engine.get_msg::<StoreIdent>(&keys::store_ident_key()));
        if res.is_none() {
            return Err(other("store must be bootstrapped before"));
        }

        let ident = res.unwrap();
        if ident.get_cluster_id() != cluster_id {
            return Err(other(format!("store ident {:?} has mismatched cluster id with {}",
                                     ident,
                                     cluster_id)));
        }

        if node_id == INVALID_NODE_ID {
            node_id = ident.get_node_id();
        } else if ident.get_node_id() != node_id {
            return Err(other(format!("store ident {:?} has mismatched node id with {}",
                                     ident,
                                     node_id)));
        }

        idents.push(ident);
    }

    if node_id == INVALID_NODE_ID {
        return Err(other("all stores have invalid node id"));
    }

    Ok(idents)
}
