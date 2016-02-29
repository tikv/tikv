use std::vec::Vec;

pub mod errors;
pub use self::errors::{Result, Error};

use proto::metapb;

pub type Key = Vec<u8>;

// Client to communicate with placement driver (pd).
pub trait Client {
    // Create the cluster with cluster ID, node, stores and first region.
    // If the cluster is already bootstrapped, return ClusterBootstrapped error.
    // When a node starts, if it finds nothing in the node and
    // cluster is not bootstrapped, it begins to create node, stores, first region
    // and then call bootstrap_cluster to let pd know it.
    // It may happen that multi nodes start at same time to try to
    // bootstrap, and only one can success, others will fail
    // and must remove their created local region data themselves.
    fn boostrap_cluster(&mut self,
                        cluster_id: u64,
                        node: metapb::Node,
                        stores: Vec<metapb::Store>,
                        region: metapb::Region)
                        -> Result<()>;

    // Return whether the cluster is bootstrapped or not.
    // We must use the cluster after bootstrapped, so when the
    // node starts, it must check it with is_cluster_bootstrapped,
    // and panic if not bootstrapped.
    fn is_cluster_bootstrapped(&self, cluster_id: u64) -> Result<bool>;

    // Allocate a unique node id.
    fn alloc_node_id(&mut self) -> Result<u64>;

    // Allocate a unique store id.
    fn alloc_store_id(&mut self) -> Result<u64>;

    // Allocate a unique peer id.
    fn alloc_peer_id(&mut self) -> Result<u64>;

    // Allocate a unique region id.
    fn alloc_region_id(&mut self) -> Result<u64>;

    // When the node starts, or some node information changed, it
    // uses put_node to inform pd.
    fn put_node(&mut self, cluster_id: u64, node: metapb::Node) -> Result<()>;

    // When the store starts, or some store information changed, it
    // uses put_store to inform pd.
    fn put_store(&mut self, cluster_id: u64, store: metapb::Store) -> Result<()>;

    // Delete the node from cluster, it is a very dangerous operation
    // and can not be recoverable, all the data belongs to this node
    // will be removed and we can't re-add this node again.
    // Sometimes, the node may crash and restart again, if the node is
    // off-line for a long time, pd will try to do auto-balance and then
    // delete the node.
    fn delete_node(&mut self, cluster_id: u64, node_id: u64) -> Result<()>;

    // Delete the store from cluster, it is a very dangerous operation
    // and can not be recoverable, all the data belongs to this store
    // will be removed and we can't re-add this store again.
    // If the store is off-line for a long time, pd will try to do
    // auto-balance and then delete the store.
    fn delete_store(&mut self, cluster_id: u64, store_id: u64) -> Result<()>;

    // Discuss (@ngaut): We don't need to support region and peer put/delete,
    // because pd knows all region and peers itself.
    // When bootstrapping, pd knows region 1 and peer 1 exists in node 1, store 1.
    // When changing peer, pd determines where to add a new peer in some store
    // for this region.
    // When region splitting, pd determines the new region id and peer id for the
    // split region.
    // When region merging, pd knows which two regions will be merged and which region
    // and peers will be removed.
    // When doing auto-balance, pd determines how to move the region from one store to another.

    // Get node information.
    fn get_node(&self, cluster_id: u64, node_id: u64) -> Result<metapb::Node>;

    // Get store information.
    fn get_store(&self, cluster_id: u64, store_id: u64) -> Result<metapb::Store>;

    // For route.
    // Get all regions which the keys belong to.
    // The returned regions are unique, if multi keys belong to same region, only one returns.
    fn get_regions(&self, cluster_id: u64, keys: Vec<Key>) -> Result<Vec<metapb::Region>>;

    // For route.
    // Scan all regions which the region start key >= given start key.
    fn scan_regions(&self,
                    cluster_id: u64,
                    start_key: Vec<u8>,
                    limit: u32)
                    -> Result<Vec<metapb::Region>>;
}
