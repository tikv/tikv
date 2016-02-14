use std::option::Option;

use proto::raft_serverpb::RaftMessage;
use proto::metapb::Peer;
use raftserver::Result;

// Transports message between different raft peers.
pub trait Transport {
    // Cache peer info for later send use.
    // The peer id is global unique, and belongs to a store
    // which creates it forever, so we can safely use peer_id
    // as the cache key.
    fn cache_peer(&mut self, peer_id: u64, peer: Peer);

    fn get_peer(&self, peer_id: u64) -> Option<Peer>;

    fn send(&self, msg: RaftMessage) -> Result<()>;
}
