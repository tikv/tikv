use std::option::Option;

use proto::raft_serverpb::RaftMessage;
use proto::metapb::Peer;
use raftserver::Result;

// Transports message between different raft peers.
pub trait Transport {
    // TODO: depreciate it later.
    fn cache_peer(&mut self, peer_id: u64, peer: Peer);

    // TODO: depreciate it later.
    fn get_peer(&self, peer_id: u64) -> Option<Peer>;

    fn send(&self, msg: RaftMessage) -> Result<()>;
}
