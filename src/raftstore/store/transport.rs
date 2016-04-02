use kvproto::raft_serverpb::RaftMessage;

use raftstore::Result;

// Transports message between different raft peers.
pub trait Transport : Send + Sync {
    fn send(&self, msg: RaftMessage) -> Result<()>;
}
