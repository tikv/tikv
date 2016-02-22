use proto::raft_serverpb::RaftMessage;
use raftserver::Result;

// Transports message between different raft peers.
pub trait Transport {
    fn send(&self, msg: RaftMessage) -> Result<()>;
}
