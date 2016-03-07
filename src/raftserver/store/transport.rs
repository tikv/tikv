use proto::raft_serverpb::RaftMessage;
use raftserver::Result;
use super::msg::SendCh;

// Transports message between different raft peers.
pub trait Transport {
    // For transporting message with store send channel.
    // TODO: we may remove these to another trait or structure later.
    fn add_sendch(&mut self, store_id: u64, ch: SendCh);
    fn remove_sendch(&mut self, store_id: u64) -> Option<SendCh>;

    fn send(&self, msg: RaftMessage) -> Result<()>;
}
