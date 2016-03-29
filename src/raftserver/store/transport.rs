use kvproto::raft_serverpb::RaftMessage;
use kvproto::raft_cmdpb::RaftCmdRequest;

use raftserver::Result;
use super::Callback;
use super::msg::SendCh;

// Transports message between different raft peers.
pub trait Transport {
    // For transporting message with store send channel.
    // TODO: we may remove these to another trait or structure later.
    fn add_sendch(&mut self, store_id: u64, ch: SendCh);
    fn remove_sendch(&mut self, store_id: u64) -> Option<SendCh>;

    // Send RaftMessage to specified store, the store must exist in current node.
    // Unlike  Send, this function can only send message to local store.
    fn send_raft_msg(&self, msg: RaftMessage) -> Result<()>;

    // Send RaftCmdRequest to specified store, the store must exist in current node.
    fn send_command(&self, req: RaftCmdRequest, cb: Callback) -> Result<()>;

    fn send(&self, msg: RaftMessage) -> Result<()>;
}
