use kvproto::metapb::{Peer as PeerMeta, Region};
use raft::eraftpb::ConfChange;

#[derive(Default, Debug)]
pub struct ChangePeer {
    pub conf_change: ConfChange,
    pub peer: PeerMeta,
    pub region: Region,
}

#[derive(Debug)]
pub struct Range {
    pub cf: String,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
}

impl Range {
    pub fn new(cf: String, start_key: Vec<u8>, end_key: Vec<u8>) -> Range {
        Range {
            cf,
            start_key,
            end_key,
        }
    }
}
