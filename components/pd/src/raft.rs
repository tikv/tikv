use kvproto::metapb;
use kvproto::pdpb;
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, SplitRequest};
use protobuf::RepeatedField;
use raft::eraftpb::ConfChangeType;

pub fn new_change_peer_request(change_type: ConfChangeType, peer: metapb::Peer) -> AdminRequest {
    let mut req = AdminRequest::new();
    req.set_cmd_type(AdminCmdType::ChangePeer);
    req.mut_change_peer().set_change_type(change_type);
    req.mut_change_peer().set_peer(peer);
    req
}

pub fn new_split_region_request(
    split_key: Vec<u8>,
    new_region_id: u64,
    peer_ids: Vec<u64>,
    right_derive: bool,
) -> AdminRequest {
    let mut req = AdminRequest::new();
    req.set_cmd_type(AdminCmdType::Split);
    req.mut_split().set_split_key(split_key);
    req.mut_split().set_new_region_id(new_region_id);
    req.mut_split().set_new_peer_ids(peer_ids);
    req.mut_split().set_right_derive(right_derive);
    req
}

pub fn new_batch_split_region_request(
    split_keys: Vec<Vec<u8>>,
    ids: Vec<pdpb::SplitID>,
    right_derive: bool,
) -> AdminRequest {
    let mut req = AdminRequest::new();
    req.set_cmd_type(AdminCmdType::BatchSplit);
    req.mut_splits().set_right_derive(right_derive);
    let mut requests = Vec::with_capacity(ids.len());
    for (mut id, key) in ids.into_iter().zip(split_keys) {
        let mut split = SplitRequest::new();
        split.set_split_key(key);
        split.set_new_region_id(id.get_new_region_id());
        split.set_new_peer_ids(id.take_new_peer_ids());
        requests.push(split);
    }
    req.mut_splits()
        .set_requests(RepeatedField::from_vec(requests));
    req
}

pub fn new_transfer_leader_request(peer: metapb::Peer) -> AdminRequest {
    let mut req = AdminRequest::new();
    req.set_cmd_type(AdminCmdType::TransferLeader);
    req.mut_transfer_leader().set_peer(peer);
    req
}

pub fn new_merge_request(merge: pdpb::Merge) -> AdminRequest {
    let mut req = AdminRequest::new();
    req.set_cmd_type(AdminCmdType::PrepareMerge);
    req.mut_prepare_merge()
        .set_target(merge.get_target().to_owned());
    req
}
