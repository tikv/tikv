// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::{
    metapb,
    raft_cmdpb::{CmdType, RaftCmdRequest, Request},
};
use tikv_util::store::new_peer;

pub(crate) fn new_snap_request(
    store_id: u64,
    peer_id: u64,
    term: u64,
    mut region: metapb::Region,
) -> RaftCmdRequest {
    let mut req = RaftCmdRequest::default();
    req.mut_header().set_peer(new_peer(store_id, peer_id));
    req.mut_header().set_term(term);
    req.mut_header().set_region_id(region.id);
    req.mut_header()
        .set_region_epoch(region.take_region_epoch());
    let mut request_inner = Request::default();
    request_inner.set_cmd_type(CmdType::Snap);
    req.mut_requests().push(request_inner);
    req
}
