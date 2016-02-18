use std::boxed::Box;
use std::error;
use std::option::Option;

use uuid::Uuid;

use proto::raft_cmdpb::RaftCommandResponse;
use proto::metapb;
use proto::errorpb::{self, ErrorDetail};

// TODO: use quick_error for store so that we can use RaftCommandResponse
// in Result error.

pub fn region_not_found_error(region_id: u64) -> RaftCommandResponse {
    let mut detail = ErrorDetail::new();
    detail.mut_region_not_found().set_region_id(region_id);
    detail_error("region is not found", detail)
}

pub fn not_leader_error(region_id: u64, leader: Option<metapb::Peer>) -> RaftCommandResponse {
    let mut detail = ErrorDetail::new();
    if let Some(leader) = leader {
        detail.mut_not_leader().set_leader(leader);
    }
    detail.mut_not_leader().set_region_id(region_id);
    detail_error("peer is not leader", detail)
}

pub fn message_error<E>(err: E) -> RaftCommandResponse
    where E: Into<Box<error::Error + Send + Sync>>
{
    new_error(err.into(), None)
}

pub fn detail_error<E>(err: E, detail: ErrorDetail) -> RaftCommandResponse
    where E: Into<Box<error::Error + Send + Sync>>
{
    new_error(err.into(), Some(detail))
}

fn new_error(err: Box<error::Error + Send + Sync>,
             detail: Option<ErrorDetail>)
             -> RaftCommandResponse {
    let mut msg = RaftCommandResponse::new();
    let mut error_header = errorpb::Error::new();
    error_header.set_message(format!("{:?}", err));
    if let Some(detail) = detail {
        error_header.set_detail(detail)
    }
    msg.mut_header().set_error(error_header);

    msg
}

pub fn bind_uuid(resp: &mut RaftCommandResponse, uuid: Uuid) {
    resp.mut_header().set_uuid(uuid.as_bytes().to_vec());
}
