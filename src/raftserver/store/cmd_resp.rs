use std::boxed::Box;
use std::error;
use std::option::Option;

use proto::raft_cmdpb::RaftCommandResponse;
use proto::errorpb::ErrorDetail;
use proto::metapb;

pub fn region_not_found_error(region_id: u64) -> RaftCommandResponse {
    let mut detail = ErrorDetail::new();
    {
        let mut msg = detail.mut_region_not_found();
        msg.set_region_id(region_id);
    }
    detail_error("region is not found", detail)
}

pub fn not_leader_error(region_id: u64, leader: Option<metapb::Peer>) -> RaftCommandResponse {
    let mut detail = ErrorDetail::new();
    {
        let mut msg = detail.mut_not_leader();
        if let Some(leader) = leader {
            msg.set_leader(leader);
        }
        msg.set_region_id(region_id);
    }
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
    {
        let mut error_header = msg.mut_header().mut_error();
        error_header.set_message(format!("{:?}", err));
        if let Some(detail) = detail {
            error_header.set_detail(detail)
        }
    }

    msg
}
