use std::boxed::Box;
use std::error;

use uuid::Uuid;

use proto::raft_cmdpb::RaftCommandResponse;
use proto::errorpb::{self, ErrorDetail};
use raftserver::Error;

pub fn bind_uuid(resp: &mut RaftCommandResponse, uuid: Uuid) {
    resp.mut_header().set_uuid(uuid.as_bytes().to_vec());
}

pub fn new_error(err: Error) -> RaftCommandResponse {
    let mut msg = RaftCommandResponse::new();
    let mut error_header = errorpb::Error::new();

    error_header.set_message(error::Error::description(&err).to_owned());

    let detail = match err {
        Error::RegionNotFound(region_id) => {
            let mut detail = ErrorDetail::new();
            detail.mut_region_not_found().set_region_id(region_id);
            Some(detail)
        }
        Error::NotLeader(region_id, leader) => {
            let mut detail = ErrorDetail::new();
            if let Some(leader) = leader {
                detail.mut_not_leader().set_leader(leader);
            }
            detail.mut_not_leader().set_region_id(region_id);
            Some(detail)
        }
        _ => None,
    };

    if let Some(detail) = detail {
        error_header.set_detail(detail);
    }

    msg.mut_header().set_error(error_header);

    msg
}

pub fn message_error<E>(err: E) -> RaftCommandResponse
    where E: Into<Box<error::Error + Send + Sync>>
{
    new_error(Error::Other(err.into()))
}
