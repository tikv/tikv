use std::boxed::Box;
use std::error;

use uuid::Uuid;

use kvproto::raft_cmdpb::RaftCmdResponse;
use kvproto::errorpb;
use raftstore::Error;

pub fn bind_uuid(resp: &mut RaftCmdResponse, uuid: Uuid) {
    resp.mut_header().set_uuid(uuid.as_bytes().to_vec());
}

pub fn bind_term(resp: &mut RaftCmdResponse, term: u64) {
    if term == 0 {
        return;
    }

    resp.mut_header().set_current_term(term);
}

pub fn bind_error(resp: &mut RaftCmdResponse, err: Error) {
    let mut error_header = errorpb::Error::new();

    error_header.set_message(error::Error::description(&err).to_owned());

    match err {
        Error::RegionNotFound(region_id) => {
            error_header.mut_region_not_found().set_region_id(region_id);
        }
        Error::NotLeader(region_id, leader) => {
            if let Some(leader) = leader {
                error_header.mut_not_leader().set_leader(leader);
            }
            error_header.mut_not_leader().set_region_id(region_id);
        }
        Error::KeyNotInRegion(key, region) => {
            error_header.mut_key_not_in_region().set_key(key);
            error_header.mut_key_not_in_region().set_region_id(region.get_id());
            error_header.mut_key_not_in_region().set_start_key(region.get_start_key().to_vec());
            error_header.mut_key_not_in_region().set_end_key(region.get_end_key().to_vec());
        }
        Error::StaleEpoch(_) => {
            error_header.set_stale_epoch(errorpb::StaleEpoch::new());
        }
        _ => {}
    };

    resp.mut_header().set_error(error_header);
}

pub fn new_error(err: Error) -> RaftCmdResponse {
    let mut resp = RaftCmdResponse::new();
    bind_error(&mut resp, err);
    resp
}

pub fn message_error<E>(err: E) -> RaftCmdResponse
    where E: Into<Box<error::Error + Send + Sync>>
{
    new_error(Error::Other(err.into()))
}
