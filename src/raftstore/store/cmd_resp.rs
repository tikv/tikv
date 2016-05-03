// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

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
        Error::NotLeader(region_id, leader_store_id) => {
            if let Some(leader_store_id) = leader_store_id {
                error_header.mut_not_leader().set_leader_store_id(leader_store_id);
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

pub fn err_resp(e: Error, uuid: Uuid, term: u64) -> RaftCmdResponse {
    let mut resp = new_error(e);
    bind_term(&mut resp, term);
    bind_uuid(&mut resp, uuid);
    resp
}

pub fn message_error<E>(err: E) -> RaftCmdResponse
    where E: Into<Box<error::Error + Send + Sync>>
{
    new_error(Error::Other(err.into()))
}
