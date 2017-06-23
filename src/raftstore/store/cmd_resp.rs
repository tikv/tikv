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

use kvproto::raft_cmdpb::RaftCmdResponse;
use raftstore::Error;

pub fn bind_term(resp: &mut RaftCmdResponse, term: u64) {
    if term == 0 {
        return;
    }

    resp.mut_header().set_current_term(term);
}

pub fn bind_error(resp: &mut RaftCmdResponse, err: Error) {
    resp.mut_header().set_error(err.into());
}

pub fn new_error(err: Error) -> RaftCmdResponse {
    let mut resp = RaftCmdResponse::new();
    bind_error(&mut resp, err);
    resp
}

pub fn err_resp(e: Error, term: u64) -> RaftCmdResponse {
    let mut resp = new_error(e);
    bind_term(&mut resp, term);
    resp
}

pub fn message_error<E>(err: E) -> RaftCmdResponse
    where E: Into<Box<error::Error + Send + Sync>>
{
    new_error(Error::Other(err.into()))
}
