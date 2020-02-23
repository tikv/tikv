// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::error;

use crate::Error;
use kvproto::raft_cmdpb::RaftCmdResponse;

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
    let mut resp = RaftCmdResponse::default();
    bind_error(&mut resp, err);
    resp
}

pub fn err_resp(e: Error, term: u64) -> RaftCmdResponse {
    let mut resp = new_error(e);
    bind_term(&mut resp, term);
    resp
}

pub fn message_error<E>(err: E) -> RaftCmdResponse
where
    E: Into<Box<dyn error::Error + Send + Sync>>,
{
    new_error(Error::Other(err.into()))
}
