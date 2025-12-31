// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::pdpb::*;

use super::*;

#[derive(Debug)]
pub struct Incompatible;

impl PdMocker for Incompatible {
    fn ask_batch_split(&self, _: &AskBatchSplitRequest) -> Option<Result<AskBatchSplitResponse>> {
        let mut err = Error::default();
        err.set_type(ErrorType::IncompatibleVersion);

        let mut header = ResponseHeader::default();
        header.set_error(err);

        let mut resp = AskBatchSplitResponse::default();
        resp.set_header(header);

        Some(Ok(resp))
    }
}
