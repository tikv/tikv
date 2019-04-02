// Copyright 2018 TiKV Project Authors.
use kvproto::pdpb::*;

use super::*;

#[derive(Debug)]
pub struct Incompatible;

impl PdMocker for Incompatible {
    fn ask_batch_split(&self, _: &AskBatchSplitRequest) -> Option<Result<AskBatchSplitResponse>> {
        let mut err = Error::new();
        err.set_field_type(ErrorType::INCOMPATIBLE_VERSION);

        let mut header = ResponseHeader::new();
        header.set_error(err);

        let mut resp = AskBatchSplitResponse::new();
        resp.set_header(header);

        Some(Ok(resp))
    }
}
