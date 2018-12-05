// Copyright 2018 PingCAP, Inc.
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
