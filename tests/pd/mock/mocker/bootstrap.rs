// Copyright 2017 PingCAP, Inc.
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

use super::Mocker;
use super::Result;
use super::DEFAULT_CLUSTER_ID;

#[derive(Debug)]
pub struct AlreadyBootstrap {}

impl AlreadyBootstrap {
    pub fn new() -> AlreadyBootstrap {
        AlreadyBootstrap {}
    }
}

impl Mocker for AlreadyBootstrap {
    fn bootstrap(&self, _: &BootstrapRequest) -> Option<Result<BootstrapResponse>> {
        let mut err = Error::new();
        err.set_field_type(ErrorType::ALREADY_BOOTSTRAPPED);
        err.set_message("cluster is already bootstrapped".to_owned());

        let mut header = ResponseHeader::new();
        header.set_error(err);
        header.set_cluster_id(DEFAULT_CLUSTER_ID);

        let mut resp = BootstrapResponse::new();
        resp.set_header(header);

        Some(Ok(resp))
    }

    fn is_bootstrapped(&self, _: &IsBootstrappedRequest) -> Option<Result<IsBootstrappedResponse>> {
        let mut header = ResponseHeader::new();
        header.set_cluster_id(DEFAULT_CLUSTER_ID);

        let mut resp = IsBootstrappedResponse::new();
        resp.set_bootstrapped(false);

        Some(Ok(resp))
    }
}
