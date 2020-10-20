// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::pdpb::*;

use super::*;

#[derive(Debug)]
pub struct AlreadyBootstrapped;

impl PdMocker for AlreadyBootstrapped {
    fn bootstrap(&self, _: &BootstrapRequest) -> Option<Result<BootstrapResponse>> {
        let mut err = Error::default();
        err.set_type(ErrorType::AlreadyBootstrapped);
        err.set_message("cluster is already bootstrapped".to_owned());

        let mut header = ResponseHeader::default();
        header.set_error(err);
        header.set_cluster_id(DEFAULT_CLUSTER_ID);

        let mut resp = BootstrapResponse::default();
        resp.set_header(header);

        Some(Ok(resp))
    }

    fn is_bootstrapped(&self, _: &IsBootstrappedRequest) -> Option<Result<IsBootstrappedResponse>> {
        let mut header = ResponseHeader::default();
        header.set_cluster_id(DEFAULT_CLUSTER_ID);

        let mut resp = IsBootstrappedResponse::default();
        resp.set_bootstrapped(false);

        Some(Ok(resp))
    }
}
