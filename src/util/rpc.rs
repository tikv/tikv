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

use std::mem;

use grpc::{Metadata, MetadataBuilder, RpcStatus, RpcStatusCode};

macro_rules! send_response {
    ($ctx:expr, $sink:expr, $resp:expr, $map_err:expr, $tag:expr) => ({
        let f = $resp.then(|v| match v {
            Ok(resp) => $sink.success(resp),
            Err(e) => $sink.fail($map_err(e)),
        });
        $ctx.spawn(f.map_err(move |e| error!("{} failed: {:?}", $tag, e)));
    });

    ($ctx:expr, $sink:expr, $err_status:expr, $tag:expr) => ({
        let f = $sink.fail($err_status);
        $ctx.spawn(f.map_err(move |e| error!("{} failed: {:?}", $tag, e)));
    })
}

macro_rules! try_check_header {
    ($ctx:expr, $sink:expr, $cluster_id:expr, $tag:expr) => ({
        if let Err(e) = $crate::util::rpc::check_header(
                $ctx.request_headers(), $cluster_id) {
            send_response!($ctx, $sink, e.into(), $tag);
            return;
        }
    })
}

const CLUSTER_ID: &str = "cluster-id-bin";

pub fn make_header(cluster_id: u64) -> Metadata {
    let mut b = MetadataBuilder::with_capacity(1);
    b.add_bytes(CLUSTER_ID, unsafe {
        &mem::transmute::<_, [u8; 8]>(u64::to_be(cluster_id))
    }).unwrap();

    b.build()
}

pub fn check_header(header: &Metadata, cluster_id: u64) -> Result<(), Error> {
    if let Some((_, id)) = header.iter().find(|&(k, _)| k == CLUSTER_ID) {
        if id.len() != 8 {
            return Err(Error::Invalid(format!("cluster id {:?}", id)));
        }
        let mut array = [0; 8];
        array.copy_from_slice(id);
        let from = u64::from_be(unsafe { mem::transmute::<_, u64>(array) });
        if from == cluster_id {
            return Ok(());
        } else {
            return Err(Error::ClusterId(from));
        }
    }
    Ok(())
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        ClusterId(id: u64) {
            description("cluster id mismatch")
            display("cluster id mismatch: {}", id)
        }
        Invalid(reason: String) {
            description("Invalid header")
            display("Invalid: {}", reason)
        }
    }
}

impl Into<RpcStatus> for Error {
    fn into(self) -> RpcStatus {
        let msg = Some(format!("{:?}", self));
        RpcStatus::new(RpcStatusCode::Unauthenticated, msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bad_header(junk: &[u8]) -> Metadata {
        let mut b = MetadataBuilder::with_capacity(1);
        b.add_bytes(CLUSTER_ID, junk).unwrap();
        b.build()
    }

    #[test]
    fn test_header() {
        let header = make_header(42);
        check_header(&header, 42).unwrap();
        check_header(&header, 41).unwrap_err();

        let bad_short = bad_header(&[1; 7]);
        check_header(&bad_short, 42).unwrap_err();
        let bad_long = bad_header(&[1; 9]);
        check_header(&bad_long, 42).unwrap_err();
    }
}
