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

use crate::grpc::{RpcContext, RpcStatus, RpcStatusCode, UnarySink};
use futures::Future;

use super::Error;

pub fn make_rpc_error(err: Error) -> RpcStatus {
    RpcStatus::new(RpcStatusCode::Unknown, Some(format!("{:?}", err)))
}

pub fn send_rpc_error<M, E>(ctx: RpcContext, sink: UnarySink<M>, error: E)
where
    Error: From<E>,
{
    let err = make_rpc_error(Error::from(error));
    ctx.spawn(sink.fail(err).map_err(|e| {
        warn!("send rpc failed"; "err" => %e);
    }));
}

macro_rules! send_rpc_response {
    ($res:ident, $sink:ident, $label:ident, $timer:ident) => {{
        let res = match $res {
            Ok(resp) => {
                IMPORT_RPC_DURATION
                    .with_label_values(&[$label, "ok"])
                    .observe($timer.elapsed_secs());
                $sink.success(resp)
            }
            Err(e) => {
                IMPORT_RPC_DURATION
                    .with_label_values(&[$label, "error"])
                    .observe($timer.elapsed_secs());
                $sink.fail(make_rpc_error(e))
            }
        };
        res.map_err(|e| warn!("send rpc response"; "err" => %e))
    }};
}
