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


use grpc::{RpcStatus, RpcStatusCode};

use super::Error;

pub fn make_rpc_error(err: Error) -> RpcStatus {
    RpcStatus::new(RpcStatusCode::Unknown, Some(format!("{:?}", err)))
}

macro_rules! send_rpc_response {
    ($res:ident, $sink: ident, $label:ident, $timer:ident) => ({
        let duration = duration_to_sec($timer.elapsed());
        let res = match $res {
            Ok(resp) => {
                IMPORT_RPC_DURATION_VEC
                    .with_label_values(&[$label, "ok"])
                    .observe(duration);
                $sink.success(resp)
            }
            Err(e) => {
                IMPORT_RPC_DURATION_VEC
                    .with_label_values(&[$label, "error"])
                    .observe(duration);
                $sink.fail(make_rpc_error(e))
            }
        };
        res.map_err(|e| warn!("send rpc response: {:?}", e))
    })
}
