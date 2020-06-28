// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use grpcio::{RpcStatus, RpcStatusCode};
use std::fmt::Debug;

pub fn make_rpc_error<E: Debug>(err: E) -> RpcStatus {
    // FIXME: Just spewing debug error formatting here seems pretty unfriendly
    RpcStatus::new(RpcStatusCode::UNKNOWN, Some(format!("{:?}", err)))
}

#[macro_export]
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
                error_inc(&e);
                $sink.fail(make_rpc_error(e))
            }
        };
        res.map_err(|e| warn!("send rpc response"; "err" => %e))
    }};
}
