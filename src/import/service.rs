// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use futures::Future;
use grpcio::{RpcContext, RpcStatus, RpcStatusCode, UnarySink};

use super::Error;

pub fn make_rpc_error(err: Error) -> RpcStatus {
    RpcStatus::new(RpcStatusCode::UNKNOWN, Some(format!("{:?}", err)))
}

pub fn send_rpc_error<M, E>(ctx: RpcContext<'_>, sink: UnarySink<M>, error: E)
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
