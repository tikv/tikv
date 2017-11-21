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

use std::boxed::FnBox;
use std::fmt::Debug;

use grpc::{RpcContext, RpcStatus, RpcStatusCode, UnarySink};
use futures::Future;
use futures::sync::oneshot;

use super::Error;

pub fn make_cb<T: Debug + Send + 'static>() -> (Box<FnBox(T) + Send>, oneshot::Receiver<T>) {
    let (tx, rx) = oneshot::channel();
    let cb = move |res| { tx.send(res).unwrap(); };
    (box cb, rx)
}

pub fn make_rpc_error(err: Error) -> RpcStatus {
    RpcStatus::new(RpcStatusCode::Unknown, Some(format!("{:?}", err)))
}

pub fn send_rpc_error<M, E>(ctx: RpcContext, sink: UnarySink<M>, error: E)
where
    Error: From<E>,
{
    let err = make_rpc_error(Error::from(error));
    ctx.spawn(sink.fail(err).map_err(|e| {
        warn!("send rpc error: {:?}", e);
    }));
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
