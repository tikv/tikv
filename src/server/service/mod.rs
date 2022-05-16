// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

mod batch;
mod debug;
mod diagnostics;
mod kv;

pub use self::{
    debug::Service as DebugService,
    diagnostics::Service as DiagnosticsService,
    kv::{
        batch_commands_request, batch_commands_response, GrpcRequestDuration,
        MeasuredBatchResponse, MeasuredSingleResponse, Service as KvService,
    },
};

#[macro_export]
macro_rules! log_net_error {
    ($err:expr, $($args:tt)*) => {{
        let e = $err;
        if let crate::server::Error::Grpc(e) = e {
            info!($($args)*, "err" => %e);
        } else {
            debug!($($args)*, "err" => %e);
        }
    }}
}
