// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

mod batch;
mod debug;
pub mod diagnostics;
mod kv;
mod raft_service;

pub use self::{
    debug::{ResolvedTsDiagnosisCallback, Service as DebugService},
    diagnostics::Service as DiagnosticsService,
    kv::{
        DefaultGrpcMessageFilter, GrpcRequestDuration, MeasuredBatchResponse,
        MeasuredSingleResponse, RaftGrpcMessageFilter, Service as KvService,
        batch_commands_request, batch_commands_response, future_flashback_to_version,
        future_prepare_flashback_to_version,
    },
    raft_service::RaftService,
};

#[macro_export]
macro_rules! log_net_error {
    ($err:expr, $($args:tt)*) => {{
        let e = $err;
        if let $crate::server::Error::Grpc(e) = e {
            info!($($args)*, "err" => %e);
        } else {
            debug!($($args)*, "err" => %e);
        }
    }}
}
