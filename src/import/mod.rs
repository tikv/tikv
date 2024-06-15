// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

//! This mod contains components to support rapid data import with the project
//! `tidb-lightning`.
//!
//! It mainly exposes one service:
//!
//! The `ImportSstService` is used to ingest the generated SST files into TiKV's
//! RocksDB instance. The ingesting process: `tidb-lightning` first uploads SST
//! files to the host where TiKV is located, and then calls the `Ingest` RPC.
//! After `ImportSstService` receives the RPC, it sends a message to raftstore
//! thread to notify it of the ingesting operation.  This service is running
//! inside TiKV because it needs to interact with raftstore.

mod duplicate_detect;
mod ingest;
mod raft_writer;
mod sst_service;

use std::fmt::Debug;

use grpcio::{RpcStatus, RpcStatusCode};
use kvproto::errorpb;
use sst_importer::metrics::IMPORTER_ERROR_VEC;
pub use sst_importer::{Config, Error, Result, SstImporter, TxnSstWriter};

pub use self::sst_service::ImportSstService;

pub fn make_rpc_error<E: Debug>(err: E) -> RpcStatus {
    // FIXME: Just spewing debug error formatting here seems pretty unfriendly
    RpcStatus::with_message(RpcStatusCode::UNKNOWN, format!("{:?}", err))
}

#[macro_export]
macro_rules! send_rpc_response {
    ($res:expr, $sink:ident, $label:ident, $timer:ident) => {{
        let res = match $res {
            Ok(resp) => {
                IMPORT_RPC_DURATION
                    .with_label_values(&[$label, "ok"])
                    .observe($timer.saturating_elapsed_secs());
                $sink.success(resp)
            }
            Err(e) => {
                IMPORT_RPC_DURATION
                    .with_label_values(&[$label, "error"])
                    .observe($timer.saturating_elapsed_secs());
                error_inc($label, &e);
                $sink.fail(make_rpc_error(e))
            }
        };
        let _ = res.map_err(|e| warn!("send rpc response"; "err" => %e)).await;
    }};
}

// add error statistics from pb error response
fn pb_error_inc(type_: &str, e: &errorpb::Error) {
    let label = if e.has_not_leader() {
        "not_leader"
    } else if e.has_store_not_match() {
        "store_not_match"
    } else if e.has_region_not_found() {
        "region_not_found"
    } else if e.has_key_not_in_region() {
        "key_not_in_range"
    } else if e.has_epoch_not_match() {
        "epoch_not_match"
    } else if e.has_server_is_busy() {
        "server_is_busy"
    } else if e.has_stale_command() {
        "stale_command"
    } else if e.has_raft_entry_too_large() {
        "raft_entry_too_large"
    } else {
        "unknown"
    };

    IMPORTER_ERROR_VEC.with_label_values(&[type_, label]).inc();
}
