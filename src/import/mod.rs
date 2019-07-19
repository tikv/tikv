// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

//! This mod contains components to support rapid data import with the project
//! `tidb-lightning`.
//!
//! It mainly exposes one service:
//!
//! The `ImportSSTService` is used to ingest the generated SST files into TiKV's
//! RocksDB instance. The ingesting process: `tidb-lightning` first uploads SST
//! files to the host where TiKV is located, and then calls the `Ingest` RPC.
//! After `ImportSSTService` receives the RPC, it sends a message to raftstore
//! thread to notify it of the ingesting operation.  This service is running
//! inside TiKV because it needs to interact with raftstore.

mod config;
mod errors;
mod metrics;
#[macro_use]
mod service;
mod import_mode;
mod sst_importer;
mod sst_service;

pub mod test_helpers;

pub use self::config::Config;
pub use self::errors::{Error, Result};
pub use self::sst_importer::SSTImporter;
pub use self::sst_service::ImportSSTService;
