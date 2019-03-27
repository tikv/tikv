// Copyright 2018 TiKV Project Authors.
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

//! This mod contains components to support rapid data import with the project
//! `tidb-lightning`.
//!
//! It mainly exposes 2 types of services:
//!
//! The `ImportKVService` is used to convert key-value pairs data to TiKV
//! compatible RocksDB SST file format for subsequent ingesting. The conversion
//! is done by writing data to a temporary created RocksDB instance. The service
//! is usually run independently of TiKV.
//!
//! The `ImportSSTService` is used to ingest the generated SST files into TiKV's
//! RocksDB instance. The ingesting process: `tidb-lightning` first uploads SST
//! files to the host where TiKV is located, and then calls the `Ingest` RPC.
//! After `ImportSSTService` receives the RPC, it sends a message to raftstore
//! thread to notify it of the ingesting operation.  This service is running
//! inside TiKV because it needs to interact with raftstore.

mod client;
mod common;
mod config;
mod engine;
mod errors;
mod import;
mod metrics;
mod prepare;
mod stream;
#[macro_use]
mod service;
mod import_mode;
mod kv_importer;
mod kv_server;
mod kv_service;
mod sst_importer;
mod sst_service;

pub mod test_helpers;

pub use self::config::Config;
pub use self::errors::{Error, Result};
pub use self::kv_importer::KVImporter;
pub use self::kv_server::ImportKVServer;
pub use self::kv_service::ImportKVService;
pub use self::sst_importer::SSTImporter;
pub use self::sst_service::ImportSSTService;
