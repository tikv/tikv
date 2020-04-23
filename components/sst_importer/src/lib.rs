// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

//! Importing RocksDB SST files into TiKV

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate slog_global;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

mod config;
mod errors;
pub mod metrics;
mod util;
#[macro_use]
pub mod service;
pub mod import_mode;
pub mod sst_importer;

pub use self::config::Config;
pub use self::errors::{error_inc, Error, Result};
pub use self::sst_importer::SSTImporter;
pub use self::util::prepare_sst_for_ingestion;
