// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

//! Importing RocksDB SST files into TiKV
#![feature(min_specialization)]

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate tikv_util;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

mod config;
mod errors;
mod import_file;
mod sst_writer;
mod util;
#[macro_use]
pub mod import_mode;
pub mod metrics;
pub mod sst_importer;

pub use self::config::Config;
pub use self::errors::{error_inc, Error, Result};
pub use self::import_file::sst_meta_to_path;
pub use self::sst_importer::SstImporter;
pub use self::sst_writer::{RawSstWriter, TxnSstWriter};
pub use self::util::prepare_sst_for_ingestion;
