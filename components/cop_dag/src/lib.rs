// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
#![feature(test)]
#![feature(fnbox)]

#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate tikv_util;
#[macro_use]
extern crate bitflags;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate fail;
#[macro_use(
    kv,
    slog_kv,
    slog_error,
    slog_warn,
    slog_debug,
    slog_log,
    slog_record,
    slog_b,
    slog_record_static
)]
extern crate slog;
#[macro_use]
extern crate slog_global;
#[cfg(test)]
extern crate test;


mod redundant_files;
pub use redundant_files::*;

pub mod batch;
pub mod batch_handler;
pub mod error;
pub mod exec_summary;
pub mod executor;
pub mod expr;
pub mod handler;
pub mod rpn_expr;
mod scanner;

pub use self::batch_handler::BatchDAGHandler;
pub use self::error::{Error, Result};
pub use self::handler::DAGRequestHandler;
pub use self::scanner::{ScanOn, Scanner};
