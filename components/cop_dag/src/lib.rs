// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

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
slog_trace,
slog_error,
slog_warn,
slog_info,
slog_debug,
slog_log,
slog_record,
slog_b,
slog_record_static
)]
extern crate slog;
#[macro_use]
extern crate slog_derive;
#[macro_use]
extern crate slog_global;

pub mod builder;

mod redundant_files;
pub use redundant_files::*;

pub mod exec_summary;
pub mod batch;
pub mod expr;
pub mod error;
pub mod rpn_expr;
pub mod executor;
mod scanner;
pub mod handler;
pub mod batch_handler;

pub use self::builder::DAGBuilder;
pub use self::error::{Error, Result};
pub use self::scanner::{ScanOn, Scanner};
pub use self::handler::DAGRequestHandler;
pub use self::batch_handler::BatchDAGHandler;