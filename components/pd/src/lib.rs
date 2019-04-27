#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;
#[macro_use(
    kv,
    slog_kv,
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
extern crate slog_global;
#[macro_use]
extern crate tikv_util;

pub use self::config::Config;
pub use self::errors::{Error, Result};

pub mod client;
pub mod config;
pub use tikv_misc::pd_errors as errors;
pub mod metrics;
pub mod raft;
pub mod stats;
pub mod util;

pub use tikv_misc::pd_client::*;
