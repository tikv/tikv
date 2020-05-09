#![feature(specialization)]

#[macro_use]
pub mod macros;

pub mod error;
pub mod execute_stats;
pub mod metrics;
pub mod storage;
pub mod util;

pub use self::error::{Error, Result};
