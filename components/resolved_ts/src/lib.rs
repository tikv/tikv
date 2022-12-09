// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Resolved TS is a timestamp that represents the lower bound of incoming
//! Commit TS
//  and the upper bound of outgoing Commit TS.
//! Through this timestamp we can get a consistent view in the transaction
//! level.
//!
//! To maintain a correct Resolved TS, these premises must be satisfied:
//! - Tracing all locks in the region, use the minimal Start TS as Resolved TS.
//! - If there is not any lock, use the latest timestamp as Resolved TS.
//! - Resolved TS must be advanced by the region leader after it has applied on
//!   its term.

#![feature(box_patterns)]
#![feature(result_flattening)]

#[macro_use]
extern crate tikv_util;

mod resolver;
pub use resolver::*;

mod cmd;
pub use cmd::*;
mod observer;
pub use observer::*;
mod advance;
pub use advance::*;
mod endpoint;
pub use endpoint::*;
mod errors;
pub use errors::*;
mod scanner;
pub use scanner::*;
mod metrics;
pub use metrics::*;
