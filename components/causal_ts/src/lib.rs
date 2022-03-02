// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
extern crate tikv_util;

mod errors;
pub use errors::*;

mod tso;
pub use tso::*;

mod hlc;
pub use hlc::*;

mod observer;
pub use observer::*;

use crate::errors::Result;
use txn_types::TimeStamp;

/// Trait of causal timestamp provider.
pub trait CausalTsProvider: Send + Sync {
    /// Get a new ts
    fn get_ts(&self) -> Result<TimeStamp>;

    /// Advance to not less than ts, to make following event "happen-after" this ts.
    fn advance(&self, ts: TimeStamp) -> Result<()>;
}
