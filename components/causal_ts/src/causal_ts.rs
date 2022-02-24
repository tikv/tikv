// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;
use txn_types::TimeStamp;

pub trait CausalTsProvider: Send + Sync {
    /// Get a new ts
    fn get_ts(&self) -> Result<TimeStamp>;

    /// Advance to not less than ts
    fn advance(&self, ts: TimeStamp) -> Result<()>;
}
