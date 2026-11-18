// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

pub mod encoded;
pub mod raw_mvcc;
mod store;

pub use store::RawStore;
use txn_types::{Key, Value};

use crate::storage::Statistics;

/// Charges a RawKV point read, hit or miss, to the default-CF flow.
///
/// Every CF that RawKV accepts holds user data, but `ReadStats` only reports
/// the write and default-CF flows. Charging `stats.data` keeps point reads of
/// every CF visible to PD.
fn record_point_read(stats: &mut Statistics, key: &Key, value: Option<&Value>) {
    stats
        .data
        .record_read(key.as_encoded().len(), value.map_or(0, |value| value.len()));
}
