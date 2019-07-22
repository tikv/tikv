// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(test)]
pub mod fixture;
mod range;
pub mod ranges_iter;
pub mod scanner;

pub use self::range::*;

use crate::coprocessor::Result;

pub type OwnedKvPair = (Vec<u8>, Vec<u8>);

/// The abstract storage interface. The table scan and index scan executor relies on a `Storage`
/// implementation to provide source data.
pub trait Storage: Send {
    type Statistics;

    // TODO: Use const generics.
    fn begin_scan(
        &mut self,
        is_backward_scan: bool,
        is_key_only: bool,
        range: IntervalRange,
    ) -> Result<()>;

    fn scan_next(&mut self) -> Result<Option<OwnedKvPair>>;

    // TODO: Use const generics.
    fn get(&mut self, is_key_only: bool, range: PointRange) -> Result<Option<OwnedKvPair>>;

    fn collect_statistics(&mut self, dest: &mut Self::Statistics);
}
