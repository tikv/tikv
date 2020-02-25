// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(test)]
pub mod fixture;
mod range;
pub mod ranges_iter;
pub mod scanner;

pub use self::range::*;

pub type Result<T> = std::result::Result<T, crate::error::StorageError>;

pub type OwnedKvPair = (Vec<u8>, Vec<u8>);

/// The abstract storage interface. The table scan and index scan executor relies on a `Storage`
/// implementation to provide source data.
pub trait Storage: Send {
    type Statistics;

    // TODO: Use const generics.
    // TODO: Use reference is better.
    fn begin_scan(
        &mut self,
        is_backward_scan: bool,
        is_key_only: bool,
        range: IntervalRange,
    ) -> Result<()>;

    fn scan_next(&mut self) -> Result<Option<OwnedKvPair>>;

    // TODO: Use const generics.
    // TODO: Use reference is better.
    fn get(&mut self, is_key_only: bool, range: PointRange) -> Result<Option<OwnedKvPair>>;

    fn collect_statistics(&mut self, dest: &mut Self::Statistics);
}

impl<T: Storage + ?Sized> Storage for Box<T> {
    type Statistics = T::Statistics;

    fn begin_scan(
        &mut self,
        is_backward_scan: bool,
        is_key_only: bool,
        range: IntervalRange,
    ) -> Result<()> {
        (**self).begin_scan(is_backward_scan, is_key_only, range)
    }

    fn scan_next(&mut self) -> Result<Option<OwnedKvPair>> {
        (**self).scan_next()
    }

    fn get(&mut self, is_key_only: bool, range: PointRange) -> Result<Option<OwnedKvPair>> {
        (**self).get(is_key_only, range)
    }

    fn collect_statistics(&mut self, dest: &mut Self::Statistics) {
        (**self).collect_statistics(dest);
    }
}
