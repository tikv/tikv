// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(test)]
pub mod fixture;
mod range;
pub mod ranges_iter;
pub mod scanner;
pub mod scanner2;

use tikv_util::buffer_vec::BufferVec;

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

    fn begin_range_scan(&mut self, _is_key_only: bool, _range: IntervalRange) -> Result<()> {
        unimplemented!()
    }

    fn range_scan_next_batch(
        &mut self,
        _n: usize,
        _out_keys: &mut BufferVec,
        _out_values: &mut BufferVec,
    ) -> Result<usize> {
        unimplemented!()
    }

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

    fn begin_range_scan(&mut self, is_key_only: bool, range: IntervalRange) -> Result<()> {
        (**self).begin_range_scan(is_key_only, range)
    }

    fn range_scan_next_batch(
        &mut self,
        n: usize,
        out_keys: &mut BufferVec,
        out_values: &mut BufferVec,
    ) -> Result<usize> {
        (**self).range_scan_next_batch(n, out_keys, out_values)
    }

    fn get(&mut self, is_key_only: bool, range: PointRange) -> Result<Option<OwnedKvPair>> {
        (**self).get(is_key_only, range)
    }

    fn collect_statistics(&mut self, dest: &mut Self::Statistics) {
        (**self).collect_statistics(dest);
    }
}
