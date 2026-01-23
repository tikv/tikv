// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod range;
pub mod ranges_iter;
pub mod scanner;
pub mod test_fixture;

pub use self::range::*;

pub type Result<T> = std::result::Result<T, crate::error::StorageError>;

pub type OwnedKvPair = (Vec<u8>, Vec<u8>);

pub struct OwnedKvPairEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub commit_ts: Option<u64>,
}

impl From<OwnedKvPairEntry> for OwnedKvPair {
    fn from(val: OwnedKvPairEntry) -> Self {
        (val.key, val.value)
    }
}

/// The abstract storage interface. The table scan and index scan executor
/// relies on a `Storage` implementation to provide source data.
pub trait Storage: Send {
    type Statistics;

    // TODO: Use const generics.
    // TODO: Use reference is better.
    fn begin_scan(
        &mut self,
        is_backward_scan: bool,
        is_key_only: bool,
        load_commit_ts: bool,
        range: IntervalRange,
    ) -> Result<()>;

    fn scan_next_entry(&mut self) -> Result<Option<OwnedKvPairEntry>>;

    #[inline]
    fn scan_next(&mut self) -> Result<Option<OwnedKvPair>> {
        Ok(self.scan_next_entry()?.map(|entry| entry.into()))
    }

    // TODO: Use const generics.
    // TODO: Use reference is better.
    fn get_entry(
        &mut self,
        is_key_only: bool,
        load_commit_ts: bool,
        range: PointRange,
    ) -> Result<Option<OwnedKvPairEntry>>;

    #[inline]
    fn get(&mut self, is_key_only: bool, range: PointRange) -> Result<Option<OwnedKvPair>> {
        Ok(self
            .get_entry(is_key_only, false, range)?
            .map(|entry| entry.into()))
    }

    fn met_uncacheable_data(&self) -> Option<bool>;

    fn collect_statistics(&mut self, dest: &mut Self::Statistics);
}

impl<T: Storage + ?Sized> Storage for Box<T> {
    type Statistics = T::Statistics;

    fn begin_scan(
        &mut self,
        is_backward_scan: bool,
        is_key_only: bool,
        load_commit_ts: bool,
        range: IntervalRange,
    ) -> Result<()> {
        (**self).begin_scan(is_backward_scan, is_key_only, load_commit_ts, range)
    }

    fn scan_next_entry(&mut self) -> Result<Option<OwnedKvPairEntry>> {
        (**self).scan_next_entry()
    }

    fn get_entry(
        &mut self,
        is_key_only: bool,
        load_commit_ts: bool,
        range: PointRange,
    ) -> Result<Option<OwnedKvPairEntry>> {
        (**self).get_entry(is_key_only, load_commit_ts, range)
    }

    fn met_uncacheable_data(&self) -> Option<bool> {
        (**self).met_uncacheable_data()
    }

    fn collect_statistics(&mut self, dest: &mut Self::Statistics) {
        (**self).collect_statistics(dest);
    }
}
