// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod range;
pub mod ranges_iter;
pub mod scanner;
pub mod test_fixture;

use async_trait::async_trait;
use kvproto::{coprocessor::KeyRange, metapb::Region};
use raft;

pub use self::range::*;

pub type Result<T> = std::result::Result<T, crate::error::StorageError>;

pub type OwnedKvPair = (Vec<u8>, Vec<u8>);

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
        range: IntervalRange,
    ) -> Result<()>;

    fn scan_next(&mut self) -> Result<Option<OwnedKvPair>>;

    // TODO: Use const generics.
    // TODO: Use reference is better.
    fn get(&mut self, is_key_only: bool, range: PointRange) -> Result<Option<OwnedKvPair>>;

    fn met_uncacheable_data(&self) -> Option<bool>;

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

    fn met_uncacheable_data(&self) -> Option<bool> {
        (**self).met_uncacheable_data()
    }

    fn collect_statistics(&mut self, dest: &mut Self::Statistics) {
        (**self).collect_statistics(dest);
    }
}

pub type StateRole = raft::StateRole;

/// The result of find_region_by_key
#[derive(Debug, Clone, PartialEq)]
pub enum FindRegionResult {
    /// A region that contains the specified key is found.
    Found { region: Region, role: StateRole },
    /// No region that contains the specified key is found.
    /// The field `next_region_start` indicates the start key of the nearest
    /// region in the local store after the specified key.
    /// For example, if the regions are distributed in the local store as
    /// follows: | ------------- [a, b) ------- c ------ [d, e) ------------- |
    ///                            |            |          |
    ///                         region1        key       region2
    /// Because the region of the key is found in the local store,
    /// NotFound { next_region_start: "d" } will be returned.
    /// It is useful to get the "hole" after the specified key in the local
    /// store, and the caller can use it to determine whether the following keys
    /// can be located in the local store or not without accessing the
    /// storage.
    NotFound { next_region_start: Option<Vec<u8>> },
}

impl FindRegionResult {
    /// Creates a `FindRegionResult` with the found region and its role.
    #[inline]
    pub fn with_found(region: Region, role: StateRole) -> Self {
        FindRegionResult::Found { region, role }
    }

    /// Creates a `FindRegionResult` with no region found; the next region start
    /// key is attached.
    #[inline]
    pub fn with_not_found(next_region_start: Option<Vec<u8>>) -> Self {
        FindRegionResult::NotFound { next_region_start }
    }

    #[inline]
    pub fn is_found(&self) -> bool {
        matches!(self, FindRegionResult::Found { .. })
    }
}

/// The abstract interface for accessing some extra region storages in the
/// cop-task.
/// For example, in the `IndexLookUp` executor can use it to find the regions
/// and storages where the primary keys are located.
#[async_trait]
pub trait RegionStorageAccessor: Sync + Send + Clone {
    type Storage;

    /// Find the region that contains the specified key.
    /// If found, `FindRegionResult::Found`, which contains the region and its
    /// role will be returned.
    /// Otherwise, `FindRegionResult::NotFound` will be returned.
    /// The argument `key` should be the comparable format, you should use
    /// `Key::from_raw` encode the raw key.
    async fn find_region_by_key(&self, key: &[u8]) -> Result<FindRegionResult>;
    /// Get the local storage for the specified region.
    /// It receives a region and a list of key ranges which need to be scanned.
    async fn get_local_region_storage(
        &self,
        region: &Region,
        key_ranges: &[KeyRange],
    ) -> Result<Self::Storage>;
}

/// StubAccessor is only a placeholder that does not provide any real
/// functionality.
/// It should not be instantiated.
#[derive(Debug)]
pub struct StubAccessor<S> {
    _phantom: std::marker::PhantomData<fn() -> S>,
}

impl<S> StubAccessor<S> {
    pub fn none() -> Option<Self> {
        None
    }
}

impl<S> Clone for StubAccessor<S> {
    fn clone(&self) -> Self {
        StubAccessor {
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<S> RegionStorageAccessor for StubAccessor<S> {
    type Storage = S;

    async fn find_region_by_key(&self, _key: &[u8]) -> Result<FindRegionResult> {
        unimplemented!()
    }

    async fn get_local_region_storage(
        &self,
        _region: &Region,
        _key_ranges: &[KeyRange],
    ) -> Result<Self::Storage> {
        unimplemented!()
    }
}
