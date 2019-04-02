// Copyright 2018 TiKV Project Authors.
use crate::raftstore::store::engine::IterOption;
use crate::storage::engine::Result;
use crate::storage::{CfName, Cursor, Key, ScanMode, Snapshot};

/// A handy utility to build a snapshot cursor according to various configurations.
pub struct CursorBuilder<'a, S: Snapshot> {
    snapshot: &'a S,
    cf: CfName,

    scan_mode: ScanMode,
    fill_cache: bool,
    prefix_seek: bool,
    upper_bound: Option<Key>,
    lower_bound: Option<Key>,
}

impl<'a, S: 'a + Snapshot> CursorBuilder<'a, S> {
    /// Initialize a new `CursorBuilder`.
    pub fn new(snapshot: &'a S, cf: CfName) -> Self {
        CursorBuilder {
            snapshot,
            cf,

            scan_mode: ScanMode::Forward,
            fill_cache: true,
            prefix_seek: false,
            upper_bound: None,
            lower_bound: None,
        }
    }

    /// Set whether or not read operations should fill the cache.
    ///
    /// Defaults to `true`.
    #[inline]
    pub fn fill_cache(mut self, fill_cache: bool) -> Self {
        self.fill_cache = fill_cache;
        self
    }

    /// Set whether or not to use prefix seek.
    ///
    /// Defaults to `false`, it means use total order seek.
    #[inline]
    pub fn prefix_seek(mut self, prefix_seek: bool) -> Self {
        self.prefix_seek = prefix_seek;
        self
    }

    /// Set iterator scanning mode.
    ///
    /// Defaults to `ScanMode::Forward`.
    #[inline]
    pub fn scan_mode(mut self, scan_mode: ScanMode) -> Self {
        self.scan_mode = scan_mode;
        self
    }

    /// Set iterator range by giving lower and upper bound.
    /// The range is left closed right open.
    ///
    /// Both defaults to `None`.
    #[inline]
    pub fn range(mut self, lower: Option<Key>, upper: Option<Key>) -> Self {
        self.lower_bound = lower;
        self.upper_bound = upper;
        self
    }

    /// Build `Cursor` from the current configuration.
    pub fn build(self) -> Result<Cursor<S::Iter>> {
        let mut iter_opt = IterOption::new(
            self.lower_bound.map(|k| k.into_encoded()),
            self.upper_bound.map(|k| k.into_encoded()),
            self.fill_cache,
        );
        if self.prefix_seek {
            iter_opt = iter_opt.use_prefix_seek().set_prefix_same_as_start(true);
        }
        self.snapshot.iter_cf(self.cf, iter_opt, self.scan_mode)
    }
}
