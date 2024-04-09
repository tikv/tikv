// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Iteration over engines and snapshots.
//!
//! For the purpose of key/value iteration, TiKV defines its own `Iterator`
//! trait, and `Iterable` types that can create iterators.
//!
//! Both `KvEngine`s and `Snapshot`s are `Iterable`.
//!
//! Iteration is performed over consistent views into the database, even when
//! iterating over the engine without creating a `Snapshot`. That is, iterating
//! over an engine behaves implicitly as if a snapshot was created first, and
//! the iteration is being performed on the snapshot.
//!
//! Iterators can be in an _invalid_ state, in which they are not positioned at
//! a key/value pair. This can occur when attempting to move before the first
//! pair, past the last pair, or when seeking to a key that does not exist.
//! There may be other conditions that invalidate iterators (TODO: I don't
//! know).
//!
//! An invalid iterator cannot move forward or back, but may be returned to a
//! valid state through a successful "seek" operation.
//!
//! As TiKV inherits its iteration semantics from RocksDB,
//! the RocksDB documentation is the ultimate reference:
//!
//! - [RocksDB iterator API](https://github.com/facebook/rocksdb/blob/master/include/rocksdb/iterator.h).
//! - [RocksDB wiki on iterators](https://github.com/facebook/rocksdb/wiki/Iterator)

use tikv_util::keybuilder::KeyBuilder;

use crate::*;

/// An iterator over a consistent set of keys and values.
///
/// Iterators are implemented for `KvEngine`s and for `Snapshot`s. They see a
/// consistent view of the database; an iterator created by an engine behaves as
/// if a snapshot was created first, and the iterator created from the snapshot.
///
/// Most methods on iterators will panic if they are not "valid",
/// as determined by the `valid` method.
/// An iterator is valid if it is currently "pointing" to a key/value pair.
///
/// Iterators begin in an invalid state; one of the `seek` methods
/// must be called before beginning iteration.
/// Iterators may become invalid after a failed `seek`,
/// or after iteration has ended after calling `next` or `prev`,
/// and they return `false`.
pub trait Iterator: Send {
    /// Move the iterator to a specific key.
    ///
    /// When an exact match is not found, `seek` sets the iterator to the next
    /// key greater than that specified as `key`, if such a key exists;
    /// `seek_for_prev` sets the iterator to the previous key less than
    /// that specified as `key`, if such a key exists.
    ///
    /// # Returns
    ///
    /// `true` if seeking succeeded and the iterator is valid,
    /// `false` if seeking failed and the iterator is invalid.
    fn seek(&mut self, key: &[u8]) -> Result<bool>;

    /// Move the iterator to a specific key.
    ///
    /// For the difference between this method and `seek`,
    /// see the documentation for `seek`.
    ///
    /// # Returns
    ///
    /// `true` if seeking succeeded and the iterator is valid,
    /// `false` if seeking failed and the iterator is invalid.
    fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool>;

    /// Seek to the first key in the engine.
    fn seek_to_first(&mut self) -> Result<bool>;

    /// Seek to the last key in the database.
    fn seek_to_last(&mut self) -> Result<bool>;

    /// Move a valid iterator to the previous key.
    ///
    /// # Panics
    ///
    /// If the iterator is invalid, iterator may panic or aborted.
    fn prev(&mut self) -> Result<bool>;

    /// Move a valid iterator to the next key.
    ///
    /// # Panics
    ///
    /// If the iterator is invalid, iterator may panic or aborted.
    fn next(&mut self) -> Result<bool>;

    /// Retrieve the current key.
    ///
    /// # Panics
    ///
    /// If the iterator is invalid, iterator may panic or aborted.
    fn key(&self) -> &[u8];

    /// Retrieve the current value.
    ///
    /// # Panics
    ///
    /// If the iterator is invalid, iterator may panic or aborted.
    fn value(&self) -> &[u8];

    /// Returns `true` if the iterator points to a `key`/`value` pair.
    fn valid(&self) -> Result<bool>;
}

pub trait RefIterable {
    type Iterator<'a>: Iterator
    where
        Self: 'a;

    fn iter(&self, opts: IterOptions) -> Result<Self::Iterator<'_>>;
}

pub trait IterMetricsCollector {
    fn internal_delete_skipped_count(&self) -> usize;

    fn internal_key_skipped_count(&self) -> usize;

    // todo: add more metrics related methods when needed.
}

pub trait MetricsExt {
    type Collector: IterMetricsCollector;
    fn metrics_collector(&self) -> Self::Collector;
}

pub trait Iterable {
    type Iterator: Iterator + MetricsExt;

    fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator>;

    fn iterator(&self, cf: &str) -> Result<Self::Iterator> {
        self.iterator_opt(cf, IterOptions::default())
    }

    /// scan the key between start_key(inclusive) and end_key(exclusive),
    /// the upper bound is omitted if end_key is empty
    fn scan<F>(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        fill_cache: bool,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let iter_opt = iter_option(start_key, end_key, fill_cache);
        scan_impl(self.iterator_opt(cf, iter_opt)?, start_key, f)
    }

    // Seek the first key >= given key, if not found, return None.
    fn seek(&self, cf: &str, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self.iterator(cf)?;
        if iter.seek(key)? {
            return Ok(Some((iter.key().to_vec(), iter.value().to_vec())));
        }
        Ok(None)
    }
}

fn scan_impl<Iter, F>(mut it: Iter, start_key: &[u8], mut f: F) -> Result<()>
where
    Iter: Iterator,
    F: FnMut(&[u8], &[u8]) -> Result<bool>,
{
    let mut remained = it.seek(start_key)?;
    while remained {
        remained = f(it.key(), it.value())? && it.next()?;
    }
    Ok(())
}

/// Collect all items of `it` into a vector, generally used for tests.
///
/// # Panics
///
/// If any errors occur during iterator.
pub fn collect<I: Iterator>(mut it: I) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut v = Vec::new();
    let mut it_valid = it.valid().unwrap();
    while it_valid {
        let kv = (it.key().to_vec(), it.value().to_vec());
        v.push(kv);
        it_valid = it.next().unwrap();
    }
    v
}

/// Build an `IterOptions` using giving data key bound. Empty upper bound will
/// be ignored.
pub fn iter_option(lower_bound: &[u8], upper_bound: &[u8], fill_cache: bool) -> IterOptions {
    let lower_bound = Some(KeyBuilder::from_slice(lower_bound, 0, 0));
    let upper_bound = if upper_bound.is_empty() {
        None
    } else {
        Some(KeyBuilder::from_slice(upper_bound, 0, 0))
    };
    IterOptions::new(lower_bound, upper_bound, fill_cache)
}
