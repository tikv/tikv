// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Per-thread cache for schema metadata derived from coprocessor requests.
//!
//! Every DAG request from TiDB carries the full `ColumnInfo` list of the
//! table or index it scans, although table schemas rarely change (see
//! tikv/tikv#6367). Turning that schema into the executor-side
//! representation (field types, column-id lookup tables, handle positions,
//! default values) is therefore repeated for every request against the same
//! table, which is a visible share of the work for short OLTP requests.
//!
//! [`SchemaCache`] de-duplicates that work. Derived metadata is stored
//! behind an [`Arc`] and keyed by a 64-bit fingerprint of the request's
//! schema description. A fingerprint may collide, so every hit is verified
//! by comparing the full source description; a mismatch is treated as a
//! miss and replaces the entry. A hit can therefore never hand out metadata
//! that belongs to a different schema.
//!
//! The cache is meant to be used through one `thread_local!` instance per
//! executor kind, so lookups on the read pool need no locking. The
//! per-thread capacity is process-wide, comes from
//! `server.end-point-schema-cache-capacity`, and `0` disables caching.

use std::{
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use tidb_query_common::{Result, metrics::SCHEMA_CACHE_METRICS};
use tikv_util::lru::LruCache;
use tipb::ColumnInfo;

/// Default number of derived schemas kept per read-pool thread.
pub const DEFAULT_SCHEMA_CACHE_CAPACITY: usize = 256;

static SCHEMA_CACHE_CAPACITY: AtomicUsize = AtomicUsize::new(DEFAULT_SCHEMA_CACHE_CAPACITY);

/// Sets the per-thread capacity of all schema caches. `0` disables caching.
///
/// The new value takes effect on each thread at its next lookup.
pub fn set_schema_cache_capacity(capacity: usize) {
    SCHEMA_CACHE_CAPACITY.store(capacity, Ordering::Relaxed);
}

/// Returns the per-thread capacity of all schema caches.
pub fn schema_cache_capacity() -> usize {
    SCHEMA_CACHE_CAPACITY.load(Ordering::Relaxed)
}

/// Feeds every schema-relevant field of `columns` into `hasher`.
///
/// `ColumnInfo` does not implement `Hash`, and the fingerprint only has to
/// be stable within one process, so the fields are hashed directly instead
/// of serializing the message.
pub fn hash_columns_info<H: Hasher>(columns: &[ColumnInfo], hasher: &mut H) {
    hasher.write_usize(columns.len());
    for ci in columns {
        hasher.write_i64(ci.get_column_id());
        hasher.write_i32(ci.get_tp());
        hasher.write_i32(ci.get_collation());
        hasher.write_i32(ci.get_column_len());
        hasher.write_i32(ci.get_decimal());
        hasher.write_i32(ci.get_flag());
        hasher.write_usize(ci.get_elems().len());
        for elem in ci.get_elems() {
            elem.hash(hasher);
        }
        ci.get_default_val().hash(hasher);
        hasher.write_u8(ci.get_pk_handle() as u8);
        hasher.write_u8(ci.get_array() as u8);
    }
}

/// Computes the cache key of `source`.
pub fn fingerprint<S: Hash>(source: &S) -> u64 {
    let mut hasher = fxhash::FxHasher::default();
    source.hash(&mut hasher);
    hasher.finish()
}

struct Entry<S, V> {
    source: S,
    value: Arc<V>,
}

/// A bounded, LRU-evicted map from schema descriptions to the metadata
/// derived from them.
///
/// `S` is the schema description a request carries and `V` the metadata
/// derived from it. `S::eq` must imply that the derived metadata is equal,
/// which is what makes sharing one `Arc<V>` between requests sound.
pub struct SchemaCache<S, V> {
    lru: LruCache<u64, Entry<S, V>>,
}

impl<S, V> Default for SchemaCache<S, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S, V> SchemaCache<S, V> {
    pub fn new() -> Self {
        Self {
            lru: LruCache::with_capacity(schema_cache_capacity().max(1)),
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.lru.len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.lru.is_empty()
    }
}

impl<S: PartialEq + Hash, V> SchemaCache<S, V> {
    /// Returns the metadata derived from `source`, reusing a cached instance
    /// when an equal source has been seen on this thread before.
    ///
    /// Errors from `derive` are returned as-is and never cached.
    #[inline]
    pub fn get_or_derive<F>(&mut self, source: S, derive: F) -> Result<Arc<V>>
    where
        F: FnOnce(&S) -> Result<V>,
    {
        self.get_or_derive_with_capacity(schema_cache_capacity(), source, derive)
    }

    /// Same as [`get_or_derive`](Self::get_or_derive), but with an explicit
    /// capacity instead of the process-wide setting.
    pub fn get_or_derive_with_capacity<F>(
        &mut self,
        capacity: usize,
        source: S,
        derive: F,
    ) -> Result<Arc<V>>
    where
        F: FnOnce(&S) -> Result<V>,
    {
        if capacity == 0 {
            SCHEMA_CACHE_METRICS.bypass.inc();
            if !self.lru.is_empty() {
                self.lru.clear();
            }
            return derive(&source).map(Arc::new);
        }
        if self.lru.capacity() != capacity {
            self.lru.resize(capacity);
        }

        let key = fingerprint(&source);
        if let Some(entry) = self.lru.get(&key) {
            if entry.source == source {
                SCHEMA_CACHE_METRICS.hit.inc();
                return Ok(entry.value.clone());
            }
            // Fingerprint collision. Fall through: derive from the new source
            // and let the insert below replace the stale entry.
        }

        SCHEMA_CACHE_METRICS.miss.inc();
        let value = Arc::new(derive(&source)?);
        self.lru.insert(
            key,
            Entry {
                source,
                value: value.clone(),
            },
        );
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use tidb_query_datatype::FieldTypeTp;

    use super::*;

    /// A source whose fingerprint is chosen explicitly, so collisions can be
    /// forced.
    #[derive(PartialEq, Debug)]
    struct Src {
        fp: u64,
        payload: u32,
    }

    impl Hash for Src {
        fn hash<H: Hasher>(&self, state: &mut H) {
            state.write_u64(self.fp);
        }
    }

    fn derive_counted(counter: &Cell<usize>) -> impl FnOnce(&Src) -> Result<u32> + '_ {
        move |s| {
            counter.set(counter.get() + 1);
            Ok(s.payload * 10)
        }
    }

    #[test]
    fn test_hit_shares_one_arc() {
        let mut cache = SchemaCache::<Src, u32>::new();
        let derived = Cell::new(0);
        let a = cache
            .get_or_derive_with_capacity(4, Src { fp: 1, payload: 7 }, derive_counted(&derived))
            .unwrap();
        let b = cache
            .get_or_derive_with_capacity(4, Src { fp: 1, payload: 7 }, derive_counted(&derived))
            .unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(*a, 70);
        assert_eq!(derived.get(), 1);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_collision_never_serves_wrong_value() {
        let mut cache = SchemaCache::<Src, u32>::new();
        let derived = Cell::new(0);
        let a = cache
            .get_or_derive_with_capacity(4, Src { fp: 1, payload: 1 }, derive_counted(&derived))
            .unwrap();
        // Same fingerprint, different source: must re-derive and replace.
        let b = cache
            .get_or_derive_with_capacity(4, Src { fp: 1, payload: 2 }, derive_counted(&derived))
            .unwrap();
        assert!(!Arc::ptr_eq(&a, &b));
        assert_eq!(*a, 10);
        assert_eq!(*b, 20);
        assert_eq!(derived.get(), 2);
        assert_eq!(cache.len(), 1);
        // The replaced entry now serves the second source.
        let c = cache
            .get_or_derive_with_capacity(4, Src { fp: 1, payload: 2 }, derive_counted(&derived))
            .unwrap();
        assert!(Arc::ptr_eq(&b, &c));
        assert_eq!(derived.get(), 2);
    }

    #[test]
    fn test_zero_capacity_bypasses_and_drops_entries() {
        let mut cache = SchemaCache::<Src, u32>::new();
        let derived = Cell::new(0);
        cache
            .get_or_derive_with_capacity(4, Src { fp: 1, payload: 1 }, derive_counted(&derived))
            .unwrap();
        assert_eq!(cache.len(), 1);
        let a = cache
            .get_or_derive_with_capacity(0, Src { fp: 1, payload: 1 }, derive_counted(&derived))
            .unwrap();
        let b = cache
            .get_or_derive_with_capacity(0, Src { fp: 1, payload: 1 }, derive_counted(&derived))
            .unwrap();
        assert!(!Arc::ptr_eq(&a, &b));
        assert_eq!(derived.get(), 3);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_capacity_bounds_entries_and_evicts_lru() {
        let mut cache = SchemaCache::<Src, u32>::new();
        let derived = Cell::new(0);
        for fp in 0..3 {
            cache
                .get_or_derive_with_capacity(2, Src { fp, payload: 1 }, derive_counted(&derived))
                .unwrap();
        }
        assert_eq!(cache.len(), 2);
        assert_eq!(derived.get(), 3);
        // fp 0 is the least recently used and must have been evicted.
        cache
            .get_or_derive_with_capacity(2, Src { fp: 0, payload: 1 }, derive_counted(&derived))
            .unwrap();
        assert_eq!(derived.get(), 4);
        // fp 2 was touched after fp 1, so fp 1 got evicted just now.
        cache
            .get_or_derive_with_capacity(2, Src { fp: 2, payload: 1 }, derive_counted(&derived))
            .unwrap();
        assert_eq!(derived.get(), 4);
        // Shrinking drops the oldest entries.
        cache
            .get_or_derive_with_capacity(1, Src { fp: 2, payload: 1 }, derive_counted(&derived))
            .unwrap();
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_derive_error_is_not_cached() {
        let mut cache = SchemaCache::<Src, u32>::new();
        let r = cache
            .get_or_derive_with_capacity(4, Src { fp: 1, payload: 1 }, |_| Err(other_err!("boom")));
        r.unwrap_err();
        assert!(cache.is_empty());
    }

    #[test]
    fn test_hash_columns_info_distinguishes_fields() {
        fn fp(columns: &[ColumnInfo]) -> u64 {
            let mut h = fxhash::FxHasher::default();
            hash_columns_info(columns, &mut h);
            h.finish()
        }
        let base: ColumnInfo = FieldTypeTp::Long.into();
        let mut with_id = base.clone();
        with_id.set_column_id(3);
        let mut with_default = with_id.clone();
        with_default.set_default_val(vec![1, 2, 3]);
        let mut with_pk = with_id.clone();
        with_pk.set_pk_handle(true);
        let mut with_elems = with_id.clone();
        with_elems.set_elems(protobuf::RepeatedField::from(vec!["a".to_owned()]));

        let all = [
            fp(std::slice::from_ref(&base)),
            fp(std::slice::from_ref(&with_id)),
            fp(&[with_default]),
            fp(&[with_pk]),
            fp(&[with_elems]),
            fp(&[with_id.clone(), with_id.clone()]),
            fp(&[]),
        ];
        for i in 0..all.len() {
            for j in 0..i {
                assert_ne!(all[i], all[j], "{} vs {}", i, j);
            }
        }
        assert_eq!(fp(std::slice::from_ref(&with_id)), fp(&[with_id]));
    }

    #[test]
    fn test_capacity_setting_round_trip() {
        let before = schema_cache_capacity();
        set_schema_cache_capacity(before + 1);
        assert_eq!(schema_cache_capacity(), before + 1);
        set_schema_cache_capacity(before);
        assert_eq!(schema_cache_capacity(), before);
    }
}
