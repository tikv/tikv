// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::RocksSstIterator;
use engine_traits::Iterator;

const DEFAULT_SEEK_NEXT_LIMIT: usize = 32;

/// DefaultSeekIterator is a lightweight helper for point seek on multiple
/// default CF SST iterators. It only supports exact seek matching and avoids
/// merge-iteration logic that is unnecessary for this access pattern. The
/// caller must request keys in non-decreasing order; this iterator does not
/// validate or repair backward seeks. For ordered requests, it advances
/// existing iterator positions with `next` before falling back to `seek`.
pub struct DefaultSeekIterator<'a> {
    sst_iters: Vec<RocksSstIterator<'a>>,
    initialized: Vec<bool>,
    valid: Vec<bool>,
    matched_idx: Option<usize>,
}

impl<'a> DefaultSeekIterator<'a> {
    pub fn new(sst_iters: Vec<RocksSstIterator<'a>>) -> Self {
        let len = sst_iters.len();
        Self {
            sst_iters,
            initialized: vec![false; len],
            valid: vec![false; len],
            matched_idx: None,
        }
    }

    pub fn seek_exact(&mut self, key: &[u8]) -> engine_traits::Result<bool> {
        self.matched_idx = None;

        for (idx, iter) in self.sst_iters.iter_mut().enumerate() {
            if !Self::advance_iter_to(iter, &mut self.initialized[idx], &mut self.valid[idx], key)?
            {
                continue;
            }
            if iter.key() == key {
                // Keep the first matched iterator. This is consistent with
                // existing behavior where deterministic tie-breaking among
                // equal keys is not relied on by current callers.
                self.matched_idx = Some(idx);
                break;
            }
        }
        Ok(self.matched_idx.is_some())
    }

    fn advance_iter_to(
        iter: &mut RocksSstIterator<'a>,
        initialized: &mut bool,
        valid: &mut bool,
        key: &[u8],
    ) -> engine_traits::Result<bool> {
        if !*initialized {
            *initialized = true;
            *valid = iter.seek(key)?;
            return Ok(*valid);
        }

        if !*valid {
            return Ok(false);
        }

        if iter.key() >= key {
            return Ok(true);
        }

        for _ in 0..DEFAULT_SEEK_NEXT_LIMIT {
            *valid = iter.next()?;
            if !*valid {
                return Ok(false);
            }
            if iter.key() >= key {
                return Ok(true);
            }
        }

        *valid = iter.seek(key)?;
        Ok(*valid)
    }

    #[cfg(test)]
    pub fn key(&self) -> &[u8] {
        self.sst_iters[self.matched_idx.unwrap()].key()
    }

    pub fn value(&self) -> &[u8] {
        self.sst_iters[self.matched_idx.unwrap()].value()
    }
}

/// BinaryIterator provides a multi-way merge iterator that can merge multiple
/// sorted SST files into a single sorted stream with automatic deduplication.
pub struct BinaryIterator<'a> {
    /// Vector of SST file iterators
    sst_iters: Vec<RocksSstIterator<'a>>,

    /// Minimum heap storing indices of sst_iters
    entry_cache: Vec<usize>,
}

impl<'a> BinaryIterator<'a> {
    pub fn new(sst_iters: Vec<RocksSstIterator<'a>>) -> Self {
        let entry_cache = Vec::with_capacity(sst_iters.len());
        BinaryIterator {
            sst_iters,
            entry_cache,
        }
    }

    fn iter_key(&self, pos: usize) -> &[u8] {
        self.sst_iters[self.entry_cache[pos]].key()
    }

    /// Sift up operation to maintain heap property (move smaller elements up)
    fn sift_up(&mut self, mut pos: usize) {
        while pos > 0 {
            let parent = (pos - 1) / 2;
            if self.iter_key(pos) < self.iter_key(parent) {
                self.entry_cache.swap(pos, parent);
                pos = parent;
            } else {
                return;
            }
        }
    }

    /// Sift down operation to maintain heap property (move larger elements
    /// down)
    fn sift_down(&mut self, mut pos: usize) {
        // Start with left child
        let mut child = pos * 2 + 1;
        while child < self.entry_cache.len() {
            // Choose the smaller child
            if child + 1 < self.entry_cache.len() && self.iter_key(child) > self.iter_key(child + 1)
            {
                child += 1; // Use right child instead
            }

            // If current position is already smaller than or equal to the smallest child,
            // stop
            if self.iter_key(pos) <= self.iter_key(child) {
                return;
            }

            // Swap and continue sifting down
            self.entry_cache.swap(pos, child);
            pos = child;
            child = pos * 2 + 1;
        }
    }

    /// Add an iterator index to the heap
    fn push(&mut self, from: usize) {
        let pos = self.entry_cache.len();
        self.entry_cache.push(from);
        self.sift_up(pos);
    }

    /// Remove and return the iterator index with the smallest key (heap top)
    fn pop(&mut self) -> Option<usize> {
        if self.entry_cache.is_empty() {
            return None;
        }
        let from = self.entry_cache.swap_remove(0);
        if !self.entry_cache.is_empty() {
            self.sift_down(0);
        }
        Some(from)
    }

    /// Build heap from current entry_cache (heapify)
    /// Called after adding all initial elements
    fn heap(&mut self) {
        if self.entry_cache.len() <= 1 {
            return;
        }
        let end = self.entry_cache.len() - 1;
        let mut parent = (end - 1) / 2;
        loop {
            self.sift_down(parent);

            if parent == 0 {
                return;
            }

            parent -= 1;
        }
    }

    /// Get the iterator index at heap top (minimum key)
    fn peek(&self) -> usize {
        self.entry_cache[0]
    }

    fn current_key_has_duplicate(&self) -> bool {
        if self.entry_cache.len() <= 1 {
            return false;
        }

        let key = self.sst_iters[self.peek()].key();
        self.iter_key(1) == key || (self.entry_cache.len() > 2 && self.iter_key(2) == key)
    }

    fn advance_current_key(&mut self) -> engine_traits::Result<()> {
        if self.entry_cache.is_empty() {
            return Ok(());
        }

        let root = self.peek();
        if self.sst_iters[root].next()? {
            self.sift_down(0);
        } else {
            self.pop();
        }
        Ok(())
    }

    fn advance_all_current_key(&mut self) -> engine_traits::Result<()> {
        let mut same_from = self.pop().unwrap();
        while !self.entry_cache.is_empty() && self.key() == self.sst_iters[same_from].key() {
            let from = self.pop().unwrap();
            self.advance_iter_and_push(same_from)?;
            same_from = from;
        }

        self.advance_iter_and_push(same_from)
    }

    fn advance_iter_and_push(&mut self, from: usize) -> engine_traits::Result<()> {
        if self.sst_iters[from].next()? {
            self.push(from);
        }
        Ok(())
    }

    fn try_deduplicate_current_key(&mut self) -> engine_traits::Result<bool> {
        loop {
            if self.entry_cache.is_empty() {
                return Ok(false);
            }

            if self.sst_iters[self.peek()].value().is_empty() {
                // compact-log-backup encodes physical delete records as empty
                // values, so any duplicate key group containing one is skipped.
                self.advance_all_current_key()?;
                continue;
            }

            if !self.current_key_has_duplicate() {
                return Ok(true);
            }

            self.advance_current_key()?;
        }
    }

    fn normalize_current_key(&mut self) -> engine_traits::Result<bool> {
        self.try_deduplicate_current_key()
    }
}

impl Iterator for BinaryIterator<'_> {
    fn seek(&mut self, key: &[u8]) -> engine_traits::Result<bool> {
        self.entry_cache.clear();
        for (i, iter) in self.sst_iters.iter_mut().enumerate() {
            if !iter.seek(key)? {
                // If seek fails or reaches end, skip this iterator
                continue;
            }
            self.entry_cache.push(i);
        }
        self.heap();
        self.normalize_current_key()
    }

    fn seek_for_prev(&mut self, _key: &[u8]) -> engine_traits::Result<bool> {
        unimplemented!("seek_for_prev is not implemented for BinaryIterator")
    }

    fn seek_to_first(&mut self) -> engine_traits::Result<bool> {
        self.entry_cache.clear();
        for (i, iter) in self.sst_iters.iter_mut().enumerate() {
            if !iter.seek_to_first()? {
                // Empty SST file, skip it
                continue;
            }
            self.entry_cache.push(i);
        }
        self.heap();
        self.normalize_current_key()
    }

    fn seek_to_last(&mut self) -> engine_traits::Result<bool> {
        unimplemented!("seek_to_last is not implemented for BinaryIterator")
    }

    fn prev(&mut self) -> engine_traits::Result<bool> {
        unimplemented!("prev is not implemented for BinaryIterator")
    }

    fn next(&mut self) -> engine_traits::Result<bool> {
        self.advance_current_key()?;
        self.normalize_current_key()
    }

    fn key(&self) -> &[u8] {
        self.sst_iters[self.peek()].key()
    }

    fn value(&self) -> &[u8] {
        self.sst_iters[self.peek()].value()
    }

    fn valid(&self) -> engine_traits::Result<bool> {
        Ok(!self.entry_cache.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, path::Path};

    use engine_rocks::RocksSstReader;
    use engine_traits::{
        IterOptions, Iterator, KvEngine, RefIterable, SstExt, SstReader, SstWriter,
        SstWriterBuilder,
    };
    use tempfile::TempDir;
    use tikv::storage::TestEngineBuilder;
    use txn_types::{Key, TimeStamp};

    use super::{BinaryIterator, DefaultSeekIterator};

    fn prepare_ssts<E: KvEngine>(name: &str, db: E, path: &Path, suffix_set: &mut HashSet<usize>) {
        let mut writer = <E as SstExt>::SstWriterBuilder::new()
            .set_cf(engine_traits::CF_WRITE)
            .set_db(&db)
            .set_compression_type(None)
            .set_compression_level(0)
            .build(path.join(name).to_str().unwrap())
            .unwrap();

        let mut suffix = 0;
        for _ in 0..10 {
            suffix += rand::random::<usize>() % 32 + 1;
            suffix_set.insert(suffix);
            let raw = format!("test{:010}", suffix);
            println!("{raw}");
            let mut key = Key::from_encoded(raw.into_bytes());
            key.append_ts_inplace(TimeStamp::new(10));
            let dkey = keys::data_key(key.as_encoded());
            let val = format!("for test {}, {}.", name, suffix);
            writer.put(&dkey, val.as_bytes()).unwrap();
        }

        writer.finish().unwrap();
    }

    fn read_ssts(names: Vec<String>, path: &Path, expected_kv_count: usize) {
        let mut readers = Vec::new();
        let mut iters = Vec::new();
        for name in names {
            let dst_file_name = path.join(name);
            let sst_reader =
                RocksSstReader::open_with_env(dst_file_name.to_str().unwrap(), None).unwrap();
            sst_reader.verify_checksum().unwrap();
            readers.push(sst_reader);
        }

        for reader in &readers {
            let iter = reader.iter(IterOptions::default()).unwrap();
            iters.push(iter);
        }

        let mut iter = BinaryIterator::new(iters);
        iter.seek_to_first().unwrap();

        let mut kv_count = 0;
        let mut last_key = Key::from_encoded(Vec::new());
        while iter.valid().unwrap() {
            let key = Key::from_encoded(iter.key().to_vec());
            assert!(last_key < key, "last = {}, current = {}", last_key, key);
            last_key = key;
            iter.next().unwrap();
            kv_count += 1;
        }
        assert_eq!(kv_count, expected_kv_count);
    }

    #[test]
    fn test_binary_iterator() {
        let temp = TempDir::new().unwrap();
        let rocks = TestEngineBuilder::new()
            .path(temp.path())
            .cfs([
                engine_traits::CF_DEFAULT,
                engine_traits::CF_LOCK,
                engine_traits::CF_WRITE,
            ])
            .build()
            .unwrap();
        let db = rocks.get_rocksdb();
        let mut names = Vec::new();
        let mut suffix_set = HashSet::new();
        for i in 0..5 {
            let name = format!("foo{i}");
            prepare_ssts(&name, db.clone(), temp.path(), &mut suffix_set);
            names.push(name);
        }

        read_ssts(names, temp.path(), suffix_set.len());
    }

    #[test]
    fn test_binary_iterator_skips_empty_value_key_groups() {
        let temp = TempDir::new().unwrap();
        let rocks = TestEngineBuilder::new()
            .path(temp.path())
            .cfs([
                engine_traits::CF_DEFAULT,
                engine_traits::CF_LOCK,
                engine_traits::CF_WRITE,
            ])
            .build()
            .unwrap();
        let db = rocks.get_rocksdb();

        let mk_key = |raw: &[u8], ts: u64| {
            let mut key = Key::from_encoded(raw.to_vec());
            key.append_ts_inplace(TimeStamp::new(ts));
            keys::data_key(key.as_encoded())
        };

        fn write_entries<E: KvEngine>(db: E, path: &Path, name: &str, entries: &[(&[u8], &[u8])]) {
            let mut writer = <E as SstExt>::SstWriterBuilder::new()
                .set_cf(engine_traits::CF_WRITE)
                .set_db(&db)
                .set_compression_type(None)
                .set_compression_level(0)
                .build(path.join(name).to_str().unwrap())
                .unwrap();
            for (k, v) in entries {
                writer.put(k, v).unwrap();
            }
            writer.finish().unwrap();
        }

        let k1 = mk_key(b"k1", 40);
        let k2 = mk_key(b"k2", 30);
        let k3 = mk_key(b"k3", 20);
        let k4 = mk_key(b"k4", 10);

        write_entries(
            db.clone(),
            temp.path(),
            "w1.sst",
            &[
                (k1.as_slice(), b"v1"),
                (k2.as_slice(), b"v2"),
                (k4.as_slice(), b""),
            ],
        );
        write_entries(
            db.clone(),
            temp.path(),
            "w2.sst",
            &[
                (k1.as_slice(), b""),
                (k2.as_slice(), b"v2-dup"),
                (k3.as_slice(), b"v3"),
            ],
        );

        let readers = [
            RocksSstReader::open_with_env(temp.path().join("w1.sst").to_str().unwrap(), None)
                .unwrap(),
            RocksSstReader::open_with_env(temp.path().join("w2.sst").to_str().unwrap(), None)
                .unwrap(),
        ];
        let iters: Vec<_> = readers
            .iter()
            .map(|reader| reader.iter(IterOptions::default()).unwrap())
            .collect();
        let mut iter = BinaryIterator::new(iters);

        assert!(iter.seek_to_first().unwrap());
        assert_eq!(iter.key(), k2.as_slice());
        assert!(iter.value() == b"v2" || iter.value() == b"v2-dup");
        assert!(iter.next().unwrap());
        assert_eq!(iter.key(), k3.as_slice());
        assert_eq!(iter.value(), b"v3");
        assert!(!iter.next().unwrap());

        assert!(iter.seek(&k1).unwrap());
        assert_eq!(iter.key(), k2.as_slice());
        assert!(iter.value() == b"v2" || iter.value() == b"v2-dup");
        assert!(!iter.seek(&k4).unwrap());
    }

    #[test]
    fn test_default_seek_iterator_seek_exact() {
        let temp = TempDir::new().unwrap();
        let rocks = TestEngineBuilder::new()
            .path(temp.path())
            .cfs([
                engine_traits::CF_DEFAULT,
                engine_traits::CF_LOCK,
                engine_traits::CF_WRITE,
            ])
            .build()
            .unwrap();
        let db = rocks.get_rocksdb();

        let mk_key = |raw: &[u8], ts: u64| {
            let mut key = Key::from_encoded(raw.to_vec());
            key.append_ts_inplace(TimeStamp::new(ts));
            keys::data_key(key.as_encoded())
        };

        fn write_entries<E: KvEngine>(db: E, path: &Path, name: &str, entries: &[(&[u8], &[u8])]) {
            let mut writer = <E as SstExt>::SstWriterBuilder::new()
                .set_cf(engine_traits::CF_DEFAULT)
                .set_db(&db)
                .set_compression_type(None)
                .set_compression_level(0)
                .build(path.join(name).to_str().unwrap())
                .unwrap();
            for (k, v) in entries {
                writer.put(k, v).unwrap();
            }
            writer.finish().unwrap();
        }

        // file a: k1@50, k2@30
        let d1k1 = mk_key(b"k1", 50);
        let d1k2 = mk_key(b"k2", 30);
        write_entries(
            db.clone(),
            temp.path(),
            "d1.sst",
            &[(d1k1.as_slice(), b"v1"), (d1k2.as_slice(), b"v2")],
        );

        // file b: k3@20
        let d2k1 = mk_key(b"k3", 20);
        write_entries(
            db.clone(),
            temp.path(),
            "d2.sst",
            &[(d2k1.as_slice(), b"v3")],
        );

        let r1 = RocksSstReader::open_with_env(temp.path().join("d1.sst").to_str().unwrap(), None)
            .unwrap();
        let r2 = RocksSstReader::open_with_env(temp.path().join("d2.sst").to_str().unwrap(), None)
            .unwrap();

        let it1 = r1.iter(IterOptions::default()).unwrap();
        let it2 = r2.iter(IterOptions::default()).unwrap();
        let mut seek_iter = DefaultSeekIterator::new(vec![it1, it2]);

        let k1 = mk_key(b"k1", 50);
        assert!(seek_iter.seek_exact(&k1).unwrap());
        assert_eq!(seek_iter.key(), k1.as_slice());
        assert_eq!(seek_iter.value(), b"v1");

        // Existing user key, but absent timestamp.
        let missing_ts = mk_key(b"k1", 40);
        assert!(!seek_iter.seek_exact(&missing_ts).unwrap());

        let k2 = mk_key(b"k2", 30);
        assert!(seek_iter.seek_exact(&k2).unwrap());
        assert_eq!(seek_iter.key(), k2.as_slice());
        assert_eq!(seek_iter.value(), b"v2");

        let k3 = mk_key(b"k3", 20);
        assert!(seek_iter.seek_exact(&k3).unwrap());
        assert_eq!(seek_iter.key(), k3.as_slice());
        assert_eq!(seek_iter.value(), b"v3");
    }
}
