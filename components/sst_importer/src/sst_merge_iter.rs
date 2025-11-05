// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::RocksSstIterator;
use engine_traits::Iterator;

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
        Ok(!self.entry_cache.is_empty())
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
        Ok(!self.entry_cache.is_empty())
    }

    fn seek_to_last(&mut self) -> engine_traits::Result<bool> {
        unimplemented!("seek_to_last is not implemented for BinaryIterator")
    }

    fn prev(&mut self) -> engine_traits::Result<bool> {
        unimplemented!("prev is not implemented for BinaryIterator")
    }

    fn next(&mut self) -> engine_traits::Result<bool> {
        if let Some(mut same_from) = self.pop() {
            // Handle duplicate keys from different SST files
            // This ensures deduplication: when multiple SSTs have the same key,
            // we advance all of them and keep only one value
            while !self.entry_cache.is_empty() && self.key() == self.sst_iters[same_from].key() {
                let from = self.pop().unwrap();
                let iter = &mut self.sst_iters[same_from];
                if iter.next()? {
                    self.push(same_from);
                }
                same_from = from;
            }

            // Advance the main iterator
            let iter = &mut self.sst_iters[same_from];
            if iter.next()? {
                self.push(same_from);
            }
        }
        Ok(!self.entry_cache.is_empty())
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

    use super::BinaryIterator;

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
}
