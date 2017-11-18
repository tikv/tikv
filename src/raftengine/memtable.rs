
use std::u64;
use std::cmp;
use std::collections::VecDeque;

use kvproto::eraftpb::Entry;

use util::collections::HashMap;
use super::Result;
use util;

const SHRINK_CACHE_CAPACITY: usize = 64;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct EntryIndex {
    pub index: u64,

    // Entry physical position in file.
    pub file_num: u64,
    pub offset: u64,
    pub len: u64,
}

impl EntryIndex {
    pub fn new(index: u64, file_num: u64, offset: u64, len: u64) -> EntryIndex {
        EntryIndex {
            index: index,
            file_num: file_num,
            offset: offset,
            len: len,
        }
    }
}

pub struct MemTable {
    region_id: u64,

    // latest N entries
    entries_cache: VecDeque<Entry>,
    cache_size: u64,
    cache_limit: u64,

    // All entries index
    entries_index: VecDeque<EntryIndex>,
    total_size: u64,

    // Region scope key/value pairs
    // key -> (value, file_num)
    kvs: HashMap<Vec<u8>, (Vec<u8>, u64)>,
}

impl MemTable {
    pub fn new(region_id: u64, cache_limit: u64) -> MemTable {
        MemTable {
            region_id: region_id,
            entries_cache: VecDeque::with_capacity(SHRINK_CACHE_CAPACITY),
            cache_size: 0,
            cache_limit: cache_limit,
            entries_index: VecDeque::with_capacity(SHRINK_CACHE_CAPACITY),
            total_size: 0,
            kvs: HashMap::default(),
        }
    }

    pub fn append(&mut self, entries: Vec<Entry>, entries_index: Vec<EntryIndex>) {
        if entries.is_empty() {
            return;
        }
        if entries.len() != entries_index.len() {
            panic!(
                "entries len {} not equal to entries_index len {}",
                entries.len(),
                entries_index.len()
            );
        }

        // last_index in entries_cache is equal to last_index in entries_index
        if let Some(cache_last_index) = self.entries_cache.back().map(|e| e.get_index()) {
            let add_first_index = entries[0].get_index();

            // Unlikely to happen
            if cache_last_index >= add_first_index {
                if self.entries_cache.front().unwrap().get_index() >= add_first_index {
                    // clear all cache
                    self.entries_cache.clear();
                    self.cache_size = 0;

                    let first_index = self.entries_index.front().unwrap().index;
                    if first_index >= add_first_index {
                        // clear all indices
                        self.entries_index.clear();
                        self.total_size = 0;
                    } else {
                        // truncate tail indices
                        let left = (add_first_index - first_index) as usize;
                        let delta_size = self.entries_index
                            .drain(left..)
                            .fold(0, |acc, i| acc + i.len);
                        assert!(self.total_size >= delta_size);
                        self.total_size -= delta_size;
                    }
                } else {
                    // truncate tail entries
                    let left = self.entries_cache.len() -
                        (cache_last_index - add_first_index + 1) as usize;
                    self.entries_cache.truncate(left);
                    let delta_size = self.entries_index
                        .drain(left..)
                        .fold(0, |acc, i| acc + i.len);
                    self.cache_size -= delta_size;
                    self.total_size -= delta_size;
                }
            } else if cache_last_index + 1 < add_first_index {
                panic!(
                    "entry cache of region {} contains unexpected hole: {} < {}",
                    self.region_id,
                    cache_last_index,
                    add_first_index
                );
            }
        }

        let delta_size = entries_index.iter().fold(0, |acc, i| acc + i.len);
        self.entries_cache.extend(entries);
        self.entries_index.extend(entries_index);
        self.cache_size += delta_size;
        self.total_size += delta_size;

        // Evict front entries from cache
        while self.cache_size > self.cache_limit && self.entries_cache.len() > 1 {
            let distance = self.entries_index.len() - self.entries_cache.len();
            let entry = self.entries_cache.pop_front().unwrap();
            assert_eq!(entry.get_index(), self.entries_index[distance].index);
            self.cache_size -= self.entries_index[distance].len;
        }
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>, file_num: u64) {
        self.kvs.insert(key, (value, file_num));
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.kvs.remove(key);
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.kvs.get(key).map(|v| v.0.clone())
    }

    pub fn compact_to(&mut self, idx: u64) -> u64 {
        if let Some(first_index) = self.entries_index.front().map(|i| i.index) {
            if first_index >= idx {
                return 0;
            }

            let last_index = self.entries_index.back().unwrap().index;
            if idx > last_index {
                panic!(
                    "compact to index {} is larger than last index {}",
                    idx,
                    last_index
                );
            }

            // Compact entries index.
            let compact_size = self.entries_index
                .drain(..(idx - first_index) as usize)
                .fold(0, |acc, i| acc + i.len);
            self.total_size -= compact_size;
            if self.entries_index.len() < SHRINK_CACHE_CAPACITY &&
                self.entries_index.capacity() > SHRINK_CACHE_CAPACITY
            {
                self.entries_index.shrink_to_fit();
            }

            // Compact cache when needed.
            if let Some(cache_first_index) = self.entries_cache.front().map(|i| i.get_index()) {
                if idx > cache_first_index {
                    self.entries_cache
                        .drain(..(idx - cache_first_index) as usize);
                    // All entries are in cache.
                    self.cache_size = self.total_size;

                    if self.entries_cache.len() < SHRINK_CACHE_CAPACITY &&
                        self.entries_cache.capacity() > SHRINK_CACHE_CAPACITY
                    {
                        self.entries_cache.shrink_to_fit();
                    }
                }
            }
            return idx - first_index;
        }
        0
    }

    // If entry exist in cache, return (Entry, None).
    // If entry exist but not in cache, return (None, EntryIndex).
    // If entry not exist, return (None, None).
    pub fn get_entry(&self, index: u64) -> Option<(Option<Entry>, Option<EntryIndex>)> {
        // Empty.
        if self.entries_index.is_empty() {
            return None;
        }

        // Out of range.
        let first_index = self.entries_index.front().unwrap().index;
        let last_index = self.entries_index.back().unwrap().index;
        if index < first_index || index > last_index {
            return None;
        }

        // Not in cache.
        let ioffset = (index - first_index) as usize;
        if self.entries_cache.is_empty() {
            // Todo: actually, shouldn't be empty when entries_index is not empty.
            return Some((None, Some(self.entries_index[ioffset].clone())));
        }

        // Not in ache.
        // cache_last_index must be equal to last_index, so don't compare to
        // cache_last_index here.
        let cache_first_index = self.entries_cache.front().unwrap().get_index();
        if index < cache_first_index {
            return Some((None, Some(self.entries_index[ioffset].clone())));
        }

        // Found in cache
        let eoffset = (index - cache_first_index) as usize;
        Some((Some(self.entries_cache[eoffset].clone()), None))
    }

    pub fn fetch_entries_to(
        &self,
        begin: u64,
        end: u64,
        max_size: Option<usize>,
        vec: &mut Vec<Entry>,
        vec_idx: &mut Vec<EntryIndex>,
    ) -> Result<u64> {
        if end <= begin {
            return Err(box_err!(
                "Range error when fetch entries for region {}.",
                self.region_id
            ));
        }

        if self.entries_index.is_empty() {
            return Err(box_err!("There is no entry for region {}.", self.region_id));
        }

        let first_index = self.entries_index.front().unwrap().index;
        let last_index = self.entries_index.back().unwrap().index;
        if begin < first_index || end > last_index + 1 {
            return Err(box_err!(
                "Wanted entries [{}, {}) out of range [{}, {})",
                begin,
                end,
                first_index,
                last_index + 1
            ));
        }

        // Check max size limitation.
        let start_pos = (begin - first_index) as usize;
        let mut end_pos = (end - begin) as usize + start_pos;
        let count_limit = self.count_limit(
            start_pos,
            end_pos,
            max_size.map_or(u64::MAX as usize, |max| max),
        );
        end_pos = start_pos + count_limit;

        // Has some entries in cache.
        let cache_first_index = self.entries_cache.front().unwrap().get_index();
        let cache_offset = (cache_first_index - first_index) as usize;
        if cache_offset < end_pos {
            if start_pos >= cache_offset {
                // All needed entries are in cache.
                let (first, second) = util::slices_in_range(
                    &self.entries_cache,
                    start_pos - cache_offset,
                    end_pos - cache_offset,
                );
                vec.extend_from_slice(first);
                vec.extend_from_slice(second);
                Ok(count_limit as u64)
            } else {
                // Partial needed entries are in cache.
                let (first, second) =
                    util::slices_in_range(&self.entries_cache, 0, end_pos - cache_offset);
                let fetch_count = (first.len() + second.len()) as u64;
                vec.extend_from_slice(first);
                vec.extend_from_slice(second);

                // Entries that not in cache should return their indices.
                let (first, second) =
                    util::slices_in_range(&self.entries_index, start_pos, cache_offset);
                vec_idx.extend_from_slice(first);
                vec_idx.extend_from_slice(second);
                Ok(fetch_count)
            }
        } else {
            // All needed entries are not in cache
            let (first, second) = util::slices_in_range(&self.entries_index, start_pos, end_pos);
            vec_idx.extend_from_slice(first);
            vec_idx.extend_from_slice(second);
            Ok(0)
        }
    }

    pub fn fetch_all(&self, vec: &mut Vec<Entry>, vec_idx: &mut Vec<EntryIndex>) {
        if self.entries_index.is_empty() {
            return;
        }

        // Fetch all entries in cache
        let (first, second) =
            util::slices_in_range(&self.entries_cache, 0, self.entries_cache.len());
        vec.extend_from_slice(first);
        vec.extend_from_slice(second);

        // Fetch remain entries index
        let first_index = self.entries_index.front().unwrap().index;
        let cache_first_index = self.entries_cache.front().unwrap().get_index();
        if first_index < cache_first_index {
            let (first, second) = util::slices_in_range(
                &self.entries_index,
                0,
                (cache_first_index - first_index) as usize,
            );
            vec_idx.extend_from_slice(first);
            vec_idx.extend_from_slice(second);
        }
    }

    pub fn fetch_all_kvs(&self, vec: &mut Vec<(Vec<u8>, Vec<u8>)>) {
        for (key, value) in &self.kvs {
            vec.push((key.clone(), value.0.clone()));
        }
    }

    pub fn min_file_num(&self) -> Option<u64> {
        let ents_min = self.entries_index.front().map(|idx| idx.file_num);
        let kvs_min = self.kvs_min_file_num();
        match (ents_min, kvs_min) {
            (Some(ents_min), Some(kvs_min)) => Some(cmp::min(ents_min, kvs_min)),
            (Some(ents_min), None) => Some(ents_min),
            (None, Some(kvs_min)) => Some(kvs_min),
            (None, None) => None,
        }
    }

    pub fn max_file_num(&self) -> Option<u64> {
        let ents_max = self.entries_index.back().map(|idx| idx.file_num);
        let kvs_max = self.kvs_max_file_num();
        match (ents_max, kvs_max) {
            (Some(ents_max), Some(kvs_max)) => Some(cmp::max(ents_max, kvs_max)),
            (Some(ents_max), None) => Some(ents_max),
            (None, Some(kvs_max)) => Some(kvs_max),
            (None, None) => None,
        }
    }

    pub fn kvs_total_count(&self) -> usize {
        self.kvs.len()
    }

    pub fn entries_count(&self) -> usize {
        self.entries_index.len()
    }

    pub fn entries_size(&self) -> u64 {
        self.total_size
    }

    pub fn cache_size(&self) -> u64 {
        self.cache_size
    }

    pub fn all_in_cache(&self) -> bool {
        self.entries_cache.len() == self.entries_index.len()
    }

    // Evict entries before `boundary_file_num` from cache.
    pub fn evict_old_from_cache(&mut self, boundary_file_num: u64) {
        if self.entries_index.is_empty() {
            return;
        }

        // No cached entries behind `boundary_file_num`, this is the mostly case.
        let first_file_num = self.entries_index.front().unwrap().file_num;
        if first_file_num >= boundary_file_num {
            return;
        }

        let first_index = self.entries_index.front().unwrap().index;
        let cache_first_index = self.entries_cache.front().unwrap().get_index();
        let mut cache_offset = (cache_first_index - first_index) as usize;

        // At lease keep one entry in cache
        while self.entries_cache.len() > 2 {
            let idx = &self.entries_index[cache_offset];
            if idx.file_num >= boundary_file_num {
                break;
            }

            let e = self.entries_cache.pop_front().unwrap();
            if e.get_index() != idx.index {
                panic!(
                    "Unexpected entry index {} from cache, expect {} from index",
                    e.get_index(),
                    idx.index
                );
            }

            self.cache_size -= idx.len;
            cache_offset += 1;
        }
    }

    pub fn region_id(&self) -> u64 {
        self.region_id
    }

    fn kvs_min_file_num(&self) -> Option<u64> {
        if self.kvs.is_empty() {
            return None;
        }
        Some(
            self.kvs
                .values()
                .fold(u64::MAX, |min, v| cmp::min(min, v.1)),
        )
    }

    fn kvs_max_file_num(&self) -> Option<u64> {
        if self.kvs.is_empty() {
            return None;
        }
        Some(self.kvs.values().fold(0, |max, v| cmp::max(max, v.1)))
    }

    fn count_limit(&self, start_idx: usize, end_idx: usize, max_size: usize) -> usize {
        assert!(start_idx < end_idx);
        let (first, second) = util::slices_in_range(&self.entries_index, start_idx, end_idx);

        let mut count = 0;
        let mut total_size = 0;
        for i in first {
            count += 1;
            total_size += i.len;
            if total_size as usize > max_size {
                // No matter max_size's value, fetch one entry at lease.
                return if count > 1 { count - 1 } else { count };
            }
        }
        for i in second {
            count += 1;
            total_size += i.len;
            if total_size as usize > max_size {
                return if count > 1 { count - 1 } else { count };
            }
        }
        count
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    use kvproto::eraftpb::Entry;

    #[test]
    fn test_memtable() {
        let mut memtable = MemTable::new(8 /* region_id */, 100 * 1024 * 1024);

        // append entries [0, 10) file_num = 1
        // append entries [10, 20) file_num = 1
        // append entries [20, 30) file_num = 2
        memtable.append(generate_ents(0, 10), generate_ents_index(0, 10, 1));
        memtable.append(generate_ents(10, 20), generate_ents_index(10, 20, 1));
        memtable.append(generate_ents(20, 30), generate_ents_index(20, 30, 2));
        assert_eq!(memtable.entries_index.len(), 30);
        assert_eq!(memtable.entries_index[0].index, 0);
        assert_eq!(memtable.entries_index[29].index, 29);
        assert_eq!(memtable.entries_index[0].file_num, 1);
        assert_eq!(memtable.entries_index[29].file_num, 2);

        assert_eq!(memtable.entries_cache.len(), 30);
        assert_eq!(memtable.entries_cache[0].get_index(), 0);
        assert_eq!(memtable.entries_cache[29].get_index(), 29);
        assert_eq!(memtable.min_file_num().unwrap(), 1);
        assert_eq!(memtable.max_file_num().unwrap(), 2);

        assert_eq!(memtable.compact_to(15), 15);
        assert_eq!(memtable.entries_index.len(), 15);
        assert_eq!(memtable.entries_index[0].index, 15);
        assert_eq!(memtable.entries_index[5].index, 20);
        assert_eq!(memtable.entries_index[14].index, 29);
        assert_eq!(memtable.entries_index[0].file_num, 1);
        assert_eq!(memtable.entries_index[5].file_num, 2);
        assert_eq!(memtable.entries_index[14].file_num, 2);

        assert_eq!(memtable.entries_cache.len(), 15);
        assert_eq!(memtable.entries_cache[0].get_index(), 15);
        assert_eq!(memtable.entries_cache[14].get_index(), 29);
        assert_eq!(memtable.min_file_num().unwrap(), 1);
        assert_eq!(memtable.max_file_num().unwrap(), 2);

        assert_eq!(memtable.compact_to(25), 10);
        assert_eq!(memtable.entries_index.len(), 5);
        assert_eq!(memtable.entries_index[0].index, 25);
        assert_eq!(memtable.entries_index[4].index, 29);
        assert_eq!(memtable.entries_index[0].file_num, 2);
        assert_eq!(memtable.entries_index[4].file_num, 2);

        assert_eq!(memtable.entries_cache.len(), 5);
        assert_eq!(memtable.entries_cache[0].get_index(), 25);
        assert_eq!(memtable.entries_cache[4].get_index(), 29);
        assert_eq!(memtable.min_file_num().unwrap(), 2);
        assert_eq!(memtable.max_file_num().unwrap(), 2);

        assert_eq!(memtable.compact_to(24), 0);

        // append entries [28, 38) file_num = 3
        memtable.append(generate_ents(28, 38), generate_ents_index(28, 38, 3));
        assert_eq!(memtable.entries_index.len(), 13);
        assert_eq!(memtable.entries_index[0].index, 25);
        assert_eq!(memtable.entries_index[12].index, 37);
        assert_eq!(memtable.entries_index[0].file_num, 2);
        assert_eq!(memtable.entries_index[3].file_num, 3);
        assert_eq!(memtable.entries_index[12].file_num, 3);

        assert_eq!(memtable.entries_cache.len(), 13);
        assert_eq!(memtable.entries_cache[0].get_index(), 25);
        assert_eq!(memtable.entries_cache[12].get_index(), 37);
        assert_eq!(memtable.min_file_num().unwrap(), 2);
        assert_eq!(memtable.max_file_num().unwrap(), 3);

        // has entries [25, 38), and all in cache.
        let mut ents = vec![];
        let mut ents_idx = vec![];
        memtable.fetch_all(&mut ents, &mut ents_idx);
        assert_eq!(ents.len(), 13);
        assert_eq!(ents[0].get_index(), 25);
        assert_eq!(ents[12].get_index(), 37);
        assert!(ents_idx.is_empty());

        ents.clear();
        // out of range
        assert!(
            memtable
                .fetch_entries_to(24, 37, None, &mut ents, &mut ents_idx)
                .is_err()
        );
        // out of range
        assert!(
            memtable
                .fetch_entries_to(35, 45, None, &mut ents, &mut ents_idx)
                .is_err()
        );
        // no max size limitation
        assert_eq!(
            memtable
                .fetch_entries_to(25, 38, None, &mut ents, &mut ents_idx)
                .unwrap(),
            13
        );
        assert_eq!(ents[0].get_index(), 25);
        assert_eq!(ents[12].get_index(), 37);
        assert!(ents_idx.is_empty());

        // max size limit to 5
        ents.clear();
        assert_eq!(
            memtable
                .fetch_entries_to(25, 38, Some(5), &mut ents, &mut ents_idx)
                .unwrap(),
            5
        );
        assert_eq!(ents[0].get_index(), 25);
        assert_eq!(ents[4].get_index(), 29);
        assert!(ents_idx.is_empty());

        // even max size limit is 0, the first entry should be return
        ents.clear();
        assert_eq!(
            memtable
                .fetch_entries_to(25, 38, Some(0), &mut ents, &mut ents_idx)
                .unwrap(),
            1
        );
        assert_eq!(ents[0].get_index(), 25);
        assert!(ents_idx.is_empty());

        ents.clear();
        assert_eq!(
            memtable
                .fetch_entries_to(30, 38, None, &mut ents, &mut ents_idx)
                .unwrap(),
            8
        );
        assert_eq!(ents[0].get_index(), 30);
        assert_eq!(ents[7].get_index(), 37);
        assert!(ents_idx.is_empty());

        // Evict entries that in file 2 from cache
        assert!(memtable.all_in_cache());
        ents.clear();
        memtable.evict_old_from_cache(3);
        assert!(!memtable.all_in_cache());
        assert_eq!(memtable.entries_index.len(), 13);
        assert_eq!(memtable.entries_index[0].index, 25);
        assert_eq!(memtable.entries_index[12].index, 37);
        assert_eq!(memtable.entries_cache.len(), 10);
        assert_eq!(memtable.entries_cache[0].get_index(), 28);
        assert_eq!(memtable.entries_cache[9].get_index(), 37);
        assert_eq!(
            memtable
                .fetch_entries_to(25, 38, None, &mut ents, &mut ents_idx)
                .unwrap(),
            10
        );
        assert_eq!(ents.len(), 10);
        assert_eq!(ents[0].get_index(), 28);
        assert_eq!(ents[9].get_index(), 37);
        assert_eq!(ents_idx.len(), 3);
        assert_eq!(ents_idx[0].index, 25);
        assert_eq!(ents_idx[0].file_num, 2);
        assert_eq!(ents_idx[2].index, 27);
        assert_eq!(ents_idx[2].file_num, 2);

        // append entries [20, 40) file_num = 3
        memtable.append(generate_ents(20, 40), generate_ents_index(20, 40, 3));
        assert_eq!(memtable.entries_index.len(), 20);
        assert_eq!(memtable.entries_index[0].index, 20);
        assert_eq!(memtable.entries_index[19].index, 39);
        assert_eq!(memtable.entries_cache.len(), 20);
        assert_eq!(memtable.entries_cache[0].get_index(), 20);
        assert_eq!(memtable.entries_cache[19].get_index(), 39);
        assert_eq!(memtable.min_file_num().unwrap(), 3);
        assert_eq!(memtable.max_file_num().unwrap(), 3);

        // put key value pairs
        let (k1, v1) = (b"key1", b"value1");
        let (k5, v5) = (b"key5", b"value5");
        memtable.put(k1.to_vec(), v1.to_vec(), 1);
        memtable.put(k5.to_vec(), v5.to_vec(), 5);
        assert_eq!(memtable.min_file_num().unwrap(), 1);
        assert_eq!(memtable.max_file_num().unwrap(), 5);
        assert_eq!(memtable.get(k1.as_ref()), Some(v1.to_vec()));
        assert_eq!(memtable.get(k5.as_ref()), Some(v5.to_vec()));
    }

    fn generate_ents(begin_idx: u64, end_idx: u64) -> Vec<Entry> {
        assert!(end_idx >= begin_idx);
        let mut ents = vec![];
        for idx in begin_idx..end_idx {
            let mut ent = Entry::new();
            ent.set_index(idx);
            ents.push(ent);
        }
        ents
    }

    fn generate_ents_index(begin_idx: u64, end_idx: u64, file_num: u64) -> Vec<EntryIndex> {
        assert!(end_idx >= begin_idx);
        let mut ents_idx = vec![];
        for idx in begin_idx..end_idx {
            let mut ent_idx = EntryIndex::default();
            ent_idx.index = idx;
            ent_idx.file_num = file_num;
            ent_idx.offset = idx; // fake offset
            ent_idx.len = 1; // fake size
            ents_idx.push(ent_idx);
        }
        ents_idx
    }
}
