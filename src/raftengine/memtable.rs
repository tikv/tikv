
use std::u64;
use std::cmp;
use std::collections::VecDeque;

use kvproto::eraftpb::Entry;

use util::collections::HashMap;
use super::Result;
use util;

const SHRINK_CACHE_CAPACITY: usize = 64;

#[derive(Debug, PartialEq)]
pub struct FileIndex {
    pub file_num: u64,

    // last entry index in this file.
    pub last_index: u64,
}

impl FileIndex {
    pub fn new(file_num: u64, last_index: u64) -> FileIndex {
        FileIndex {
            file_num: file_num,
            last_index: last_index,
        }
    }
}

pub struct MemTable {
    region_id: u64,
    entry_queue: VecDeque<Entry>,
    entry_size: VecDeque<usize>,
    file_index: VecDeque<FileIndex>,

    // Region scope key/value pairs
    // key -> (value, file_num)
    kvs: HashMap<Vec<u8>, (Vec<u8>, u64)>,
}

impl MemTable {
    pub fn new(region_id: u64) -> MemTable {
        MemTable {
            region_id: region_id,
            entry_queue: VecDeque::with_capacity(SHRINK_CACHE_CAPACITY),
            entry_size: VecDeque::with_capacity(SHRINK_CACHE_CAPACITY),
            file_index: VecDeque::new(),
            kvs: HashMap::default(),
        }
    }

    pub fn append(&mut self, entries: Vec<Entry>, entries_size: Vec<usize>, file_num: u64) {
        if entries.is_empty() {
            return;
        }
        if entries.len() != entries_size.len() {
            panic!(
                "entries len {} not equal to entries_size len {}",
                entries.len(),
                entries_size.len()
            );
        }
        if let Some(cache_last_index) = self.entry_queue.back().map(|e| e.get_index()) {
            let first_index = entries[0].get_index();

            // Unlikely to happen
            if cache_last_index >= first_index {
                if self.entry_queue.front().unwrap().get_index() >= first_index {
                    self.entry_queue.clear();
                    self.entry_size.clear();
                    self.file_index.clear();
                } else {
                    let left =
                        self.entry_queue.len() - (cache_last_index - first_index + 1) as usize;
                    self.entry_queue.truncate(left);
                    self.entry_size.truncate(left);

                    let cache_last_index = self.entry_queue.back().unwrap().get_index();
                    self.truncate_file_index(cache_last_index);
                }
            } else if cache_last_index + 1 < first_index {
                panic!(
                    "entry cache of region {} contains unexpected hole: {} < {}",
                    self.region_id,
                    cache_last_index,
                    first_index
                );
            }
        }

        // Append entries.
        self.entry_queue.extend(entries);
        self.entry_size.extend(entries_size);

        // Update file index.
        let cache_last_index = self.entry_queue.back().unwrap().get_index();
        if !self.file_index.is_empty() && self.file_index.back().unwrap().file_num == file_num {
            self.file_index.back_mut().unwrap().last_index = cache_last_index;
        } else {
            self.file_index
                .push_back(FileIndex::new(file_num, cache_last_index));
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
        let cache_first_idx = match self.entry_queue.front() {
            None => return 0,
            Some(e) => e.get_index(),
        };

        if cache_first_idx >= idx {
            return 0;
        }
        let cache_last_idx = self.entry_queue.back().unwrap().get_index();
        let compact_count = cmp::min(cache_last_idx, idx) - cache_first_idx;
        self.entry_queue.drain(..compact_count as usize);
        self.entry_size.drain(..compact_count as usize);

        if self.entry_queue.len() < SHRINK_CACHE_CAPACITY &&
            self.entry_queue.capacity() > SHRINK_CACHE_CAPACITY
        {
            self.entry_queue.shrink_to_fit();
            self.entry_size.shrink_to_fit();
        }

        let first_index = match self.entry_queue.front() {
            Some(e) => e.get_index(),
            None => {
                self.file_index.clear();
                return compact_count;
            }
        };

        let pos = self.file_index_pos(first_index).unwrap_or_else(|| {
            panic!("Can't find file index for entry index: {}", first_index)
        });
        if pos > 0 {
            self.file_index.drain(..pos);
        }
        compact_count
    }

    pub fn get_entry(&self, index: u64) -> Option<Entry> {
        if self.entry_queue.is_empty() {
            return None;
        }

        let first_index = self.entry_queue.front().unwrap().get_index();
        let last_index = self.entry_queue.back().unwrap().get_index();
        if index < first_index || index > last_index {
            return None;
        }

        Some(self.entry_queue[(index - first_index) as usize].clone())
    }

    pub fn fetch_entries_to(
        &self,
        begin: u64,
        end: u64,
        max_size: Option<usize>,
        vec: &mut Vec<Entry>,
    ) -> Result<u64> {
        if end <= begin {
            return Err(box_err!(
                "Range error when fetch entries for region {}.",
                self.region_id
            ));
        }

        if self.entry_queue.is_empty() {
            return Err(box_err!("There is no entry for region {}.", self.region_id));
        }

        let first_index = self.entry_queue.front().unwrap().get_index();
        let last_index = self.entry_queue.back().unwrap().get_index();
        if begin < first_index || end > last_index + 1 {
            return Err(box_err!(
                "Wanted entries [{}, {}) out of range [{}, {})",
                begin,
                end,
                first_index,
                last_index + 1
            ));
        }

        let start_idx = (begin - first_index) as usize;
        let end_idx = cmp::min((end - begin) as usize + start_idx, self.entry_queue.len());
        let (first, second) = util::slices_in_range(&self.entry_queue, start_idx, end_idx);
        match max_size {
            Some(max_size) => {
                let count_limit = self.count_limit(start_idx, end_idx, max_size);
                if first.len() >= count_limit {
                    vec.extend_from_slice(&first[..count_limit]);
                } else {
                    vec.extend_from_slice(first);
                    let left = count_limit - first.len();
                    vec.extend_from_slice(&second[..left]);
                }
                Ok(count_limit as u64)
            }
            None => {
                vec.extend_from_slice(first);
                vec.extend_from_slice(second);
                Ok(first.len() as u64 + second.len() as u64)
            }
        }
    }


    pub fn fetch_all(&self, vec: &mut Vec<Entry>) {
        let (first, second) = util::slices_in_range(&self.entry_queue, 0, self.entry_queue.len());
        vec.extend_from_slice(first);
        vec.extend_from_slice(second);
    }

    pub fn fetch_all_kvs(&self, vec: &mut Vec<(Vec<u8>, Vec<u8>)>) {
        for (key, value) in &self.kvs {
            vec.push((key.clone(), value.0.clone()));
        }
    }

    pub fn min_file_num(&self) -> Option<u64> {
        let ents_min = self.file_index.front().map(|index| index.file_num);
        let kvs_min = self.kvs_min_file_num();
        match (ents_min, kvs_min) {
            (Some(ents_min), Some(kvs_min)) => Some(cmp::min(ents_min, kvs_min)),
            (Some(ents_min), None) => Some(ents_min),
            (None, Some(kvs_min)) => Some(kvs_min),
            (None, None) => None,
        }
    }

    pub fn max_file_num(&self) -> Option<u64> {
        let ents_max = self.file_index.back().map(|index| index.file_num);
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
        self.entry_queue.len()
    }

    pub fn entries_size(&self) -> usize {
        self.entry_size.iter().fold(0, |sum, val| sum + val)
    }

    pub fn entries_count_before_file(&self, file_num: u64) -> usize {
        let mut last_index_before_file = 0;
        for idx in &self.file_index {
            if idx.file_num >= file_num {
                break;
            }
            last_index_before_file = idx.last_index;
        }

        // No entries before the file whose file number is `file_num`
        if last_index_before_file == 0 {
            return 0;
        }

        let first_index = match self.entry_queue.front() {
            Some(e) => e.get_index(),
            None => panic!("Expected has entries, but empty."),
        };
        assert!(last_index_before_file >= first_index);
        (last_index_before_file - first_index + 1) as usize
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

    fn file_index_pos(&self, entry_index: u64) -> Option<usize> {
        for (pos, index) in self.file_index.iter().enumerate() {
            if index.last_index >= entry_index {
                return Some(pos);
            }
        }
        None
    }

    fn truncate_file_index(&mut self, cache_last_index: u64) {
        let pos = self.file_index_pos(cache_last_index).unwrap_or_else(|| {
            panic!(
                "Can't find associated file index for entry index: {}",
                cache_last_index
            )
        });
        self.file_index[pos].last_index = cache_last_index;
        self.file_index.drain(pos + 1..);
    }

    fn count_limit(&self, start_idx: usize, end_idx: usize, max_size: usize) -> usize {
        assert!(start_idx < end_idx);
        let (first, second) = util::slices_in_range(&self.entry_size, start_idx, end_idx);

        let mut count = 0;
        let mut total_size = 0;
        for size in first {
            count += 1;
            total_size += *size;
            if total_size > max_size {
                // No matter max_size's value, fetch one entry at lease.
                return if count > 1 { count - 1 } else { count };
            }

        }
        for size in second {
            count += 1;
            total_size += *size;
            if total_size > max_size {
                return if count > 1 { count - 1 } else { count };
            }
        }
        count
    }
}


#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;

    use kvproto::eraftpb::Entry;

    #[test]
    fn test_memtable() {
        let mut memtable = MemTable::new(8 /* region_id */);

        // append entries [0, 10) file_num = 1
        // append entries [10, 20) file_num = 1
        // append entries [20, 30) file_num = 2
        memtable.append(generate_ents(0, 10), vec![1; 10], 1);
        memtable.append(generate_ents(10, 20), vec![1; 10], 1);
        memtable.append(generate_ents(20, 30), vec![1; 10], 2);
        assert_eq!(
            memtable.file_index,
            VecDeque::from(vec![FileIndex::new(1, 19), FileIndex::new(2, 29)])
        );
        assert_eq!(memtable.entry_queue.len(), 30);
        assert_eq!(memtable.entry_queue[0].get_index(), 0);
        assert_eq!(memtable.entry_queue[29].get_index(), 29);
        assert_eq!(memtable.min_file_num().unwrap(), 1);
        assert_eq!(memtable.max_file_num().unwrap(), 2);

        memtable.compact_to(15);
        assert_eq!(
            memtable.file_index,
            VecDeque::from(vec![FileIndex::new(1, 19), FileIndex::new(2, 29)])
        );
        assert_eq!(memtable.entry_queue.len(), 15);
        assert_eq!(memtable.entry_queue[0].get_index(), 15);
        assert_eq!(memtable.entry_queue[14].get_index(), 29);
        assert_eq!(memtable.min_file_num().unwrap(), 1);
        assert_eq!(memtable.max_file_num().unwrap(), 2);

        memtable.compact_to(25);
        assert_eq!(
            memtable.file_index,
            VecDeque::from(vec![FileIndex::new(2, 29)])
        );
        assert_eq!(memtable.entry_queue.len(), 5);
        assert_eq!(memtable.entry_queue[0].get_index(), 25);
        assert_eq!(memtable.entry_queue[4].get_index(), 29);
        assert_eq!(memtable.min_file_num().unwrap(), 2);
        assert_eq!(memtable.max_file_num().unwrap(), 2);

        memtable.compact_to(24);
        assert_eq!(
            memtable.file_index,
            VecDeque::from(vec![FileIndex::new(2, 29)])
        );
        assert_eq!(memtable.entry_queue.len(), 5);
        assert_eq!(memtable.entry_queue[0].get_index(), 25);
        assert_eq!(memtable.entry_queue[4].get_index(), 29);
        assert_eq!(memtable.min_file_num().unwrap(), 2);
        assert_eq!(memtable.max_file_num().unwrap(), 2);

        // append entries [28, 38) file_num = 3
        memtable.append(generate_ents(28, 38), vec![1; 10], 3);
        assert_eq!(
            memtable.file_index,
            VecDeque::from(vec![FileIndex::new(2, 27), FileIndex::new(3, 37)])
        );
        assert_eq!(memtable.entry_queue.len(), 13);
        assert_eq!(memtable.entry_queue[0].get_index(), 25);
        assert_eq!(memtable.entry_queue[12].get_index(), 37);
        assert_eq!(memtable.min_file_num().unwrap(), 2);
        assert_eq!(memtable.max_file_num().unwrap(), 3);

        // has entries [25, 38)
        let mut ents = vec![];
        memtable.fetch_all(&mut ents);
        assert_eq!(ents.len(), 13);
        assert_eq!(ents[0].get_index(), 25);
        assert_eq!(ents[12].get_index(), 37);

        ents.clear();
        // out of range
        assert!(memtable.fetch_entries_to(24, 37, None, &mut ents).is_err());
        // out of range
        assert!(memtable.fetch_entries_to(35, 45, None, &mut ents).is_err());
        // no max size limitation
        assert_eq!(
            memtable.fetch_entries_to(25, 38, None, &mut ents).unwrap(),
            13
        );
        assert_eq!(ents[0].get_index(), 25);
        assert_eq!(ents[12].get_index(), 37);

        // max size limit to 5
        ents.clear();
        assert_eq!(
            memtable
                .fetch_entries_to(25, 38, Some(5), &mut ents)
                .unwrap(),
            5
        );
        assert_eq!(ents[0].get_index(), 25);
        assert_eq!(ents[4].get_index(), 29);

        // even max size limit is 0, the first entry should be return
        ents.clear();
        assert_eq!(
            memtable
                .fetch_entries_to(25, 38, Some(0), &mut ents)
                .unwrap(),
            1
        );
        assert_eq!(ents[0].get_index(), 25);

        ents.clear();
        assert_eq!(
            memtable.fetch_entries_to(30, 38, None, &mut ents).unwrap(),
            8
        );
        assert_eq!(ents[0].get_index(), 30);
        assert_eq!(ents[7].get_index(), 37);

        // append entries [20, 40) file_num = 3
        memtable.append(generate_ents(20, 40), vec![1; 20], 3);
        assert_eq!(
            memtable.file_index,
            VecDeque::from(vec![FileIndex::new(3, 39)])
        );
        assert_eq!(memtable.entry_queue.len(), 20);
        assert_eq!(memtable.entry_queue[0].get_index(), 20);
        assert_eq!(memtable.entry_queue[19].get_index(), 39);
        assert_eq!(memtable.min_file_num().unwrap(), 3);
        assert_eq!(memtable.max_file_num().unwrap(), 3);

        // put
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
}
