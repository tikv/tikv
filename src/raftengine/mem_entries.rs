
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

pub struct MemEntries {
    pub region_id: u64,
    pub entry_queue: VecDeque<Entry>,
    pub file_index: VecDeque<FileIndex>,

    // Region scope key/value pairs
    // key -> (value, file_num)
    pub kvs: HashMap<Vec<u8>, (Vec<u8>, u64)>,
}

impl MemEntries {
    pub fn new(region_id: u64) -> MemEntries {
        MemEntries {
            region_id: region_id,
            entry_queue: VecDeque::with_capacity(SHRINK_CACHE_CAPACITY),
            file_index: VecDeque::new(),
            kvs: HashMap::default(),
        }
    }

    pub fn append(&mut self, entries: Vec<Entry>, file_num: u64) {
        if entries.is_empty() {
            return;
        }
        if let Some(cache_last_index) = self.entry_queue.back().map(|e| e.get_index()) {
            let first_index = entries[0].get_index();

            // Unlikely to happen
            if cache_last_index >= first_index {
                if self.entry_queue.front().unwrap().get_index() >= first_index {
                    self.entry_queue.clear();
                    self.file_index.clear();
                } else {
                    let left =
                        self.entry_queue.len() - (cache_last_index - first_index + 1) as usize;
                    self.entry_queue.truncate(left);

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

        // Update file index.
        let cache_last_index = self.entry_queue.back().unwrap().get_index();
        if !self.file_index.is_empty() && self.file_index.back().unwrap().file_num == file_num {
            self.file_index.back_mut().unwrap().last_index = cache_last_index;
        } else {
            self.file_index
                .push_back(FileIndex::new(file_num, cache_last_index));
        }
    }

    pub fn add_kv(&mut self, key: Vec<u8>, value: Vec<u8>, file_num: u64) {
        self.kvs.insert(key, (value, file_num));
    }

    pub fn compact_to(&mut self, idx: u64) {
        let cache_first_idx = match self.entry_queue.front() {
            None => return,
            Some(e) => e.get_index(),
        };

        if cache_first_idx >= idx {
            return;
        }
        let cache_last_idx = self.entry_queue.back().unwrap().get_index();
        self.entry_queue
            .drain(..(cmp::min(cache_last_idx, idx) - cache_first_idx) as usize);

        if self.entry_queue.len() < SHRINK_CACHE_CAPACITY &&
            self.entry_queue.capacity() > SHRINK_CACHE_CAPACITY
        {
            self.entry_queue.shrink_to_fit();
        }

        let first_index = match self.entry_queue.front() {
            Some(e) => e.get_index(),
            None => {
                self.file_index.clear();
                return;
            }
        };

        let pos = self.file_index_pos(first_index).unwrap_or_else(|| {
            panic!("Can't find file index for entry index: {}", first_index)
        });
        if pos > 0 {
            self.file_index.drain(..pos);
        }
    }

    pub fn fetch_entries(&self, begin: u64, end: u64, vec: &mut Vec<Entry>) -> Result<()> {
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
        if begin < first_index {
            return Err(box_err!(
                "First entry's index is larger than wanted index. \
                 first index {}, wanted first index {}, region {}",
                first_index,
                begin,
                self.region_id
            ));
        }
        if begin > last_index {
            return Err(box_err!(
                "Wanted first index is larger than last index. \
                 last index {}, wanted first index {}, region {}",
                last_index,
                begin,
                self.region_id
            ));
        }

        let start_idx = (begin - first_index) as usize;
        let end_idx = cmp::min((end - begin) as usize + start_idx, self.entry_queue.len());
        let (first, second) = util::slices_in_range(&self.entry_queue, start_idx, end_idx);
        vec.extend_from_slice(first);
        vec.extend_from_slice(second);
        Ok(())
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

    fn kvs_min_file_num(&self) -> Option<u64> {
        if self.kvs.is_empty() {
            return None;
        }
        Some(self.kvs.values().fold(0, |max, v| cmp::max(max, v.1)))
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
}


#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;

    use kvproto::eraftpb::Entry;

    #[test]
    fn test_mem_entries() {
        let mut mem_entries = MemEntries::new(8 /* region_id */);

        // append entries [0, 10) file_num = 1
        // append entries [10, 20) file_num = 1
        // append entries [20, 30) file_num = 2
        mem_entries.append(generate_ents(0, 10), 1);
        mem_entries.append(generate_ents(10, 20), 1);
        mem_entries.append(generate_ents(20, 30), 2);
        assert_eq!(
            mem_entries.file_index,
            VecDeque::from(vec![FileIndex::new(1, 19), FileIndex::new(2, 29)])
        );
        assert_eq!(mem_entries.entry_queue.len(), 30);
        assert_eq!(mem_entries.entry_queue[0].get_index(), 0);
        assert_eq!(mem_entries.entry_queue[29].get_index(), 29);
        assert_eq!(mem_entries.min_file_num().unwrap(), 1);
        assert_eq!(mem_entries.max_file_num().unwrap(), 2);

        mem_entries.compact_to(15);
        assert_eq!(
            mem_entries.file_index,
            VecDeque::from(vec![FileIndex::new(1, 19), FileIndex::new(2, 29)])
        );
        assert_eq!(mem_entries.entry_queue.len(), 15);
        assert_eq!(mem_entries.entry_queue[0].get_index(), 15);
        assert_eq!(mem_entries.entry_queue[14].get_index(), 29);
        assert_eq!(mem_entries.min_file_num().unwrap(), 1);
        assert_eq!(mem_entries.max_file_num().unwrap(), 2);

        mem_entries.compact_to(25);
        assert_eq!(
            mem_entries.file_index,
            VecDeque::from(vec![FileIndex::new(2, 29)])
        );
        assert_eq!(mem_entries.entry_queue.len(), 5);
        assert_eq!(mem_entries.entry_queue[0].get_index(), 25);
        assert_eq!(mem_entries.entry_queue[4].get_index(), 29);
        assert_eq!(mem_entries.min_file_num().unwrap(), 2);
        assert_eq!(mem_entries.max_file_num().unwrap(), 2);

        mem_entries.compact_to(24);
        assert_eq!(
            mem_entries.file_index,
            VecDeque::from(vec![FileIndex::new(2, 29)])
        );
        assert_eq!(mem_entries.entry_queue.len(), 5);
        assert_eq!(mem_entries.entry_queue[0].get_index(), 25);
        assert_eq!(mem_entries.entry_queue[4].get_index(), 29);
        assert_eq!(mem_entries.min_file_num().unwrap(), 2);
        assert_eq!(mem_entries.max_file_num().unwrap(), 2);

        // append entries [28, 38) file_num = 3
        mem_entries.append(generate_ents(28, 38), 3);
        assert_eq!(
            mem_entries.file_index,
            VecDeque::from(vec![FileIndex::new(2, 27), FileIndex::new(3, 37)])
        );
        assert_eq!(mem_entries.entry_queue.len(), 13);
        assert_eq!(mem_entries.entry_queue[0].get_index(), 25);
        assert_eq!(mem_entries.entry_queue[12].get_index(), 37);
        assert_eq!(mem_entries.min_file_num().unwrap(), 2);
        assert_eq!(mem_entries.max_file_num().unwrap(), 3);

        // has entries [25, 38)
        let mut ents = vec![];
        mem_entries.fetch_all(&mut ents);
        assert_eq!(ents.len(), 13);
        assert_eq!(ents[0].get_index(), 25);
        assert_eq!(ents[12].get_index(), 37);

        ents.clear();
        assert!(mem_entries.fetch_entries(24, 37, &mut ents).is_err());
        assert!(mem_entries.fetch_entries(40, 45, &mut ents).is_err());
        mem_entries.fetch_entries(25, 38, &mut ents).unwrap();
        assert_eq!(ents.len(), 13);
        assert_eq!(ents[0].get_index(), 25);
        assert_eq!(ents[12].get_index(), 37);

        ents.clear();
        mem_entries.fetch_entries(30, 40, &mut ents).unwrap();
        assert_eq!(ents.len(), 8);
        assert_eq!(ents[0].get_index(), 30);
        assert_eq!(ents[7].get_index(), 37);

        // append entries [20, 40) file_num = 3
        mem_entries.append(generate_ents(20, 40), 3);
        assert_eq!(
            mem_entries.file_index,
            VecDeque::from(vec![FileIndex::new(3, 39)])
        );
        assert_eq!(mem_entries.entry_queue.len(), 20);
        assert_eq!(mem_entries.entry_queue[0].get_index(), 20);
        assert_eq!(mem_entries.entry_queue[19].get_index(), 39);
        assert_eq!(mem_entries.min_file_num().unwrap(), 3);
        assert_eq!(mem_entries.max_file_num().unwrap(), 3);
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
