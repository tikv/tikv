// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{
    table::{
        search,
        sstable::{SSTable, TableIterator},
        Iterator, Value,
    },
    LevelHandler,
};

// ConcatIterator concatenates the sequences defined by several iterators.  (It only works with
// TableIterators, probably just because it's faster to not be so generic.)
pub(crate) struct ConcatIterator {
    idx: i32,
    iter: Option<Box<TableIterator>>,
    level: LevelHandler,
    reversed: bool,
}

#[allow(dead_code)]
impl ConcatIterator {
    pub(crate) fn new(level: LevelHandler, reversed: bool) -> Self {
        ConcatIterator {
            idx: -1,
            iter: None,
            level,
            reversed,
        }
    }

    pub(crate) fn new_with_tables(tables: Vec<SSTable>, reversed: bool) -> Self {
        let level = LevelHandler::new(1, tables);
        ConcatIterator {
            idx: -1,
            iter: None,
            level,
            reversed,
        }
    }

    fn get_table(&self, idx: usize) -> &SSTable {
        &self.level.tables[idx]
    }

    fn num_tables(&self) -> usize {
        self.level.tables.len()
    }

    fn set_idx(&mut self, idx: i32) {
        if self.idx == idx {
            return;
        }
        self.idx = idx;
        if idx < 0 || idx as usize >= self.num_tables() {
            self.iter = None;
        } else {
            let mut iter = self.get_table(idx as usize).new_iterator(self.reversed);
            iter.rewind();
            self.iter = Some(iter);
        }
    }
}

impl Iterator for ConcatIterator {
    fn next(&mut self) {
        if self.iter.is_none() {
            return;
        }
        if let Some(iter) = &mut self.iter {
            iter.next();
            if iter.valid() {
                return;
            }
        }
        loop {
            if !self.reversed {
                if self.idx == self.num_tables() as i32 - 1 {
                    self.iter = None
                } else {
                    self.set_idx(self.idx + 1);
                }
            } else {
                self.set_idx(self.idx - 1);
            }
            match &mut self.iter {
                None => return,
                Some(iter) => {
                    iter.rewind();
                    if iter.valid() {
                        return;
                    }
                }
            }
        }
    }

    fn next_version(&mut self) -> bool {
        self.iter.as_mut().unwrap().next_version()
    }

    fn rewind(&mut self) {
        if self.num_tables() == 0 {
            return;
        }
        if !self.reversed {
            self.set_idx(0);
        } else {
            self.set_idx(self.num_tables() as i32 - 1);
        }
        self.iter.as_mut().unwrap().rewind();
    }

    fn seek(&mut self, key: &[u8]) {
        use std::cmp::Ordering::*;
        let idx: i32;
        if !self.reversed {
            idx = search(self.num_tables(), |idx| {
                self.get_table(idx).biggest().cmp(key) != Less
            }) as i32;
            if idx >= self.num_tables() as i32 {
                self.iter = None;
                return;
            }
        } else {
            let n = self.num_tables();
            let ridx = search(n, |idx| {
                self.get_table(n - 1 - idx).smallest().cmp(key) != Greater
            }) as i32;
            if ridx >= n as i32 {
                self.iter = None;
                return;
            }
            idx = n as i32 - 1 - ridx;
        }
        self.set_idx(idx as i32);
        self.iter.as_mut().unwrap().seek(key);
    }

    fn key(&self) -> &[u8] {
        self.iter.as_ref().unwrap().key()
    }

    fn value(&self) -> Value {
        self.iter.as_ref().unwrap().value()
    }

    fn valid(&self) -> bool {
        match &self.iter {
            Some(x) => x.valid(),
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        concat_iterator::ConcatIterator,
        table::{
            sstable::{
                build_test_table_with_kvs, build_test_table_with_prefix, new_test_cache, SSTable,
            },
            Iterator,
        },
    };

    #[test]
    fn test_concat_iterator_one_table() {
        let tf = build_test_table_with_kvs(vec![
            ("k1".to_string(), "a1".to_string()),
            ("k2".to_string(), "a2".to_string()),
        ]);
        let t = SSTable::new(tf, new_test_cache()).unwrap();
        let tables = vec![t];
        let mut it = ConcatIterator::new_with_tables(tables, false);
        it.rewind();
        assert_eq!(it.valid(), true);
        assert_eq!(it.key(), "k1".as_bytes());
        let v = it.value();
        assert_eq!(v.get_value(), "a1".as_bytes());
        assert_eq!(v.meta, b'A');
    }

    #[test]
    fn test_concat_iterator() {
        let tf1 = build_test_table_with_prefix("keya", 10000);
        let tf2 = build_test_table_with_prefix("keyb", 10000);
        let tf3 = build_test_table_with_prefix("keyc", 10000);
        let t1 = SSTable::new(tf1, new_test_cache()).unwrap();
        let t2 = SSTable::new(tf2, new_test_cache()).unwrap();
        let t3 = SSTable::new(tf3, new_test_cache()).unwrap();
        let tables = vec![t1, t2, t3];
        {
            let mut it = ConcatIterator::new_with_tables(tables.clone(), false);
            it.rewind();
            assert_eq!(it.valid(), true);
            let mut cnt = 0;
            while it.valid() {
                let v = it.value();
                assert_eq!(v.get_value(), format!("{}", cnt % 10000).as_bytes());
                assert_eq!(v.meta, b'A');
                cnt += 1;
                it.next();
            }
            assert_eq!(cnt, 30000);
            it.seek("a".as_bytes());
            assert_eq!(it.key(), "keya0000".as_bytes());
            assert_eq!(it.value().get_value(), "0".as_bytes());

            it.seek("keyb".as_bytes());
            assert_eq!(it.key(), "keyb0000".as_bytes());
            assert_eq!(it.value().get_value(), "0".as_bytes());

            it.seek("keyb9999b".as_bytes());
            assert_eq!(it.key(), "keyc0000".as_bytes());
            assert_eq!(it.value().get_value(), "0".as_bytes());

            it.seek("keyd".as_bytes());
            assert_eq!(it.valid(), false);
        }
        {
            let mut it = ConcatIterator::new_with_tables(tables, true);
            it.rewind();
            assert_eq!(it.valid(), true);
            let mut cnt = 0;
            while it.valid() {
                let v = it.value();
                assert_eq!(
                    v.get_value(),
                    format!("{}", 10000 - (cnt % 10000) - 1).as_bytes()
                );
                assert_eq!(v.meta, b'A');
                cnt += 1;
                it.next();
            }
            assert_eq!(cnt, 30000);

            it.seek("a".as_bytes());
            assert_eq!(it.valid(), false);

            it.seek("keyb".as_bytes());
            assert_eq!(it.key(), "keya9999".as_bytes());
            assert_eq!(it.value().get_value(), "9999".as_bytes());

            it.seek("keyb9999b".as_bytes());
            assert_eq!(it.key(), "keyb9999".as_bytes());
            assert_eq!(it.value().get_value(), "9999".as_bytes());

            it.seek("keyd".as_bytes());
            assert_eq!(it.key(), "keyc9999".as_bytes());
            assert_eq!(it.value().get_value(), "9999".as_bytes());
        }
    }
}
