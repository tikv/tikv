// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::table::*;
use bytes::Bytes;

// ConcatIterator concatenates the sequences defined by several iterators.  (It only works with
// TableIterators, probably just because it's faster to not be so generic.)
struct ConcatIterator {
    idx: i32,
    iter: Option<Box<dyn Iterator>>,
    tables: Vec<Box<dyn Table>>,
    reversed: bool,
}

#[allow(dead_code)]
impl ConcatIterator {
    pub fn new(tables: Vec<Box<dyn Table>>, reversed: bool) -> Self {
        ConcatIterator {
            idx: 0,
            iter: None,
            tables,
            reversed,
        }
    }

    fn set_idx(&mut self, idx: i32) {
        if self.idx == idx {
            return;
        }
        self.idx = idx;
        if idx < 0 || idx as usize >= self.tables.len() {
            self.iter = None;
        } else {
            self.iter = Some(self.tables[idx as usize].new_iterator());
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
                if self.idx == self.tables.len() as i32 - 1 {
                    self.iter = None
                } else {
                    self.set_idx(self.idx + 1);
                }
            } else {
                if self.idx == 0 {
                    self.iter = None
                } else {
                    self.set_idx(self.idx - 1);
                }
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
        if self.tables.len() == 0 {
            return;
        }
        if !self.reversed {
            self.set_idx(0);
        } else {
            self.set_idx(self.tables.len() as i32 - 1);
        }
        self.iter.as_mut().unwrap().rewind();
    }

    fn seek(&mut self, key: &[u8]) {
        use std::cmp::Ordering::*;
        let idx;
        if !self.reversed {
            idx = search(self.tables.len(), |idx| {
                self.tables[idx].biggest().cmp(key) != Less
            });
            if idx >= self.tables.len() {
                self.iter = None;
                return;
            }
        } else {
            let n = self.tables.len();
            let ridx = search(self.tables.len(), |idx| {
                self.tables[n - 1 - idx].smallest().cmp(key) != Greater
            });
            if ridx >= self.tables.len() {
                self.iter = None;
                return;
            }
            idx = n - 1 - ridx;
        }
        self.set_idx(idx as i32)
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

    fn close(&self) {
        if let Some(x) = &self.iter {
            x.close()
        }
    }
}
