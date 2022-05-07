// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::table::*;
use std::cmp::Ordering::*;
use std::mem;

pub struct MergeIterator<'a> {
    smaller: Box<MergeIteratorChild<'a>>,
    bigger: Box<MergeIteratorChild<'a>>,
    reverse: bool,
    same_key: bool,
}

pub(crate) struct MergeIteratorChild<'a> {
    is_first: bool,
    valid: bool,
    iter: Box<dyn Iterator + 'a>,
    ver: u64,
}

impl<'a> MergeIteratorChild<'a> {
    pub(crate) fn new(is_first: bool, iter: Box<dyn Iterator + 'a>) -> Self {
        MergeIteratorChild {
            is_first,
            valid: false,
            iter,
            ver: 0,
        }
    }

    fn reset(&mut self) {
        self.valid = self.iter.valid();
        if self.valid {
            self.ver = self.iter.value().version;
        }
    }
}

impl Iterator for MergeIterator<'_> {
    fn next(&mut self) {
        self.smaller.iter.next();
        self.smaller.reset();
        if self.same_key && self.bigger.valid {
            self.bigger.iter.next();
            self.bigger.reset();
        }
        self.fix()
    }

    fn next_version(&mut self) -> bool {
        if self.smaller.iter.next_version() {
            self.smaller.reset();
            return true;
        }
        if !self.same_key {
            return false;
        }
        if !self.bigger.valid {
            return false;
        }
        if self.smaller.ver < self.bigger.ver {
            return false;
        }
        if self.smaller.ver == self.bigger.ver {
            // have duplicated key in the two iterators.
            if self.bigger.iter.next_version() {
                self.bigger.reset();
                self.swap();
                return true;
            }
            return false;
        }
        self.swap();
        true
    }

    fn rewind(&mut self) {
        self.smaller.iter.rewind();
        self.smaller.reset();
        self.bigger.iter.rewind();
        self.bigger.reset();
        self.fix();
    }

    fn seek(&mut self, key: &[u8]) {
        self.smaller.iter.seek(key);
        self.smaller.reset();
        self.bigger.iter.seek(key);
        self.bigger.reset();
        self.fix();
    }

    fn key(&self) -> &[u8] {
        self.smaller.iter.key()
    }

    fn value(&self) -> Value {
        self.smaller.iter.value()
    }

    fn valid(&self) -> bool {
        self.smaller.valid
    }
}

impl<'a> MergeIterator<'a> {
    pub(crate) fn new(
        first: Box<MergeIteratorChild<'a>>,
        second: Box<MergeIteratorChild<'a>>,
        reverse: bool,
    ) -> Self {
        Self {
            smaller: first,
            bigger: second,
            reverse,
            same_key: false,
        }
    }

    fn fix(&mut self) {
        if !self.bigger.valid {
            return;
        }
        if self.smaller.valid {
            match self.smaller.iter.key().cmp(self.bigger.iter.key()) {
                Equal => {
                    self.same_key = true;
                    if !self.smaller.is_first {
                        self.swap();
                    }
                }
                Less => {
                    self.same_key = false;
                    if self.reverse {
                        self.swap();
                    }
                }
                Greater => {
                    self.same_key = false;
                    if !self.reverse {
                        self.swap();
                    }
                }
            }
            return;
        }
        self.swap();
    }

    fn swap(&mut self) {
        mem::swap(&mut self.smaller, &mut self.bigger)
    }
}
