// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::table::*;
use bytes::Bytes;
use std::cmp::Ordering::*;
use std::mem;
use std::ptr::null;

pub struct MergeIterator {
    smaller: Box<MergeIteratorChild>,
    bigger: Box<MergeIteratorChild>,
    reverse: bool,
    same_key: bool,
}

struct MergeIteratorChild {
    is_first: bool,
    valid: bool,
    iter: Box<dyn Iterator>,
    ver: u64,
}

impl MergeIteratorChild {
    fn new(is_first: bool, iter: Box<dyn Iterator>) -> Self {
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

impl Iterator for MergeIterator {
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

impl MergeIterator {
    fn new(first: Box<MergeIteratorChild>, second: Box<MergeIteratorChild>, reverse: bool) -> Self {
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
        while self.smaller.valid {
            match self.smaller.iter.key().cmp(&self.bigger.iter.key()) {
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

pub fn new_merge_iterator(mut iters: Vec<Box<dyn Iterator>>, reverse: bool) -> Box<dyn Iterator> {
    match iters.len() {
        0 => Box::new(EmptyIterator {}),
        1 => iters.pop().unwrap(),
        2 => {
            let second_iter = iters.pop().unwrap();
            let first_iter = iters.pop().unwrap();
            let first = Box::new(MergeIteratorChild::new(true, first_iter));
            let second = Box::new(MergeIteratorChild::new(false, second_iter));
            let merge_iter = MergeIterator::new(first, second, reverse);
            Box::new(merge_iter)
        }
        _ => {
            let mid = iters.len() / 2;
            let mut second = vec![];
            for _ in 0..mid {
                second.push(iters.pop().unwrap())
            }
            second.reverse();
            let first_it = new_merge_iterator(iters, reverse);
            let second_it = new_merge_iterator(second, reverse);
            new_merge_iterator(vec![first_it, second_it], reverse)
        }
    }
}

struct EmptyIterator;

impl Iterator for EmptyIterator {
    fn next(&mut self) {}

    fn next_version(&mut self) -> bool {
        false
    }

    fn rewind(&mut self) {}

    fn seek(&mut self, _: &[u8]) {}

    fn key(&self) -> &[u8] {
        &[]
    }

    fn value(&self) -> Value {
        Value::new()
    }

    fn valid(&self) -> bool {
        false
    }
}
