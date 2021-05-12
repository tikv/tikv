// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::VecDeque;

#[derive(Debug)]
pub struct WrappingDeque<T> {
    deque: VecDeque<T>,
    capacity: usize,
}

impl<T> WrappingDeque<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            deque: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    pub fn push(&mut self, value: T) -> Option<T> {
        let res = (self.deque.len() == self.capacity).then(|| self.deque.pop_front().unwrap());
        self.deque.push_back(value);
        res
    }

    pub fn pop(&mut self) -> Option<T> {
        self.deque.pop_front()
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.deque.iter()
    }
}
