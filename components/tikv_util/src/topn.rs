// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
pub struct TopN<T> {
    cap: usize,
    heap: BinaryHeap<Reverse<T>>,
}

impl<T: Ord> TopN<T> {
    pub fn new(cap: usize) -> TopN<T> {
        TopN {
            cap,
            heap: BinaryHeap::with_capacity(cap),
        }
    }

    pub fn push(&mut self, item: T) {
        if self.cap < 1 {
            return;
        }
        if self.cap > self.heap.len() {
            self.heap.push(Reverse(item));
            return;
        }
        let p = &self.heap.peek().unwrap().0;
        match item.cmp(p) {
            Ordering::Greater => {
                self.heap.pop();
                self.heap.push(Reverse(item))
            }
            _ => {},
        }
    }

    pub fn pop(&mut self) -> Option<T> {
        self.heap.pop().map(|Reverse(x)| x)
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    pub fn peek(&self) -> Option<&T> {
        self.heap.peek().map(|Reverse(x)| x)
    }

    pub fn into_vec(self) -> Vec<T> {
        self.heap.into_iter().map(|it| it.0).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fix_topn() {
        let mut fix_topn = TopN::new(5);
        fix_topn.push(1);
        fix_topn.push(2);
        fix_topn.push(3);
        fix_topn.push(6);
        fix_topn.push(5);
        fix_topn.push(4);
        assert_eq!(fix_topn.len(), 5);
        assert_eq!(fix_topn.pop().unwrap(), 2);
        assert_eq!(fix_topn.pop().unwrap(), 3);
        assert_eq!(fix_topn.pop().unwrap(), 4);
        assert_eq!(fix_topn.pop().unwrap(), 5);
        assert_eq!(fix_topn.pop().unwrap(), 6);
    }
}
