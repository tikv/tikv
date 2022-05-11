// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::Reverse,
    collections::{binary_heap, BinaryHeap},
    iter,
};

// todo: Make it TopN<T, const N: usize> when we update our Rust version
/// TopN is used to collect the largest `cap` items pushed in
pub struct TopN<T> {
    capacity: usize,
    heap: BinaryHeap<Reverse<T>>,
}

impl<T: Ord> TopN<T> {
    /// Create a `TopN` with fixed capacity.
    pub fn new(capacity: usize) -> TopN<T> {
        TopN {
            capacity,
            heap: BinaryHeap::with_capacity(capacity),
        }
    }

    /// Pushes an `item` into `TopN`.
    pub fn push(&mut self, item: T) {
        self.heap.push(Reverse(item));
        if self.heap.len() > self.capacity {
            self.heap.pop();
        }
    }

    /// Pops an `item` from `TopN`
    pub fn pop(&mut self) -> Option<T> {
        self.heap.pop().map(|Reverse(x)| x)
    }

    /// Returns the length of the `TopN`.
    pub fn len(&self) -> usize {
        self.heap.len()
    }

    /// Returns whether the `TopN` is empty.
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// Returns the smallest item in the `TopN`, or `None` if it is empty.
    pub fn peek(&self) -> Option<&T> {
        self.heap.peek().map(|Reverse(x)| x)
    }
}

impl<T> IntoIterator for TopN<T> {
    type Item = T;

    // this is added for rust-clippy#1013
    #[allow(clippy::type_complexity)]
    type IntoIter = iter::Map<binary_heap::IntoIter<Reverse<T>>, fn(Reverse<T>) -> T>;

    // note: IntoIterator doesn't require the result in order, there is an `IntoIterSorted`, implement that if necessary
    fn into_iter(self) -> Self::IntoIter {
        self.heap.into_iter().map(|Reverse(x)| x)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_0_capacity() {
        let mut cap_0_topn = TopN::new(0);
        cap_0_topn.push(1);
        assert_eq!(cap_0_topn.pop(), None);
        assert_eq!(cap_0_topn.len(), 0);
        assert_eq!(cap_0_topn.is_empty(), true);
    }

    #[test]
    fn test_1_capactiy() {
        let mut cap_0_topn = TopN::new(1);
        cap_0_topn.push(1);
        assert_eq!(cap_0_topn.peek(), Some(&1));
        assert_eq!(cap_0_topn.len(), 1);
        assert_eq!(cap_0_topn.is_empty(), false);
    }

    #[test]
    fn test_trivial() {
        let mut fix_topn = TopN::new(5);
        fix_topn.push(1);
        fix_topn.push(2);
        fix_topn.push(3);
        fix_topn.push(6);
        fix_topn.push(5);
        fix_topn.push(4);
        assert_eq!(fix_topn.len(), 5);
        assert_eq!(fix_topn.pop(), Some(2));
        assert_eq!(fix_topn.pop(), Some(3));
        assert_eq!(fix_topn.pop(), Some(4));
        assert_eq!(fix_topn.pop(), Some(5));
        assert_eq!(fix_topn.pop(), Some(6));
        assert_eq!(fix_topn.pop(), None);

        let mut fix_topn = TopN::new(5);
        fix_topn.push(1);
        fix_topn.push(2);
        fix_topn.push(3);
        fix_topn.push(6);
        fix_topn.push(5);
        fix_topn.push(4);
        let mut v: Vec<_> = fix_topn.into_iter().collect();
        v.sort_unstable();
        assert_eq!(v.len(), 5);
        assert_eq!(v, vec![2, 3, 4, 5, 6])
    }
}
