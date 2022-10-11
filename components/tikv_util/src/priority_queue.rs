// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::hash::Hash;

use collections::HashMap;

/// A priority queue implemented with binary heap, and additionally supports
/// efficiently removing element by key.
pub struct PriorityQueue<K: Eq + Hash + Copy, T: Ord> {
    /// Indexes key to the element's index(position) in `heap`.
    index_map: HashMap<K, usize>,
    /// The binary heap. `None` indicates a hole, which only exists temporarily
    /// when some modification is being made.
    heap: Vec<Option<(K, T)>>,
}

impl<K: Eq + Hash + Copy, T: Ord> Default for PriorityQueue<K, T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + Hash + Copy, T: Ord> PriorityQueue<K, T> {
    pub fn new() -> Self {
        Self {
            index_map: HashMap::default(),
            heap: Vec::new(),
        }
    }

    /// Moves specified element (a key-value pair) to the specified index in the
    /// heap. The caller must guarantee the element at the target index `i`
    /// is currently a hole.
    #[inline]
    fn move_to_index(&mut self, i: usize, data: (K, T)) {
        *self.index_map.get_mut(&data.0).unwrap() = i;
        debug_assert!(self.heap[i].is_none());
        self.heap[i] = Some(data);
    }

    /// Calculates index of the left child of the specified index.
    #[inline]
    fn left_child(index: usize) -> usize {
        (index << 1) + 1
    }

    /// Calculates the parent index of the specified index. Assuming the given
    /// index is not the root (`index != 0`).
    #[inline]
    fn parent(index: usize) -> usize {
        (index - 1) >> 1
    }

    /// Perform the percolate-down operation on the element at the specified
    /// index in the heap.
    fn percolate_down(&mut self, mut index: usize) {
        let tmp_item = self.heap[index].take().unwrap();
        let initial_index = index;
        loop {
            let mut max_index = index;
            // Let max_index = the index of max{index, left_child(index),
            // right_child(index)} (if any).
            let l = Self::left_child(index);
            if l < self.heap.len() {
                let mut max_item = &tmp_item.1;
                let left_item = &self.heap[l].as_ref().unwrap().1;
                if left_item > max_item {
                    max_item = left_item;
                    max_index = l;
                }

                if l + 1 < self.heap.len() {
                    let right_item = &self.heap[l + 1].as_ref().unwrap().1;
                    if right_item > max_item {
                        max_index = l + 1;
                    }
                }
            }

            if max_index == index {
                break;
            }

            let data = self.heap[max_index].take().unwrap();
            self.move_to_index(index, data);
            index = max_index;
        }

        if index != initial_index {
            self.move_to_index(index, tmp_item);
        } else {
            // Not moved, simply put it back to the hole.
            self.heap[index] = Some(tmp_item);
        }
    }

    /// Perform the percolate-up operation on the element at the specified index
    /// in the heap.
    fn percolate_up(&mut self, mut index: usize) {
        let tmp_item = self.heap[index].take().unwrap();
        let initial_index = index;

        while index > 0 {
            let parent = Self::parent(index);
            if tmp_item.1 <= self.heap[parent].as_ref().unwrap().1 {
                break;
            }

            let data = self.heap[parent].take().unwrap();
            self.move_to_index(index, data);
            index = parent;
        }

        if index != initial_index {
            self.move_to_index(index, tmp_item);
        } else {
            // Not moved, simply put it back to the hole.
            self.heap[index] = Some(tmp_item);
        }
    }

    /// Pushes an element `item`, which can be found by the specified key `key`,
    /// to the priority queue.
    ///
    /// The caller must guarantee the uniqueness of the `key`. Panics if the
    /// `key` already exists in the priority queue.
    pub fn push(&mut self, key: K, item: T) {
        let replaced = self.index_map.insert(key, self.index_map.len());
        // Duplicated key is disallowed.
        assert!(replaced.is_none());
        self.heap.push(Some((key, item)));
        self.percolate_up(self.heap.len() - 1);
    }

    /// Accesses the element at top of the priority queue. Returns `None` if the
    /// queue is empty.
    pub fn peek(&self) -> Option<&T> {
        self.heap.first().map(|data| &data.as_ref().unwrap().1)
    }

    /// Pops the element at the top of the priority queue. Returns `None` if the
    /// queue is empty.
    pub fn pop(&mut self) -> Option<T> {
        if self.heap.is_empty() {
            return None;
        }
        if self.heap.len() == 1 {
            let (key, item) = self.heap.pop().unwrap().unwrap();
            self.index_map.remove(&key).unwrap();
            assert!(self.index_map.is_empty());
            return Some(item);
        }

        // Take the top element from the heap, and replace the top position with the
        // last element in the heap.
        let (key, item) = self.heap.swap_remove(0).unwrap();
        self.index_map.remove(&key).unwrap();
        let new_head_key = self.heap[0].as_ref().unwrap().0;
        *self.index_map.get_mut(&new_head_key).unwrap() = 0;

        self.percolate_down(0);

        Some(item)
    }

    /// Removes the element corresponding to the specified key from the priority
    /// queue. Returns the element if it's successfully removed from the
    /// priority queue, or `None` if `key` is not found.
    ///
    /// This function has `O(log(n))` complexity.
    pub fn remove_by_key(&mut self, key: K) -> Option<T> {
        let index = self.index_map.remove(&key)?;
        // Take the element from the heap, and replace the original position with the
        // last element at the end of the heap.
        let (removed_key, item) = self.heap.swap_remove(index).unwrap();
        debug_assert!(removed_key == key);
        if index == self.heap.len() {
            // The item is at the tail of the heap. No need to do extra operation to keep
            // the heap order.
            debug_assert_eq!(self.heap.len(), self.index_map.len());
            return Some(item);
        }
        // Adjust the index map after moving the last element to the removed position.
        let (tail_key, tail_item) = self.heap[index].as_ref().unwrap();
        *self.index_map.get_mut(tail_key).unwrap() = index;

        // The element that takes place of the previously removed one may either need to
        // percolate up or down.
        let less_than_parent = if index > 0 {
            let parent = Self::parent(index);
            let (_, parent_item) = self.heap[parent].as_ref().unwrap();
            tail_item < parent_item
        } else {
            true
        };
        if less_than_parent {
            self.percolate_down(index);
        } else {
            self.percolate_up(index);
        }

        debug_assert_eq!(self.heap.len(), self.index_map.len());
        Some(item)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.heap.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BinaryHeap, VecDeque},
        fmt::Debug,
        iter::Iterator,
    };

    use rand::{seq::SliceRandom, thread_rng, RngCore};
    use test::{black_box, Bencher};

    use super::*;

    impl<K: Eq + Hash + Copy + Debug, T: Ord + Debug> PriorityQueue<K, T> {
        fn check_is_consistent(&self) {
            // Check consistency between the heap and the index map.
            assert_eq!(self.index_map.len(), self.heap.len());
            let mut passes = vec![false; self.heap.len()];
            for (index_map_key, index) in self.index_map.iter() {
                let (heap_key, _) = self.heap[*index].as_ref().unwrap();
                assert_eq!(heap_key, index_map_key);
                passes[*index] = true;
            }
            assert!(passes.into_iter().all(|x| x));

            // Check heap order: All elements must be greater than or equal to its children
            // (if any).
            if self.heap.len() > 1 {
                for i in 0..=Self::parent(self.heap.len() - 1) {
                    let item = &self.heap[i].as_ref().unwrap().1;
                    let left = &self.heap[Self::left_child(i)].as_ref().unwrap().1;
                    assert!(item >= left);

                    if Self::left_child(i) + 1 < self.heap.len() {
                        let right = &self.heap[Self::left_child(i) + 1].as_ref().unwrap().1;
                        assert!(item >= right);
                    }
                }
            }
        }

        fn from_raw(data: Vec<(K, T)>) -> Self {
            let index_map = data.iter().enumerate().map(|(i, (k, _))| (*k, i)).collect();
            let heap = data.into_iter().map(Some).collect();
            let res = Self { index_map, heap };
            res.check_is_consistent();
            res
        }

        fn assert_state(&self, data: &[(K, T)]) {
            assert_eq!(self.heap.len(), data.len());
            assert_eq!(self.index_map.len(), data.len());
            for (i, (k, v)) in data.iter().enumerate() {
                assert_eq!(*self.index_map.get(k).unwrap(), i);
                assert_eq!(&self.heap[i].as_ref().unwrap().1, v);
            }
        }
    }

    #[test]
    fn test_random_push_pop() {
        #[derive(Clone)]
        enum Op {
            Push(usize),
            Pop,
        }

        let max_size = 50000;
        let mut ops: Vec<_> = (0..max_size).map(Op::Push).collect();
        ops.extend(std::iter::repeat(Op::Pop).take(max_size));
        ops.shuffle(&mut thread_rng());
        let mut ops = VecDeque::from(ops);

        let mut q = PriorityQueue::new();
        let mut std_q = BinaryHeap::new();

        while let Some(op) = ops.pop_front() {
            match op {
                Op::Push(v) => {
                    q.push(v, v);
                    std_q.push(v);
                }
                Op::Pop => {
                    if std_q.is_empty() {
                        // Move to the end of the ops list.
                        ops.push_back(Op::Pop);
                        continue;
                    }
                    let expected_next = std_q.pop().unwrap();
                    let v = *q.peek().unwrap();
                    assert_eq!(v, expected_next);
                    let v = q.pop().unwrap();
                    assert_eq!(v, expected_next);
                }
            }
        }

        assert!(q.is_empty());
        assert!(q.peek().is_none());
        assert!(q.pop().is_none());
        q.check_is_consistent();
    }

    #[test]
    fn test_basic_push_pop() {
        enum Op {
            Push(i32),
            Pop(i32),
        }
        let test_data = vec![
            Op::Push(10),
            Op::Push(20),
            Op::Push(5),
            Op::Push(15),
            Op::Pop(20),
            Op::Pop(15),
            Op::Pop(10),
            Op::Pop(5),
            Op::Push(10),
            Op::Push(10),
            Op::Push(20),
            Op::Push(20),
            Op::Pop(20),
            Op::Push(30),
            Op::Pop(30),
            Op::Pop(20),
            Op::Pop(10),
            Op::Pop(10),
        ];

        let mut q = PriorityQueue::new();
        let mut std_q = BinaryHeap::new();
        assert_eq!(q.len(), 0);
        assert!(q.is_empty());
        assert!(q.peek().is_none());
        assert!(q.pop().is_none());

        for (i, op) in test_data.into_iter().enumerate() {
            match op {
                Op::Push(v) => {
                    q.push(i, v);
                    std_q.push(v);
                }
                Op::Pop(v) => {
                    assert_eq!(q.pop().unwrap(), v);
                    assert_eq!(std_q.pop().unwrap(), v);
                }
            }

            q.check_is_consistent();
            assert_eq!(q.len(), std_q.len());
            assert_eq!(q.is_empty(), std_q.is_empty());
            assert_eq!(q.peek(), std_q.peek());
        }
    }

    #[test]
    fn test_removing_by_key() {
        // Each case contains the initial state of the heap, and the final state of the
        // heap after removing the element with key = 0.
        let cases = vec![
            (vec![(0, 0)], vec![], 0),
            (vec![(0, 1), (1, 0)], vec![(1, 0)], 1),
            (vec![(1, 1), (0, 0)], vec![(1, 1)], 0),
            (
                vec![(1, 10), (0, 8), (2, 7), (3, 3)],
                vec![(1, 10), (3, 3), (2, 7)],
                8,
            ),
            (
                vec![(0, 10), (1, 8), (2, 7), (3, 3)],
                vec![(1, 8), (3, 3), (2, 7)],
                10,
            ),
            (
                vec![(1, 10), (2, 8), (3, 7), (0, 3)],
                vec![(1, 10), (2, 8), (3, 7)],
                3,
            ),
            (
                vec![(1, 10), (0, 8), (2, 7), (3, 3), (4, 0)],
                vec![(1, 10), (3, 3), (2, 7), (4, 0)],
                8,
            ),
            (
                vec![(1, 10), (0, 8), (2, 7), (3, 3), (4, 5)],
                vec![(1, 10), (4, 5), (2, 7), (3, 3)],
                8,
            ),
            (
                vec![(1, 10), (0, 9), (3, 10), (4, 8), (5, 7), (6, 10)],
                vec![(1, 10), (6, 10), (3, 10), (4, 8), (5, 7)],
                9,
            ),
            (
                vec![(1, 10), (0, 9), (3, 10), (4, 8), (5, 7), (6, 8)],
                vec![(1, 10), (6, 8), (3, 10), (4, 8), (5, 7)],
                9,
            ),
            (
                vec![(1, 10), (2, 9), (3, 10), (0, 8), (5, 7), (6, 10)],
                vec![(1, 10), (6, 10), (3, 10), (2, 9), (5, 7)],
                8,
            ),
        ];

        for (initial_state, final_state, removed_value) in cases {
            let mut q = PriorityQueue::from_raw(initial_state);
            assert_eq!(q.remove_by_key(0).unwrap(), removed_value);
            q.assert_state(&final_state);
            q.check_is_consistent();
        }
    }

    #[test]
    fn test_random_removing() {
        let max_size = 1000usize;
        let mut expected_items: Vec<_> = (0..max_size)
            .map(|key| (key, thread_rng().next_u64() % 500))
            .collect();

        let mut q = PriorityQueue::new();
        for i in 0..max_size {
            q.push(expected_items[i].0, expected_items[i].1);
        }
        q.check_is_consistent();

        expected_items.shuffle(&mut thread_rng());

        for (key, expected_value) in expected_items {
            let value = q.remove_by_key(key).unwrap();
            assert_eq!(value, expected_value);
            q.check_is_consistent();
        }

        assert!(q.is_empty());
    }

    #[bench]
    fn bench_push_pop(b: &mut Bencher) {
        let mut q = PriorityQueue::new();

        let max_size = 2000usize;
        let mut id = 0;
        for _ in 0..max_size {
            q.push(id, thread_rng().next_u64() % 1000);
            id += 1;
        }

        let mut v = 0;

        b.iter(|| {
            q.push(id, v);
            q.pop();
            id += 1;
            v = (v + 1) % 1000;
        });
    }

    #[bench]
    fn bench_std_push_pop(b: &mut Bencher) {
        let mut q = BinaryHeap::new();

        let max_size = 2000usize;
        for _ in 0..max_size {
            q.push(thread_rng().next_u64() % 1000);
        }

        let mut id = 2000;
        let mut v = 0;

        b.iter(|| {
            q.push(v);
            black_box(id);
            q.pop();
            id += 1;
            v = (v + 1) % 1000;
        });
    }
}
