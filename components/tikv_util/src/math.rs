// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{
    atomic::{AtomicU32, Ordering},
    Mutex,
};

struct MovingMaxU32Inner {
    buffer: Vec<u32>,
    cached_max_index: usize,
    current_index: usize,
}

pub struct MovingMaxU32 {
    protected: Mutex<MovingMaxU32Inner>,
    cached_max: AtomicU32,
}

impl MovingMaxU32 {
    pub fn new(size: usize) -> Self {
        MovingMaxU32 {
            protected: Mutex::new(MovingMaxU32Inner {
                buffer: vec![0; size],
                cached_max_index: 0,
                current_index: 0,
            }),
            cached_max: AtomicU32::new(0),
        }
    }

    pub fn add(&self, sample: u32) -> u32 {
        let mut inner = self.protected.lock().unwrap();
        let current_index = (inner.current_index + 1) % inner.buffer.len();
        inner.current_index = current_index;
        inner.buffer[current_index] = sample;
        if inner.cached_max_index == current_index {
            let mut max = sample;
            for i in 1..inner.buffer.len() {
                let idx = (current_index + i) % inner.buffer.len();
                if inner.buffer[idx] > max {
                    inner.cached_max_index = idx;
                    max = inner.buffer[idx];
                }
            }
            self.cached_max.store(max, Ordering::Relaxed);
        } else if inner.buffer[inner.cached_max_index] <= sample {
            inner.cached_max_index = current_index;
            self.cached_max.store(sample, Ordering::Relaxed);
        }
        inner.buffer[inner.cached_max_index]
    }

    pub fn fetch(&self) -> u32 {
        self.cached_max.load(Ordering::Relaxed)
    }

    pub fn clear(&self) {
        let mut inner = self.protected.lock().unwrap();
        inner.buffer.fill(0);
        inner.cached_max_index = 0;
        inner.current_index = 0;
        self.cached_max.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monotonic_sequence() {
        let large_max = MovingMaxU32::new(100);
        let small_max = MovingMaxU32::new(5);
        for i in (0..100).rev() {
            large_max.add(i);
            small_max.add(i);
            assert_eq!(large_max.fetch(), 99);
            assert_eq!(small_max.fetch(), std::cmp::min(i + 4, 99));
        }
        large_max.clear();
        small_max.clear();
        for i in 0..100 {
            large_max.add(i);
            small_max.add(i);
            assert_eq!(large_max.fetch(), i);
            assert_eq!(small_max.fetch(), i);
        }
    }

    #[test]
    fn test_random_sequence() {
        use rand::Rng;
        let mut rng = rand::thread_rng();

        let max = MovingMaxU32::new(105);
        let mut external_max = 0;
        for _ in 0..100 {
            let i: u32 = rng.gen();
            if i > external_max {
                external_max = i;
            }
            max.add(i);
            assert_eq!(max.fetch(), external_max);
        }
    }
}
