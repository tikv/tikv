// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{
    atomic::{AtomicU32, AtomicU64, Ordering},
    Mutex,
};

struct MovingMaxU32Inner {
    buffer: Vec<u32>,
    current_index: usize,
    cached_max_index: usize,
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
                current_index: 0,
                cached_max_index: 0,
            }),
            cached_max: AtomicU32::new(0),
        }
    }

    pub fn add(&self, sample: u32) -> (u32, u32) {
        let mut inner = self.protected.lock().unwrap();
        let current_index = (inner.current_index + 1) % inner.buffer.len();
        inner.current_index = current_index;
        inner.buffer[current_index] = sample;
        let old_max = inner.buffer[inner.cached_max_index];
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
        (old_max, inner.buffer[inner.cached_max_index])
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

struct MovingAvgU64Inner {
    buffer: Vec<u64>,
    current_index: usize,
    sum: u64,
}

pub struct MovingAvgU64 {
    protected: Mutex<MovingAvgU64Inner>,
    cached_avg: AtomicU64,
}

impl MovingAvgU64 {
    pub fn new(size: usize) -> Self {
        MovingAvgU64 {
            protected: Mutex::new(MovingAvgU64Inner {
                buffer: vec![0; size],
                current_index: 0,
                sum: 0,
            }),
            cached_avg: AtomicU64::new(0),
        }
    }

    pub fn add(&self, sample: u64) -> (u64, u64) {
        let mut inner = self.protected.lock().unwrap();
        let current_index = (inner.current_index + 1) % inner.buffer.len();
        inner.current_index = current_index;
        let old_avg = inner.sum / inner.buffer.len() as u64;
        inner.sum = inner.sum + sample - inner.buffer[current_index];
        inner.buffer[current_index] = sample;
        let new_avg = inner.sum / inner.buffer.len() as u64;
        self.cached_avg.store(new_avg, Ordering::Relaxed);
        (old_avg, new_avg)
    }

    pub fn fetch(&self) -> u64 {
        self.cached_avg.load(Ordering::Relaxed)
    }

    pub fn clear(&self) {
        let mut inner = self.protected.lock().unwrap();
        inner.buffer.fill(0);
        inner.current_index = 0;
        inner.sum = 0;
        self.cached_avg.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monotonic_sequence() {
        let large_max = MovingMaxU32::new(100);
        let small_max = MovingMaxU32::new(5);
        let avg = MovingAvgU64::new(5);
        for i in (0..100).rev() {
            large_max.add(i);
            assert_eq!(large_max.fetch(), 99);
            small_max.add(i);
            assert_eq!(small_max.fetch(), std::cmp::min(i + 4, 99));
            avg.add(i as u64);
            if 100 - i >= 5 {
                assert_eq!(avg.fetch(), i as u64 + 2);
            } else {
                assert_eq!(avg.fetch(), ((i + 99) * (100 - i) / 10) as u64);
            }
        }
        large_max.clear();
        small_max.clear();
        avg.clear();
        for i in 0..100 {
            large_max.add(i);
            assert_eq!(large_max.fetch(), i);
            small_max.add(i);
            assert_eq!(small_max.fetch(), i);
            avg.add(i as u64);
            if i >= 4 {
                assert_eq!(avg.fetch(), i as u64 - 2);
            } else {
                assert_eq!(avg.fetch(), (i * (i + 1) / 10) as u64);
            }
        }
    }

    #[test]
    fn test_random_sequence() {
        use rand::Rng;
        let mut rng = rand::thread_rng();

        let max = MovingMaxU32::new(105);
        let mut external_max = 0;
        let avg = MovingAvgU64::new(105);
        let mut external_sum = 0;
        for _ in 0..100 {
            let n: u32 = rng.gen();
            if n > external_max {
                external_max = n;
            }
            max.add(n);
            assert_eq!(max.fetch(), external_max);
            external_sum += n as u64;
            avg.add(n as u64);
            assert_eq!(avg.fetch(), external_sum / 105);
        }
    }
}
