// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{
    atomic::{AtomicU32, Ordering},
    Mutex,
};

pub struct MovingAvgU32 {
    buffer: Vec<u32>,
    current_index: usize,
    sum: u32,
}

impl MovingAvgU32 {
    pub fn new(size: usize) -> Self {
        MovingAvgU32 {
            buffer: vec![0; size],
            current_index: 0,
            sum: 0,
        }
    }

    pub fn add(&mut self, sample: u32) -> (u32, u32) {
        let current_index = (self.current_index + 1) % self.buffer.len();
        self.current_index = current_index;
        let old_avg = self.sum / self.buffer.len() as u32;
        self.sum = self.sum + sample - self.buffer[current_index];
        self.buffer[current_index] = sample;
        let new_avg = self.sum / self.buffer.len() as u32;
        (old_avg, new_avg)
    }

    pub fn fetch(&mut self) -> u32 {
        self.sum / self.buffer.len() as u32
    }

    pub fn clear(&mut self) {
        self.buffer.fill(0);
        self.current_index = 0;
        self.sum = 0;
    }
}

pub struct AtomicMovingAvgU32 {
    protected: Mutex<MovingAvgU32>,
    cached_avg: AtomicU32,
}

impl AtomicMovingAvgU32 {
    pub fn new(size: usize) -> Self {
        AtomicMovingAvgU32 {
            protected: Mutex::new(MovingAvgU32::new(size)),
            cached_avg: AtomicU32::new(0),
        }
    }

    pub fn add(&self, sample: u32) -> (u32, u32) {
        let mut inner = self.protected.lock().unwrap();
        let (old, new) = inner.add(sample);
        self.cached_avg.store(new, Ordering::Relaxed);
        (old, new)
    }

    pub fn fetch(&self) -> u32 {
        self.cached_avg.load(Ordering::Relaxed)
    }

    pub fn clear(&self) {
        let mut inner = self.protected.lock().unwrap();
        inner.clear();
        self.cached_avg.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monotonic_sequence() {
        let avg = AtomicMovingAvgU32::new(5);
        for i in (0..100).rev() {
            avg.add(i);
            if 100 - i >= 5 {
                assert_eq!(avg.fetch(), i + 2);
            } else {
                assert_eq!(avg.fetch(), ((i + 99) * (100 - i) / 10));
            }
        }
        avg.clear();
        for i in 0..100 {
            avg.add(i);
            if i >= 4 {
                assert_eq!(avg.fetch(), i - 2);
            } else {
                assert_eq!(avg.fetch(), (i * (i + 1) / 10));
            }
        }
    }

    #[test]
    fn test_random_sequence() {
        use rand::Rng;
        let mut rng = rand::thread_rng();

        let avg = AtomicMovingAvgU32::new(105);
        let mut external_sum = 0;
        for _ in 0..100 {
            let n: u32 = rng.gen_range(0..u32::MAX / 100);
            external_sum += n;
            avg.add(n);
            assert_eq!(avg.fetch(), external_sum / 105);
        }
    }
}
