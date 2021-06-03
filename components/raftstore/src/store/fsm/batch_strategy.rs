// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

const MIN_PROCESS_TIME: f64 = 0.1;

pub struct BatchStrategy {
    process_wait_time: f64,
    optimized_batch_size: usize,
    batch_size_limit: usize,
    base_batch_size: usize,
}

impl BatchStrategy {
    pub fn new(base_batch_size: usize, max_batch_size: usize) -> BatchStrategy {
        BatchStrategy {
            process_wait_time: MIN_PROCESS_TIME,
            batch_size_limit: max_batch_size,
            optimized_batch_size: base_batch_size,
            base_batch_size,
        }
    }

    pub fn get_batch_size(&self) -> usize {
        self.optimized_batch_size
    }

    pub fn optimize_current_batch_limit(&mut self, batch_size: usize, t: f64) {
        // If the batch size does not reach the limit but process wait time still increases, it
        // means that even if we continue increase batch size, we can not speed up processing message.
        if batch_size < self.optimized_batch_size && t > self.process_wait_time {
            return;
        }

        if t > self.process_wait_time * 2.0 {
            if self.optimized_batch_size < self.batch_size_limit {
                self.optimized_batch_size *= 2;
                self.process_wait_time *= 2.0;
            }
        } else if t * 2.0 < self.process_wait_time {
            if self.optimized_batch_size > self.base_batch_size {
                self.optimized_batch_size /= 2;
                self.process_wait_time /= 2.0;
            }
        }
    }
}
