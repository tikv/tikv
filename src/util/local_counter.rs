// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use prometheus::Counter;

pub struct LocalCounter {
    counter: Counter,
    val: f64,
}

// LocalCounter is a thread local copy of Counter
impl LocalCounter {
    pub fn new(counter: Counter) -> LocalCounter {
        LocalCounter {
            counter: counter,
            val: 0.0,
        }
    }

    /// `inc` increments the local counter by 1.
    #[inline]
    pub fn inc(&mut self) {
        self.val += 1.0;
    }

    /// `flush` the local counter value to the counter
    #[inline]
    pub fn flush(&mut self) {
        if self.val == 0.0 {
            return;
        }
        self.counter.inc_by(self.val).unwrap();
        self.val = 0.0;
    }
}

#[cfg(test)]
mod tests {
    use prometheus::Counter;
    use super::LocalCounter;

    #[test]
    fn test_local_counter() {
        let counter = Counter::new("counter", "counter helper").unwrap();
        let mut local_counter1 = LocalCounter::new(counter.clone());
        let mut local_counter2 = LocalCounter::new(counter.clone());

        local_counter1.inc();
        local_counter2.inc();
        assert_eq!(counter.get() as u64, 0);
        local_counter1.flush();
        assert_eq!(counter.get() as u64, 1);
        local_counter2.flush();
        assert_eq!(counter.get() as u64, 2);
    }
}
