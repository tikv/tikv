// Copyright 2016 PingCAP, Inc.
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

use std::sync::{Mutex, MutexGuard};
use std::hash::{Hash, SipHasher, Hasher};

pub struct ShardMutex {
    mutex: Vec<Mutex<()>>,
    size: usize,
}

impl ShardMutex {
    pub fn new(size: usize) -> ShardMutex {
        ShardMutex {
            mutex: (0..size).map(|_| Mutex::new(())).collect(),
            size: size,
        }
    }

    pub fn lock<H>(&self, keys: &[H]) -> Vec<MutexGuard<()>>
        where H: Hash
    {
        let mut indices: Vec<usize> = keys.iter().map(|x| self.shard_index(x)).collect();
        indices.sort();
        indices.dedup();
        indices.iter().map(|&i| self.mutex[i].lock().unwrap()).collect()
    }

    fn shard_index<H>(&self, key: &H) -> usize
        where H: Hash
    {
        let mut s = SipHasher::new();
        key.hash(&mut s);
        (s.finish() as usize) % self.size
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use rand;
    use super::*;

    #[test]
    fn test_shard_mutex_deadlock() {
        const MUTEX_SIZE: usize = 10;
        const THREAD_NUM: usize = 10;
        const VALUE_NUM: usize = 10;
        const VALUE_RANGE: usize = 30;

        let sm = Arc::new(ShardMutex::new(MUTEX_SIZE));
        let mut children = vec![];
        for _ in 0..THREAD_NUM {
            let sm = sm.clone();
            children.push(thread::spawn(move || {
                let values: Vec<i32> = (0..VALUE_NUM)
                                           .map(|_| rand::random::<i32>() % VALUE_RANGE as i32)
                                           .collect();
                let _guard = sm.lock(&values);
                thread::sleep(Duration::from_millis(1));
            }));
        }
        for t in children {
            t.join().unwrap();
        }
    }
}
