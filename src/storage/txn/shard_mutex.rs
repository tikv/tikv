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
