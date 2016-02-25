use std::sync::{Mutex, MutexGuard};
use std::hash::{Hash, SipHasher, Hasher};

pub struct ShardLock {
    size: usize,
    locks: Vec<Mutex<()>>,
}

impl ShardLock {
    pub fn new(size: usize) -> ShardLock {
        let mut locks = Vec::<_>::with_capacity(size);
        for _ in 0..size {
            locks.push(Mutex::new(()));
        }
        ShardLock {
            size: size,
            locks: locks,
        }
    }

    pub fn lock<H>(&self, keys: &[H]) -> Vec<MutexGuard<()>>
        where H: Hash
    {
        let mut indices: Vec<usize> = keys.iter().map(|x| self.shard_index(x)).collect();
        indices.sort();
        indices.dedup();
        indices.iter().map(|&i| self.locks[i].lock().unwrap()).collect()
    }

    fn shard_index<H>(&self, key: &H) -> usize
        where H: Hash
    {
        let mut s = SipHasher::new();
        key.hash(&mut s);
        (s.finish() as usize) % self.size
    }
}
