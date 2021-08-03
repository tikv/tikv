
use std::{collections::HashMap, sync::{Mutex, atomic::Ordering}};

use crate::*;

pub struct Engine {
    shards: Mutex<HashMap<u64, Shard>>,
    opts: Options,
}

impl Engine {
    pub fn get_shard(&self, id: u64) -> Option<Shard> {
        if let Some(shd) = self.shards.lock().unwrap().get(&id) {
            return Some(shd.clone())
        }
        None
    }

    pub fn new_snap_access(&self, shard: Shard) -> iterator::SnapAccess {
        iterator::SnapAccess::new(shard, self.opts.cfs)
    }

    pub fn remove_shard(&mut self, shard_id: u64, remove_file: bool) -> bool {
        let x = self.shards.lock().unwrap().remove_entry(&shard_id);
        if let Some((_, shd)) = x {
            if remove_file {
                shd.remove_file.store(true, Ordering::Release);
            }
            return true
        }
        false
    }
}

