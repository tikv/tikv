use std::{collections::HashMap, sync::{Arc, Mutex, atomic::{AtomicBool, AtomicPtr, Ordering}}};
use bytes::Bytes;

use crate::{table::{self, sstable::SSTable}, write::L0Tables};
use crate::*;

#[derive(Clone)]
pub struct Shard {
    inner: Arc<shard::ShardInner>,
}

impl std::ops::Deref for Shard {
    type Target = Arc<ShardInner>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct ShardInner {
    pub id: u64,
    pub ver: u64,
    pub start: Bytes,
    pub end: Bytes,
    cfs: [ShardCF; NUM_CFS],
    pub remove_file: AtomicBool,
    mem_tbls: AtomicPtr<MemTables>,
    l0_tbls: AtomicPtr<L0Tables>,
    pub properties: Mutex<HashMap<Bytes, Bytes>>,
}

impl ShardInner {
    fn get_level(&self, cf: usize, level: usize) -> LevelHandler {
        let ptr = self.cfs[cf].levels[level-1].load(Ordering::Relaxed);
        unsafe {ptr.as_ref().unwrap().clone()}
    }

    pub fn load_mem_tbls(&self) -> MemTables {
        let ptr = self.mem_tbls.load(Ordering::Acquire);
        unsafe {ptr.as_ref().unwrap().clone()}
    }

    pub fn load_l0_tbls(&self) -> L0Tables {
        let ptr = self.l0_tbls.load(Ordering::Relaxed);
        unsafe {ptr.as_ref().unwrap().clone()}
    }
}

struct ShardCF {
    levels: [AtomicPtr<LevelHandler>; NUM_LEVELS],
}

#[derive(Clone)]
struct LevelHandler {
    inner: Arc<LevelInner>,
}

struct LevelInner {
    tables: Vec<SSTable>,
}
