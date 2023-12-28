// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use engine_traits::{Mutable, Result, WriteBatch, WriteBatchExt, WriteOptions, CF_DEFAULT};
use tikv_util::box_err;

use crate::{
    engine::{cf_to_id, RegionCacheMemoryEngineCore},
    keys::{encode_key, ValueType},
    RegionCacheMemoryEngine,
};

type RegionCacheMemoryEngineCorePtr = Arc<Mutex<RegionCacheMemoryEngineCore>>;

/// RegionCacheWriteBatch maintains its own in-memory buffer.
#[derive(Clone)]
pub struct RegionCacheWriteBatch {
    buffer: Vec<RegionCacheWriteBatchEntry>,
    sequence_number: Option<u64>,
    core: RegionCacheMemoryEngineCorePtr,
}

impl RegionCacheWriteBatch {
    pub fn new(core: &RegionCacheMemoryEngineCorePtr) -> Self {
        Self {
            buffer: Vec::new(),
            sequence_number: None,
            core: Arc::clone(core),
        }
    }
    pub fn with_capacity(core: &RegionCacheMemoryEngineCorePtr, cap: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(cap),
            sequence_number: None,
            core: Arc::clone(core),
        }
    }

    #[inline]
    fn write_impl(&mut self, seq: u64) {
        let mut core = self.core.lock().unwrap();
        let mut skiplist_put_fn = |region_id: u64, cf: String, key: Bytes, value: Bytes| {
            let sl = core.engine.get_mut(&region_id).unwrap().data[cf_to_id(&cf)].clone();
            let _ = sl.put(key, value);
        };
        for entry in self.buffer.iter() {
            entry.append_entry(seq, &mut skiplist_put_fn)
        }
    }
}

#[derive(Clone, Debug)]
enum RegionCacheWriteBatchMutation {
    InsertOrUpdate(Bytes),
    Delete,
}

#[derive(Clone, Debug)]
struct RegionCacheWriteBatchEntry {
    cf: String,
    key: Bytes,
    region_id: u64,
    mutation: RegionCacheWriteBatchMutation,
}

impl RegionCacheWriteBatchEntry {
    pub fn append_entry<F>(&self, seq: u64, mut f: F)
    where
        F: FnMut(u64, String, Bytes, Bytes),
    {
        let (key, value) = match &self.mutation {
            RegionCacheWriteBatchMutation::InsertOrUpdate(value) => {
                let key = encode_key(&self.key, seq, ValueType::Value);
                (key, value.clone())
            }
            RegionCacheWriteBatchMutation::Delete => {
                let key = encode_key(&self.key, seq, ValueType::Deletion);
                (key, Bytes::default())
            }
        };
        f(self.region_id, self.cf.clone(), key, value)
    }
}

impl WriteBatchExt for RegionCacheMemoryEngine {
    type WriteBatch = RegionCacheWriteBatch;
    // todo: adjust it
    const WRITE_BATCH_MAX_KEYS: usize = 256;

    fn write_batch(&self) -> Self::WriteBatch {
        RegionCacheWriteBatch::new(&self.core)
    }

    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch {
        RegionCacheWriteBatch::with_capacity(&self.core, cap)
    }
}

impl WriteBatch for RegionCacheWriteBatch {
    fn write_opt(&mut self, _: &WriteOptions) -> Result<u64> {
        self.sequence_number
            .map(|seq| {
                self.write_impl(seq);
                seq
            })
            .ok_or(box_err!("Sequence number not set"))
    }

    fn data_size(&self) -> usize {
        unimplemented!()
    }

    fn count(&self) -> usize {
        unimplemented!()
    }

    fn is_empty(&self) -> bool {
        unimplemented!()
    }

    fn should_write_to_engine(&self) -> bool {
        unimplemented!()
    }

    fn clear(&mut self) {
        self.buffer.clear()
    }

    fn set_save_point(&mut self) {
        unimplemented!()
    }

    fn pop_save_point(&mut self) -> Result<()> {
        unimplemented!()
    }

    fn rollback_to_save_point(&mut self) -> Result<()> {
        unimplemented!()
    }

    fn merge(&mut self, _: Self) -> Result<()> {
        unimplemented!()
    }

    fn set_sequence_number(&mut self, seq: u64) -> Result<()> {
        if let Some(seqno) = self.sequence_number {
            return Err(box_err!("Sequence number {} already set", seqno));
        };
        self.sequence_number = Some(seq);
        Ok(())
    }

    fn write(&mut self) -> Result<u64> {
        self.write_opt(&WriteOptions::default())
    }
}

impl Mutable for RegionCacheWriteBatch {
    fn put(&mut self, _: &[u8], _: &[u8]) -> Result<()> {
        Err(box_err!(
            "Do not call put directly on RegionCacheWriteBatch, region id must be specified."
        ))
    }

    fn put_cf(&mut self, key: &str, value: &[u8], _: &[u8]) -> Result<()> {
        Err(box_err!(
            "Do not call put_cf directly on RegionCacheWriteBatch, region id must be specified."
        ))
    }

    fn delete(&mut self, _: &[u8]) -> Result<()> {
        Err(box_err!(
            "Do not call delete directly on RegionCacheWriteBatch, region id must be specified."
        ))
    }

    fn delete_cf(&mut self, _: &str, _: &[u8]) -> Result<()> {
        Err(box_err!(
            "Do not call delete_cf directly on RegionCacheWriteBatch, region id must be specified."
        ))
    }

    fn delete_range(&mut self, _: &[u8], _: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn delete_range_cf(&mut self, _: &str, _: &[u8], _: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn put_region(&mut self, region_id: u64, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_region_cf(region_id, CF_DEFAULT, key, value)
    }

    fn put_region_cf(&mut self, region_id: u64, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.buffer.push(RegionCacheWriteBatchEntry {
            key: Bytes::copy_from_slice(key),
            cf: cf.to_owned(),
            region_id,
            mutation: RegionCacheWriteBatchMutation::InsertOrUpdate(Bytes::copy_from_slice(value)),
        });
        Ok(())
    }

    fn delete_region(&mut self, region_id: u64, key: &[u8]) -> Result<()> {
        self.delete_region_cf(region_id, CF_DEFAULT, key)
    }

    fn delete_region_cf(&mut self, region_id: u64, cf: &str, key: &[u8]) -> Result<()> {
        self.buffer.push(RegionCacheWriteBatchEntry {
            key: Bytes::copy_from_slice(key),
            cf: cf.to_owned(),
            region_id,
            mutation: RegionCacheWriteBatchMutation::Delete,
        });
        Ok(())
    }
}
