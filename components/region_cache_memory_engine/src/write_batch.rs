use bytes::Bytes;
use engine_traits::{Mutable, Result, WriteBatch, WriteBatchExt, WriteOptions};
use tikv_util::box_err;

use crate::RegionCacheMemoryEngine;

type ApplyEncodedEntryCb = Box<dyn FnMut(&str, Bytes, Bytes) -> Result<()> + Send + Sync>;

/// RegionCacheWriteBatch maintains its own in-memory buffer.
pub struct RegionCacheWriteBatch {
    buffer: Vec<RegionCacheWriteBatchEntry>,
    apply_cb: ApplyEncodedEntryCb,
    sequence_number: Option<u64>,
}

impl std::fmt::Debug for RegionCacheWriteBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegionCacheWriteBatch")
            .field("buffer", &self.buffer)
            .finish()
    }
}
impl RegionCacheWriteBatch {
    pub fn new(apply_cb: ApplyEncodedEntryCb) -> Self {
        Self {
            buffer: Vec::new(),
            apply_cb,
            sequence_number: None,
        }
    }

    pub fn with_capacity(apply_cb: ApplyEncodedEntryCb, cap: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(cap),
            apply_cb,
            sequence_number: None,
        }
    }

    /// Sets the sequence number for this batch. This should only be called
    /// prior to writing the batch.
    pub fn set_sequence_number(&mut self, seq: u64) -> Result<()> {
        if let Some(seqno) = self.sequence_number {
            return Err(box_err!("Sequence number {} already set", seqno));
        };
        self.sequence_number = Some(seq);
        Ok(())
    }
}

#[derive(Clone, Debug)]
enum CacheWriteBatchEntryMutation {
    InsertOrUpdate(Bytes),
    Delete,
}

#[derive(Clone, Debug)]
struct RegionCacheWriteBatchEntry {
    cf: String,
    key: Bytes,
    mutation: CacheWriteBatchEntryMutation,
}

impl RegionCacheMemoryEngine {
    fn apply_cb(&self) -> ApplyEncodedEntryCb {
        Box::new(|_cf, _key, _value| Ok(()))
    }
}
impl WriteBatchExt for RegionCacheMemoryEngine {
    type WriteBatch = RegionCacheWriteBatch;
    // todo: adjust it
    const WRITE_BATCH_MAX_KEYS: usize = 256;

    fn write_batch(&self) -> Self::WriteBatch {
        RegionCacheWriteBatch::new(self.apply_cb())
    }

    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch {
        RegionCacheWriteBatch::with_capacity(self.apply_cb(), cap)
    }
}

impl WriteBatch for RegionCacheWriteBatch {
    fn write_opt(&mut self, _: &WriteOptions) -> Result<u64> {
        unimplemented!()
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
        unimplemented!()
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
}

impl Mutable for RegionCacheWriteBatch {
    fn put(&mut self, _: &[u8], _: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn put_cf(&mut self, _: &str, _: &[u8], _: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn delete(&mut self, _: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn delete_cf(&mut self, _: &str, _: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn delete_range(&mut self, _: &[u8], _: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn delete_range_cf(&mut self, _: &str, _: &[u8], _: &[u8]) -> Result<()> {
        unimplemented!()
    }
}
