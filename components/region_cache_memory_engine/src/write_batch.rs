use bytes::Bytes;
use engine_traits::{Mutable, Result, WriteBatch, WriteBatchExt, WriteOptions};
use tikv_util::box_err;

use crate::RangeCacheMemoryEngine;

/// RangeCacheWriteBatch maintains its own in-memory buffer.
#[derive(Default, Clone, Debug)]
pub struct RangeCacheWriteBatch {
    buffer: Vec<RangeCacheWriteBatchEntry>,
    sequence_number: Option<u64>,
}

impl RangeCacheWriteBatch {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(cap),
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
struct RangeCacheWriteBatchEntry {
    cf: String,
    key: Bytes,
    mutation: (), // TODO,
}

impl WriteBatchExt for RangeCacheMemoryEngine {
    type WriteBatch = RangeCacheWriteBatch;
    // todo: adjust it
    const WRITE_BATCH_MAX_KEYS: usize = 256;

    fn write_batch(&self) -> Self::WriteBatch {
        RangeCacheWriteBatch::default()
    }

    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch {
        RangeCacheWriteBatch::with_capacity(cap)
    }
}

impl WriteBatch for RangeCacheWriteBatch {
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

impl Mutable for RangeCacheWriteBatch {
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
