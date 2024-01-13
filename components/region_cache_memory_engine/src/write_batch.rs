use bytes::Bytes;
use engine_traits::{Mutable, Result, WriteBatch, WriteBatchExt, WriteOptions, CF_DEFAULT};
use tikv_util::box_err;

use crate::{
    keys::{encode_key, ValueType},
    RegionCacheMemoryEngine,
};

/// Callback to apply an encoded entry to cache engine.
///
/// Arguments: &str - cf name, Bytes - (encoded) key, Bytes - value.
///
/// TODO: consider refactoring into a trait once RegionCacheMemoryEngine API
/// stabilizes.
type ApplyEncodedEntryCb = Box<dyn FnMut(&str, Bytes, Bytes) -> Result<()> + Send + Sync>;

/// RegionCacheWriteBatch maintains its own in-memory buffer.
pub struct RegionCacheWriteBatch {
    buffer: Vec<RegionCacheWriteBatchEntry>,
    apply_cb: ApplyEncodedEntryCb,
    sequence_number: Option<u64>,
    save_points: Vec<usize>,
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
            save_points: Vec::new(),
        }
    }

    pub fn with_capacity(apply_cb: ApplyEncodedEntryCb, cap: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(cap),
            apply_cb,
            sequence_number: None,
            save_points: Vec::new(),
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

    fn write_impl(&mut self, seq: u64) -> Result<()> {
        self.buffer
            .iter()
            .map(|e| (e.cf.as_str(), e.encode(seq)))
            .try_for_each(|(cf, (key, value))| (self.apply_cb)(cf, key, value))
    }
}

#[derive(Clone, Debug)]
enum CacheWriteBatchEntryMutation {
    PutValue(Bytes),
    Deletion,
}

impl CacheWriteBatchEntryMutation {
    fn encode(&self, key: &[u8], seq: u64) -> (Bytes, Bytes) {
        match self {
            CacheWriteBatchEntryMutation::PutValue(value) => {
                (encode_key(key, seq, ValueType::Value), value.clone())
            }
            CacheWriteBatchEntryMutation::Deletion => {
                (encode_key(key, seq, ValueType::Deletion), Bytes::new())
            }
        }
    }
    fn data_size(&self) -> usize {
        match self {
            CacheWriteBatchEntryMutation::PutValue(value) => value.len(),
            CacheWriteBatchEntryMutation::Deletion => 0,
        }
    }
}
#[derive(Clone, Debug)]
struct RegionCacheWriteBatchEntry {
    cf: String,
    key: Bytes,
    mutation: CacheWriteBatchEntryMutation,
}

impl RegionCacheWriteBatchEntry {
    pub fn put_value(cf: &str, key: &[u8], value: &[u8]) -> Self {
        Self {
            cf: cf.to_owned(),
            key: Bytes::copy_from_slice(key),
            mutation: CacheWriteBatchEntryMutation::PutValue(Bytes::copy_from_slice(key)),
        }
    }

    pub fn deletion(cf: &str, key: &[u8]) -> Self {
        Self {
            cf: cf.to_owned(),
            key: Bytes::copy_from_slice(key),
            mutation: CacheWriteBatchEntryMutation::Deletion(Bytes::copy_from_slice(key)),
        }
    }

    #[inline]
    pub fn encode(&self, seq: u64) -> (Bytes, Bytes) {
        self.mutation.encode(&self.key, seq)
    }

    pub fn data_size(&self) -> usize {
        self.key.len() + std::mem::size_of::<u64>() + self.mutation.data_size()
    }
}
impl RegionCacheMemoryEngine {
    fn apply_cb(&self) -> ApplyEncodedEntryCb {
        // TODO: use the stabilized API for appending to the skip list here.
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
        self.sequence_number
            .map(|seq| self.write_impl(seq).and_then(|()| Ok(seq)))
            .transpose()
            .map(|o| o.ok_or_else(|| box_err!("sequence_number must be set!")))?
    }

    fn data_size(&self) -> usize {
        self.buffer
            .iter()
            .map(RegionCacheWriteBatchEntry::data_size)
            .sum()
    }

    fn count(&self) -> usize {
        self.buffer.len()
    }

    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    fn should_write_to_engine(&self) -> bool {
        unimplemented!()
    }

    fn clear(&mut self) {
        self.buffer.clear();
        self.save_points.clear();
        _ = self.sequence_number.take();
    }

    fn set_save_point(&mut self) {
        self.save_points.push(self.buffer.len())
    }

    fn pop_save_point(&mut self) -> Result<()> {
        self.save_points
            .pop()
            .map(|_| ())
            .ok_or_else(|| box_err!("no save points available"))
    }

    fn rollback_to_save_point(&mut self) -> Result<()> {
        self.save_points
            .pop()
            .map(|sp| {
                self.buffer.truncate(sp);
            })
            .ok_or_else(|| box_err!("no save point available!"))
    }

    fn merge(&mut self, mut other: Self) -> Result<()> {
        self.buffer.append(&mut other.buffer);
        Ok(())
    }
}

impl Mutable for RegionCacheWriteBatch {
    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        self.put_cf(CF_DEFAULT, key, val)
    }

    fn put_cf(&mut self, cf: &str, key: &[u8], val: &[u8]) -> Result<()> {
        self.buffer
            .push(RegionCacheWriteBatchEntry::put_value(cf, key, val));
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.delete_cf(CF_DEFAULT, key)
    }

    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        self.buffer
            .push(RegionCacheWriteBatchEntry::deletion(cf, key));
        Ok(())
    }

    fn delete_range(&mut self, _: &[u8], _: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn delete_range_cf(&mut self, _: &str, _: &[u8], _: &[u8]) -> Result<()> {
        unimplemented!()
    }
}
