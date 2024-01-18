use bytes::Bytes;
use engine_traits::{Mutable, Result, WriteBatch, WriteBatchExt, WriteOptions, CF_DEFAULT};
use tikv_util::box_err;

use crate::{
    engine::{cf_to_id, SkiplistEngine},
    keys::{encode_key, ValueType},
    RangeCacheMemoryEngine,
};

/// Callback to apply an encoded entry to cache engine.
///
/// Arguments: &str - cf name, Bytes - (encoded) key, Bytes - value.
///
/// TODO: consider refactoring into a trait once RangeCacheMemoryEngine API
/// stabilizes.
type ApplyEncodedEntryCb = Box<dyn FnMut(&str, Bytes, Bytes) -> Result<()> + Send + Sync>;

/// RangeCacheWriteBatch maintains its own in-memory buffer.
pub struct RangeCacheWriteBatch {
    buffer: Vec<RangeCacheWriteBatchEntry>,
    apply_cb: ApplyEncodedEntryCb,
    sequence_number: Option<u64>,
    save_points: Vec<usize>,
}

impl std::fmt::Debug for RangeCacheWriteBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RangeCacheWriteBatch")
            .field("buffer", &self.buffer)
            .finish()
    }
}

impl RangeCacheWriteBatch {
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
struct RangeCacheWriteBatchEntry {
    cf: String,
    key: Bytes,
    mutation: CacheWriteBatchEntryMutation,
}

impl RangeCacheWriteBatchEntry {
    pub fn put_value(cf: &str, key: &[u8], value: &[u8]) -> Self {
        Self {
            cf: cf.to_owned(),
            key: Bytes::copy_from_slice(key),
            mutation: CacheWriteBatchEntryMutation::PutValue(Bytes::copy_from_slice(value)),
        }
    }

    pub fn deletion(cf: &str, key: &[u8]) -> Self {
        Self {
            cf: cf.to_owned(),
            key: Bytes::copy_from_slice(key),
            mutation: CacheWriteBatchEntryMutation::Deletion,
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
impl RangeCacheMemoryEngine {
    fn apply_cb(&self) -> ApplyEncodedEntryCb {
        // TODO: use the stabilized API for appending to the skip list here.
        Box::new(|_cf, _key, _value| Ok(()))
    }
}

impl From<&SkiplistEngine> for RangeCacheWriteBatch {
    fn from(engine: &SkiplistEngine) -> Self {
        let engine_clone = engine.clone();
        let apply_cb = Box::new(move |cf: &'_ str, key, value| {
            engine_clone.data[cf_to_id(cf)].put(key, value);
            Ok(())
        });
        RangeCacheWriteBatch::new(apply_cb)
    }
}

impl WriteBatchExt for RangeCacheMemoryEngine {
    type WriteBatch = RangeCacheWriteBatch;
    // todo: adjust it
    const WRITE_BATCH_MAX_KEYS: usize = 256;

    fn write_batch(&self) -> Self::WriteBatch {
        RangeCacheWriteBatch::new(self.apply_cb())
    }

    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch {
        RangeCacheWriteBatch::with_capacity(self.apply_cb(), cap)
    }
}

impl WriteBatch for RangeCacheWriteBatch {
    fn write_opt(&mut self, _: &WriteOptions) -> Result<u64> {
        self.sequence_number
            .map(|seq| self.write_impl(seq).map(|()| seq))
            .transpose()
            .map(|o| o.ok_or_else(|| box_err!("sequence_number must be set!")))?
    }

    fn data_size(&self) -> usize {
        self.buffer
            .iter()
            .map(RangeCacheWriteBatchEntry::data_size)
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

impl Mutable for RangeCacheWriteBatch {
    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        self.put_cf(CF_DEFAULT, key, val)
    }

    fn put_cf(&mut self, cf: &str, key: &[u8], val: &[u8]) -> Result<()> {
        self.buffer
            .push(RangeCacheWriteBatchEntry::put_value(cf, key, val));
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.delete_cf(CF_DEFAULT, key)
    }

    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        self.buffer
            .push(RangeCacheWriteBatchEntry::deletion(cf, key));
        Ok(())
    }

    fn delete_range(&mut self, _: &[u8], _: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn delete_range_cf(&mut self, _: &str, _: &[u8], _: &[u8]) -> Result<()> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use engine_traits::{CacheRange, Peekable, RangeCacheEngine, WriteBatch};

    use super::*;

    #[test]
    fn test_write_to_skiplist() {
        let engine = SkiplistEngine::new(Arc::default());
        let mut wb = RangeCacheWriteBatch::from(&engine);
        wb.put(b"aaa", b"bbb").unwrap();
        wb.set_sequence_number(1).unwrap();
        assert_eq!(wb.write().unwrap(), 1);
        let sl = engine.data[cf_to_id(CF_DEFAULT)].clone();
        let actual = sl.get(&encode_key(b"aaa", 1, ValueType::Value)).unwrap();
        assert_eq!(&b"bbb"[..], actual)
    }

    #[test]
    fn test_savepoints() {
        let engine = SkiplistEngine::new(Arc::default());
        let mut wb = RangeCacheWriteBatch::from(&engine);
        wb.put(b"aaa", b"bbb").unwrap();
        wb.set_save_point();
        wb.put(b"aaa", b"ccc").unwrap();
        wb.put(b"ccc", b"ddd").unwrap();
        wb.rollback_to_save_point().unwrap();
        wb.set_sequence_number(1).unwrap();
        assert_eq!(wb.write().unwrap(), 1);
        let sl = engine.data[cf_to_id(CF_DEFAULT)].clone();
        let actual = sl.get(&encode_key(b"aaa", 1, ValueType::Value)).unwrap();
        assert_eq!(&b"bbb"[..], actual);
        assert!(sl.get(&encode_key(b"ccc", 1, ValueType::Value)).is_none())
    }

    #[test]
    fn test_put_write_clear_delete_put_write() {
        let engine = RangeCacheMemoryEngine::new(Arc::default());
        let r = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(r.clone());
        let engine_for_writes = {
            let mut core = engine.core.lock().unwrap();
            core.mut_range_manager().set_range_readable(&r, true);
            core.mut_range_manager().set_safe_ts(&r, 10);
            core.engine()
        };
        let mut wb = RangeCacheWriteBatch::from(&engine_for_writes);
        wb.put(b"aaa", b"bbb").unwrap();
        wb.set_sequence_number(1).unwrap();
        _ = wb.write().unwrap();
        wb.clear();
        wb.put(b"bbb", b"ccc").unwrap();
        wb.delete(b"aaa").unwrap();
        wb.set_sequence_number(2).unwrap();
        _ = wb.write().unwrap();
        let snapshot = engine.snapshot(r, u64::MAX, 2).unwrap();
        assert_eq!(
            snapshot.get_value(&b"bbb"[..]).unwrap().unwrap(),
            &b"ccc"[..]
        );
        assert!(snapshot.get_value(&b"aaa"[..]).unwrap().is_none())
    }
}
