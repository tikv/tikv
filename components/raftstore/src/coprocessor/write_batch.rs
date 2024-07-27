// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CacheRange, Mutable, Result, WriteBatch, WriteOptions, CF_DEFAULT};

pub trait WriteBatchObserver: Send {
    fn create_observable_write_batch(&self) -> Box<dyn ObservableWriteBatch>;
}

/// A simplified version of `engine_trait::WriteBatch` that observe the write
/// operations of a `engine_trait::WriteBatch`.
///
/// It exists because raftstore coprocessor only accepts trait object while
/// the original `engine_trait::WriteBatch` can't be a trait object.
// TODO: May be we can unified it with `CmdObserver`?
pub trait ObservableWriteBatch: Send {
    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]);
    fn delete_cf(&mut self, cf: &str, key: &[u8]);
    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]);
    fn set_save_point(&mut self);
    fn pop_save_point(&mut self);
    fn rollback_to_save_point(&mut self);
    fn clear(&mut self);
    fn merge(&mut self, other: Vec<u8>);
    fn to_vec(&mut self) -> Vec<u8>;
    fn write_opt(&mut self, opts: &WriteOptions, seq_num: u64);
    fn prepare_for_range(&mut self, range: CacheRange);
}

pub struct WriteBatchWrapper<WB> {
    write_batch: WB,
    observable_write_batch: Option<Box<dyn ObservableWriteBatch>>,
}

impl<WB> WriteBatchWrapper<WB> {
    pub fn new(
        write_batch: WB,
        observable_write_batch: Option<Box<dyn ObservableWriteBatch>>,
    ) -> Self {
        Self {
            write_batch,
            observable_write_batch,
        }
    }
}

impl<WB: WriteBatch> WriteBatch for WriteBatchWrapper<WB> {
    fn write_opt(&mut self, opts: &WriteOptions) -> Result<u64> {
        self.write_callback_opt(opts, |_| ())
    }

    fn write_callback_opt(&mut self, opts: &WriteOptions, mut cb: impl FnMut(u64)) -> Result<u64> {
        self.write_batch.write_callback_opt(opts, |s| {
            self.observable_write_batch
                .as_mut()
                .map(|w| w.write_opt(opts, s));
            cb(s);
        })
    }

    fn data_size(&self) -> usize {
        self.write_batch.data_size()
    }

    fn count(&self) -> usize {
        self.write_batch.count()
    }

    fn is_empty(&self) -> bool {
        self.write_batch.is_empty()
    }

    fn should_write_to_engine(&self) -> bool {
        self.write_batch.should_write_to_engine()
    }

    fn clear(&mut self) {
        self.observable_write_batch.as_mut().map(|w| w.clear());
        self.write_batch.clear();
    }

    fn set_save_point(&mut self) {
        self.observable_write_batch
            .as_mut()
            .map(|w| w.set_save_point());
        self.write_batch.set_save_point()
    }

    fn pop_save_point(&mut self) -> Result<()> {
        self.observable_write_batch
            .as_mut()
            .map(|w| w.pop_save_point());
        self.write_batch.pop_save_point()
    }

    fn rollback_to_save_point(&mut self) -> Result<()> {
        self.observable_write_batch
            .as_mut()
            .map(|w| w.rollback_to_save_point());
        self.write_batch.rollback_to_save_point()
    }

    fn merge(&mut self, mut other: Self) -> Result<()> {
        self.observable_write_batch
            .as_mut()
            .map(|w| w.merge(other.observable_write_batch.as_mut().unwrap().to_vec()));
        self.write_batch.merge(other.write_batch)
    }

    fn prepare_for_range(&mut self, range: CacheRange) {
        self.observable_write_batch
            .as_mut()
            .map(|w| w.prepare_for_range(range));
    }
}

impl<WB: WriteBatch> Mutable for WriteBatchWrapper<WB> {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.observable_write_batch
            .as_mut()
            .map(|w| w.put_cf(CF_DEFAULT, key, value));
        self.write_batch.put(key, value)
    }

    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.observable_write_batch
            .as_mut()
            .map(|w| w.put_cf(cf, key, value));
        self.write_batch.put_cf(cf, key, value)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.observable_write_batch
            .as_mut()
            .map(|w| w.delete_cf(CF_DEFAULT, key));
        self.write_batch.delete(key)
    }

    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        self.observable_write_batch
            .as_mut()
            .map(|w| w.delete_cf(cf, key));
        self.write_batch.delete_cf(cf, key)
    }

    fn delete_range(&mut self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        // delete_range in range cache engine means eviction -- all ranges overlapped
        // with [begin_key, end_key] will be evicted.
        self.observable_write_batch
            .as_mut()
            .map(|w| w.delete_range_cf(CF_DEFAULT, begin_key, end_key));
        self.write_batch.delete_range(begin_key, end_key)
    }

    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        // delete_range in range cache engine means eviction -- all ranges overlapped
        // with [begin_key, end_key] will be evicted.
        self.observable_write_batch
            .as_mut()
            .map(|w| w.delete_range_cf(cf, begin_key, end_key));
        self.write_batch.delete_range_cf(cf, begin_key, end_key)
    }

    // Override the default methods `put_msg` and `put_msg_cf` to prevent
    // potential loss of put observations if WB also overrides them.
    fn put_msg<M: protobuf::Message>(&mut self, key: &[u8], m: &M) -> Result<()> {
        // It's okay to call `self.put` even though it does not strictly
        // follow the `put_msg` semantics, as there are no implementors
        // that override it.
        self.put(key, &m.write_to_bytes()?)
    }
    fn put_msg_cf<M: protobuf::Message>(&mut self, cf: &str, key: &[u8], m: &M) -> Result<()> {
        // See put_msg.
        self.put_cf(cf, key, &m.write_to_bytes()?)
    }
}
