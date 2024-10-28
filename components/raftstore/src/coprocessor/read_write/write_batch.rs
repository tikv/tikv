// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicBool, Ordering};

use engine_traits::{Mutable, Result, WriteBatch, WriteOptions, CF_DEFAULT};
use kvproto::metapb;

pub trait WriteBatchObserver: Send {
    fn create_observable_write_batch(&self) -> Box<dyn ObservableWriteBatch>;
}

/// It observes write operations of an `engine_trait::WriteBatch`, and provides
/// additional methods to specify which region the write operations belong to.
// TODO: May be we can unified it with `CmdObserver`?
pub trait ObservableWriteBatch: WriteBatch + Send {
    /// It declares that the following consecutive write will be within this
    /// region.
    fn prepare_for_region(&mut self, region: &metapb::Region);
    /// Commit the WriteBatch with the given options and sequence number.
    fn write_opt_seq(&mut self, opts: &WriteOptions, seq_num: u64);
    /// It is called after a write operation is finished.
    fn post_write(&mut self);
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

    pub fn prepare_for_region(&mut self, region: &metapb::Region) {
        if let Some(w) = self.observable_write_batch.as_mut() {
            w.prepare_for_region(region)
        }
    }
}

impl<WB: WriteBatch> WriteBatch for WriteBatchWrapper<WB> {
    fn write(&mut self) -> Result<u64> {
        self.write_opt(&WriteOptions::default())
    }

    fn write_opt(&mut self, opts: &WriteOptions) -> Result<u64> {
        self.write_callback_opt(opts, |_| ())
    }

    fn write_callback_opt(&mut self, opts: &WriteOptions, mut cb: impl FnMut(u64)) -> Result<u64> {
        let called = AtomicBool::new(false);
        let res = self.write_batch.write_callback_opt(opts, |s| {
            if !called.fetch_or(true, Ordering::SeqCst) {
                if let Some(w) = self.observable_write_batch.as_mut() {
                    w.write_opt_seq(opts, s);
                }
            }
            cb(s);
        });
        if let Some(w) = self.observable_write_batch.as_mut() {
            w.post_write();
        }
        res
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
        if let Some(w) = self.observable_write_batch.as_mut() {
            w.clear()
        }
        self.write_batch.clear();
    }

    fn set_save_point(&mut self) {
        if let Some(w) = self.observable_write_batch.as_mut() {
            w.set_save_point()
        }
        self.write_batch.set_save_point()
    }

    fn pop_save_point(&mut self) -> Result<()> {
        if let Some(w) = self.observable_write_batch.as_mut() {
            w.pop_save_point()?;
        }
        self.write_batch.pop_save_point()
    }

    fn rollback_to_save_point(&mut self) -> Result<()> {
        if let Some(w) = self.observable_write_batch.as_mut() {
            w.rollback_to_save_point()?;
        }
        self.write_batch.rollback_to_save_point()
    }

    fn merge(&mut self, _: Self) -> Result<()> {
        unimplemented!("WriteBatchWrapper does not support merge")
    }
}

impl<WB: WriteBatch> Mutable for WriteBatchWrapper<WB> {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if let Some(w) = self.observable_write_batch.as_mut() {
            w.put_cf(CF_DEFAULT, key, value)?;
        }
        self.write_batch.put(key, value)
    }

    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        if let Some(w) = self.observable_write_batch.as_mut() {
            w.put_cf(cf, key, value)?;
        }
        self.write_batch.put_cf(cf, key, value)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        if let Some(w) = self.observable_write_batch.as_mut() {
            w.delete_cf(CF_DEFAULT, key)?;
        }
        self.write_batch.delete(key)
    }

    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        if let Some(w) = self.observable_write_batch.as_mut() {
            w.delete_cf(cf, key)?;
        }
        self.write_batch.delete_cf(cf, key)
    }

    fn delete_range(&mut self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        if let Some(w) = self.observable_write_batch.as_mut() {
            w.delete_range_cf(CF_DEFAULT, begin_key, end_key)?;
        }
        self.write_batch.delete_range(begin_key, end_key)
    }

    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        if let Some(w) = self.observable_write_batch.as_mut() {
            w.delete_range_cf(cf, begin_key, end_key)?;
        }
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
