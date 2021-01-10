// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;
use crate::options::WriteOptions;

/// Engines that can create write batches
pub trait WriteBatchExt: Sized {
    type WriteBatch: WriteBatch<Self>;
    /// `WriteBatchVec` is used for `multi_batch_write` of RocksEngine and other Engine could also
    /// implement another kind of WriteBatch according to their needs.
    type WriteBatchVec: WriteBatch<Self>;

    /// The number of puts/deletes made to a write batch before the batch should
    /// be committed with `write`. More entries than this will cause
    /// `should_write_to_engine` to return true.
    ///
    /// In practice it seems that exceeding this number of entries is possible
    /// and does not result in an error. It isn't clear the consequence of
    /// exceeding this limit.
    const WRITE_BATCH_MAX_KEYS: usize;

    /// Indicates whether the WriteBatchVec type can be created and works
    /// as expected.
    ///
    /// If this returns false then creating a WriteBatchVec will panic.
    fn support_write_batch_vec(&self) -> bool;

    fn write_batch(&self) -> Self::WriteBatch;
    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch;
}

/// A trait implemented by WriteBatch
///
/// FIXME: This trait is misnamed, as it is implemented only by WriteBatch and
/// is filled with WriteBatch-specific methods. Merge it into WriteBatch.
pub trait Mutable: Send {
    /// The data size of a write batch
    ///
    /// This is necessarily engine-dependent. In RocksDB though it appears to
    /// represent the byte length of all write commands in the batch, as
    /// serialized in memory, prior to being written to disk.
    fn data_size(&self) -> usize;

    /// The number of commands in this batch
    fn count(&self) -> usize;

    /// Whether any commands have been issued to this batch
    fn is_empty(&self) -> bool;

    /// Whether the number of commands exceeds WRITE_BATCH_MAX_KEYS
    ///
    /// If so, the `write` method should be called.
    fn should_write_to_engine(&self) -> bool;

    /// Clears the WriteBatch of all commands
    ///
    /// It may be reused afterward as an empty batch.
    fn clear(&mut self);

    /// Push a save point onto the save point stack
    fn set_save_point(&mut self);

    /// Pop a save point from the save point stack
    ///
    /// This has no effect on the commands already issued to the write batch
    fn pop_save_point(&mut self) -> Result<()>;

    /// Revert all commands issued since the last save point
    ///
    /// Additionally pops the last save point from the save point stack.
    fn rollback_to_save_point(&mut self) -> Result<()>;

    /// Write a key/value in the default column family
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Write a key/value in a given column family
    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()>;

    /// Delete a key/value in the default column family
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Delete a key/value in a given column family
    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()>;

    /// Delete a range of key/values in the default column family
    fn delete_range(&mut self, begin_key: &[u8], end_key: &[u8]) -> Result<()>;

    /// Delete a range of key/values in a given column family
    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()>;

    fn put_msg<M: protobuf::Message>(&mut self, key: &[u8], m: &M) -> Result<()> {
        self.put(key, &m.write_to_bytes()?)
    }
    fn put_msg_cf<M: protobuf::Message>(&mut self, cf: &str, key: &[u8], m: &M) -> Result<()> {
        self.put_cf(cf, key, &m.write_to_bytes()?)
    }
}

/// Batches of multiple writes that are committed atomically
///
/// Each write batch consists of a series of commands: put, delete
/// delete_range, and their column-family-specific equivalents.
///
/// Because write batches are atomic, once written to disk all their effects are
/// visible as if all other writes in the system were written either before or
/// after the batch. This includes range deletes.
///
/// The exact strategy used by WriteBatch is up to the implementation.
/// RocksDB though _seems_ to serialize the writes to an in-memory buffer,
/// and then write the whole serialized batch to disk at once.
///
/// Write batches may be reused after being written. In that case they write
/// exactly the same data as previously, Replacing any keys that may have
/// changed in between the two batch writes.
///
/// Commands issued to write batches can be rolled back prior to being committed
/// by use of _save points_. At any point in the life of a write batch a save
/// point can be recorded. Any number of save points can be recorded to a stack.
/// Calling `rollback_to_save_point` reverts all commands issued since the last
/// save point, and pops the save point from the stack.
pub trait WriteBatch<E: WriteBatchExt + Sized>: Mutable {

    /// Create a write batch with a given command capacity
    fn with_capacity(e: &E, cap: usize) -> Self;

    /// Commit the WriteBatch to disk with the given options
    fn write_opt(&self, opts: &WriteOptions) -> Result<()>;

    /// Commit the WriteBatch to disk atomically
    fn write(&self) -> Result<()> {
        self.write_opt(&WriteOptions::default())
    }
}
