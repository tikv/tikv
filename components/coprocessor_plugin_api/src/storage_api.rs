// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use async_trait::async_trait;
use std::error::Error;
use std::fmt;
use std::ops::Range;
use std::time::Duration;

/// A raw key in the storage.
pub type Key = Vec<u8>;
/// A raw value from the storage.
pub type Value = Vec<u8>;
/// A pair of a raw key and its value.
pub type KvPair = (Key, Value);

/// Result returned by operations on [`RawStorage`].
pub type StorageResult<T> = std::result::Result<T, StorageError>;

/// Some information about the current region the coprocessor is running in.
#[derive(Debug, Clone)]
pub struct Region {
    pub id: u64,
    pub start_key: Key,
    pub end_key: Key,
    pub region_epoch: RegionEpoch,
}

#[derive(Debug, Clone)]
pub struct RegionEpoch {
    pub conf_ver: u64,
    pub version: u64,
}

/// Errors when operating on [`RawStorage`].
#[derive(Debug)]
pub enum StorageError {
    KeyNotInRegion {
        key: Key,
        region: Region,
        start_key: Key,
        end_key: Key,
    },
    Timeout(Duration),
    Canceled,

    /// Errors that can not be handled by a coprocessor plugin but should instead be returned to the
    /// client.
    ///
    /// If such an error appears, plugins can run some cleanup code and return early from the
    /// request. The error will be passed to the client and the client might retry the request.
    /// TODO: we said `Box<dyn Any>`, but isn't `Box<dyn Error>` also fine?
    Other(Box<dyn Error>),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StorageError::KeyNotInRegion {
                key,
                region,
                start_key,
                end_key,
            } => write!(f, "key {:?} not found in region {:?}", key, region.id),
            StorageError::Timeout(d) => write!(f, "timeout after {:?}", d),
            StorageError::Canceled => write!(f, "request canceled"),
            StorageError::Other(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for StorageError {}

/// Storage access for coprocessor plugins.
///
/// [`RawStorage`] allows coprocessor plugins to interact with TiKV storage on a low level.
///
/// Batch operations should be preferred due to their better performance.
#[async_trait(?Send)]
pub trait RawStorage {
    /// Retrieves the value for a given key from the storage on the current node.
    /// Returns [`Option::None`] if the key is not present in the database.
    async fn get(&self, key: Key) -> StorageResult<Option<Value>>;

    /// Same as [`RawStorage::get()`], but retrieves values for multiple keys at once.
    async fn batch_get(&self, keys: Vec<Key>) -> StorageResult<Vec<KvPair>>;

    /// Same as [`RawStorage::get()`], but accepts a `key_range` such that values for keys in
    /// `[key_range.start, key_range.end)` are retrieved.
    /// The upper bound of the `key_range` is exclusive.
    async fn scan(&self, key_range: Range<Key>) -> StorageResult<Vec<Value>>;

    /// Inserts a new key-value pair into the storage on the current node.
    async fn put(&self, key: Key, value: Value) -> StorageResult<()>;

    /// Same as [`RawStorage::put()`], but inserts multiple key-value pairs at once.
    async fn batch_put(&self, kv_pairs: Vec<KvPair>) -> StorageResult<()>;

    /// Deletes a key-value pair from the storage on the current node given a `key`.
    /// Returns [`Result::Ok]` if the key was successfully deleted.
    async fn delete(&self, key: Key) -> StorageResult<()>;

    /// Same as [`RawStorage::delete()`], but deletes multiple key-value pairs at once.
    async fn batch_delete(&self, keys: Vec<Key>) -> StorageResult<()>;

    /// Same as [`RawStorage::delete()`], but deletes multiple key-values pairs at once
    /// given a `key_range`. All records with keys in `[key_range.start, key_range.end)`
    /// will be deleted. The upper bound of the `key_range` is exclusive.
    async fn delete_range(&self, key_range: Range<Key>) -> StorageResult<()>;
}
