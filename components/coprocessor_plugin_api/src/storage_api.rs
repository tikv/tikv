// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::Range;

use async_trait::async_trait;

use crate::PluginResult;

/// A raw key in the storage.
pub type Key = Vec<u8>;
/// A raw value from the storage.
pub type Value = Vec<u8>;
/// A pair of a raw key and its value.
pub type KvPair = (Key, Value);

/// Storage access for coprocessor plugins.
///
/// [`RawStorage`] allows coprocessor plugins to interact with TiKV storage on a low level.
///
/// Batch operations should be preferred due to their better performance.
#[async_trait(?Send)]
pub trait RawStorage {
    /// Retrieves the value for a given key from the storage on the current node.
    /// Returns [`Option::None`] if the key is not present in the database.
    async fn get(&self, key: Key) -> PluginResult<Option<Value>>;

    /// Same as [`RawStorage::get()`], but retrieves values for multiple keys at once.
    async fn batch_get(&self, keys: Vec<Key>) -> PluginResult<Vec<KvPair>>;

    /// Same as [`RawStorage::get()`], but accepts a `key_range` such that values for keys in
    /// `[key_range.start, key_range.end)` are retrieved.
    /// The upper bound of the `key_range` is exclusive.
    async fn scan(&self, key_range: Range<Key>) -> PluginResult<Vec<Value>>;

    /// Inserts a new key-value pair into the storage on the current node.
    async fn put(&self, key: Key, value: Value) -> PluginResult<()>;

    /// Same as [`RawStorage::put()`], but inserts multiple key-value pairs at once.
    async fn batch_put(&self, kv_pairs: Vec<KvPair>) -> PluginResult<()>;

    /// Deletes a key-value pair from the storage on the current node given a `key`.
    /// Returns [`Result::Ok]` if the key was successfully deleted.
    async fn delete(&self, key: Key) -> PluginResult<()>;

    /// Same as [`RawStorage::delete()`], but deletes multiple key-value pairs at once.
    async fn batch_delete(&self, keys: Vec<Key>) -> PluginResult<()>;

    /// Same as [`RawStorage::delete()`], but deletes multiple key-values pairs at once
    /// given a `key_range`. All records with keys in `[key_range.start, key_range.end)`
    /// will be deleted. The upper bound of the `key_range` is exclusive.
    async fn delete_range(&self, key_range: Range<Key>) -> PluginResult<()>;
}
