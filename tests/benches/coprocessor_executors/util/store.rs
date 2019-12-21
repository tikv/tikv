// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tikv::storage::kv::RocksSnapshot;
use tikv::storage::txn::{FixtureStore, SnapshotStore, Store};

/// `MemStore` is a store provider that operates directly over a BTreeMap.
pub type MemStore = FixtureStore;

/// `RocksStore` is a store provider that operates over a disk-based RocksDB storage.
pub type RocksStore = SnapshotStore<RocksSnapshot>;

pub trait StoreDescriber {
    /// Describes a store for Criterion to output.
    fn name() -> String;
}

impl<S: Store> StoreDescriber for S {
    default fn name() -> String {
        unimplemented!()
    }
}

impl StoreDescriber for MemStore {
    fn name() -> String {
        "Memory".to_owned()
    }
}

impl StoreDescriber for RocksStore {
    fn name() -> String {
        "RocksDB".to_owned()
    }
}
