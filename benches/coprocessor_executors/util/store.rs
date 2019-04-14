// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use tikv::storage::kv::RocksSnapshot;
use tikv::storage::{FixtureStore, SnapshotStore};

/// `MemStore` is a store provider that operates directly over a BTreeMap.
pub type MemStore = FixtureStore;

/// `RocksStore` is a store provider that operates over a disk-based RocksDB storage.
pub type RocksStore = SnapshotStore<RocksSnapshot>;

pub trait StoreDescriber {
    /// Describes a store for Criterion to output.
    fn name() -> String;
}

impl<S: tikv::storage::Store> StoreDescriber for S {
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
