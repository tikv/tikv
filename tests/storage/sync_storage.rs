// Copyright 2016 PingCAP, Inc.
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

use std::sync::Arc;
use tikv::storage::{Storage, Engine, Key, Value, KvPair, Mutation, Result};
use tikv::storage::config::Config;
use kvproto::kvrpcpb::Context;

/// `SyncStorage` wraps `Storage` with sync API, usually used for testing.
pub struct SyncStorage(Storage);

impl SyncStorage {
    pub fn new(config: &Config) -> SyncStorage {
        let mut storage = Storage::new(&config).unwrap();
        storage.start(&config).unwrap();
        SyncStorage(storage)
    }

    pub fn from_engine(engine: Box<Engine>, config: &Config) -> SyncStorage {
        let mut storage = Storage::from_engine(engine, config).unwrap();
        storage.start(&config).unwrap();
        SyncStorage(storage)
    }

    pub fn get_engine(&self) -> Arc<Box<Engine>> {
        self.0.get_engine()
    }

    pub fn get(&self, ctx: Context, key: &Key, start_ts: u64) -> Result<Option<Value>> {
        wait_event!(|cb| self.0.async_get(ctx, key.to_owned(), start_ts, cb).unwrap()).unwrap()
    }

    #[allow(dead_code)]
    pub fn batch_get(&self,
                     ctx: Context,
                     keys: &[Key],
                     start_ts: u64)
                     -> Result<Vec<Result<KvPair>>> {
        wait_event!(|cb| self.0.async_batch_get(ctx, keys.to_owned(), start_ts, cb).unwrap())
            .unwrap()
    }

    pub fn scan(&self,
                ctx: Context,
                key: Key,
                limit: usize,
                start_ts: u64)
                -> Result<Vec<Result<KvPair>>> {
        wait_event!(|cb| self.0.async_scan(ctx, key, limit, start_ts, cb).unwrap()).unwrap()
    }

    pub fn prewrite(&self,
                    ctx: Context,
                    mutations: Vec<Mutation>,
                    primary: Vec<u8>,
                    start_ts: u64)
                    -> Result<Vec<Result<()>>> {
        wait_event!(|cb| self.0.async_prewrite(ctx, mutations, primary, start_ts, cb).unwrap())
            .unwrap()
    }

    pub fn commit(&self,
                  ctx: Context,
                  keys: Vec<Key>,
                  start_ts: u64,
                  commit_ts: u64)
                  -> Result<()> {
        wait_event!(|cb| self.0.async_commit(ctx, keys, start_ts, commit_ts, cb).unwrap()).unwrap()
    }

    pub fn commit_then_get(&self,
                           ctx: Context,
                           key: Key,
                           lock_ts: u64,
                           commit_ts: u64,
                           get_ts: u64)
                           -> Result<Option<Value>> {
        wait_event!(|cb| {
                self.0
                    .async_commit_then_get(ctx, key, lock_ts, commit_ts, get_ts, cb)
                    .unwrap();
            })
            .unwrap()
    }

    #[allow(dead_code)]
    pub fn cleanup(&self, ctx: Context, key: Key, start_ts: u64) -> Result<()> {
        wait_event!(|cb| self.0.async_cleanup(ctx, key, start_ts, cb).unwrap()).unwrap()
    }

    pub fn rollback(&self, ctx: Context, keys: Vec<Key>, start_ts: u64) -> Result<()> {
        wait_event!(|cb| self.0.async_rollback(ctx, keys, start_ts, cb).unwrap()).unwrap()
    }

    pub fn rollback_then_get(&self, ctx: Context, key: Key, lock_ts: u64) -> Result<Option<Value>> {
        wait_event!(|cb| self.0.async_rollback_then_get(ctx, key, lock_ts, cb).unwrap()).unwrap()
    }
}

impl Drop for SyncStorage {
    fn drop(&mut self) {
        self.0.stop().unwrap()
    }
}
