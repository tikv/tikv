// Copyright 2018 PingCAP, Inc.
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

use super::*;

use std::collections::{BTreeMap, HashMap};

use kvproto::kvrpcpb::Context;

use test_storage::SyncStorage;
use tikv::coprocessor::codec::{datum, table, Datum};
use tikv::server::readpool::{self, ReadPool};
use tikv::storage::{self, Engine, Key, Mutation};
use tikv::util::worker::FutureWorker;

pub struct Insert<'a, E: Engine> {
    store: &'a mut Store<E>,
    table: &'a Table,
    values: BTreeMap<i64, Datum>,
}

impl<'a, E: Engine> Insert<'a, E> {
    pub fn new(store: &'a mut Store<E>, table: &'a Table) -> Self {
        Insert {
            store,
            table,
            values: BTreeMap::new(),
        }
    }

    pub fn set(mut self, col: Column, value: Datum) -> Self {
        assert!(self.table.cols.contains_key(&col.id));
        self.values.insert(col.id, value);
        self
    }

    pub fn execute(self) -> i64 {
        self.execute_with_ctx(Context::new())
    }

    pub fn execute_with_ctx(self, ctx: Context) -> i64 {
        let handle = self
            .values
            .get(&self.table.handle_id)
            .cloned()
            .unwrap_or_else(|| Datum::I64(next_id()));
        let key = table::encode_row_key(self.table.id, handle.i64());
        let ids: Vec<_> = self.values.keys().cloned().collect();
        let values: Vec<_> = self.values.values().cloned().collect();
        let value = table::encode_row(values, &ids).unwrap();
        let mut kvs = vec![];
        kvs.push((key, value));
        for (&id, idxs) in &self.table.idxs {
            let mut v: Vec<_> = idxs.iter().map(|id| self.values[id].clone()).collect();
            v.push(handle.clone());
            let encoded = datum::encode_key(&v).unwrap();
            let idx_key = table::encode_index_seek_key(self.table.id, id, &encoded);
            kvs.push((idx_key, vec![0]));
        }
        self.store.put(ctx, kvs);
        handle.i64()
    }
}

pub struct Delete<'a, E: Engine> {
    store: &'a mut Store<E>,
    table: &'a Table,
}

impl<'a, E: Engine> Delete<'a, E> {
    pub fn new(store: &'a mut Store<E>, table: &'a Table) -> Self {
        Delete { store, table }
    }

    pub fn execute(self, id: i64, row: Vec<Datum>) {
        let mut values = HashMap::new();
        for (&id, v) in self.table.cols.keys().zip(row) {
            values.insert(id, v);
        }
        let key = table::encode_row_key(self.table.id, id);
        let mut keys = vec![];
        keys.push(key);
        for (&idx_id, idx_cols) in &self.table.idxs {
            let mut v: Vec<_> = idx_cols.iter().map(|id| values[id].clone()).collect();
            v.push(Datum::I64(id));
            let encoded = datum::encode_key(&v).unwrap();
            let idx_key = table::encode_index_seek_key(self.table.id, idx_id, &encoded);
            keys.push(idx_key);
        }
        self.store.delete(keys);
    }
}

pub struct Store<E: Engine> {
    store: SyncStorage<E>,
    current_ts: u64,
    handles: Vec<Vec<u8>>,
}

impl<E: Engine> Store<E> {
    pub fn new(engine: E) -> Self {
        let pd_worker = FutureWorker::new("test-future–worker");
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || storage::ReadPoolContext::new(pd_worker.scheduler())
        });
        Self {
            store: SyncStorage::from_engine(engine, &Default::default(), read_pool),
            current_ts: 1,
            handles: vec![],
        }
    }

    pub fn get_engine(&self) -> E {
        self.store.get_engine()
    }

    pub fn begin(&mut self) {
        self.current_ts = next_id() as u64;
        self.handles.clear();
    }

    pub fn insert_into<'a>(&'a mut self, table: &'a Table) -> Insert<'a, E> {
        Insert::new(self, table)
    }

    pub fn put(&mut self, ctx: Context, mut kv: Vec<(Vec<u8>, Vec<u8>)>) {
        self.handles.extend(kv.iter().map(|&(ref k, _)| k.clone()));
        let pk = kv[0].0.clone();
        let kv = kv
            .drain(..)
            .map(|(k, v)| Mutation::Put((Key::from_raw(&k), v)))
            .collect();
        self.store.prewrite(ctx, kv, pk, self.current_ts).unwrap();
    }

    pub fn delete_from<'a>(&'a mut self, table: &'a Table) -> Delete<'a, E> {
        Delete::new(self, table)
    }

    pub fn delete(&mut self, mut keys: Vec<Vec<u8>>) {
        self.handles.extend(keys.clone());
        let pk = keys[0].clone();
        let mutations = keys
            .drain(..)
            .map(|k| Mutation::Delete(Key::from_raw(&k)))
            .collect();
        self.store
            .prewrite(Context::new(), mutations, pk, self.current_ts)
            .unwrap();
    }

    pub fn commit(&mut self) {
        self.commit_with_ctx(Context::new());
    }

    pub fn commit_with_ctx(&mut self, ctx: Context) {
        let handles = self.handles.drain(..).map(|x| Key::from_raw(&x)).collect();
        self.store
            .commit(ctx, handles, self.current_ts, next_id() as u64)
            .unwrap();
    }
}
