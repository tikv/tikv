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
use tikv::storage::{self, Engine, Key, Mutation, FixtureStore};
use tikv::util::worker::FutureWorker;

pub struct Insert<'a, S: Store + 'a> {
    store: &'a mut S,
    table: &'a Table,
    values: BTreeMap<i64, Datum>,
}

impl<'a, S: Store + 'a> Insert<'a, S> {
    pub fn new(store: &'a mut S, table: &'a Table) -> Self {
        Insert {
            store,
            table,
            values: BTreeMap::new(),
        }
    }

    pub fn set(mut self, col: &Column, value: Datum) -> Self {
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

pub struct Delete<'a, S: Store + 'a> {
    store: &'a mut S,
    table: &'a Table,
}

impl<'a, S: Store + 'a> Delete<'a, S> {
    pub fn new(store: &'a mut S, table: &'a Table) -> Self {
        Delete { store, table }
    }

    pub fn execute(self, id: i64, row: Vec<Datum>) {
        self.execute_with_ctx(Context::new(), id, row)
    }

    pub fn execute_with_ctx(self, ctx: Context, id: i64, row: Vec<Datum>) {
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
        self.store.delete(ctx, keys);
    }
}

pub trait Store {
    fn begin(&mut self);

    fn put(&mut self, ctx: Context, kv: Vec<(Vec<u8>, Vec<u8>)>);

    fn insert_into<'a>(&'a mut self, table: &'a Table) -> Insert<'a, Self>
    where
        Self: Sized,
    {
        Insert::new(self, table)
    }

    fn delete(&mut self, ctx: Context, keys: Vec<Vec<u8>>);

    fn delete_from<'a>(&'a mut self, table: &'a Table) -> Delete<'a, Self>
    where
        Self: Sized,
    {
        Delete::new(self, table)
    }

    fn commit_with_ctx(&mut self, ctx: Context);

    fn commit(&mut self) {
        self.commit_with_ctx(Context::new());
    }
}

/// A store that operates over MVCC and support transactions.
pub struct MvccTransactionalStore<E: Engine> {
    store: SyncStorage<E>,
    current_ts: u64,
    handles: Vec<Vec<u8>>,
}

impl<E: Engine> Store for MvccTransactionalStore<E> {
    fn begin(&mut self) {
        self.current_ts = next_id() as u64;
        self.handles.clear();
    }

    fn put(&mut self, ctx: Context, mut kv: Vec<(Vec<u8>, Vec<u8>)>) {
        self.handles.extend(kv.iter().map(|&(ref k, _)| k.clone()));
        let pk = kv[0].0.clone();
        let kv = kv
            .drain(..)
            .map(|(k, v)| Mutation::Put((Key::from_raw(&k), v)))
            .collect();
        self.store.prewrite(ctx, kv, pk, self.current_ts).unwrap();
    }

    fn delete(&mut self, ctx: Context, mut keys: Vec<Vec<u8>>) {
        self.handles.extend(keys.clone());
        let pk = keys[0].clone();
        let mutations = keys
            .drain(..)
            .map(|k| Mutation::Delete(Key::from_raw(&k)))
            .collect();
        self.store
            .prewrite(ctx, mutations, pk, self.current_ts)
            .unwrap();
    }

    fn commit_with_ctx(&mut self, ctx: Context) {
        let handles = self.handles.drain(..).map(|x| Key::from_raw(&x)).collect();
        self.store
            .commit(ctx, handles, self.current_ts, next_id() as u64)
            .unwrap();
    }
}

impl<E: Engine> MvccTransactionalStore<E> {
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
}

/// A store that operates in memory BTreeMap.
///
/// Data will be written to the underlying BTreeMap only after commit and
/// there is neither support for concurrent transactions nor MVCC.
///
/// Since this store produce a final data view for a series of operations,
/// it is suitable for generating benchmark source so that MVCC cost can be
/// excluded. It is sufficient for initializing final data view for Coprocessor
/// query, but may be not useful for other purpose.
#[derive(Clone, Debug)]
pub struct SimpleBTreeMapStore {
    data: BTreeMap<Key, Vec<u8>>,
    pending_mutations: Vec<Mutation>,
}

impl Store for SimpleBTreeMapStore {
    fn begin(&mut self) {
        self.pending_mutations.clear();
    }

    fn put(&mut self, _ctx: Context, kv: Vec<(Vec<u8>, Vec<u8>)>) {
        let mut mutations = kv
            .into_iter()
            .map(|(k, v)| Mutation::Put((Key::from_raw(&k), v)))
            .collect();
        self.pending_mutations.append(&mut mutations);
    }

    fn delete(&mut self, _ctx: Context, keys: Vec<Vec<u8>>) {
        let mut mutations = keys
            .into_iter()
            .map(|k| Mutation::Delete(Key::from_raw(&k)))
            .collect();
        self.pending_mutations.append(&mut mutations);
    }

    fn commit_with_ctx(&mut self, _ctx: Context) {
        let mutations = ::std::mem::replace(&mut self.pending_mutations, Vec::new());
        mutations.into_iter().for_each(|mutation| match mutation {
            Mutation::Put((k, v)) => {
                self.data.insert(k, v);
            }
            Mutation::Delete(k) => {
                self.data.remove(&k);
            }
            _ => {
                unimplemented!();
            }
        });
    }
}

impl SimpleBTreeMapStore {
    pub fn new() -> Self {
        SimpleBTreeMapStore {
            data: BTreeMap::default(),
            pending_mutations: Vec::new(),
        }
    }

    pub fn into_map(self) -> BTreeMap<Key, Vec<u8>> {
        self.data
    }

    pub fn into_map_ok<E>(self) -> BTreeMap<Key, Result<Vec<u8>, E>> {
        self.data.into_iter().map(|(k, v)| (k, Ok(v))).collect()
    }

    pub fn into_fixture_store(self) -> FixtureStore {
        FixtureStore::new(self.into_map_ok())
    }
}
