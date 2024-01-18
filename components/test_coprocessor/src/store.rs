// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeMap;

use collections::HashMap;
use kvproto::kvrpcpb::{Context, IsolationLevel};
use test_storage::SyncTestStorageApiV1;
use tidb_query_datatype::{
    codec::{
        data_type::ScalarValue,
        datum,
        row::v2::encoder_for_test::{Column as ColumnV2, RowEncoder},
        table, Datum,
    },
    expr::EvalContext,
};
use tikv::{
    server::gc_worker::GcConfig,
    storage::{
        kv::{Engine, RocksEngine},
        lock_manager::MockLockManager,
        txn::FixtureStore,
        SnapshotStore, StorageApiV1, TestStorageBuilderApiV1,
    },
};
use txn_types::{Key, Mutation, TimeStamp};

use super::*;

pub struct Insert<'a, E: Engine> {
    store: &'a mut Store<E>,
    table: &'a Table,
    values: BTreeMap<i64, Datum>,
    values_v2: BTreeMap<i64, ScalarValue>,
}

impl<'a, E: Engine> Insert<'a, E> {
    pub fn new(store: &'a mut Store<E>, table: &'a Table) -> Self {
        Insert {
            store,
            table,
            values: BTreeMap::new(),
            values_v2: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn set(mut self, col: &Column, value: Datum) -> Self {
        assert!(self.table.column_by_id(col.id).is_some());
        self.values.insert(col.id, value);
        self
    }

    pub fn set_v2(mut self, col: &Column, value: ScalarValue) -> Self {
        assert!(self.table.column_by_id(col.id).is_some());
        self.values_v2.insert(col.id, value);
        self
    }

    pub fn execute(self) -> i64 {
        self.execute_with_ctx(Context::default())
    }

    fn prepare_index_kv(&self, handle: &Datum, buf: &mut Vec<(Vec<u8>, Vec<u8>)>) {
        for (&id, idxs) in &self.table.idxs {
            let mut v: Vec<_> = idxs.iter().map(|id| self.values[id].clone()).collect();
            v.push(handle.clone());
            let encoded = datum::encode_key(&mut EvalContext::default(), &v).unwrap();
            let idx_key = table::encode_index_seek_key(self.table.id, id, &encoded);
            buf.push((idx_key, vec![0]));
        }
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
        let value = table::encode_row(&mut EvalContext::default(), values, &ids).unwrap();
        let mut kvs = vec![(key, value)];
        self.prepare_index_kv(&handle, &mut kvs);
        self.store.put(ctx, kvs);
        handle.i64()
    }

    pub fn execute_with_v2_checksum(
        self,
        ctx: Context,
        with_checksum: bool,
        extra_checksum: Option<u32>,
    ) -> i64 {
        let handle = self
            .values
            .get(&self.table.handle_id)
            .cloned()
            .unwrap_or_else(|| Datum::I64(next_id()));
        let key = table::encode_row_key(self.table.id, handle.i64());
        let mut columns: Vec<ColumnV2> = Vec::new();
        for (id, value) in self.values_v2.iter() {
            let col_info = self.table.column_by_id(*id).unwrap();
            columns.push(ColumnV2::new_with_ft(
                *id,
                col_info.as_field_type(),
                value.to_owned(),
            ));
        }
        let mut val_buf = Vec::new();
        if with_checksum {
            val_buf
                .write_row_with_checksum(&mut EvalContext::default(), columns, extra_checksum)
                .unwrap();
        } else {
            val_buf
                .write_row(&mut EvalContext::default(), columns)
                .unwrap();
        }
        let mut kvs = vec![(key, val_buf)];
        self.prepare_index_kv(&handle, &mut kvs);
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
        self.execute_with_ctx(Context::default(), id, row)
    }

    pub fn execute_with_ctx(self, ctx: Context, id: i64, row: Vec<Datum>) {
        let values: HashMap<_, _> = self
            .table
            .columns
            .iter()
            .map(|(_, col)| col.id)
            .zip(row)
            .collect();
        let key = table::encode_row_key(self.table.id, id);
        let mut keys = vec![key];
        for (&idx_id, idx_cols) in &self.table.idxs {
            let mut v: Vec<_> = idx_cols.iter().map(|id| values[id].clone()).collect();
            v.push(Datum::I64(id));
            let encoded = datum::encode_key(&mut EvalContext::default(), &v).unwrap();
            let idx_key = table::encode_index_seek_key(self.table.id, idx_id, &encoded);
            keys.push(idx_key);
        }
        self.store.delete(ctx, keys);
    }
}

/// A store that operates over MVCC and support transactions.
pub struct Store<E: Engine> {
    store: SyncTestStorageApiV1<E>,
    current_ts: TimeStamp,
    last_committed_ts: TimeStamp,
    handles: Vec<Vec<u8>>,
}

impl Store<RocksEngine> {
    pub fn new() -> Self {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .build()
            .unwrap();
        Self::from_storage(storage)
    }
}

impl Default for Store<RocksEngine> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: Engine> Store<E> {
    pub fn from_storage(storage: StorageApiV1<E, MockLockManager>) -> Self {
        Self {
            store: SyncTestStorageApiV1::from_storage(0, storage, GcConfig::default()).unwrap(),
            current_ts: 1.into(),
            last_committed_ts: TimeStamp::zero(),
            handles: vec![],
        }
    }

    pub fn current_ts(&self) -> TimeStamp {
        self.current_ts
    }

    pub fn begin(&mut self) {
        self.current_ts = (next_id() as u64).into();
        self.handles.clear();
    }

    pub fn put(&mut self, ctx: Context, mut kv: Vec<(Vec<u8>, Vec<u8>)>) {
        self.handles.extend(kv.iter().map(|(k, _)| k.clone()));
        let pk = kv[0].0.clone();
        let kv = kv
            .drain(..)
            .map(|(k, v)| Mutation::make_put(Key::from_raw(&k), v))
            .collect();
        self.store.prewrite(ctx, kv, pk, self.current_ts).unwrap();
    }

    pub fn insert_into<'a>(&'a mut self, table: &'a Table) -> Insert<'a, E> {
        Insert::new(self, table)
    }

    pub fn delete(&mut self, ctx: Context, mut keys: Vec<Vec<u8>>) {
        self.handles.extend(keys.clone());
        let pk = keys[0].clone();
        let mutations = keys
            .drain(..)
            .map(|k| Mutation::make_delete(Key::from_raw(&k)))
            .collect();
        self.store
            .prewrite(ctx, mutations, pk, self.current_ts)
            .unwrap();
    }

    pub fn delete_from<'a>(&'a mut self, table: &'a Table) -> Delete<'a, E> {
        Delete::new(self, table)
    }

    pub fn commit_with_ctx(&mut self, ctx: Context) {
        let commit_ts = (next_id() as u64).into();
        let handles: Vec<_> = self.handles.drain(..).map(|x| Key::from_raw(&x)).collect();
        if !handles.is_empty() {
            self.store
                .commit(ctx, handles, self.current_ts, commit_ts)
                .unwrap();
            self.last_committed_ts = commit_ts;
        }
    }

    pub fn commit(&mut self) {
        self.commit_with_ctx(Context::default());
    }

    pub fn get_engine(&self) -> E {
        self.store.get_engine()
    }

    pub fn get_storage(&self) -> SyncTestStorageApiV1<E> {
        self.store.clone()
    }

    /// Strip off committed MVCC information to get a final data view.
    ///
    /// Notice: Only first 100000 records can be retrieved.
    // TODO: Support unlimited records once #3773 is resolved.
    pub fn export(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.store
            .scan(
                Context::default(),
                Key::from_encoded(vec![]),
                None,
                100_000,
                false,
                self.last_committed_ts,
            )
            .unwrap()
            .into_iter()
            .flatten()
            .collect()
    }

    /// Directly creates a `SnapshotStore` over current committed data.
    pub fn to_snapshot_store(&self) -> SnapshotStore<E::Snap> {
        let snapshot = self.get_engine().snapshot(Default::default()).unwrap();
        SnapshotStore::new(
            snapshot,
            self.last_committed_ts,
            IsolationLevel::Si,
            true,
            Default::default(),
            Default::default(),
            false,
        )
    }

    /// Strip off committed MVCC information to create a `FixtureStore`.
    pub fn to_fixture_store(&self) -> FixtureStore {
        let data = self
            .export()
            .into_iter()
            .map(|(key, value)| (Key::from_raw(&key), Ok(value)))
            .collect();
        FixtureStore::new(data)
    }

    pub fn insert_all_null_row(
        &mut self,
        tbl: &Table,
        ctx: Context,
        with_checksum: bool,
        extra_checksum: Option<u32>,
    ) {
        self.begin();
        let inserts = self
            .insert_into(tbl)
            .set(&tbl["id"], Datum::Null)
            .set(&tbl["name"], Datum::Null)
            .set(&tbl["count"], Datum::Null)
            .set_v2(&tbl["id"], ScalarValue::Int(None))
            .set_v2(&tbl["name"], ScalarValue::Bytes(None))
            .set_v2(&tbl["count"], ScalarValue::Int(None));
        inserts.execute_with_v2_checksum(ctx, with_checksum, extra_checksum);
        self.commit();
    }
}

/// A trait for a general implementation to convert to a Txn store.
pub trait ToTxnStore<S: tikv::storage::Store> {
    /// Converts to a specific Txn Store.
    fn to_store(&self) -> S;
}

impl<E: Engine, S: tikv::storage::Store> ToTxnStore<S> for Store<E> {
    default fn to_store(&self) -> S {
        unimplemented!()
    }
}

impl<E: Engine> ToTxnStore<SnapshotStore<E::Snap>> for Store<E> {
    fn to_store(&self) -> SnapshotStore<<E as Engine>::Snap> {
        self.to_snapshot_store()
    }
}

impl<E: Engine> ToTxnStore<FixtureStore> for Store<E> {
    fn to_store(&self) -> FixtureStore {
        self.to_fixture_store()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_export() {
        let mut store = Store::new();
        store.begin();
        store.put(
            Context::default(),
            vec![(b"key1".to_vec(), b"value1".to_vec())],
        );
        store.put(
            Context::default(),
            vec![(b"key2".to_vec(), b"foo".to_vec())],
        );
        store.delete(Context::default(), vec![b"key0".to_vec()]);
        store.commit();

        assert_eq!(
            store.export(),
            vec![
                (b"key1".to_vec(), b"value1".to_vec()),
                (b"key2".to_vec(), b"foo".to_vec()),
            ]
        );

        store.begin();
        store.put(
            Context::default(),
            vec![(b"key1".to_vec(), b"value2".to_vec())],
        );
        store.put(
            Context::default(),
            vec![(b"key2".to_vec(), b"foo".to_vec())],
        );
        store.delete(Context::default(), vec![b"key0".to_vec()]);
        store.commit();

        assert_eq!(
            store.export(),
            vec![
                (b"key1".to_vec(), b"value2".to_vec()),
                (b"key2".to_vec(), b"foo".to_vec()),
            ]
        );

        store.begin();
        store.delete(Context::default(), vec![b"key0".to_vec(), b"key2".to_vec()]);
        store.commit();

        assert_eq!(store.export(), vec![(b"key1".to_vec(), b"value2".to_vec())]);

        store.begin();
        store.delete(Context::default(), vec![b"key1".to_vec()]);
        store.commit();

        assert_eq!(store.export(), vec![]);

        store.begin();
        store.put(
            Context::default(),
            vec![(b"key2".to_vec(), b"bar".to_vec())],
        );
        store.put(
            Context::default(),
            vec![(b"key1".to_vec(), b"foo".to_vec())],
        );
        store.put(Context::default(), vec![(b"k".to_vec(), b"box".to_vec())]);
        store.commit();

        assert_eq!(
            store.export(),
            vec![
                (b"k".to_vec(), b"box".to_vec()),
                (b"key1".to_vec(), b"foo".to_vec()),
                (b"key2".to_vec(), b"bar".to_vec()),
            ]
        );

        store.begin();
        store.delete(Context::default(), vec![b"key1".to_vec(), b"key1".to_vec()]);
        store.commit();

        assert_eq!(
            store.export(),
            vec![
                (b"k".to_vec(), b"box".to_vec()),
                (b"key2".to_vec(), b"bar".to_vec()),
            ]
        );

        store.begin();
        store.delete(Context::default(), vec![b"key2".to_vec()]);

        assert_eq!(
            store.export(),
            vec![
                (b"k".to_vec(), b"box".to_vec()),
                (b"key2".to_vec(), b"bar".to_vec()),
            ]
        );

        store.commit();

        assert_eq!(store.export(), vec![(b"k".to_vec(), b"box".to_vec())]);

        store.begin();
        store.put(Context::default(), vec![(b"key1".to_vec(), b"v1".to_vec())]);
        store.put(Context::default(), vec![(b"key2".to_vec(), b"v2".to_vec())]);

        assert_eq!(store.export(), vec![(b"k".to_vec(), b"box".to_vec())]);

        store.commit();

        assert_eq!(
            store.export(),
            vec![
                (b"k".to_vec(), b"box".to_vec()),
                (b"key1".to_vec(), b"v1".to_vec()),
                (b"key2".to_vec(), b"v2".to_vec()),
            ]
        );
    }
}
