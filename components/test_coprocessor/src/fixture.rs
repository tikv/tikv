// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use concurrency_manager::ConcurrencyManager;
use kvproto::kvrpcpb::Context;
use resource_metering::ResourceTagFactory;
use tidb_query_datatype::codec::Datum;
use tikv::{
    config::CoprReadPoolConfig,
    coprocessor::{readpool_impl, Endpoint},
    read_pool::ReadPool,
    server::Config,
    storage::{
        kv::RocksEngine, lock_manager::DummyLockManager, Engine, TestEngineBuilder,
        TestStorageBuilderApiV1,
    },
};
use tikv_util::{quota_limiter::QuotaLimiter, thread_group::GroupProperties};

use super::*;

#[derive(Clone)]
pub struct ProductTable(Table);

impl ProductTable {
    pub fn new() -> ProductTable {
        let id = ColumnBuilder::new()
            .col_type(TYPE_LONG)
            .primary_key(true)
            .build();
        let idx_id = next_id();
        let name = ColumnBuilder::new()
            .col_type(TYPE_VAR_CHAR)
            .index_key(idx_id)
            .build();
        let count = ColumnBuilder::new()
            .col_type(TYPE_LONG)
            .index_key(idx_id)
            .build();
        let table = TableBuilder::new()
            .add_col("id", id)
            .add_col("name", name)
            .add_col("count", count)
            .build();
        ProductTable(table)
    }
}

impl Default for ProductTable {
    fn default() -> Self {
        Self::new()
    }
}

impl std::ops::Deref for ProductTable {
    type Target = Table;

    fn deref(&self) -> &Table {
        &self.0
    }
}

pub fn init_data_with_engine_and_commit<E: Engine>(
    ctx: Context,
    engine: E,
    tbl: &ProductTable,
    vals: &[(i64, Option<&str>, i64)],
    commit: bool,
) -> (Store<E>, Endpoint<E>) {
    init_data_with_details(ctx, engine, tbl, vals, commit, &Config::default())
}

pub fn init_data_with_details<E: Engine>(
    ctx: Context,
    engine: E,
    tbl: &ProductTable,
    vals: &[(i64, Option<&str>, i64)],
    commit: bool,
    cfg: &Config,
) -> (Store<E>, Endpoint<E>) {
    let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, DummyLockManager)
        .build()
        .unwrap();
    let mut store = Store::from_storage(storage);

    store.begin();
    for &(id, name, count) in vals {
        store
            .insert_into(tbl)
            .set(&tbl["id"], Datum::I64(id))
            .set(&tbl["name"], name.map(str::as_bytes).into())
            .set(&tbl["count"], Datum::I64(count))
            .execute_with_ctx(ctx.clone());
    }
    if commit {
        store.commit_with_ctx(ctx);
    }

    tikv_util::thread_group::set_properties(Some(GroupProperties::default()));
    let pool = ReadPool::from(readpool_impl::build_read_pool_for_test(
        &CoprReadPoolConfig::default_for_test(),
        store.get_engine(),
    ));
    let cm = ConcurrencyManager::new(1.into());
    let copr = Endpoint::new(
        cfg,
        pool.handle(),
        cm,
        ResourceTagFactory::new_for_test(),
        Arc::new(QuotaLimiter::default()),
    );
    (store, copr)
}

pub fn init_data_with_commit(
    tbl: &ProductTable,
    vals: &[(i64, Option<&str>, i64)],
    commit: bool,
) -> (Store<RocksEngine>, Endpoint<RocksEngine>) {
    let engine = TestEngineBuilder::new().build().unwrap();
    init_data_with_engine_and_commit(Context::default(), engine, tbl, vals, commit)
}

// This function will create a Product table and initialize with the specified data.
pub fn init_with_data(
    tbl: &ProductTable,
    vals: &[(i64, Option<&str>, i64)],
) -> (Store<RocksEngine>, Endpoint<RocksEngine>) {
    init_data_with_commit(tbl, vals, true)
}
