// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use concurrency_manager::ConcurrencyManager;
use kvproto::kvrpcpb::Context;
use resource_metering::ResourceTagFactory;
use tidb_query_datatype::codec::{row::v2::CODEC_VERSION, Datum};
use tikv::{
    config::CoprReadPoolConfig,
    coprocessor::{readpool_impl, Endpoint},
    read_pool::ReadPool,
    server::Config,
    storage::{
        kv::RocksEngine, lock_manager::MockLockManager, Engine, TestEngineBuilder,
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
) -> (Store<E>, Endpoint<E>, Arc<QuotaLimiter>) {
    init_data_with_details(ctx, engine, tbl, vals, commit, &Config::default())
}

pub fn init_data_with_engine_and_commit_v2_checksum<E: Engine>(
    ctx: Context,
    engine: E,
    tbl: &ProductTable,
    vals: &[(i64, Option<&str>, i64)],
    commit: bool,
    with_checksum: bool,
    extra_checksum: Option<u32>,
) -> (Store<E>, Endpoint<E>, Arc<QuotaLimiter>) {
    init_data_with_details_v2_checksum(
        ctx,
        engine,
        tbl,
        vals,
        commit,
        &Config::default(),
        with_checksum,
        extra_checksum,
    )
}

pub fn init_data_with_details<E: Engine>(
    ctx: Context,
    engine: E,
    tbl: &ProductTable,
    vals: &[(i64, Option<&str>, i64)],
    commit: bool,
    cfg: &Config,
) -> (Store<E>, Endpoint<E>, Arc<QuotaLimiter>) {
    init_data_with_details_impl(ctx, engine, tbl, vals, commit, cfg, 0, false, None)
}

pub fn init_data_with_details_v2_checksum<E: Engine>(
    ctx: Context,
    engine: E,
    tbl: &ProductTable,
    vals: &[(i64, Option<&str>, i64)],
    commit: bool,
    cfg: &Config,
    with_checksum: bool,
    extra_checksum: Option<u32>,
) -> (Store<E>, Endpoint<E>, Arc<QuotaLimiter>) {
    init_data_with_details_impl(
        ctx,
        engine,
        tbl,
        vals,
        commit,
        cfg,
        CODEC_VERSION,
        with_checksum,
        extra_checksum,
    )
}

fn init_data_with_details_impl<E: Engine>(
    ctx: Context,
    engine: E,
    tbl: &ProductTable,
    vals: &[(i64, Option<&str>, i64)],
    commit: bool,
    cfg: &Config,
    codec_ver: u8,
    with_checksum: bool,
    extra_checksum: Option<u32>,
) -> (Store<E>, Endpoint<E>, Arc<QuotaLimiter>) {
    let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, MockLockManager::new())
        .build()
        .unwrap();
    let mut store = Store::from_storage(storage);

    store.begin();
    for &(id, name, count) in vals {
        let mut inserts = store
            .insert_into(tbl)
            .set(&tbl["id"], Datum::I64(id))
            .set(&tbl["name"], name.map(str::as_bytes).into())
            .set(&tbl["count"], Datum::I64(count));
        if codec_ver == CODEC_VERSION {
            inserts = inserts
                .set_v2(&tbl["id"], id.into())
                .set_v2(&tbl["name"], name.unwrap().into())
                .set_v2(&tbl["count"], count.into());
            inserts.execute_with_v2_checksum(ctx.clone(), with_checksum, extra_checksum);
        } else {
            inserts.execute_with_ctx(ctx.clone());
        }
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
    let limiter = Arc::new(QuotaLimiter::default());
    let copr = Endpoint::new(
        cfg,
        pool.handle(),
        cm,
        ResourceTagFactory::new_for_test(),
        limiter.clone(),
        None,
    );
    (store, copr, limiter)
}

pub fn init_data_with_commit(
    tbl: &ProductTable,
    vals: &[(i64, Option<&str>, i64)],
    commit: bool,
) -> (Store<RocksEngine>, Endpoint<RocksEngine>, Arc<QuotaLimiter>) {
    let engine = TestEngineBuilder::new().build().unwrap();
    init_data_with_engine_and_commit(Context::default(), engine, tbl, vals, commit)
}

// This function will create a Product table and initialize with the specified
// data.
pub fn init_with_data(
    tbl: &ProductTable,
    vals: &[(i64, Option<&str>, i64)],
) -> (Store<RocksEngine>, Endpoint<RocksEngine>) {
    let (store, endpoint, _) = init_data_with_commit(tbl, vals, true);
    (store, endpoint)
}

// Same as init_with_data except returned values include Arc<QuotaLimiter>
pub fn init_with_data_ext(
    tbl: &ProductTable,
    vals: &[(i64, Option<&str>, i64)],
) -> (Store<RocksEngine>, Endpoint<RocksEngine>, Arc<QuotaLimiter>) {
    init_data_with_commit(tbl, vals, true)
}

pub fn init_data_with_commit_v2_checksum(
    tbl: &ProductTable,
    vals: &[(i64, Option<&str>, i64)],
    with_checksum: bool,
    extra_checksum: Option<u32>,
) -> (Store<RocksEngine>, Endpoint<RocksEngine>) {
    let engine = TestEngineBuilder::new().build().unwrap();
    let (store, endpoint, _) = init_data_with_engine_and_commit_v2_checksum(
        Context::default(),
        engine,
        tbl,
        vals,
        true,
        with_checksum,
        extra_checksum,
    );
    (store, endpoint)
}
