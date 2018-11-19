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

use kvproto::kvrpcpb::Context;

use tikv::coprocessor::codec::Datum;
use tikv::coprocessor::{Endpoint, ReadPoolContext};
use tikv::server::readpool::{self, ReadPool};
use tikv::server::Config;
use tikv::storage::engine::RocksEngine;
use tikv::storage::{Engine, TestEngineBuilder};
use tikv::util::worker::FutureWorker;

/// An example table for test purpose.
#[derive(Clone)]
pub struct ProductTable {
    pub id: Column,
    pub name: Column,
    pub count: Column,
    pub table: Table,
}

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
            .add_col(id.clone())
            .add_col(name.clone())
            .add_col(count.clone())
            .build();

        ProductTable {
            id,
            name,
            count,
            table,
        }
    }
}

pub fn init_data_with_engine_and_commit<E: Engine>(
    ctx: Context,
    engine: E,
    tbl: &ProductTable,
    vals: &[(i64, Option<&str>, i64)],
    commit: bool,
) -> (Store<E>, Endpoint<E>) {
    init_data_with_details(
        ctx,
        engine,
        tbl,
        vals,
        commit,
        &Config::default(),
        &readpool::Config::default_for_test(),
    )
}

pub fn init_data_with_details<E: Engine>(
    ctx: Context,
    engine: E,
    tbl: &ProductTable,
    vals: &[(i64, Option<&str>, i64)],
    commit: bool,
    cfg: &Config,
    read_pool_cfg: &readpool::Config,
) -> (Store<E>, Endpoint<E>) {
    let mut store = Store::from_engine(engine);

    store.begin();
    for &(id, name, count) in vals {
        store
            .insert_into(&tbl.table)
            .set(&tbl.id, Datum::I64(id))
            .set(&tbl.name, name.map(|s| s.as_bytes()).into())
            .set(&tbl.count, Datum::I64(count))
            .execute_with_ctx(ctx.clone());
    }
    if commit {
        store.commit_with_ctx(ctx);
    }
    let pd_worker = FutureWorker::new("test-pd-worker");
    let pool = ReadPool::new("readpool", read_pool_cfg, || {
        || ReadPoolContext::new(pd_worker.scheduler())
    });
    let cop = Endpoint::new(cfg, store.get_engine(), pool);
    (store, cop)
}

pub fn init_data_with_commit(
    tbl: &ProductTable,
    vals: &[(i64, Option<&str>, i64)],
    commit: bool,
) -> (Store<RocksEngine>, Endpoint<RocksEngine>) {
    let engine = TestEngineBuilder::new().build().unwrap();
    init_data_with_engine_and_commit(Context::new(), engine, tbl, vals, commit)
}

// This function will create a Product table and initialize with the specified data.
pub fn init_with_data(
    tbl: &ProductTable,
    vals: &[(i64, Option<&str>, i64)],
) -> (Store<RocksEngine>, Endpoint<RocksEngine>) {
    init_data_with_commit(tbl, vals, true)
}
