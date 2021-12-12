// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use test_coprocessor::*;
use tikv::storage::RocksEngine;

/// Builds a fixture table, which contains two columns: id, foo and there is an index over
/// `foo` column.
pub fn table_with_2_columns_and_one_index(rows: usize) -> (i64, Table, Store<RocksEngine>) {
    let index_id = next_id();
    let id = ColumnBuilder::new()
        .col_type(TYPE_LONG)
        .primary_key(true)
        .build();
    let foo = ColumnBuilder::new()
        .col_type(TYPE_LONG)
        .index_key(index_id)
        .build();
    let table = TableBuilder::new()
        .add_col("id", id)
        .add_col("foo", foo)
        .build();

    let store = crate::util::FixtureBuilder::new(rows)
        .push_column_i64_0_n()
        .push_column_i64_random()
        .build_store(&table, &["id", "foo"]);

    (index_id, table, store)
}
