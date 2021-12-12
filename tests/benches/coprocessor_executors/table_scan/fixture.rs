// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use test_coprocessor::*;
use tikv::storage::RocksEngine;

/// Builds a fixture table, which contains two columns: id, foo.
pub fn table_with_2_columns(rows: usize) -> (Table, Store<RocksEngine>) {
    let id = ColumnBuilder::new()
        .col_type(TYPE_LONG)
        .primary_key(true)
        .build();
    let foo = ColumnBuilder::new().col_type(TYPE_LONG).build();
    let table = TableBuilder::new()
        .add_col("id", id)
        .add_col("foo", foo)
        .build();

    let store = crate::util::FixtureBuilder::new(rows)
        .push_column_i64_0_n()
        .push_column_i64_0_n()
        .build_store(&table, &["id", "foo"]);

    (table, store)
}

/// Builds a fixture table, which contains specified number of columns: col0, col1, col2, ...
pub fn table_with_multi_columns(rows: usize, columns: usize) -> (Table, Store<RocksEngine>) {
    let mut table = TableBuilder::new();
    for idx in 0..columns {
        let col = ColumnBuilder::new().col_type(TYPE_LONG).build();
        table = table.add_col(format!("col{}", idx), col);
    }
    let table = table.build();

    let mut fb = crate::util::FixtureBuilder::new(rows);
    let mut col_names = vec![];
    for idx in 0..columns {
        fb = fb.push_column_i64_random();
        col_names.push(format!("col{}", idx));
    }
    let col_names: Vec<_> = col_names.iter().map(|s| s.as_str()).collect();
    let store = fb.build_store(&table, col_names.as_slice());

    (table, store)
}

/// Builds a fixture table, which contains specified number of columns: col0, col1, col2, ...,
/// but the first column does not present in data.
pub fn table_with_missing_column(rows: usize, columns: usize) -> (Table, Store<RocksEngine>) {
    let mut table = TableBuilder::new();
    for idx in 0..columns {
        let col = ColumnBuilder::new().col_type(TYPE_LONG).build();
        table = table.add_col(format!("col{}", idx), col);
    }
    let table = table.build();

    // Starting from col1, so that col0 is missing in the row.
    let mut fb = crate::util::FixtureBuilder::new(rows);
    let mut col_names = vec![];
    for idx in 1..columns {
        fb = fb.push_column_i64_random();
        col_names.push(format!("col{}", idx));
    }
    let col_names: Vec<_> = col_names.iter().map(|s| s.as_str()).collect();
    let store = fb.build_store(&table, col_names.as_slice());

    (table, store)
}

/// Builds a fixture table, which contains three columns, id, foo, bar. Column bar is very long.
pub fn table_with_long_column(rows: usize) -> (Table, Store<RocksEngine>) {
    let id = ColumnBuilder::new()
        .col_type(TYPE_LONG)
        .primary_key(true)
        .build();
    let foo = ColumnBuilder::new().col_type(TYPE_LONG).build();
    let bar = ColumnBuilder::new().col_type(TYPE_VAR_CHAR).build();
    let table = TableBuilder::new()
        .add_col("id", id)
        .add_col("foo", foo)
        .add_col("bar", bar)
        .build();

    let store = crate::util::FixtureBuilder::new(rows)
        .push_column_i64_0_n()
        .push_column_i64_random()
        .push_column_bytes_random_fixed_len(200)
        .build_store(&table, &["id", "foo", "bar"]);

    (table, store)
}
