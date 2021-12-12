// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use test_coprocessor::*;
use tikv::storage::RocksEngine;

pub fn table_with_int_column_two_groups(rows: usize) -> (Table, Store<RocksEngine>) {
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
        .push_column_i64_sampled(&[0x123456, 0xCCCC])
        .build_store(&table, &["id", "foo"]);

    (table, store)
}

pub fn table_with_int_column_two_groups_ordered(rows: usize) -> (Table, Store<RocksEngine>) {
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
        .push_column_i64_ordered(&[0x123456, 0xCCCC])
        .build_store(&table, &["id", "foo"]);

    (table, store)
}

pub fn table_with_int_column_n_groups(rows: usize) -> (Table, Store<RocksEngine>) {
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

pub fn table_with_3_int_columns_random(rows: usize) -> (Table, Store<RocksEngine>) {
    let id = ColumnBuilder::new()
        .col_type(TYPE_LONG)
        .primary_key(true)
        .build();
    let table = TableBuilder::new()
        .add_col("id", id)
        .add_col("col1", ColumnBuilder::new().col_type(TYPE_LONG).build())
        .add_col("col2", ColumnBuilder::new().col_type(TYPE_LONG).build())
        .build();

    let store = crate::util::FixtureBuilder::new(rows)
        .push_column_i64_0_n()
        .push_column_i64_random()
        .push_column_i64_random()
        .build_store(&table, &["id", "col1", "col2"]);

    (table, store)
}
