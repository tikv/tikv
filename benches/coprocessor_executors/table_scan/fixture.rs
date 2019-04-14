// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use test_coprocessor::*;
use tikv::coprocessor::codec::Datum;
use tikv::storage::RocksEngine;

/// Builds a fixture table, which contains two columns: id, foo.
pub fn table_with_two_columns() -> (Table, Store<RocksEngine>) {
    let id = ColumnBuilder::new()
        .col_type(TYPE_LONG)
        .primary_key(true)
        .build();
    let foo = ColumnBuilder::new().col_type(TYPE_LONG).build();
    let table = TableBuilder::new()
        .add_col("id", id)
        .add_col("foo", foo)
        .build();

    let mut store = Store::new();
    for i in 0..=1024 {
        store.begin();
        store
            .insert_into(&table)
            .set(&table["id"], Datum::I64(i))
            .set(&table["foo"], Datum::I64(0xDEADBEEF))
            .execute();
        store.commit();
    }

    (table, store)
}

/// Builds a fixture table, which contains specified number of columns: col0, col1, col2, ...
pub fn table_with_multi_columns(number_of_columns: usize) -> (Table, Store<RocksEngine>) {
    let mut table = TableBuilder::new();
    for idx in 0..number_of_columns {
        let col = ColumnBuilder::new().col_type(TYPE_LONG).build();
        table = table.add_col(format!("col{}", idx), col);
    }
    let table = table.build();

    let mut store = Store::new();
    for i in 0..=1024 {
        store.begin();
        {
            let mut insert = store.insert_into(&table);
            for idx in 0..number_of_columns {
                insert = insert.set(&table[format!("col{}", idx)], Datum::I64((i ^ idx) as i64));
            }
            insert.execute();
        }
        store.commit();
    }

    (table, store)
}

/// Builds a fixture table, which contains specified number of columns: col0, col1, col2, ...,
/// but the first column does not present in data.
pub fn table_with_missing_column(number_of_columns: usize) -> (Table, Store<RocksEngine>) {
    let mut table = TableBuilder::new();
    for idx in 0..number_of_columns {
        let col = ColumnBuilder::new().col_type(TYPE_LONG).build();
        table = table.add_col(format!("col{}", idx), col);
    }
    let table = table.build();

    let mut store = Store::new();
    for i in 0..=1024 {
        store.begin();
        {
            let mut insert = store.insert_into(&table);
            // Starting from col1, so that col0 is missing in the row.
            for idx in 1..number_of_columns {
                insert = insert.set(&table[format!("col{}", idx)], Datum::I64((i ^ idx) as i64));
            }
            insert.execute();
        }
        store.commit();
    }

    (table, store)
}

/// Builds a fixture table, which contains three columns, id, foo, bar. Column bar is very long.
pub fn table_with_long_column() -> (Table, Store<RocksEngine>) {
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

    let mut store = Store::new();
    for i in 0..=1024 {
        store.begin();
        store
            .insert_into(&table)
            .set(&table["id"], Datum::I64(i))
            .set(&table["foo"], Datum::I64(0xDEADBEEF))
            .set(&table["bar"], Datum::Bytes([0xCC].repeat(200)))
            .execute();
        store.commit();
    }

    (table, store)
}
