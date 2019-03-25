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

#![feature(repeat_generic_slice)]

#[macro_use]
extern crate criterion;

use criterion::{black_box, Bencher, Criterion};
use protobuf::RepeatedField;

use kvproto::coprocessor::KeyRange;
use tipb::executor::{Executor as PbExecutor, IndexScan, Selection, TableScan};
use tipb::expression::{Expr, ExprType, ScalarFuncSig};

use test_coprocessor::*;
use tikv::coprocessor::codec::Datum;
use tikv::coprocessor::dag::batch_executor::interface::*;
use tikv::coprocessor::dag::batch_executor::statistics::ExecSummaryCollectorDisabled;
use tikv::coprocessor::dag::executor::Executor;
use tikv::storage::RocksEngine;

fn bench_table_scan_next(
    b: &mut Bencher,
    meta: &TableScan,
    ranges: &[KeyRange],
    store: &Store<RocksEngine>,
) {
    use tikv::coprocessor::dag::executor::TableScanExecutor;

    b.iter_with_setup(
        || {
            let mut executor = TableScanExecutor::table_scan(
                meta.clone(),
                ranges.to_vec(),
                store.to_fixture_store(),
                false,
            )
            .unwrap();
            // There is a step of building scanner in the first `next()` which cost time,
            // so we next() before hand.
            executor.next().unwrap().unwrap();
            executor
        },
        |mut executor| {
            black_box(black_box(&mut executor).next().unwrap().unwrap());
        },
    );
}

fn bench_table_scan_next_1000(
    b: &mut Bencher,
    meta: &TableScan,
    ranges: &[KeyRange],
    store: &Store<RocksEngine>,
) {
    use tikv::coprocessor::dag::executor::TableScanExecutor;

    b.iter_with_setup(
        || {
            let mut executor = TableScanExecutor::table_scan(
                meta.clone(),
                ranges.to_vec(),
                store.to_fixture_store(),
                false,
            )
            .unwrap();
            // There is a step of building scanner in the first `next()` which cost time,
            // so we next() before hand.
            executor.next().unwrap().unwrap();
            executor
        },
        |mut executor| {
            let executor = black_box(&mut executor);
            for _ in 0..1000 {
                black_box(executor.next().unwrap().unwrap());
            }
        },
    );
}

fn bench_table_scan_next_1000_batch(
    b: &mut Bencher,
    columns_info: Vec<tipb::schema::ColumnInfo>,
    ranges: &[KeyRange],
    store: &Store<RocksEngine>,
) {
    use tikv::coprocessor::dag::batch_executor::executors::BatchTableScanExecutor;

    b.iter_with_setup(
        || {
            let mut executor = BatchTableScanExecutor::new(
                ExecSummaryCollectorDisabled,
                store.to_fixture_store(),
                std::sync::Arc::new(tikv::coprocessor::dag::expr::EvalConfig::default()),
                columns_info.clone(),
                ranges.to_vec(),
                false,
            )
            .unwrap();
            // There is a step of building scanner in the first `next()` which cost time,
            // so we next() before hand.
            executor.next_batch(1);
            executor
        },
        |mut executor| {
            let executor = black_box(&mut executor);
            black_box(executor.next_batch(1000));
        },
    );
}

/// next() for 1 time, 1 interested column, which is PK (which is in the key)
///
/// This kind of scanner is used in SQLs like SELECT COUNT(*).
fn bench_table_scan_primary_key(c: &mut Criterion) {
    c.bench_function("table_scan_primary_key", |b| {
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
        for i in 0..10 {
            store.begin();
            store
                .insert_into(&table)
                .set(&table["id"], Datum::I64(i))
                .set(&table["foo"], Datum::I64(0xDEADBEEF))
                .execute();
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(table["id"].as_column_info());

        bench_table_scan_next(b, &meta, &[table.get_record_range_all()], &store);
    });
}

/// next() for 1 time, 1 interested column, at the front of each row. Each row contains 100 columns.
///
/// This kind of scanner is used in SQLs like `SELECT COUNT(column)`.
fn bench_table_scan_datum_front(c: &mut Criterion) {
    const COLUMNS: usize = 100;

    c.bench_function("table_scan_datum_front", |b| {
        let mut table = TableBuilder::new();
        for idx in 0..COLUMNS {
            let col = ColumnBuilder::new().col_type(TYPE_LONG).build();
            table = table.add_col(format!("col{}", idx), col);
        }
        let table = table.build();

        let mut store = Store::new();
        for i in 0..10 {
            store.begin();
            {
                let mut insert = store.insert_into(&table);
                for idx in 0..COLUMNS {
                    insert =
                        insert.set(&table[format!("col{}", idx)], Datum::I64((i ^ idx) as i64));
                }
                insert.execute();
            }
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(table["col0"].as_column_info());

        bench_table_scan_next(b, &meta, &[table.get_record_range_all()], &store);
    });
}

/// next() for 1 time, 2 interested columns, at the front of each row. Each row contains 100
/// columns.
fn bench_table_scan_datum_multi_front(c: &mut Criterion) {
    const COLUMNS: usize = 100;

    c.bench_function("table_scan_datum_multi_front", |b| {
        let mut table = TableBuilder::new();
        for idx in 0..COLUMNS {
            let col = ColumnBuilder::new().col_type(TYPE_LONG).build();
            table = table.add_col(format!("col{}", idx), col);
        }
        let table = table.build();

        let mut store = Store::new();
        for i in 0..10 {
            store.begin();
            {
                let mut insert = store.insert_into(&table);
                for idx in 0..COLUMNS {
                    insert =
                        insert.set(&table[format!("col{}", idx)], Datum::I64((i ^ idx) as i64));
                }
                insert.execute();
            }
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(table["col0"].as_column_info());
        meta.mut_columns().push(table["col1"].as_column_info());

        bench_table_scan_next(b, &meta, &[table.get_record_range_all()], &store);
    });
}

/// next() for 1 time, 1 interested column, at the end of each row. Each row contains 100 columns.
fn bench_table_scan_datum_end(c: &mut Criterion) {
    const COLUMNS: usize = 100;

    c.bench_function("table_scan_datum_end", |b| {
        let mut table = TableBuilder::new();
        for idx in 0..COLUMNS {
            let col = ColumnBuilder::new().col_type(TYPE_LONG).build();
            table = table.add_col(format!("col{}", idx), col);
        }
        let table = table.build();

        let mut store = Store::new();
        for i in 0..10 {
            store.begin();
            {
                let mut insert = store.insert_into(&table);
                for idx in 0..COLUMNS {
                    insert =
                        insert.set(&table[format!("col{}", idx)], Datum::I64((i ^ idx) as i64));
                }
                insert.execute();
            }
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(table["col99"].as_column_info());

        bench_table_scan_next(b, &meta, &[table.get_record_range_all()], &store);
    });
}

/// next() for 1 time, 100 interested columns, each column in the row is interested
/// (i.e. there is totally 100 columns in the row).
///
/// This kind of scanner is used in SQLs like `SELECT *`.
fn bench_table_scan_datum_all(c: &mut Criterion) {
    const COLUMNS: usize = 100;

    c.bench_function("table_scan_datum_all", |b| {
        let mut table = TableBuilder::new();
        for idx in 0..COLUMNS {
            let col = ColumnBuilder::new().col_type(TYPE_LONG).build();
            table = table.add_col(format!("col{}", idx), col);
        }
        let table = table.build();

        let mut store = Store::new();
        for i in 0..10 {
            store.begin();
            {
                let mut insert = store.insert_into(&table);
                for idx in 0..COLUMNS {
                    insert =
                        insert.set(&table[format!("col{}", idx)], Datum::I64((i ^ idx) as i64));
                }
                insert.execute();
            }
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.set_columns(RepeatedField::from_vec(table.columns_info()));

        bench_table_scan_next(b, &meta, &[table.get_record_range_all()], &store);
    });
}

/// next() for 1 time, 3 columns in the row and the last column is very long but only PK is
/// interested.
fn bench_table_scan_long_datum_primary_key(c: &mut Criterion) {
    c.bench_function("table_scan_long_datum_primary_key", |b| {
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
        for i in 0..10 {
            store.begin();
            store
                .insert_into(&table)
                .set(&table["id"], Datum::I64(i))
                .set(&table["foo"], Datum::I64(0xDEADBEEF))
                .set(&table["bar"], Datum::Bytes([0xCC].repeat(200)))
                .execute();
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(table["id"].as_column_info());

        bench_table_scan_next(b, &meta, &[table.get_record_range_all()], &store);
    });
}

/// next() for 1 time, 3 columns in the row and the last column is very long but only PK is
/// interested.
fn bench_table_scan_long_datum_normal(c: &mut Criterion) {
    c.bench_function("table_scan_long_datum_normal", |b| {
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
        for i in 0..10 {
            store.begin();
            store
                .insert_into(&table)
                .set(&table["id"], Datum::I64(i))
                .set(&table["foo"], Datum::I64(0xDEADBEEF))
                .set(&table["bar"], Datum::Bytes([0xCC].repeat(200)))
                .execute();
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(table["foo"].as_column_info());

        bench_table_scan_next(b, &meta, &[table.get_record_range_all()], &store);
    });
}

/// next() for 1 time, 3 columns in the row and the last column is very long but only PK is
/// interested.
fn bench_table_scan_long_datum_long(c: &mut Criterion) {
    c.bench_function("table_scan_long_datum_long", |b| {
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
        for i in 0..10 {
            store.begin();
            store
                .insert_into(&table)
                .set(&table["id"], Datum::I64(i))
                .set(&table["foo"], Datum::I64(0xDEADBEEF))
                .set(&table["bar"], Datum::Bytes([0xCC].repeat(200)))
                .execute();
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(table["bar"].as_column_info());

        bench_table_scan_next(b, &meta, &[table.get_record_range_all()], &store);
    });
}

/// next() for 1 time, 3 columns in the row and the last column is very long but only PK is
/// interested.
fn bench_table_scan_long_datum_all(c: &mut Criterion) {
    c.bench_function("table_scan_long_datum_all", |b| {
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
        for i in 0..10 {
            store.begin();
            store
                .insert_into(&table)
                .set(&table["id"], Datum::I64(i))
                .set(&table["foo"], Datum::I64(0xDEADBEEF))
                .set(&table["bar"], Datum::Bytes([0xCC].repeat(200)))
                .execute();
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(table["id"].as_column_info());
        meta.mut_columns().push(table["foo"].as_column_info());
        meta.mut_columns().push(table["bar"].as_column_info());

        bench_table_scan_next(b, &meta, &[table.get_record_range_all()], &store);
    });
}

/// next() for 1 time, 3 columns in the row and the last column is very long but only PK is
/// interested.
fn bench_table_scan_long_datum_primary_key_multi_rows(c: &mut Criterion) {
    c.bench_function("table_scan_long_datum_primary_key_multi_rows", |b| {
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
        for i in 0..1001 {
            store.begin();
            store
                .insert_into(&table)
                .set(&table["id"], Datum::I64(i))
                .set(&table["foo"], Datum::I64(0xDEADBEEF))
                .set(&table["bar"], Datum::Bytes([0xCC].repeat(200)))
                .execute();
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(table["id"].as_column_info());

        bench_table_scan_next_1000(b, &meta, &[table.get_record_range_all()], &store);
    });
}

/// next() for 1 time, 3 columns in the row and the last column is very long but only PK is
/// interested.
fn bench_table_scan_long_datum_normal_multi_rows(c: &mut Criterion) {
    c.bench_function("table_scan_long_datum_normal_multi_rows", |b| {
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
        for i in 0..1001 {
            store.begin();
            store
                .insert_into(&table)
                .set(&table["id"], Datum::I64(i))
                .set(&table["foo"], Datum::I64(0xDEADBEEF))
                .set(&table["bar"], Datum::Bytes([0xCC].repeat(200)))
                .execute();
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(table["foo"].as_column_info());

        bench_table_scan_next_1000(b, &meta, &[table.get_record_range_all()], &store);
    });
}

/// next() for 1 time, 3 columns in the row and the last column is very long but only PK is
/// interested.
fn bench_table_scan_long_datum_long_multi_rows(c: &mut Criterion) {
    c.bench_function("table_scan_long_datum_long_multi_rows", |b| {
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
        for i in 0..1001 {
            store.begin();
            store
                .insert_into(&table)
                .set(&table["id"], Datum::I64(i))
                .set(&table["foo"], Datum::I64(0xDEADBEEF))
                .set(&table["bar"], Datum::Bytes([0xCC].repeat(200)))
                .execute();
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(table["bar"].as_column_info());

        bench_table_scan_next_1000(b, &meta, &[table.get_record_range_all()], &store);
    });
}

/// next() for 1 time, 3 columns in the row and the last column is very long but only PK is
/// interested.
fn bench_table_scan_long_datum_all_multi_rows(c: &mut Criterion) {
    c.bench_function("table_scan_long_datum_all_multi_rows", |b| {
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
        for i in 0..1001 {
            store.begin();
            store
                .insert_into(&table)
                .set(&table["id"], Datum::I64(i))
                .set(&table["foo"], Datum::I64(0xDEADBEEF))
                .set(&table["bar"], Datum::Bytes([0xCC].repeat(200)))
                .execute();
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(table["id"].as_column_info());
        meta.mut_columns().push(table["foo"].as_column_info());
        meta.mut_columns().push(table["bar"].as_column_info());

        bench_table_scan_next_1000(b, &meta, &[table.get_record_range_all()], &store);
    });
}

/// next() for 1 time, 3 columns in the row and the last column is very long but only PK is
/// interested.
fn bench_batch_table_scan_long_datum_primary_key_multi_rows(c: &mut Criterion) {
    c.bench_function("batch_table_scan_long_datum_primary_key_multi_rows", |b| {
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
        for i in 0..1001 {
            store.begin();
            store
                .insert_into(&table)
                .set(&table["id"], Datum::I64(i))
                .set(&table["foo"], Datum::I64(0xDEADBEEF))
                .set(&table["bar"], Datum::Bytes([0xCC].repeat(200)))
                .execute();
            store.commit();
        }

        let columns_info = vec![table["id"].as_column_info()];
        bench_table_scan_next_1000_batch(b, columns_info, &[table.get_record_range_all()], &store);
    });
}

/// next() for 1 time, 3 columns in the row and the last column is very long but only PK is
/// interested.
fn bench_batch_table_scan_long_datum_normal_multi_rows(c: &mut Criterion) {
    c.bench_function("batch_table_scan_long_datum_normal_multi_rows", |b| {
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
        for i in 0..1001 {
            store.begin();
            store
                .insert_into(&table)
                .set(&table["id"], Datum::I64(i))
                .set(&table["foo"], Datum::I64(0xDEADBEEF))
                .set(&table["bar"], Datum::Bytes([0xCC].repeat(200)))
                .execute();
            store.commit();
        }

        let columns_info = vec![table["foo"].as_column_info()];
        bench_table_scan_next_1000_batch(b, columns_info, &[table.get_record_range_all()], &store);
    });
}

/// next() for 1 time, 3 columns in the row and the last column is very long but only PK is
/// interested.
fn bench_batch_table_scan_long_datum_long_multi_rows(c: &mut Criterion) {
    c.bench_function("batch_table_scan_long_datum_long_multi_rows", |b| {
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
        for i in 0..1001 {
            store.begin();
            store
                .insert_into(&table)
                .set(&table["id"], Datum::I64(i))
                .set(&table["foo"], Datum::I64(0xDEADBEEF))
                .set(&table["bar"], Datum::Bytes([0xCC].repeat(200)))
                .execute();
            store.commit();
        }

        let columns_info = vec![table["bar"].as_column_info()];
        bench_table_scan_next_1000_batch(b, columns_info, &[table.get_record_range_all()], &store);
    });
}

/// next() for 1 time, 3 columns in the row and the last column is very long but only PK is
/// interested.
fn bench_batch_table_scan_long_datum_all_multi_rows(c: &mut Criterion) {
    c.bench_function("batch_table_scan_long_datum_all_multi_rows", |b| {
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
        for i in 0..1001 {
            store.begin();
            store
                .insert_into(&table)
                .set(&table["id"], Datum::I64(i))
                .set(&table["foo"], Datum::I64(0xDEADBEEF))
                .set(&table["bar"], Datum::Bytes([0xCC].repeat(200)))
                .execute();
            store.commit();
        }

        let columns_info = vec![
            table["id"].as_column_info(),
            table["foo"].as_column_info(),
            table["bar"].as_column_info(),
        ];
        bench_table_scan_next_1000_batch(b, columns_info, &[table.get_record_range_all()], &store);
    });
}

/// next() for 1 time, 1 interested column, but the column is missing from each row (i.e. it's
/// default value is used instead). Each row contains totally 10 columns.
fn bench_table_scan_datum_absent(c: &mut Criterion) {
    const COLUMNS: usize = 10;

    c.bench_function("table_scan_datum_absent", |b| {
        let mut table = TableBuilder::new();
        for idx in 0..COLUMNS {
            let col = ColumnBuilder::new().col_type(TYPE_LONG).build();
            table = table.add_col(format!("col{}", idx), col);
        }
        let table = table.build();

        let mut store = Store::new();
        for i in 0..10 {
            store.begin();
            {
                let mut insert = store.insert_into(&table);
                // Starting from col1, so that col0 is missing in the row.
                for idx in 1..COLUMNS {
                    insert =
                        insert.set(&table[format!("col{}", idx)], Datum::I64((i ^ idx) as i64));
                }
                insert.execute();
            }
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(table["col0"].as_column_info());

        bench_table_scan_next(b, &meta, &[table.get_record_range_all()], &store);
    });
}

/// next() for 1 time, 1 interested column, but the column is missing from each row (i.e. it's
/// default value is used instead). Each row contains totally 100 columns.
fn bench_table_scan_datum_absent_large_row(c: &mut Criterion) {
    const COLUMNS: usize = 100;

    c.bench_function("table_scan_datum_absent_large_row", |b| {
        let mut table = TableBuilder::new();
        for idx in 0..COLUMNS {
            let col = ColumnBuilder::new().col_type(TYPE_LONG).build();
            table = table.add_col(format!("col{}", idx), col);
        }
        let table = table.build();

        let mut store = Store::new();
        for i in 0..10 {
            store.begin();
            {
                let mut insert = store.insert_into(&table);
                // Starting from col1, so that col0 is missing in the row.
                for idx in 1..COLUMNS {
                    insert =
                        insert.set(&table[format!("col{}", idx)], Datum::I64((i ^ idx) as i64));
                }
                insert.execute();
            }
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(table["col0"].as_column_info());

        bench_table_scan_next(b, &meta, &[table.get_record_range_all()], &store);
    });
}

/// next() for 1 time, 1 interested column, which is PK. However the range given is a point range.
fn bench_table_scan_point_range(c: &mut Criterion) {
    c.bench_function("table_scan_point_range", |b| {
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
        for i in 0..10 {
            store.begin();
            store
                .insert_into(&table)
                .set(&table["id"], Datum::I64(i))
                .set(&table["foo"], Datum::I64(0xDEADBEEF))
                .execute();
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(table["id"].as_column_info());

        // We pass 2 point-ranges instead of 1 point-range, because there is a warm-up next().
        bench_table_scan_next(
            b,
            &meta,
            &[table.get_record_range_one(0), table.get_record_range_one(1)],
            &store,
        );
    });
}

/// 1 interested column, which is PK. However 1000 point ranges (in ascending order) are given and
/// this case benches the performance when all ranges are consumed.
fn bench_table_scan_multi_point_range(c: &mut Criterion) {
    use tikv::coprocessor::dag::executor::TableScanExecutor;

    c.bench_function("table_scan_multi_point_range", |b| {
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
        for i in 0..1001 {
            store.begin();
            store
                .insert_into(&table)
                .set(&table["id"], Datum::I64(i))
                .set(&table["foo"], Datum::I64(0xDEADBEEF))
                .execute();
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(table["id"].as_column_info());

        b.iter_with_setup(
            || {
                let mut ranges = vec![];
                // Generate 1001 ranges, because there will be a warm-up next().
                for i in 0..1001 {
                    ranges.push(table.get_record_range_one(i));
                }
                let mut executor = TableScanExecutor::table_scan(
                    meta.clone(),
                    ranges,
                    store.to_fixture_store(),
                    false,
                )
                .unwrap();
                // There is a step of building scanner in the first `next()` which cost time,
                // so we next() before hand.
                executor.next().unwrap().unwrap();
                executor
            },
            |mut executor| {
                let executor = black_box(&mut executor);
                for _ in 0..1000 {
                    black_box(executor.next().unwrap().unwrap());
                }
            },
        );
    });
}

/// 1 interested column, which is PK. One range is given, which contains 1000 rows.
///
/// This case benches the performance when all records of this range are consumed.
fn bench_table_scan_multi_rows(c: &mut Criterion) {
    use tikv::coprocessor::dag::executor::TableScanExecutor;

    c.bench_function("table_scan_multi_rows", |b| {
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
        for i in 0..1001 {
            store.begin();
            store
                .insert_into(&table)
                .set(&table["id"], Datum::I64(i))
                .set(&table["foo"], Datum::I64(0xDEADBEEF))
                .execute();
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(table["id"].as_column_info());

        b.iter_with_setup(
            || {
                let mut executor = TableScanExecutor::table_scan(
                    meta.clone(),
                    vec![table.get_record_range_all()],
                    store.to_fixture_store(),
                    false,
                )
                .unwrap();
                // There is a step of building scanner in the first `next()` which cost time,
                // so we next() before hand.
                executor.next().unwrap().unwrap();
                executor
            },
            |mut executor| {
                let executor = black_box(&mut executor);
                for _ in 0..1000 {
                    black_box(executor.next().unwrap().unwrap());
                }
            },
        );
    });
}

/// 100 interested columns, each column in the row is interested. One range is given,
/// which contains 1000 rows.
///
/// This case benches the performance when all records of this range are consumed for SQLs like
/// `SELECT *`.
fn bench_table_scan_datum_all_multi_rows(c: &mut Criterion) {
    use tikv::coprocessor::dag::executor::TableScanExecutor;

    const COLUMNS: usize = 100;

    c.bench_function("table_scan_datum_all_multi_rows", |b| {
        let mut table = TableBuilder::new();
        for idx in 0..COLUMNS {
            let col = ColumnBuilder::new().col_type(TYPE_LONG).build();
            table = table.add_col(format!("col{}", idx), col);
        }
        let table = table.build();

        let mut store = Store::new();
        for i in 0..1001 {
            store.begin();
            {
                let mut insert = store.insert_into(&table);
                for idx in 0..COLUMNS {
                    insert =
                        insert.set(&table[format!("col{}", idx)], Datum::I64((i ^ idx) as i64));
                }
                insert.execute();
            }
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.set_columns(RepeatedField::from_vec(table.columns_info()));

        b.iter_with_setup(
            || {
                let mut executor = TableScanExecutor::table_scan(
                    meta.clone(),
                    vec![table.get_record_range_all()],
                    store.to_fixture_store(),
                    false,
                )
                .unwrap();
                // There is a step of building scanner in the first `next()` which cost time,
                // so we next() before hand.
                executor.next().unwrap().unwrap();
                executor
            },
            |mut executor| {
                let executor = black_box(&mut executor);
                for _ in 0..1000 {
                    black_box(executor.next().unwrap().unwrap());
                }
            },
        );
    });
}

/// 1 interested column, which is PK. One range is given, which contains 1000 rows.
///
/// This case benches the performance when all records of this range are consumed.
fn bench_batch_table_scan_multi_rows(c: &mut Criterion) {
    use tikv::coprocessor::dag::batch_executor::executors::BatchTableScanExecutor;

    c.bench_function("batch_table_scan_multi_rows", |b| {
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
        for i in 0..1001 {
            store.begin();
            store
                .insert_into(&table)
                .set(&table["id"], Datum::I64(i))
                .set(&table["foo"], Datum::I64(0xDEADBEEF))
                .execute();
            store.commit();
        }

        let columns_info = vec![table["id"].as_column_info()];
        b.iter_with_setup(
            || {
                let mut executor = BatchTableScanExecutor::new(
                    ExecSummaryCollectorDisabled,
                    store.to_fixture_store(),
                    std::sync::Arc::new(tikv::coprocessor::dag::expr::EvalConfig::default()),
                    columns_info.clone(),
                    vec![table.get_record_range_all()],
                    false,
                )
                .unwrap();
                // There is a step of building scanner in the first `next()` which cost time,
                // so we next() before hand.
                executor.next_batch(1);
                executor
            },
            |mut executor| {
                let executor = black_box(&mut executor);
                black_box(executor.next_batch(1000));
            },
        );
    });
}

/// 100 interested columns, each column in the row is interested. One range is given,
/// which contains 1000 rows.
///
/// This case benches the performance when all records of this range are consumed for SQLs like
/// `SELECT *`.
fn bench_batch_table_scan_datum_all_multi_rows(c: &mut Criterion) {
    use tikv::coprocessor::dag::batch_executor::executors::BatchTableScanExecutor;

    const COLUMNS: usize = 100;

    c.bench_function("batch_table_scan_datum_all_multi_rows", |b| {
        let mut table = TableBuilder::new();
        for idx in 0..COLUMNS {
            let col = ColumnBuilder::new().col_type(TYPE_LONG).build();
            table = table.add_col(format!("col{}", idx), col);
        }
        let table = table.build();

        let mut store = Store::new();
        for i in 0..1001 {
            store.begin();
            {
                let mut insert = store.insert_into(&table);
                for idx in 0..COLUMNS {
                    insert =
                        insert.set(&table[format!("col{}", idx)], Datum::I64((i ^ idx) as i64));
                }
                insert.execute();
            }
            store.commit();
        }

        b.iter_with_setup(
            || {
                let mut executor = BatchTableScanExecutor::new(
                    ExecSummaryCollectorDisabled,
                    store.to_fixture_store(),
                    std::sync::Arc::new(tikv::coprocessor::dag::expr::EvalConfig::default()),
                    table.columns_info(),
                    vec![table.get_record_range_all()],
                    false,
                )
                .unwrap();
                // There is a step of building scanner in the first `next()` which cost time,
                // so we next() before hand.
                executor.next_batch(1);
                executor
            },
            |mut executor| {
                let executor = black_box(&mut executor);
                black_box(executor.next_batch(1000));
            },
        );
    });
}

fn bench_index_scan_next(
    b: &mut Bencher,
    meta: &IndexScan,
    unique: bool,
    ranges: &[KeyRange],
    store: &Store<RocksEngine>,
) {
    use tikv::coprocessor::dag::executor::IndexScanExecutor;

    b.iter_with_setup(
        || {
            let mut executor = IndexScanExecutor::index_scan(
                meta.clone(),
                ranges.to_vec(),
                store.to_fixture_store(),
                unique,
                false,
            )
            .unwrap();
            // There is a step of building scanner in the first `next()` which cost time,
            // so we next() before hand.
            executor.next().unwrap().unwrap();
            executor
        },
        |mut executor| {
            black_box(black_box(&mut executor).next().unwrap().unwrap());
        },
    );
}

/// next() for 1 time, 1 interested column, which is PK (which is in the key).
///
/// This kind of scanner is used in SQLs like `SELECT * FROM .. WHERE index = X`, an index lookup
/// will be performed so that PK is needed.
fn bench_normal_index_scan_primary_key(c: &mut Criterion) {
    c.bench_function("normal_index_scan_primary_key", |b| {
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

        let mut store = Store::new();
        for i in 0..10 {
            store.begin();
            store
                .insert_into(&table)
                .set(&table["id"], Datum::I64(i))
                .set(&table["foo"], Datum::I64(0xDEADBEEF))
                .execute();
            store.commit();
        }

        let mut meta = IndexScan::new();
        meta.set_table_id(table.id);
        meta.set_index_id(index_id);
        meta.mut_columns().push(table["id"].as_column_info());
        meta.set_desc(false);
        meta.set_unique(false);

        bench_index_scan_next(
            b,
            &meta,
            false,
            &[table.get_index_range_all(index_id)],
            &store,
        );
    });
}

/// next() for 1 time, 1 interested column, which is the column of the index itself (which is in
/// the key).
///
/// This kind of scanner is used in SQLs like `SELECT COUNT(*) FROM .. WHERE index = X` or
/// `SELECT index FROM .. WHERE index = X`. There is no double read.
fn bench_normal_index_scan_index(c: &mut Criterion) {
    c.bench_function("normal_index_scan_index", |b| {
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

        let mut store = Store::new();
        for i in 0..10 {
            store.begin();
            store
                .insert_into(&table)
                .set(&table["id"], Datum::I64(i))
                .set(&table["foo"], Datum::I64(0xDEADBEEF))
                .execute();
            store.commit();
        }

        let mut meta = IndexScan::new();
        meta.set_table_id(table.id);
        meta.set_index_id(index_id);
        meta.mut_columns().push(table["foo"].as_column_info());
        meta.set_desc(false);
        meta.set_unique(false);

        bench_index_scan_next(
            b,
            &meta,
            false,
            &[table.get_index_range_all(index_id)],
            &store,
        );
    });
}

fn bench_dag_handle(
    b: &mut Bencher,
    executors: &[PbExecutor],
    ranges: &[KeyRange],
    store: &Store<RocksEngine>,
    enable_batch: bool,
) {
    use tikv::coprocessor::dag::DAGRequestHandler;
    use tikv::coprocessor::Deadline;
    use tipb::select::DAGRequest;

    let mut dag = DAGRequest::new();
    dag.set_executors(RepeatedField::from_vec(executors.to_vec()));

    b.iter_with_setup(
        || {
            DAGRequestHandler::build(
                dag.clone(),
                ranges.to_vec(),
                store.to_fixture_store(),
                Deadline::from_now("", std::time::Duration::from_secs(10)),
                64,
                false,
                enable_batch,
            )
            .unwrap()
        },
        |mut dag| {
            dag.handle_request().unwrap();
        },
    );
}

/// Integrate DAGRequestHandler + TableScan, scans a range with 1000 keys, 1 interested column (PK).
fn bench_dag_table_scan_primary_key(c: &mut Criterion) {
    use tipb::executor::ExecType;

    fn bench(c: &mut Criterion, id: &'static str, batch: bool) {
        c.bench_function(id, move |b| {
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
            for i in 0..1000 {
                store.begin();
                store
                    .insert_into(&table)
                    .set(&table["id"], Datum::I64(i))
                    .set(&table["foo"], Datum::I64(0xDEADBEEF))
                    .execute();
                store.commit();
            }

            let mut meta = TableScan::new();
            meta.set_table_id(table.id);
            meta.set_desc(false);
            meta.mut_columns().push(table["id"].as_column_info());

            let mut exec = PbExecutor::new();
            exec.set_tp(ExecType::TypeTableScan);
            exec.set_tbl_scan(meta);

            bench_dag_handle(b, &[exec], &[table.get_record_range_all()], &store, batch);
        });
    }

    bench(c, "dag_normal_table_scan_primary_key", false);
    bench(c, "dag_batch_table_scan_primary_key", true);
}

/// Integrate DAGRequestHandler + TableScan, scans a range with 1000 keys, 1 interested column, which is
/// at the front in a 100 columns row.
fn bench_dag_table_scan_datum_front(c: &mut Criterion) {
    use tipb::executor::ExecType;

    const COLUMNS: usize = 100;

    fn bench(c: &mut Criterion, id: &'static str, batch: bool) {
        c.bench_function(id, move |b| {
            let mut table = TableBuilder::new();
            for idx in 0..COLUMNS {
                let col = ColumnBuilder::new().col_type(TYPE_LONG).build();
                table = table.add_col(format!("col{}", idx), col);
            }
            let table = table.build();

            let mut store = Store::new();
            for i in 0..1000 {
                store.begin();
                {
                    let mut insert = store.insert_into(&table);
                    for idx in 0..COLUMNS {
                        insert =
                            insert.set(&table[format!("col{}", idx)], Datum::I64((i ^ idx) as i64));
                    }
                    insert.execute();
                }
                store.commit();
            }

            let mut meta = TableScan::new();
            meta.set_table_id(table.id);
            meta.set_desc(false);
            meta.mut_columns().push(table["col0"].as_column_info());

            let mut exec = PbExecutor::new();
            exec.set_tp(ExecType::TypeTableScan);
            exec.set_tbl_scan(meta);

            bench_dag_handle(b, &[exec], &[table.get_record_range_all()], &store, batch);
        });
    }

    bench(c, "dag_normal_table_scan_datum_front", false);
    bench(c, "dag_batch_table_scan_datum_front", true);
}

/// Integrate DAGRequestHandler + TableScan, scans a range with 1000 keys, all column are interested and
/// there are 100 columns in the row.
fn bench_dag_table_scan_datum_all(c: &mut Criterion) {
    use tipb::executor::ExecType;

    const COLUMNS: usize = 100;

    fn bench(c: &mut Criterion, id: &'static str, batch: bool) {
        c.bench_function(id, move |b| {
            let mut table = TableBuilder::new();
            for idx in 0..COLUMNS {
                let col = ColumnBuilder::new().col_type(TYPE_LONG).build();
                table = table.add_col(format!("col{}", idx), col);
            }
            let table = table.build();

            let mut store = Store::new();
            for i in 0..1000 {
                store.begin();
                {
                    let mut insert = store.insert_into(&table);
                    for idx in 0..COLUMNS {
                        insert =
                            insert.set(&table[format!("col{}", idx)], Datum::I64((i ^ idx) as i64));
                    }
                    insert.execute();
                }
                store.commit();
            }

            let mut meta = TableScan::new();
            meta.set_table_id(table.id);
            meta.set_desc(false);
            meta.set_columns(RepeatedField::from_vec(table.columns_info()));

            let mut exec = PbExecutor::new();
            exec.set_tp(ExecType::TypeTableScan);
            exec.set_tbl_scan(meta);

            bench_dag_handle(b, &[exec], &[table.get_record_range_all()], &store, batch);
        });
    }

    bench(c, "dag_normal_table_scan_datum_all", false);
    bench(c, "dag_batch_table_scan_datum_all", true);
}

/// Integrate DAGRequestHandler + TableScan, scans a range with 1000 keys, 2 interested column: PK & front
/// column.
fn bench_dag_table_scan_pk_front(c: &mut Criterion) {
    use tipb::executor::ExecType;

    fn bench(c: &mut Criterion, id: &'static str, batch: bool) {
        c.bench_function(id, move |b| {
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
            for i in 0..1000 {
                store.begin();
                store
                    .insert_into(&table)
                    .set(&table["id"], Datum::I64(i))
                    .set(&table["foo"], Datum::I64(0xDEADBEEF))
                    .execute();
                store.commit();
            }

            let mut meta = TableScan::new();
            meta.set_table_id(table.id);
            meta.set_desc(false);
            meta.mut_columns().push(table["id"].as_column_info());
            meta.mut_columns().push(table["foo"].as_column_info());

            let mut exec = PbExecutor::new();
            exec.set_tp(ExecType::TypeTableScan);
            exec.set_tbl_scan(meta);

            bench_dag_handle(b, &[exec], &[table.get_record_range_all()], &store, batch);
        });
    }

    bench(c, "dag_normal_table_scan_pk_front", false);
    bench(c, "dag_batch_table_scan_pk_front", true);
}

// TODO: Remove this test.
/// Integrate DAGRequestHandler + TableScan + Selection, scans a range with 1000 keys and retain 500 keys,
/// 2 interested column, PK & the column to filter.
fn bench_dag_table_scan_selection_pk_front(c: &mut Criterion) {
    use cop_datatype::{FieldTypeAccessor, FieldTypeTp};
    use tikv::util::codec::number::NumberEncoder;
    use tipb::executor::ExecType;

    fn bench(c: &mut Criterion, id: &'static str, batch: bool) {
        c.bench_function(id, move |b| {
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
            for i in 0..1000 {
                store.begin();
                store
                    .insert_into(&table)
                    .set(&table["id"], Datum::I64(i))
                    .set(&table["foo"], Datum::I64(1000 - i))
                    .execute();
                store.commit();
            }

            let mut table_exec = PbExecutor::new();
            table_exec.set_tp(ExecType::TypeTableScan);
            table_exec.set_tbl_scan({
                let mut meta = TableScan::new();
                meta.set_table_id(table.id);
                meta.set_desc(false);
                meta.mut_columns().push(table["id"].as_column_info());
                meta.mut_columns().push(table["foo"].as_column_info());
                meta
            });

            let mut selection_exec = PbExecutor::new();
            selection_exec.set_tp(ExecType::TypeSelection);
            selection_exec.set_selection({
                let mut meta = Selection::new();
                meta.mut_conditions().push({
                    let mut expr = Expr::new();
                    expr.set_tp(ExprType::ScalarFunc);
                    expr.set_sig(ScalarFuncSig::GTInt);
                    expr.mut_field_type()
                        .as_mut_accessor()
                        .set_tp(FieldTypeTp::LongLong);
                    expr.mut_children().push({
                        let mut lhs = Expr::new();
                        lhs.mut_field_type()
                            .as_mut_accessor()
                            .set_tp(FieldTypeTp::LongLong);
                        lhs.set_tp(ExprType::ColumnRef);
                        lhs.mut_val().encode_i64(1).unwrap();
                        lhs
                    });
                    expr.mut_children().push({
                        let mut rhs = Expr::new();
                        rhs.mut_field_type()
                            .as_mut_accessor()
                            .set_tp(FieldTypeTp::LongLong);
                        rhs.set_tp(ExprType::Uint64);
                        rhs.mut_val().encode_u64(500).unwrap();
                        rhs
                    });
                    expr
                });
                meta
            });

            bench_dag_handle(
                b,
                &[table_exec, selection_exec],
                &[table.get_record_range_all()],
                &store,
                batch,
            );
        });
    }

    bench(c, "dag_normal_table_scan_selection_pk_front", false);
    bench(c, "dag_batch_table_scan_selection_pk_front", true);
}

fn bench_dag_table_scan_long_datum_primary_key(c: &mut Criterion) {
    use tipb::executor::ExecType;

    fn bench(c: &mut Criterion, id: &'static str, batch: bool) {
        c.bench_function(id, move |b| {
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
            for i in 0..1001 {
                store.begin();
                store
                    .insert_into(&table)
                    .set(&table["id"], Datum::I64(i))
                    .set(&table["foo"], Datum::I64(0xDEADBEEF))
                    .set(&table["bar"], Datum::Bytes([0xCC].repeat(200)))
                    .execute();
                store.commit();
            }

            let mut meta = TableScan::new();
            meta.set_table_id(table.id);
            meta.set_desc(false);
            meta.mut_columns().push(table["id"].as_column_info());

            let mut exec = PbExecutor::new();
            exec.set_tp(ExecType::TypeTableScan);
            exec.set_tbl_scan(meta);

            bench_dag_handle(b, &[exec], &[table.get_record_range_all()], &store, batch);
        });
    }

    bench(c, "dag_normal_table_scan_long_datum_primary_key", false);
    bench(c, "dag_batch_table_scan_long_datum_primary_key", true);
}

fn bench_dag_table_scan_long_datum_normal(c: &mut Criterion) {
    use tipb::executor::ExecType;

    fn bench(c: &mut Criterion, id: &'static str, batch: bool) {
        c.bench_function(id, move |b| {
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
            for i in 0..1001 {
                store.begin();
                store
                    .insert_into(&table)
                    .set(&table["id"], Datum::I64(i))
                    .set(&table["foo"], Datum::I64(0xDEADBEEF))
                    .set(&table["bar"], Datum::Bytes([0xCC].repeat(200)))
                    .execute();
                store.commit();
            }

            let mut meta = TableScan::new();
            meta.set_table_id(table.id);
            meta.set_desc(false);
            meta.mut_columns().push(table["foo"].as_column_info());

            let mut exec = PbExecutor::new();
            exec.set_tp(ExecType::TypeTableScan);
            exec.set_tbl_scan(meta);

            bench_dag_handle(b, &[exec], &[table.get_record_range_all()], &store, batch);
        });
    }

    bench(c, "dag_normal_table_scan_long_datum_normal", false);
    bench(c, "dag_batch_table_scan_long_datum_normal", true);
}

fn bench_dag_table_scan_long_datum_long(c: &mut Criterion) {
    use tipb::executor::ExecType;

    fn bench(c: &mut Criterion, id: &'static str, batch: bool) {
        c.bench_function(id, move |b| {
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
            for i in 0..1001 {
                store.begin();
                store
                    .insert_into(&table)
                    .set(&table["id"], Datum::I64(i))
                    .set(&table["foo"], Datum::I64(0xDEADBEEF))
                    .set(&table["bar"], Datum::Bytes([0xCC].repeat(200)))
                    .execute();
                store.commit();
            }

            let mut meta = TableScan::new();
            meta.set_table_id(table.id);
            meta.set_desc(false);
            meta.mut_columns().push(table["bar"].as_column_info());

            let mut exec = PbExecutor::new();
            exec.set_tp(ExecType::TypeTableScan);
            exec.set_tbl_scan(meta);

            bench_dag_handle(b, &[exec], &[table.get_record_range_all()], &store, batch);
        });
    }

    bench(c, "dag_normal_table_scan_long_datum_long", false);
    bench(c, "dag_batch_table_scan_long_datum_long", true);
}

fn bench_dag_table_scan_long_datum_all(c: &mut Criterion) {
    use tipb::executor::ExecType;

    fn bench(c: &mut Criterion, id: &'static str, batch: bool) {
        c.bench_function(id, move |b| {
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
            for i in 0..1001 {
                store.begin();
                store
                    .insert_into(&table)
                    .set(&table["id"], Datum::I64(i))
                    .set(&table["foo"], Datum::I64(0xDEADBEEF))
                    .set(&table["bar"], Datum::Bytes([0xCC].repeat(200)))
                    .execute();
                store.commit();
            }

            let mut meta = TableScan::new();
            meta.set_table_id(table.id);
            meta.set_desc(false);
            meta.mut_columns().push(table["id"].as_column_info());
            meta.mut_columns().push(table["foo"].as_column_info());
            meta.mut_columns().push(table["bar"].as_column_info());

            let mut exec = PbExecutor::new();
            exec.set_tp(ExecType::TypeTableScan);
            exec.set_tbl_scan(meta);

            bench_dag_handle(b, &[exec], &[table.get_record_range_all()], &store, batch);
        });
    }

    bench(c, "dag_normal_table_scan_long_datum_all", false);
    bench(c, "dag_batch_table_scan_long_datum_all", true);
}

criterion_group!(
    benches,
    bench_table_scan_primary_key,
    bench_table_scan_datum_front,
    bench_table_scan_datum_multi_front,
    bench_table_scan_datum_end,
    bench_table_scan_datum_all,
    bench_table_scan_long_datum_primary_key,
    bench_table_scan_long_datum_normal,
    bench_table_scan_long_datum_long,
    bench_table_scan_long_datum_all,
    bench_table_scan_long_datum_primary_key_multi_rows,
    bench_table_scan_long_datum_normal_multi_rows,
    bench_table_scan_long_datum_long_multi_rows,
    bench_table_scan_long_datum_all_multi_rows,
    bench_batch_table_scan_long_datum_primary_key_multi_rows,
    bench_batch_table_scan_long_datum_normal_multi_rows,
    bench_batch_table_scan_long_datum_long_multi_rows,
    bench_batch_table_scan_long_datum_all_multi_rows,
    bench_table_scan_datum_absent,
    bench_table_scan_datum_absent_large_row,
    bench_table_scan_point_range,
    bench_table_scan_multi_point_range,
    bench_table_scan_multi_rows,
    bench_table_scan_datum_all_multi_rows,
    bench_batch_table_scan_multi_rows,
    bench_batch_table_scan_datum_all_multi_rows,
    bench_normal_index_scan_primary_key,
    bench_normal_index_scan_index,
    bench_dag_table_scan_primary_key,
    bench_dag_table_scan_datum_front,
    bench_dag_table_scan_datum_all,
    bench_dag_table_scan_pk_front,
    bench_dag_table_scan_selection_pk_front,
    bench_dag_table_scan_long_datum_primary_key,
    bench_dag_table_scan_long_datum_normal,
    bench_dag_table_scan_long_datum_long,
    bench_dag_table_scan_long_datum_all,
);
criterion_main!(benches);
