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

#[macro_use]
extern crate criterion;

extern crate kvproto;
extern crate test_coprocessor;
extern crate tikv;
extern crate tipb;

use criterion::{black_box, Bencher, Criterion};

use kvproto::coprocessor::KeyRange;
use tipb::executor::{IndexScan, TableScan};

use test_coprocessor::*;
use tikv::coprocessor::codec::Datum;
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
            let mut executor = TableScanExecutor::new(
                meta.clone(),
                ranges.to_vec(),
                store.to_fixture_store(),
                false,
            ).unwrap();
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
            .add_col(id.clone())
            .add_col(foo.clone())
            .build();

        let mut store = Store::new();
        for i in 0..10 {
            store.begin();
            store
                .insert_into(&table)
                .set(&id, Datum::I64(i))
                .set(&foo, Datum::I64(0xDEADBEEF))
                .execute();
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(id.get_column_info());

        bench_table_scan_next(b, &meta, &[table.get_select_range()], &store);
    });
}

/// next() for 1 time, 1 interested column, at the front of each row. Each row contains 100 columns.
///
/// This kind of scanner is used in SQLs like SELECT COUNT(column)
fn bench_table_scan_datum_front(c: &mut Criterion) {
    c.bench_function("table_scan_datum_front", |b| {
        let mut cols = vec![];
        for _ in 0..100 {
            let col = ColumnBuilder::new().col_type(TYPE_LONG).build();
            cols.push(col);
        }
        let mut table = TableBuilder::new();
        for col in &cols {
            table = table.add_col(col.clone());
        }
        let table = table.build();

        let mut store = Store::new();
        for i in 0..10 {
            store.begin();
            {
                let mut insert = store.insert_into(&table);
                for (idx, col) in cols.iter().enumerate() {
                    insert = insert.set(&col, Datum::I64((i ^ idx) as i64));
                }
                insert.execute();
            }
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(cols[0].get_column_info());

        bench_table_scan_next(b, &meta, &[table.get_select_range()], &store);
    });
}

/// next() for 1 time, 1 interested column, at the front of each row. Each row contains 100 columns
/// and length is very long.
///
/// Bench the impact of large values.
fn bench_table_scan_long_datum_front(c: &mut Criterion) {
    c.bench_function("table_scan_long_datum_front", |b| {
        let mut cols = vec![];
        for _ in 0..100 {
            let col = ColumnBuilder::new().col_type(TYPE_VAR_CHAR).build();
            cols.push(col);
        }
        let mut table = TableBuilder::new();
        for col in &cols {
            table = table.add_col(col.clone());
        }
        let table = table.build();

        let mut store = Store::new();
        for _ in 0..10 {
            let bytes = vec![0xCC; 1000];
            store.begin();
            {
                let mut insert = store.insert_into(&table);
                for col in &cols {
                    insert = insert.set(col, Datum::Bytes(bytes.clone()));
                }
                insert.execute();
            }
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(cols[0].get_column_info());

        bench_table_scan_next(b, &meta, &[table.get_select_range()], &store);
    });
}

/// next() for 1 time, 2 interested columns, at the front of each row. Each row contains 100
/// columns.
fn bench_table_scan_datum_multi_front(c: &mut Criterion) {
    c.bench_function("table_scan_datum_multi_front", |b| {
        let mut cols = vec![];
        for _ in 0..100 {
            let col = ColumnBuilder::new().col_type(TYPE_LONG).build();
            cols.push(col);
        }
        let mut table = TableBuilder::new();
        for col in &cols {
            table = table.add_col(col.clone());
        }
        let table = table.build();

        let mut store = Store::new();
        for i in 0..10 {
            store.begin();
            {
                let mut insert = store.insert_into(&table);
                for (idx, col) in cols.iter().enumerate() {
                    insert = insert.set(&col, Datum::I64((i ^ idx) as i64));
                }
                insert.execute();
            }
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(cols[0].get_column_info());
        meta.mut_columns().push(cols[1].get_column_info());

        bench_table_scan_next(b, &meta, &[table.get_select_range()], &store);
    });
}

/// next() for 1 time, 1 interested column, at the end of each row. Each row contains 100 columns.
fn bench_table_scan_datum_end(c: &mut Criterion) {
    c.bench_function("table_scan_datum_end", |b| {
        let mut cols = vec![];
        for _ in 0..100 {
            let col = ColumnBuilder::new().col_type(TYPE_LONG).build();
            cols.push(col);
        }
        let mut table = TableBuilder::new();
        for col in &cols {
            table = table.add_col(col.clone());
        }
        let table = table.build();

        let mut store = Store::new();
        for i in 0..10 {
            store.begin();
            {
                let mut insert = store.insert_into(&table);
                for (idx, col) in cols.iter().enumerate() {
                    insert = insert.set(&col, Datum::I64((i ^ idx) as i64));
                }
                insert.execute();
            }
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(cols[99].get_column_info());

        bench_table_scan_next(b, &meta, &[table.get_select_range()], &store);
    });
}

/// next() for 1 time, 100 interested columns, each column in the row is interested
/// (i.e. there is totally 100 columns in the row).
fn bench_table_scan_datum_all(c: &mut Criterion) {
    c.bench_function("table_scan_datum_all", |b| {
        let mut cols = vec![];
        for _ in 0..100 {
            let col = ColumnBuilder::new().col_type(TYPE_LONG).build();
            cols.push(col);
        }
        let mut table = TableBuilder::new();
        for col in &cols {
            table = table.add_col(col.clone());
        }
        let table = table.build();

        let mut store = Store::new();
        for i in 0..10 {
            store.begin();
            {
                let mut insert = store.insert_into(&table);
                for (idx, col) in cols.iter().enumerate() {
                    insert = insert.set(&col, Datum::I64((i ^ idx) as i64));
                }
                insert.execute();
            }
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        for col in &cols {
            meta.mut_columns().push(col.get_column_info());
        }

        bench_table_scan_next(b, &meta, &[table.get_select_range()], &store);
    });
}

/// next() for 1 time, 1 interested column, but the column is missing from each row (i.e. it's
/// default value is used instead). Each row contains totally 10 columns.
fn bench_table_scan_datum_absent(c: &mut Criterion) {
    c.bench_function("table_scan_datum_absent", |b| {
        let mut cols = vec![];
        for _ in 0..10 {
            let col = ColumnBuilder::new().col_type(TYPE_LONG).build();
            cols.push(col);
        }
        let mut table = TableBuilder::new();
        for col in &cols {
            table = table.add_col(col.clone());
        }
        let table = table.build();

        let mut store = Store::new();
        for i in 0..10 {
            store.begin();
            {
                let mut insert = store.insert_into(&table);
                for (idx, col) in cols.iter().enumerate().skip(1) {
                    insert = insert.set(&col, Datum::I64((i ^ idx) as i64));
                }
                insert.execute();
            }
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(cols[0].get_column_info());

        bench_table_scan_next(b, &meta, &[table.get_select_range()], &store);
    });
}

/// next() for 1 time, 1 interested column, but the column is missing from each row (i.e. it's
/// default value is used instead). Each row contains totally 100 columns.
fn bench_table_scan_datum_absent_large_row(c: &mut Criterion) {
    c.bench_function("table_scan_datum_absent_large_row", |b| {
        let mut cols = vec![];
        for _ in 0..100 {
            let col = ColumnBuilder::new().col_type(TYPE_LONG).build();
            cols.push(col);
        }
        let mut table = TableBuilder::new();
        for col in &cols {
            table = table.add_col(col.clone());
        }
        let table = table.build();

        let mut store = Store::new();
        for i in 0..10 {
            store.begin();
            {
                let mut insert = store.insert_into(&table);
                for (idx, col) in cols.iter().enumerate().skip(1) {
                    insert = insert.set(&col, Datum::I64((i ^ idx) as i64));
                }
                insert.execute();
            }
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(cols[0].get_column_info());

        bench_table_scan_next(b, &meta, &[table.get_select_range()], &store);
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
            .add_col(id.clone())
            .add_col(foo.clone())
            .build();

        let mut store = Store::new();
        for i in 0..10 {
            store.begin();
            store
                .insert_into(&table)
                .set(&id, Datum::I64(i))
                .set(&foo, Datum::I64(0xDEADBEEF))
                .execute();
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(id.get_column_info());

        // We pass 2 point-ranges instead of 1 point-range, because there is a warm-up next().
        bench_table_scan_next(
            b,
            &meta,
            &[
                table.get_point_select_range(0),
                table.get_point_select_range(1),
            ],
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
            .add_col(id.clone())
            .add_col(foo.clone())
            .build();

        let mut store = Store::new();
        for i in 0..1001 {
            store.begin();
            store
                .insert_into(&table)
                .set(&id, Datum::I64(i))
                .set(&foo, Datum::I64(0xDEADBEEF))
                .execute();
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(id.get_column_info());

        b.iter_with_setup(
            || {
                let mut ranges = vec![];
                // Generate 1001 ranges, because there will be a warm-up next().
                for i in 0..1001 {
                    ranges.push(table.get_point_select_range(i));
                }
                let mut executor =
                    TableScanExecutor::new(meta.clone(), ranges, store.to_fixture_store(), false)
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

/// 1 interested column, which is PK. One range is given, which contains 1000 rows. This case
/// benches the performance when all records of this range are consumed.
fn bench_table_scan_multi_rows(c: &mut Criterion) {
    use tikv::coprocessor::dag::executor::TableScanExecutor;

    c.bench_function("table_scan_multi_rows", |b| {
        let id = ColumnBuilder::new()
            .col_type(TYPE_LONG)
            .primary_key(true)
            .build();
        let foo = ColumnBuilder::new().col_type(TYPE_LONG).build();
        let table = TableBuilder::new()
            .add_col(id.clone())
            .add_col(foo.clone())
            .build();

        let mut store = Store::new();
        for i in 0..1001 {
            store.begin();
            store
                .insert_into(&table)
                .set(&id, Datum::I64(i))
                .set(&foo, Datum::I64(0xDEADBEEF))
                .execute();
            store.commit();
        }

        let mut meta = TableScan::new();
        meta.set_table_id(table.id);
        meta.set_desc(false);
        meta.mut_columns().push(id.get_column_info());

        b.iter_with_setup(
            || {
                let mut executor = TableScanExecutor::new(
                    meta.clone(),
                    vec![table.get_select_range()],
                    store.to_fixture_store(),
                    false,
                ).unwrap();
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
            let mut executor = IndexScanExecutor::new(
                meta.clone(),
                ranges.to_vec(),
                store.to_fixture_store(),
                unique,
                false,
            ).unwrap();
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
            .add_col(id.clone())
            .add_col(foo.clone())
            .build();

        let mut store = Store::new();
        for i in 0..10 {
            store.begin();
            store
                .insert_into(&table)
                .set(&id, Datum::I64(i))
                .set(&foo, Datum::I64(0xDEADBEEF))
                .execute();
            store.commit();
        }

        let mut meta = IndexScan::new();
        meta.set_table_id(table.id);
        meta.set_index_id(index_id);
        meta.mut_columns().push(id.get_column_info());
        meta.set_desc(false);
        meta.set_unique(false);

        bench_index_scan_next(b, &meta, false, &[table.get_index_range(index_id)], &store);
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
            .add_col(id.clone())
            .add_col(foo.clone())
            .build();

        let mut store = Store::new();
        for i in 0..10 {
            store.begin();
            store
                .insert_into(&table)
                .set(&id, Datum::I64(i))
                .set(&foo, Datum::I64(0xDEADBEEF))
                .execute();
            store.commit();
        }

        let mut meta = IndexScan::new();
        meta.set_table_id(table.id);
        meta.set_index_id(index_id);
        meta.mut_columns().push(foo.get_column_info());
        meta.set_desc(false);
        meta.set_unique(false);

        bench_index_scan_next(b, &meta, false, &[table.get_index_range(index_id)], &store);
    });
}

criterion_group!(
    benches,
    bench_table_scan_primary_key,
    bench_table_scan_datum_front,
    bench_table_scan_datum_multi_front,
    bench_table_scan_long_datum_front,
    bench_table_scan_datum_multi_front,
    bench_table_scan_datum_end,
    bench_table_scan_datum_all,
    bench_table_scan_datum_absent,
    bench_table_scan_datum_absent_large_row,
    bench_table_scan_point_range,
    bench_table_scan_multi_point_range,
    bench_table_scan_multi_rows,
    bench_normal_index_scan_primary_key,
    bench_normal_index_scan_index,
);
criterion_main!(benches);
