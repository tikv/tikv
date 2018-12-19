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
extern crate protobuf;

extern crate cop_datatype;
extern crate kvproto;
extern crate test_coprocessor;
extern crate tikv;
extern crate tipb;

use criterion::{black_box, Bencher, Criterion};

use kvproto::coprocessor::KeyRange;
use tipb::executor::{Executor as PbExecutor, IndexScan, Selection, TableScan};
use tipb::expression::{Expr, ExprType, ScalarFuncSig};

use test_coprocessor::*;
use tikv::coprocessor::codec::Datum;
use tikv::coprocessor::dag::batch_executor::interface::*;
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
/// This kind of scanner is used in SQLs like `SELECT COUNT(column)`.
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
///
/// This kind of scanner is used in SQLs like `SELECT *`.
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

/// 100 interested columns, each column in the row is interested. One range is given,
/// which contains 1000 rows.
///
/// This case benches the performance when all records of this range are consumed for SQLs like
/// `SELECT *`.
fn bench_table_scan_datum_all_multi_rows(c: &mut Criterion) {
    use tikv::coprocessor::dag::executor::TableScanExecutor;

    c.bench_function("table_scan_datum_all_multi_rows", |b| {
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
        for i in 0..1001 {
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

        let context = {
            let columns_info = vec![id.get_column_info()];
            ExecutorContext::new(columns_info)
        };

        b.iter_with_setup(
            || {
                let mut executor = BatchTableScanExecutor::new(
                    store.to_fixture_store(),
                    context.clone(),
                    vec![table.get_select_range()],
                    false,
                ).unwrap();
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

    c.bench_function("batch_table_scan_datum_all_multi_rows", |b| {
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
        for i in 0..1001 {
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

        let context = {
            let mut columns_info = vec![];
            for col in &cols {
                columns_info.push(col.get_column_info());
            }
            ExecutorContext::new(columns_info)
        };

        b.iter_with_setup(
            || {
                let mut executor = BatchTableScanExecutor::new(
                    store.to_fixture_store(),
                    context.clone(),
                    vec![table.get_select_range()],
                    false,
                ).unwrap();
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

fn bench_dag_handle(
    b: &mut Bencher,
    executors: &[PbExecutor],
    ranges: &[KeyRange],
    store: &Store<RocksEngine>,
    enable_batch: bool,
) {
    use tikv::coprocessor::dag::DAGContext;
    use tikv::coprocessor::Deadline;
    use tipb::select::DAGRequest;

    let mut dag = DAGRequest::new();
    dag.set_executors(::protobuf::RepeatedField::from_vec(executors.to_vec()));

    b.iter_with_setup(
        || {
            DAGContext::new(
                dag.clone(),
                ranges.to_vec(),
                store.to_fixture_store(),
                Deadline::from_now("", ::std::time::Duration::from_secs(10)),
                64,
                false,
                enable_batch,
            ).unwrap()
        },
        |mut dag| {
            use tikv::coprocessor::RequestHandler;
            dag.handle_request().unwrap();
        },
    );
}

/// Integrate DAGContext + TableScan, scans a range with 1000 keys, 1 interested column (PK).
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
                .add_col(id.clone())
                .add_col(foo.clone())
                .build();

            let mut store = Store::new();
            for i in 0..1000 {
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

            let mut exec = PbExecutor::new();
            exec.set_tp(ExecType::TypeTableScan);
            exec.set_tbl_scan(meta);

            bench_dag_handle(b, &[exec], &[table.get_select_range()], &store, batch);
        });
    }

    bench(c, "dag_normal_table_scan_primary_key", false);
    bench(c, "dag_batch_table_scan_primary_key", true);
}

/// Integrate DAGContext + TableScan, scans a range with 1000 keys, 1 interested column, which is
/// at the front in a 100 columns row.
fn bench_dag_table_scan_datum_front(c: &mut Criterion) {
    use tipb::executor::ExecType;

    fn bench(c: &mut Criterion, id: &'static str, batch: bool) {
        c.bench_function(id, move |b| {
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
            for i in 0..1000 {
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

            let mut exec = PbExecutor::new();
            exec.set_tp(ExecType::TypeTableScan);
            exec.set_tbl_scan(meta);

            bench_dag_handle(b, &[exec], &[table.get_select_range()], &store, batch);
        });
    }

    bench(c, "dag_normal_table_scan_datum_front", false);
    bench(c, "dag_batch_table_scan_datum_front", true);
}

/// Integrate DAGContext + TableScan, scans a range with 1000 keys, all column are interested and
/// there are 100 columns in the row.
fn bench_dag_table_scan_datum_all(c: &mut Criterion) {
    use tipb::executor::ExecType;

    fn bench(c: &mut Criterion, id: &'static str, batch: bool) {
        c.bench_function(id, move |b| {
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
            for i in 0..1000 {
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

            let mut exec = PbExecutor::new();
            exec.set_tp(ExecType::TypeTableScan);
            exec.set_tbl_scan(meta);

            bench_dag_handle(b, &[exec], &[table.get_select_range()], &store, batch);
        });
    }

    bench(c, "dag_normal_table_scan_datum_all", false);
    bench(c, "dag_batch_table_scan_datum_all", true);
}

// TODO: Remove this test.
/// Integrate DAGContext + TableScan + Selection, scans a range with 1000 keys and retain 500 keys,
/// 2 interested column, PK & the column to filter.
fn bench_dag_table_scan_selection_primary_key(c: &mut Criterion) {
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
                .add_col(id.clone())
                .add_col(foo.clone())
                .build();

            let mut store = Store::new();
            for i in 0..1000 {
                store.begin();
                store
                    .insert_into(&table)
                    .set(&id, Datum::I64(i))
                    .set(&foo, Datum::I64(1000 - i))
                    .execute();
                store.commit();
            }

            let mut table_exec = PbExecutor::new();
            table_exec.set_tp(ExecType::TypeTableScan);
            table_exec.set_tbl_scan({
                let mut meta = TableScan::new();
                meta.set_table_id(table.id);
                meta.set_desc(false);
                meta.mut_columns().push(id.get_column_info());
                meta.mut_columns().push(foo.get_column_info());
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
                &[table.get_select_range()],
                &store,
                batch,
            );
        });
    }

    bench(c, "dag_normal_table_scan_selection_primary_key", false);
    bench(c, "dag_batch_table_scan_selection_primary_key", true);
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
    bench_table_scan_datum_all_multi_rows,
    bench_batch_table_scan_multi_rows,
    bench_batch_table_scan_datum_all_multi_rows,
    bench_normal_index_scan_primary_key,
    bench_normal_index_scan_index,
    bench_dag_table_scan_primary_key,
    bench_dag_table_scan_datum_front,
    bench_dag_table_scan_datum_all,
    bench_dag_table_scan_selection_primary_key,
);
criterion_main!(benches);
