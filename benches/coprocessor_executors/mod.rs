// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(specialization)]
#![feature(repeat_generic_slice)]

mod integrated;
mod simple_aggr;
mod table_scan;
mod util;

fn main() {
    let mut c = criterion::Criterion::default().configure_from_args();

    util::fixture_executor::bench(&mut c);
    table_scan::bench(&mut c);
    simple_aggr::bench(&mut c);
    integrated::bench(&mut c);

    c.final_summary();
}

/*

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

*/
