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

extern crate criterion;

extern crate test_coprocessor;
extern crate tikv;
extern crate tipb;

use criterion::{black_box, Bencher, Criterion};

use tipb::executor::TableScan;

use test_coprocessor::*;
use tikv::coprocessor::codec::Datum;
use tikv::coprocessor::dag::executor::{Executor, TableScanExecutor};

fn bench_table_scan_primary_key(b: &mut Bencher) {
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
            executor.next().unwrap();
            executor
        },
        |mut executor| {
            black_box(executor.next().unwrap());
        },
    );
}

fn main() {
    let mut criterion = Criterion::default().sample_size(10).configure_from_args();
    criterion.bench_function("table_scan_primary_key", bench_table_scan_primary_key);
    criterion.final_summary();
}
