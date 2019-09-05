// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

pub mod fixture;
mod util;

use criterion::measurement::Measurement;

use crate::util::scan_bencher::ScanBencher;
use crate::util::store::*;
use crate::util::BenchCase;

const ROWS: usize = 5000;

/// 1 interested column, which is PK (which is in the key)
///
/// This kind of scanner is used in SQLs like SELECT COUNT(*).
fn bench_table_scan_primary_key<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (table, store) = fixture::table_with_2_columns(ROWS);
    input.0.bench(
        b,
        &[table["id"].as_column_info()],
        &[table.get_record_range_all()],
        &store,
        (),
    );
}

/// 1 interested column, at the front of each row. Each row contains 100 columns.
///
/// This kind of scanner is used in SQLs like `SELECT COUNT(column)`.
fn bench_table_scan_datum_front<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (table, store) = fixture::table_with_multi_columns(ROWS, 100);
    input.0.bench(
        b,
        &[table["col0"].as_column_info()],
        &[table.get_record_range_all()],
        &store,
        (),
    );
}

/// 2 interested columns, at the front of each row. Each row contains 100 columns.
fn bench_table_scan_datum_multi_front<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (table, store) = fixture::table_with_multi_columns(ROWS, 100);
    input.0.bench(
        b,
        &[
            table["col0"].as_column_info(),
            table["col1"].as_column_info(),
        ],
        &[table.get_record_range_all()],
        &store,
        (),
    );
}

/// 1 interested column, at the end of each row. Each row contains 100 columns.
fn bench_table_scan_datum_end<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (table, store) = fixture::table_with_multi_columns(ROWS, 100);
    input.0.bench(
        b,
        &[table["col99"].as_column_info()],
        &[table.get_record_range_all()],
        &store,
        (),
    );
}

/// 100 interested columns, all columns in the row are interested (i.e. there are totally 100
/// columns in the row).
fn bench_table_scan_datum_all<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (table, store) = fixture::table_with_multi_columns(ROWS, 100);
    input.0.bench(
        b,
        &table.columns_info(),
        &[table.get_record_range_all()],
        &store,
        (),
    );
}

/// 3 columns in the row and the last column is very long but only PK is interested.
fn bench_table_scan_long_datum_primary_key<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (table, store) = fixture::table_with_long_column(ROWS);
    input.0.bench(
        b,
        &[table["id"].as_column_info()],
        &[table.get_record_range_all()],
        &store,
        (),
    );
}

/// 3 columns in the row and the last column is very long but a short column is interested.
fn bench_table_scan_long_datum_normal<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (table, store) = fixture::table_with_long_column(ROWS);
    input.0.bench(
        b,
        &[table["foo"].as_column_info()],
        &[table.get_record_range_all()],
        &store,
        (),
    );
}

/// 3 columns in the row and the last column is very long and the long column is interested.
fn bench_table_scan_long_datum_long<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (table, store) = fixture::table_with_long_column(ROWS);
    input.0.bench(
        b,
        &[table["bar"].as_column_info()],
        &[table.get_record_range_all()],
        &store,
        (),
    );
}

/// 3 columns in the row and the last column is very long and the all columns are interested.
fn bench_table_scan_long_datum_all<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (table, store) = fixture::table_with_long_column(ROWS);
    input.0.bench(
        b,
        &[
            table["id"].as_column_info(),
            table["foo"].as_column_info(),
            table["bar"].as_column_info(),
        ],
        &[table.get_record_range_all()],
        &store,
        (),
    );
}

/// 1 interested column, but the column is missing from each row (i.e. it's default value is
/// used instead). Each row contains totally 10 columns.
fn bench_table_scan_datum_absent<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (table, store) = fixture::table_with_missing_column(ROWS, 10);
    input.0.bench(
        b,
        &[table["col0"].as_column_info()],
        &[table.get_record_range_all()],
        &store,
        (),
    );
}

/// 1 interested column, but the column is missing from each row (i.e. it's default value is
/// used instead). Each row contains totally 100 columns.
fn bench_table_scan_datum_absent_large_row<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (table, store) = fixture::table_with_missing_column(ROWS, 100);
    input.0.bench(
        b,
        &[table["col0"].as_column_info()],
        &[table.get_record_range_all()],
        &store,
        (),
    );
}

/// 1 interested column, which is PK. However the range given are point ranges.
fn bench_table_scan_point_range<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let (table, store) = fixture::table_with_2_columns(ROWS);

    let mut ranges = vec![];
    for i in 0..=1024 {
        ranges.push(table.get_record_range_one(i));
    }

    input
        .0
        .bench(b, &[table["id"].as_column_info()], &ranges, &store, ());
}

#[derive(Clone)]
struct Input<M>(Box<dyn ScanBencher<util::TableScanParam, M>>)
where
    M: Measurement + 'static;

impl<M> Input<M>
where
    M: Measurement + 'static,
{
    pub fn new<T: ScanBencher<util::TableScanParam, M> + 'static>(b: T) -> Self {
        Self(Box::new(b))
    }
}

impl<M> std::fmt::Display for Input<M>
where
    M: Measurement + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.name())
    }
}

pub fn bench<M>(c: &mut criterion::Criterion<M>)
where
    M: Measurement + 'static,
{
    let mut inputs = vec![
        Input::new(util::NormalTableScanNext1024Bencher::<MemStore>::new()),
        Input::new(util::BatchTableScanNext1024Bencher::<MemStore>::new()),
        Input::new(util::TableScanDAGBencher::<RocksStore>::new(false, ROWS)),
        Input::new(util::TableScanDAGBencher::<RocksStore>::new(true, ROWS)),
    ];
    if crate::util::bench_level() >= 2 {
        let mut additional_inputs = vec![
            Input::new(util::NormalTableScanNext1024Bencher::<RocksStore>::new()),
            Input::new(util::BatchTableScanNext1024Bencher::<RocksStore>::new()),
            Input::new(util::NormalTableScanNext1Bencher::<MemStore>::new()),
            Input::new(util::NormalTableScanNext1Bencher::<RocksStore>::new()),
            Input::new(util::TableScanDAGBencher::<MemStore>::new(false, ROWS)),
            Input::new(util::TableScanDAGBencher::<MemStore>::new(true, ROWS)),
        ];
        inputs.append(&mut additional_inputs);
    }

    let mut cases = vec![
        BenchCase::new("table_scan_primary_key", bench_table_scan_primary_key),
        BenchCase::new("table_scan_long_datum_all", bench_table_scan_long_datum_all),
        BenchCase::new(
            "table_scan_datum_absent_large_row",
            bench_table_scan_datum_absent_large_row,
        ),
    ];
    if crate::util::bench_level() >= 1 {
        let mut additional_cases = vec![
            BenchCase::new("table_scan_datum_front", bench_table_scan_datum_front),
            BenchCase::new("table_scan_datum_all", bench_table_scan_datum_all),
            BenchCase::new("table_scan_point_range", bench_table_scan_point_range),
        ];
        cases.append(&mut additional_cases);
    }
    if crate::util::bench_level() >= 2 {
        let mut additional_cases = vec![
            BenchCase::new(
                "table_scan_datum_multi_front",
                bench_table_scan_datum_multi_front,
            ),
            BenchCase::new("table_scan_datum_end", bench_table_scan_datum_end),
            BenchCase::new(
                "table_scan_long_datum_primary_key",
                bench_table_scan_long_datum_primary_key,
            ),
            BenchCase::new(
                "table_scan_long_datum_normal",
                bench_table_scan_long_datum_normal,
            ),
            BenchCase::new(
                "table_scan_long_datum_long",
                bench_table_scan_long_datum_long,
            ),
            BenchCase::new("table_scan_datum_absent", bench_table_scan_datum_absent),
        ];
        cases.append(&mut additional_cases);
    }

    cases.sort();
    for case in cases {
        let mut group = c.benchmark_group(case.get_name());
        for input in inputs.iter() {
            group.bench_with_input(
                criterion::BenchmarkId::from_parameter(input),
                input,
                case.get_fn(),
            ); // TODO: add parameter for each bench
        }
        group.finish();
    }
}
