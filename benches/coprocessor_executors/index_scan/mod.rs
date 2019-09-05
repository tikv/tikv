// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

pub mod fixture;
mod util;

use criterion::measurement::Measurement;

use crate::util::scan_bencher::ScanBencher;
use crate::util::store::*;
use crate::util::BenchCase;

const ROWS: usize = 5000;

/// 1 interested column, which is PK (which is in the key).
///
/// This kind of scanner is used in SQLs like `SELECT * FROM .. WHERE index = X`, an index lookup
/// will be performed so that PK is needed.
fn bench_index_scan_primary_key<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement + 'static,
{
    let (index_id, table, store) = fixture::table_with_2_columns_and_one_index(ROWS);
    input.0.bench(
        b,
        &[table["id"].as_column_info()],
        &[table.get_index_range_all(index_id)],
        &store,
        false,
    );
}

/// 1 interested column, which is the column of the index itself (which is in the key).
///
/// This kind of scanner is used in SQLs like `SELECT COUNT(*) FROM .. WHERE index = X` or
/// `SELECT index FROM .. WHERE index = X`. There is no double read.
fn bench_index_scan_index<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement + 'static,
{
    let (index_id, table, store) = fixture::table_with_2_columns_and_one_index(ROWS);
    input.0.bench(
        b,
        &[table["foo"].as_column_info()],
        &[table.get_index_range_all(index_id)],
        &store,
        false,
    );
}

#[derive(Clone)]
struct Input<M>(Box<dyn ScanBencher<util::IndexScanParam, M>>)
where
    M: Measurement + 'static;

impl<M> Input<M>
where
    M: Measurement + 'static,
{
    pub fn new<T: ScanBencher<util::IndexScanParam, M> + 'static>(b: T) -> Self {
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
        Input::new(util::NormalIndexScanNext1024Bencher::<MemStore>::new()),
        Input::new(util::BatchIndexScanNext1024Bencher::<MemStore>::new()),
        Input::new(util::IndexScanDAGBencher::<RocksStore>::new(false, ROWS)),
        Input::new(util::IndexScanDAGBencher::<RocksStore>::new(true, ROWS)),
    ];
    if crate::util::bench_level() >= 2 {
        let mut additional_inputs = vec![
            Input::new(util::NormalIndexScanNext1024Bencher::<RocksStore>::new()),
            Input::new(util::BatchIndexScanNext1024Bencher::<RocksStore>::new()),
            Input::new(util::NormalIndexScanNext1Bencher::<MemStore>::new()),
            Input::new(util::NormalIndexScanNext1Bencher::<RocksStore>::new()),
            Input::new(util::IndexScanDAGBencher::<MemStore>::new(false, ROWS)),
            Input::new(util::IndexScanDAGBencher::<MemStore>::new(true, ROWS)),
        ];
        inputs.append(&mut additional_inputs);
    }

    let mut cases = vec![
        BenchCase::new("index_scan_primary_key", bench_index_scan_primary_key),
        BenchCase::new("index_scan_index", bench_index_scan_index),
    ];

    cases.sort();
    for case in cases {
        let mut group = c.benchmark_group(case.get_name());
        for input in inputs.iter() {
            group.bench_with_input(
                criterion::BenchmarkId::from_parameter(input),
                input,
                case.get_fn(),
            );
        }
        group.finish();
    }
}
