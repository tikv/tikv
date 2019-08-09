// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod util;

use tidb_query_datatype::FieldTypeTp;
use tipb::ScalarFuncSig;
use tipb_helper::ExprDefBuilder;

use crate::util::{BenchCase, FixtureBuilder};

fn bench_top_n_1_order_by_impl(
    columns: usize,
    n: usize,
    b: &mut criterion::Bencher,
    input: &Input,
) {
    assert!(columns >= 1);
    assert!(n > 0);
    let mut fb = FixtureBuilder::new(input.src_rows);
    for _ in 0..columns {
        fb = fb.push_column_i64_random();
    }
    let order_by = vec![ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build()];
    input.bencher.bench(b, &fb, &order_by, &[false], n);
}

/// ORDER BY col LIMIT 10. 1 projection field.
fn bench_top_n_1_order_by_1_column_limit_10(b: &mut criterion::Bencher, input: &Input) {
    bench_top_n_1_order_by_impl(1, 10, b, input);
}

/// ORDER BY col LIMIT 4000. 1 projection field.
fn bench_top_n_1_order_by_1_column_limit_4000(b: &mut criterion::Bencher, input: &Input) {
    bench_top_n_1_order_by_impl(1, 4000, b, input);
}

/// ORDER BY col LIMIT 10. 50 projection fields.
fn bench_top_n_1_order_by_50_column_limit_10(b: &mut criterion::Bencher, input: &Input) {
    bench_top_n_1_order_by_impl(50, 10, b, input);
}

/// ORDER BY col LIMIT 4000. 50 projection fields.
fn bench_top_n_1_order_by_50_column_limit_4000(b: &mut criterion::Bencher, input: &Input) {
    bench_top_n_1_order_by_impl(50, 4000, b, input);
}

fn bench_top_n_3_order_by_impl(
    columns: usize,
    n: usize,
    b: &mut criterion::Bencher,
    input: &Input,
) {
    assert!(columns >= 3);
    assert!(n > 0);
    let mut fb = FixtureBuilder::new(input.src_rows);
    for _ in 0..columns {
        fb = fb.push_column_i64_random();
    }
    let order_by = vec![
        ExprDefBuilder::scalar_func(ScalarFuncSig::IntIsNull, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
            .build(),
        ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build(),
        ExprDefBuilder::column_ref(1, FieldTypeTp::LongLong).build(),
    ];
    input
        .bencher
        .bench(b, &fb, &order_by, &[false, false, true], n);
}

/// ORDER BY isnull(col0), col0, col1 DESC LIMIT 10. 3 projection fields.
fn bench_top_n_3_order_by_3_column_limit_10(b: &mut criterion::Bencher, input: &Input) {
    bench_top_n_3_order_by_impl(3, 10, b, input)
}

/// ORDER BY isnull(col0), col0, col1 DESC LIMIT 4000. 3 projection fields.
fn bench_top_n_3_order_by_3_column_limit_4000(b: &mut criterion::Bencher, input: &Input) {
    bench_top_n_3_order_by_impl(3, 4000, b, input)
}

/// ORDER BY isnull(col0), col0, col1 DESC LIMIT 10. 50 projection fields.
fn bench_top_n_3_order_by_50_column_limit_10(b: &mut criterion::Bencher, input: &Input) {
    bench_top_n_3_order_by_impl(50, 10, b, input)
}

/// ORDER BY isnull(col0), col0, col1 DESC LIMIT 4000. 50 projection fields.
fn bench_top_n_3_order_by_50_column_limit_4000(b: &mut criterion::Bencher, input: &Input) {
    bench_top_n_3_order_by_impl(50, 4000, b, input)
}

#[derive(Clone)]
struct Input {
    /// How many rows to sort
    src_rows: usize,

    /// The top n executor (batch / normal) to use
    bencher: Box<dyn util::TopNBencher>,
}

impl std::fmt::Debug for Input {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/rows={}", self.bencher.name(), self.src_rows)
    }
}

pub fn bench(c: &mut criterion::Criterion) {
    let mut inputs = vec![];

    let mut rows_options = vec![5000];
    if crate::util::bench_level() >= 1 {
        rows_options.push(5);
    }
    if crate::util::bench_level() >= 2 {
        rows_options.push(1);
    }
    let bencher_options: Vec<Box<dyn util::TopNBencher>> =
        vec![Box::new(util::NormalBencher), Box::new(util::BatchBencher)];

    for rows in &rows_options {
        for bencher in &bencher_options {
            inputs.push(Input {
                src_rows: *rows,
                bencher: bencher.box_clone(),
            });
        }
    }

    let mut cases = vec![
        BenchCase::new(
            "top_n_3_order_by_3_column_limit_10",
            bench_top_n_3_order_by_3_column_limit_10,
        ),
        BenchCase::new(
            "top_n_3_order_by_3_column_limit_4000",
            bench_top_n_3_order_by_3_column_limit_4000,
        ),
        BenchCase::new(
            "top_n_3_order_by_50_column_limit_10",
            bench_top_n_3_order_by_50_column_limit_10,
        ),
        BenchCase::new(
            "top_n_3_order_by_50_column_limit_4000",
            bench_top_n_3_order_by_50_column_limit_4000,
        ),
    ];
    if crate::util::bench_level() >= 1 {
        let mut additional_cases = vec![
            BenchCase::new(
                "top_n_1_order_by_1_column_limit_10",
                bench_top_n_1_order_by_1_column_limit_10,
            ),
            BenchCase::new(
                "top_n_1_order_by_1_column_limit_4000",
                bench_top_n_1_order_by_1_column_limit_4000,
            ),
            BenchCase::new(
                "top_n_1_order_by_50_column_limit_10",
                bench_top_n_1_order_by_50_column_limit_10,
            ),
            BenchCase::new(
                "top_n_1_order_by_50_column_limit_4000",
                bench_top_n_1_order_by_50_column_limit_4000,
            ),
        ];
        cases.append(&mut additional_cases);
    }

    cases.sort();
    for case in cases {
        c.bench_function_over_inputs(case.name, case.f, inputs.clone());
    }
}
