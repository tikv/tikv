// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod util;

use criterion::measurement::Measurement;

use tidb_query_datatype::FieldTypeTp;
use tipb::ScalarFuncSig;
use tipb_helper::ExprDefBuilder;

use crate::util::{BenchCase, FixtureBuilder};

/// For SQLs like `WHERE column`.
fn bench_selection_column<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let fb = FixtureBuilder::new(input.src_rows).push_column_i64_random();
    let expr = ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build();
    input.bencher.bench(b, &fb, &[expr]);
}

/// For SQLs like `WHERE a > b`.
fn bench_selection_binary_func_column_column<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let fb = FixtureBuilder::new(input.src_rows)
        .push_column_f64_random()
        .push_column_f64_random();
    let expr = ExprDefBuilder::scalar_func(ScalarFuncSig::GtReal, FieldTypeTp::LongLong)
        .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::Double))
        .push_child(ExprDefBuilder::column_ref(1, FieldTypeTp::Double))
        .build();
    input.bencher.bench(b, &fb, &[expr]);
}

/// For SQLS like `WHERE a > 1`.
fn bench_selection_binary_func_column_constant<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let fb = FixtureBuilder::new(input.src_rows).push_column_f64_random();
    let expr = ExprDefBuilder::scalar_func(ScalarFuncSig::GtReal, FieldTypeTp::LongLong)
        .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::Double))
        .push_child(ExprDefBuilder::constant_real(0.42))
        .build();
    input.bencher.bench(b, &fb, &[expr]);
}

/// For SQLs like `WHERE a > 1 AND b > 2`.
fn bench_selection_multiple_predicate<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let fb = FixtureBuilder::new(input.src_rows)
        .push_column_i64_random()
        .push_column_f64_random();
    let exprs = [
        ExprDefBuilder::scalar_func(ScalarFuncSig::GtReal, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::column_ref(1, FieldTypeTp::Double))
            .push_child(ExprDefBuilder::constant_real(0.63))
            .build(),
        ExprDefBuilder::scalar_func(ScalarFuncSig::LeInt, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
            .push_child(ExprDefBuilder::constant_int(0x10FF10))
            .build(),
    ];
    input.bencher.bench(b, &fb, &exprs);
}

#[derive(Clone)]
struct Input<M>
where
    M: Measurement,
{
    /// How many rows to filter
    src_rows: usize,

    /// The selection executor (batch / normal) to use
    bencher: Box<dyn util::SelectionBencher<M>>,
}

impl<M> std::fmt::Display for Input<M>
where
    M: Measurement,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/rows={}", self.bencher.name(), self.src_rows)
    }
}

pub fn bench<M>(c: &mut criterion::Criterion<M>)
where
    M: Measurement + 'static,
{
    let mut inputs = vec![];

    let mut rows_options = vec![5000];
    if crate::util::bench_level() >= 1 {
        rows_options.push(5);
    }
    if crate::util::bench_level() >= 2 {
        rows_options.push(1);
    }
    let bencher_options: Vec<Box<dyn util::SelectionBencher<M>>> =
        vec![Box::new(util::BatchBencher)];

    for rows in &rows_options {
        for bencher in &bencher_options {
            inputs.push(Input {
                src_rows: *rows,
                bencher: bencher.box_clone(),
            });
        }
    }

    let mut cases = vec![BenchCase::new(
        "selection_binary_func_column_constant",
        bench_selection_binary_func_column_constant,
    )];
    if crate::util::bench_level() >= 1 {
        let mut additional_cases = vec![
            BenchCase::new("selection_column", bench_selection_column),
            BenchCase::new(
                "selection_binary_func_column_column",
                bench_selection_binary_func_column_column,
            ),
            BenchCase::new(
                "selection_multiple_predicate",
                bench_selection_multiple_predicate,
            ),
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
            );
        }
        group.finish();
    }
}
