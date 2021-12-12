// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod util;

use criterion::measurement::Measurement;

use tidb_query_datatype::FieldTypeTp;
use tipb::ExprType;
use tipb_helper::ExprDefBuilder;

use crate::util::{BenchCase, FixtureBuilder};

/// COUNT(1) GROUP BY COL where COL is a int column.
/// Each row is a new group.
fn bench_stream_aggr_count_1_group_by_int_col<M>(b: &mut criterion::Bencher<M>, input: &Input<M>)
where
    M: Measurement,
{
    let fb = FixtureBuilder::new(input.src_rows).push_column_i64_0_n();
    let group_by = vec![ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build()];
    let expr = ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
        .push_child(ExprDefBuilder::constant_int(1))
        .build();
    input.bencher.bench(b, &fb, &group_by, &[expr]);
}

/// COUNT(1) GROUP BY COL where COL is a int column.
/// There will be two groups totally.
fn bench_stream_aggr_count_1_group_by_int_col_2_groups<M>(
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let fb = FixtureBuilder::new(input.src_rows).push_column_i64_ordered(&[0x123456, 0xCCCC]);
    let group_by = vec![ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build()];
    let expr = ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
        .push_child(ExprDefBuilder::constant_int(1))
        .build();
    input.bencher.bench(b, &fb, &group_by, &[expr]);
}

/// COUNT(1) GROUP BY COL where COL is a decimal column.
/// Each row is a new group.
fn bench_stream_aggr_count_1_group_by_decimal_col<M>(
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let fb = FixtureBuilder::new(input.src_rows).push_column_decimal_0_n();
    let group_by = vec![ExprDefBuilder::column_ref(0, FieldTypeTp::NewDecimal).build()];
    let expr = ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
        .push_child(ExprDefBuilder::constant_int(1))
        .build();
    input.bencher.bench(b, &fb, &group_by, &[expr]);
}

/// COUNT(1) GROUP BY COL where COL is a decimal column.
/// There will be two groups totally.
fn bench_stream_aggr_count_1_group_by_decimal_col_2_groups<M>(
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let fb = FixtureBuilder::new(input.src_rows)
        .push_column_decimal_ordered(&["680644618.9451818", "767257805709854474.824642776567"]);
    let group_by = vec![ExprDefBuilder::column_ref(0, FieldTypeTp::NewDecimal).build()];
    let expr = ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
        .push_child(ExprDefBuilder::constant_int(1))
        .build();
    input.bencher.bench(b, &fb, &group_by, &[expr]);
}

/// COUNT(1) GROUP BY COL1, COL2 where COL1 is a int column and COL2 is a real column.
/// Each row is a new group.
fn bench_stream_aggr_count_1_group_by_int_col_real_col<M>(
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let fb = FixtureBuilder::new(input.src_rows)
        .push_column_i64_0_n()
        .push_column_f64_0_n();
    let group_by = vec![
        ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build(),
        ExprDefBuilder::column_ref(1, FieldTypeTp::Double).build(),
    ];
    let expr = ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
        .push_child(ExprDefBuilder::constant_int(1))
        .build();
    input.bencher.bench(b, &fb, &group_by, &[expr]);
}

/// COUNT(1) GROUP BY COL1, COL2 where COL1 is a int column and COL2 is a real column.
/// There will be two groups totally.
fn bench_stream_aggr_count_1_group_by_int_col_real_col_2_groups<M>(
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let fb = FixtureBuilder::new(input.src_rows)
        .push_column_i64_ordered(&[0xDEADBEEF, 0xFEE1DEAD])
        .push_column_f64_ordered(&[680644618.9451818]);
    let group_by = vec![
        ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build(),
        ExprDefBuilder::column_ref(1, FieldTypeTp::Double).build(),
    ];
    let expr = ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
        .push_child(ExprDefBuilder::constant_int(1))
        .build();
    input.bencher.bench(b, &fb, &group_by, &[expr]);
}

/// COUNT(1), FIRST(COL3) GROUP BY COL1, COL2 where COL1 is a int column and
/// COL2, COL3 are real columns. Each row is a new group.
fn bench_stream_aggr_count_1_first_group_by_int_col_real_col<M>(
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let fb = FixtureBuilder::new(input.src_rows)
        .push_column_i64_0_n()
        .push_column_f64_0_n()
        .push_column_f64_random();
    let group_by = vec![
        ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build(),
        ExprDefBuilder::column_ref(1, FieldTypeTp::Double).build(),
    ];
    let expr = [
        ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::constant_int(1))
            .build(),
        ExprDefBuilder::aggr_func(ExprType::First, FieldTypeTp::Double)
            .push_child(ExprDefBuilder::column_ref(2, FieldTypeTp::Double))
            .build(),
    ];
    input.bencher.bench(b, &fb, &group_by, &expr);
}

/// COUNT(1), FIRST(COL3) GROUP BY COL1, COL2 where COL1 is a int column and
/// COL2, COL3 are real columns. There will be two groups totally.
fn bench_stream_aggr_count_1_first_group_by_int_col_real_col_2_groups<M>(
    b: &mut criterion::Bencher<M>,
    input: &Input<M>,
) where
    M: Measurement,
{
    let fb = FixtureBuilder::new(input.src_rows)
        .push_column_i64_ordered(&[0xDEADBEEF, 0xFEE1DEAD])
        .push_column_f64_ordered(&[680644618.9451818])
        .push_column_f64_random();
    let group_by = vec![
        ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build(),
        ExprDefBuilder::column_ref(1, FieldTypeTp::Double).build(),
    ];
    let expr = [
        ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::constant_int(1))
            .build(),
        ExprDefBuilder::aggr_func(ExprType::First, FieldTypeTp::Double)
            .push_child(ExprDefBuilder::column_ref(2, FieldTypeTp::Double))
            .build(),
    ];
    input.bencher.bench(b, &fb, &group_by, &expr);
}

#[derive(Clone)]
struct Input<M>
where
    M: Measurement,
{
    /// How many rows to aggregate
    src_rows: usize,

    /// The aggregate executor (batch / normal) to use
    bencher: Box<dyn util::StreamAggrBencher<M>>,
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
    let bencher_options: Vec<Box<dyn util::StreamAggrBencher<M>>> =
        vec![Box::new(util::BatchBencher)];

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
            "stream_aggr_count_1_group_by_int_col_2_groups",
            bench_stream_aggr_count_1_group_by_int_col_2_groups,
        ),
        BenchCase::new(
            "stream_aggr_count_1_group_by_decimal_col_2_groups",
            bench_stream_aggr_count_1_group_by_decimal_col_2_groups,
        ),
        BenchCase::new(
            "stream_aggr_count_1_group_by_int_col_real_col_2_groups",
            bench_stream_aggr_count_1_group_by_int_col_real_col_2_groups,
        ),
        BenchCase::new(
            "stream_aggr_count_1_first_group_by_int_col_real_col_2_groups",
            bench_stream_aggr_count_1_first_group_by_int_col_real_col_2_groups,
        ),
    ];
    if crate::util::bench_level() >= 1 {
        let mut additional_cases = vec![
            BenchCase::new(
                "stream_aggr_count_1_group_by_int_col",
                bench_stream_aggr_count_1_group_by_int_col,
            ),
            BenchCase::new(
                "stream_aggr_count_1_group_by_decimal_col",
                bench_stream_aggr_count_1_group_by_decimal_col,
            ),
            BenchCase::new(
                "stream_aggr_count_1_group_by_int_col_real_col",
                bench_stream_aggr_count_1_group_by_int_col_real_col,
            ),
            BenchCase::new(
                "stream_aggr_count_1_first_group_by_int_col_real_col",
                bench_stream_aggr_count_1_first_group_by_int_col_real_col,
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
