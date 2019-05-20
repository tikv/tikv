// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod util;

use cop_datatype::FieldTypeTp;
use tipb::expression::{ExprType, ScalarFuncSig};
use tipb_helper::ExprDefBuilder;

use crate::util::FixtureBuilder;

/// COUNT(1) GROUP BY COL where COL is a int column.
/// Each row is a new group.
fn bench_hash_aggr_count_1_group_by_int_col(b: &mut criterion::Bencher, input: &Input) {
    let fb = FixtureBuilder::new(input.src_rows).push_column_i64_random();
    let group_by = vec![ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build()];
    let expr = ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
        .push_child(ExprDefBuilder::constant_int(1))
        .build();
    input.bencher.bench(b, &fb, &group_by, &expr);
}

/// COUNT(1) GROUP BY COL where COL is a int column.
/// There will be two groups totally.
fn bench_hash_aggr_count_1_group_by_int_col_2_groups(b: &mut criterion::Bencher, input: &Input) {
    let fb = FixtureBuilder::new(input.src_rows).push_column_i64_sampled(&[0x123456, 0xCCCC]);
    let group_by = vec![ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build()];
    let expr = ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
        .push_child(ExprDefBuilder::constant_int(1))
        .build();
    input.bencher.bench(b, &fb, &group_by, &expr);
}

/// COUNT(1) GROUP BY COL > X.
/// Half of the row belong to one group and the rest belong to another group. Thus there are
/// totally two groups.
fn bench_hash_aggr_count_1_group_by_fn_2_groups(b: &mut criterion::Bencher, input: &Input) {
    let fb = FixtureBuilder::new(input.src_rows).push_column_i64_0_n();
    let group_by = vec![
        ExprDefBuilder::scalar_func(ScalarFuncSig::GTInt, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
            .push_child(ExprDefBuilder::constant_int((input.src_rows / 2) as i64))
            .build(),
    ];
    let expr = ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
        .push_child(ExprDefBuilder::constant_int(1))
        .build();
    input.bencher.bench(b, &fb, &group_by, &expr);
}

/// COUNT(1) GROUP BY COL where COL is a decimal column.
/// Each row is a new group.
fn bench_hash_aggr_count_1_group_by_decimal_col(b: &mut criterion::Bencher, input: &Input) {
    let fb = FixtureBuilder::new(input.src_rows).push_column_decimal_random();
    let group_by = vec![ExprDefBuilder::column_ref(0, FieldTypeTp::NewDecimal).build()];
    let expr = ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
        .push_child(ExprDefBuilder::constant_int(1))
        .build();
    input.bencher.bench(b, &fb, &group_by, &expr);
}

/// COUNT(1) GROUP BY COL where COL is a decimal column.
/// There will be two groups totally.
fn bench_hash_aggr_count_1_group_by_decimal_col_2_groups(
    b: &mut criterion::Bencher,
    input: &Input,
) {
    let fb = FixtureBuilder::new(input.src_rows)
        .push_column_decimal_sampled(&["680644618.9451818", "767257805709854474.824642776567"]);
    let group_by = vec![ExprDefBuilder::column_ref(0, FieldTypeTp::NewDecimal).build()];
    let expr = ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
        .push_child(ExprDefBuilder::constant_int(1))
        .build();
    input.bencher.bench(b, &fb, &group_by, &expr);
}

/// COUNT(1) GROUP BY COL1, COL2 where COL1 is a int column and COL2 is a real column.
/// Each row is a new group.
fn bench_hash_aggr_count_1_group_by_int_col_real_col(b: &mut criterion::Bencher, input: &Input) {
    let fb = FixtureBuilder::new(input.src_rows)
        .push_column_i64_random()
        .push_column_f64_random();
    let group_by = vec![
        ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build(),
        ExprDefBuilder::column_ref(1, FieldTypeTp::Double).build(),
    ];
    let expr = ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
        .push_child(ExprDefBuilder::constant_int(1))
        .build();
    input.bencher.bench(b, &fb, &group_by, &expr);
}

/// COUNT(1) GROUP BY COL1, COL2 where COL1 is a int column and COL2 is a real column.
/// There will be two groups totally.
fn bench_hash_aggr_count_1_group_by_int_col_real_col_2_groups(
    b: &mut criterion::Bencher,
    input: &Input,
) {
    let fb = FixtureBuilder::new(input.src_rows)
        .push_column_i64_sampled(&[0xDEADBEEF, 0xFEE1DEAD])
        .push_column_f64_sampled(&[680644618.9451818]);
    let group_by = vec![
        ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build(),
        ExprDefBuilder::column_ref(1, FieldTypeTp::Double).build(),
    ];
    let expr = ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
        .push_child(ExprDefBuilder::constant_int(1))
        .build();
    input.bencher.bench(b, &fb, &group_by, &expr);
}

#[derive(Clone)]
struct Input {
    /// How many rows to aggregate
    src_rows: usize,

    /// The aggregate executor (batch / normal) to use
    bencher: Box<dyn util::HashAggrBencher>,
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
    let bencher_options: Vec<Box<dyn util::HashAggrBencher>> =
        vec![Box::new(util::NormalBencher), Box::new(util::BatchBencher)];

    for bencher in &bencher_options {
        for rows in &rows_options {
            inputs.push(Input {
                src_rows: *rows,
                bencher: bencher.box_clone(),
            });
        }
    }

    c.bench_function_over_inputs(
        "hash_aggr_count_1_group_by_int_col_2_groups",
        bench_hash_aggr_count_1_group_by_int_col_2_groups,
        inputs.clone(),
    );
    c.bench_function_over_inputs(
        "hash_aggr_count_1_group_by_fn_2_groups",
        bench_hash_aggr_count_1_group_by_fn_2_groups,
        inputs.clone(),
    );
    c.bench_function_over_inputs(
        "hash_aggr_count_1_group_by_decimal_col_2_groups",
        bench_hash_aggr_count_1_group_by_decimal_col_2_groups,
        inputs.clone(),
    );
    c.bench_function_over_inputs(
        "hash_aggr_count_1_group_by_int_col_real_col_2_groups",
        bench_hash_aggr_count_1_group_by_int_col_real_col_2_groups,
        inputs.clone(),
    );
    if crate::util::bench_level() >= 1 {
        c.bench_function_over_inputs(
            "hash_aggr_count_1_group_by_int_col",
            bench_hash_aggr_count_1_group_by_int_col,
            inputs.clone(),
        );
        c.bench_function_over_inputs(
            "hash_aggr_count_1_group_by_decimal_col",
            bench_hash_aggr_count_1_group_by_decimal_col,
            inputs.clone(),
        );
        c.bench_function_over_inputs(
            "hash_aggr_count_1_group_by_int_col_real_col",
            bench_hash_aggr_count_1_group_by_int_col_real_col,
            inputs.clone(),
        );
    }
}
