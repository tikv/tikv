// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod util;

use cop_datatype::FieldTypeTp;
use tipb::expression::{ExprType, ScalarFuncSig};
use tipb_helper::ExprDefBuilder;

use crate::util::executor_descriptor::*;
use crate::util::store::*;

/// SELECT COUNT(1) FROM Table, or SELECT COUNT(PrimaryKey) FROM Table
fn bench_select_count_1_from_table(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = crate::table_scan::fixture::table_with_two_columns(input.rows);

    // TODO: Change to use `DAGSelect` helper when it no longer place unnecessary columns.
    let executors = &[
        table_scan(&[table["id"].as_column_info()]),
        simple_aggregate(
            &ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::constant_int(1))
                .build(),
        ),
    ];

    input
        .bencher
        .bench(b, executors, &[table.get_record_range_all()], &store);
}

/// SELECT COUNT(column) FROM Table
fn bench_select_count_column_from_table(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = crate::table_scan::fixture::table_with_two_columns(input.rows);

    let executors = &[
        table_scan(&[table["foo"].as_column_info()]),
        simple_aggregate(
            &ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
                .build(),
        ),
    ];

    input
        .bencher
        .bench(b, executors, &[table.get_record_range_all()], &store);
}

/// SELECT column FROM Table WHERE column
fn bench_select_where_column_from_table(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = crate::table_scan::fixture::table_with_two_columns(input.rows);

    let executors = &[
        table_scan(&[table["foo"].as_column_info()]),
        selection(&[ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build()]),
    ];

    input
        .bencher
        .bench(b, executors, &[table.get_record_range_all()], &store);
}

fn bench_select_where_func_from_table_impl(
    selectivity: f64,
    b: &mut criterion::Bencher,
    input: &Input,
) {
    let (table, store) = crate::table_scan::fixture::table_with_two_columns(input.rows);

    let executors = &[
        table_scan(&[table["foo"].as_column_info()]),
        selection(&[
            ExprDefBuilder::scalar_func(ScalarFuncSig::GTInt, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
                .push_child(ExprDefBuilder::constant_int(
                    (input.rows as f64 * selectivity) as i64,
                ))
                .build(),
        ]),
    ];

    input
        .bencher
        .bench(b, executors, &[table.get_record_range_all()], &store);
}

/// SELECT column FROM Table WHERE column > X (selectivity = 5%)
fn bench_select_where_func_from_table_selectivity_l(b: &mut criterion::Bencher, input: &Input) {
    bench_select_where_func_from_table_impl(0.05, b, input);
}

/// SELECT column FROM Table WHERE column > X (selectivity = 50%)
fn bench_select_where_func_from_table_selectivity_m(b: &mut criterion::Bencher, input: &Input) {
    bench_select_where_func_from_table_impl(0.5, b, input);
}

/// SELECT column FROM Table WHERE column > X (selectivity = 95%)
fn bench_select_where_func_from_table_selectivity_h(b: &mut criterion::Bencher, input: &Input) {
    bench_select_where_func_from_table_impl(0.95, b, input);
}

fn bench_select_count_1_where_func_from_table_impl(
    selectivity: f64,
    b: &mut criterion::Bencher,
    input: &Input,
) {
    let (table, store) = crate::table_scan::fixture::table_with_two_columns(input.rows);

    let executors = &[
        table_scan(&[table["foo"].as_column_info()]),
        selection(&[
            ExprDefBuilder::scalar_func(ScalarFuncSig::GTInt, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
                .push_child(ExprDefBuilder::constant_int(
                    (input.rows as f64 * selectivity) as i64,
                ))
                .build(),
        ]),
        simple_aggregate(
            &ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::constant_int(1))
                .build(),
        ),
    ];

    input
        .bencher
        .bench(b, executors, &[table.get_record_range_all()], &store);
}

/// SELECT COUNT(1) FROM Table WHERE column > X (selectivity = 5%)
fn bench_select_count_1_where_func_from_table_selectivity_l(
    b: &mut criterion::Bencher,
    input: &Input,
) {
    bench_select_count_1_where_func_from_table_impl(0.05, b, input);
}

/// SELECT COUNT(1) FROM Table WHERE column > X (selectivity = 50%)
fn bench_select_count_1_where_func_from_table_selectivity_m(
    b: &mut criterion::Bencher,
    input: &Input,
) {
    bench_select_count_1_where_func_from_table_impl(0.5, b, input);
}

/// SELECT COUNT(1) FROM Table WHERE column > X (selectivity = 95%)
fn bench_select_count_1_where_func_from_table_selectivity_h(
    b: &mut criterion::Bencher,
    input: &Input,
) {
    bench_select_count_1_where_func_from_table_impl(0.95, b, input);
}

#[derive(Clone)]
struct Input {
    rows: usize,
    bencher: Box<dyn util::IntegratedBencher>,
}

impl std::fmt::Debug for Input {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/rows={}", self.bencher.name(), self.rows)
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
    let mut bencher_options: Vec<Box<dyn util::IntegratedBencher>> = vec![
        Box::new(util::DAGBencher::<RocksStore>::new(false)),
        Box::new(util::DAGBencher::<RocksStore>::new(true)),
    ];
    if crate::util::bench_level() >= 2 {
        let mut additional_inputs: Vec<Box<dyn util::IntegratedBencher>> = vec![
            Box::new(util::NormalBencher::<MemStore>::new()),
            Box::new(util::BatchBencher::<MemStore>::new()),
            Box::new(util::NormalBencher::<RocksStore>::new()),
            Box::new(util::BatchBencher::<RocksStore>::new()),
            Box::new(util::DAGBencher::<MemStore>::new(false)),
            Box::new(util::DAGBencher::<MemStore>::new(true)),
        ];
        bencher_options.append(&mut additional_inputs);
    }

    for bencher in &bencher_options {
        for rows in &rows_options {
            inputs.push(Input {
                rows: *rows,
                bencher: bencher.box_clone(),
            });
        }
    }

    c.bench_function_over_inputs(
        "select_count_1_from_table",
        bench_select_count_1_from_table,
        inputs.clone(),
    );
    c.bench_function_over_inputs(
        "select_where_func_from_table_selectivity_m",
        bench_select_where_func_from_table_selectivity_m,
        inputs.clone(),
    );
    c.bench_function_over_inputs(
        "select_count_1_where_func_from_table_selectivity_m",
        bench_select_count_1_where_func_from_table_selectivity_m,
        inputs.clone(),
    );
    if crate::util::bench_level() >= 1 {
        c.bench_function_over_inputs(
            "select_count_column_from_table",
            bench_select_count_column_from_table,
            inputs.clone(),
        );
        c.bench_function_over_inputs(
            "select_where_column_from_table",
            bench_select_where_column_from_table,
            inputs.clone(),
        );
        c.bench_function_over_inputs(
            "select_where_func_from_table_selectivity_l",
            bench_select_where_func_from_table_selectivity_l,
            inputs.clone(),
        );
        c.bench_function_over_inputs(
            "select_where_func_from_table_selectivity_h",
            bench_select_where_func_from_table_selectivity_h,
            inputs.clone(),
        );
        c.bench_function_over_inputs(
            "select_count_1_where_func_from_table_selectivity_l",
            bench_select_count_1_where_func_from_table_selectivity_l,
            inputs.clone(),
        );
        c.bench_function_over_inputs(
            "select_count_1_where_func_from_table_selectivity_h",
            bench_select_count_1_where_func_from_table_selectivity_h,
            inputs.clone(),
        );
    }
}
