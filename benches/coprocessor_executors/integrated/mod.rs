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

/// SELECT column FROM Table WHERE column > 42
fn bench_select_where_func_from_table(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = crate::table_scan::fixture::table_with_two_columns(input.rows);

    let executors = &[
        table_scan(&[table["foo"].as_column_info()]),
        selection(&[
            ExprDefBuilder::scalar_func(ScalarFuncSig::GTInt, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
                .push_child(ExprDefBuilder::constant_int(42))
                .build(),
        ]),
    ];

    input
        .bencher
        .bench(b, executors, &[table.get_record_range_all()], &store);
}

/// SELECT COUNT(1) FROM Table WHERE column > 42
fn bench_select_count_where_func_from_table(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = crate::table_scan::fixture::table_with_two_columns(input.rows);

    let executors = &[
        table_scan(&[table["foo"].as_column_info()]),
        selection(&[
            ExprDefBuilder::scalar_func(ScalarFuncSig::GTInt, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
                .push_child(ExprDefBuilder::constant_int(42))
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

    let rows_options = if crate::util::use_full_payload() {
        vec![1, 10, 5000]
    } else {
        vec![5000]
    };
    let mut bencher_options: Vec<Box<dyn util::IntegratedBencher>> = vec![
        Box::new(util::DAGBencher::<RocksStore>::new(false)),
        Box::new(util::DAGBencher::<RocksStore>::new(true)),
    ];
    if crate::util::use_full_payload() {
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
        "select_where_func_from_table",
        bench_select_where_func_from_table,
        inputs.clone(),
    );
    c.bench_function_over_inputs(
        "select_count_where_func_from_table",
        bench_select_count_where_func_from_table,
        inputs.clone(),
    );
    if crate::util::use_full_payload() {
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
    }
}
