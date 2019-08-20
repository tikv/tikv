// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod fixture;
mod util;

use tidb_query_datatype::FieldTypeTp;
use tipb::{ExprType, ScalarFuncSig};
use tipb_helper::ExprDefBuilder;

use crate::util::executor_descriptor::*;
use crate::util::store::*;
use crate::util::BenchCase;
use test_coprocessor::*;
use tikv::storage::RocksEngine;

/// SELECT COUNT(1) FROM Table, or SELECT COUNT(PrimaryKey) FROM Table
fn bench_select_count_1(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = crate::table_scan::fixture::table_with_2_columns(input.rows);

    // TODO: Change to use `DAGSelect` helper when it no longer place unnecessary columns.
    let executors = &[
        table_scan(&[table["id"].as_column_info()]),
        simple_aggregate(&[
            ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::constant_int(1))
                .build(),
        ]),
    ];

    input
        .bencher
        .bench(b, executors, &[table.get_record_range_all()], &store);
}

/// SELECT COUNT(column) FROM Table
fn bench_select_count_col(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = crate::table_scan::fixture::table_with_2_columns(input.rows);

    let executors = &[
        table_scan(&[table["foo"].as_column_info()]),
        simple_aggregate(&[
            ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
                .build(),
        ]),
    ];

    input
        .bencher
        .bench(b, executors, &[table.get_record_range_all()], &store);
}

/// SELECT column FROM Table WHERE column
fn bench_select_where_col(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = crate::table_scan::fixture::table_with_2_columns(input.rows);

    let executors = &[
        table_scan(&[table["foo"].as_column_info()]),
        selection(&[ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build()]),
    ];

    input
        .bencher
        .bench(b, executors, &[table.get_record_range_all()], &store);
}

fn bench_select_col_where_fn_impl(selectivity: f64, b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = crate::table_scan::fixture::table_with_2_columns(input.rows);

    let executors = &[
        table_scan(&[table["foo"].as_column_info()]),
        selection(&[
            ExprDefBuilder::scalar_func(ScalarFuncSig::GtInt, FieldTypeTp::LongLong)
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
fn bench_select_col_where_fn_sel_l(b: &mut criterion::Bencher, input: &Input) {
    bench_select_col_where_fn_impl(0.05, b, input);
}

/// SELECT column FROM Table WHERE column > X (selectivity = 50%)
fn bench_select_col_where_fn_sel_m(b: &mut criterion::Bencher, input: &Input) {
    bench_select_col_where_fn_impl(0.5, b, input);
}

/// SELECT column FROM Table WHERE column > X (selectivity = 95%)
fn bench_select_col_where_fn_sel_h(b: &mut criterion::Bencher, input: &Input) {
    bench_select_col_where_fn_impl(0.95, b, input);
}

fn bench_select_count_1_where_fn_impl(selectivity: f64, b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = crate::table_scan::fixture::table_with_2_columns(input.rows);

    let executors = &[
        table_scan(&[table["foo"].as_column_info()]),
        selection(&[
            ExprDefBuilder::scalar_func(ScalarFuncSig::GtInt, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
                .push_child(ExprDefBuilder::constant_int(
                    (input.rows as f64 * selectivity) as i64,
                ))
                .build(),
        ]),
        simple_aggregate(&[
            ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::constant_int(1))
                .build(),
        ]),
    ];

    input
        .bencher
        .bench(b, executors, &[table.get_record_range_all()], &store);
}

/// SELECT COUNT(1) FROM Table WHERE column > X (selectivity = 5%)
fn bench_select_count_1_where_fn_sel_l(b: &mut criterion::Bencher, input: &Input) {
    bench_select_count_1_where_fn_impl(0.05, b, input);
}

/// SELECT COUNT(1) FROM Table WHERE column > X (selectivity = 50%)
fn bench_select_count_1_where_fn_sel_m(b: &mut criterion::Bencher, input: &Input) {
    bench_select_count_1_where_fn_impl(0.5, b, input);
}

/// SELECT COUNT(1) FROM Table WHERE column > X (selectivity = 95%)
fn bench_select_count_1_where_fn_sel_h(b: &mut criterion::Bencher, input: &Input) {
    bench_select_count_1_where_fn_impl(0.95, b, input);
}

fn bench_select_count_1_group_by_int_col_impl(
    table: Table,
    store: Store<RocksEngine>,
    b: &mut criterion::Bencher,
    input: &Input,
) {
    let executors = &[
        table_scan(&[table["foo"].as_column_info()]),
        hash_aggregate(
            &[
                ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::constant_int(1))
                    .build(),
            ],
            &[ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build()],
        ),
    ];

    input
        .bencher
        .bench(b, executors, &[table.get_record_range_all()], &store);
}

/// SELECT COUNT(1) FROM Table GROUP BY int_col (2 groups)
fn bench_select_count_1_group_by_int_col_group_few(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = self::fixture::table_with_int_column_two_groups(input.rows);
    bench_select_count_1_group_by_int_col_impl(table, store, b, input);
}

/// SELECT COUNT(1) FROM Table GROUP BY int_col (n groups, n = row_count)
fn bench_select_count_1_group_by_int_col_group_many(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = self::fixture::table_with_int_column_n_groups(input.rows);
    bench_select_count_1_group_by_int_col_impl(table, store, b, input);
}

fn bench_select_count_1_group_by_int_col_stream_impl(
    table: Table,
    store: Store<RocksEngine>,
    b: &mut criterion::Bencher,
    input: &Input,
) {
    let executors = &[
        table_scan(&[table["foo"].as_column_info()]),
        stream_aggregate(
            &[
                ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::constant_int(1))
                    .build(),
            ],
            &[ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build()],
        ),
    ];

    input
        .bencher
        .bench(b, executors, &[table.get_record_range_all()], &store);
}

/// SELECT COUNT(1) FROM Table GROUP BY int_col (2 groups, stream aggregation)
fn bench_select_count_1_group_by_int_col_group_few_stream(
    b: &mut criterion::Bencher,
    input: &Input,
) {
    let (table, store) = self::fixture::table_with_int_column_two_groups_ordered(input.rows);
    bench_select_count_1_group_by_int_col_stream_impl(table, store, b, input);
}

/// SELECT COUNT(1) FROM Table GROUP BY int_col (n groups, n = row_count, stream aggregation)
fn bench_select_count_1_group_by_int_col_group_many_stream(
    b: &mut criterion::Bencher,
    input: &Input,
) {
    let (table, store) = self::fixture::table_with_int_column_n_groups(input.rows);
    bench_select_count_1_group_by_int_col_stream_impl(table, store, b, input);
}

fn bench_select_count_1_group_by_fn_impl(
    table: Table,
    store: Store<RocksEngine>,
    b: &mut criterion::Bencher,
    input: &Input,
) {
    let executors = &[
        table_scan(&[table["foo"].as_column_info()]),
        hash_aggregate(
            &[
                ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::constant_int(1))
                    .build(),
            ],
            &[
                ExprDefBuilder::scalar_func(ScalarFuncSig::PlusInt, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
                    .push_child(ExprDefBuilder::constant_int(1))
                    .build(),
            ],
        ),
    ];

    input
        .bencher
        .bench(b, executors, &[table.get_record_range_all()], &store);
}

/// SELECT COUNT(1) FROM Table GROUP BY int_col + 1 (2 groups)
fn bench_select_count_1_group_by_fn_group_few(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = self::fixture::table_with_int_column_two_groups(input.rows);
    bench_select_count_1_group_by_fn_impl(table, store, b, input);
}

/// SELECT COUNT(1) FROM Table GROUP BY int_col + 1 (n groups, n = row_count)
fn bench_select_count_1_group_by_fn_group_many(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = self::fixture::table_with_int_column_n_groups(input.rows);
    bench_select_count_1_group_by_fn_impl(table, store, b, input);
}

fn bench_select_count_1_group_by_2_col_impl(
    table: Table,
    store: Store<RocksEngine>,
    b: &mut criterion::Bencher,
    input: &Input,
) {
    let executors = &[
        table_scan(&[table["foo"].as_column_info()]),
        hash_aggregate(
            &[
                ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::constant_int(1))
                    .build(),
            ],
            &[
                ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build(),
                ExprDefBuilder::scalar_func(ScalarFuncSig::PlusInt, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
                    .push_child(ExprDefBuilder::constant_int(1))
                    .build(),
            ],
        ),
    ];

    input
        .bencher
        .bench(b, executors, &[table.get_record_range_all()], &store);
}

/// SELECT COUNT(1) FROM Table GROUP BY int_col, int_col + 1 (2 groups)
fn bench_select_count_1_group_by_2_col_group_few(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = self::fixture::table_with_int_column_two_groups(input.rows);
    bench_select_count_1_group_by_2_col_impl(table, store, b, input);
}

/// SELECT COUNT(1) FROM Table GROUP BY int_col, int_col + 1 (n groups, n = row_count)
fn bench_select_count_1_group_by_2_col_group_many(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = self::fixture::table_with_int_column_n_groups(input.rows);
    bench_select_count_1_group_by_2_col_impl(table, store, b, input);
}

fn bench_select_count_1_group_by_2_col_stream_impl(
    table: Table,
    store: Store<RocksEngine>,
    b: &mut criterion::Bencher,
    input: &Input,
) {
    let executors = &[
        table_scan(&[table["foo"].as_column_info()]),
        stream_aggregate(
            &[
                ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::constant_int(1))
                    .build(),
            ],
            &[
                ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build(),
                ExprDefBuilder::scalar_func(ScalarFuncSig::PlusInt, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
                    .push_child(ExprDefBuilder::constant_int(1))
                    .build(),
            ],
        ),
    ];

    input
        .bencher
        .bench(b, executors, &[table.get_record_range_all()], &store);
}

/// SELECT COUNT(1) FROM Table GROUP BY int_col, int_col + 1 (2 groups, stream aggregation)
fn bench_select_count_1_group_by_2_col_group_few_stream(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = self::fixture::table_with_int_column_two_groups_ordered(input.rows);
    bench_select_count_1_group_by_2_col_stream_impl(table, store, b, input);
}

/// SELECT COUNT(1) FROM Table GROUP BY int_col, int_col + 1 (n groups, n = row_count, stream aggregation)
fn bench_select_count_1_group_by_2_col_group_many_stream(
    b: &mut criterion::Bencher,
    input: &Input,
) {
    let (table, store) = self::fixture::table_with_int_column_n_groups(input.rows);
    bench_select_count_1_group_by_2_col_stream_impl(table, store, b, input);
}

/// SELECT COUNT(1) FROM Table WHERE id > X GROUP BY int_col (2 groups, selectivity = 5%)
fn bench_select_count_1_where_fn_group_by_int_col_group_few_sel_l(
    b: &mut criterion::Bencher,
    input: &Input,
) {
    let (table, store) = self::fixture::table_with_int_column_two_groups(input.rows);

    let executors = &[
        table_scan(&[table["id"].as_column_info(), table["foo"].as_column_info()]),
        selection(&[
            ExprDefBuilder::scalar_func(ScalarFuncSig::GtInt, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
                .push_child(ExprDefBuilder::constant_int(
                    (input.rows as f64 * 0.05) as i64,
                ))
                .build(),
        ]),
        hash_aggregate(
            &[
                ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::constant_int(1))
                    .build(),
            ],
            &[ExprDefBuilder::column_ref(1, FieldTypeTp::LongLong).build()],
        ),
    ];

    input
        .bencher
        .bench(b, executors, &[table.get_record_range_all()], &store);
}

/// SELECT COUNT(1) FROM Table WHERE id > X GROUP BY int_col
/// (2 groups, selectivity = 5%, stream aggregation)
fn bench_select_count_1_where_fn_group_by_int_col_group_few_sel_l_stream(
    b: &mut criterion::Bencher,
    input: &Input,
) {
    let (table, store) = self::fixture::table_with_int_column_two_groups_ordered(input.rows);

    let executors = &[
        table_scan(&[table["id"].as_column_info(), table["foo"].as_column_info()]),
        selection(&[
            ExprDefBuilder::scalar_func(ScalarFuncSig::GtInt, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
                .push_child(ExprDefBuilder::constant_int(
                    (input.rows as f64 * 0.05) as i64,
                ))
                .build(),
        ]),
        stream_aggregate(
            &[
                ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::constant_int(1))
                    .build(),
            ],
            &[ExprDefBuilder::column_ref(1, FieldTypeTp::LongLong).build()],
        ),
    ];

    input
        .bencher
        .bench(b, executors, &[table.get_record_range_all()], &store);
}

fn bench_select_order_by_3_col_impl(limit: usize, b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = self::fixture::table_with_3_int_columns_random(input.rows);

    let executors = &[
        table_scan(&[
            table["id"].as_column_info(),
            table["col1"].as_column_info(),
            table["col2"].as_column_info(),
        ]),
        top_n(
            &[
                ExprDefBuilder::scalar_func(ScalarFuncSig::IntIsNull, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::column_ref(1, FieldTypeTp::LongLong))
                    .build(),
                ExprDefBuilder::column_ref(1, FieldTypeTp::LongLong).build(),
                ExprDefBuilder::column_ref(2, FieldTypeTp::LongLong).build(),
            ],
            &[false, false, true],
            limit,
        ),
    ];

    input
        .bencher
        .bench(b, executors, &[table.get_record_range_all()], &store);
}

/// SELECT id, col1, col2 FROM Table ORDER BY isnull(col1), col1, col2 DESC LIMIT 10
fn bench_select_order_by_3_col_limit_small(b: &mut criterion::Bencher, input: &Input) {
    bench_select_order_by_3_col_impl(10, b, input);
}

/// SELECT id, col1, col2 FROM Table ORDER BY isnull(col1), col1, col2 DESC LIMIT 4000
fn bench_select_order_by_3_col_limit_large(b: &mut criterion::Bencher, input: &Input) {
    if input.rows < 4000 {
        // Skipped
        b.iter(|| {});
        return;
    }
    bench_select_order_by_3_col_impl(4000, b, input);
}

fn bench_select_where_fn_order_by_3_col_impl(
    limit: usize,
    b: &mut criterion::Bencher,
    input: &Input,
) {
    let (table, store) = self::fixture::table_with_3_int_columns_random(input.rows);

    let executors = &[
        table_scan(&[
            table["id"].as_column_info(),
            table["col1"].as_column_info(),
            table["col2"].as_column_info(),
        ]),
        selection(&[
            ExprDefBuilder::scalar_func(ScalarFuncSig::GtInt, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
                .push_child(ExprDefBuilder::constant_int(0))
                .build(),
        ]),
        top_n(
            &[
                ExprDefBuilder::scalar_func(ScalarFuncSig::IntIsNull, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::column_ref(1, FieldTypeTp::LongLong))
                    .build(),
                ExprDefBuilder::column_ref(1, FieldTypeTp::LongLong).build(),
                ExprDefBuilder::column_ref(2, FieldTypeTp::LongLong).build(),
            ],
            &[false, false, true],
            limit,
        ),
    ];

    input
        .bencher
        .bench(b, executors, &[table.get_record_range_all()], &store);
}

/// SELECT id, col1, col2 FROM Table WHERE id > X ORDER BY isnull(col1), col1, col2 DESC LIMIT 10
/// (selectivity = 0%)
fn bench_select_where_fn_order_by_3_col_limit_small(b: &mut criterion::Bencher, input: &Input) {
    bench_select_where_fn_order_by_3_col_impl(10, b, input);
}

/// SELECT id, col1, col2 FROM Table WHERE id > X ORDER BY isnull(col1), col1, col2 DESC LIMIT 4000
/// (selectivity = 0%)
fn bench_select_where_fn_order_by_3_col_limit_large(b: &mut criterion::Bencher, input: &Input) {
    if input.rows < 4000 {
        // Skipped
        b.iter(|| {});
        return;
    }
    bench_select_where_fn_order_by_3_col_impl(4000, b, input);
}

fn bench_select_50_col_order_by_1_col_impl(
    limit: usize,
    b: &mut criterion::Bencher,
    input: &Input,
) {
    let (table, store) = crate::table_scan::fixture::table_with_multi_columns(input.rows, 50);

    let executors = &[
        table_scan(&table.columns_info()),
        top_n(
            &[ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build()],
            &[false],
            limit,
        ),
    ];

    input
        .bencher
        .bench(b, executors, &[table.get_record_range_all()], &store);
}

/// SELECT * FROM Table ORDER BY col0 LIMIT 10, there are 50 columns.
fn bench_select_50_col_order_by_1_col_limit_small(b: &mut criterion::Bencher, input: &Input) {
    bench_select_50_col_order_by_1_col_impl(10, b, input);
}

/// SELECT * FROM Table ORDER BY col0 LIMIT 4000, there are 50 columns.
fn bench_select_50_col_order_by_1_col_limit_large(b: &mut criterion::Bencher, input: &Input) {
    if input.rows < 4000 {
        // Skipped
        b.iter(|| {});
        return;
    }
    bench_select_50_col_order_by_1_col_impl(4000, b, input);
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

    for rows in &rows_options {
        for bencher in &bencher_options {
            inputs.push(Input {
                rows: *rows,
                bencher: bencher.box_clone(),
            });
        }
    }

    let mut cases = vec![
        BenchCase::new("select_count_1", bench_select_count_1),
        BenchCase::new("select_col_where_fn_sel_m", bench_select_col_where_fn_sel_m),
        BenchCase::new(
            "select_count_1_where_fn_sel_m",
            bench_select_count_1_where_fn_sel_m,
        ),
        BenchCase::new(
            "select_count_1_group_by_int_col_group_few",
            bench_select_count_1_group_by_int_col_group_few,
        ),
        BenchCase::new(
            "select_count_1_group_by_int_col_group_few_stream",
            bench_select_count_1_group_by_int_col_group_few_stream,
        ),
        BenchCase::new(
            "select_count_1_group_by_2_col_group_few",
            bench_select_count_1_group_by_2_col_group_few,
        ),
        BenchCase::new(
            "select_count_1_group_by_2_col_group_few_stream",
            bench_select_count_1_group_by_2_col_group_few_stream,
        ),
        BenchCase::new(
            "select_count_1_where_fn_group_by_int_col_group_few_sel_l",
            bench_select_count_1_where_fn_group_by_int_col_group_few_sel_l,
        ),
        BenchCase::new(
            "select_count_1_where_fn_group_by_int_col_group_few_sel_l_stream",
            bench_select_count_1_where_fn_group_by_int_col_group_few_sel_l_stream,
        ),
        BenchCase::new(
            "select_order_by_3_col_limit_small",
            bench_select_order_by_3_col_limit_small,
        ),
        BenchCase::new(
            "select_where_fn_order_by_3_col_limit_small",
            bench_select_where_fn_order_by_3_col_limit_small,
        ),
        BenchCase::new(
            "select_50_col_order_by_1_col_limit_small",
            bench_select_50_col_order_by_1_col_limit_small,
        ),
    ];
    if crate::util::bench_level() >= 1 {
        let mut additional_cases = vec![
            BenchCase::new("select_count_col", bench_select_count_col),
            BenchCase::new("select_col_where_fn_sel_l", bench_select_col_where_fn_sel_l),
            BenchCase::new("select_col_where_fn_sel_h", bench_select_col_where_fn_sel_h),
            BenchCase::new(
                "select_count_1_where_fn_sel_l",
                bench_select_count_1_where_fn_sel_l,
            ),
            BenchCase::new(
                "select_count_1_where_fn_sel_h",
                bench_select_count_1_where_fn_sel_h,
            ),
            BenchCase::new(
                "select_count_1_group_by_fn_group_few",
                bench_select_count_1_group_by_fn_group_few,
            ),
            BenchCase::new(
                "select_count_1_group_by_int_col_group_many",
                bench_select_count_1_group_by_int_col_group_many,
            ),
            BenchCase::new(
                "select_count_1_group_by_int_col_group_many_stream",
                bench_select_count_1_group_by_int_col_group_many_stream,
            ),
            BenchCase::new(
                "select_count_1_group_by_fn_group_many",
                bench_select_count_1_group_by_fn_group_many,
            ),
            BenchCase::new(
                "select_count_1_group_by_2_col_group_many",
                bench_select_count_1_group_by_2_col_group_many,
            ),
            BenchCase::new(
                "select_count_1_group_by_2_col_group_many_stream",
                bench_select_count_1_group_by_2_col_group_many_stream,
            ),
            BenchCase::new(
                "select_order_by_3_col_limit_large",
                bench_select_order_by_3_col_limit_large,
            ),
            BenchCase::new(
                "select_where_fn_order_by_3_col_limit_large",
                bench_select_where_fn_order_by_3_col_limit_large,
            ),
            BenchCase::new(
                "select_50_col_order_by_1_col_limit_large",
                bench_select_50_col_order_by_1_col_limit_large,
            ),
        ];
        cases.append(&mut additional_cases);
    }
    if crate::util::bench_level() >= 2 {
        let mut additional_cases = vec![BenchCase::new("select_where_col", bench_select_where_col)];
        cases.append(&mut additional_cases);
    }

    cases.sort();
    for case in cases {
        c.bench_function_over_inputs(case.name, case.f, inputs.clone());
    }
}
