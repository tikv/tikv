// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod util;

use crate::util::store::*;

/// SELECT COUNT(1) FROM Table, or SELECT COUNT(PrimaryKey) FROM Table
fn bench_select_count_1_from_table(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = crate::table_scan::fixture::table_with_two_columns(input.rows);

    // TODO: Change to use DAGSelect when it no longer place unnecessary columns.
    let executors = &[
        crate::util::executor_descriptor::table_scan(&[table["id"].as_column_info()]),
        crate::util::executor_descriptor::simple_aggregate(
            &crate::util::aggr::create_expr_count_1(),
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
        vec![1, 10, 1024, 5000]
    } else {
        vec![1, 5000]
    };
    let mut bencher_options: Vec<Box<dyn util::IntegratedBencher>> = vec![
        Box::new(util::NormalBencher::<MemStore>::new()),
        Box::new(util::BatchBencher::<MemStore>::new()),
    ];
    if crate::util::use_full_payload() {
        let mut additional_inputs: Vec<Box<dyn util::IntegratedBencher>> = vec![
            Box::new(util::NormalBencher::<RocksStore>::new()),
            Box::new(util::BatchBencher::<RocksStore>::new()),
            Box::new(util::DAGBencher::<MemStore>::new(false)),
            Box::new(util::DAGBencher::<RocksStore>::new(false)),
            Box::new(util::DAGBencher::<MemStore>::new(true)),
            Box::new(util::DAGBencher::<RocksStore>::new(true)),
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
}
