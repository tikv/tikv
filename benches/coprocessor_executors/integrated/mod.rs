// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod util;

use crate::util::store::*;

/// SELECT COUNT(1) FROM Table, or SELECT COUNT(PrimaryKey) FROM Table
fn bench_select_count_1_from_table(b: &mut criterion::Bencher, input: &Input) {
    let (table, store) = crate::table_scan::fixture::table_with_two_columns();

    // TODO: Change to use DAGSelect when it no longer place unnecessary columns.
    let executors = &[
        crate::util::executor_descriptor::table_scan(&[table["id"].as_column_info()]),
        crate::util::executor_descriptor::simple_aggregate(
            &crate::util::aggr::create_expr_count_1(),
        ),
    ];

    input
        .0
        .bench(b, executors, &[table.get_record_range_all()], &store);
}

struct Input(Box<dyn util::IntegratedBencher>);

impl Input {
    pub fn new<T: util::IntegratedBencher + 'static>(b: T) -> Self {
        Self(Box::new(b))
    }
}

impl Clone for Input {
    fn clone(&self) -> Self {
        Input(self.0.box_clone())
    }
}

impl std::fmt::Debug for Input {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.name())
    }
}

pub fn bench(c: &mut criterion::Criterion) {
    let mut inputs = vec![
        Input::new(util::NormalBencher::<MemStore>::new()),
        Input::new(util::BatchBencher::<MemStore>::new()),
    ];
    if crate::util::use_full_payload() {
        let mut additional_inputs = vec![
            Input::new(util::NormalBencher::<RocksStore>::new()),
            Input::new(util::BatchBencher::<RocksStore>::new()),
            Input::new(util::DAGBencher::<MemStore>::new(false)),
            Input::new(util::DAGBencher::<RocksStore>::new(false)),
            Input::new(util::DAGBencher::<MemStore>::new(true)),
            Input::new(util::DAGBencher::<RocksStore>::new(true)),
        ];
        inputs.append(&mut additional_inputs);
    }

    c.bench_function_over_inputs(
        "select_count_1_from_table",
        bench_select_count_1_from_table,
        inputs.clone(),
    );
}
