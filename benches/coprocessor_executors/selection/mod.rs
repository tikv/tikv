use cop_datatype::FieldTypeTp;
use tipb::expression::ScalarFuncSig;
use tipb_helper::ExprDefBuilder;

mod util;

/// For SQLs like `WHERE column`.
fn bench_selection_column(b: &mut criterion::Bencher, input: &Input) {
    let expr = ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build();
    input.bencher.bench(b, &[expr], input.src_rows);
}

/// For SQLs like `WHERE a > b`.
fn bench_selection_binary_func_column_column(b: &mut criterion::Bencher, input: &Input) {
    let expr = ExprDefBuilder::scalar_func(ScalarFuncSig::GTReal, FieldTypeTp::LongLong)
        .push_child(ExprDefBuilder::column_ref(1, FieldTypeTp::Double))
        .push_child(ExprDefBuilder::column_ref(1, FieldTypeTp::Double))
        .build();
    input.bencher.bench(b, &[expr], input.src_rows);
}

/// For SQLS like `WHERE a > 1`.
fn bench_selection_binary_func_column_constant(b: &mut criterion::Bencher, input: &Input) {
    let expr = ExprDefBuilder::scalar_func(ScalarFuncSig::GTReal, FieldTypeTp::LongLong)
        .push_child(ExprDefBuilder::column_ref(1, FieldTypeTp::Double))
        .push_child(ExprDefBuilder::constant_real(55.4))
        .build();
    input.bencher.bench(b, &[expr], input.src_rows);
}

/// For SQLs like `WHERE a > 1 AND b > 2`.
fn bench_selection_multiple_predicate(b: &mut criterion::Bencher, input: &Input) {
    let exprs = [
        ExprDefBuilder::scalar_func(ScalarFuncSig::GTReal, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::column_ref(1, FieldTypeTp::Double))
            .push_child(ExprDefBuilder::constant_real(55.4))
            .build(),
        ExprDefBuilder::scalar_func(ScalarFuncSig::LEInt, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
            .push_child(ExprDefBuilder::constant_int(42))
            .build(),
    ];
    input.bencher.bench(b, &exprs, input.src_rows);
}

#[derive(Clone)]
struct Input {
    /// How many rows to filter
    src_rows: usize,

    /// The selection executor (batch / normal) to use
    bencher: Box<dyn util::SelectionBencher>,
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
    let bencher_options: Vec<Box<dyn util::SelectionBencher>> =
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
        "selection_binary_func_column_constant",
        bench_selection_binary_func_column_constant,
        inputs.clone(),
    );
    if crate::util::bench_level() >= 1 {
        c.bench_function_over_inputs("selection_column", bench_selection_column, inputs.clone());
        c.bench_function_over_inputs(
            "selection_binary_func_column_column",
            bench_selection_binary_func_column_column,
            inputs.clone(),
        );
        c.bench_function_over_inputs(
            "selection_multiple_predicate",
            bench_selection_multiple_predicate,
            inputs.clone(),
        );
    }
}
