// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

/// SELECT COUNT(1) FROM Table, or SELECT COUNT(PrimaryKey) FROM Table
fn bench_select_count_1_from_table() {

}

pub fn bench(c: &mut criterion::Criterion) {
    let inputs = vec![
        Input(Box::new(util::NormalTableScanNext1Bencher)),
        Input(Box::new(util::NormalTableScanNext1024Bencher)),
        Input(Box::new(util::BatchTableScanNext1024Bencher)),
        Input(Box::new(util::TableScanDAGBencher { batch: false })),
        Input(Box::new(util::TableScanDAGBencher { batch: true })),
    ];

    c.bench_function_over_inputs(
        "table_scan_primary_key",
        bench_table_scan_primary_key,
        inputs.clone(),
    );
    c.bench_function_over_inputs(
        "table_scan_datum_front",
        bench_table_scan_datum_front,
        inputs.clone(),
    );
    c.bench_function_over_inputs(
        "table_scan_datum_multi_front",
        bench_table_scan_datum_multi_front,
        inputs.clone(),
    );
    c.bench_function_over_inputs(
        "table_scan_datum_end",
        bench_table_scan_datum_end,
        inputs.clone(),
    );
    c.bench_function_over_inputs(
        "table_scan_datum_all",
        bench_table_scan_datum_all,
        inputs.clone(),
    );
    c.bench_function_over_inputs(
        "table_scan_long_datum_primary_key",
        bench_table_scan_long_datum_primary_key,
        inputs.clone(),
    );
    c.bench_function_over_inputs(
        "table_scan_long_datum_normal",
        bench_table_scan_long_datum_normal,
        inputs.clone(),
    );
    c.bench_function_over_inputs(
        "table_scan_long_datum_long",
        bench_table_scan_long_datum_long,
        inputs.clone(),
    );
    c.bench_function_over_inputs(
        "table_scan_long_datum_all",
        bench_table_scan_long_datum_all,
        inputs.clone(),
    );
    c.bench_function_over_inputs(
        "table_scan_datum_absent",
        bench_table_scan_datum_absent,
        inputs.clone(),
    );
    c.bench_function_over_inputs(
        "table_scan_datum_absent_large_row",
        bench_table_scan_datum_absent_large_row,
        inputs.clone(),
    );
    c.bench_function_over_inputs(
        "table_scan_point_range",
        bench_table_scan_point_range,
        inputs.clone(),
    );
}
