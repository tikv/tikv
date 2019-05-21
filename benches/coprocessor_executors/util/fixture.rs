// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::str::FromStr;
use std::sync::Arc;

use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_xorshift::XorShiftRng;

use cop_datatype::{FieldTypeAccessor, FieldTypeTp};
use test_coprocessor::*;
use tikv_util::collections::HashMap;
use tipb::expression::FieldType;
use tipb::schema::ColumnInfo;

use tikv::coprocessor::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use tikv::coprocessor::codec::data_type::Decimal;
use tikv::coprocessor::codec::datum::{Datum, DatumEncoder};
use tikv::coprocessor::codec::table::RowColsDict;
use tikv::coprocessor::dag::batch::interface::*;
use tikv::coprocessor::dag::exec_summary::ExecSummary;
use tikv::coprocessor::dag::executor::{Executor, ExecutorMetrics, Row};
use tikv::coprocessor::dag::expr::EvalWarnings;
use tikv::storage::RocksEngine;

use crate::util::bencher::Bencher;

const SEED_1: u64 = 0x525C682A2F7CE3DB;
const SEED_2: u64 = 0xB7CEACC38146676B;
const SEED_3: u64 = 0x2B877E351BD8628E;

#[derive(Clone)]
pub struct FixtureBuilder {
    rows: usize,
    field_types: Vec<FieldType>,
    columns: Vec<Vec<Datum>>,
}

impl FixtureBuilder {
    pub fn new(rows: usize) -> Self {
        Self {
            rows,
            field_types: Vec::new(),
            columns: Vec::new(),
        }
    }

    pub fn push_column_i64_0_n(mut self) -> Self {
        let mut col = Vec::with_capacity(self.rows);
        for i in 0..self.rows {
            col.push(Datum::I64(i as i64));
        }
        self.columns.push(col);
        self.field_types.push(FieldTypeTp::LongLong.into());
        self
    }

    pub fn push_column_i64_random(mut self) -> Self {
        let mut rng: XorShiftRng = SeedableRng::seed_from_u64(SEED_1);
        let mut col = Vec::with_capacity(self.rows);
        for _ in 0..self.rows {
            col.push(Datum::I64(rng.gen()));
        }
        self.columns.push(col);
        self.field_types.push(FieldTypeTp::LongLong.into());
        self
    }

    pub fn push_column_i64_sampled(mut self, samples: &[i64]) -> Self {
        let mut rng: XorShiftRng = SeedableRng::seed_from_u64(SEED_1);
        let mut col = Vec::with_capacity(self.rows);
        for _ in 0..self.rows {
            col.push(Datum::I64(*samples.choose(&mut rng).unwrap()));
        }
        self.columns.push(col);
        self.field_types.push(FieldTypeTp::LongLong.into());
        self
    }

    pub fn push_column_f64_random(mut self) -> Self {
        let mut rng: XorShiftRng = SeedableRng::seed_from_u64(SEED_1);
        let mut col = Vec::with_capacity(self.rows);
        for _ in 0..self.rows {
            col.push(Datum::F64(rng.gen_range(-1e50, 1e50)));
        }
        self.columns.push(col);
        self.field_types.push(FieldTypeTp::Double.into());
        self
    }

    pub fn push_column_f64_sampled(mut self, samples: &[f64]) -> Self {
        let mut rng: XorShiftRng = SeedableRng::seed_from_u64(SEED_1);
        let mut col = Vec::with_capacity(self.rows);
        for _ in 0..self.rows {
            col.push(Datum::F64(*samples.choose(&mut rng).unwrap()));
        }
        self.columns.push(col);
        self.field_types.push(FieldTypeTp::Double.into());
        self
    }

    pub fn push_column_decimal_random(mut self) -> Self {
        let mut rng: XorShiftRng = SeedableRng::seed_from_u64(SEED_2);
        let mut col = Vec::with_capacity(self.rows);
        let mut dec_str = String::new();
        for _ in 0..self.rows {
            dec_str.clear();
            let number_of_int_digits = rng.gen_range(1, 30);
            let number_of_frac_digits = rng.gen_range(1, 20);
            for _ in 0..number_of_int_digits {
                dec_str.push(std::char::from_digit(rng.gen_range(0, 10), 10).unwrap());
            }
            dec_str.push('.');
            for _ in 0..number_of_frac_digits {
                dec_str.push(std::char::from_digit(rng.gen_range(0, 10), 10).unwrap());
            }
            col.push(Datum::Dec(Decimal::from_str(&dec_str).unwrap()));
        }
        self.columns.push(col);
        self.field_types.push(FieldTypeTp::NewDecimal.into());
        self
    }

    pub fn push_column_decimal_sampled(mut self, samples: &[&str]) -> Self {
        let mut rng: XorShiftRng = SeedableRng::seed_from_u64(SEED_2);
        let mut col = Vec::with_capacity(self.rows);
        for _ in 0..self.rows {
            let dec_str = *samples.choose(&mut rng).unwrap();
            col.push(Datum::Dec(Decimal::from_str(dec_str).unwrap()));
        }
        self.columns.push(col);
        self.field_types.push(FieldTypeTp::NewDecimal.into());
        self
    }

    pub fn push_column_bytes_random_fixed_len(mut self, len: usize) -> Self {
        let mut rng: XorShiftRng = SeedableRng::seed_from_u64(SEED_3);
        let mut col = Vec::with_capacity(self.rows);
        for _ in 0..self.rows {
            let str: String = std::iter::repeat(())
                .map(|_| rng.sample(rand::distributions::Alphanumeric))
                .take(len)
                .collect();
            col.push(Datum::Bytes(str.into_bytes()));
        }
        self.columns.push(col);
        self.field_types.push(FieldTypeTp::VarChar.into());
        self
    }

    pub fn build_store(self, table: &Table, columns: &[&str]) -> Store<RocksEngine> {
        assert!(!columns.is_empty());
        assert_eq!(self.columns.len(), columns.len());
        let mut store = Store::new();
        for row_index in 0..self.rows {
            store.begin();
            let mut si = store.insert_into(&table);
            for col_index in 0..columns.len() {
                si = si.set(
                    &table[columns[col_index]],
                    self.columns[col_index][row_index].clone(),
                );
            }
            si.execute();
            store.commit();
        }
        store
    }

    pub fn build_batch_fixture_executor(self) -> BatchFixtureExecutor {
        assert!(!self.columns.is_empty());
        let columns: Vec<_> = self
            .columns
            .into_iter()
            .map(|datums| {
                let mut c = LazyBatchColumn::raw_with_capacity(datums.len());
                for datum in datums {
                    let mut v = vec![];
                    DatumEncoder::encode(&mut v, &[datum], false).unwrap();
                    c.push_raw(v);
                }
                c
            })
            .collect();
        BatchFixtureExecutor {
            schema: self.field_types,
            columns,
        }
    }

    pub fn build_normal_fixture_executor(self) -> NormalFixtureExecutor {
        assert!(!self.columns.is_empty());
        let columns_info: Vec<_> = self
            .field_types
            .into_iter()
            .enumerate()
            .map(|(index, ft)| {
                let mut ci = ColumnInfo::new();
                ci.set_column_id(index as i64);
                ci.as_mut_accessor()
                    .set_tp(ft.tp())
                    .set_flag(ft.flag())
                    .set_flen(ft.flen())
                    .set_decimal(ft.decimal())
                    .set_collation(ft.collation());
                ci
            })
            .collect();
        let columns_info = Arc::new(columns_info);

        let rows_len = self.columns[0].len();
        let mut rows = Vec::with_capacity(rows_len);
        for row_index in 0..rows_len {
            let mut data = RowColsDict::new(HashMap::default(), Vec::new());
            for col_index in 0..self.columns.len() {
                let mut v = vec![];
                DatumEncoder::encode(&mut v, &[self.columns[col_index][row_index].clone()], false)
                    .unwrap();
                data.append(col_index as i64, &mut v);
            }
            rows.push(Row::origin(
                row_index as i64,
                data,
                Arc::clone(&columns_info),
            ));
        }

        NormalFixtureExecutor {
            rows: rows.into_iter(),
            columns: self.columns.len(),
        }
    }
}

pub struct BatchFixtureExecutor {
    schema: Vec<FieldType>,
    columns: Vec<LazyBatchColumn>,
}

impl BatchExecutor for BatchFixtureExecutor {
    #[inline]
    fn schema(&self) -> &[FieldType] {
        &self.schema
    }

    #[inline]
    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        let mut columns = Vec::with_capacity(self.columns.len());
        for col in &mut self.columns {
            let mut column = LazyBatchColumn::raw_with_capacity(scan_rows);
            if col.len() > scan_rows {
                column.mut_raw().extend(col.mut_raw().drain(..scan_rows));
            } else {
                column.mut_raw().append(col.mut_raw());
            }
            columns.push(column);
        }

        BatchExecuteResult {
            data: LazyBatchColumnVec::from(columns),
            warnings: EvalWarnings::default(),
            is_drained: Ok(self.columns[0].is_empty()),
        }
    }

    #[inline]
    fn collect_statistics(&mut self, _destination: &mut BatchExecuteStatistics) {
        // DO NOTHING
    }
}

pub struct NormalFixtureExecutor {
    columns: usize,
    rows: ::std::vec::IntoIter<Row>,
}

impl Executor for NormalFixtureExecutor {
    #[inline]
    fn next(&mut self) -> tikv::coprocessor::Result<Option<Row>> {
        Ok(self.rows.next())
    }

    #[inline]
    fn collect_output_counts(&mut self, _counts: &mut Vec<i64>) {
        // DO NOTHING
    }

    #[inline]
    fn collect_metrics_into(&mut self, _metrics: &mut ExecutorMetrics) {
        // DO NOTHING
    }

    #[inline]
    fn collect_execution_summaries(&mut self, _target: &mut [ExecSummary]) {
        // DO NOTHING
    }

    #[inline]
    fn get_len_of_columns(&self) -> usize {
        self.columns
    }
}

/// Benches the performance of the batch fixture executor itself. When using it as the source
/// executor in other benchmarks, we need to take out these costs.
fn bench_util_batch_fixture_executor_next_1024(b: &mut criterion::Bencher) {
    super::bencher::BatchNext1024Bencher::new(|| {
        FixtureBuilder::new(5000)
            .push_column_i64_random()
            .build_batch_fixture_executor()
    })
    .bench(b);
}

fn bench_util_normal_fixture_executor_next_1024(b: &mut criterion::Bencher) {
    super::bencher::NormalNext1024Bencher::new(|| {
        FixtureBuilder::new(5000)
            .push_column_i64_random()
            .build_normal_fixture_executor()
    })
    .bench(b);
}

/// Checks whether our test utilities themselves are fast enough.
pub fn bench(c: &mut criterion::Criterion) {
    if crate::util::bench_level() >= 1 {
        c.bench_function(
            "util_batch_fixture_executor_next_1024",
            bench_util_batch_fixture_executor_next_1024,
        );
        c.bench_function(
            "util_normal_fixture_executor_next_1024",
            bench_util_normal_fixture_executor_next_1024,
        );
    }
}
