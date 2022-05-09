// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::str::FromStr;

use criterion::measurement::Measurement;
use rand::{seq::SliceRandom, Rng, SeedableRng};
use rand_xorshift::XorShiftRng;
use test_coprocessor::*;
use tidb_query_common::storage::IntervalRange;
use tidb_query_datatype::{
    codec::{
        batch::{LazyBatchColumn, LazyBatchColumnVec},
        data_type::Decimal,
        datum::{Datum, DatumEncoder},
    },
    expr::{EvalContext, EvalWarnings},
    FieldTypeTp,
};
use tidb_query_executors::interface::*;
use tikv::storage::{RocksEngine, Statistics};
use tipb::FieldType;

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

    /// Pushes a i64 column that values are sequentially filled by 0 to n.
    pub fn push_column_i64_0_n(mut self) -> Self {
        let mut col = Vec::with_capacity(self.rows);
        for i in 0..self.rows {
            col.push(Datum::I64(i as i64));
        }
        self.columns.push(col);
        self.field_types.push(FieldTypeTp::LongLong.into());
        self
    }

    /// Pushes a i64 column that values are randomly generated in the i64 range.
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

    /// Pushes a i64 column that values are randomly sampled from the giving values.
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

    /// Pushes a i64 column that values are filled according to the given values in order.
    ///
    /// For example, if 3 values `[a, b, c]` are given, then the first 1/3 values in the column are
    /// `a`, the second 1/3 values are `b` and the last 1/3 values are `c`.
    pub fn push_column_i64_ordered(mut self, samples: &[i64]) -> Self {
        let mut col = Vec::with_capacity(self.rows);
        for i in 0..self.rows {
            let pos = ((i as f64) / (self.rows as f64) * (samples.len() as f64)).floor() as usize;
            col.push(Datum::I64(samples[pos]));
        }
        self.columns.push(col);
        self.field_types.push(FieldTypeTp::LongLong.into());
        self
    }

    /// Pushes a f64 column that values are sequentially filled by 0 to n.
    pub fn push_column_f64_0_n(mut self) -> Self {
        let mut col = Vec::with_capacity(self.rows);
        for i in 0..self.rows {
            col.push(Datum::F64(i as f64));
        }
        self.columns.push(col);
        self.field_types.push(FieldTypeTp::Double.into());
        self
    }

    /// Pushes a f64 column that values are randomly generated in the f64 range.
    ///
    /// Generated values range from -1e50 to 1e50.
    pub fn push_column_f64_random(mut self) -> Self {
        let mut rng: XorShiftRng = SeedableRng::seed_from_u64(SEED_1);
        let mut col = Vec::with_capacity(self.rows);
        for _ in 0..self.rows {
            col.push(Datum::F64(rng.gen_range(-1e50..1e50)));
        }
        self.columns.push(col);
        self.field_types.push(FieldTypeTp::Double.into());
        self
    }

    /// Pushes a f64 column that values are randomly sampled from the giving values.
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

    /// Pushes a f64 column that values are filled according to the given values in order.
    ///
    /// For example, if 3 values `[a, b, c]` are given, then the first 1/3 values in the column are
    /// `a`, the second 1/3 values are `b` and the last 1/3 values are `c`.
    pub fn push_column_f64_ordered(mut self, samples: &[f64]) -> Self {
        let mut col = Vec::with_capacity(self.rows);
        for i in 0..self.rows {
            let pos = ((i as f64) / (self.rows as f64) * (samples.len() as f64)).floor() as usize;
            col.push(Datum::F64(samples[pos]));
        }
        self.columns.push(col);
        self.field_types.push(FieldTypeTp::Double.into());
        self
    }

    /// Pushes a decimal column that values are sequentially filled by 0 to n.
    pub fn push_column_decimal_0_n(mut self) -> Self {
        let mut col = Vec::with_capacity(self.rows);
        for i in 0..self.rows {
            col.push(Datum::Dec(Decimal::from(i as i64)));
        }
        self.columns.push(col);
        self.field_types.push(FieldTypeTp::NewDecimal.into());
        self
    }

    /// Pushes a decimal column that values are randomly generated.
    ///
    /// Generated decimals have 1 to 30 integer digits and 1 to 20 fractional digits.
    pub fn push_column_decimal_random(mut self) -> Self {
        let mut rng: XorShiftRng = SeedableRng::seed_from_u64(SEED_2);
        let mut col = Vec::with_capacity(self.rows);
        let mut dec_str = String::new();
        for _ in 0..self.rows {
            dec_str.clear();
            let number_of_int_digits = rng.gen_range(1..30);
            let number_of_frac_digits = rng.gen_range(1..20);
            for _ in 0..number_of_int_digits {
                dec_str.push(std::char::from_digit(rng.gen_range(0..10), 10).unwrap());
            }
            dec_str.push('.');
            for _ in 0..number_of_frac_digits {
                dec_str.push(std::char::from_digit(rng.gen_range(0..10), 10).unwrap());
            }
            col.push(Datum::Dec(Decimal::from_str(&dec_str).unwrap()));
        }
        self.columns.push(col);
        self.field_types.push(FieldTypeTp::NewDecimal.into());
        self
    }

    /// Pushes a decimal column that values are randomly sampled from the giving values.
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

    /// Pushes a decimal column that values are filled according to the given values in order.
    ///
    /// For example, if 3 values `[a, b, c]` are given, then the first 1/3 values in the column are
    /// `a`, the second 1/3 values are `b` and the last 1/3 values are `c`.
    pub fn push_column_decimal_ordered(mut self, samples: &[&str]) -> Self {
        let mut col = Vec::with_capacity(self.rows);
        for i in 0..self.rows {
            let pos = ((i as f64) / (self.rows as f64) * (samples.len() as f64)).floor() as usize;
            let dec_str = samples[pos];
            col.push(Datum::Dec(Decimal::from_str(dec_str).unwrap()));
        }
        self.columns.push(col);
        self.field_types.push(FieldTypeTp::NewDecimal.into());
        self
    }

    /// Pushes a bytes column that values are randomly generated and each value has the same length
    /// as specified.
    pub fn push_column_bytes_random_fixed_len(mut self, len: usize) -> Self {
        let mut rng: XorShiftRng = SeedableRng::seed_from_u64(SEED_3);
        let mut col = Vec::with_capacity(self.rows);
        for _ in 0..self.rows {
            let bytes: Vec<u8> = std::iter::repeat(())
                .map(|_| rng.sample(rand::distributions::Alphanumeric))
                .take(len)
                .collect();
            col.push(Datum::Bytes(bytes));
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
            let mut si = store.insert_into(table);
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
        let mut ctx = EvalContext::default();
        let columns: Vec<_> = self
            .columns
            .into_iter()
            .map(|datums| {
                let mut c = LazyBatchColumn::raw_with_capacity(datums.len());
                for datum in datums {
                    let mut v = vec![];
                    v.write_datum(&mut ctx, &[datum], false).unwrap();
                    c.mut_raw().push(v);
                }
                c
            })
            .collect();
        BatchFixtureExecutor {
            schema: self.field_types,
            columns,
        }
    }
}

pub struct BatchFixtureExecutor {
    schema: Vec<FieldType>,
    columns: Vec<LazyBatchColumn>,
}

impl BatchExecutor for BatchFixtureExecutor {
    type StorageStats = Statistics;

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
                column.mut_raw().copy_n_from(col.raw(), scan_rows);
                col.mut_raw().shift(scan_rows);
            } else {
                column.mut_raw().copy_from(col.raw());
                col.mut_raw().clear();
            }
            columns.push(column);
        }

        let physical_columns = LazyBatchColumnVec::from(columns);
        let logical_rows = (0..physical_columns.rows_len()).collect();
        BatchExecuteResult {
            physical_columns,
            logical_rows,
            warnings: EvalWarnings::default(),
            is_drained: Ok(self.columns[0].is_empty()),
        }
    }

    #[inline]
    fn collect_exec_stats(&mut self, _dest: &mut ExecuteStats) {
        // Do nothing
    }

    #[inline]
    fn collect_storage_stats(&mut self, _dest: &mut Self::StorageStats) {
        // Do nothing
    }

    #[inline]
    fn take_scanned_range(&mut self) -> IntervalRange {
        unreachable!()
    }

    #[inline]
    fn can_be_cached(&self) -> bool {
        unreachable!()
    }
}

/// Benches the performance of the batch fixture executor itself. When using it as the source
/// executor in other benchmarks, we need to take out these costs.
fn bench_util_batch_fixture_executor_next_1024<M>(b: &mut criterion::Bencher<'_, M>)
where
    M: Measurement,
{
    super::bencher::BatchNext1024Bencher::new(|| {
        FixtureBuilder::new(5000)
            .push_column_i64_random()
            .build_batch_fixture_executor()
    })
    .bench(b);
}

/// Checks whether our test utilities themselves are fast enough.
pub fn bench<M>(c: &mut criterion::Criterion<M>)
where
    M: Measurement + 'static,
{
    if crate::util::bench_level() >= 1 {
        c.bench_function(
            "util_batch_fixture_executor_next_1024",
            bench_util_batch_fixture_executor_next_1024::<M>,
        );
    }
}
