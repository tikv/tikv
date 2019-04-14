// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use smallvec::SmallVec;

use cop_datatype::{FieldTypeAccessor, FieldTypeTp};
use tipb::expression::FieldType;
use tipb::schema::ColumnInfo;

use tikv::coprocessor::codec::datum::{Datum, DatumEncoder};
use tikv::coprocessor::dag::batch::interface::*;
use tikv::coprocessor::dag::executor::{Executor, ExecutorMetrics, Row};

use super::bencher::Bencher;

/// A batch executor that always return 3 encoded columns in type (int, float, string).
pub struct EncodedFixtureBatchExecutor {
    schema: Vec<FieldType>,

    /// The data of each column. For each column, it simply contains a list of encoded datums.
    data: [Vec<SmallVec<[u8; 10]>>; 3],
}

impl EncodedFixtureBatchExecutor {
    pub fn new(rows: usize) -> Self {
        let schema = vec![
            {
                let mut ft = FieldType::new();
                ft.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ft
            },
            {
                let mut ft = FieldType::new();
                ft.as_mut_accessor().set_tp(FieldTypeTp::Double);
                ft
            },
            {
                let mut ft = FieldType::new();
                ft.as_mut_accessor().set_tp(FieldTypeTp::VarChar);
                ft
            },
        ];
        let mut columns_datum = [Vec::new(), Vec::new(), Vec::new()];
        for i in 0..rows {
            columns_datum[0].push(Datum::I64(i as i64));
            columns_datum[1].push(Datum::F64(i as f64));
            columns_datum[2].push(Datum::Bytes(i.to_string().as_bytes().to_vec()));
        }

        let mut data = [Vec::new(), Vec::new(), Vec::new()];
        for column_index in 0..3 {
            for datum in &columns_datum[column_index] {
                let mut v = vec![];
                DatumEncoder::encode(&mut v, &[datum.clone()], false).unwrap();
                data[column_index].push(SmallVec::<[u8; 10]>::from_slice(v.as_slice()));
            }
        }

        Self { schema, data }
    }
}

impl BatchExecutor for EncodedFixtureBatchExecutor {
    #[inline]
    fn schema(&self) -> &[FieldType] {
        &self.schema
    }

    /// Each `next_batch()` takes about 30us.
    #[inline]
    fn next_batch(&mut self, expect_rows: usize) -> BatchExecuteResult {
        use tikv::coprocessor::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
        use tikv::coprocessor::dag::expr::EvalWarnings;

        let mut columns = Vec::with_capacity(3);
        for column_index in 0..3 {
            let mut column = LazyBatchColumn::raw_with_capacity(expect_rows);
            if self.data[column_index].len() > expect_rows {
                column
                    .mut_raw()
                    .extend(self.data[column_index].drain(..expect_rows));
            } else {
                column.mut_raw().append(&mut self.data[column_index]);
            }
            columns.push(column);
        }
        BatchExecuteResult {
            data: LazyBatchColumnVec::from(columns),
            warnings: EvalWarnings::default(),
            is_drained: Ok(self.data[0].is_empty()),
        }
    }

    #[inline]
    fn collect_statistics(&mut self, _destination: &mut BatchExecuteStatistics) {
        // DO NOTHING
    }
}

/// A normal executor that always return 3 encoded columns in type (int, float, string).
pub struct EncodedFixtureNormalExecutor {
    rows: ::std::vec::IntoIter<Row>,
}

impl EncodedFixtureNormalExecutor {
    pub fn new(rows: usize) -> Self {
        use std::sync::Arc;
        use tikv::coprocessor::codec::table::RowColsDict;
        use tikv::util::collections::HashMap;

        let mut rows_datum = Vec::new();
        for i in 0..rows {
            rows_datum.push(vec![
                Datum::I64(i as i64),
                Datum::F64(i as f64),
                Datum::Bytes(i.to_string().as_bytes().to_vec()),
            ]);
        }

        let columns_info = Arc::new(vec![
            {
                let mut ft = ColumnInfo::new();
                ft.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ft.set_column_id(0);
                ft
            },
            {
                let mut ft = ColumnInfo::new();
                ft.as_mut_accessor().set_tp(FieldTypeTp::Double);
                ft.set_column_id(1);
                ft
            },
            {
                let mut ft = ColumnInfo::new();
                ft.as_mut_accessor().set_tp(FieldTypeTp::VarChar);
                ft.set_column_id(2);
                ft
            },
        ]);

        let rows: Vec<_> = rows_datum
            .into_iter()
            .enumerate()
            .map(|(row_index, row_datum)| {
                let mut data = RowColsDict::new(HashMap::default(), Vec::new());
                for column_index in 0..3 {
                    let mut v = vec![];
                    DatumEncoder::encode(&mut v, &[row_datum[column_index].clone()], false)
                        .unwrap();
                    data.append(column_index as i64, &mut v);
                }
                Row::origin(row_index as i64, data, Arc::clone(&columns_info))
            })
            .collect();

        Self {
            rows: rows.into_iter(),
        }
    }
}

impl Executor for EncodedFixtureNormalExecutor {
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
    fn get_len_of_columns(&self) -> usize {
        3
    }
}

/// Benches the performance of the batch fixture executor itself. When using it as the source
/// executor in other benchmarks, we need to take out these costs.
fn bench_util_batch_fixture_executor_next_1024(b: &mut criterion::Bencher) {
    super::bencher::BatchNext1024Bencher::new(|| EncodedFixtureBatchExecutor::new(5000)).bench(b);
}

fn bench_util_normal_fixture_executor_next_1024(b: &mut criterion::Bencher) {
    super::bencher::NormalNext1024Bencher::new(|| EncodedFixtureNormalExecutor::new(5000)).bench(b);
}

/// Checks whether our test utilities themselves are fast enough.
pub fn bench(c: &mut criterion::Criterion) {
    c.bench_function(
        "util_batch_fixture_executor_next_1024",
        bench_util_batch_fixture_executor_next_1024,
    );
    c.bench_function(
        "util_normal_fixture_executor_next_1024",
        bench_util_normal_fixture_executor_next_1024,
    );
}
