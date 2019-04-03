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

use super::interface::*;
use crate::coprocessor::Result;
use tipb::expression::FieldType;

pub struct BatchLimitExecutor<Src: BatchExecutor> {
    src: Src,
    expect_rows: usize,
}

impl<Src: BatchExecutor> BatchLimitExecutor<Src> {
    pub fn new(src: Src, limit: usize) -> Result<Self> {
        if limit == 0 {
            return Err(box_err!("limit should not be zero"));
        }
        Ok(Self {
            src,
            expect_rows: limit,
        })
    }
}

impl<Src: BatchExecutor> BatchExecutor for BatchLimitExecutor<Src> {
    #[inline]
    fn next_batch(&mut self, expect_rows: usize) -> BatchExecuteResult {
        let mut result = self.src.next_batch(expect_rows);
        if result.data.rows_len() <= self.expect_rows {
            self.expect_rows -= result.data.rows_len();
            return result;
        }

        result.data.truncate(self.expect_rows);
        self.expect_rows = 0;
        result.is_drained = Ok(true);
        result
    }

    #[inline]
    fn collect_statistics(&mut self, destination: &mut BatchExecuteStatistics) {
        self.src.collect_statistics(destination);
    }

    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.src.schema()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coprocessor::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
    use crate::coprocessor::codec::data_type::VectorValue;
    use crate::coprocessor::dag::expr::EvalConfig;
    use cop_datatype::{EvalType, FieldTypeAccessor, FieldTypeTp};
    use tipb::expression::FieldType;

    struct MockExecutor {
        // ID(INT,PK), Foo(INT), Bar(Float,Default 4.5)
        data: Vec<(i64, Option<i64>, Option<f64>)>,
        field_types: Vec<FieldType>,
        offset: usize,
        cfg: EvalConfig,
    }

    fn field_type(ft: FieldTypeTp) -> FieldType {
        let mut f = FieldType::new();
        f.as_mut_accessor().set_tp(ft);
        f
    }

    impl MockExecutor {
        /// create the MockExecutor with fixed schema and data.
        fn new() -> MockExecutor {
            let expect_rows = vec![
                (1, Some(10), Some(5.2)),
                (3, Some(-5), None),
                (4, None, Some(4.5)),
                (5, None, Some(0.1)),
                (6, None, Some(4.5)),
            ];

            let field_types = vec![
                field_type(FieldTypeTp::LongLong),
                field_type(FieldTypeTp::LongLong),
                field_type(FieldTypeTp::Double),
            ];

            MockExecutor {
                data: expect_rows,
                field_types,
                offset: 0,
                cfg: EvalConfig::default(),
            }
        }
    }

    impl BatchExecutor for MockExecutor {
        #[inline]
        fn next_batch(&mut self, expect_rows: usize) -> BatchExecuteResult {
            let upper = if expect_rows + self.offset > self.data.len() {
                self.data.len()
            } else {
                expect_rows + self.offset
            };
            if upper == self.offset {
                // return error;//no data
            }
            let mut pks = VectorValue::with_capacity(self.data.len(), EvalType::Int);
            let mut foos = VectorValue::with_capacity(self.data.len(), EvalType::Int);
            let mut bars = VectorValue::with_capacity(self.data.len(), EvalType::Real);
            for id in self.offset..upper {
                let (handle, foo, bar) = self.data[id];
                pks.push_int(Some(handle));
                foos.push_int(foo);
                bars.push_real(bar);
            }
            self.offset = upper;
            BatchExecuteResult {
                data: LazyBatchColumnVec::from(vec![
                    LazyBatchColumn::Decoded(pks),
                    LazyBatchColumn::Decoded(foos),
                    LazyBatchColumn::Decoded(bars),
                ]),
                warnings: self.cfg.new_eval_warnings(),
                is_drained: Ok(self.offset == self.data.len()),
            }
        }

        #[inline]
        fn collect_statistics(&mut self, _: &mut BatchExecuteStatistics) {}

        #[inline]
        fn schema(&self) -> &[FieldType] {
            self.field_types.as_ref()
        }
    }

    #[test]
    fn test_basic_limit_normal() {
        let src = MockExecutor::new();
        // we have 5 rows in table and the limit is 4
        let limit_num = 4;
        let mut limit = BatchLimitExecutor::new(src, limit_num).unwrap();
        let expect_rows = 3;
        // first we get 3 rows, and 2 rows left in the executor.
        let res = limit.next_batch(expect_rows);
        assert!(!res.is_drained.as_ref().unwrap());
        assert_eq!(res.data.rows_len(), expect_rows);
        // then we get the left rows, we will get 1 since limit is 4.
        let res = limit.next_batch(expect_rows);
        assert!(res.is_drained.as_ref().unwrap());
        assert_eq!(res.data.rows_len(), limit_num - expect_rows);
    }

    #[test]
    fn test_invalid_limit() {
        let src = MockExecutor::new();
        assert!(BatchLimitExecutor::new(src, 0).is_err());
    }

    /// MockErrExecutor is based on MockExecutor, the only difference is
    /// that when call the function next_batch, it always returns is_drained error.
    struct MockErrExecutor(MockExecutor);
    impl Default for MockErrExecutor {
        fn default() -> Self {
            MockErrExecutor(MockExecutor::new())
        }
    }
    impl BatchExecutor for MockErrExecutor {
        #[inline]
        fn next_batch(&mut self, expect_rows: usize) -> BatchExecuteResult {
            let mut result = self.0.next_batch(expect_rows);
            result.is_drained = Err(box_err!("next batch mock error"));
            result
        }

        #[inline]
        fn collect_statistics(&mut self, _: &mut BatchExecuteStatistics) {}

        #[inline]
        fn schema(&self) -> &[FieldType] {
            self.0.schema()
        }
    }

    #[test]
    fn test_next_batch_err_after_last_row() {
        let src = MockErrExecutor::default();
        let mut limit = BatchLimitExecutor::new(src, 1).unwrap();
        let res = limit.next_batch(10);
        assert!(res.is_drained.is_ok());
        assert!(res.is_drained.as_ref().unwrap());
        assert_eq!(res.data.rows_len(), 1);
    }

    #[test]
    fn test_next_batch_err_before_last_row() {
        let src = MockErrExecutor::default();
        let mut limit = BatchLimitExecutor::new(src, 3).unwrap();
        let res = limit.next_batch(1);
        assert!(res.is_drained.is_err());
    }
}
