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
    remaining_rows: usize,
}

impl<Src: BatchExecutor> BatchLimitExecutor<Src> {
    pub fn new(src: Src, limit: usize) -> Result<Self> {
        if limit == 0 {
            return Err(box_err!("limit should not be zero"));
        }
        Ok(Self {
            src,
            remaining_rows: limit,
        })
    }
}

impl<Src: BatchExecutor> BatchExecutor for BatchLimitExecutor<Src> {
    #[inline]
    fn next_batch(&mut self, expect_rows: usize) -> BatchExecuteResult {
        let mut result = self.src.next_batch(expect_rows);
        if result.data.rows_len() < self.remaining_rows {
            self.remaining_rows -= result.data.rows_len();
            return result;
        }

        result.data.truncate(self.remaining_rows);
        self.remaining_rows = 0;
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
        // when is true, will return 0 rows for next batch, while it
        // will scan next batch after current call of next_batch
        pub zero_rows_for_next_batch: bool,
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
                zero_rows_for_next_batch: false,
            }
        }
    }

    impl BatchExecutor for MockExecutor {
        #[inline]
        fn next_batch(&mut self, expect_rows: usize) -> BatchExecuteResult {
            let upper = if self.zero_rows_for_next_batch {
                self.zero_rows_for_next_batch = false;
                // return zero rows
                self.offset
            } else if expect_rows + self.offset > self.data.len() {
                self.data.len()
            } else {
                expect_rows + self.offset
            };
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
    fn test_limit_normal() {
        let data = vec![
            //limit, expect_rows(real get rows)
            (4, 3), // get less than limit
            (3, 4), // get more than limit
            (3, 3), // get equals to limit
        ];
        for (limit, get_rows) in data {
            let src = MockExecutor::new();
            let mut executor = BatchLimitExecutor::new(src, limit).unwrap();
            let res = executor.next_batch(get_rows);
            if limit <= get_rows {
                // is drained
                assert!(res.is_drained.as_ref().unwrap());
                assert_eq!(res.data.rows_len(), limit);
            } else {
                assert!(!res.is_drained.as_ref().unwrap());
                assert_eq!(res.data.rows_len(), get_rows);
            }
        }
    }

    #[test]
    fn test_limit_normal_zero_rows() {
        let data = vec![
            //limit, expect_rows(real get rows)
            (4, 3), // get less than limit
            (3, 4), // get more than limit
            (3, 3), // get equals to limit
        ];
        for (limit, get_rows) in data {
            let mut src = MockExecutor::new();
            src.zero_rows_for_next_batch = true;
            let mut executor = BatchLimitExecutor::new(src, limit).unwrap();
            // it will return 0 row since src returns 0 row.
            let res = executor.next_batch(get_rows);
            assert!(!res.is_drained.as_ref().unwrap());
            assert_eq!(res.data.rows_len(), 0);
            let res = executor.next_batch(get_rows);
            if limit <= get_rows {
                // is drained
                assert!(res.is_drained.as_ref().unwrap());
                assert_eq!(res.data.rows_len(), limit);
            } else {
                assert!(!res.is_drained.as_ref().unwrap());
                assert_eq!(res.data.rows_len(), get_rows);
            }
        }
    }

    #[test]
    fn test_invalid_limit() {
        let src = MockExecutor::new();
        assert!(BatchLimitExecutor::new(src, 0).is_err());
    }

    /// MockErrExecutor is based on MockExecutor, the only difference is
    /// that when call the function next_batch, it always returns is_drained error.
    struct MockErrExecutor(MockExecutor);
    impl MockErrExecutor {
        fn new() -> Self {
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
    fn test_src_next_batch_err() {
        let data = vec![
            //limit, expect_rows(real get rows)
            (4, 3), // error happens before limit rows
            (3, 4), // error happens after limit rows
            (3, 3), // error happens when get the limit + 1 row.
        ];
        for (limit, get_rows) in data {
            let src = MockErrExecutor::new();
            let mut executor = BatchLimitExecutor::new(src, limit).unwrap();
            let res = executor.next_batch(get_rows);
            if limit <= get_rows {
                // error happens after limit rows
                assert!(res.is_drained.as_ref().unwrap());
                assert_eq!(res.data.rows_len(), limit);
            } else {
                // error happens before limit rows
                assert!(res.is_drained.as_ref().is_err());
                assert_eq!(res.data.rows_len(), get_rows);
            }
        }
    }
}
