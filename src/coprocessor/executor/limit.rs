// Copyright 2017 PingCAP, Inc.
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

// remove later
#![allow(dead_code)]

use tipb::executor::Limit;

use super::{Row, Executor};
use super::super::Result;
use super::super::metrics::*;

struct LimitExecutor<'a> {
    limit: u64,
    cursor: u64,
    src: Box<Executor + 'a>,
}

impl<'a> LimitExecutor<'a> {
    fn new(limit: Limit, src: Box<Executor + 'a>) -> LimitExecutor {
        COPR_EXECUTOR_COUNT.with_label_values(&["limit"]).inc();
        LimitExecutor {
            limit: limit.get_limit(),
            cursor: 0,
            src: src,
        }
    }
}

impl<'a> Executor for LimitExecutor<'a> {
    fn next(&mut self) -> Result<Option<Row>> {
        if self.cursor >= self.limit {
            return Ok(None);
        }
        if let Some(row) = try!(self.src.next()) {
            self.cursor += 1;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use super::super::table_scan::TableScanExecutor;
    use super::super::scanner::test::{TestStore, get_range, new_col_info};
    use super::super::topn::test::gen_table_data;
    use util::codec::mysql::types;
    use util::codec::datum::Datum;
    use storage::Statistics;
    use tipb::executor::TableScan;
    use protobuf::RepeatedField;
    use kvproto::kvrpcpb::IsolationLevel;

    #[test]
    fn test_limit_executor() {
        // prepare data and store
        let tid = 1;
        let cis = vec![new_col_info(1, types::LONG_LONG), new_col_info(2, types::VARCHAR)];
        let raw_data = vec![vec![Datum::I64(1), Datum::Bytes(b"a".to_vec())],
                            vec![Datum::I64(2), Datum::Bytes(b"b".to_vec())],
                            vec![Datum::I64(3), Datum::Bytes(b"c".to_vec())],
                            vec![Datum::I64(4), Datum::Bytes(b"d".to_vec())],
                            vec![Datum::I64(5), Datum::Bytes(b"e".to_vec())],
                            vec![Datum::I64(6), Datum::Bytes(b"f".to_vec())],
                            vec![Datum::I64(7), Datum::Bytes(b"g".to_vec())]];
        let table_data = gen_table_data(tid, &cis, &raw_data);
        let mut test_store = TestStore::new(&table_data);
        // init table scan meta
        let mut table_scan = TableScan::new();
        table_scan.set_table_id(tid);
        table_scan.set_columns(RepeatedField::from_vec(cis.clone()));
        // prepare range
        let range1 = get_range(tid, 0, 4);
        let range2 = get_range(tid, 5, 10);
        let key_ranges = vec![range1, range2];
        // init TableScan
        let (snapshot, start_ts) = test_store.get_snapshot();
        let mut statistics = Statistics::default();
        let ts_ect = TableScanExecutor::new(table_scan,
                                            key_ranges,
                                            snapshot,
                                            &mut statistics,
                                            start_ts,
                                            IsolationLevel::SI);

        // init Limit meta
        let mut limit_meta = Limit::default();
        let limit = 5;
        limit_meta.set_limit(limit);
        // init topn executor
        let mut limit_ect = LimitExecutor::new(limit_meta, Box::new(ts_ect));
        let mut limit_rows = Vec::with_capacity(limit as usize);
        while let Some(row) = limit_ect.next().unwrap() {
            limit_rows.push(row);
        }
        assert_eq!(limit_rows.len(), limit as usize);
        let expect_row_handles = vec![1, 2, 3, 5, 6];
        for (row, handle) in limit_rows.iter().zip(expect_row_handles) {
            assert_eq!(row.handle, handle);
        }
    }
}
