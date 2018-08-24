// Copyright 2018 PingCAP, Inc.
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

use coprocessor::codec::datum;
use coprocessor::*;
use kvproto::coprocessor::{KeyRange, Response};
use kvproto::kvrpcpb::IsolationLevel;
use protobuf::{Message as PbMsg, RepeatedField};
use storage::engine::ScanMode;
use storage::{Key, Snapshot, SnapshotStore, StoreScanner};
use tipb::executor::ExecType;
use tipb::expression::ExprType;
use tipb::schema::ColumnInfo;
use tipb::select::{Chunk, DAGRequest, SelectResponse};

pub struct FlatAggExecutor<S: Snapshot + 'static> {
    scanner: StoreScanner<S>,

    col: ColumnInfo,
}

impl<S: Snapshot + 'static> FlatAggExecutor<S> {
    pub fn new(range: &KeyRange, snap: S, start_ts: u64, col: ColumnInfo) -> Self {
        let store = SnapshotStore::new(snap, start_ts, IsolationLevel::SI, true);
        let lower_bound = Some(Key::from_raw(range.get_start()));
        let upper_bound = Some(Key::from_raw(range.get_end()));

        let scanner = store
            .scanner(
                ScanMode::Forward,
                col.get_pk_handle(),
                lower_bound,
                upper_bound,
            )
            .unwrap();
        Self { scanner, col }
    }

    pub fn exec(&mut self) -> Result<Response> {
        let col_id = self.col.get_column_id();
        let count_pk_handle = self.col.get_pk_handle();

        let mut count: i64 = 0;
        while let Some((_key, val)) = self.scanner.next()? {
            if count_pk_handle {
                count += 1;
            } else if let Some(loc) = extract_col_location(&val, col_id)? {
                if val[loc.offset] != datum::NIL_FLAG {
                    count += 1;
                }
            }
        }

        // Construct response
        let mut chunk = Chunk::new();
        chunk
            .mut_rows_data()
            .extend_from_slice(&datum::encode_value(&[datum::Datum::I64(count)])?);
        let mut sel_resp = SelectResponse::new();
        sel_resp.set_chunks(RepeatedField::from_vec(vec![chunk]));
        let mut resp = Response::new();
        let data = box_try!(sel_resp.write_to_bytes());
        resp.set_data(data);
        Ok(resp)
    }
}

struct ColLocation {
    pub id: i64,
    pub offset: usize,
    pub len: usize,
}

impl ColLocation {
    pub fn new(id: i64, offset: usize, len: usize) -> Self {
        Self { id, offset, len }
    }
}

fn extract_col_location(mut data: &[u8], col_id: i64) -> Result<Option<ColLocation>> {
    if data.is_empty() || (data.len() == 1 && data[0] == datum::NIL_FLAG) {
        return Ok(None);
    }

    let length = data.len();
    while !data.is_empty() {
        let id = datum::decode_datum(&mut data)?.i64();
        let offset = length - data.len();
        let (val, rem) = datum::split_datum(data, false)?;
        if id == col_id {
            return Ok(Some(ColLocation::new(id, offset, val.len())));
        }
        data = rem;
    }

    Ok(None)
}

/// Support `select count(*)/count(k) from ...` now.
/// TODO: support sum/avg
pub fn build_flat_agg_from_dag<S: Snapshot + 'static>(
    req: &DAGRequest,
    ranges: &[KeyRange],
    snap: S,
) -> Option<FlatAggExecutor<S>> {
    let executors = req.get_executors();
    if executors.len() != 2 {
        return None;
    }

    if ranges.len() != 1 {
        return None;
    }

    let mut unique = false;
    let mut cols = vec![];
    match executors[0].get_tp() {
        ExecType::TypeTableScan => {
            cols = executors[0].get_tbl_scan().get_columns().to_owned();
            unique = true;
        }
        ExecType::TypeIndexScan => {
            cols = executors[0].get_idx_scan().get_columns().to_owned();
            unique = executors[0].get_idx_scan().get_unique();
        }
        _ => panic!("unknown first executor type {:?}", executors[0].get_tp()),
    }

    if cols.len() != 1 {
        return None;
    }

    //    if !cols[0].get_pk_handle() {
    //        return None;
    //    }

    if !executors[1].has_aggregation() {
        return None;
    }

    let agg = executors[1].get_aggregation();
    let agg_funcs = agg.get_agg_func();
    if agg_funcs.len() != 1 {
        return None;
    }

    if !agg.get_group_by().is_empty() {
        return None;
    }

    if agg_funcs[0].get_tp() != ExprType::Count {
        return None;
    }

    let children = agg_funcs[0].get_children();
    if children.len() != 1 {
        return None;
    }

    //    if children[0].get_tp() != ExprType::Int64 {
    //        return None;
    //    }

    Some(FlatAggExecutor::<S>::new(
        &ranges[0],
        snap,
        req.get_start_ts(),
        cols.into_iter().next().unwrap(),
    ))
}
