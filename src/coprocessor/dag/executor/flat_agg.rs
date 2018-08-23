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

use super::Executor;
use coprocessor::codec::datum;
use coprocessor::*;
use kvproto::coprocessor::{KeyRange, Response};
use kvproto::kvrpcpb::IsolationLevel;
use protobuf::{Message as PbMsg, RepeatedField};
use storage::engine::ScanMode;
use storage::{Iterator, Key, Snapshot, SnapshotStore, StoreScanner};
use tipb::executor::ExecType;
use tipb::expression::ExprType;
use tipb::select::{Chunk, DAGRequest, SelectResponse};

pub struct FlatAggExecutor<S: Snapshot + 'static> {
    store: SnapshotStore<S>,
    scanner: StoreScanner<S>,
}

impl<S: Snapshot + 'static> FlatAggExecutor<S> {
    pub fn new(range: &KeyRange, snap: S, start_ts: u64) -> Self {
        let store = SnapshotStore::new(snap, start_ts, IsolationLevel::SI, true);
        let lower_bound = Some(Key::from_raw(range.get_start()));
        let upper_bound = Some(Key::from_raw(range.get_end()));
        let scanner = store
            .scanner(ScanMode::Forward, true, lower_bound, upper_bound)
            .unwrap();
        Self { store, scanner }
    }
    pub fn exec(&mut self) -> Result<Response> {
        let mut count: i64 = 0;
        while let Some((_k, _v)) = self.scanner.next()? {
            count += 1;
        }
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
/// Only support `select count(*) from ...` now.
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
    if !cols[0].get_pk_handle() {
        return None;
    }
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
    if children[0].get_tp() != ExprType::Int64 {
        return None;
    }
    Some(FlatAggExecutor::<S>::new(
        &ranges[0],
        snap,
        req.get_start_ts(),
    ))
}
