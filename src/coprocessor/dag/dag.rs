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

use std::rc::Rc;
use std::time::Instant;

use tipb::executor::{ExecType, Executor};
use tipb::schema::ColumnInfo;
use tipb::select::{DAGRequest, RowMeta, SelectResponse};
use kvproto::coprocessor::{KeyRange, Response};
use kvproto::kvrpcpb::IsolationLevel;
use protobuf::{Message as PbMsg, RepeatedField};

use coprocessor::codec::mysql;
use coprocessor::codec::datum::{Datum, DatumEncoder};
use coprocessor::select::xeval::EvalContext;
use coprocessor::{Error, Result};
use coprocessor::endpoint::{check_if_outdated, get_chunk, get_pk, to_pb_error, REQ_TYPE_DAG};
use storage::{Snapshot, SnapshotStore, Statistics};

use super::executor::{AggregationExecutor, Executor as DAGExecutor, IndexScanExecutor,
                      LimitExecutor, Row, SelectionExecutor, TableScanExecutor, TopNExecutor};

pub struct DAGContext<'s> {
    deadline: Instant,
    columns: Rc<Vec<ColumnInfo>>,
    has_aggr: bool,
    req: DAGRequest,
    ranges: Vec<KeyRange>,
    snap: &'s Snapshot,
    eval_ctx: Rc<EvalContext>,
    isolation_level: IsolationLevel,
}

impl<'s> DAGContext<'s> {
    pub fn new(
        req: DAGRequest,
        deadline: Instant,
        ranges: Vec<KeyRange>,
        snap: &'s Snapshot,
        eval_ctx: Rc<EvalContext>,
        isolation_level: IsolationLevel,
    ) -> DAGContext<'s> {
        DAGContext {
            req: req,
            deadline: deadline,
            columns: Rc::new(vec![]),
            ranges: ranges,
            snap: snap,
            has_aggr: false,
            eval_ctx: eval_ctx,
            isolation_level: isolation_level,
        }
    }

    pub fn handle_request(mut self, statistics: &'s mut Statistics) -> Result<Response> {
        try!(self.validate_dag());
        let mut exec = try!(self.build_dag(statistics));
        let mut chunks = vec![];
        loop {
            match exec.next() {
                Ok(Some(row)) => {
                    try!(check_if_outdated(self.deadline, REQ_TYPE_DAG));
                    let mut chunk = get_chunk(&mut chunks);
                    let length = chunk.get_rows_data().len();
                    if self.has_aggr {
                        chunk.mut_rows_data().extend_from_slice(&row.data.value);
                    } else {
                        let value = try!(inflate_cols(
                            &row,
                            &self.columns,
                            self.req.get_output_offsets()
                        ));
                        chunk.mut_rows_data().extend_from_slice(&value);
                    }
                    let mut meta = RowMeta::new();
                    meta.set_handle(row.handle);
                    meta.set_length((chunk.get_rows_data().len() - length) as i64);
                    chunk.mut_rows_meta().push(meta);
                }
                Ok(None) => {
                    let mut resp = Response::new();
                    let mut sel_resp = SelectResponse::new();
                    sel_resp.set_chunks(RepeatedField::from_vec(chunks));
                    let data = box_try!(sel_resp.write_to_bytes());
                    resp.set_data(data);
                    return Ok(resp);
                }
                Err(e) => if let Error::Other(_) = e {
                    let mut resp = Response::new();
                    let mut sel_resp = SelectResponse::new();
                    sel_resp.set_error(to_pb_error(&e));
                    resp.set_data(box_try!(sel_resp.write_to_bytes()));
                    resp.set_other_error(format!("{}", e));
                    return Ok(resp);
                } else {
                    return Err(e);
                },
            }
        }
    }

    fn validate_dag(&mut self) -> Result<()> {
        let execs = self.req.get_executors();
        let first = try!(
            execs
                .first()
                .ok_or_else(|| Error::Other(box_err!("has no executor")))
        );
        // check whether first exec is *scan and get the column info
        match first.get_tp() {
            ExecType::TypeTableScan => {
                self.columns = Rc::new(first.get_tbl_scan().get_columns().to_vec());
            }
            ExecType::TypeIndexScan => {
                self.columns = Rc::new(first.get_idx_scan().get_columns().to_vec());
            }
            _ => {
                return Err(box_err!(
                    "first exec type should be *Scan, but get {:?}",
                    first.get_tp()
                ))
            }
        }
        // check whether dag has a aggregation action and take a flag
        if execs
            .iter()
            .rev()
            .any(|exec| exec.get_tp() == ExecType::TypeAggregation)
        {
            self.has_aggr = true;
        }
        Ok(())
    }

    // seperate first exec build action from `build_dag`
    // since it will generte mutable conflict when putting together
    fn build_first(
        &'s self,
        mut first: Executor,
        statistics: &'s mut Statistics,
    ) -> Box<DAGExecutor + 's> {
        let store = SnapshotStore::new(self.snap, self.req.get_start_ts(), self.isolation_level);

        match first.get_tp() {
            ExecType::TypeTableScan => Box::new(TableScanExecutor::new(
                first.take_tbl_scan(),
                self.ranges.clone(),
                store,
                statistics,
            )),
            ExecType::TypeIndexScan => Box::new(IndexScanExecutor::new(
                first.take_idx_scan(),
                self.ranges.clone(),
                store,
                statistics,
            )),
            _ => unreachable!(),
        }
    }

    fn build_dag(&'s self, statistics: &'s mut Statistics) -> Result<Box<DAGExecutor + 's>> {
        let mut execs = self.req.get_executors().to_vec().into_iter();
        let mut src = self.build_first(execs.next().unwrap(), statistics);
        for mut exec in execs {
            let curr: Box<DAGExecutor> = match exec.get_tp() {
                ExecType::TypeTableScan | ExecType::TypeIndexScan => {
                    return Err(box_err!("got too much *scan exec, should be only one"))
                }
                ExecType::TypeSelection => Box::new(try!(SelectionExecutor::new(
                    exec.take_selection(),
                    self.eval_ctx.clone(),
                    self.columns.clone(),
                    src
                ))),
                ExecType::TypeAggregation => Box::new(try!(AggregationExecutor::new(
                    exec.take_aggregation(),
                    self.eval_ctx.clone(),
                    self.columns.clone(),
                    src
                ))),
                ExecType::TypeTopN => Box::new(try!(TopNExecutor::new(
                    exec.take_topN(),
                    self.eval_ctx.clone(),
                    self.columns.clone(),
                    src
                ))),
                ExecType::TypeLimit => Box::new(LimitExecutor::new(exec.take_limit(), src)),
            };
            src = curr;
        }
        Ok(src)
    }
}

#[inline]
fn inflate_cols(row: &Row, cols: &[ColumnInfo], output_offsets: &[u32]) -> Result<Vec<u8>> {
    let data = &row.data;
    // TODO capacity is not enough
    let mut values = Vec::with_capacity(data.value.len());
    for offset in output_offsets {
        let col = &cols[*offset as usize];
        let col_id = col.get_column_id();
        match data.get(col_id) {
            Some(value) => values.extend_from_slice(value),
            None if col.get_pk_handle() => {
                let pk = get_pk(col, row.handle);
                box_try!(values.encode(&[pk], false));
            }
            None if col.has_default_val() => {
                values.extend_from_slice(col.get_default_val());
            }
            None if mysql::has_not_null_flag(col.get_flag() as u64) => {
                return Err(box_err!("column {} of {} is missing", col_id, row.handle));
            }
            None => {
                box_try!(values.encode(&[Datum::Null], false));
            }
        }
    }
    Ok(values)
}
