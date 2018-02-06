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

use std::sync::Arc;

use tipb::schema::ColumnInfo;
use tipb::select::{Chunk, DAGRequest, EncodeType, SelectResponse, StreamResponse};
use kvproto::coprocessor::{KeyRange, Response};
use protobuf::{Message as PbMsg, RepeatedField};

use coprocessor::codec::mysql;
use coprocessor::codec::datum::{Datum, DatumEncoder};
use coprocessor::dag::expr::EvalContext;
use coprocessor::local_metrics::*;
use coprocessor::{Error, Result};
use coprocessor::endpoint::{get_pk, prefix_next, to_pb_error, ReqContext};
use storage::{Snapshot, SnapshotStore, Statistics};

use super::executor::{build_exec, Executor, Row};

pub struct DAGContext {
    columns: Arc<Vec<ColumnInfo>>,
    has_aggr: bool,
    req_ctx: ReqContext,
    exec: Box<Executor>,
    output_offsets: Vec<u32>,
}

impl DAGContext {
    pub fn new(
        mut req: DAGRequest,
        ranges: Vec<KeyRange>,
        snap: Box<Snapshot>,
        req_ctx: ReqContext,
    ) -> Result<DAGContext> {
        let eval_ctx = Arc::new(box_try!(EvalContext::new(
            req.get_time_zone_offset(),
            req.get_flags()
        )));
        let store = SnapshotStore::new(
            snap,
            req.get_start_ts(),
            req_ctx.isolation_level,
            req_ctx.fill_cache,
        );

        let dag_executor = build_exec(req.take_executors().into_vec(), store, ranges, eval_ctx)?;
        Ok(DAGContext {
            columns: dag_executor.columns,
            has_aggr: dag_executor.has_aggr,
            req_ctx: req_ctx,
            exec: dag_executor.exec,
            output_offsets: req.take_output_offsets(),
        })
    }

    pub fn handle_request(&mut self, batch_row_limit: usize) -> Result<Response> {
        let mut record_cnt = 0;
        let mut chunks = Vec::new();
        loop {
            match self.exec.next() {
                Ok(Some(row)) => {
                    self.req_ctx.check_if_outdated()?;
                    if chunks.is_empty() || record_cnt >= batch_row_limit {
                        let chunk = Chunk::new();
                        chunks.push(chunk);
                        record_cnt = 0;
                    }
                    let chunk = chunks.last_mut().unwrap();
                    record_cnt += 1;
                    if self.has_aggr {
                        chunk.mut_rows_data().extend_from_slice(&row.data.value);
                    } else {
                        let value = inflate_cols(&row, &self.columns, &self.output_offsets)?;
                        chunk.mut_rows_data().extend_from_slice(&value);
                    }
                }
                Ok(None) => {
                    let mut resp = Response::new();
                    let mut sel_resp = SelectResponse::new();
                    sel_resp.set_chunks(RepeatedField::from_vec(chunks));
                    self.exec
                        .collect_output_counts(sel_resp.mut_output_counts());
                    let data = box_try!(sel_resp.write_to_bytes());
                    resp.set_data(data);
                    return Ok(resp);
                }
                Err(e) => match e {
                    Error::Other(_) => {
                        let mut resp = Response::new();
                        let mut sel_resp = SelectResponse::new();
                        sel_resp.set_error(to_pb_error(&e));
                        resp.set_data(box_try!(sel_resp.write_to_bytes()));
                        resp.set_other_error(format!("{}", e));
                        return Ok(resp);
                    }
                    _ => return Err(e),
                },
            }
        }
    }

    pub fn handle_streaming_request(
        &mut self,
        batch_row_limit: usize,
    ) -> Result<(Option<Response>, bool)> {
        let (mut record_cnt, mut finished) = (0, false);
        let mut chunk = Chunk::new();
        let (mut start_key, mut support_partial_retry) = (None, true);
        while record_cnt < batch_row_limit {
            match self.exec.next() {
                Ok(Some(row)) => {
                    self.req_ctx.check_if_outdated()?;
                    record_cnt += 1;
                    if support_partial_retry && start_key.is_none() {
                        start_key = self.exec.take_last_key();
                        if start_key.is_none() {
                            support_partial_retry = false;
                        }
                    }
                    if self.has_aggr {
                        chunk.mut_rows_data().extend_from_slice(&row.data.value);
                    } else {
                        let value = inflate_cols(&row, &self.columns, &self.output_offsets)?;
                        chunk.mut_rows_data().extend_from_slice(&value);
                    }
                }
                Ok(None) => {
                    finished = true;
                    break;
                }
                Err(e) => match e {
                    Error::Other(_) => {
                        let mut resp = Response::new();
                        let mut s_resp = StreamResponse::new();
                        s_resp.set_error(to_pb_error(&e));
                        resp.set_data(box_try!(s_resp.write_to_bytes()));
                        resp.set_other_error(format!("{}", e));
                        return Ok((Some(resp), true));
                    }
                    _ => return Err(e),
                },
            }
        }
        if record_cnt > 0 {
            let end_key = self.exec.take_last_key();
            self.req_ctx.renew_streaming_outdated();
            return self.make_stream_response(chunk, start_key, end_key)
                .map(|r| (Some(r), finished));
        }
        Ok((None, true))
    }

    fn make_stream_response(
        &mut self,
        chunk: Chunk,
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
    ) -> Result<Response> {
        let mut s_resp = StreamResponse::new();
        s_resp.set_encode_type(EncodeType::TypeDefault);
        s_resp.set_data(box_try!(chunk.write_to_bytes()));
        self.exec.collect_output_counts(s_resp.mut_output_counts());

        let mut resp = Response::new();
        resp.set_data(box_try!(s_resp.write_to_bytes()));

        // `start_key` and `end_key` indicates the key_range which has been scaned
        // for generating the response. It's for TiDB can retry requests partially,
        // but some `Executor`s (e.g. TopN and Aggr) don't support that, in which
        // cases both `start_key` and `end_key` should be None.
        let (start, end) = match (start_key, end_key) {
            (Some(start_key), Some(end_key)) => if start_key > end_key {
                (end_key, prefix_next(&start_key))
            } else {
                (start_key, prefix_next(&end_key))
            },
            (Some(start_key), None) => {
                let end_key = prefix_next(&start_key);
                (start_key, end_key)
            }
            (None, None) => return Ok(resp),
            _ => unreachable!(),
        };
        let mut range = KeyRange::new();
        range.set_start(start);
        range.set_end(end);
        resp.set_range(range);
        Ok(resp)
    }

    pub fn collect_statistics_into(&mut self, statistics: &mut Statistics) {
        self.exec.collect_statistics_into(statistics);
    }

    pub fn collect_metrics_into(&mut self, metrics: &mut ScanCounter) {
        self.exec.collect_metrics_into(metrics);
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
