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

use kvproto::coprocessor::{KeyRange, Response};
use protobuf::{Message as PbMsg, RepeatedField};
use tipb::schema::ColumnInfo;
use tipb::select::{Chunk, DAGRequest, EncodeType, SelectResponse, StreamResponse};

use storage::{Snapshot, SnapshotStore};

use coprocessor::codec::datum::{Datum, DatumEncoder};
use coprocessor::codec::mysql;
use coprocessor::dag::expr::EvalConfig;
use coprocessor::cache::*;
use coprocessor::metrics::*;
use coprocessor::util;
use coprocessor::*;

use super::executor::{build_exec, Executor, ExecutorMetrics, Row};

pub struct DAGContext {
    columns: Arc<Vec<ColumnInfo>>,
    has_aggr: bool,
    has_topn: bool,
    req_ctx: ReqContext,
    exec: Box<Executor + Send>,
    output_offsets: Vec<u32>,
    cache_key: String,
    distsql_cache: Option<Arc<SQLCache>>,
    start_ts: u64,
    distsql_cache_entry_max_size: usize,
    batch_row_limit: usize,
}

impl DAGContext {
    pub fn new(
        mut req: DAGRequest,
        ranges: Vec<KeyRange>,
        snap: Box<Snapshot>,
        req_ctx: ReqContext,
        distsql_cache: Option<Arc<SQLCache>>,
        distsql_cache_entry_max_size: usize,
        batch_row_limit: usize,
    ) -> Result<DAGContext> {
        let mut eval_cfg = box_try!(EvalConfig::new(req.get_time_zone_offset(), req.get_flags(),));
        if req.has_max_warning_count() {
            eval_cfg.set_max_warning_cnt(req.get_max_warning_count() as usize);
        }
        if req.has_sql_mode() {
            eval_cfg.set_sql_mode(req.get_sql_mode())
        }
        if req.has_is_strict_sql_mode() {
            eval_cfg.set_strict_sql_mode(req.get_is_strict_sql_mode());
        }
        let store = SnapshotStore::new(
            snap,
            req.get_start_ts(),
            req_ctx.context.get_isolation_level(),
            !req_ctx.context.get_not_fill_cache(),
        );
        let cache_key = format!(
            "{:?} {:?} {:?} {:?}",
            ranges,
            req.get_flags(),
            req.get_output_offsets(),
            req.get_executors()
        );

        let dag_executor = build_exec(
            req.take_executors().into_vec(),
            store,
            ranges,
            Arc::new(eval_cfg),
            req.get_collect_range_counts(),
        )?;
        Ok(DAGContext {
            columns: dag_executor.columns,
            has_aggr: dag_executor.has_aggr,
            has_topn: dag_executor.has_topn,
            req_ctx: req_ctx,
            exec: dag_executor.exec,
            output_offsets: req.take_output_offsets(),
            cache_key: cache_key,
            distsql_cache: distsql_cache,
            start_ts: req.get_start_ts(),
            distsql_cache_entry_max_size: distsql_cache_entry_max_size,
        })
    }

    fn make_stream_response(&mut self, chunk: Chunk, range: Option<KeyRange>) -> Result<Response> {
        let mut s_resp = StreamResponse::new();
        s_resp.set_encode_type(EncodeType::TypeDefault);
        s_resp.set_data(box_try!(chunk.write_to_bytes()));
        if let Some(eval_warnings) = self.exec.take_eval_warnings() {
            s_resp.set_warnings(RepeatedField::from_vec(eval_warnings.warnings));
            s_resp.set_warning_count(eval_warnings.warning_cnt as i64);
        }
        self.exec.collect_output_counts(s_resp.mut_output_counts());

        let mut resp = Response::new();
        resp.set_data(box_try!(s_resp.write_to_bytes()));
        if let Some(range) = range {
            resp.set_range(range);
        }
        Ok(resp)
    }
}

impl RequestHandler for DAGContext {
    fn handle_request(&mut self, region_id: u64) -> Result<Response> {
        let mut record_cnt = 0;
        let mut chunks = Vec::new();
        let mut version: u64 = 0;
        if self.can_cache() {
            let mut cache = self.distsql_cache.as_mut().unwrap().lock();
            let (rver, entry) =
                cache.get_region_version_and_cache_entry(region_id, &self.cache_key, self.start_ts);
            version = rver;
            if let Some(data) = entry {
                debug!("Cache Hit: {}, region_id: {}", self.cache_key, region_id);
                CORP_DISTSQL_CACHE_COUNT.with_label_values(&["hit"]).inc();
                let mut resp = Response::new();
                resp.set_data(data.clone());
                return Ok(resp);
            }
        }

        loop {
            match self.exec.next() {
                Ok(Some(row)) => {
                    self.req_ctx.check_if_outdated()?;
                    if chunks.is_empty() || record_cnt >= self.batch_row_limit {
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
                    if let Some(eval_warnings) = self.exec.take_eval_warnings() {
                        sel_resp.set_warnings(RepeatedField::from_vec(eval_warnings.warnings));
                        sel_resp.set_warning_count(eval_warnings.warning_cnt as i64);
                    }
                    self.exec
                        .collect_output_counts(sel_resp.mut_output_counts());
                    let data = box_try!(sel_resp.write_to_bytes());
                    // If the result set is bigger than distsql_cache_entry_max_size
                    // it cannot be cached.
                    if self.can_cache_with_size(&data) {
                        debug!(
                            "Cache It: {}, region_id: {}, start_ts: {}",
                            &self.cache_key, region_id, self.start_ts
                        );
                        self.distsql_cache.as_mut().unwrap().lock().put(
                            region_id,
                            self.cache_key.clone(),
                            version,
                            data.clone(),
                            self.start_ts,
                        );
                        CORP_DISTSQL_CACHE_COUNT.with_label_values(&["miss"]).inc();
                    }
                    resp.set_data(data);
                    return Ok(resp);
                }
                Err(Error::Eval(err)) => {
                    let mut resp = Response::new();
                    let mut sel_resp = SelectResponse::new();
                    sel_resp.set_error(err);
                    let data = box_try!(sel_resp.write_to_bytes());
                    resp.set_data(data);
                    return Ok(resp);
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub fn can_cache(&mut self) -> bool {
        self.distsql_cache.is_some() && (self.has_aggr || self.has_topn)
    }

    pub fn can_cache_with_size(&mut self, data: &[u8]) -> bool {
        if data.len() > self.distsql_cache_entry_max_size {
            false
        } else {
            self.can_cache()
        }
    }

    fn handle_streaming_request(&mut self) -> Result<(Option<Response>, bool)> {
        let (mut record_cnt, mut finished) = (0, false);
        let mut chunk = Chunk::new();
        self.exec.start_scan();
        while record_cnt < self.batch_row_limit {
            match self.exec.next() {
                Ok(Some(row)) => {
                    record_cnt += 1;
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
                Err(Error::Eval(err)) => {
                    let mut resp = Response::new();
                    let mut sel_resp = StreamResponse::new();
                    sel_resp.set_error(err);
                    let data = box_try!(sel_resp.write_to_bytes());
                    resp.set_data(data);
                    return Ok((Some(resp), true));
                }
                Err(e) => return Err(e),
            }
        }
        if record_cnt > 0 {
            let range = self.exec.stop_scan();
            return self
                .make_stream_response(chunk, range)
                .map(|r| (Some(r), finished));
        }
        Ok((None, true))
    }

    fn collect_metrics_into(&mut self, metrics: &mut ExecutorMetrics) {
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
                let pk = util::get_pk(col, row.handle);
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
