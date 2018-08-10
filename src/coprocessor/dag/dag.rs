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

use std::marker::PhantomData;
use std::sync::Arc;

use kvproto::coprocessor::{KeyRange, Response};
use protobuf::{Message as PbMsg, RepeatedField};
use tipb::select::{Chunk, DAGRequest, EncodeType, SelectResponse, StreamResponse};

use coprocessor::cache::*;
use coprocessor::dag::expr::EvalConfig;
use coprocessor::metrics::*;
use coprocessor::*;
use storage::{Snapshot, SnapshotStore};

use super::executor::{build_exec, check_aggr_and_topn, Executor, ExecutorMetrics};

pub struct DAGContext<S: Snapshot + 'static> {
    _phantom: PhantomData<S>,
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

impl<S: Snapshot + 'static> DAGContext<S> {
    pub fn new(
        mut req: DAGRequest,
        ranges: Vec<KeyRange>,
        snap: S,
        req_ctx: ReqContext,
        batch_row_limit: usize,
        distsql_cache: Option<Arc<SQLCache>>,
        distsql_cache_entry_max_size: usize,
    ) -> Result<Self> {
        let mut eval_cfg = box_try!(EvalConfig::new(req.get_flags()));

        // We respect time zone name first, then offset.
        if req.has_time_zone_name() && !req.get_time_zone_name().is_empty() {
            box_try!(eval_cfg.set_time_zone_by_name(req.get_time_zone_name()));
        } else if req.has_time_zone_offset() {
            box_try!(eval_cfg.set_time_zone_by_offset(req.get_time_zone_offset()));
        } else {
            // This should not be reachable. However we will not panic here in case
            // of compatibility issues.
        }

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

        let (has_aggr, has_topn) = check_aggr_and_topn(req.get_executors().to_vec());

        let dag_executor = build_exec(
            req.take_executors().into_vec(),
            store,
            ranges,
            Arc::new(eval_cfg),
            req.get_collect_range_counts(),
        )?;

        Ok(Self {
            _phantom: Default::default(),
            has_aggr: has_aggr,
            has_topn: has_topn,
            req_ctx,
            exec: dag_executor,
            output_offsets: req.take_output_offsets(),
            cache_key,
            distsql_cache,
            start_ts: req.get_start_ts(),
            distsql_cache_entry_max_size,
            batch_row_limit,
        })
    }

    fn can_cache(&mut self) -> bool {
        self.distsql_cache.is_some() && (self.has_aggr || self.has_topn)
    }

    fn can_cache_with_size(&mut self, data: &[u8]) -> bool {
        if data.len() > self.distsql_cache_entry_max_size {
            false
        } else {
            self.can_cache()
        }
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

impl<S: Snapshot> RequestHandler for DAGContext<S> {
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
                    // for default encode type
                    let value = row.get_binary(&self.output_offsets)?;
                    chunk.mut_rows_data().extend_from_slice(&value);
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

    fn handle_streaming_request(&mut self) -> Result<(Option<Response>, bool)> {
        let (mut record_cnt, mut finished) = (0, false);
        let mut chunk = Chunk::new();
        self.exec.start_scan();
        while record_cnt < self.batch_row_limit {
            match self.exec.next() {
                Ok(Some(row)) => {
                    record_cnt += 1;
                    let value = row.get_binary(&self.output_offsets)?;
                    chunk.mut_rows_data().extend_from_slice(&value);
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
