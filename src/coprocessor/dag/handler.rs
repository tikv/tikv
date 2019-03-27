// Copyright 2017 TiKV Project Authors.
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
use protobuf::{Message, RepeatedField};
use tipb::select::{Chunk, DAGRequest, SelectResponse, StreamResponse};

use crate::coprocessor::dag::expr::EvalConfig;
use crate::coprocessor::*;
use crate::storage::Store;

use super::executor::{Executor, ExecutorMetrics};

/// Handles Coprocessor DAG requests.
pub struct DAGRequestHandler {
    deadline: Deadline,
    executor: Box<dyn Executor + Send>,
    output_offsets: Vec<u32>,
    batch_row_limit: usize,
}

impl DAGRequestHandler {
    fn build_dag<S: Store + 'static>(
        eval_cfg: EvalConfig,
        mut req: DAGRequest,
        ranges: Vec<KeyRange>,
        store: S,
        deadline: Deadline,
        batch_row_limit: usize,
    ) -> Result<Self> {
        let executor = super::builder::DAGBuilder::build_normal(
            req.take_executors().into_vec(),
            store,
            ranges,
            Arc::new(eval_cfg),
            req.get_collect_range_counts(),
        )?;
        Ok(Self {
            deadline,
            executor,
            output_offsets: req.take_output_offsets(),
            batch_row_limit,
        })
    }

    fn build_batch_dag<S: Store + 'static>(
        deadline: Deadline,
        config: EvalConfig,
        mut req: DAGRequest,
        ranges: Vec<KeyRange>,
        store: S,
    ) -> Result<super::batch_handler::BatchDAGHandler> {
        let ranges_len = ranges.len();

        let config = Arc::new(config);
        let out_most_executor = super::builder::DAGBuilder::build_batch(
            req.take_executors().into_vec(),
            store,
            ranges,
            config.clone(),
        )?;
        Ok(super::batch_handler::BatchDAGHandler::new(
            deadline,
            out_most_executor,
            req.take_output_offsets(),
            config,
            ranges_len,
        ))
    }

    pub fn build<S: Store + 'static>(
        req: DAGRequest,
        ranges: Vec<KeyRange>,
        store: S,
        deadline: Deadline,
        batch_row_limit: usize,
        is_streaming: bool,
        enable_batch_if_possible: bool,
    ) -> Result<Box<dyn RequestHandler>> {
        let mut eval_cfg = EvalConfig::from_flags(req.get_flags());
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
            eval_cfg.set_sql_mode(req.get_sql_mode());
        }
        if req.has_is_strict_sql_mode() {
            eval_cfg.set_strict_sql_mode(req.get_is_strict_sql_mode());
        }

        let is_batch = enable_batch_if_possible
            && !is_streaming
            && super::builder::DAGBuilder::can_build_batch(req.get_executors());

        if is_batch {
            Ok(Self::build_batch_dag(deadline, eval_cfg, req, ranges, store)?.into_boxed())
        } else {
            Ok(
                Self::build_dag(eval_cfg, req, ranges, store, deadline, batch_row_limit)?
                    .into_boxed(),
            )
        }
    }

    fn make_stream_response(&mut self, chunk: Chunk, range: Option<KeyRange>) -> Result<Response> {
        let mut s_resp = StreamResponse::new();
        s_resp.set_data(box_try!(chunk.write_to_bytes()));
        if let Some(eval_warnings) = self.executor.take_eval_warnings() {
            s_resp.set_warnings(RepeatedField::from_vec(eval_warnings.warnings));
            s_resp.set_warning_count(eval_warnings.warning_cnt as i64);
        }
        self.executor
            .collect_output_counts(s_resp.mut_output_counts());

        let mut resp = Response::new();
        resp.set_data(box_try!(s_resp.write_to_bytes()));
        if let Some(range) = range {
            resp.set_range(range);
        }
        Ok(resp)
    }
}

impl RequestHandler for DAGRequestHandler {
    fn handle_request(&mut self) -> Result<Response> {
        let mut record_cnt = 0;
        let mut chunks = Vec::new();
        loop {
            match self.executor.next() {
                Ok(Some(row)) => {
                    self.deadline.check_if_exceeded()?;
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
                    if let Some(eval_warnings) = self.executor.take_eval_warnings() {
                        sel_resp.set_warnings(RepeatedField::from_vec(eval_warnings.warnings));
                        sel_resp.set_warning_count(eval_warnings.warning_cnt as i64);
                    }
                    self.executor
                        .collect_output_counts(sel_resp.mut_output_counts());
                    let data = box_try!(sel_resp.write_to_bytes());
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
        self.executor.start_scan();
        while record_cnt < self.batch_row_limit {
            match self.executor.next() {
                Ok(Some(row)) => {
                    self.deadline.check_if_exceeded()?;
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
            let range = self.executor.stop_scan();
            return self
                .make_stream_response(chunk, range)
                .map(|r| (Some(r), finished));
        }
        Ok((None, true))
    }

    fn collect_metrics_into(&mut self, metrics: &mut ExecutorMetrics) {
        self.executor.collect_metrics_into(metrics);
    }
}
