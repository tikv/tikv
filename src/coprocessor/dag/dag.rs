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
use protobuf::{Message, RepeatedField};
use tipb::select::{Chunk, DAGRequest, SelectResponse, StreamResponse};

use coprocessor::dag::expr::EvalConfig;
use coprocessor::*;
use storage::Store;

use super::batch_executor::interface::BatchExecutor;
use super::executor::{Executor, ExecutorMetrics};

pub struct DAGContext {
    deadline: Deadline,
    normal_executor: Option<Box<Executor + Send>>,
    batch_executor: Option<Box<BatchExecutor>>,
    output_offsets: Vec<u32>,
    batch_row_limit: usize,
    is_streaming: bool,
    is_batch: bool,
}

impl DAGContext {
    pub fn new<S: Store + 'static>(
        mut req: DAGRequest,
        ranges: Vec<KeyRange>,
        store: S,
        deadline: Deadline,
        batch_row_limit: usize,
        is_streaming: bool,
        enable_batch_if_possible: bool,
    ) -> Result<Self> {
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
            && super::pipeline::ExecutorPipelineBuilder::can_build_batch(req.get_executors());
        let eval_ctx = Arc::new(eval_cfg);
        let executor_descriptors = req.take_executors().into_vec();

        let (normal_executor, batch_executor) = if is_batch {
            (
                None,
                Some(super::pipeline::ExecutorPipelineBuilder::build_batch(
                    executor_descriptors,
                    store,
                    ranges,
                    eval_ctx,
                )?),
            )
        } else {
            (
                Some(super::pipeline::ExecutorPipelineBuilder::build_normal(
                    executor_descriptors,
                    store,
                    ranges,
                    eval_ctx,
                    req.get_collect_range_counts(),
                )?),
                None,
            )
        };

        Ok(Self {
            deadline,
            normal_executor,
            batch_executor,
            output_offsets: req.take_output_offsets(),
            batch_row_limit,
            is_streaming,
            is_batch,
        })
    }

    fn make_stream_response(&mut self, chunk: Chunk, range: Option<KeyRange>) -> Result<Response> {
        assert!(!self.is_batch);

        let exec = self.normal_executor.as_mut().unwrap();
        let mut s_resp = StreamResponse::new();
        s_resp.set_data(box_try!(chunk.write_to_bytes()));
        if let Some(eval_warnings) = exec.take_eval_warnings() {
            s_resp.set_warnings(RepeatedField::from_vec(eval_warnings.warnings));
            s_resp.set_warning_count(eval_warnings.warning_cnt as i64);
        }
        exec.collect_output_counts(s_resp.mut_output_counts());

        let mut resp = Response::new();
        resp.set_data(box_try!(s_resp.write_to_bytes()));
        if let Some(range) = range {
            resp.set_range(range);
        }
        Ok(resp)
    }

    fn handle_normal_request(&mut self) -> Result<Response> {
        assert!(!self.is_batch);

        let exec = self.normal_executor.as_mut().unwrap();
        let mut record_cnt = 0;
        let mut chunks = Vec::new();
        loop {
            match exec.next() {
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
                    if let Some(eval_warnings) = exec.take_eval_warnings() {
                        sel_resp.set_warnings(RepeatedField::from_vec(eval_warnings.warnings));
                        sel_resp.set_warning_count(eval_warnings.warning_cnt as i64);
                    }
                    exec.collect_output_counts(sel_resp.mut_output_counts());
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

    fn handle_batch_request(&mut self) -> Result<Response> {
        assert!(self.is_batch);

        let exec = self.batch_executor.as_mut().unwrap();
        let mut chunks = vec![];
        loop {
            self.deadline.check_if_exceeded()?;
            let result = exec.next_batch(1024);

            // Check error first, because it means that we should directly respond error.
            match result.error {
                Some(Error::Eval(err)) => {
                    let mut resp = Response::new();
                    let mut sel_resp = SelectResponse::new();
                    sel_resp.set_error(err);
                    let data = box_try!(sel_resp.write_to_bytes());
                    resp.set_data(data);
                    return Ok(resp);
                }
                Some(e) => return Err(e),
                None => {}
            }

            let number_of_rows = result.data.rows_len();
            if number_of_rows > 0 {
                let mut chunk = Chunk::new();
                {
                    let data = chunk.mut_rows_data();
                    data.reserve(result.data.encoded_size(&self.output_offsets)?);
                    result.data.encode(&self.output_offsets, data)?;
                }
                chunks.push(chunk);
            } else {
                let mut resp = Response::new();
                let mut sel_resp = SelectResponse::new();
                sel_resp.set_chunks(RepeatedField::from_vec(chunks));

                // TODO: Warning
                // TODO: Output counts

                let data = box_try!(sel_resp.write_to_bytes());
                resp.set_data(data);
                return Ok(resp);
            }
        }
    }
}

impl RequestHandler for DAGContext {
    fn handle_request(&mut self) -> Result<Response> {
        assert!(!self.is_streaming);
        if self.is_batch {
            self.handle_batch_request()
        } else {
            self.handle_normal_request()
        }
    }

    fn handle_streaming_request(&mut self) -> Result<(Option<Response>, bool)> {
        assert!(self.is_streaming);
        let (mut record_cnt, mut finished) = (0, false);
        let mut chunk = Chunk::new();
        self.normal_executor.as_mut().unwrap().start_scan();
        while record_cnt < self.batch_row_limit {
            match self.normal_executor.as_mut().unwrap().next() {
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
            let range = self.normal_executor.as_mut().unwrap().stop_scan();
            return self
                .make_stream_response(chunk, range)
                .map(|r| (Some(r), finished));
        }
        Ok((None, true))
    }

    fn collect_metrics_into(&mut self, metrics: &mut ExecutorMetrics) {
        if !self.is_batch {
            self.normal_executor
                .as_mut()
                .unwrap()
                .collect_metrics_into(metrics);
        }
        // TODO
    }
}
