// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::coprocessor::{KeyRange, Response};
use protobuf::{Message, RepeatedField};
use tipb::executor::ExecutorExecutionSummary;
use tipb::select::{Chunk, SelectResponse, StreamResponse};

use super::executor::{Executor, ExecutorMetrics};

use crate::coprocessor::dag::exec_summary::ExecSummary;
use crate::coprocessor::*;

/// Handles Coprocessor DAG requests.
pub struct DAGRequestHandler {
    deadline: Deadline,
    executor: Box<dyn Executor + Send>,
    output_offsets: Vec<u32>,
    batch_row_limit: usize,
    /// To construct ExecutionSummary target.
    number_of_executors: usize,
    collect_exec_summary: bool,
}

impl DAGRequestHandler {
    pub fn new(
        deadline: Deadline,
        executor: Box<dyn Executor + Send>,
        output_offsets: Vec<u32>,
        batch_row_limit: usize,
        number_of_executors: usize,
        collect_exec_summary: bool,
    ) -> Self {
        Self {
            deadline,
            executor,
            output_offsets,
            batch_row_limit,
            number_of_executors,
            collect_exec_summary,
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

                    if self.collect_exec_summary {
                        let mut summary_per_executor =
                            vec![ExecSummary::default(); self.number_of_executors];
                        self.executor
                            .collect_execution_summaries(&mut summary_per_executor);
                        let summaries = summary_per_executor
                            .iter()
                            .map(|summary| {
                                let mut ret = ExecutorExecutionSummary::new();
                                ret.set_num_iterations(summary.num_iterations as u64);
                                ret.set_num_produced_rows(summary.num_produced_rows as u64);
                                ret.set_time_processed_ns(summary.time_processed_ns as u64);
                                ret
                            })
                            .collect();
                        sel_resp.set_execution_summaries(RepeatedField::from_vec(summaries));
                    }

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
