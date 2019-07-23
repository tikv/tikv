// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::coprocessor::{KeyRange, Response};
use protobuf::Message;
use tipb::executor::ExecutorExecutionSummary;
use tipb::select::{Chunk, SelectResponse, StreamResponse};

use super::executor::Executor;
use crate::coprocessor::dag::execute_stats::ExecuteStats;
use crate::coprocessor::*;
use crate::storage::Statistics;

/// Handles Coprocessor DAG requests.
pub struct DAGRequestHandler {
    deadline: Deadline,
    executor: Box<dyn Executor>,
    output_offsets: Vec<u32>,
    batch_row_limit: usize,
    collect_exec_summary: bool,
    exec_stats: ExecuteStats,
}

impl DAGRequestHandler {
    pub fn new(
        deadline: Deadline,
        executor: Box<dyn Executor>,
        output_offsets: Vec<u32>,
        batch_row_limit: usize,
        collect_exec_summary: bool,
        exec_stats: ExecuteStats,
    ) -> Self {
        Self {
            deadline,
            executor,
            output_offsets,
            batch_row_limit,
            collect_exec_summary,
            exec_stats,
        }
    }

    fn make_stream_response(&mut self, chunk: Chunk, range: Option<KeyRange>) -> Result<Response> {
        self.executor.collect_exec_stats(&mut self.exec_stats);

        let mut s_resp = StreamResponse::default();
        s_resp.set_data(box_try!(chunk.write_to_bytes()));
        if let Some(eval_warnings) = self.executor.take_eval_warnings() {
            s_resp.set_warnings(eval_warnings.warnings.into());
            s_resp.set_warning_count(eval_warnings.warning_cnt as i64);
        }

        s_resp.set_output_counts(
            self.exec_stats
                .scanned_rows_per_range
                .iter()
                .map(|v| *v as i64)
                .collect(),
        );

        let mut resp = Response::default();
        resp.set_data(box_try!(s_resp.write_to_bytes()));
        if let Some(range) = range {
            resp.set_range(range);
        }

        self.exec_stats.clear();

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
                    self.executor.collect_exec_stats(&mut self.exec_stats);

                    let mut resp = Response::default();
                    let mut sel_resp = SelectResponse::default();
                    sel_resp.set_chunks(chunks.into());
                    if let Some(eval_warnings) = self.executor.take_eval_warnings() {
                        sel_resp.set_warnings(eval_warnings.warnings.into());
                        sel_resp.set_warning_count(eval_warnings.warning_cnt as i64);
                    }

                    sel_resp.set_output_counts(
                        self.exec_stats
                            .scanned_rows_per_range
                            .iter()
                            .map(|v| *v as i64)
                            .collect(),
                    );

                    if self.collect_exec_summary {
                        let summaries = self
                            .exec_stats
                            .summary_per_executor
                            .iter()
                            .map(|summary| {
                                let mut ret = ExecutorExecutionSummary::new();
                                ret.set_num_iterations(summary.num_iterations as u64);
                                ret.set_num_produced_rows(summary.num_produced_rows as u64);
                                ret.set_time_processed_ns(summary.time_processed_ns as u64);
                                ret
                            })
                            .collect::<Vec<_>>();
                        sel_resp.set_execution_summaries(summaries.into());
                    }

                    // In case of this function is called multiple times.
                    self.exec_stats.clear();

                    let data = box_try!(sel_resp.write_to_bytes());
                    resp.set_data(data);
                    return Ok(resp);
                }
                Err(Error::Eval(err)) => {
                    let mut resp = Response::default();
                    let mut sel_resp = SelectResponse::default();
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
                    let mut resp = Response::default();
                    let mut sel_resp = StreamResponse::default();
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

    fn collect_scan_statistics(&mut self, dest: &mut Statistics) {
        // TODO: A better way is to fill storage stats in `handle_request`, or
        // return SelectResponse in `handle_request`.
        self.executor.collect_storage_stats(dest);
    }
}
