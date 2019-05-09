// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::coprocessor::{KeyRange, Response};
use protobuf::{Message, RepeatedField};
use tipb::select::{Chunk, SelectResponse, StreamResponse};

use crate::*;

use super::executor::{Executor, ExecutorMetrics};

/// Handles Coprocessor DAG requests.
pub struct DAGRequestHandler {
    deadline: Deadline,
    executor: Box<dyn Executor + Send>,
    output_offsets: Vec<u32>,
    batch_row_limit: usize,
}

impl DAGRequestHandler {
    pub fn new(
        deadline: Deadline,
        executor: Box<dyn Executor + Send>,
        output_offsets: Vec<u32>,
        batch_row_limit: usize,
    ) -> Self {
        Self {
            deadline,
            executor,
            output_offsets,
            batch_row_limit,
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

// TODO: impl RequestHandler for DAGRequestHandler