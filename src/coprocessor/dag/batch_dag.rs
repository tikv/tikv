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

use protobuf::{Message, RepeatedField};

use kvproto::coprocessor::Response;
use tipb::select::{Chunk, SelectResponse};

use super::batch_executor::interface::{BatchExecutor, ExecutorContext};
use super::executor::ExecutorMetrics;
use coprocessor::*;

/// Must be built from DAGContext.
pub struct BatchDAGHandler {
    out_most_executor: Box<BatchExecutor>,
    output_offsets: Vec<u32>,
    executor_context: ExecutorContext,
}

impl BatchDAGHandler {
    pub fn new(
        out_most_executor: Box<BatchExecutor>,
        output_offsets: Vec<u32>,
        executor_context: ExecutorContext,
    ) -> Self {
        Self {
            out_most_executor,
            output_offsets,
            executor_context,
        }
    }
}

impl RequestHandler for BatchDAGHandler {
    fn handle_request(&mut self) -> Result<Response> {
        let mut chunks = vec![];
        loop {
            // self.deadline.check_if_exceeded()?;
            let result = self.out_most_executor.next_batch(1024);

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
                    result.data.encode(
                        &self.output_offsets,
                        &self.executor_context.columns_info,
                        data,
                    )?;
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

    fn collect_metrics_into(&mut self, _metrics: &mut ExecutorMetrics) {
        // TODO
    }
}
