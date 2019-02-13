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
use crate::coprocessor::*;

const BATCH_INITIAL_SIZE: usize = 32;
const BATCH_MAX_SIZE: usize = 1024;
const BATCH_GROW_FACTOR: usize = 2;

/// Must be built from DAGRequestHandler.
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
        let mut batch_size = BATCH_INITIAL_SIZE;

        loop {
            // self.deadline.check_if_exceeded()?;
            let result = self.out_most_executor.next_batch(batch_size);

            let is_drained;

            // Check error first, because it means that we should directly respond error.
            match result.is_drained {
                Err(Error::Eval(err)) => {
                    let mut resp = Response::new();
                    let mut sel_resp = SelectResponse::new();
                    sel_resp.set_error(err);
                    let data = box_try!(sel_resp.write_to_bytes());
                    resp.set_data(data);
                    return Ok(resp);
                }
                Err(e) => return Err(e),
                Ok(f) => is_drained = f,
            }

            if result.data.rows_len() > 0 {
                let mut chunk = Chunk::new();
                {
                    let data = chunk.mut_rows_data();
                    data.reserve(result.data.maximum_encoded_size(&self.output_offsets)?);
                    result.data.encode(
                        &self.output_offsets,
                        &self.executor_context.columns_info,
                        data,
                    )?;
                }
                chunks.push(chunk);
            }

            if is_drained {
                let mut resp = Response::new();
                let mut sel_resp = SelectResponse::new();
                sel_resp.set_chunks(RepeatedField::from_vec(chunks));

                // TODO: Warning
                // TODO: Output counts

                let data = box_try!(sel_resp.write_to_bytes());
                resp.set_data(data);
                return Ok(resp);
            }

            // Grow batch size
            if batch_size < BATCH_MAX_SIZE {
                batch_size *= BATCH_GROW_FACTOR;
                if batch_size > BATCH_MAX_SIZE {
                    batch_size = BATCH_MAX_SIZE
                }
            }
        }
    }

    fn collect_metrics_into(&mut self, _metrics: &mut ExecutorMetrics) {
        // TODO
    }
}
