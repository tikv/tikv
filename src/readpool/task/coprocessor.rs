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

use futures::Future;

use coprocessor;
use storage;
use util;
use kvproto::kvrpcpb;

use super::*;

pub struct CoprocessorTask {
    request: Option<coppb::Request>,
    recursion_limit: u32,
    batch_row_limit: usize,
}

impl CoprocessorTask {
    pub fn new(
        request: coppb::Request,
        recursion_limit: u32,
        batch_row_limit: usize,
    ) -> CoprocessorTask {
        CoprocessorTask {
            request: Some(request),
            recursion_limit,
            batch_row_limit,
        }
    }
}

impl Task for CoprocessorTask {
    fn build(&mut self, context: &WorkerThreadContext) -> BoxedFuture {
        let request = self.request.take().unwrap();
        let recursion_limit = self.recursion_limit;
        let batch_row_limit = self.batch_row_limit;
        box context
            .engine
            .future_snapshot(request.get_context())
            // map storage::engine::Error -> storage::Error
            .map_err(storage::Error::from)
            .and_then(move |snapshot: Box<storage::Snapshot>| {
                let (on_response, future) = util::future::future_from_callback();
                let endpoint = coprocessor::EndPoint::new(snapshot);
                let request_task = coprocessor::RequestTask::new(
                    request,
                    on_response,
                    recursion_limit,
                );
                // TODO: let statistics = endpoint.handle_request(...)
                endpoint.handle_request(request_task, batch_row_limit);

                future
                    // map future::oneshot::Canceled to storage::engine::Error::Other
                    .map_err(storage::engine::Error::from)
                    // map storage::engine::Error to storage::Error
                    .map_err(storage::Error::from)
            })
            // map storage::Error -> Error
            .map_err(Error::from)
            .map(Value::Coprocessor)
    }
}

// TODO: Test
