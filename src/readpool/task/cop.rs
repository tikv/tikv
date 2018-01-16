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

use kvproto::{coprocessor as coppb, kvrpcpb};
use storage;
use coprocessor;

use super::*;
use super::util::*;

#[derive(Debug)]
pub struct CoprocessorSubTask {
    pub req_context: kvrpcpb::Context,
    pub request: Option<coppb::Request>,
}

impl SnapshotSubTask for CoprocessorSubTask {
    #[inline]
    fn new_next_subtask_builder(&mut self) -> Box<SnapshotNextSubTaskBuilder> {
        box CoprocessorSubTaskSecondBuilder {
            request: self.request.take(),
        }
    }
    #[inline]
    fn get_request_context(&self) -> &kvrpcpb::Context {
        &self.req_context
    }
}

#[derive(Debug)]
struct CoprocessorSubTaskSecondBuilder {
    request: Option<coppb::Request>,
}

impl SnapshotNextSubTaskBuilder for CoprocessorSubTaskSecondBuilder {
    fn build(mut self: Box<Self>, snapshot: Box<storage::Snapshot>) -> Box<SubTask> {
        box CoprocessorSubTaskSecond {
            snapshot: Some(snapshot),
            request: self.request.take().unwrap(),
        }
    }
}

#[derive(Debug)]
struct CoprocessorSubTaskSecond {
    snapshot: Option<Box<storage::Snapshot>>,
    request: coppb::Request,
}

impl SubTask for CoprocessorSubTaskSecond {
    fn async_work(
        mut self: Box<Self>,
        context: &mut WorkerThreadContext,
        on_done: SubTaskCallback,
    ) {
        let endpoint = coprocessor::EndPoint::new(self.snapshot.take().unwrap());
        let on_response = box |res: coppb::Response| {
            on_done(SubTaskResult::Finish(Ok(Value::Coprocessor(res))));
        };
        let request_task = coprocessor::RequestTask::new(
            self.request,
            on_response,
            context.end_point_recursion_limit,
        );
        let statistics = endpoint.handle_request(request_task, context.end_point_batch_row_limit);
        // TODO: handle statistics
    }
}
