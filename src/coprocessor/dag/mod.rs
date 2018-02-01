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

pub mod executor;
pub mod dag;
pub mod expr;
pub use self::dag::DAGContext;

use std::sync::Arc;
use protobuf::{CodedInputStream, Message};
use kvproto::kvrpcpb;
use kvproto::coprocessor as coppb;
use tipb::select::DAGRequest;

use storage::Snapshot;

use super::*;

pub struct CopRequestDAG {
    ctx: Arc<CopContext>,
    body: Option<DAGRequest>,
    raw_req: coppb::Request,
    batch_row_limit: usize,
}

impl CopRequestDAG {
    pub fn new(
        raw_req: coppb::Request,
        recursion_limit: u32,
        batch_row_limit: usize,
    ) -> Result<CopRequestDAG> {
        let mut body = DAGRequest::new();
        {
            let mut is = CodedInputStream::from_bytes(raw_req.get_data());
            is.set_recursion_limit(recursion_limit);
            if let Err(e) = body.merge_from(&mut is) {
                return Err(box_err!(e));
            }
        }
        let ctx = Arc::new(CopContext::new(raw_req.get_context(), "dag"));
        Ok(CopRequestDAG {
            ctx,
            body: Some(body),
            raw_req,
            batch_row_limit,
        })
    }
}

impl CopRequest for CopRequestDAG {
    fn handle(&mut self, snapshot: Box<Snapshot>, stats: &mut CopStats) -> Result<coppb::Response> {
        let ranges = self.raw_req.take_ranges().into_vec();
        let mut dag_ctx = DAGContext::new(
            self.body.take().unwrap(),
            ranges,
            snapshot,
            Arc::clone(&self.ctx),
            self.batch_row_limit,
        )?;
        let res = dag_ctx.handle_request();
        dag_ctx.collect_statistics_into(&mut stats.stats);
        dag_ctx.collect_metrics_into(&mut stats.scan_counter);
        res
    }

    fn get_context(&self) -> &kvrpcpb::Context {
        self.raw_req.get_context()
    }

    fn get_cop_context(&self) -> Arc<CopContext> {
        Arc::clone(&self.ctx)
    }
}
