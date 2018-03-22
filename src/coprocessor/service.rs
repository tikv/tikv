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

use futures::{Future, Stream, stream};
use protobuf::{CodedInputStream, Message};

use kvproto::{coprocessor as coppb, kvrpcpb};
use tipb::executor::ExecType;
use tipb::select::DAGRequest;
use tipb::analyze::{AnalyzeReq, AnalyzeType};
use tipb::checksum::{ChecksumRequest, ChecksumScanOn};

use util;
use storage;
use server::Config;
use server::readpool::{self, ReadPool};

use super::*;
use super::util as cop_util;
use super::dag::DAGContext;
use super::statistics::analyze::AnalyzeContext;
use super::checksum::ChecksumContext;

#[derive(Clone)]
pub struct Service {
    engine: Box<storage::Engine>,
    read_pool: ReadPool<ReadPoolContext>,
    recursion_limit: u32,
    batch_row_limit: usize,
    stream_batch_row_limit: usize,
    max_handle_duration: Duration,
}

impl util::AssertSend for Service {}

impl Service {
    pub fn new(
        cfg: &Config,
        engine: Box<storage::Engine>,
        read_pool: ReadPool<ReadPoolContext>,
    ) -> Service {
        Service {
            engine,
            read_pool,
            recursion_limit: cfg.end_point_recursion_limit,
            batch_row_limit: cfg.end_point_batch_row_limit,
            stream_batch_row_limit: cfg.end_point_stream_batch_row_limit,
            max_handle_duration: cfg.end_point_request_max_handle_duration.0,
        }
    }

    fn get_batch_row_limit(&self, is_streaming: bool) -> usize {
        if is_streaming {
            self.stream_batch_row_limit
        } else {
            self.batch_row_limit
        }
    }

    fn handle_request(
        &self,
        req: coppb::Request,
        use_streaming_interface: bool,
    ) -> Box<Stream<Item = coppb::Response, Error = ()>> {
        let request_handler_builder = match self.new_request_handler_builder(req) {
            Ok(builder) => builder,
            Err(e) => return box stream::once::<_, ()>(make_error_response(e)),
        };
        let engine = self.engine.clone();
        let priority = readpool::Priority::from(req.get_context().get_priority());
        box stream::once::<_, ()>(
            self.read_pool.future_execute(priority, move |ctxd| {
                engine.async_snapshot()
            })
        )
    }

    /// Create a `RequestHandlerBuilder` and returns `Err` if fails.
    fn new_request_handler_builder(
        &self,
        mut req: coppb::Request,
    ) -> Result<Box<RequestHandlerBuilder>> {
        let mut is = CodedInputStream::from_bytes(req.get_data());
        is.set_recursion_limit(self.recursion_limit);

        let ranges = req.take_ranges().into_vec();

        let builder = match req.get_tp() {
            REQ_TYPE_DAG => {
                let mut dag = DAGRequest::new();
                box_try!(dag.merge_from(&mut is));
                let mut table_scan = false;
                if let Some(scan) = dag.get_executors().iter().next() {
                    table_scan = scan.get_tp() == ExecType::TypeTableScan;
                }
                let start_ts = dag.get_start_ts();
                let req_ctx =
                    ReqContext::new(req.get_context(), table_scan, self.max_handle_duration);
                box move |snap| DAGContext::new(dag, ranges, snap, req_ctx)
            }
            REQ_TYPE_ANALYZE => {
                let mut analyze = AnalyzeReq::new();
                box_try!(analyze.merge_from(&mut is));
                let table_scan = analyze.get_tp() == AnalyzeType::TypeColumn;
                let start_ts = analyze.get_start_ts();
                let req_ctx =
                    ReqContext::new(req.get_context(), table_scan, self.max_handle_duration);
                box move |snap| AnalyzeContext::new(analyze, ranges, snap, &req_ctx)
            }
            REQ_TYPE_CHECKSUM => {
                let mut checksum = ChecksumRequest::new();
                box_try!(checksum.merge_from(&mut is));
                let table_scan = checksum.get_scan_on() == ChecksumScanOn::Table;
                let start_ts = checksum.get_start_ts();
                let req_ctx =
                    ReqContext::new(req.get_context(), table_scan, self.max_handle_duration);
                box move |snap| ChecksumContext::new(checksum, ranges, snap, &req_ctx)
            }
            tp => return Err(box_err!("unsupported tp {}", tp)),
        };
        Ok(builder)
    }
//
//    /// Creates a `RequestHandlerBuilder`.
//    fn new_request_handler_builder(&self, req: coppb::Request) -> Box<RequestHandlerBuilder> {
//        self.try_new_request_handler_builder(req)
//            .unwrap_or_else(|e| box move |_| Ok(cop_util::ErrorRequestHandler::from_error(e)))
//    }

    pub fn handle_stream_request(
        &self,
        req: coppb::Request,
    ) -> impl Stream<Item = coppb::Response, Error = ()> {
        self.handle_request(req, true)
    }

    pub fn handle_unary_request(
        &self,
        req: coppb::Request,
    ) -> impl Future<Item = coppb::Response, Error = ()> {
        self.handle_request(req, false)
            .take(1)
            .fold(None, |_, resp| Some(resp))
            .then(|result| {
                let resp = match result {
                    Ok(Some(resp)) => resp,
                    Ok(None) => (), // TODO
                    Err(e) => (),   // TODO
                };
                Ok::<_, ()>(resp)
            })
    }
}
