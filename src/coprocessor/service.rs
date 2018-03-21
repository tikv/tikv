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

use futures::{Stream, Future};
use protobuf::CodedInputStream;

use kvproto::{kvrpcpb, coprocessor as coppb};
use tipb::select::DAGRequest;
use tipb::analyze::AnalyzeReq;
use tipb::checksum::ChecksumRequest;

use util;

use super::*;
use super::util as cop_util;
use super::dag::DAGContext;
use super::statistics::analyze::AnalyzeContext;
use super::checksum::ChecksumContext;

#[derive(Clone)]
pub struct Service {
    recursion_limit: u32,
}

impl util::AssertSend for Service {}

impl Service {
    pub fn new() -> Service {
        Service {

        }
    }

    fn handle_request(&self, req: coppb::Request, use_streaming_interface: bool) -> impl Stream<Item = coppb::Response, Error = ()> {
        let request_handler = self.build_request_handler(req);

    }

    fn try_build_request_handler(&self, req: coppb::Request) -> Result<Box<RequestHandler>> {
        let mut is = CodedInputStream::from_bytes(req.get_data());
        is.set_recursion_limit(self.recursion_limit);

        match req.get_tp() {
            REQ_TYPE_DAG => {
                let mut dag = DAGRequest::new();
                box_try!(dag.merge_from(&mut is));
                let mut table_scan = false;
                if let Some(scan) = dag.get_executors().iter().next() {
                    table_scan = scan.get_tp() == ExecType::TypeTableScan;
                }
                let start_ts = dag.get_start_ts();
                let req_ctx = ReqContext::new(req.get_context(), table_scan, self.max_handle_duration);
                return box_try!(DAGContext::new())
            },
            REQ_TYPE_ANALYZE => {
                let mut analyze = AnalyzeReq::new();
                box_try!(analyze.merge_from(&mut is));
                let table_scan = analyze.get_tp() == AnalyzeType::TypeColumn;
                let start_ts = analyze.get_start_ts();
                return box_try!(AnalyzeContext::new());
            },
            REQ_TYPE_CHECKSUM => {
                let mut checksum = ChecksumRequest::new();
                box_try!(checksum.merge_from(&mut is));
                let table_scan = checksum.get_scan_on() == ChecksumScanOn::Table;
                let start_ts = checksum.get_start_ts();
                return box_try!(ChecksumContext::new());
            }
            tp => return Err(box_err!("unsupported tp {}", tp)),
        }
    }

    fn build_request_handler(&self, req: coppb::Request) -> Box<RequestHandler> {
        self.try_build_request_handler(req)
            .unwrap_or_else(|e| {
                cop_util::ErrorRequestHandler::from_error(e)
            })
    }

    pub fn handle_stream_request(&self, req: coppb::Request) -> impl Stream<Item = coppb::Response, Error = ()> {
        self.handle_request(req, true)
    }

    pub fn handle_unary_request(&self, req: coppb::Request) -> impl Future<Item = coppb::Response, Error = ()>  {
        self.handle_request(req, false).take(1).collect().map(|responses| responses[0])
    }
}