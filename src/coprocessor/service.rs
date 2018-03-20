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

use util;

use super::*;

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

    fn handle_request(&self, use_streaming_interface: bool) -> impl Stream<Item = coppb::Response, Error = ()> {

    }

    fn build_request_handler(&self, req: coppb::Request) -> Box<RequestHandler> {
        let mut is = CodedInputStream::from_bytes(req.get_data());
        is.set_recursion_limit(self.recursion_limit);

        let mut table_scan = false;
        let (start_ts, cop_req) = match req.get_tp() {
            REQ_TYPE_DAG => {
                let mut dag = DAGRequest::new();
                box_try!(dag.merge_from(&mut is));
                if let Some(scan) = dag.get_executors().iter().next() {
                    table_scan = scan.get_tp() == ExecType::TypeTableScan;
                }
                (dag.get_start_ts(), CopRequest::DAG(dag))
            }
            REQ_TYPE_ANALYZE => {
                let mut analyze = AnalyzeReq::new();
                box_try!(analyze.merge_from(&mut is));
                table_scan = analyze.get_tp() == AnalyzeType::TypeColumn;
                (analyze.get_start_ts(), CopRequest::Analyze(analyze))
            }
            REQ_TYPE_CHECKSUM => {
                let mut checksum = ChecksumRequest::new();
                box_try!(checksum.merge_from(&mut is));
                table_scan = checksum.get_scan_on() == ChecksumScanOn::Table;
                (checksum.get_start_ts(), CopRequest::Checksum(checksum))
            }
            tp => return Err(box_err!("unsupported tp {}", tp)),
        };
    }

    pub fn handle_stream_request(&self) -> impl Stream<Item = coppb::Response, Error = ()> {
        self.handle_request(true)
    }

    pub fn handle_unary_request(&self) -> impl Future<Item = coppb::Response, Error = ()>  {
        self.handle_request(false).take(1).collect().map(|responses| responses[0])
    }
}