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

use futures::{future, stream, Future, Stream};
use protobuf::{CodedInputStream, Message};

use kvproto::{coprocessor as coppb, errorpb, kvrpcpb};
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
use super::local_metrics::BasicLocalMetrics;

const OUTDATED_ERROR_MSG: &str = "request outdated.";
const ENDPOINT_IS_BUSY: &str = "endpoint is busy";

pub struct Service {
    engine: Box<storage::Engine>,
    read_pool: ReadPool<ReadPoolContext>,
    recursion_limit: u32,
    batch_row_limit: usize,
    stream_batch_row_limit: usize,
    max_handle_duration: Duration,
}

impl Clone for Service {
    fn clone(&self) -> Self {
        Service {
            engine: self.engine.clone(),
            read_pool: self.read_pool.clone(),
            ..*self
        }
    }
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

    /// Create a `RequestHandlerBuilder` and returns `Err` if fails.
    fn try_new_request_handler_builder(
        &self,
        req: &coppb::Request,
    ) -> Result<RequestHandlerBuilder> {
        let mut is = CodedInputStream::from_bytes(req.get_data());
        is.set_recursion_limit(self.recursion_limit);

        let ranges = req.get_ranges().to_vec();

        let builder: RequestHandlerBuilder = match req.get_tp() {
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
                box move |snap| {
                    DAGContext::new(dag, ranges, snap, req_ctx).map(|ctx| ctx.into_boxed())
                }
            }
            REQ_TYPE_ANALYZE => {
                let mut analyze = AnalyzeReq::new();
                box_try!(analyze.merge_from(&mut is));
                let table_scan = analyze.get_tp() == AnalyzeType::TypeColumn;
                let start_ts = analyze.get_start_ts();
                let req_ctx =
                    ReqContext::new(req.get_context(), table_scan, self.max_handle_duration);
                box move |snap| {
                    AnalyzeContext::new(analyze, ranges, snap, req_ctx).map(|ctx| ctx.into_boxed())
                }
            }
            REQ_TYPE_CHECKSUM => {
                let mut checksum = ChecksumRequest::new();
                box_try!(checksum.merge_from(&mut is));
                let table_scan = checksum.get_scan_on() == ChecksumScanOn::Table;
                let start_ts = checksum.get_start_ts();
                let req_ctx =
                    ReqContext::new(req.get_context(), table_scan, self.max_handle_duration);
                box move |snap| {
                    ChecksumContext::new(checksum, ranges, snap, req_ctx)
                        .map(|ctx| ctx.into_boxed())
                }
            }
            tp => return Err(box_err!("unsupported tp {}", tp)),
        };
        Ok(builder)
    }

    /// Creates a `RequestHandlerBuilder`.
    fn new_request_handler_builder(&self, req: &coppb::Request) -> RequestHandlerBuilder {
        self.try_new_request_handler_builder(req)
            .unwrap_or_else(|e| {
                box move |_| Ok(cop_util::ErrorRequestHandler::from_error(e).into_boxed())
            })
    }

    fn get_batch_row_limit(&self, is_streaming: bool) -> usize {
        if is_streaming {
            self.stream_batch_row_limit
        } else {
            self.batch_row_limit
        }
    }

    fn async_snapshot(
        engine: Box<storage::Engine>,
        ctx: &kvrpcpb::Context,
    ) -> impl Future<Item = Box<storage::Snapshot>, Error = Error> {
        let (callback, future) = util::future::paired_future_callback();
        let val = engine.async_snapshot(ctx, callback);
        future::result(val)
            .and_then(|_| future.map_err(|cancel| storage::Engine::Error::Other(box_err!(cancel))))
            .and_then(|(_ctx, result)| result)
            // map engine::Error -> coprocessor::Error
            .map_err(Error::from)
    }

    fn handle_request(
        &self,
        req: coppb::Request,
        use_streaming_interface: bool,
    ) -> impl Stream<Item = coppb::Response, Error = ()> {
        let batch_row_limit = self.get_batch_row_limit(use_streaming_interface);
        let request_handler_builder = self.new_request_handler_builder(&req);
        let engine = self.engine.clone();
        let priority = readpool::Priority::from(req.get_context().get_priority());
        let result = self.read_pool.future_execute(priority, move |_ctxd| {
            Service::async_snapshot(engine, req.get_context()).and_then(|snapshot| {
                let mut handler: Box<RequestHandler> = match request_handler_builder(snapshot) {
                    Ok(handler) => handler,
                    Err(e) => cop_util::ErrorRequestHandler::from_error(e).into_boxed(),
                };
                handler.check_if_outdated()?;
                let resp_stream: Box<Stream<Item = _, Error = _> + Send> =
                    if use_streaming_interface {
                        box stream::once(handler.handle_request(batch_row_limit))
                    } else {
                        box stream::unfold((handler, false), move |(mut handler, finished)| {
                            if finished {
                                return None;
                            }
                            match handler.handle_streaming_request(batch_row_limit) {
                                Ok((None, _)) => None,
                                Ok((Some(resp), finished)) => {
                                    let yielded = resp;
                                    let next_state = (handler, finished);
                                    Some(Ok((yielded, next_state)))
                                }
                                Err(e) => Some(Err(e)),
                            }
                        })
                    };
                Ok(resp_stream)
            })
        });
        let future_of_stream = future::result(result).map_err(|_| Error::Full).flatten();
        stream::unfold(true, move |is_initial| {
            // TODO: Can be simplified using `stream::once(future_of_stream)` in futures 0.2.
            if !is_initial {
                return None;
            }
            Some(future_of_stream.map(|stream| (stream, false)))
        }).flatten()
            .then(|result| {
                // TODO: Can be simplified using `recover()` in futures 0.2.
                let resp = match result {
                    Ok(resp) => resp,
                    Err(e) => {
                        let mut metrics = BasicLocalMetrics::default();
                        make_error_response(e, &mut metrics)
                    }
                };
                Ok::<_, ()>(resp)
            })
    }

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
            .fold(None, |_, resp| Ok(Some(resp)))
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

fn make_error_response(e: Error, metrics: &mut BasicLocalMetrics) -> coppb::Response {
    let mut resp = coppb::Response::new();
    let tag = match e {
        Error::Region(e) => {
            let tag = storage::get_tag_from_header(&e);
            resp.set_region_error(e);
            tag
        }
        Error::Locked(info) => {
            resp.set_locked(info);
            "lock"
        }
        Error::Outdated(elapsed, scan_tag) => {
            metrics
                .outdate_time
                .with_label_values(&[scan_tag])
                .observe(elapsed.as_secs() as f64);
            resp.set_other_error(OUTDATED_ERROR_MSG.to_owned());
            "outdated"
        }
        Error::Full => {
            let mut errorpb = errorpb::Error::new();
            errorpb.set_message(format!("running batches reach limit"));
            let mut server_is_busy_err = errorpb::ServerIsBusy::new();
            server_is_busy_err.set_reason(ENDPOINT_IS_BUSY.to_owned());
            errorpb.set_server_is_busy(server_is_busy_err);
            resp.set_region_error(errorpb);
            "full"
        }
        Error::Other(_) => {
            resp.set_other_error(format!("{}", e));
            "other"
        }
    };
    metrics.error_cnt.with_label_values(&[tag]).inc();
    resp
}
