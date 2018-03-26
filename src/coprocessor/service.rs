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
use futures::sync::mpsc;
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
const COPROCESSOR_BUSY_ERROR_MSG: &str = "coprocessor is busy";

pub struct Service {
    engine: Box<storage::Engine>,
    read_pool: ReadPool<ReadPoolContext>,
    recursion_limit: u32,
    batch_row_limit: usize,
    stream_batch_row_limit: usize,
    stream_channel_size: usize,
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
            stream_channel_size: cfg.end_point_stream_channel_size,
            max_handle_duration: cfg.end_point_request_max_handle_duration.0,
        }
    }

    /// Create a `RequestHandlerBuilder` and returns `Err` if fails.
    fn try_new_request_handler_builder(
        &self,
        req: &coppb::Request,
        is_streaming: bool,
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
                // let start_ts = dag.get_start_ts();
                let req_ctx =
                    ReqContext::new(req.get_context(), table_scan, self.max_handle_duration);
                let batch_row_limit = self.get_batch_row_limit(is_streaming);
                box move |snap| {
                    DAGContext::new(dag, ranges, snap, req_ctx, batch_row_limit)
                        .map(|ctx| ctx.into_boxed())
                }
            }
            REQ_TYPE_ANALYZE => {
                let mut analyze = AnalyzeReq::new();
                box_try!(analyze.merge_from(&mut is));
                let table_scan = analyze.get_tp() == AnalyzeType::TypeColumn;
                // let start_ts = analyze.get_start_ts();
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
                // let start_ts = checksum.get_start_ts();
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
    #[inline]
    fn new_request_handler_builder(
        &self,
        req: &coppb::Request,
        is_streaming: bool,
    ) -> RequestHandlerBuilder {
        self.try_new_request_handler_builder(req, is_streaming)
            .unwrap_or_else(|e| box move |_| Ok(cop_util::ErrorRequestHandler::new(e).into_boxed()))
    }

    #[inline]
    fn get_batch_row_limit(&self, is_streaming: bool) -> usize {
        if is_streaming {
            self.stream_batch_row_limit
        } else {
            self.batch_row_limit
        }
    }

    #[inline]
    fn async_snapshot(
        engine: Box<storage::Engine>,
        ctx: &kvrpcpb::Context,
    ) -> impl Future<Item = Box<storage::Snapshot + 'static>, Error = Error> {
        let (callback, future) = util::future::paired_future_callback();
        let val = engine.async_snapshot(ctx, callback);
        future::result(val)
            .and_then(|_| future.map_err(|cancel| storage::engine::Error::Other(box_err!(cancel))))
            .and_then(|(_ctx, result)| result)
            // map engine::Error -> coprocessor::Error
            .map_err(Error::from)
    }

    #[inline]
    fn run_handler(
        handler: Box<RequestHandler>,
        is_streaming: bool,
    ) -> impl Stream<Item = coppb::Response, Error = Error> {
        stream::unfold((handler, false), move |(mut handler, finished)| {
            // TODO: Shall we check whether it is outdated in each streaming iterate?
            if finished {
                return None;
            }
            if is_streaming {
                match handler.handle_streaming_request() {
                    Ok((None, _)) => {
                        // if we get `None`, stream is always considered finished.
                        None
                    }
                    Ok((Some(resp), finished)) => {
                        let yielded = resp;
                        let next_state = (handler, finished);
                        Some(Ok((yielded, next_state)))
                    }
                    Err(e) => Some(Err(e)),
                }
            } else {
                match handler.handle_request() {
                    Ok(resp) => {
                        let yielded = resp;
                        let next_state = (handler, true);
                        Some(Ok((yielded, next_state)))
                    }
                    Err(e) => Some(Err(e)),
                }
            }
        })
    }

    fn handle_request_by_custom_handler(
        &self,
        context: kvrpcpb::Context,
        handler_builder: RequestHandlerBuilder,
        is_streaming: bool,
    ) -> impl Stream<Item = coppb::Response, Error = ()> {
        let (tx, rx) = mpsc::channel::<Result<coppb::Response>>(self.stream_channel_size);
        let engine = self.engine.clone();
        let priority = readpool::Priority::from(context.get_priority());

        let (tx1, tx2) = (tx.clone(), tx.clone());
        let result = self.read_pool.future_execute(priority, move |_ctxd| {
            Service::async_snapshot(engine, &context)
                .and_then(move |snapshot| {
                    let handler = match handler_builder(snapshot) {
                        Ok(handler) => handler,
                        Err(e) => cop_util::ErrorRequestHandler::new(e).into_boxed(),
                    };
                    future::result(handler.check_if_outdated()).and_then(move |_| {
                        Service::run_handler(handler, is_streaming)
                            .then(Ok::<_, mpsc::SendError<_>>)
                            .forward(tx1)
                            .map_err(|_| Error::Other(box_err!("sink send failed")))
                            .map(|_| ())
                    })
                })
                .map_err(move |e| {
                    stream::once::<_, mpsc::SendError<_>>(Ok(Err(e)))
                        .forward(tx2)
                        .wait()
                        .unwrap();
                })
        });

        match result {
            Err(_) => {
                stream::once::<_, mpsc::SendError<_>>(Ok(Err(Error::Full)))
                    .forward(tx)
                    .wait()
                    .unwrap();
            }
            Ok(cpu_future) => {
                // keep running on the FuturePool
                cpu_future.forget();
            }
        }

        rx.map(|result| match result {
            Ok(resp) => resp,
            Err(e) => {
                let mut metrics = BasicLocalMetrics::default();
                make_error_response(e, &mut metrics)
            }
        })
    }

    /// Specifying custom request handler is useful in tests.
    #[inline]
    fn handle_stream_request_by_custom_handler(
        &self,
        context: kvrpcpb::Context,
        handler_builder: RequestHandlerBuilder,
    ) -> impl Stream<Item = coppb::Response, Error = ()> {
        self.handle_request_by_custom_handler(context, handler_builder, true)
    }

    /// Specifying custom request handler is useful in tests.
    fn handle_unary_request_by_custom_handler(
        &self,
        context: kvrpcpb::Context,
        handler_builder: RequestHandlerBuilder,
    ) -> impl Future<Item = coppb::Response, Error = ()> {
        self.handle_request_by_custom_handler(context, handler_builder, false)
            .into_future()  // called `.next()` in futures 0.2
            .then(|result| {
                let resp = match result {
                    Ok((Some(resp), _)) => resp,
                    Ok((None, _)) => unreachable!(),
                    Err((_, _)) => unreachable!(),
                };
                Ok::<_, ()>(resp)
            })
    }

    #[inline]
    pub fn handle_stream_request(
        &self,
        mut req: coppb::Request,
    ) -> impl Stream<Item = coppb::Response, Error = ()> {
        let handler_builder = self.new_request_handler_builder(&req, true);
        self.handle_stream_request_by_custom_handler(req.take_context(), handler_builder)
    }

    #[inline]
    pub fn handle_unary_request(
        &self,
        mut req: coppb::Request,
    ) -> impl Future<Item = coppb::Response, Error = ()> {
        let handler_builder = self.new_request_handler_builder(&req, false);
        self.handle_unary_request_by_custom_handler(req.take_context(), handler_builder)
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
            let mut server_is_busy_err = errorpb::ServerIsBusy::new();
            server_is_busy_err.set_reason(COPROCESSOR_BUSY_ERROR_MSG.to_owned());
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::vec;
    use std::sync::mpsc;
    use std::thread;

    use storage::engine::{self, TEMP_DIR};

    use tipb::expression::Expr;
    use tipb::executor::Executor;

    /// a `RequestHandler` that is always outdated
    struct OutdatedFixture;

    impl OutdatedFixture {
        pub fn new() -> OutdatedFixture {
            OutdatedFixture {}
        }
    }

    impl RequestHandler for OutdatedFixture {
        fn check_if_outdated(&self) -> Result<()> {
            Err(Error::Outdated(Duration::from_secs(1), "select"))
        }
    }

    /// an unary `RequestHandler` that always produces a fixture.
    struct UnaryFixture {
        handle_duration_millis: u64,
        result: Option<Result<coppb::Response>>,
    }

    impl UnaryFixture {
        pub fn new(result: Result<coppb::Response>) -> UnaryFixture {
            UnaryFixture {
                handle_duration_millis: 0,
                result: Some(result),
            }
        }

        pub fn new_with_duration(
            result: Result<coppb::Response>,
            handle_duration_millis: u64,
        ) -> UnaryFixture {
            UnaryFixture {
                handle_duration_millis,
                result: Some(result),
            }
        }
    }

    impl RequestHandler for UnaryFixture {
        fn handle_request(&mut self) -> Result<coppb::Response> {
            thread::sleep(Duration::from_millis(self.handle_duration_millis));
            self.result.take().unwrap()
        }

        fn check_if_outdated(&self) -> Result<()> {
            Ok(())
        }
    }

    /// a streaming `RequestHandler` that always produces a fixture.
    struct StreamFixture {
        result_len: usize,
        result_iter: vec::IntoIter<Result<coppb::Response>>,
        nth: usize,
    }

    impl StreamFixture {
        pub fn new(result: Vec<Result<coppb::Response>>) -> StreamFixture {
            StreamFixture {
                result_len: result.len(),
                result_iter: result.into_iter(),
                nth: 0,
            }
        }
    }

    impl RequestHandler for StreamFixture {
        fn handle_streaming_request(&mut self) -> Result<(Option<coppb::Response>, bool)> {
            let is_finished = if self.result_len == 0 {
                true
            } else {
                self.nth >= (self.result_len - 1)
            };
            let ret = match self.result_iter.next() {
                None => {
                    assert!(is_finished);
                    Ok((None, is_finished))
                }
                Some(Ok(resp)) => Ok((Some(resp), is_finished)),
                Some(Err(e)) => Err(e),
            };
            self.nth += 1;
            ret
        }

        fn check_if_outdated(&self) -> Result<()> {
            Ok(())
        }
    }

    /// a streaming `RequestHandler` that produces values according a closure.
    struct StreamFromClosure {
        result_generator: Box<Fn(usize) -> HandlerStreamStepResult + Send>,
        nth: usize,
    }

    impl StreamFromClosure {
        pub fn new<F>(result_generator: F) -> StreamFromClosure
        where
            F: Fn(usize) -> HandlerStreamStepResult + Send + 'static,
        {
            StreamFromClosure {
                result_generator: box result_generator,
                nth: 0,
            }
        }
    }

    impl RequestHandler for StreamFromClosure {
        fn handle_streaming_request(&mut self) -> Result<(Option<coppb::Response>, bool)> {
            let result = (self.result_generator)(self.nth);
            self.nth += 1;
            result
        }

        fn check_if_outdated(&self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_outdated_request() {
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(None)
        });
        let service = Service::new(&Config::default(), engine, read_pool);

        // a normal request
        let handler_builder =
            box |_| Ok(UnaryFixture::new(Ok(coppb::Response::new())).into_boxed());
        let resp = service
            .handle_unary_request_by_custom_handler(kvrpcpb::Context::new(), handler_builder)
            .wait()
            .unwrap();
        assert!(resp.get_other_error().is_empty());

        // an outdated request
        let handler_builder = box |_| Ok(OutdatedFixture::new().into_boxed());
        let resp = service
            .handle_unary_request_by_custom_handler(kvrpcpb::Context::new(), handler_builder)
            .wait()
            .unwrap();
        assert_eq!(resp.get_other_error(), OUTDATED_ERROR_MSG);
    }

    #[test]
    fn test_stack_guard() {
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(None)
        });
        let service = Service::new(
            &Config {
                end_point_recursion_limit: 5,
                ..Config::default()
            },
            engine,
            read_pool,
        );

        let req = {
            let mut expr = Expr::new();
            for _ in 0..10 {
                let mut e = Expr::new();
                e.mut_children().push(expr);
                expr = e;
            }
            let mut e = Executor::new();
            e.mut_selection().mut_conditions().push(expr);
            let mut dag = DAGRequest::new();
            dag.mut_executors().push(e);
            let mut req = coppb::Request::new();
            req.set_tp(REQ_TYPE_DAG);
            req.set_data(dag.write_to_bytes().unwrap());
            req
        };

        let resp: coppb::Response = service.handle_unary_request(req).wait().unwrap();
        assert!(!resp.get_other_error().is_empty());
    }

    #[test]
    fn test_invalid_req_type() {
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(None)
        });
        let service = Service::new(&Config::default(), engine, read_pool);

        let mut req = coppb::Request::new();
        req.set_tp(9999);

        let resp: coppb::Response = service.handle_unary_request(req).wait().unwrap();
        assert!(!resp.get_other_error().is_empty());
    }

    #[test]
    fn test_invalid_req_body() {
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(None)
        });
        let service = Service::new(&Config::default(), engine, read_pool);

        let mut req = coppb::Request::new();
        req.set_tp(REQ_TYPE_DAG);
        req.set_data(vec![1, 2, 3]);

        let resp = service.handle_unary_request(req).wait().unwrap();
        assert!(!resp.get_other_error().is_empty());
    }

    #[test]
    fn test_full() {
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let read_pool = ReadPool::new(
            "readpool",
            &readpool::Config {
                normal_concurrency: 1,
                max_tasks_normal: 2,
                ..readpool::Config::default_for_test()
            },
            || || ReadPoolContext::new(None),
        );
        let service = Service::new(&Config::default(), engine, read_pool);

        let (tx, rx) = mpsc::channel();

        // first 2 requests are processed as normal and laters are returned as errors
        for i in 0..5 {
            let mut response = coppb::Response::new();
            response.set_data(vec![1, 2, i]);

            let mut context = kvrpcpb::Context::new();
            context.set_priority(kvrpcpb::CommandPri::Normal);

            let handler_builder =
                box |_| Ok(UnaryFixture::new_with_duration(Ok(response), 1000).into_boxed());
            let future = service.handle_unary_request_by_custom_handler(context, handler_builder);
            let tx = tx.clone();
            thread::spawn(move || tx.send(future.wait().unwrap()));
            thread::sleep(Duration::from_millis(100));
        }

        // verify
        for _ in 2..5 {
            let resp: coppb::Response = rx.recv().unwrap();
            assert_eq!(resp.get_data().len(), 0);
            assert!(resp.has_region_error());
            assert!(resp.get_region_error().has_server_is_busy());
            assert_eq!(
                resp.get_region_error().get_server_is_busy().get_reason(),
                COPROCESSOR_BUSY_ERROR_MSG
            );
        }
        for i in 0..2 {
            let resp = rx.recv().unwrap();
            assert_eq!(resp.get_data(), [1, 2, i]);
            assert!(!resp.has_region_error());
        }
    }

    #[test]
    fn test_error_unary_response() {
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(None)
        });
        let service = Service::new(&Config::default(), engine, read_pool);

        let handler_builder =
            box |_| Ok(UnaryFixture::new(Err(Error::Other(box_err!("foo")))).into_boxed());
        let resp = service
            .handle_unary_request_by_custom_handler(kvrpcpb::Context::new(), handler_builder)
            .wait()
            .unwrap();
        assert_eq!(resp.get_data().len(), 0);
        assert!(!resp.get_other_error().is_empty());
    }

    #[test]
    #[allow(needless_range_loop)]
    fn test_error_streaming_response() {
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(None)
        });
        let service = Service::new(&Config::default(), engine, read_pool);

        // Fail immediately
        let handler_builder =
            box |_| Ok(StreamFixture::new(vec![Err(Error::Other(box_err!("foo")))]).into_boxed());
        let resp_vec = service
            .handle_stream_request_by_custom_handler(kvrpcpb::Context::new(), handler_builder)
            .collect()
            .wait()
            .unwrap();
        assert_eq!(resp_vec.len(), 1);
        assert_eq!(resp_vec[0].get_data().len(), 0);
        assert!(!resp_vec[0].get_other_error().is_empty());

        // Fail after some success responses
        let mut responses = Vec::new();
        for i in 0..5 {
            let mut resp = coppb::Response::new();
            resp.set_data(vec![1, 2, i]);
            responses.push(Ok(resp));
        }
        responses.push(Err(Error::Other(box_err!("foo"))));

        let handler_builder = box move |_| Ok(StreamFixture::new(responses).into_boxed());
        let resp_vec = service
            .handle_stream_request_by_custom_handler(kvrpcpb::Context::new(), handler_builder)
            .collect()
            .wait()
            .unwrap();
        assert_eq!(resp_vec.len(), 6);
        for i in 0..5 {
            assert_eq!(resp_vec[i].get_data(), [1, 2, i as u8]);
        }
        assert_eq!(resp_vec[5].get_data().len(), 0);
        assert!(!resp_vec[5].get_other_error().is_empty());
    }

    #[test]
    fn test_empty_streaming_response() {
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(None)
        });
        let service = Service::new(&Config::default(), engine, read_pool);

        let handler_builder = box |_| Ok(StreamFixture::new(vec![]).into_boxed());
        let resp_vec = service
            .handle_stream_request_by_custom_handler(kvrpcpb::Context::new(), handler_builder)
            .collect()
            .wait()
            .unwrap();
        assert_eq!(resp_vec.len(), 0);
    }

    // TODO: Test panic?

    #[test]
    fn test_special_streaming_handlers() {
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(None)
        });
        let service = Service::new(&Config::default(), engine, read_pool);

        // handler returns `finished == true` should not be called again.
        let handler = StreamFromClosure::new(|nth| match nth {
            0 => {
                let mut resp = coppb::Response::new();
                resp.set_data(vec![1, 2, 7]);
                Ok((Some(resp), true))
            }
            _ => Err(box_err!("unreachable")),
        });
        let handler_builder = box |_| Ok(handler.into_boxed());
        let resp_vec = service
            .handle_stream_request_by_custom_handler(kvrpcpb::Context::new(), handler_builder)
            .collect()
            .wait()
            .unwrap();
        assert_eq!(resp_vec.len(), 1);
        assert_eq!(resp_vec[0].get_data(), [1, 2, 7]);

        // handler returns `None` but `finished == false` should not be called again.
        let handler = StreamFromClosure::new(|nth| match nth {
            0 => {
                let mut resp = coppb::Response::new();
                resp.set_data(vec![1, 2, 13]);
                Ok((Some(resp), false))
            }
            1 => Ok((None, false)),
            _ => Err(box_err!("unreachable")),
        });
        let handler_builder = box |_| Ok(handler.into_boxed());
        let resp_vec = service
            .handle_stream_request_by_custom_handler(kvrpcpb::Context::new(), handler_builder)
            .collect()
            .wait()
            .unwrap();
        assert_eq!(resp_vec.len(), 1);
        assert_eq!(resp_vec[0].get_data(), [1, 2, 13]);

        // handler returns `Err(..)` should not be called again.
        let handler = StreamFromClosure::new(|nth| match nth {
            0 => {
                let mut resp = coppb::Response::new();
                resp.set_data(vec![1, 2, 23]);
                Ok((Some(resp), false))
            }
            1 => Err(box_err!("foo")),
            _ => Err(box_err!("unreachable")),
        });
        let handler_builder = box |_| Ok(handler.into_boxed());
        let resp_vec = service
            .handle_stream_request_by_custom_handler(kvrpcpb::Context::new(), handler_builder)
            .collect()
            .wait()
            .unwrap();
        assert_eq!(resp_vec.len(), 2);
        assert_eq!(resp_vec[0].get_data(), [1, 2, 23]);
        assert!(!resp_vec[1].get_other_error().is_empty());
    }
}
