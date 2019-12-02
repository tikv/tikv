// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod storage_impl;

pub use self::storage_impl::TiKVStorage;

use kvproto::coprocessor::{KeyRange, Response};
use protobuf::Message;
use tidb_query::storage::IntervalRange;
use tipb::{DagRequest, SelectResponse, StreamResponse};

use crate::coprocessor::metrics::*;
use crate::coprocessor::{Deadline, RequestHandler, Result};
use crate::storage::{Statistics, Store};

pub fn build_handler<S: Store + 'static>(
    req: DagRequest,
    ranges: Vec<KeyRange>,
    start_ts: u64,
    store: S,
    data_version: Option<u64>,
    deadline: Deadline,
    batch_row_limit: usize,
    is_streaming: bool,
    enable_batch_if_possible: bool,
) -> Result<Box<dyn RequestHandler>> {
    let mut is_batch = false;
    if enable_batch_if_possible && !is_streaming {
        let is_supported =
            tidb_query::batch::runner::BatchExecutorsRunner::check_supported(req.get_executors());
        if let Err(e) = is_supported {
            // Not supported, will fallback to normal executor.
            // To avoid user worries, let's output success message.
            debug!("Successfully use normal Coprocessor query engine"; "start_ts" => start_ts, "reason" => %e);
        } else {
            is_batch = true;
        }
    }

    if is_batch {
        COPR_DAG_REQ_COUNT.with_label_values(&["batch"]).inc();
        Ok(BatchDAGHandler::new(req, ranges, store, data_version, deadline)?.into_boxed())
    } else {
        COPR_DAG_REQ_COUNT.with_label_values(&["normal"]).inc();
        Ok(DAGHandler::new(
            req,
            ranges,
            store,
            data_version,
            deadline,
            batch_row_limit,
            is_streaming,
        )?
        .into_boxed())
    }
}

pub struct DAGHandler {
    runner: tidb_query::executor::ExecutorsRunner<Statistics>,
    data_version: Option<u64>,
}

impl DAGHandler {
    pub fn new<S: Store + 'static>(
        req: DagRequest,
        ranges: Vec<KeyRange>,
        store: S,
        data_version: Option<u64>,
        deadline: Deadline,
        batch_row_limit: usize,
        is_streaming: bool,
    ) -> Result<Self> {
        Ok(Self {
            runner: tidb_query::executor::ExecutorsRunner::from_request(
                req,
                ranges,
                TiKVStorage::from(store),
                deadline,
                batch_row_limit,
                is_streaming,
            )?,
            data_version,
        })
    }
}

impl RequestHandler for DAGHandler {
    fn handle_request(&mut self) -> Result<Response> {
        handle_qe_response(self.runner.handle_request(), self.data_version)
    }

    fn handle_streaming_request(&mut self) -> Result<(Option<Response>, bool)> {
        handle_qe_stream_response(self.runner.handle_streaming_request())
    }

    fn collect_scan_statistics(&mut self, dest: &mut Statistics) {
        self.runner.collect_storage_stats(dest);
    }
}

pub struct BatchDAGHandler {
    runner: tidb_query::batch::runner::BatchExecutorsRunner<Statistics>,
    data_version: Option<u64>,
}

impl BatchDAGHandler {
    pub fn new<S: Store + 'static>(
        req: DagRequest,
        ranges: Vec<KeyRange>,
        store: S,
        data_version: Option<u64>,
        deadline: Deadline,
    ) -> Result<Self> {
        Ok(Self {
            runner: tidb_query::batch::runner::BatchExecutorsRunner::from_request(
                req,
                ranges,
                TiKVStorage::from(store),
                deadline,
            )?,
            data_version,
        })
    }
}

impl RequestHandler for BatchDAGHandler {
    fn handle_request(&mut self) -> Result<Response> {
        handle_qe_response(self.runner.handle_request(), self.data_version)
    }

    fn collect_scan_statistics(&mut self, dest: &mut Statistics) {
        self.runner.collect_storage_stats(dest);
    }
}

fn handle_qe_response(
    result: tidb_query::Result<SelectResponse>,
    data_version: Option<u64>,
) -> Result<Response> {
    use tidb_query::error::ErrorInner;

    match result {
        Ok(sel_resp) => {
            let mut resp = Response::default();
            resp.set_data(box_try!(sel_resp.write_to_bytes()));
            resp.set_is_cache_hit(false);
            if let Some(v) = data_version {
                resp.set_cache_last_version(v);
            }
            Ok(resp)
        }
        Err(err) => match *err.0 {
            ErrorInner::Storage(err) => Err(err.into()),
            ErrorInner::Evaluate(err) => {
                let mut resp = Response::default();
                let mut sel_resp = SelectResponse::default();
                sel_resp.mut_error().set_code(err.code());
                sel_resp.mut_error().set_msg(err.to_string());
                resp.set_data(box_try!(sel_resp.write_to_bytes()));
                resp.set_is_cache_hit(false);
                Ok(resp)
            }
        },
    }
}

fn handle_qe_stream_response(
    result: tidb_query::Result<(Option<(StreamResponse, IntervalRange)>, bool)>,
) -> Result<(Option<Response>, bool)> {
    use tidb_query::error::ErrorInner;

    match result {
        Ok((Some((s_resp, range)), finished)) => {
            let mut resp = Response::default();
            resp.set_data(box_try!(s_resp.write_to_bytes()));
            resp.mut_range().set_start(range.lower_inclusive);
            resp.mut_range().set_end(range.upper_exclusive);
            Ok((Some(resp), finished))
        }
        Ok((None, finished)) => Ok((None, finished)),
        Err(err) => match *err.0 {
            ErrorInner::Storage(err) => Err(err.into()),
            ErrorInner::Evaluate(err) => {
                let mut resp = Response::default();
                let mut s_resp = StreamResponse::default();
                s_resp.mut_error().set_code(err.code());
                s_resp.mut_error().set_msg(err.to_string());
                resp.set_data(box_try!(s_resp.write_to_bytes()));
                Ok((Some(resp), true))
            }
        },
    }
}
