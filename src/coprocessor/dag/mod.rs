// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod storage_impl;

pub use self::storage_impl::TiKVStorage;

use async_trait::async_trait;
use kvproto::coprocessor::{KeyRange, Response};
use protobuf::Message;
use tidb_query_common::storage::IntervalRange;
use tipb::{DagRequest, SelectResponse, StreamResponse};

use crate::coprocessor::metrics::*;
use crate::coprocessor::{Deadline, RequestHandler, Result};
use crate::storage::{Statistics, Store};

pub struct DagHandlerBuilder<S: Store + 'static> {
    req: DagRequest,
    ranges: Vec<KeyRange>,
    store: S,
    data_version: Option<u64>,
    deadline: Deadline,
    batch_row_limit: usize,
    is_streaming: bool,
    is_cache_enabled: bool,
    enable_batch_if_possible: bool,
}

impl<S: Store + 'static> DagHandlerBuilder<S> {
    pub fn new(
        req: DagRequest,
        ranges: Vec<KeyRange>,
        store: S,
        deadline: Deadline,
        batch_row_limit: usize,
        is_streaming: bool,
        is_cache_enabled: bool,
    ) -> Self {
        DagHandlerBuilder {
            req,
            ranges,
            store,
            data_version: None,
            deadline,
            batch_row_limit,
            is_streaming,
            is_cache_enabled,
            enable_batch_if_possible: true,
        }
    }

    pub fn data_version(mut self, data_version: Option<u64>) -> Self {
        self.data_version = data_version;
        self
    }

    pub fn enable_batch_if_possible(mut self, enable_batch_if_possible: bool) -> Self {
        self.enable_batch_if_possible = enable_batch_if_possible;
        self
    }

    pub fn build(self) -> Result<Box<dyn RequestHandler>> {
        // TODO: support batch executor while handling server-side streaming requests
        // https://github.com/tikv/tikv/pull/5945
        if self.enable_batch_if_possible && !self.is_streaming {
            tidb_query_vec_executors::runner::BatchExecutorsRunner::check_supported(
                self.req.get_executors(),
            )?;
            COPR_DAG_REQ_COUNT.with_label_values(&["batch"]).inc();
            Ok(BatchDAGHandler::new(
                self.req,
                self.ranges,
                self.store,
                self.data_version,
                self.deadline,
                self.is_cache_enabled,
            )?
            .into_boxed())
        } else {
            COPR_DAG_REQ_COUNT.with_label_values(&["normal"]).inc();
            Ok(DAGHandler::new(
                self.req,
                self.ranges,
                self.store,
                self.data_version,
                self.deadline,
                self.batch_row_limit,
                self.is_streaming,
                self.is_cache_enabled,
            )?
            .into_boxed())
        }
    }
}

pub struct DAGHandler {
    runner: tidb_query_normal_executors::ExecutorsRunner<Statistics>,
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
        is_cache_enabled: bool,
    ) -> Result<Self> {
        Ok(Self {
            runner: tidb_query_normal_executors::ExecutorsRunner::from_request(
                req,
                ranges,
                TiKVStorage::new(store, is_cache_enabled),
                deadline,
                batch_row_limit,
                is_streaming,
            )?,
            data_version,
        })
    }
}

#[async_trait]
impl RequestHandler for DAGHandler {
    async fn handle_request(&mut self) -> Result<Response> {
        let result = self.runner.handle_request();
        handle_qe_response(result, self.runner.can_be_cached(), self.data_version)
    }

    fn handle_streaming_request(&mut self) -> Result<(Option<Response>, bool)> {
        handle_qe_stream_response(self.runner.handle_streaming_request())
    }

    fn collect_scan_statistics(&mut self, dest: &mut Statistics) {
        self.runner.collect_storage_stats(dest);
    }
}

pub struct BatchDAGHandler {
    runner: tidb_query_vec_executors::runner::BatchExecutorsRunner<Statistics>,
    data_version: Option<u64>,
}

impl BatchDAGHandler {
    pub fn new<S: Store + 'static>(
        req: DagRequest,
        ranges: Vec<KeyRange>,
        store: S,
        data_version: Option<u64>,
        deadline: Deadline,
        is_cache_enabled: bool,
    ) -> Result<Self> {
        Ok(Self {
            runner: tidb_query_vec_executors::runner::BatchExecutorsRunner::from_request(
                req,
                ranges,
                TiKVStorage::new(store, is_cache_enabled),
                deadline,
            )?,
            data_version,
        })
    }
}

#[async_trait]
impl RequestHandler for BatchDAGHandler {
    async fn handle_request(&mut self) -> Result<Response> {
        let result = self.runner.handle_request().await;
        handle_qe_response(result, self.runner.can_be_cached(), self.data_version)
    }

    fn collect_scan_statistics(&mut self, dest: &mut Statistics) {
        self.runner.collect_storage_stats(dest);
    }
}

fn handle_qe_response(
    result: tidb_query_common::Result<SelectResponse>,
    can_be_cached: bool,
    data_version: Option<u64>,
) -> Result<Response> {
    use tidb_query_common::error::ErrorInner;

    match result {
        Ok(sel_resp) => {
            let mut resp = Response::default();
            resp.set_data(box_try!(sel_resp.write_to_bytes()));
            resp.set_can_be_cached(can_be_cached);
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
                resp.set_can_be_cached(can_be_cached);
                resp.set_is_cache_hit(false);
                Ok(resp)
            }
        },
    }
}

fn handle_qe_stream_response(
    result: tidb_query_common::Result<(Option<(StreamResponse, IntervalRange)>, bool)>,
) -> Result<(Option<Response>, bool)> {
    use tidb_query_common::error::ErrorInner;

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
