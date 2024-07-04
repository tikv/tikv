// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Handles simple SQL query executors locally.
//!
//! Most TiDB read queries are processed by Coprocessor instead of KV interface.
//! By doing so, the CPU of TiKV nodes can be utilized for computing and the
//! amount of data to transfer can be reduced (i.e. filtered at TiKV side).
//!
//! Notice that Coprocessor handles more than simple SQL query executors (DAG
//! request). It also handles analyzing requests and checksum requests.
//!
//! The entry point of handling all coprocessor requests is `Endpoint`. Common
//! steps are:
//! - Parse the request into a DAG request, Checksum request or Analyze request.
//! - Retrieve a snapshot from the underlying engine according to the given
//!   timestamp.
//! - Build corresponding request handlers from the snapshot and request detail.
//! - Run request handlers once (for unary requests) or multiple times (for
//!   streaming requests) on a future thread pool.
//! - Return handling result as a response.
//!
//! Please refer to `Endpoint` for more details.

#![allow(clippy::diverging_sub_expression)]

mod cache;
mod checksum;
mod config_manager;
pub mod dag;
mod endpoint;
mod error;
mod interceptors;
pub(crate) mod metrics;
pub mod readpool_impl;
mod statistics;
mod tracker;

use std::sync::Arc;

use async_trait::async_trait;
pub use checksum::checksum_crc64_xor;
use engine_traits::PerfLevel;
use kvproto::{coprocessor as coppb, kvrpcpb};
use lazy_static::lazy_static;
use metrics::ReqTag;
use rand::prelude::*;
use tidb_query_common::execute_stats::ExecSummary;
use tikv_alloc::{mem_trace, Id, MemoryTrace, MemoryTraceGuard};
use tikv_util::{deadline::Deadline, memory::HeapSize, time::Duration};
use tipb::{DagRequest, FieldType, TableScan};
use txn_types::TsSet;

pub use self::{
    endpoint::Endpoint,
    error::{Error, Result},
};
use crate::storage::{mvcc::TimeStamp, Statistics};

pub const REQ_TYPE_DAG: i64 = 103;
pub const REQ_TYPE_ANALYZE: i64 = 104;
pub const REQ_TYPE_CHECKSUM: i64 = 105;

pub const REQ_FLAG_TIDB_SYSSESSION: u64 = 2048;

type HandlerStreamStepResult = Result<(Option<coppb::Response>, bool)>;

/// An interface for all kind of Coprocessor request handlers.
#[async_trait]
pub trait RequestHandler: Send {
    /// Processes current request and produces a response.
    async fn handle_request(&mut self) -> Result<MemoryTraceGuard<coppb::Response>> {
        panic!("unary request is not supported for this handler");
    }

    /// Processes current request and produces streaming responses.
    async fn handle_streaming_request(&mut self) -> HandlerStreamStepResult {
        panic!("streaming request is not supported for this handler");
    }

    /// Collects scan statistics generated in this request handler so far.
    fn collect_scan_statistics(&mut self, _dest: &mut Statistics) {
        // Do nothing by default
    }

    /// Collects scan executor time in this request handler so far.
    fn collect_scan_summary(&mut self, _dest: &mut ExecSummary) {
        // Do nothing by default
    }

    fn index_lookup(&self) -> Option<(Vec<FieldType>, TableScan)> {
        None
    }

    fn get_req(&self) -> Option<DagRequest> {
        None
    }

    fn get_schema(&self) -> Option<Vec<FieldType>> {
        None
    }

    fn into_boxed(self) -> Box<dyn RequestHandler>
    where
        Self: 'static + Sized,
    {
        Box::new(self)
    }
}

type RequestHandlerBuilder<Snap> =
    Box<dyn for<'a> FnOnce(Snap, &ReqContext) -> Result<Box<dyn RequestHandler>> + Send>;

/// Encapsulate the `kvrpcpb::Context` to provide some extra properties.
#[derive(Debug, Clone)]
pub struct ReqContext {
    /// The tag of the request
    pub tag: ReqTag,

    /// The rpc context carried in the request
    pub context: kvrpcpb::Context,

    /// Scan ranges of this request
    pub ranges: Vec<coppb::KeyRange>,

    /// The deadline of the request
    pub deadline: Deadline,

    /// The peer address of the request
    pub peer: Option<String>,

    /// Whether the request is a descending scan (only applicable to DAG)
    pub is_desc_scan: Option<bool>,

    /// The transaction start_ts of the request
    pub txn_start_ts: TimeStamp,

    /// The set of timestamps of locks that can be bypassed during the reading
    /// because either they will be rolled back or their commit_ts > read
    /// request's start_ts.
    pub bypass_locks: TsSet,

    /// The set of timestamps of locks that value in it can be accessed during
    /// the reading because they will be committed and their commit_ts <=
    /// read request's start_ts.
    pub access_locks: TsSet,

    /// The data version to match. If it matches the underlying data version,
    /// request will not be processed (i.e. cache hit).
    ///
    /// None means don't try to hit the cache.
    pub cache_match_version: Option<u64>,

    /// The lower bound key in ranges of the request
    pub lower_bound: Vec<u8>,

    /// The upper bound key in ranges of the request
    pub upper_bound: Vec<u8>,

    /// Perf level
    pub perf_level: PerfLevel,

    /// Whether the request is allowed in the flashback state.
    pub allowed_in_flashback: bool,
}

impl HeapSize for ReqContext {
    fn approximate_heap_size(&self) -> usize {
        self.context.approximate_heap_size()
            + self.ranges.approximate_heap_size()
            + self.peer.as_ref().map_or(0, |p| p.as_bytes().len())
            + self.lower_bound.approximate_heap_size()
            + self.upper_bound.approximate_heap_size()
    }
}

impl ReqContext {
    pub fn new(
        tag: ReqTag,
        mut context: kvrpcpb::Context,
        ranges: Vec<coppb::KeyRange>,
        max_handle_duration: Duration,
        peer: Option<String>,
        is_desc_scan: Option<bool>,
        txn_start_ts: TimeStamp,
        cache_match_version: Option<u64>,
        perf_level: PerfLevel,
    ) -> Self {
        let mut deadline_duration = max_handle_duration;
        if context.max_execution_duration_ms > 0 {
            deadline_duration = Duration::from_millis(context.max_execution_duration_ms);
        }
        let deadline = Deadline::from_now(deadline_duration);
        let bypass_locks = TsSet::from_u64s(context.take_resolved_locks());
        let access_locks = TsSet::from_u64s(context.take_committed_locks());
        let lower_bound = match ranges.first().as_ref() {
            Some(range) => range.start.clone(),
            None => vec![],
        };
        let upper_bound = match ranges.last().as_ref() {
            Some(range) => range.end.clone(),
            None => vec![],
        };
        Self {
            tag,
            context,
            deadline,
            peer,
            is_desc_scan,
            txn_start_ts,
            ranges,
            bypass_locks,
            access_locks,
            cache_match_version,
            lower_bound,
            upper_bound,
            perf_level,
            allowed_in_flashback: false,
        }
    }

    #[cfg(test)]
    pub fn default_for_test() -> Self {
        Self::new(
            ReqTag::test,
            Default::default(),
            Vec::new(),
            Duration::from_secs(100),
            None,
            None,
            TimeStamp::max(),
            None,
            PerfLevel::EnableCount,
        )
    }

    pub fn build_task_id(&self) -> u64 {
        const ID_SHIFT: u32 = 16;
        const MASK: u64 = u64::max_value() >> ID_SHIFT;
        const MAX_TS: u64 = u64::max_value();
        let base = match self.txn_start_ts.into_inner() {
            0 | MAX_TS => thread_rng().next_u64(),
            start_ts => start_ts,
        };
        let task_id: u64 = self.context.get_task_id();
        if task_id > 0 {
            // It is assumed that the lower bits of task IDs in a single transaction
            // tend to be different. So if task_id is provided, we concatenate the
            // low 16 bits of the task_id and the low 48 bits of the start_ts to build
            // the final task id.
            (task_id << (64 - ID_SHIFT)) | (base & MASK)
        } else {
            // Otherwise we use the start_ts as the task_id.
            base
        }
    }
}

lazy_static! {
    pub static ref MEMTRACE_ROOT: Arc<MemoryTrace> = mem_trace!(coprocessor, [analyze]);
    pub static ref MEMTRACE_ANALYZE: Arc<MemoryTrace> =
        MEMTRACE_ROOT.sub_trace(Id::Name("analyze"));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_req_ctx_with_ctx_duration(
        context: kvrpcpb::Context,
        max_handle_duration: Duration,
    ) -> ReqContext {
        ReqContext::new(
            ReqTag::test,
            context,
            Vec::new(),
            max_handle_duration,
            None,
            None,
            TimeStamp::max(),
            None,
            PerfLevel::EnableCount,
        )
    }

    #[test]
    fn test_build_task_id() {
        let mut ctx = ReqContext::default_for_test();
        let start_ts: u64 = 0x05C6_1BFA_2648_324A;
        ctx.txn_start_ts = start_ts.into();
        ctx.context.set_task_id(1);
        assert_eq!(ctx.build_task_id(), 0x0001_1BFA_2648_324A);

        ctx.context.set_task_id(0);
        assert_eq!(ctx.build_task_id(), start_ts);
    }

    #[test]
    fn test_deadline_from_req_ctx() {
        let ctx = kvrpcpb::Context::default();
        let max_handle_duration = Duration::from_millis(100);
        let req_ctx = default_req_ctx_with_ctx_duration(ctx, max_handle_duration);
        // sleep at least 100ms
        std::thread::sleep(Duration::from_millis(200));
        req_ctx
            .deadline
            .check()
            .expect_err("deadline should exceed");

        let mut ctx = kvrpcpb::Context::default();
        ctx.max_execution_duration_ms = 100_000;
        let req_ctx = default_req_ctx_with_ctx_duration(ctx, max_handle_duration);
        // sleep at least 100ms
        std::thread::sleep(Duration::from_millis(200));
        req_ctx
            .deadline
            .check()
            .expect("deadline should not exceed");
    }
}
