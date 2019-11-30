// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Handles simple SQL query executors locally.
//!
//! Most TiDB read queries are processed by Coprocessor instead of KV interface.
//! By doing so, the CPU of TiKV nodes can be utilized for computing and the
//! amount of data to transfer can be reduced (i.e. filtered at TiKV side).
//!
//! Notice that Coprocessor handles more than simple SQL query executors (DAG request). It also
//! handles analyzing requests and checksum requests.
//!
//! The entry point of handling all coprocessor requests is `Endpoint`. Common steps are:
//! 1. Parse the request into a DAG request, Checksum request or Analyze request.
//! 2. Retrieve a snapshot from the underlying engine according to the given timestamp.
//! 3. Build corresponding request handlers from the snapshot and request detail.
//! 4. Run request handlers once (for unary requests) or multiple times (for streaming requests)
//!    on a future thread pool.
//! 5. Return handling result as a response.
//!
//! Please refer to `Endpoint` for more details.

mod cache;
mod checksum;
pub mod dag;
mod endpoint;
mod error;
pub mod local_metrics;
mod metrics;
pub mod readpool_impl;
mod statistics;
mod tracker;

pub use self::endpoint::Endpoint;
pub use self::error::{Error, Result};
pub use checksum::checksum_crc64_xor;

use kvproto::{coprocessor as coppb, kvrpcpb};

use tikv_util::deadline::Deadline;
use tikv_util::time::Duration;

use crate::storage::mvcc::TsSet;
use crate::storage::Statistics;

pub const REQ_TYPE_DAG: i64 = 103;
pub const REQ_TYPE_ANALYZE: i64 = 104;
pub const REQ_TYPE_CHECKSUM: i64 = 105;

type HandlerStreamStepResult = Result<(Option<coppb::Response>, bool)>;

/// An interface for all kind of Coprocessor request handlers.
pub trait RequestHandler: Send {
    /// Processes current request and produces a response.
    fn handle_request(&mut self) -> Result<coppb::Response> {
        panic!("unary request is not supported for this handler");
    }

    /// Processes current request and produces streaming responses.
    fn handle_streaming_request(&mut self) -> HandlerStreamStepResult {
        panic!("streaming request is not supported for this handler");
    }

    /// Collects scan statistics generated in this request handler so far.
    fn collect_scan_statistics(&mut self, _dest: &mut Statistics) {
        // Do nothing by default
    }

    fn into_boxed(self) -> Box<dyn RequestHandler>
    where
        Self: 'static + Sized,
    {
        Box::new(self)
    }
}

type RequestHandlerBuilder<Snap> =
    Box<dyn for<'a> FnOnce(Snap, &'a ReqContext) -> Result<Box<dyn RequestHandler>> + Send>;

/// Encapsulate the `kvrpcpb::Context` to provide some extra properties.
#[derive(Debug, Clone)]
pub struct ReqContext {
    /// The tag of the request
    pub tag: &'static str,

    /// The rpc context carried in the request
    pub context: kvrpcpb::Context,

    /// The first range of the request
    pub first_range: Option<coppb::KeyRange>,

    /// The length of the range
    pub ranges_len: usize,

    /// The deadline of the request
    pub deadline: Deadline,

    /// The peer address of the request
    pub peer: Option<String>,

    /// Whether the request is a descending scan (only applicable to DAG)
    pub is_desc_scan: Option<bool>,

    /// The transaction start_ts of the request
    pub txn_start_ts: Option<u64>,

    /// The set of timestamps of locks that can be bypassed during the reading.
    pub bypass_locks: TsSet,

    /// The data version to match. If it matches the underlying data version,
    /// request will not be processed (i.e. cache hit).
    ///
    /// None means don't try to hit the cache.
    pub cache_match_version: Option<u64>,
}

impl ReqContext {
    pub fn new(
        tag: &'static str,
        mut context: kvrpcpb::Context,
        ranges: &[coppb::KeyRange],
        max_handle_duration: Duration,
        peer: Option<String>,
        is_desc_scan: Option<bool>,
        txn_start_ts: Option<u64>,
        cache_match_version: Option<u64>,
    ) -> Self {
        let deadline = Deadline::from_now(max_handle_duration);
        let bypass_locks = TsSet::from_u64s(context.take_resolved_locks());
        Self {
            tag,
            context,
            deadline,
            peer,
            is_desc_scan,
            txn_start_ts,
            first_range: ranges.first().cloned(),
            ranges_len: ranges.len(),
            bypass_locks,
            cache_match_version,
        }
    }

    #[cfg(test)]
    pub fn default_for_test() -> Self {
        Self::new(
            "test",
            kvrpcpb::Context::default(),
            &[],
            Duration::from_secs(100),
            None,
            None,
            None,
            None,
        )
    }
}
