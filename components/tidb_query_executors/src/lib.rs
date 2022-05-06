// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This crate implements a simple SQL query engine to work with TiDB pushed down executors.
//!
//! The query engine is able to scan and understand rows stored by TiDB, run against a
//! series of executors and then return the execution result. The query engine is provided via
//! TiKV Coprocessor interface. However standalone UDF functions are also exported and can be used
//! standalone.

#![allow(incomplete_features)]
#![feature(proc_macro_hygiene)]
#![feature(specialization)]
#![feature(const_fn_fn_ptr_basics)]
#![feature(const_fn_trait_bound)]
#![feature(const_mut_refs)]

#[macro_use(box_try, warn)]
extern crate tikv_util;

#[macro_use(other_err)]
extern crate tidb_query_common;

#[cfg(test)]
pub use tidb_query_aggr::*;
#[cfg(test)]
pub use tidb_query_expr::function::*;
#[cfg(test)]
pub use tidb_query_expr::*;
mod fast_hash_aggr_executor;
mod index_scan_executor;
pub mod interface;
mod limit_executor;
mod projection_executor;
pub mod runner;
mod selection_executor;
mod simple_aggr_executor;
mod slow_hash_aggr_executor;
mod stream_aggr_executor;
mod table_scan_executor;
mod top_n_executor;
mod util;

pub use self::{
    fast_hash_aggr_executor::BatchFastHashAggregationExecutor,
    index_scan_executor::BatchIndexScanExecutor, limit_executor::BatchLimitExecutor,
    projection_executor::BatchProjectionExecutor, selection_executor::BatchSelectionExecutor,
    simple_aggr_executor::BatchSimpleAggregationExecutor,
    slow_hash_aggr_executor::BatchSlowHashAggregationExecutor,
    stream_aggr_executor::BatchStreamAggregationExecutor,
    table_scan_executor::BatchTableScanExecutor, top_n_executor::BatchTopNExecutor,
};
