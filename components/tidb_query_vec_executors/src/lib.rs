// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This crate implements a simple SQL query engine to work with TiDB pushed down executors.
//!
//! The query engine is able to scan and understand rows stored by TiDB, run against a
//! series of executors and then return the execution result. The query engine is provided via
//! TiKV Coprocessor interface. However standalone UDF functions are also exported and can be used
//! standalone.

#![feature(proc_macro_hygiene)]
#![feature(specialization)]
#![feature(const_fn)]
#![feature(decl_macro)]

#[macro_use(warn)]
extern crate slog_global;

#[macro_use(box_try)]
extern crate tikv_util;

#[macro_use(other_err)]
extern crate tidb_query_common;

#[cfg(test)]
pub use tidb_query_vec_aggr::*;
#[cfg(test)]
pub use tidb_query_vec_expr::function::*;
#[cfg(test)]
pub use tidb_query_vec_expr::*;
mod fast_hash_aggr_executor;
mod index_scan_executor;
pub mod interface;
mod limit_executor;
pub mod runner;
mod selection_executor;
mod simple_aggr_executor;
mod slow_hash_aggr_executor;
mod stream_aggr_executor;
mod table_scan_executor;
mod top_n_executor;
mod util;

pub use self::fast_hash_aggr_executor::BatchFastHashAggregationExecutor;
pub use self::index_scan_executor::BatchIndexScanExecutor;
pub use self::limit_executor::BatchLimitExecutor;
pub use self::selection_executor::BatchSelectionExecutor;
pub use self::simple_aggr_executor::BatchSimpleAggregationExecutor;
pub use self::slow_hash_aggr_executor::BatchSlowHashAggregationExecutor;
pub use self::stream_aggr_executor::BatchStreamAggregationExecutor;
pub use self::table_scan_executor::BatchTableScanExecutor;
pub use self::top_n_executor::BatchTopNExecutor;
