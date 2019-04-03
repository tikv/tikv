// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

//! DAG request is the most frequently used Coprocessor request. It includes some (simple) query
//! executors, e.g. table scan, index scan, selection, etc. Rows are fetched from the underlying KV
//! engine over a given snapshot, flows through each query executor and finally collected together.
//!
//! Although this request is called DAG request, these executors are executed one by one (i.e. as
//! a pipeline) at present.
//!
//! Generally, there are two kinds of query executors:
//!
//! - Executors that only produce rows (i.e. fetch data from the KV layer)
//!
//!   Samples: TableScanExecutor, IndexScanExecutor
//!
//!   Obviously, this kind of executor must be the first executor in the pipeline.
//!
//! - Executors that only work over previous executor's output row and produce a new row (or just
//!   eat it)
//!
//!   Samples: SelectionExecutor, AggregationExecutor, LimitExecutor, etc
//!
//!   Obviously, this kind of executor must not be the first executor in the pipeline.

pub mod batch_executor;
pub mod batch_handler;
mod builder;
pub mod executor;
pub mod expr;
pub mod handler;
pub mod rpn_expr;

pub use self::executor::{ScanOn, Scanner};
pub use self::handler::DAGRequestHandler;
