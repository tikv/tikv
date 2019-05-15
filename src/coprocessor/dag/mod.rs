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

pub mod aggr_fn;
pub mod batch_handler;
pub mod builder;
pub mod handler;

pub use cop_dag::batch;
pub use cop_dag::exec_summary;
pub use cop_dag::executor;
pub use cop_dag::expr;
pub use cop_dag::rpn_expr;
pub use cop_dag::{ScanOn, Scanner};

pub use self::batch_handler::BatchDAGHandler;
pub use self::builder::DAGBuilder;
pub use self::handler::DAGRequestHandler;
