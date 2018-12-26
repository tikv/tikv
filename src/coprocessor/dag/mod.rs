// Copyright 2017 PingCAP, Inc.
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

pub mod dag;
pub mod executor;
pub mod expr;

pub use self::dag::DAGContext;
pub use self::executor::{ScanOn, Scanner};
