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

//! Executor common interfaces

use std::sync::Arc;

use tipb::schema::ColumnInfo;

use coprocessor::codec::batch::LazyBatchColumnVec;
use coprocessor::Error;

/// A shared context for all executors built from a single Coprocessor DAG request.
///
/// It is both Send and Sync, allows concurrent access from different executors in future.
///
/// It is designed to be used in new generation executors, i.e. executors support batch execution.
/// The old executors will not be refined to use this kind of context.
#[derive(Clone)]
pub struct ExecutorContext(Arc<ExecutorContextInner>);

impl ExecutorContext {
    pub fn new(columns_info: Vec<ColumnInfo>) -> Self {
        let inner = ExecutorContextInner {
            collect_range_counts: true,
            columns_info,
        };
        ExecutorContext(Arc::new(inner))
    }
}

impl ::std::ops::Deref for ExecutorContext {
    type Target = ExecutorContextInner;

    fn deref(&self) -> &ExecutorContextInner {
        self.0.deref()
    }
}

impl ::util::AssertSend for ExecutorContext {}

impl ::util::AssertSync for ExecutorContext {}

pub struct ExecutorContextInner {
    pub collect_range_counts: bool,
    pub columns_info: Vec<ColumnInfo>,
}

/// Data to be flowed between parent and child executors' single `next_batch()` invocation.
///
/// Note: there are other data flow between executors, like warnings, metrics and output statistics.
/// However they are flowed at once, just before response, instead of each step during execution.
/// Hence they are not covered by this structure.
///
/// It is only Send but not Sync because executor returns its own data copy. However Send enables
/// executors to live in different threads.
///
/// It is designed to be used in new generation executors, i.e. executors support batch execution.
/// The old executors will not be refined to return this kind of result.
pub struct BatchExecuteResult {
    pub data: LazyBatchColumnVec,
    pub error: Option<Error>,
}
