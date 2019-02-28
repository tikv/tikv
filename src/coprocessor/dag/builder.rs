// Copyright 2019 PingCAP, Inc.
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

use std::sync::Arc;

use kvproto::coprocessor::KeyRange;
use tipb::executor::{self, ExecType};

use crate::storage::Store;

use super::batch_executor::interface::*;
use super::executor::{
    Executor, HashAggExecutor, IndexScanExecutor, LimitExecutor, SelectionExecutor,
    StreamAggExecutor, TableScanExecutor, TopNExecutor,
};
use crate::coprocessor::dag::expr::EvalConfig;
use crate::coprocessor::*;

/// Utilities to build an executor DAG.
///
/// Currently all executors are executed in sequence and there is no task scheduler, so this
/// builder is in fact a pipeline builder. The builder will finally build a `Box<Executor>` which
/// may contain another executor as its source, embedded in the field, one after another. These
/// nested executors together form an executor pipeline that a single iteration at the out-most
/// executor (i.e. calling `next()`) will drive the whole pipeline.
pub struct DAGBuilder;

impl DAGBuilder {
    /// Given a list of executor descriptors and returns whether all executor descriptors can
    /// be used to build batch executors.
    pub fn can_build_batch(_exec_descriptors: &[executor::Executor]) -> bool {
        // Currently no batch executors, so always returns false.
        false
    }

    // Note: `S` is `'static` because we have trait objects `Executor`.
    pub fn build_batch<S: Store + 'static>(
        _executor_descriptors: Vec<executor::Executor>,
        _store: S,
        _ranges: Vec<KeyRange>,
        _eval_config: EvalConfig,
    ) -> Result<(Box<dyn BatchExecutor>, BatchExecutorContext)> {
        // Currently no batch executors and this function should never be called, so unreachable.
        unreachable!()
    }

    /// Builds a normal executor pipeline.
    ///
    /// Normal executors iterate rows one by one.
    pub fn build_normal<S: Store + 'static>(
        exec_descriptors: Vec<executor::Executor>,
        store: S,
        ranges: Vec<KeyRange>,
        ctx: Arc<EvalConfig>,
        collect: bool,
    ) -> Result<Box<dyn Executor + Send>> {
        let mut exec_descriptors = exec_descriptors.into_iter();
        let first = exec_descriptors
            .next()
            .ok_or_else(|| Error::Other(box_err!("has no executor")))?;
        let mut src = Self::build_normal_first_executor(first, store, ranges, collect)?;
        for mut exec in exec_descriptors {
            let curr: Box<dyn Executor + Send> = match exec.get_tp() {
                ExecType::TypeTableScan | ExecType::TypeIndexScan => {
                    return Err(box_err!("got too much *scan exec, should be only one"));
                }
                ExecType::TypeSelection => Box::new(SelectionExecutor::new(
                    exec.take_selection(),
                    Arc::clone(&ctx),
                    src,
                )?),
                ExecType::TypeAggregation => Box::new(HashAggExecutor::new(
                    exec.take_aggregation(),
                    Arc::clone(&ctx),
                    src,
                )?),
                ExecType::TypeStreamAgg => Box::new(StreamAggExecutor::new(
                    Arc::clone(&ctx),
                    src,
                    exec.take_aggregation(),
                )?),
                ExecType::TypeTopN => {
                    Box::new(TopNExecutor::new(exec.take_topN(), Arc::clone(&ctx), src)?)
                }
                ExecType::TypeLimit => Box::new(LimitExecutor::new(exec.take_limit(), src)),
            };
            src = curr;
        }
        Ok(src)
    }

    /// Builds the inner-most executor for the normal executor pipeline, which can produce rows to
    /// other executors and never receive rows from other executors.
    ///
    /// The inner-most executor must be a table scan executor or an index scan executor.
    fn build_normal_first_executor<S: Store + 'static>(
        mut first: executor::Executor,
        store: S,
        ranges: Vec<KeyRange>,
        collect: bool,
    ) -> Result<Box<dyn Executor + Send>> {
        match first.get_tp() {
            ExecType::TypeTableScan => {
                let ex = Box::new(TableScanExecutor::new(
                    first.take_tbl_scan(),
                    ranges,
                    store,
                    collect,
                )?);
                Ok(ex)
            }
            ExecType::TypeIndexScan => {
                let unique = first.get_idx_scan().get_unique();
                let ex = Box::new(IndexScanExecutor::new(
                    first.take_idx_scan(),
                    ranges,
                    store,
                    unique,
                    collect,
                )?);
                Ok(ex)
            }
            _ => Err(box_err!(
                "first exec type should be *Scan, but get {:?}",
                first.get_tp()
            )),
        }
    }
}
