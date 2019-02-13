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

use cop_datatype::EvalType;
use kvproto::coprocessor::KeyRange;
use tipb::executor::{self, ExecType};
use tipb::expression::{Expr, ExprType};

use crate::storage::Store;

use super::batch_executor::executors::*;
use super::batch_executor::interface::*;
use super::executor::{
    Executor, HashAggExecutor, IndexScanExecutor, LimitExecutor, SelectionExecutor,
    StreamAggExecutor, TableScanExecutor, TopNExecutor,
};
use crate::coprocessor::dag::expr::EvalConfig;
use crate::coprocessor::dag::rpn_expr::RpnFunction;
use crate::coprocessor::*;

/// Utilities to build an executor DAG.
///
/// Currently all executors are executed in sequence and there is no task scheduler, so this
/// builder is in fact a pipeline builder. The builder will finally build a `Box<Executor>` which
/// may contain another executor as its source, embedded in the field, one after another. These
/// nested executors together form an executor pipeline that a single iteration at the out-most
/// executor (i.e. calling `next()`) will drive the whole pipeline.
pub struct DAGBuilder;

fn check_condition(c: &Expr) -> bool {
    use cop_datatype::FieldTypeAccessor;
    use std::convert::TryFrom;

    let eval_type = EvalType::try_from(c.get_field_type().tp());
    if eval_type.is_err() {
        return false;
    }
    match c.get_tp() {
        ExprType::ScalarFunc => {
            let sig = c.get_sig();
            let func = RpnFunction::try_from(sig);
            if func.is_err() {
                return false;
            }
            for n in c.get_children() {
                if !check_condition(n) {
                    return false;
                }
            }
        }
        ExprType::Null => {}
        ExprType::Int64 => {}
        ExprType::Uint64 => {}
        ExprType::String | ExprType::Bytes => {}
        ExprType::Float32 | ExprType::Float64 => {}
        ExprType::MysqlTime => {}
        ExprType::MysqlDuration => {}
        ExprType::MysqlDecimal => {}
        ExprType::MysqlJson => {}
        ExprType::ColumnRef => {}
        _ => return false,
    }

    true
}

impl DAGBuilder {
    /// Given a list of executor descriptors and returns whether all executor descriptors can
    /// be used to build batch executors.
    pub fn can_build_batch(exec_descriptors: &[executor::Executor]) -> bool {
        use cop_datatype::EvalType;
        use cop_datatype::FieldTypeAccessor;
        use std::convert::TryFrom;

        for ed in exec_descriptors {
            match ed.get_tp() {
                ExecType::TypeTableScan => {
                    let descriptor = ed.get_tbl_scan();
                    for column in descriptor.get_columns() {
                        let eval_type = EvalType::try_from(column.tp());
                        if eval_type.is_err() {
                            info!("Coprocessor request cannot be batched because column eval type {:?} is not supported", eval_type);
                            return false;
                        }
                    }
                }
                ExecType::TypeIndexScan => {
                    let descriptor = ed.get_idx_scan();
                    for column in descriptor.get_columns() {
                        let eval_type = EvalType::try_from(column.tp());
                        if eval_type.is_err() {
                            info!("Coprocessor request cannot be batched because column eval type {:?} is not supported", eval_type);
                            return false;
                        }
                    }
                }
                ExecType::TypeSelection => {
                    let descriptor = ed.get_selection();
                    let conditions = descriptor.get_conditions();
                    for c in conditions {
                        if !check_condition(c) {
                            info!("Coprocessor request cannot be batched because condition {:?} is not supported", c);
                            return false;
                        }
                    }
                }
                _ => {
                    info!(
                        "Coprocessor request cannot be batched because {:?} is not supported",
                        ed.get_tp()
                    );
                    return false;
                }
            }
        }
        true
    }

    // Note: `S` is `'static` because we have trait objects `Executor`.
    pub fn build_batch<S: Store + 'static>(
        executor_descriptors: Vec<executor::Executor>,
        store: S,
        ranges: Vec<KeyRange>,
        eval_config: EvalConfig,
    ) -> Result<(Box<BatchExecutor>, ExecutorContext)> {
        // Shared in multiple executors, so wrap with Rc.
        let eval_config = Arc::new(eval_config);
        let mut executor_descriptors = executor_descriptors.into_iter();
        let mut first_ed = executor_descriptors
            .next()
            .ok_or_else(|| Error::Other(box_err!("No executors")))?;

        let executor_context;
        let mut executor: Box<BatchExecutor>;

        match first_ed.get_tp() {
            ExecType::TypeTableScan => {
                let mut descriptor = first_ed.take_tbl_scan();
                // println!("Table Scan {:?}", descriptor);
                executor_context = ExecutorContext::new(descriptor.take_columns().into_vec());
                executor = box BatchTableScanExecutor::new(
                    store,
                    executor_context.clone(),
                    ranges,
                    descriptor.get_desc(),
                )?;
            }
            ExecType::TypeIndexScan => {
                let mut descriptor = first_ed.take_idx_scan();
                // println!("Index Scan {:?}", descriptor);
                executor_context = ExecutorContext::new(descriptor.take_columns().into_vec());
                executor = box BatchIndexScanExecutor::new(
                    store,
                    executor_context.clone(),
                    ranges,
                    descriptor.get_desc(),
                    descriptor.get_unique(),
                )?;
            }
            _ => {
                return Err(Error::Other(box_err!(
                    "Unexpected first executor {:?}",
                    first_ed.get_tp()
                )));
            }
        }

        for mut ed in executor_descriptors {
            let new_executor: Box<BatchExecutor> = match ed.get_tp() {
                ExecType::TypeTableScan | ExecType::TypeIndexScan => {
                    return Err(Error::Other(box_err!(
                        "Unexpected non-first executor {:?}",
                        ed.get_tp()
                    )));
                }
                ExecType::TypeSelection => {
                    // println!("Selection {:?}", ed.get_selection());
                    Box::new(BatchSelectionExecutor::new(
                        executor_context.clone(),
                        executor,
                        ed.take_selection().take_conditions().into_vec(),
                        Arc::clone(&eval_config),
                    )?)
                }
                _ => {
                    return Err(Error::Other(box_err!(
                        "Unexpected non-first executor {:?}",
                        first_ed.get_tp()
                    )));
                }
            };
            executor = new_executor;
        }

        Ok((executor, executor_context))
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
    ) -> Result<Box<Executor + Send>> {
        let mut exec_descriptors = exec_descriptors.into_iter();
        let first = exec_descriptors
            .next()
            .ok_or_else(|| Error::Other(box_err!("has no executor")))?;
        let mut src = Self::build_normal_first_executor(first, store, ranges, collect)?;
        for mut exec in exec_descriptors {
            let curr: Box<Executor + Send> = match exec.get_tp() {
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
    ) -> Result<Box<Executor + Send>> {
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
