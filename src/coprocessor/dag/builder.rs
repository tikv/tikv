// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use kvproto::coprocessor::KeyRange;
use tipb::executor::{self, ExecType};

use crate::storage::Store;

use super::batch::executors::*;
use super::batch::interface::*;
use super::executor::{
    Executor, HashAggExecutor, LimitExecutor, ScanExecutor, SelectionExecutor, StreamAggExecutor,
    TopNExecutor,
};
use crate::coprocessor::dag::batch::statistics::ExecSummaryCollectorDisabled;
use crate::coprocessor::dag::expr::EvalConfig;
use crate::coprocessor::metrics::*;
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
                            debug!(
                                "Coprocessor request cannot be batched";
                                "unsupported_column_tp" => ?column.tp(),
                            );
                            return false;
                        }
                    }
                }
                ExecType::TypeIndexScan => {
                    let descriptor = ed.get_idx_scan();
                    for column in descriptor.get_columns() {
                        let eval_type = EvalType::try_from(column.tp());
                        if eval_type.is_err() {
                            debug!(
                                "Coprocessor request cannot be batched";
                                "unsupported_column_tp" => ?column.tp(),
                            );
                            return false;
                        }
                    }
                }
                ExecType::TypeLimit => {}
                _ => {
                    debug!(
                        "Coprocessor request cannot be batched";
                        "unsupported_executor_tp" => ?ed.get_tp(),
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
        config: Arc<EvalConfig>,
    ) -> Result<Box<dyn BatchExecutor>> {
        let mut executor_descriptors = executor_descriptors.into_iter();
        let mut first_ed = executor_descriptors
            .next()
            .ok_or_else(|| Error::Other(box_err!("No executors")))?;

        let mut executor: Box<dyn BatchExecutor>;

        match first_ed.get_tp() {
            ExecType::TypeTableScan => {
                // TODO: Use static metrics.
                COPR_EXECUTOR_COUNT.with_label_values(&["tblscan"]).inc();

                let mut descriptor = first_ed.take_tbl_scan();
                let columns_info = descriptor.take_columns().into_vec();
                executor = Box::new(BatchTableScanExecutor::new(
                    ExecSummaryCollectorDisabled,
                    store,
                    config.clone(),
                    columns_info,
                    ranges,
                    descriptor.get_desc(),
                )?);
            }
            ExecType::TypeIndexScan => {
                COPR_EXECUTOR_COUNT.with_label_values(&["idxscan"]).inc();

                let mut descriptor = first_ed.take_idx_scan();
                let columns_info = descriptor.take_columns().into_vec();
                executor = Box::new(BatchIndexScanExecutor::new(
                    ExecSummaryCollectorDisabled,
                    store,
                    config.clone(),
                    columns_info,
                    ranges,
                    descriptor.get_desc(),
                    descriptor.get_unique(),
                )?);
            }
            _ => {
                return Err(Error::Other(box_err!(
                    "Unexpected first executor {:?}",
                    first_ed.get_tp()
                )));
            }
        }

        for ex in executor_descriptors {
            match ex.get_tp() {
                ExecType::TypeLimit => {
                    executor = Box::new(BatchLimitExecutor::new(
                        executor,
                        ex.get_limit().get_limit() as usize,
                        ExecSummaryCollectorDisabled,
                    )?);
                }
                _ => {
                    return Err(Error::Other(box_err!(
                        "Unexpected non-first executor {:?}",
                        first_ed.get_tp()
                    )));
                }
            }
        }

        Ok(executor)
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
                let ex = Box::new(ScanExecutor::table_scan(
                    first.take_tbl_scan(),
                    ranges,
                    store,
                    collect,
                )?);
                Ok(ex)
            }
            ExecType::TypeIndexScan => {
                let unique = first.get_idx_scan().get_unique();
                let ex = Box::new(ScanExecutor::index_scan(
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
