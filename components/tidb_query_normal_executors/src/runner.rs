// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use kvproto::coprocessor::KeyRange;
use protobuf::Message;
use tipb::{self, ExecType, ExecutorExecutionSummary};
use tipb::{Chunk, DagRequest, SelectResponse, StreamResponse};

use tikv_util::deadline::Deadline;

use super::Executor;
use tidb_query_common::execute_stats::*;
use tidb_query_common::metrics::*;
use tidb_query_common::storage::{IntervalRange, Storage};
use tidb_query_common::Result;
use tidb_query_datatype::expr::{EvalConfig, EvalContext};

pub struct ExecutorsRunner<SS> {
    deadline: Deadline,
    executor: Box<dyn Executor<StorageStats = SS> + Send>,
    output_offsets: Vec<u32>,
    batch_row_limit: usize,
    collect_exec_summary: bool,
    context: EvalContext,
    exec_stats: ExecuteStats,
}

/// Builds a normal executor pipeline.
///
/// Normal executors iterate rows one by one.
pub fn build_executors<S: Storage + 'static, C: ExecSummaryCollector + 'static>(
    exec_descriptors: Vec<tipb::Executor>,
    storage: S,
    ranges: Vec<KeyRange>,
    ctx: Arc<EvalConfig>,
    is_streaming: bool,
) -> Result<Box<dyn Executor<StorageStats = S::Statistics> + Send>> {
    let mut exec_descriptors = exec_descriptors.into_iter();
    let first = exec_descriptors
        .next()
        .ok_or_else(|| other_err!("No executor specified"))?;

    let mut src = build_first_executor::<_, C>(first, storage, ranges, ctx.clone(), is_streaming)?;
    let mut summary_slot_index = 0;

    for mut exec in exec_descriptors {
        summary_slot_index += 1;

        let curr: Box<dyn Executor<StorageStats = S::Statistics> + Send> = match exec.get_tp() {
            ExecType::TypeSelection => {
                EXECUTOR_COUNT_METRICS.selection.inc();

                Box::new(
                    super::SelectionExecutor::new(exec.take_selection(), Arc::clone(&ctx), src)?
                        .with_summary_collector(C::new(summary_slot_index)),
                )
            }
            ExecType::TypeAggregation => {
                EXECUTOR_COUNT_METRICS.hash_aggr.inc();

                Box::new(
                    super::HashAggExecutor::new(exec.take_aggregation(), Arc::clone(&ctx), src)?
                        .with_summary_collector(C::new(summary_slot_index)),
                )
            }
            ExecType::TypeStreamAgg => {
                EXECUTOR_COUNT_METRICS.stream_aggr.inc();

                Box::new(
                    super::StreamAggExecutor::new(Arc::clone(&ctx), src, exec.take_aggregation())?
                        .with_summary_collector(C::new(summary_slot_index)),
                )
            }
            ExecType::TypeTopN => {
                EXECUTOR_COUNT_METRICS.top_n.inc();

                Box::new(
                    super::TopNExecutor::new(exec.take_top_n(), Arc::clone(&ctx), src)?
                        .with_summary_collector(C::new(summary_slot_index)),
                )
            }
            ExecType::TypeLimit => {
                EXECUTOR_COUNT_METRICS.limit.inc();

                Box::new(
                    super::LimitExecutor::new(exec.take_limit(), src)
                        .with_summary_collector(C::new(summary_slot_index)),
                )
            }
            _ => {
                return Err(other_err!(
                    "Unexpected non-first executor {:?}",
                    exec.get_tp()
                ));
            }
        };
        src = curr;
    }

    Ok(src)
}

/// Builds the inner-most executor for the normal executor pipeline, which can produce rows to
/// other executors and never receive rows from other executors.
///
/// The inner-most executor must be a table scan executor or an index scan executor.
fn build_first_executor<S: Storage + 'static, C: ExecSummaryCollector + 'static>(
    mut first: tipb::Executor,
    storage: S,
    ranges: Vec<KeyRange>,
    context: Arc<EvalConfig>,
    is_streaming: bool,
) -> Result<Box<dyn Executor<StorageStats = S::Statistics> + Send>> {
    let context = EvalContext::new(context);
    match first.get_tp() {
        ExecType::TypeTableScan => {
            EXECUTOR_COUNT_METRICS.table_scan.inc();

            let ex = Box::new(
                super::ScanExecutor::table_scan(
                    first.take_tbl_scan(),
                    context,
                    ranges,
                    storage,
                    is_streaming,
                )?
                .with_summary_collector(C::new(0)),
            );
            Ok(ex)
        }
        ExecType::TypeIndexScan => {
            EXECUTOR_COUNT_METRICS.index_scan.inc();

            let unique = first.get_idx_scan().get_unique();
            let ex = Box::new(
                super::ScanExecutor::index_scan(
                    first.take_idx_scan(),
                    context,
                    ranges,
                    storage,
                    unique,
                    is_streaming,
                )?
                .with_summary_collector(C::new(0)),
            );
            Ok(ex)
        }
        _ => Err(other_err!("Unexpected first scanner: {:?}", first.get_tp())),
    }
}

impl<SS: 'static> ExecutorsRunner<SS> {
    pub fn from_request<S: Storage<Statistics = SS> + 'static>(
        mut req: DagRequest,
        ranges: Vec<KeyRange>,
        storage: S,
        deadline: Deadline,
        batch_row_limit: usize,
        is_streaming: bool,
    ) -> Result<Self> {
        let executors_len = req.get_executors().len();
        let collect_exec_summary = req.get_collect_execution_summaries();
        let config = Arc::new(EvalConfig::from_request(&req)?);
        let context = EvalContext::new(config.clone());

        let executor = if !(req.get_collect_execution_summaries()) {
            build_executors::<_, ExecSummaryCollectorDisabled>(
                req.take_executors().into(),
                storage,
                ranges,
                config,
                is_streaming,
            )?
        } else {
            build_executors::<_, ExecSummaryCollectorEnabled>(
                req.take_executors().into(),
                storage,
                ranges,
                config,
                is_streaming,
            )?
        };

        let exec_stats = ExecuteStats::new(if collect_exec_summary {
            executors_len
        } else {
            0 // Avoid allocation for executor summaries when it is not needed
        });

        Ok(Self {
            deadline,
            executor,
            output_offsets: req.take_output_offsets(),
            batch_row_limit,
            collect_exec_summary,
            context,
            exec_stats,
        })
    }

    fn make_stream_response(&mut self, chunk: Chunk) -> Result<StreamResponse> {
        self.executor.collect_exec_stats(&mut self.exec_stats);

        let mut s_resp = StreamResponse::default();
        s_resp.set_data(box_try!(chunk.write_to_bytes()));
        if let Some(eval_warnings) = self.executor.take_eval_warnings() {
            s_resp.set_warnings(eval_warnings.warnings.into());
            s_resp.set_warning_count(eval_warnings.warning_cnt as i64);
        }
        s_resp.set_output_counts(
            self.exec_stats
                .scanned_rows_per_range
                .iter()
                .map(|v| *v as i64)
                .collect(),
        );

        self.exec_stats.clear();

        Ok(s_resp)
    }

    pub fn handle_request(&mut self) -> Result<SelectResponse> {
        let mut record_cnt = 0;
        let mut chunks = Vec::new();
        loop {
            match self.executor.next()? {
                Some(row) => {
                    self.deadline.check()?;
                    if chunks.is_empty() || record_cnt >= self.batch_row_limit {
                        let chunk = Chunk::default();
                        chunks.push(chunk);
                        record_cnt = 0;
                    }
                    let chunk = chunks.last_mut().unwrap();
                    record_cnt += 1;
                    // for default encode type
                    let value = row.get_binary(&mut self.context, &self.output_offsets)?;
                    chunk.mut_rows_data().extend_from_slice(&value);
                }
                None => {
                    self.executor.collect_exec_stats(&mut self.exec_stats);

                    let mut sel_resp = SelectResponse::default();
                    sel_resp.set_chunks(chunks.into());
                    if let Some(eval_warnings) = self.executor.take_eval_warnings() {
                        sel_resp.set_warnings(eval_warnings.warnings.into());
                        sel_resp.set_warning_count(eval_warnings.warning_cnt as i64);
                    }
                    // TODO: output_counts should not be i64. Let's fix it in Coprocessor DAG V2.
                    sel_resp.set_output_counts(
                        self.exec_stats
                            .scanned_rows_per_range
                            .iter()
                            .map(|v| *v as i64)
                            .collect(),
                    );

                    if self.collect_exec_summary {
                        let summaries = self
                            .exec_stats
                            .summary_per_executor
                            .iter()
                            .map(|summary| {
                                let mut ret = ExecutorExecutionSummary::default();
                                ret.set_num_iterations(summary.num_iterations as u64);
                                ret.set_num_produced_rows(summary.num_produced_rows as u64);
                                ret.set_time_processed_ns(summary.time_processed_ns as u64);
                                ret
                            })
                            .collect::<Vec<_>>();
                        sel_resp.set_execution_summaries(summaries.into());
                    }

                    // In case of this function is called multiple times.
                    self.exec_stats.clear();

                    return Ok(sel_resp);
                }
            }
        }
    }

    // TODO: IntervalRange should be placed inside `StreamResponse`.
    pub fn handle_streaming_request(
        &mut self,
    ) -> Result<(Option<(StreamResponse, IntervalRange)>, bool)> {
        let (mut record_cnt, mut finished) = (0, false);
        let mut chunk = Chunk::default();
        while record_cnt < self.batch_row_limit {
            match self.executor.next()? {
                Some(row) => {
                    self.deadline.check()?;
                    record_cnt += 1;
                    let value = row.get_binary(&mut self.context, &self.output_offsets)?;
                    chunk.mut_rows_data().extend_from_slice(&value);
                }
                None => {
                    finished = true;
                    break;
                }
            }
        }
        if record_cnt > 0 {
            let range = self.executor.take_scanned_range();
            return self
                .make_stream_response(chunk)
                .map(|r| (Some((r, range)), finished));
        }
        Ok((None, true))
    }

    #[inline]
    pub fn collect_storage_stats(&mut self, dest: &mut SS) {
        // TODO: A better way is to fill storage stats in `handle_request`, or
        // return SelectResponse in `handle_request`.
        self.executor.collect_storage_stats(dest);
    }

    #[inline]
    pub fn can_be_cached(&self) -> bool {
        self.executor.can_be_cached()
    }
}
