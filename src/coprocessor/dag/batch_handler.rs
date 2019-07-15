// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use protobuf::{Message, RepeatedField};

use kvproto::coprocessor::Response;
use tipb::executor::ExecutorExecutionSummary;
use tipb::select::{Chunk, SelectResponse};

use super::batch::interface::{BatchExecuteStatistics, BatchExecutor};
use super::executor::ExecutorMetrics;
use crate::coprocessor::dag::expr::EvalConfig;
use crate::coprocessor::*;

// TODO: The value is chosen according to some very subjective experience, which is not tuned
// carefully. We need to benchmark to find a best value. Also we may consider accepting this value
// from TiDB side.
const BATCH_INITIAL_SIZE: usize = 32;

// TODO: This value is chosen based on MonetDB/X100's research without our own benchmarks.
pub const BATCH_MAX_SIZE: usize = 1024;

// TODO: Maybe there can be some better strategy. Needs benchmarks and tunes.
const BATCH_GROW_FACTOR: usize = 2;

/// Must be built from DAGRequestHandler.
pub struct BatchDAGHandler {
    /// The deadline of this handler. For each check point (e.g. each iteration) we need to check
    /// whether or not the deadline is exceeded and break the process if so.
    // TODO: Deprecate it using a better deadline mechanism.
    deadline: Deadline,

    out_most_executor: Box<dyn BatchExecutor>,

    /// The offset of the columns need to be outputted. For example, TiDB may only needs a subset
    /// of the columns in the result so that unrelated columns don't need to be encoded and
    /// returned back.
    output_offsets: Vec<u32>,

    config: Arc<EvalConfig>,

    /// Accumulated statistics.
    // TODO: Currently we return statistics only once, so these statistics are accumulated only
    // once. However in future when we introduce reenterable DAG processor, these statistics may
    // be accumulated and returned several times during the life time of the request. At that time
    // we may remove this field.
    statistics: BatchExecuteStatistics,

    /// Traditional metric interface.
    // TODO: Deprecate it in Coprocessor DAG v2.
    metrics: ExecutorMetrics,

    /// Whether or not execution summary need to be collected.
    collect_exec_summary: bool,
}

impl BatchDAGHandler {
    pub fn new(
        deadline: Deadline,
        out_most_executor: Box<dyn BatchExecutor>,
        output_offsets: Vec<u32>,
        config: Arc<EvalConfig>,
        statistics: BatchExecuteStatistics,
        collect_exec_summary: bool,
    ) -> Self {
        Self {
            deadline,
            out_most_executor,
            output_offsets,
            config,
            statistics,
            metrics: ExecutorMetrics::default(),
            collect_exec_summary,
        }
    }
}

impl RequestHandler for BatchDAGHandler {
    fn handle_request(&mut self) -> Result<Response> {
        let mut chunks = vec![];
        let mut batch_size = BATCH_INITIAL_SIZE;
        let mut warnings = self.config.new_eval_warnings();

        loop {
            self.deadline.check_if_exceeded()?;

            let mut result = self.out_most_executor.next_batch(batch_size);

            let is_drained;

            // Check error first, because it means that we should directly respond error.
            match result.is_drained {
                Err(Error::Eval(err)) => {
                    let mut resp = Response::new();
                    let mut sel_resp = SelectResponse::new();
                    sel_resp.set_error(err);
                    let data = box_try!(sel_resp.write_to_bytes());
                    resp.set_data(data);
                    return Ok(resp);
                }
                Err(e) => return Err(e),
                Ok(f) => is_drained = f,
            }

            // We will only get warnings limited by max_warning_count. Note that in future we
            // further want to ignore warnings from unused rows. See TODOs in the `result.warnings`
            // field.
            warnings.merge(&mut result.warnings);

            // Notice that logical rows len == 0 doesn't mean that it is drained.
            if !result.logical_rows.is_empty() {
                assert_eq!(
                    result.physical_columns.columns_len(),
                    self.out_most_executor.schema().len()
                );
                let mut chunk = Chunk::new();
                {
                    let data = chunk.mut_rows_data();
                    data.reserve(
                        result
                            .physical_columns
                            .maximum_encoded_size(&result.logical_rows, &self.output_offsets)?,
                    );
                    // Although `schema()` can be deeply nested, it is ok since we process data in
                    // batch.
                    result.physical_columns.encode(
                        &result.logical_rows,
                        &self.output_offsets,
                        self.out_most_executor.schema(),
                        data,
                    )?;
                }
                chunks.push(chunk);
            }

            if is_drained {
                self.out_most_executor
                    .collect_statistics(&mut self.statistics);
                self.metrics.cf_stats.add(&self.statistics.cf_stats);

                let mut resp = Response::new();
                let mut sel_resp = SelectResponse::new();
                sel_resp.set_chunks(chunks.into());
                // TODO: output_counts should not be i64. Let's fix it in Coprocessor DAG V2.
                sel_resp.set_output_counts(
                    self.statistics
                        .scanned_rows_per_range
                        .iter()
                        .map(|v| *v as i64)
                        .collect(),
                );

                if self.collect_exec_summary {
                    let summaries = self
                        .statistics
                        .summary_per_executor
                        .iter()
                        .map(|summary| {
                            let mut ret = ExecutorExecutionSummary::new();
                            ret.set_num_iterations(summary.num_iterations as u64);
                            ret.set_num_produced_rows(summary.num_produced_rows as u64);
                            ret.set_time_processed_ns(summary.time_processed_ns as u64);
                            ret
                        })
                        .collect();
                    sel_resp.set_execution_summaries(RepeatedField::from_vec(summaries));
                }

                sel_resp.set_warnings(warnings.warnings.into());
                sel_resp.set_warning_count(warnings.warning_cnt as i64);

                let data = box_try!(sel_resp.write_to_bytes());
                resp.set_data(data);

                // Not really useful here, because we only collect it once. But when we change it
                // in future, hope we will not forget it.
                self.statistics.clear();

                return Ok(resp);
            }

            // Grow batch size
            if batch_size < BATCH_MAX_SIZE {
                batch_size *= BATCH_GROW_FACTOR;
                if batch_size > BATCH_MAX_SIZE {
                    batch_size = BATCH_MAX_SIZE
                }
            }
        }
    }

    fn collect_metrics_into(&mut self, target_metrics: &mut ExecutorMetrics) {
        // FIXME: This interface will be broken in streaming mode.
        target_metrics.merge(&mut self.metrics);

        // Notice: Exec count is collected during building the batch handler.
    }
}
