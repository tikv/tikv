// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use protobuf::{Message, RepeatedField};

use kvproto::coprocessor::Response;
use tipb::executor::ExecutorExecutionSummary;
use tipb::select::{Chunk, SelectResponse};

use super::batch::interface::{BatchExecuteStatistics, BatchExecutor};
use super::executor::ExecutorMetrics;
use crate::expr::EvalConfig;
use crate::*;

// TODO: The value is chosen according to some very subjective experience, which is not tuned
// carefully. We need to benchmark to find a best value. Also we may consider accepting this value
// from TiDB side.
const BATCH_INITIAL_SIZE: usize = 32;

// TODO: This value is chosen based on MonetDB/X100's research without our own benchmarks.
const BATCH_MAX_SIZE: usize = 1024;

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
}

impl BatchDAGHandler {
    pub fn new(
        deadline: Deadline,
        out_most_executor: Box<dyn BatchExecutor>,
        output_offsets: Vec<u32>,
        config: Arc<EvalConfig>,
        ranges_len: usize,
        executors_len: usize,
    ) -> Self {
        Self {
            deadline,
            out_most_executor,
            output_offsets,
            config,
            statistics: BatchExecuteStatistics::new(executors_len, ranges_len),
            metrics: ExecutorMetrics::default(),
        }
    }
}

// TODO: impl RequestHandler for DAGRequestHandler