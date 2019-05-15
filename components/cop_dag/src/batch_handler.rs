// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use super::batch::interface::{BatchExecuteStatistics, BatchExecutor};
use super::executor::ExecutorMetrics;
use crate::expr::EvalConfig;
use crate::*;

/// Must be built from DAGRequestHandler.
pub struct BatchDAGHandler {
    /// The deadline of this handler. For each check point (e.g. each iteration) we need to check
    /// whether or not the deadline is exceeded and break the process if so.
    // TODO: Deprecate it using a better deadline mechanism.
    pub deadline: Deadline,

    pub out_most_executor: Box<dyn BatchExecutor>,

    /// The offset of the columns need to be outputted. For example, TiDB may only needs a subset
    /// of the columns in the result so that unrelated columns don't need to be encoded and
    /// returned back.
    pub output_offsets: Vec<u32>,

    pub config: Arc<EvalConfig>,

    /// Accumulated statistics.
    // TODO: Currently we return statistics only once, so these statistics are accumulated only
    // once. However in future when we introduce reenterable DAG processor, these statistics may
    // be accumulated and returned several times during the life time of the request. At that time
    // we may remove this field.
    pub statistics: BatchExecuteStatistics,

    /// Traditional metric interface.
    // TODO: Deprecate it in Coprocessor DAG v2.
    pub metrics: ExecutorMetrics,
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
