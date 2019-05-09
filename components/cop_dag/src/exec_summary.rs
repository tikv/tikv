// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

/// Execution summaries to support `EXPLAIN ANALYZE` statements.
#[derive(Default)]
pub struct ExecSummary {
    /// Total time cost in this executor.
    pub time_processed_ms: usize,

    /// How many rows this executor produced totally.
    pub num_produced_rows: usize,

    /// How many times executor's `next_batch()` is called.
    pub num_iterations: usize,
}

impl ExecSummary {
    #[inline]
    pub fn merge_into(self, target: &mut Self) {
        target.time_processed_ms += self.time_processed_ms;
        target.num_produced_rows += self.num_produced_rows;
        target.num_iterations += self.num_iterations;
    }
}

/// A trait for all execution summary collectors.
pub trait ExecSummaryCollector: Send {
    type DurationRecorder;

    /// Creates a new instance with specified output slot index.
    fn new(output_index: usize) -> Self
        where
            Self: Sized;

    /// Returns an instance that will record elapsed duration and increase
    /// the iterations counter. The instance should be later passed back to
    /// `on_finish_iterate` when processing of `next_batch` is completed.
    fn on_start_iterate(&mut self) -> Self::DurationRecorder;

    // Increases the process time and produced rows counter.
    // It should be called when `next_batch` is completed.
    fn on_finish_iterate(&mut self, dr: Self::DurationRecorder, rows: usize);

    /// Takes and appends current execution summary into `target`.
    fn collect_into(&mut self, target: &mut [Option<ExecSummary>]);
}

/// A normal `ExecSummaryCollector` that simply collects execution summaries.
/// It acts like `collect = true`.
pub struct ExecSummaryCollectorEnabled {
    output_index: usize,
    counts: ExecSummary,
}

impl ExecSummaryCollector for ExecSummaryCollectorEnabled {
    type DurationRecorder = tikv_util::time::Instant;

    #[inline]
    fn new(output_index: usize) -> ExecSummaryCollectorEnabled {
        ExecSummaryCollectorEnabled {
            output_index,
            counts: Default::default(),
        }
    }

    #[inline]
    fn on_start_iterate(&mut self) -> Self::DurationRecorder {
        self.counts.num_iterations += 1;
        tikv_util::time::Instant::now_coarse()
    }

    #[inline]
    fn on_finish_iterate(&mut self, dr: Self::DurationRecorder, rows: usize) {
        self.counts.num_produced_rows += rows;
        let elapsed_time = tikv_util::time::duration_to_ms(dr.elapsed()) as usize;
        self.counts.time_processed_ms += elapsed_time;
    }

    #[inline]
    fn collect_into(&mut self, target: &mut [Option<ExecSummary>]) {
        let current_summary = std::mem::replace(&mut self.counts, ExecSummary::default());
        if let Some(t) = &mut target[self.output_index] {
            current_summary.merge_into(t);
        } else {
            target[self.output_index] = Some(current_summary);
        }
    }
}

/// A `ExecSummaryCollector` that does not collect anything. Acts like `collect = false`.
pub struct ExecSummaryCollectorDisabled;

impl ExecSummaryCollector for ExecSummaryCollectorDisabled {
    type DurationRecorder = ();

    #[inline]
    fn new(_output_index: usize) -> ExecSummaryCollectorDisabled {
        ExecSummaryCollectorDisabled
    }

    #[inline]
    fn on_start_iterate(&mut self) -> Self::DurationRecorder {}

    #[inline]
    fn on_finish_iterate(&mut self, _dr: Self::DurationRecorder, _rows: usize) {}

    #[inline]
    fn collect_into(&mut self, _target: &mut [Option<ExecSummary>]) {}
}
