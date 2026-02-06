// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::{
    HistogramVec, IntCounter, IntCounterVec, exponential_buckets, register_histogram_vec,
    register_int_counter, register_int_counter_vec,
};

lazy_static! {
    /// Number of SST files handled by the BR Parquet exporter, labeled by stage.
    pub static ref BR_PARQUET_SSTS: IntCounterVec = register_int_counter_vec!(
        "tikv_br_parquet_sst_events_total",
        "Number of SST files handled by the BR Parquet exporter",
        &["event"]
    )
    .unwrap();
    /// Total number of rows exported to Parquet by the BR Parquet exporter.
    pub static ref BR_PARQUET_ROWS: IntCounter = register_int_counter!(
        "tikv_br_parquet_rows_emitted_total",
        "Rows exported to Parquet files by the BR Parquet exporter"
    )
    .unwrap();
    /// Total number of Parquet bytes uploaded by the BR Parquet exporter.
    pub static ref BR_PARQUET_OUTPUT_BYTES: IntCounter = register_int_counter!(
        "tikv_br_parquet_output_bytes_total",
        "Total Parquet bytes uploaded by the BR Parquet exporter"
    )
    .unwrap();
    /// Duration spent in major BR Parquet exporter stages.
    pub static ref BR_PARQUET_STAGE_DURATION: HistogramVec = register_histogram_vec!(
        "tikv_br_parquet_stage_duration_seconds",
        "Duration spent in major BR Parquet exporter stages",
        &["stage"],
        exponential_buckets(0.01, 2.0, 16).unwrap()
    )
    .unwrap();
}

/// A snapshot of BR Parquet exporter counters for reporting progress.
#[derive(Clone, Debug, Default)]
pub struct ExporterMetricsSnapshot {
    /// Number of SST files downloaded.
    pub downloaded: u64,
    /// Number of SST files converted.
    pub converted: u64,
    /// Number of Parquet files uploaded.
    pub uploaded: u64,
    /// Number of SST files fully processed.
    pub completed: u64,
    /// Total number of exported rows.
    pub rows: u64,
    /// Total number of output bytes uploaded.
    pub output_bytes: u64,
}

impl ExporterMetricsSnapshot {
    /// Computes the delta between this snapshot and `base` using saturating
    /// subtraction.
    pub fn delta_since(&self, base: &ExporterMetricsSnapshot) -> ExporterMetricsSnapshot {
        ExporterMetricsSnapshot {
            downloaded: self.downloaded.saturating_sub(base.downloaded),
            converted: self.converted.saturating_sub(base.converted),
            uploaded: self.uploaded.saturating_sub(base.uploaded),
            completed: self.completed.saturating_sub(base.completed),
            rows: self.rows.saturating_sub(base.rows),
            output_bytes: self.output_bytes.saturating_sub(base.output_bytes),
        }
    }
}

/// Takes a point-in-time snapshot of exporter counters.
pub fn snapshot() -> ExporterMetricsSnapshot {
    ExporterMetricsSnapshot {
        downloaded: BR_PARQUET_SSTS.with_label_values(&["downloaded"]).get(),
        converted: BR_PARQUET_SSTS.with_label_values(&["converted"]).get(),
        uploaded: BR_PARQUET_SSTS.with_label_values(&["uploaded"]).get(),
        completed: BR_PARQUET_SSTS.with_label_values(&["completed"]).get(),
        rows: BR_PARQUET_ROWS.get(),
        output_bytes: BR_PARQUET_OUTPUT_BYTES.get(),
    }
}
