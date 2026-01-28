// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::{
    HistogramVec, IntCounter, IntCounterVec, exponential_buckets, register_histogram_vec,
    register_int_counter, register_int_counter_vec,
};

lazy_static! {
    pub static ref BR_PARQUET_SSTS: IntCounterVec = register_int_counter_vec!(
        "tikv_br_parquet_sst_events_total",
        "Number of SST files handled by the BR Parquet exporter",
        &["event"]
    )
    .unwrap();
    pub static ref BR_PARQUET_ROWS: IntCounter = register_int_counter!(
        "tikv_br_parquet_rows_emitted_total",
        "Rows exported to Parquet files by the BR Parquet exporter"
    )
    .unwrap();
    pub static ref BR_PARQUET_OUTPUT_BYTES: IntCounter = register_int_counter!(
        "tikv_br_parquet_output_bytes_total",
        "Total Parquet bytes uploaded by the BR Parquet exporter"
    )
    .unwrap();
    pub static ref BR_PARQUET_STAGE_DURATION: HistogramVec = register_histogram_vec!(
        "tikv_br_parquet_stage_duration_seconds",
        "Duration spent in major BR Parquet exporter stages",
        &["stage"],
        exponential_buckets(0.01, 2.0, 16).unwrap()
    )
    .unwrap();
}

#[derive(Clone, Debug, Default)]
pub struct ExporterMetricsSnapshot {
    pub downloaded: u64,
    pub converted: u64,
    pub uploaded: u64,
    pub completed: u64,
    pub rows: u64,
    pub output_bytes: u64,
}

impl ExporterMetricsSnapshot {
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
