// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::*;


lazy_static! {
  pub static ref PRIORITY_FOR_DIFFERENT_LEVEL: HistogramVec = register_histogram_vec!(
      "tikv_resource_control_priority_in_levels",
      "The priority of different levels",
      &["level"],
      exponential_buckets(0.001, 2.0, 16).unwrap()
  )
  .unwrap();
}