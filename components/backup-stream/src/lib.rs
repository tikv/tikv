// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
#![feature(slice_group_by)]
#![feature(result_flattening)]
#![feature(assert_matches)]
#![feature(test)]

mod checkpoint_manager;
pub mod config;
mod endpoint;
pub mod errors;
mod event_loader;
pub mod metadata;
pub(crate) mod metrics;
pub mod observer;
pub mod router;
mod service;
mod subscription_manager;
mod subscription_track;
mod utils;

pub use checkpoint_manager::GetCheckpointResult;
pub use endpoint::{Endpoint, ObserveOp, RegionCheckpointOperation, RegionSet, Task};
pub use service::Service;
