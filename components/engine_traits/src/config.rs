// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

pub use crate::perf_context::PerfLevel;
use tikv_util::rocksdb_enum_mod;

rocksdb_enum_mod! {perf_level_serde PerfLevel {
    Uninitialized = 0,
    Disable = 1,
    EnableCount = 2,
    EnableTimeExceptForMutex = 3,
    EnableTimeAndCPUTimeExceptForMutex = 4,
    EnableTime = 5,
    OutOfBounds = 6,
}}
