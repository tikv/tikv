// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::CpuRecorder;

#[cfg(not(target_os = "linux"))]
mod dummy;
#[cfg(not(target_os = "linux"))]
pub use dummy::CpuRecorder;
