// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

/// Start profiling. Always returns false if `profiling` feature is not enabled.
pub fn start(_name: impl AsRef<str>) -> bool {
    // Do nothing
    false
}

/// Stop profiling. Always returns false if `profiling` feature is not enabled.
pub fn stop() -> bool {
    // Do nothing
    false
}
