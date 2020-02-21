// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::*;
use std::io;
use std::result::Result;
use std::time::Duration;
pub trait MetricsFlusher<K, R> {
    fn new(engines: KvEngines<K, R>, interval: Duration) -> Self;
    fn start(&mut self) -> Result<(), io::Error>;
    fn stop(&mut self);
}
