// Copyright 2016 Neil Shen <overvenus@gmail.com>.
// Copyright 2015 The Rust Project Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::error;
use std::fmt;
use std::mem;
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};

#[macro_use]
pub mod macros;

static mut CLIENT: *const Metric = &NopMetric;
static STATE: AtomicUsize = ATOMIC_USIZE_INIT;
const UNINITIALIZED: usize = 0;
const INITIALIZED: usize = 1;

pub trait Metric: Sync + Send {
    /// Increment the counter by `1`
    fn incr(&self, key: &str);

    /// Decrement the counter by `1`
    fn decr(&self, key: &str);

    /// Increment or decrement the counter by the given amount
    fn count(&self, key: &str, count: i64);

    /// Record a timing in milliseconds with the given key
    fn time(&self, key: &str, time: u64);

    /// Record a gauge value with the given key
    fn gauge(&self, key: &str, value: u64);

    /// Record a single metered event with the given key
    fn mark(&self, key: &str);

    /// Record a meter value with the given key
    fn meter(&self, key: &str, value: u64);
}

pub struct NopMetric;

impl Metric for NopMetric {
    fn incr(&self, _: &str) {}

    fn decr(&self, _: &str) {}

    fn count(&self, _: &str, _: i64) {}

    fn time(&self, _: &str, _: u64) {}

    fn gauge(&self, _: &str, _: u64) {}

    fn mark(&self, _: &str) {}

    fn meter(&self, _: &str, _: u64) {}
}

/// The type returned by `set_metric` if `set_metric` has already been called.
#[allow(missing_copy_implementations)]
#[derive(Debug)]
pub struct SetMetricError(());

impl fmt::Display for SetMetricError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt,
               "attempted to set a metric client after the metric system was already initialized")
    }
}

// The Error trait is not available in libcore
impl error::Error for SetMetricError {
    fn description(&self) -> &str {
        "set_metric() called multiple times"
    }
}

pub fn set_metric<M>(make_metric: M) -> Result<(), SetMetricError>
    where M: FnOnce() -> Box<Metric>
{
    unsafe {
        if STATE.compare_and_swap(UNINITIALIZED, INITIALIZED, Ordering::SeqCst) != UNINITIALIZED {
            return Err(SetMetricError(()));
        }

        CLIENT = mem::transmute(make_metric());
        Ok(())
    }
}

#[doc(hidden)]
pub fn __client() -> Option<&'static Metric> {
    if STATE.load(Ordering::SeqCst) != INITIALIZED {
        None
    } else {
        Some(unsafe { &*CLIENT })
    }
}
