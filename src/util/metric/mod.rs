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
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};

use cadence::prelude::*;

#[macro_use]
pub mod macros;

static mut CLIENT: Option<*const Metric> = None;
static STATE: AtomicUsize = ATOMIC_USIZE_INIT;
const UNINITIALIZED: usize = 0;
const INITIALIZED: usize = 1;

pub trait Metric: Counted + Gauged + Metered + Timed {}

impl<T: Counted + Gauged + Metered + Timed> Metric for T {}

/// The type returned by `set_metric_client` if `set_metric_client` has already been called.
#[allow(missing_copy_implementations)]
#[derive(Debug)]
pub struct SetMetricError(());

impl fmt::Display for SetMetricError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt,
               "attempted to set a metric client after the metric system was already initialized")
    }
}

impl error::Error for SetMetricError {
    fn description(&self) -> &str {
        "set_metric_client() called multiple times"
    }
}

pub fn set_metric_client(client: Box<Metric + Send + Sync>) -> Result<(), SetMetricError> {
    unsafe {
        if STATE.compare_and_swap(UNINITIALIZED, INITIALIZED, Ordering::SeqCst) != UNINITIALIZED {
            return Err(SetMetricError(()));
        }

        CLIENT = Some(Box::into_raw(client));
        Ok(())
    }
}

#[doc(hidden)]
pub fn __client() -> Option<&'static Metric> {
    if STATE.load(Ordering::SeqCst) != INITIALIZED {
        return None;
    }

    unsafe { CLIENT.map_or(None, |c| Some(&*c)) }
}
