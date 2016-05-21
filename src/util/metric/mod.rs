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
use std::sync::atomic::{AtomicBool, Ordering};

use cadence::prelude::*;

#[macro_use]
pub mod macros;

static mut CLIENT: Option<*const Metric> = None;
// IS_INITIALIZED indicates the state of CLIENT,
// `false` for uninitialized, `true` for initialized.
static IS_INITIALIZED: AtomicBool = AtomicBool::new(false);

pub trait Metric: Counted + Gauged + Metered + Timed {}

impl<T: Counted + Gauged + Metered + Timed> Metric for T {}

/// The type returned by `set_metric_client` if `set_metric_client` has already been called.
#[derive(Debug)]
pub struct SetMetricError;

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
        if IS_INITIALIZED.compare_and_swap(false, true, Ordering::SeqCst) != false {
            return Err(SetMetricError);
        }

        CLIENT = Some(Box::into_raw(client));
        Ok(())
    }
}

#[doc(hidden)]
pub fn client() -> Option<&'static Metric> {
    if IS_INITIALIZED.load(Ordering::SeqCst) != true {
        return None;
    }

    unsafe { CLIENT.map(|c| &*c) }
}
