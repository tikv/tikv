// Copyright 2019 PingCAP, Inc.
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

use futures::{Future, IntoFuture};

use crate::util::future::paired_future_callback;
use crate::util::worker::FutureScheduler;

use super::{PdFuture, PdTask, Result};

/// A TSO is composed by calculating `(physical << TSO_PHYSICAL_SHIFT) + logical`.
const TSO_PHYSICAL_SHIFT: u64 = 18;

pub trait Oracle {
    fn get_timestamp(&self) -> Result<u64> {
        self.async_get_timestamp().wait()
    }
    fn async_get_timestamp(&self) -> PdFuture<u64>;
}

pub struct PdOracle {
    scheduler: FutureScheduler<PdTask>,
}

impl PdOracle {
    pub fn new(scheduler: FutureScheduler<PdTask>) -> Self {
        Self { scheduler }
    }
}

impl Oracle for PdOracle {
    fn async_get_timestamp(&self) -> PdFuture<u64> {
        let (callback, future) = paired_future_callback();
        let future = self
            .scheduler
            .schedule(PdTask::Tso { callback })
            .map_err(|e| box_err!("faield to send tso task: {:?}", e))
            .into_future()
            .and_then(move |_| {
                future
                    .map_err(|e| box_err!("failed to receive tso result: {:?}", e))
                    .and_then(|res| {
                        res.map(|(physical, logical)| {
                            ((physical << TSO_PHYSICAL_SHIFT) + logical) as u64
                        })
                    })
            });
        Box::new(future)
    }
}

#[inline]
pub fn compose_ts(physical: i64, logical: i64) -> u64 {
    ((physical << TSO_PHYSICAL_SHIFT) + logical) as u64
}
