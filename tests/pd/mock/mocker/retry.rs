// Copyright 2017 PingCAP, Inc.
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

use std::sync::atomic::{AtomicUsize, Ordering};

use grpc::error::GrpcError;

use kvproto::pdpb::*;

use super::Mocker;
use super::Result;

#[derive(Debug)]
pub struct Retry {
    retry: usize,
    count: AtomicUsize,
}

impl Retry {
    pub fn new(retry: usize) -> Retry {
        info!("[Retry] return Ok(_) every {:?} times", retry);

        Retry {
            retry: retry,
            count: AtomicUsize::new(0),
        }
    }
}

impl Mocker for Retry {
    fn get_region_by_id(&self, _: &GetRegionByIDRequest) -> Option<Result<GetRegionResponse>> {
        let count = self.count.fetch_add(1, Ordering::SeqCst);
        if count != 0 && count % self.retry == 0 {
            info!("[Retry] return Ok(_)");
            Some(Ok(GetRegionResponse::new()))
        } else {
            info!("[Retry] return Err(_)");
            Some(Err(GrpcError::Other("Please retry")))
        }
    }
}
