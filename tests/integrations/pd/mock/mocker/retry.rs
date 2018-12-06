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

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use kvproto::pdpb::*;
use tikv::pd::RECONNECT_INTERVAL_SEC;

use super::*;

#[derive(Debug)]
pub struct Retry {
    retry: usize,
    count: AtomicUsize,
}

impl Retry {
    pub fn new(retry: usize) -> Retry {
        info!("[Retry] return Ok(_) every {:?} times", retry);

        Retry {
            retry,
            count: AtomicUsize::new(0),
        }
    }

    fn is_ok(&self) -> bool {
        let count = self.count.fetch_add(1, Ordering::SeqCst);
        if count != 0 && count % self.retry == 0 {
            // it's ok.
            return true;
        }
        // let's sleep awhile, so that client will update its connection.
        thread::sleep(Duration::from_secs(RECONNECT_INTERVAL_SEC));
        false
    }
}

impl PdMocker for Retry {
    fn get_region_by_id(&self, _: &GetRegionByIDRequest) -> Option<Result<GetRegionResponse>> {
        if self.is_ok() {
            info!("[Retry] get_region_by_id returns Ok(_)");
            Some(Ok(GetRegionResponse::new()))
        } else {
            info!("[Retry] get_region_by_id returns Err(_)");
            Some(Err("please retry".to_owned()))
        }
    }

    fn get_store(&self, _: &GetStoreRequest) -> Option<Result<GetStoreResponse>> {
        if self.is_ok() {
            info!("[Retry] get_store returns Ok(_)");
            Some(Ok(GetStoreResponse::new()))
        } else {
            info!("[Retry] get_store returns Err(_)");
            Some(Err("please retry".to_owned()))
        }
    }
}

#[derive(Debug)]
pub struct NotRetry {
    is_visited: AtomicBool,
}

impl NotRetry {
    pub fn new() -> NotRetry {
        info!(
            "[NotRetry] return error response for the first time and return Ok() for reset times."
        );

        NotRetry {
            is_visited: AtomicBool::new(false),
        }
    }
}

impl PdMocker for NotRetry {
    fn get_region_by_id(&self, _: &GetRegionByIDRequest) -> Option<Result<GetRegionResponse>> {
        if !self.is_visited.swap(true, Ordering::Relaxed) {
            info!(
                "[NotRetry] get_region_by_id returns Ok(_) with header has INCOMPATIBLE_VERSION error"
            );
            let mut err = Error::new();
            err.set_field_type(ErrorType::INCOMPATIBLE_VERSION);
            let mut resp = GetRegionResponse::new();
            resp.mut_header().set_error(err);
            Some(Ok(resp))
        } else {
            info!("[NotRetry] get_region_by_id returns Ok()");
            Some(Ok(GetRegionResponse::new()))
        }
    }

    fn get_store(&self, _: &GetStoreRequest) -> Option<Result<GetStoreResponse>> {
        if !self.is_visited.swap(true, Ordering::Relaxed) {
            info!(
                "[NotRetry] get_region_by_id returns Ok(_) with header has INCOMPATIBLE_VERSION error"
            );
            let mut err = Error::new();
            err.set_field_type(ErrorType::INCOMPATIBLE_VERSION);
            let mut resp = GetStoreResponse::new();
            resp.mut_header().set_error(err);
            Some(Ok(resp))
        } else {
            info!("[NotRetry] get_region_by_id returns Ok()");
            Some(Ok(GetStoreResponse::new()))
        }
    }
}
