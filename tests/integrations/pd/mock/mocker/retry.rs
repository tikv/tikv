// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use kvproto::pdpb::*;
use pd_client::RECONNECT_INTERVAL_SEC;

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
    fn get_region_by_id(&self, _: &GetRegionByIdRequest) -> Option<Result<GetRegionResponse>> {
        if self.is_ok() {
            info!("[Retry] get_region_by_id returns Ok(_)");
            Some(Ok(GetRegionResponse::default()))
        } else {
            info!("[Retry] get_region_by_id returns Err(_)");
            Some(Err("please retry".to_owned()))
        }
    }

    fn get_store(&self, _: &GetStoreRequest) -> Option<Result<GetStoreResponse>> {
        if self.is_ok() {
            info!("[Retry] get_store returns Ok(_)");
            Some(Ok(GetStoreResponse::default()))
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
    fn get_region_by_id(&self, _: &GetRegionByIdRequest) -> Option<Result<GetRegionResponse>> {
        if !self.is_visited.swap(true, Ordering::Relaxed) {
            info!(
                "[NotRetry] get_region_by_id returns Ok(_) with header has IncompatibleVersion error"
            );
            let mut err = Error::default();
            err.set_type(ErrorType::IncompatibleVersion);
            let mut resp = GetRegionResponse::default();
            resp.mut_header().set_error(err);
            Some(Ok(resp))
        } else {
            info!("[NotRetry] get_region_by_id returns Ok()");
            Some(Ok(GetRegionResponse::default()))
        }
    }

    fn get_store(&self, _: &GetStoreRequest) -> Option<Result<GetStoreResponse>> {
        if !self.is_visited.swap(true, Ordering::Relaxed) {
            info!(
                "[NotRetry] get_region_by_id returns Ok(_) with header has IncompatibleVersion error"
            );
            let mut err = Error::default();
            err.set_type(ErrorType::IncompatibleVersion);
            let mut resp = GetStoreResponse::default();
            resp.mut_header().set_error(err);
            Some(Ok(resp))
        } else {
            info!("[NotRetry] get_region_by_id returns Ok()");
            Some(Ok(GetStoreResponse::default()))
        }
    }
}
