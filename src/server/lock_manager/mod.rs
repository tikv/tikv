// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod client;
mod config;
pub mod deadlock;
mod metrics;
pub mod waiter_manager;

pub use self::config::Config;
pub use self::deadlock::{
    register_detector_role_change_observer, Detector, Scheduler as DetectorScheduler, Service,
};

pub use self::waiter_manager::{
    Scheduler as WaiterMgrScheduler, Task as WaiterTask, WaiterManager,
};
use futures::future::Future;
use futures::Canceled;
use pd_client::Error as PdError;
use std::error;
use std::result;

type DeadlockFuture<T> = Box<dyn Future<Item = T, Error = Error>>;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Grpc(err: grpcio::Error) {
            from()
            cause(err)
            display("{:?}", err)
            description(err.description())
        }
        NoLeader {
            display("no leader")
            description("no leader")
        }
        Canceled(err: Canceled) {
            from()
            cause(err)
            display("{:?}", err)
            description(err.description())
        }
        Pd(err: PdError) {
            from()
            cause(err)
            display("{:?}", err)
            description(err.description())
        }
        Other(err: Box<dyn error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            display("{:?}", err)
            description(err.description())
        }
    }
}

pub type Result<T> = result::Result<T, Error>;
