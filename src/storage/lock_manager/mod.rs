// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod client;
mod config;
pub mod deadlock;
mod leader_change_notifier;
mod metrics;
mod util;
pub mod waiter_manager;

pub use self::config::Config;
pub use self::deadlock::{
    DetectType, Detector, Scheduler as DetectorScheduler, Service, Task as DetectTask,
};
pub use self::leader_change_notifier::Notifier as LeaderChangeNotifier;
pub use self::util::{extract_lock_from_result, gen_key_hash, gen_key_hashes};
pub use self::waiter_manager::{
    store_wait_table_is_empty, wait_table_is_empty, Scheduler as WaiterMgrScheduler,
    Task as WaiterTask, WaiterManager,
};
use crate::pd::Error as PdError;
use futures::future::Future;
use futures::Canceled;
use std::error;
use std::result;
use std::time::Duration;

type DeadlockFuture<T> = Box<dyn Future<Item = T, Error = Error>>;

#[derive(Clone, PartialEq, Debug, Default)]
pub struct Lock {
    pub ts: u64,
    pub hash: u64,
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Grpc(err: grpcio::Error) {
            from()
            cause(err)
            display("{:?}", err)
            description(err.description())
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
        Timeout(d: Duration) {
            description("request timeout")
            display("timeout after {:?}", d)
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
