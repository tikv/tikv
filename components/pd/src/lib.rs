#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate quick_error;

#[macro_use]
extern crate serde_derive;

use futures::Future;

pub use self::errors::{Error, Result};

pub mod config;
pub mod errors;
pub mod metrics;

pub type PdFuture<T> = Box<dyn Future<Item = T, Error = Error> + Send>;

pub const REQUEST_TIMEOUT: u64 = 2; // 2s
