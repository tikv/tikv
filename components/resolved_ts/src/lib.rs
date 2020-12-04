// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
#![feature(box_patterns)]

#[macro_use]
extern crate tikv_util;
#[macro_use(fail_point)]
extern crate fail;

mod endpoint;
mod errors;
mod metrics;
mod observer;
mod resolver;
mod scanner;
mod util;

pub use resolver::Resolver;
