// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::record::Records;

pub mod grpc;

mod util;

pub trait Endpoint: Send + 'static {
    fn init(address: &str) -> Box<dyn Endpoint>
    where
        Self: Sized;
    fn update(&mut self, address: &str);
    fn report(&mut self, records: Records);
}
