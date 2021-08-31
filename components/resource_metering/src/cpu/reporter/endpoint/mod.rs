// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::record::Records;

pub mod grpc;
pub mod vm;

mod util;

pub trait Endpoint {
    fn update(&mut self, address: &str);
    fn report(&mut self, records: Records);
}
