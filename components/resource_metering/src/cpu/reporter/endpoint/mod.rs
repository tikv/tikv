// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::record::Records;

pub mod grpc;

mod util;

pub trait Endpoint: Send + 'static {
    fn report(&mut self, address: &str, records: Records);
}
