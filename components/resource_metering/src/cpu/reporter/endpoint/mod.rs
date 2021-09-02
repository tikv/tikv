// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::record::Records;
use crate::cpu::reporter::endpoint::grpc::GRPCEndpoint;
use crate::cpu::reporter::endpoint::vmetrics::VictoriaMetricsEndpoint;

pub mod grpc;
pub mod vmetrics;

mod util;

pub trait Endpoint: Send + 'static {
    fn report(&mut self, instance_name: &str, address: &str, records: Records);
    fn name(&self) -> &'static str;
}

pub fn init(endpoint_type: &str) -> Box<dyn Endpoint> {
    match endpoint_type {
        "grpc" => Box::new(GRPCEndpoint::default()),
        "victoria-metrics" => Box::new(VictoriaMetricsEndpoint::default()),
        _ => unreachable!(),
    }
}
