// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::record::Records;
use crate::cpu::reporter::endpoint::grpc::GRPCEndpoint;
use crate::cpu::reporter::endpoint::vmetrics::VictoriaMetricsEndpoint;

pub mod grpc;
pub mod vmetrics;

mod util;

pub trait Endpoint: Send + 'static {
    fn init(instance_name: &str, address: &str) -> Box<dyn Endpoint>
    where
        Self: Sized;
    fn update(&mut self, address: &str);
    fn report(&mut self, records: Records);
    fn name(&self) -> &'static str;
}

pub fn init(endpoint_type: &str, instance_name: &str, address: &str) -> Box<dyn Endpoint> {
    match endpoint_type {
        "grpc" => GRPCEndpoint::init(instance_name, address),
        "victoria-metrics" => VictoriaMetricsEndpoint::init(instance_name, address),
        _ => unreachable!(),
    }
}
