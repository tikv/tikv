// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cpu::CpuRecords;
use crate::summary::SummaryRecord;
use collections::HashMap;

/// This trait abstracts the interface to communicate with the remote.
/// We can simply mock this interface to test without RPC.
pub trait Client {
    fn upload_cpu_records(&mut self, _address: &str, _v: &CpuRecords) {}

    fn upload_summary_records(&mut self, _address: &str, _v: &HashMap<Vec<u8>, SummaryRecord>) {}
}

/// `GrpcClient` is the default implementation of [Client], which uses gRPC
/// to report data to the remote end.
#[derive(Clone)]
pub struct GrpcClient {}

impl Client for GrpcClient {
    fn upload_cpu_records(&mut self, address: &str, v: &CpuRecords) {
        println!(
            ">>> upload_cpu_records: {}, {:?}, {:?}",
            address, v.records, v.others
        );
    }

    fn upload_summary_records(&mut self, address: &str, v: &HashMap<Vec<u8>, SummaryRecord>) {
        println!(">>> upload_summary_records: {}, {:?}", address, v);
    }
}

impl GrpcClient {
    pub fn new() -> Self {
        Self {}
    }
}
