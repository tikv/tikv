// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::thread;
use std::time::Duration;

use prometheus::{self, CounterVec, Encoder, TextEncoder};

lazy_static! {
    pub static ref CHANNEL_FULL_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_channel_full_total",
            "Total number of channel full errors.",
            &["type"]
        ).unwrap();
}

/// `run_prometheus` runs a background prometheus client.
pub fn run_prometheus(
    interval: Duration,
    address: &str,
    job: &str,
) -> Option<thread::JoinHandle<()>> {
    if interval == Duration::from_secs(0) {
        return None;
    }

    let job = job.to_owned();
    let address = address.to_owned();
    let handler = thread::Builder::new()
        .name("promepusher".to_owned())
        .spawn(move || loop {
            let metric_familys = prometheus::gather();

            let res = prometheus::push_metrics(
                &job,
                prometheus::hostname_grouping_key(),
                &address,
                metric_familys,
            );
            if let Err(e) = res {
                error!("fail to push metrics: {}", e);
            }

            thread::sleep(interval);
        })
        .unwrap();

    Some(handler)
}

pub fn dump_metrics() -> String {
    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    let metric_familys = prometheus::gather();
    for mf in metric_familys {
        if let Err(e) = encoder.encode(&[mf], &mut buffer) {
            warn!("ignore prometheus encoding error: {:?}", e);
        }
    }
    String::from_utf8(buffer).unwrap()
}
