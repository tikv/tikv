// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::resource_metering::test_suite::TestSuite;

use std::collections::HashMap;
use std::iter;
use std::thread::sleep;
use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use kvproto::resource_usage_agent::Request;
use rand::prelude::*;
use resource_metering::TEST_TAG_PREFIX;

const ONE_SEC: Duration = Duration::from_secs(1);

pub fn case_normal(test_suite: &mut TestSuite) {
    test_suite.reset();
    test_suite.cfg_enabled(true);
    test_suite.cfg_max_resource_groups(5);

    // Workload
    // [req-{1..5} * 10, req-{6..10} * 1]
    let mut wl = iter::repeat(1..=5)
        .take(10)
        .flatten()
        .chain(6..=10)
        .map(|n| format!("req-{}", n))
        .collect::<Vec<_>>();
    wl.shuffle(&mut rand::thread_rng());
    test_suite.setup_workload(wl);

    let client = test_suite.subscriber_client();
    let stream = client.sub_cpu_time_record(&Request::default()).unwrap();
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ONE_SEC);

    let mut res = HashMap::new();
    test_suite
        .rt
        .block_on(stream.try_collect().for_each(|r| {
            let tag = String::from_utf8_lossy(
                (!r.get_resource_group_tag().is_empty())
                    .then(|| r.resource_group_tag.split_at(TEST_TAG_PREFIX.len()).1)
                    .unwrap_or(b""),
            )
            .into_owned();
            let (ts, cpu_time) = res.entry(tag).or_insert((vec![], vec![]));
            ts.extend(&r.record_list_timestamp_sec);
            cpu_time.extend(&r.record_list_cpu_time_ms);
        }))
        .unwrap();
}
