// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::resource_metering::test_suite::TestSuite;

use std::collections::HashMap;
use std::iter;

use futures::StreamExt;
use kvproto::resource_usage_agent::Request;
use rand::prelude::*;
use resource_metering::TEST_TAG_PREFIX;

pub fn case_basic(test_suite: &mut TestSuite) {
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

    // one subscriber
    {
        let client = test_suite.subscriber_client();
        let stream = client.sub_cpu_time_record(&Request::default()).unwrap();
        let res = test_suite.rt.block_on(async move {
            let mut res: HashMap<String, (Vec<u64>, Vec<u32>)> = HashMap::new();

            let stream = stream.take(6);
            let records = stream.collect::<Vec<_>>().await;
            for r in records {
                let r = r.unwrap();
                let tag = String::from_utf8_lossy(
                    (!r.get_resource_group_tag().is_empty())
                        .then(|| r.resource_group_tag.split_at(TEST_TAG_PREFIX.len()).1)
                        .unwrap_or(b""),
                )
                .into_owned();
                let (ts, cpu_time) = res.entry(tag).or_insert((vec![], vec![]));
                ts.extend(&r.record_list_timestamp_sec);
                cpu_time.extend(&r.record_list_cpu_time_ms);
            }

            res
        });
        assert!(res.contains_key("req-1"));
        assert!(res.contains_key("req-2"));
        assert!(res.contains_key("req-3"));
        assert!(res.contains_key("req-4"));
        assert!(res.contains_key("req-5"));
        assert!(res.contains_key(""));
    }

    // two subscribers
    {
        let client = test_suite.subscriber_client();
        let stream1 = client.sub_cpu_time_record(&Request::default()).unwrap();
        let client = test_suite.subscriber_client();
        let stream2 = client.sub_cpu_time_record(&Request::default()).unwrap();
        let (res1, res2) = test_suite.rt.block_on(async move {
            let mut res1: HashMap<String, (Vec<u64>, Vec<u32>)> = HashMap::new();
            let mut res2: HashMap<String, (Vec<u64>, Vec<u32>)> = HashMap::new();

            let stream = stream1.take(6);
            let records1 = stream.collect::<Vec<_>>().await;
            let stream = stream2.take(6);
            let records2 = stream.collect::<Vec<_>>().await;
            for (records, res) in [(records1, &mut res1), (records2, &mut res2)] {
                for r in records {
                    let r = r.unwrap();
                    let tag = String::from_utf8_lossy(
                        (!r.get_resource_group_tag().is_empty())
                            .then(|| r.resource_group_tag.split_at(TEST_TAG_PREFIX.len()).1)
                            .unwrap_or(b""),
                    )
                    .into_owned();
                    let (ts, cpu_time) = res.entry(tag).or_insert((vec![], vec![]));
                    ts.extend(&r.record_list_timestamp_sec);
                    cpu_time.extend(&r.record_list_cpu_time_ms);
                }
            }

            (res1, res2)
        });
        for res in [res1, res2] {
            assert!(res.contains_key("req-1"));
            assert!(res.contains_key("req-2"));
            assert!(res.contains_key("req-3"));
            assert!(res.contains_key("req-4"));
            assert!(res.contains_key("req-5"));
            assert!(res.contains_key(""));
        }
    }
}
