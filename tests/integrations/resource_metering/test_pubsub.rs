// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::resource_metering::test_suite::TestSuite;

use collections::hash_set_with_capacity;
use futures::StreamExt;
use tikv_util::config::ReadableDuration;

#[test]
pub fn test_basic() {
    let mut test_suite = TestSuite::new(resource_metering::Config {
        report_receiver_interval: ReadableDuration::secs(3),
        precision: ReadableDuration::secs(1),
        ..Default::default()
    });

    // Workload
    // [req-1, req-2]
    test_suite.setup_workload(vec!["req-1", "req-2"]);

    let (client, stream) = test_suite.subscribe();
    let res = test_suite.rt.block_on(async move {
        let _client = client;
        let mut res = hash_set_with_capacity(4);

        let stream = stream.take(4);
        let records = stream.collect::<Vec<_>>().await;
        for r in records {
            let r = r.unwrap();
            let tag = String::from_utf8_lossy(r.get_record().get_resource_group_tag()).into_owned();
            res.insert(tag);
        }

        res
    });

    assert!(res.contains("req-1"));
    assert!(res.contains("req-2"));
}

#[test]
pub fn test_multiple_subscribers() {
    let mut test_suite = TestSuite::new(resource_metering::Config {
        report_receiver_interval: ReadableDuration::secs(3),
        precision: ReadableDuration::secs(1),
        ..Default::default()
    });

    // Workload
    // [req-1, req-2]
    test_suite.setup_workload(vec!["req-1", "req-2"]);
    let jhs: Vec<_> = (0..3)
        .map(|_| {
            let (client, stream) = test_suite.subscribe();
            test_suite.rt.spawn(async move {
                let _client = client;
                let mut res = hash_set_with_capacity(4);

                let stream = stream.take(4);
                let records = stream.collect::<Vec<_>>().await;
                for r in records {
                    let r = r.unwrap();
                    let tag = String::from_utf8_lossy(r.get_record().get_resource_group_tag())
                        .into_owned();
                    res.insert(tag);
                }

                res
            })
        })
        .collect();

    for jh in jhs {
        let res = test_suite.rt.block_on(jh).unwrap();
        assert!(res.contains("req-1"));
        assert!(res.contains("req-2"));
    }
}
