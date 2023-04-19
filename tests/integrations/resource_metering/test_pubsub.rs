// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;

use futures::{executor::block_on, StreamExt};
use tikv_util::config::ReadableDuration;

use crate::resource_metering::test_suite::TestSuite;

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

    let (_client, stream) = test_suite.subscribe();
    let tags = stream.take(4).map(|record| {
        String::from_utf8_lossy(record.unwrap().get_record().get_resource_group_tag()).into_owned()
    });
    let res = block_on(tags.collect::<HashSet<_>>());

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
                let tags = stream.take(4).map(|record| {
                    String::from_utf8_lossy(record.unwrap().get_record().get_resource_group_tag())
                        .into_owned()
                });
                tags.collect::<HashSet<_>>().await
            })
        })
        .collect();

    for jh in jhs {
        let res = test_suite.rt.block_on(jh).unwrap();
        assert!(res.contains("req-1"));
        assert!(res.contains("req-2"));
    }
}
