// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::resource_metering::test_suite::TestSuite;

use std::time::Duration;

use tikv_util::config::ReadableDuration;

const DEFAULT_DELAY: Duration = Duration::from_secs(10);

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

    let mut subsriber = test_suite.subscribe();
    let res = subsriber.next_batch_tags(DEFAULT_DELAY);

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

    let mut subsriber1 = test_suite.subscribe();
    let mut subsriber2 = test_suite.subscribe();

    let res1 = subsriber1.next_batch_tags(DEFAULT_DELAY);
    assert!(res1.contains("req-1"));
    assert!(res1.contains("req-2"));

    let res2 = subsriber2.next_batch_tags(DEFAULT_DELAY);
    assert!(res2.contains("req-1"));
    assert!(res2.contains("req-2"));
}

#[test]
pub fn test_reconnect_subscriber() {
    let mut test_suite = TestSuite::new(resource_metering::Config {
        report_receiver_interval: ReadableDuration::secs(3),
        precision: ReadableDuration::secs(1),
        ..Default::default()
    });

    // Workload
    // [req-1, req-2]
    test_suite.setup_workload(vec!["req-1", "req-2"]);

    [0..3].iter().for_each(|_| {
        let mut subsriber = test_suite.subscribe();

        let res1 = subsriber.next_batch_tags(DEFAULT_DELAY);
        assert!(res1.contains("req-1"));
        assert!(res1.contains("req-2"));
    });
}
