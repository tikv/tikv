// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::test_suite::TestSuite;

use std::iter;
use std::thread::sleep;
use std::time::Duration;

use rand::prelude::SliceRandom;
use test_util::alloc_port;
use tikv_util::config::ReadableDuration;

#[test]
pub fn test_enable() {
    let mut test_suite = TestSuite::new(resource_metering::Config {
        enabled: false,
        receiver_address: "".to_string(),
        report_receiver_interval: ReadableDuration::millis(500),
        max_resource_groups: 5000,
        precision: ReadableDuration::millis(100),
    });

    let port = alloc_port();
    test_suite.start_receiver_at(port);

    // Workload
    // [req-1, req-2]
    test_suite.setup_workload(vec!["req-1", "req-2"]);

    // | Address | Enabled |
    // |   x     |    o    |
    test_suite.cfg_enabled(true);
    sleep(Duration::from_millis(550));
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // Workload
    // []
    test_suite.cancel_workload();

    // | Address | Enabled |
    // |   o     |    o    |
    test_suite.cfg_receiver_address(format!("127.0.0.1:{}", port));
    test_suite.flush_receiver();
    sleep(Duration::from_millis(550));
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // Workload
    // [req-1, req-2]
    test_suite.setup_workload(vec!["req-1", "req-2"]);

    // | Address | Enabled |
    // |   o     |    o    |
    sleep(Duration::from_millis(550));
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 2);
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));

    // | Address | Enabled |
    // |   x     |    o    |
    test_suite.cfg_receiver_address("");
    test_suite.flush_receiver();
    sleep(Duration::from_millis(550));
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // | Address | Enabled |
    // |   o     |    x    |
    test_suite.cfg_enabled(false);
    test_suite.cfg_receiver_address(format!("127.0.0.1:{}", port));
    sleep(Duration::from_millis(550));
    assert!(test_suite.fetch_reported_cpu_time().is_empty());
}

#[test]
pub fn test_report_interval() {
    let port = alloc_port();
    let mut test_suite = TestSuite::new(resource_metering::Config {
        enabled: true,
        receiver_address: format!("127.0.0.1:{}", port),
        report_receiver_interval: ReadableDuration::millis(500),
        max_resource_groups: 5000,
        precision: ReadableDuration::millis(100),
    });

    test_suite.start_receiver_at(port);

    // Workload
    // [req-1, req-2]
    test_suite.setup_workload(vec!["req-1", "req-2"]);

    // | Report Interval |
    // |       1s        |
    test_suite.cfg_report_receiver_interval("1s");
    test_suite.flush_receiver();

    sleep(Duration::from_millis(550));
    assert!(test_suite.fetch_reported_cpu_time().is_empty());
    sleep(Duration::from_millis(550));
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 2);
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));

    // | Report Interval |
    // |       2s        |
    test_suite.cfg_report_receiver_interval("2s");
    test_suite.flush_receiver();

    sleep(Duration::from_millis(2200));
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 2);
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));
}

#[test]
pub fn test_max_resource_groups() {
    let port = alloc_port();
    let mut test_suite = TestSuite::new(resource_metering::Config {
        enabled: true,
        receiver_address: format!("127.0.0.1:{}", port),
        report_receiver_interval: ReadableDuration::secs(3),
        max_resource_groups: 5000,
        precision: ReadableDuration::secs(1),
    });

    test_suite.start_receiver_at(port);

    // Workload
    // [req-{1..3} * 10, req-{4..5} * 1]
    let mut wl = iter::repeat(1..=3)
        .take(10)
        .flatten()
        .chain(4..=5)
        .map(|n| format!("req-{}", n))
        .collect::<Vec<_>>();
    wl.shuffle(&mut rand::thread_rng());
    test_suite.setup_workload(wl);

    // | Max Resource Groups |
    // |       5000          |
    sleep(Duration::from_millis(3500));
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 5);
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));
    assert!(res.contains_key("req-3"));
    assert!(res.contains_key("req-4"));
    assert!(res.contains_key("req-5"));

    // | Max Resource Groups |
    // |        3            |
    test_suite.cfg_max_resource_groups(3);
    test_suite.flush_receiver();
    sleep(Duration::from_millis(3500));
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 4);
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));
    assert!(res.contains_key("req-3"));
    assert!(res.contains_key(""));
}

#[test]
pub fn test_precision() {
    let port = alloc_port();
    let mut test_suite = TestSuite::new(resource_metering::Config {
        enabled: true,
        receiver_address: format!("127.0.0.1:{}", port),
        report_receiver_interval: ReadableDuration::millis(1500),
        max_resource_groups: 5000,
        precision: ReadableDuration::secs(1),
    });

    test_suite.start_receiver_at(port);

    // Workload
    // [req-1]
    test_suite.setup_workload(vec!["req-1"]);

    // | Precision |
    // |    1s     |
    sleep(Duration::from_millis(2000));
    let res = test_suite.fetch_reported_cpu_time();
    let (secs, _) = res.get("req-1").unwrap();
    for (l, r) in secs.iter().zip({
        let mut next_secs = secs.iter();
        next_secs.next();
        next_secs
    }) {
        let diff = r - l;
        assert!(diff <= 2);
    }

    // | Precision |
    // |    3s     |
    test_suite.cfg_precision("3s");
    test_suite.cfg_report_receiver_interval("5s");
    test_suite.flush_receiver();
    sleep(Duration::from_millis(6000));
    let res = test_suite.fetch_reported_cpu_time();
    let (secs, _) = res.get("req-1").unwrap();
    for (l, r) in secs.iter().zip({
        let mut next_secs = secs.iter();
        next_secs.next();
        next_secs
    }) {
        let diff = r - l;
        assert!(2 <= diff && diff <= 4);
    }
}
