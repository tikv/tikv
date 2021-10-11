// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::test_suite::TestSuite;

use std::iter;
use std::thread::sleep;
use std::time::Duration;

use rand::prelude::SliceRandom;
use test_util::alloc_port;

const ONE_SEC: Duration = Duration::from_secs(1);

pub fn case_enable(test_suite: &mut TestSuite) {
    test_suite.reset();
    let port = alloc_port();
    test_suite.start_receiver_at(port);

    // Workload
    // [req-1, req-2]
    test_suite.setup_workload(vec!["req-1", "req-2"]);

    // | Address | Enabled |
    // |   x     |    o    |
    test_suite.cfg_enabled(true);
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ONE_SEC);
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // Workload
    // []
    test_suite.cancel_workload();

    // | Address | Enabled |
    // |   o     |    o    |
    test_suite.cfg_receiver_address(format!("127.0.0.1:{}", port));
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ONE_SEC);
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // Workload
    // [req-1, req-2]
    test_suite.setup_workload(vec!["req-1", "req-2"]);

    // | Address | Enabled |
    // |   o     |    o    |
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ONE_SEC);
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 2);
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));

    // | Address | Enabled |
    // |   x     |    o    |
    test_suite.cfg_receiver_address("");
    test_suite.flush_receiver();
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ONE_SEC);
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // | Address | Enabled |
    // |   o     |    x    |
    test_suite.cfg_enabled(false);
    test_suite.cfg_receiver_address(format!("127.0.0.1:{}", port));
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ONE_SEC);
    assert!(test_suite.fetch_reported_cpu_time().is_empty());
}

pub fn case_report_interval(test_suite: &mut TestSuite) {
    test_suite.reset();
    let port = alloc_port();
    test_suite.start_receiver_at(port);
    test_suite.cfg_enabled(true);
    test_suite.cfg_receiver_address(format!("127.0.0.1:{}", port));

    // Workload
    // [req-1, req-2]
    test_suite.setup_workload(vec!["req-1", "req-2"]);

    // | Report Interval |
    // |       15s       |
    test_suite.cfg_report_receiver_interval("15s");
    test_suite.flush_receiver();

    sleep(Duration::from_secs(5));
    assert!(test_suite.fetch_reported_cpu_time().is_empty());
    sleep(Duration::from_secs(5));
    assert!(test_suite.fetch_reported_cpu_time().is_empty());
    sleep(Duration::from_secs(10));
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 2);
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));

    // | Report Interval |
    // |       5s        |
    test_suite.cfg_report_receiver_interval("5s");
    sleep(Duration::from_secs(10));
    test_suite.flush_receiver();

    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ONE_SEC);
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 2);
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));
}

pub fn case_max_resource_groups(test_suite: &mut TestSuite) {
    test_suite.reset();
    let port = alloc_port();
    test_suite.start_receiver_at(port);
    test_suite.cfg_enabled(true);
    test_suite.cfg_receiver_address(format!("127.0.0.1:{}", port));

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
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ONE_SEC);
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
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ONE_SEC);
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 4);
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));
    assert!(res.contains_key("req-3"));
    assert!(res.contains_key(""));
}

pub fn case_precision(test_suite: &mut TestSuite) {
    test_suite.reset();
    let port = alloc_port();
    test_suite.start_receiver_at(port);
    test_suite.cfg_report_receiver_interval("10s");
    test_suite.cfg_enabled(true);
    test_suite.cfg_receiver_address(format!("127.0.0.1:{}", port));

    // Workload
    // [req-1]
    test_suite.setup_workload(vec!["req-1"]);

    // | Precision |
    // |    1s     |
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ONE_SEC);
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
    test_suite.flush_receiver();
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ONE_SEC);
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
