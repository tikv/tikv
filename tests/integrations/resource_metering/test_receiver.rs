// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::test_suite::TestSuite;

use std::thread::sleep;
use std::time::Duration;

use test_util::alloc_port;
use tikv_util::config::ReadableDuration;

#[test]
pub fn test_alter_receiver_addr() {
    let port = alloc_port();
    let mut test_suite = TestSuite::new(resource_metering::Config {
        enabled: true,
        receiver_address: "".to_string(),
        report_receiver_interval: ReadableDuration::millis(500),
        max_resource_groups: 5000,
        precision: ReadableDuration::millis(100),
    });
    test_suite.start_receiver_at(port);

    // Workload
    // [req-1, req-2]
    test_suite.setup_workload(vec!["req-1", "req-2"]);

    // | Address | Enabled |
    // |   x     |    o    |
    sleep(Duration::from_millis(550));
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // | Address | Enabled |
    // |   o     |    o    |
    test_suite.cfg_receiver_address(format!("127.0.0.1:{}", port));
    sleep(Duration::from_millis(550));
    let res = test_suite.fetch_reported_cpu_time();
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));

    // | Address | Enabled |
    // |   !     |    o    |
    test_suite.cfg_receiver_address(format!("127.0.0.1:{}", port + 1));
    test_suite.flush_receiver();
    sleep(Duration::from_millis(550));
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // | Address | Enabled |
    // |   o     |    o    |
    test_suite.cfg_receiver_address(format!("127.0.0.1:{}", port));
    test_suite.flush_receiver();
    sleep(Duration::from_millis(550));
    let res = test_suite.fetch_reported_cpu_time();
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));
}

#[test]
pub fn test_receiver_blocking() {
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

    // | Block Receiver |
    // |       x        |
    sleep(Duration::from_millis(550));
    let res = test_suite.fetch_reported_cpu_time();
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));

    // | Block Receiver |
    // |       o        |
    test_suite.block_receiver();
    test_suite.flush_receiver();
    sleep(Duration::from_millis(550));
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // Workload
    // [req-3, req-4]
    test_suite.cancel_workload();
    test_suite.setup_workload(vec!["req-3", "req-4"]);

    // | Block Receiver |
    // |       x        |
    test_suite.unblock_receiver();
    sleep(Duration::from_millis(1100));
    test_suite.flush_receiver();
    sleep(Duration::from_millis(550));
    let res = test_suite.fetch_reported_cpu_time();
    assert!(res.contains_key("req-3"));
    assert!(res.contains_key("req-4"));
}

#[test]
pub fn test_receiver_shutdown() {
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

    // | Receiver Alive |
    // |       o        |
    sleep(Duration::from_millis(550));
    let res = test_suite.fetch_reported_cpu_time();
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));

    // Workload
    // [req-3, req-4]
    test_suite.cancel_workload();
    test_suite.setup_workload(vec!["req-3", "req-4"]);

    // | Receiver Alive |
    // |       x        |
    test_suite.shutdown_receiver();
    test_suite.flush_receiver();
    sleep(Duration::from_millis(550));
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // | Receiver Alive |
    // |       o        |
    test_suite.start_receiver_at(port);
    sleep(Duration::from_millis(550));
    let res = test_suite.fetch_reported_cpu_time();
    assert!(res.contains_key("req-3"));
    assert!(res.contains_key("req-4"));
}
