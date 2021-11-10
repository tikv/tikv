// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::test_suite::TestSuite;

use std::thread::sleep;
use std::time::Duration;

use test_util::alloc_port;

const ERROR_DELAY: Duration = Duration::from_millis(100);

pub fn case_alter_receiver_addr(test_suite: &mut TestSuite) {
    test_suite.reset();
    let port = alloc_port();
    test_suite.start_receiver_at(port);
    test_suite.cfg_enabled(true);

    // Workload
    // [req-1, req-2]
    test_suite.setup_workload(vec!["req-1", "req-2"]);

    // | Address | Enabled |
    // |   x     |    o    |
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ERROR_DELAY);
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // | Address | Enabled |
    // |   o     |    o    |
    test_suite.cfg_receiver_address(format!("127.0.0.1:{}", port));
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ERROR_DELAY);
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 2);
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));

    // | Address | Enabled |
    // |   !     |    o    |
    test_suite.cfg_receiver_address(format!("127.0.0.1:{}", port + 1));
    test_suite.flush_receiver();
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ERROR_DELAY);
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // | Address | Enabled |
    // |   o     |    o    |
    test_suite.cfg_receiver_address(format!("127.0.0.1:{}", port));
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ERROR_DELAY);
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 2);
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));
}

pub fn case_receiver_blocking(test_suite: &mut TestSuite) {
    test_suite.reset();
    let port = alloc_port();
    test_suite.start_receiver_at(port);
    test_suite.cfg_enabled(true);
    test_suite.cfg_receiver_address(format!("127.0.0.1:{}", port));

    // Workload
    // [req-1, req-2]
    test_suite.setup_workload(vec!["req-1", "req-2"]);

    // | Block Receiver |
    // |      x      |
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ERROR_DELAY);
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 2);
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));

    // | Block Receiver |
    // |        o       |
    fail::cfg("mock-receiver", "sleep(1000)").unwrap();
    test_suite.flush_receiver();
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ERROR_DELAY);
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // | Block Receiver |
    // |        x       |
    fail::remove("mock-receiver");
    test_suite.flush_receiver();
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ERROR_DELAY);
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 2);
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));
}

pub fn case_receiver_shutdown(test_suite: &mut TestSuite) {
    test_suite.reset();
    let port = alloc_port();
    test_suite.start_receiver_at(port);
    test_suite.cfg_enabled(true);
    test_suite.cfg_receiver_address(format!("127.0.0.1:{}", port));

    // Workload
    // [req-1, req-2]
    test_suite.setup_workload(vec!["req-1", "req-2"]);

    // | Receiver Alive |
    // |      o      |
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ERROR_DELAY);
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 2);
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));

    // | Receiver Alive |
    // |        x       |
    test_suite.shutdown_receiver();
    test_suite.flush_receiver();
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ERROR_DELAY);
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // | Receiver Alive |
    // |        o       |
    test_suite.start_receiver_at(port);
    test_suite.flush_receiver();
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ERROR_DELAY);
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 2);
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));
}
