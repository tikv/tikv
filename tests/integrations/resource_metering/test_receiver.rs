// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::test_suite::TestSuite;

use std::iter;
use std::thread::sleep;
use std::time::Duration;

use rand::seq::SliceRandom;
use test_util::alloc_port;

const ONE_SEC: Duration = Duration::from_secs(1);

pub fn case_alter_receiver_addr(test_suite: &mut TestSuite) {
    test_suite.reset();
    let port = alloc_port();
    test_suite.start_receiver_at(port);
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

    // | Address | Enabled |
    // |   x     |    o    |
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ONE_SEC);
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // | Address | Enabled |
    // |   o     |    o    |
    test_suite.cfg_receiver_address(format!("127.0.0.1:{}", port));
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ONE_SEC);
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 6);
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));
    assert!(res.contains_key("req-3"));
    assert!(res.contains_key("req-4"));
    assert!(res.contains_key("req-5"));
    assert!(res.contains_key(""));

    // | Address | Enabled |
    // |   !     |    o    |
    test_suite.cfg_receiver_address(format!("127.0.0.1:{}", port + 1));
    test_suite.flush_receiver();
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ONE_SEC);
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // | Address | Enabled |
    // |   o     |    o    |
    test_suite.cfg_receiver_address(format!("127.0.0.1:{}", port));
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ONE_SEC);
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 6);
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));
    assert!(res.contains_key("req-3"));
    assert!(res.contains_key("req-4"));
    assert!(res.contains_key("req-5"));
    assert!(res.contains_key(""));
}

pub fn case_receiver_blocking(test_suite: &mut TestSuite) {
    test_suite.reset();
    let port = alloc_port();
    test_suite.start_receiver_at(port);
    test_suite.cfg_enabled(true);
    test_suite.cfg_max_resource_groups(5);
    test_suite.cfg_receiver_address(format!("127.0.0.1:{}", port));

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

    // | Block Receiver |
    // |      x      |
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ONE_SEC);
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 6);
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));
    assert!(res.contains_key("req-3"));
    assert!(res.contains_key("req-4"));
    assert!(res.contains_key("req-5"));
    assert!(res.contains_key(""));

    // | Block Receiver |
    // |      o      |
    fail::cfg("mock-receiver", "sleep(5000)").unwrap();
    test_suite.flush_receiver();
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ONE_SEC);
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // Workload
    // [req-{1..5} * 1, req-{6..10} * 3]
    test_suite.cancel_workload();
    let mut wl = (1..=10)
        .chain(6..=10)
        .chain(6..=10)
        .map(|n| format!("req-{}", n))
        .collect::<Vec<_>>();
    wl.shuffle(&mut rand::thread_rng());
    test_suite.setup_workload(wl);

    // | Block Receiver |
    // |      x      |
    fail::remove("mock-receiver");
    test_suite.flush_receiver();
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ONE_SEC);
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 6);
    assert!(res.contains_key("req-6"));
    assert!(res.contains_key("req-7"));
    assert!(res.contains_key("req-8"));
    assert!(res.contains_key("req-9"));
    assert!(res.contains_key("req-10"));
    assert!(res.contains_key(""));
}

pub fn case_receiver_shutdown(test_suite: &mut TestSuite) {
    test_suite.reset();
    let port = alloc_port();
    test_suite.start_receiver_at(port);
    test_suite.cfg_enabled(true);
    test_suite.cfg_max_resource_groups(5);
    test_suite.cfg_receiver_address(format!("127.0.0.1:{}", port));

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

    // | Receiver Alive |
    // |      o      |
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ONE_SEC);
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 6);
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));
    assert!(res.contains_key("req-3"));
    assert!(res.contains_key("req-4"));
    assert!(res.contains_key("req-5"));
    assert!(res.contains_key(""));

    // | Receiver Alive |
    // |      x      |
    test_suite.shutdown_receiver();
    test_suite.flush_receiver();
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ONE_SEC);
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // Workload
    // [req-{1..5} * 1, req-{6..10} * 10]
    test_suite.cancel_workload();
    let mut wl = iter::repeat(6..=10)
        .take(10)
        .flatten()
        .chain(1..=5)
        .map(|n| format!("req-{}", n))
        .collect::<Vec<_>>();
    wl.shuffle(&mut rand::thread_rng());
    test_suite.setup_workload(wl);

    // | Receiver Alive |
    // |      o      |
    test_suite.start_receiver_at(port);
    test_suite.flush_receiver();
    sleep(test_suite.get_current_cfg().report_receiver_interval.0 + ONE_SEC);
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 6);
    assert!(res.contains_key("req-6"));
    assert!(res.contains_key("req-7"));
    assert!(res.contains_key("req-8"));
    assert!(res.contains_key("req-9"));
    assert!(res.contains_key("req-10"));
    assert!(res.contains_key(""));
}
