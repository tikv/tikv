// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::test_suite::TestSuite;

use std::thread::sleep;

use test_util::alloc_port;

pub fn case_alter_agent_addr(test_suite: &mut TestSuite) {
    test_suite.reset();
    let port = alloc_port();
    test_suite.cfg_enabled(true);
    test_suite.cfg_max_resource_groups(5);
    test_suite.start_agent_at(port);
    test_suite.cfg_failpoint_op_duration(50);

    // | Address | Enabled | Requests
    // |   x     |    o    | [req{1..5} * 2, req{6..10} * 1]
    test_suite.submit_requests(
        (1..=5)
            .chain(1..=5)
            .chain(6..=10)
            .map(|n| format!("req-{}", n))
            .collect(),
    );
    sleep(test_suite.get_current_cfg().report_agent_interval.0 * 2);
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // | Address | Enabled | Requests
    // |   o     |    o    | [req{1..5} * 2, req{6..10} * 1]
    test_suite.cfg_agent_address(format!("127.0.0.1:{}", port));
    test_suite.submit_requests(
        (1..=5)
            .chain(1..=5)
            .chain(6..=10)
            .map(|n| format!("req-{}", n))
            .collect(),
    );
    sleep(test_suite.get_current_cfg().report_agent_interval.0 * 2);
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 6);
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));
    assert!(res.contains_key("req-3"));
    assert!(res.contains_key("req-4"));
    assert!(res.contains_key("req-5"));
    assert!(res.contains_key(""));

    // | Address | Enabled | Requests
    // |   !     |    o    | [req{1..5} * 2, req{6..10} * 1]
    test_suite.cfg_agent_address(format!("127.0.0.1:{}", port + 1));
    test_suite.submit_requests(
        (1..=5)
            .chain(1..=5)
            .chain(6..=10)
            .map(|n| format!("req-{}", n))
            .collect(),
    );
    sleep(test_suite.get_current_cfg().report_agent_interval.0 * 2);
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // | Address | Enabled | Requests
    // |   o     |    o    | [req{1..5} * 1, req{6..10} * 2]
    test_suite.cfg_agent_address(format!("127.0.0.1:{}", port));
    test_suite.submit_requests(
        (1..=5)
            .chain(6..=10)
            .chain(6..=10)
            .map(|n| format!("req-{}", n))
            .collect(),
    );
    sleep(test_suite.get_current_cfg().report_agent_interval.0 * 2);
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 6);
    assert!(res.contains_key("req-6"));
    assert!(res.contains_key("req-7"));
    assert!(res.contains_key("req-8"));
    assert!(res.contains_key("req-9"));
    assert!(res.contains_key("req-10"));
    assert!(res.contains_key(""));
}
