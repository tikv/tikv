// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::test_suite::TestSuite;

use std::thread::sleep;

use std::time::Duration;
use test_util::alloc_port;

pub fn case_enable(test_suite: &mut TestSuite) {
    test_suite.reset();
    let port = alloc_port();
    test_suite.start_agent_at(port);
    test_suite.cfg_failpoint_op_duration(50);

    // | Address | Enabled | Requests
    // |   x     |    o    | [req1, req2]
    test_suite.cfg_enabled(true);
    test_suite.submit_requests(vec!["req1", "req2"]);
    sleep(test_suite.get_current_cfg().report_agent_interval.0 * 2);
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // | Address | Enabled | Requests
    // |   o     |    o    | []
    test_suite.cfg_agent_address(format!("127.0.0.1:{}", port));
    sleep(test_suite.get_current_cfg().report_agent_interval.0 * 2);
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // | Address | Enabled | Requests
    // |   o     |    o    | [req1, req2]
    test_suite.submit_requests(vec!["req1", "req2"]);
    sleep(test_suite.get_current_cfg().report_agent_interval.0 * 2);
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 2);
    assert!(res.contains_key("req1"));
    assert!(res.contains_key("req2"));

    // | Address | Enabled | Requests
    // |   x     |    o    | [req1, req2]
    test_suite.cfg_agent_address("");
    test_suite.submit_requests(vec!["req1", "req2"]);
    sleep(test_suite.get_current_cfg().report_agent_interval.0 * 2);
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // | Address | Enabled | Requests
    // |   o     |    x    | [req1, req2]
    test_suite.cfg_enabled(false);
    test_suite.cfg_agent_address(format!("127.0.0.1:{}", port));
    test_suite.submit_requests(vec!["req1", "req2"]);
    sleep(test_suite.get_current_cfg().report_agent_interval.0 * 2);
    assert!(test_suite.fetch_reported_cpu_time().is_empty());
}

pub fn case_report_interval(test_suite: &mut TestSuite) {
    test_suite.reset();
    let port = alloc_port();
    test_suite.start_agent_at(port);
    test_suite.cfg_failpoint_op_duration(50);
    test_suite.cfg_enabled(true);
    test_suite.cfg_agent_address(format!("127.0.0.1:{}", port));

    // | Report Interval
    // |       15s
    test_suite.cfg_report_agent_interval("15s");
    sleep(Duration::from_secs(5));
    test_suite.submit_requests(vec!["req1", "req2"]);
    sleep(Duration::from_secs(5));
    assert!(test_suite.fetch_reported_cpu_time().is_empty());
    sleep(Duration::from_secs(10));
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 2);
    assert!(res.contains_key("req1"));
    assert!(res.contains_key("req2"));

    // | Report Interval
    // |       5s
    test_suite.cfg_report_agent_interval("5s");
    sleep(Duration::from_secs(15));
    test_suite.submit_requests(vec!["req1", "req2"]);
    sleep(Duration::from_secs(5));
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 2);
    assert!(res.contains_key("req1"));
    assert!(res.contains_key("req2"));
}

pub fn case_max_resource_groups(test_suite: &mut TestSuite) {
    test_suite.reset();
    let port = alloc_port();
    test_suite.start_agent_at(port);
    test_suite.cfg_failpoint_op_duration(50);
    test_suite.cfg_enabled(true);
    test_suite.cfg_agent_address(format!("127.0.0.1:{}", port));

    // | Max Resource Groups | Requests
    // |       5000          | [req1 * 2, req2 * 2, req3 * 2, req4, req5]
    test_suite.submit_requests(vec![
        "req1", "req1", "req2", "req2", "req3", "req3", "req4", "req5",
    ]);
    sleep(test_suite.get_current_cfg().report_agent_interval.0 * 2);
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 5);
    assert!(res.contains_key("req1"));
    assert!(res.contains_key("req2"));
    assert!(res.contains_key("req3"));
    assert!(res.contains_key("req4"));
    assert!(res.contains_key("req5"));

    // | Max Resource Groups | Requests
    // |        3            | [req1 * 2, req2 * 2, req3 * 2, req4, req5]
    test_suite.cfg_max_resource_groups(3);
    test_suite.submit_requests(vec![
        "req1", "req1", "req2", "req2", "req3", "req3", "req4", "req5",
    ]);
    sleep(test_suite.get_current_cfg().report_agent_interval.0 * 2);
    let res = test_suite.fetch_reported_cpu_time();
    assert_eq!(res.len(), 4);
    assert!(res.contains_key("req1"));
    assert!(res.contains_key("req2"));
    assert!(res.contains_key("req3"));
    assert!(res.contains_key(""));
}

pub fn case_precision(test_suite: &mut TestSuite) {
    test_suite.reset();
    let port = alloc_port();
    test_suite.start_agent_at(port);
    test_suite.cfg_report_agent_interval("10s");
    test_suite.cfg_failpoint_op_duration(8000);
    test_suite.cfg_enabled(true);
    test_suite.cfg_agent_address(format!("127.0.0.1:{}", port));

    // | Precision
    // |    1s
    test_suite.submit_requests(vec!["req1"]);
    sleep(test_suite.get_current_cfg().report_agent_interval.0 * 2);
    let res = test_suite.fetch_reported_cpu_time();
    let (secs, _) = res.get("req1").unwrap();
    for (l, r) in secs.iter().zip({
        let mut next_secs = secs.iter();
        next_secs.next();
        next_secs
    }) {
        assert_eq!(*l + 1, *r);
    }

    // | Precision
    // |    3s
    test_suite.cfg_precision("3s");
    test_suite.submit_requests(vec!["req1"]);
    sleep(test_suite.get_current_cfg().report_agent_interval.0 * 2);
    let res = test_suite.fetch_reported_cpu_time();
    let (secs, _) = res.get("req1").unwrap();
    for (l, r) in secs.iter().zip({
        let mut next_secs = secs.iter();
        next_secs.next();
        next_secs
    }) {
        assert_eq!(*l + 3, *r);
    }
}
