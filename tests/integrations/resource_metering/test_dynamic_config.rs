// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::test_suite::TestSuite;

use std::thread::sleep;

use test_util::alloc_port;

pub fn case(test_suite: &mut TestSuite) {
    test_suite.reset();
    let port = alloc_port();
    test_suite.start_agent_at(port);

    // | Address | Enabled | Operations
    // |   x     |    o    | [op1, op2]
    test_suite.cfg_enabled(true);
    test_suite.cfg_failpoint_op_duration(300);
    test_suite.execute_ops(vec!["op1", "op2"]);
    sleep(test_suite.get_current_cfg().report_agent_interval.0);
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // | Address | Enabled | Operations
    // |   o     |    o    | []
    test_suite.cfg_agent_address(format!("127.0.0.1:{}", port));
    sleep(test_suite.get_current_cfg().report_agent_interval.0);
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // | Address | Enabled | Operations
    // |   o     |    o    | [op1, op2]
    test_suite.execute_ops(vec!["op1", "op2"]);
    sleep(test_suite.get_current_cfg().report_agent_interval.0);
    let res = test_suite.fetch_reported_cpu_time();
    assert!(!res.is_empty());
    assert_eq!(res.len(), 2);
    res.contains_key("op1");
    res.contains_key("op2");

    // | Address | Enabled | Operations
    // |   x     |    o    | [op1, op2]
    test_suite.cfg_agent_address("");
    test_suite.execute_ops(vec!["op1", "op2"]);
    sleep(test_suite.get_current_cfg().report_agent_interval.0);
    assert!(test_suite.fetch_reported_cpu_time().is_empty());

    // | Address | Enabled | Operations
    // |   o     |    x    | [op1, op2]
    test_suite.cfg_enabled(false);
    test_suite.cfg_agent_address(format!("127.0.0.1:{}", port));
    test_suite.execute_ops(vec!["op1", "op2"]);
    sleep(test_suite.get_current_cfg().report_agent_interval.0);
    assert!(test_suite.fetch_reported_cpu_time().is_empty());
}
