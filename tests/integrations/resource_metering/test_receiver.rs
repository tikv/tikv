// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{thread::sleep, time::Duration};

use test_util::alloc_port;
use tikv_util::config::ReadableDuration;

use super::test_suite::TestSuite;

#[test]
pub fn test_alter_receiver_address() {
    let port = alloc_port();
    let mut test_suite = TestSuite::new(resource_metering::Config {
        receiver_address: format!("127.0.0.1:{}", port),
        report_receiver_interval: ReadableDuration::secs(3),
        max_resource_groups: 5000,
        precision: ReadableDuration::secs(1),
        enable_network_io_collection: false,
    });
    test_suite.start_receiver_at(port);

    // Workload
    // [req-1, req-2]
    test_suite.setup_workload(vec!["req-1", "req-2"]);

    // | Address |
    // |   o     |
    let res = test_suite.block_receive_one();
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));

    // | Address |
    // |   !     |
    test_suite.cfg_receiver_address(format!("127.0.0.1:{}", port + 1));
    test_suite.flush_receiver();
    sleep(Duration::from_millis(3500));
    assert!(test_suite.nonblock_receiver_all().is_empty());

    // | Address |
    // |   o     |
    test_suite.cfg_receiver_address(format!("127.0.0.1:{}", port));
    let res = test_suite.block_receive_one();
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));
}

#[test]
pub fn test_receiver_blocking() {
    let port = alloc_port();
    let mut test_suite = TestSuite::new(resource_metering::Config {
        receiver_address: format!("127.0.0.1:{}", port),
        report_receiver_interval: ReadableDuration::secs(3),
        max_resource_groups: 5000,
        precision: ReadableDuration::secs(1),
        enable_network_io_collection: false,
    });
    test_suite.start_receiver_at(port);

    // Workload
    // [req-1, req-2]
    test_suite.setup_workload(vec!["req-1", "req-2"]);

    // | Block Receiver |
    // |       x        |
    let res = test_suite.block_receive_one();
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));

    // | Block Receiver |
    // |       o        |
    test_suite.block_receiver();
    test_suite.flush_receiver();
    sleep(Duration::from_millis(3500));
    assert!(test_suite.nonblock_receiver_all().is_empty());

    // | Block Receiver |
    // |       x        |
    test_suite.unblock_receiver();
    sleep(Duration::from_millis(3500));
    test_suite.flush_receiver();
    let res = test_suite.block_receive_one();
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));
}

#[test]
pub fn test_receiver_shutdown() {
    let port = alloc_port();
    let mut test_suite = TestSuite::new(resource_metering::Config {
        receiver_address: format!("127.0.0.1:{}", port),
        report_receiver_interval: ReadableDuration::secs(3),
        max_resource_groups: 5000,
        precision: ReadableDuration::secs(1),
        enable_network_io_collection: false,
    });
    test_suite.start_receiver_at(port);

    // Workload
    // [req-1, req-2]
    test_suite.setup_workload(vec!["req-1", "req-2"]);

    // | Receiver Alive |
    // |       o        |
    let res = test_suite.block_receive_one();
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
    sleep(Duration::from_millis(3500));
    assert!(test_suite.nonblock_receiver_all().is_empty());

    // | Receiver Alive |
    // |       o        |
    test_suite.start_receiver_at(port);
    let res = test_suite.block_receive_one();
    assert!(res.contains_key("req-3"));
    assert!(res.contains_key("req-4"));
}
