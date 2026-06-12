// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{iter, thread::sleep, time::Duration};

use rand::prelude::SliceRandom;
use resource_metering::ENABLE_NETWORK_IO_COLLECTION;
use test_util::alloc_port;
use tikv_util::config::ReadableDuration;
use tokio::time::Instant;

use super::test_suite::TestSuite;

#[test]
pub fn test_enable() {
    let mut test_suite = TestSuite::new(resource_metering::Config {
        receiver_address: "".to_string(),
        report_receiver_interval: ReadableDuration::millis(2500),
        max_resource_groups: 5000,
        precision: ReadableDuration::secs(1),
        enable_network_io_collection: false,
    });

    let port = alloc_port();
    test_suite.start_receiver_at(port);

    // Workload
    // [req-1, req-2]
    test_suite.setup_workload(vec!["req-1", "req-2"]);

    // | Address |
    // |   x     |
    sleep(Duration::from_millis(3000));
    assert!(test_suite.nonblock_receiver_all().is_empty());

    // | Address |
    // |   o     |
    test_suite.cfg_receiver_address(format!("127.0.0.1:{}", port));
    let res = test_suite.block_receive_one();
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));

    // | Address |
    // |   x     |
    test_suite.cfg_receiver_address("");
    test_suite.flush_receiver();
    sleep(Duration::from_millis(3000));
    assert!(test_suite.nonblock_receiver_all().is_empty());

    // | Address |
    // |   o     |
    test_suite.cfg_receiver_address(format!("127.0.0.1:{}", port));
    let res = test_suite.block_receive_one();
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));
}

#[test]
pub fn test_report_interval() {
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

    // | Report Interval |
    // |       3s        |
    let res = test_suite.block_receive_one();
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));

    // | Report Interval |
    // |       1s        |
    test_suite.cfg_report_receiver_interval("1s");

    const RETRY_TIMES: usize = 3;
    let (_, mut first_recv_time) = (test_suite.block_receive_one(), Instant::now());
    for _ in 0..RETRY_TIMES {
        let (_, second_recv_time) = (test_suite.block_receive_one(), Instant::now());
        let duration = second_recv_time - first_recv_time;

        if Duration::from_millis(800) < duration && duration < Duration::from_millis(1200) {
            // test passed
            return;
        }
        first_recv_time = second_recv_time;
    }
    panic!("failed {} times", RETRY_TIMES)
}

#[test]
pub fn test_max_resource_groups() {
    let port = alloc_port();
    let mut test_suite = TestSuite::new(resource_metering::Config {
        receiver_address: format!("127.0.0.1:{}", port),
        report_receiver_interval: ReadableDuration::secs(4),
        max_resource_groups: 5000,
        precision: ReadableDuration::secs(2),
        enable_network_io_collection: false,
    });
    test_suite.start_receiver_at(port);

    // Workload
    // [req-{1..3} * 6, req-{4..5} * 1]
    let mut wl = iter::repeat_n(1..=3, 6)
        .flatten()
        .chain(4..=5)
        .map(|n| format!("req-{}", n))
        .collect::<Vec<_>>();
    wl.shuffle(&mut rand::thread_rng());
    test_suite.setup_workload(wl);

    // | Max Resource Groups |
    // |       5000          |
    let res = test_suite.block_receive_one();
    assert!(res.contains_key("req-1"));
    assert!(res.contains_key("req-2"));
    assert!(res.contains_key("req-3"));
    assert!(res.contains_key("req-4"));
    assert!(res.contains_key("req-5"));

    // | Max Resource Groups |
    // |        3            |
    test_suite.cfg_max_resource_groups(3);
    test_suite.flush_receiver();
    let res = test_suite.block_receive_one();
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
        receiver_address: format!("127.0.0.1:{}", port),
        report_receiver_interval: ReadableDuration::secs(3),
        max_resource_groups: 5000,
        precision: ReadableDuration::secs(1),
        enable_network_io_collection: false,
    });
    test_suite.start_receiver_at(port);

    // Workload
    // [req-1]
    test_suite.setup_workload(vec!["req-1"]);

    // | Precision |
    // |    1s     |
    let res = test_suite.block_receive_one();
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
    test_suite.cfg_report_receiver_interval("9s");
    test_suite.flush_receiver();
    let res = test_suite.block_receive_one();
    let (secs, _) = res.get("req-1").unwrap();
    for (l, r) in secs.iter().zip({
        let mut next_secs = secs.iter();
        next_secs.next();
        next_secs
    }) {
        let diff = r - l;
        assert!((2..=4).contains(&diff));
    }
}

#[test]
pub fn test_enable_network_io_collection() {
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
    // [req-1]
    test_suite.setup_workload(vec!["req-1"]);

    test_suite.cfg_enable_network_io_collection("true");
    test_suite.flush_receiver();
    let _res = test_suite.block_receive_one();
    assert!(ENABLE_NETWORK_IO_COLLECTION.load(std::sync::atomic::Ordering::Relaxed));
}
