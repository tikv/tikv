// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::test_suite::TestSuite;

use std::iter;
use std::time::Duration;

use rand::prelude::SliceRandom;
use tikv_util::config::ReadableDuration;
use tokio::time::Instant;

const DEFAULT_DELAY: Duration = Duration::from_secs(10);

#[test]
pub fn test_report_interval() {
    let mut test_suite = TestSuite::new(resource_metering::Config {
        report_receiver_interval: ReadableDuration::secs(3),
        precision: ReadableDuration::secs(1),
        ..Default::default()
    });

    // Workload
    // [req-1, req-2]
    test_suite.setup_workload(vec!["req-1", "req-2"]);

    // | Report Interval |
    // |       3s        |
    let mut subscriber = test_suite.subscribe();
    let res = subscriber.next_batch_tags(DEFAULT_DELAY);
    assert!(res.contains("req-1"));
    assert!(res.contains("req-2"));
    let now = Instant::now();

    let res = subscriber.next_batch_tags(DEFAULT_DELAY);
    assert!(res.contains("req-1"));
    assert!(res.contains("req-2"));
    let duration = now.elapsed();

    assert!(Duration::from_millis(2500) < duration && duration < Duration::from_millis(3500));

    // | Report Interval |
    // |       1s        |
    test_suite.cfg_report_receiver_interval("1s");
    let mut retry = 0;

    loop {
        if retry > 3 {
            panic!("retried too many times");
        }

        let res = subscriber.next_batch_tags(DEFAULT_DELAY);
        assert!(res.contains("req-1"));
        assert!(res.contains("req-2"));
        let now = Instant::now();

        let res = subscriber.next_batch_tags(DEFAULT_DELAY);
        assert!(res.contains("req-1"));
        assert!(res.contains("req-2"));
        let duration = now.elapsed();

        if Duration::from_millis(800) < duration && duration < Duration::from_millis(1200) {
            break;
        }

        retry += 1;
    }
}

#[test]
pub fn test_max_resource_groups() {
    let mut test_suite = TestSuite::new(resource_metering::Config {
        report_receiver_interval: ReadableDuration::secs(4),
        precision: ReadableDuration::secs(2),
        ..Default::default()
    });

    // Workload
    // [req-{1..3} * 6, req-{4..5} * 1]
    let mut wl = iter::repeat(1..=3)
        .take(6)
        .flatten()
        .chain(4..=5)
        .map(|n| format!("req-{}", n))
        .collect::<Vec<_>>();
    wl.shuffle(&mut rand::thread_rng());
    test_suite.setup_workload(wl);

    let mut subscriber = test_suite.subscribe();

    // | Max Resource Groups |
    // |      default        |
    let res = subscriber.next_batch_tags(DEFAULT_DELAY);
    assert!(res.contains("req-1"));
    assert!(res.contains("req-2"));
    assert!(res.contains("req-3"));
    assert!(res.contains("req-4"));
    assert!(res.contains("req-5"));

    // | Max Resource Groups |
    // |          3          |
    test_suite.cfg_max_resource_groups(3);

    let mut retry = 0;

    loop {
        if retry > 3 {
            panic!("retried too many times");
        }

        let res = subscriber.next_batch_tags(DEFAULT_DELAY);
        if res.len() == 4 {
            assert!(res.contains("req-1"));
            assert!(res.contains("req-2"));
            assert!(res.contains("req-3"));
            assert!(res.contains(""));
            break;
        }

        retry += 1;
    }
}

#[test]
pub fn test_precision() {
    let mut test_suite = TestSuite::new(resource_metering::Config {
        report_receiver_interval: ReadableDuration::secs(3),
        max_resource_groups: 5000,
        ..Default::default()
    });

    // Workload
    // [req-1]
    test_suite.setup_workload(vec!["req-1"]);

    let mut subscriber = test_suite.subscribe();

    // | Precision |
    // |    1s     |
    let res = subscriber.next_batch_records(DEFAULT_DELAY);

    let secs = res
        .get("req-1")
        .unwrap()
        .iter()
        .map(|r| r.get_timestamp_sec())
        .collect::<Vec<_>>();
    for (l, r) in secs.iter().zip({
        let mut next_secs = secs.iter();
        next_secs.next();
        next_secs
    }) {
        let diff = r - l;
        assert!(diff <= 2);
    }

    // | Precision |
    // |    4s     |
    test_suite.cfg_precision("4s");
    test_suite.cfg_report_receiver_interval("9s");

    let mut retry = 0;

    loop {
        if retry > 3 {
            panic!("retried too many times");
        }

        let res = subscriber.next_batch_records(Duration::from_secs(50));

        let secs = res
            .get("req-1")
            .unwrap()
            .iter()
            .map(|r| r.get_timestamp_sec())
            .collect::<Vec<_>>();
        let mut passed = true;
        for (l, r) in secs.iter().zip({
            let mut next_secs = secs.iter();
            next_secs.next();
            next_secs
        }) {
            let diff = r - l;

            if !(3 <= diff && diff <= 5) {
                passed = false;
                break;
            }
        }

        if passed {
            break;
        }

        retry += 1;
    }
}
