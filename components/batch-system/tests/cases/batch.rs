// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use batch_system::test_runner::*;
use batch_system::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use tikv_util::mpsc;

#[test]
fn test_batch() {
    let (control_tx, control_fsm) = Runner::new(10);
    let (router, mut system) =
        batch_system::create_system(&Config::default(), control_tx, control_fsm);
    let builder = Builder::new();
    let metrics = builder.metrics.clone();
    system.spawn("test".to_owned(), builder);
    let mut expected_metrics = HandleMetrics::default();
    assert_eq!(*metrics.lock().unwrap(), expected_metrics);
    let (tx, rx) = mpsc::unbounded();
    let tx_ = tx.clone();
    let r = router.clone();
    router
        .send_control(Message::Callback(Box::new(
            move |_: &Handler, _: &mut Runner| {
                let (tx, runner) = Runner::new(10);
                let mailbox = BasicMailbox::new(tx, runner, Arc::default());
                r.register(1, mailbox);
                tx_.send(1).unwrap();
            },
        )))
        .unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(3)), Ok(1));
    // sleep to wait Batch-System to finish calling end().
    sleep(Duration::from_millis(20));
    router
        .send(
            1,
            Message::Callback(Box::new(move |_: &Handler, _: &mut Runner| {
                tx.send(2).unwrap();
            })),
        )
        .unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(3)), Ok(2));
    system.shutdown();
    expected_metrics.control = 1;
    expected_metrics.normal = 1;
    expected_metrics.begin = 2;
    assert_eq!(*metrics.lock().unwrap(), expected_metrics);
}

#[test]
fn test_priority() {
    let (control_tx, control_fsm) = Runner::new(10);
    let (router, mut system) =
        batch_system::create_system(&Config::default(), control_tx, control_fsm);
    let builder = Builder::new();
    system.spawn("test".to_owned(), builder);
    let (tx, rx) = mpsc::unbounded();
    let tx_ = tx.clone();
    let r = router.clone();
    let state_cnt = Arc::new(AtomicUsize::new(0));
    router
        .send_control(Message::Callback(Box::new(
            move |_: &Handler, _: &mut Runner| {
                let (tx, runner) = Runner::new(10);
                r.register(1, BasicMailbox::new(tx, runner, state_cnt.clone()));
                let (tx2, mut runner2) = Runner::new(10);
                runner2.set_priority(Priority::Low);
                r.register(2, BasicMailbox::new(tx2, runner2, state_cnt));
                tx_.send(1).unwrap();
            },
        )))
        .unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(3)), Ok(1));

    let tx_ = tx.clone();
    router
        .send(
            1,
            Message::Callback(Box::new(move |h: &Handler, r: &mut Runner| {
                assert_eq!(h.get_priority(), Priority::Normal);
                assert_eq!(h.get_priority(), r.get_priority());
                tx_.send(2).unwrap();
            })),
        )
        .unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(3)), Ok(2));

    let tx_ = tx.clone();
    router
        .send(
            2,
            Message::Callback(Box::new(move |h: &Handler, r: &mut Runner| {
                assert_eq!(h.get_priority(), Priority::Low);
                assert_eq!(h.get_priority(), r.get_priority());
                tx_.send(3).unwrap();
            })),
        )
        .unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(3)), Ok(3));
}

#[test]
fn test_before_pause_wait() {
    let (control_tx, control_fsm) = Runner::new(10);
    let mut cfg = Config::default();
    // Use one thread for counting pause easily
    cfg.pool_size = 1;
    cfg.low_priority_pool_size = 0;
    cfg.before_pause_wait = Some(Duration::from_millis(1));
    let (router, mut system) = batch_system::create_system(&cfg, control_tx, control_fsm);
    let builder = Builder::new();
    let metrics = builder.metrics.clone();
    let pause_counter = builder.pause_counter.clone();
    system.spawn("test".to_owned(), builder);
    let mut expected_metrics = HandleMetrics::default();
    assert_eq!(*metrics.lock().unwrap(), expected_metrics);
    // Pause should be increased by one
    sleep(Duration::from_millis(10));
    let (tx, rx) = mpsc::unbounded();
    let tx_ = tx.clone();
    let r = router.clone();
    router
        .send_control(Message::Callback(Box::new(
            move |_: &Handler, _: &mut Runner| {
                let (tx, runner) = Runner::new(10);
                let mailbox = BasicMailbox::new(tx, runner, Arc::default());
                r.register(1, mailbox);
                tx_.send(1).unwrap();
            },
        )))
        .unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(3)), Ok(1));
    // sleep to wait Batch-System to finish calling end().
    // Pause should be increased by one
    sleep(Duration::from_millis(20));
    router
        .send(
            1,
            Message::Callback(Box::new(move |_: &Handler, _: &mut Runner| {
                tx.send(2).unwrap();
            })),
        )
        .unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(3)), Ok(2));
    // Pause should be increased by one
    sleep(Duration::from_millis(10));
    system.shutdown();
    expected_metrics.control = 1;
    expected_metrics.normal = 1;
    expected_metrics.begin = 2;
    assert_eq!(*metrics.lock().unwrap(), expected_metrics);
    assert_eq!(pause_counter.load(Ordering::SeqCst), 3);
}
