// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use batch_system::test_runner::*;
use batch_system::*;
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
            move |_: &mut Runner, _: &HandleMetrics| {
                let (tx, runner) = Runner::new(10);
                let mailbox = BasicMailbox::new(tx, runner);
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
            Message::Callback(Box::new(move |_: &mut Runner, _: &HandleMetrics| {
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
fn test_process_count() {
    let (control_tx, control_fsm) = Runner::new(10);
    let mut cfg = Config::default();
    cfg.max_batch_size = 10;
    cfg.pool_size = 1;
    let (router, mut system) = batch_system::create_system(&cfg, control_tx, control_fsm);
    let builder = Builder::new();
    let metrics = builder.metrics.clone();
    system.spawn("test".to_owned(), builder);
    let (tx, rx) = mpsc::unbounded();

    for addr in 1..5 {
        let r = router.clone();
        let tx_ = tx.clone();
        router
            .send_control(Message::Callback(Box::new(
                move |_: &mut Runner, _: &HandleMetrics| {
                    let (tx, runner) = Runner::new(100);
                    let mailbox = BasicMailbox::new(tx, runner);
                    r.register(addr, mailbox);
                    tx_.send(1).unwrap();
                },
            )))
            .unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)), Ok(1));
    }

    // Block batch-system to wait other msg reached.
    let (tx1, rx1) = mpsc::unbounded();
    router
        .send(
            1,
            Message::Callback(Box::new(move |_: &mut Runner, _: &HandleMetrics| {
                let _ = rx1.recv();
            })),
        )
        .unwrap();
    for _ in 0..cfg.max_batch_size {
        router.send(1, Message::Loop(1)).unwrap();
    }
    let (tx2, rx2) = mpsc::unbounded();
    let (tx3, rx3) = mpsc::unbounded();
    router
        .send(
            1,
            Message::Callback(Box::new(move |_, _| {
                tx3.send(()).unwrap();
                let _ = rx2.recv();
            })),
        )
        .unwrap();
    tx1.send(()).unwrap();
    // block test thread until region-1 blocks by second callback again.
    let _ = rx3.recv();

    // The handler has not flush because the batch size does not exceed max_batch_size
    assert_eq!(0, metrics.lock().unwrap().processed_count);

    // The handler has not flush because heartbeat does not make difference
    router.send(2, Message::HeartBeat).unwrap();
    let max_batch_size = cfg.max_batch_size;
    router
        .send(
            2,
            Message::Callback(Box::new(move |_: &mut Runner, m: &HandleMetrics| {
                assert_eq!(max_batch_size, m.processed_count)
            })),
        )
        .unwrap();

    router.send(3, Message::Loop(1)).unwrap();
    let (tx4, rx4) = mpsc::unbounded();
    // The 4th region will notify batch-system and make it aware that the processed count has exceed
    // max_batch_size
    router
        .send(
            4,
            Message::Callback(Box::new(move |_, _| {
                tx4.send(()).unwrap();
            })),
        )
        .unwrap();
    tx2.send(()).unwrap();
    let _ = rx4.recv();
    assert_eq!(
        cfg.max_batch_size + 1,
        metrics.lock().unwrap().processed_count
    );
}
