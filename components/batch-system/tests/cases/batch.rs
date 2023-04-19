// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{atomic::AtomicUsize, Arc},
    thread::sleep,
    time::Duration,
};

use batch_system::{test_runner::*, *};
use kvproto::resource_manager::{GroupMode, GroupRawResourceSettings, ResourceGroup};
use resource_control::ResourceGroupManager;
use tikv_util::mpsc;

#[test]
fn test_batch() {
    let (control_tx, control_fsm) = Runner::new(10);
    let (router, mut system) =
        batch_system::create_system(&Config::default(), control_tx, control_fsm, None);
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
        batch_system::create_system(&Config::default(), control_tx, control_fsm, None);
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

    router
        .send(
            2,
            Message::Callback(Box::new(move |h: &Handler, r: &mut Runner| {
                assert_eq!(h.get_priority(), Priority::Low);
                assert_eq!(h.get_priority(), r.get_priority());
                tx.send(3).unwrap();
            })),
        )
        .unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(3)), Ok(3));
}

#[test]
fn test_resource_group() {
    let (control_tx, control_fsm) = Runner::new(10);
    let resource_manager = ResourceGroupManager::default();

    let get_group = |name: &str, read_tokens: u64, write_tokens: u64| -> ResourceGroup {
        let mut group = ResourceGroup::new();
        group.set_name(name.to_string());
        group.set_mode(GroupMode::RawMode);
        let mut resource_setting = GroupRawResourceSettings::new();
        resource_setting
            .mut_cpu()
            .mut_settings()
            .set_fill_rate(read_tokens);
        resource_setting
            .mut_io_write()
            .mut_settings()
            .set_fill_rate(write_tokens);
        group.set_raw_resource_settings(resource_setting);
        group
    };

    resource_manager.add_resource_group(get_group("group1", 10, 10));
    resource_manager.add_resource_group(get_group("group2", 100, 100));

    let mut cfg = Config::default();
    cfg.pool_size = 1;
    let (router, mut system) = batch_system::create_system(
        &cfg,
        control_tx,
        control_fsm,
        Some(resource_manager.derive_controller("test".to_string(), false)),
    );
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
                let (tx2, runner2) = Runner::new(10);
                r.register(2, BasicMailbox::new(tx2, runner2, state_cnt));
                tx_.send(0).unwrap();
            },
        )))
        .unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(3)), Ok(0));

    let tx_ = tx.clone();
    let (tx1, rx1) = std::sync::mpsc::sync_channel(0);
    // block the thread
    router
        .send_control(Message::Callback(Box::new(
            move |_: &Handler, _: &mut Runner| {
                tx_.send(0).unwrap();
                tx1.send(0).unwrap();
            },
        )))
        .unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(3)), Ok(0));

    router
        .send(1, Message::Resource("group1".to_string(), 1))
        .unwrap();
    let tx_ = tx.clone();
    router
        .send(
            1,
            Message::Callback(Box::new(move |_: &Handler, _: &mut Runner| {
                tx_.send(1).unwrap();
            })),
        )
        .unwrap();

    router
        .send(2, Message::Resource("group2".to_string(), 1))
        .unwrap();
    router
        .send(
            2,
            Message::Callback(Box::new(move |_: &Handler, _: &mut Runner| {
                tx.send(2).unwrap();
            })),
        )
        .unwrap();

    // pause the blocking thread
    assert_eq!(rx1.recv_timeout(Duration::from_secs(3)), Ok(0));

    // should recv from group2 first, because group2 has more tokens and it would be
    // handled with higher priority.
    assert_eq!(rx.recv_timeout(Duration::from_secs(3)), Ok(2));
    assert_eq!(rx.recv_timeout(Duration::from_secs(3)), Ok(1));
}
