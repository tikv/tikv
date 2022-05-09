// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{atomic::*, Arc},
    time::Duration,
};

use batch_system::{test_runner::*, *};
use crossbeam::channel::*;
use tikv_util::mpsc;

fn counter_closure(counter: &Arc<AtomicUsize>) -> Message {
    let c = counter.clone();
    Message::Callback(Box::new(move |_: &Handler, _: &mut Runner| {
        c.fetch_add(1, Ordering::SeqCst);
    }))
}

fn noop() -> Message {
    Message::Callback(Box::new(|_, _| ()))
}

fn unreachable() -> Message {
    Message::Callback(Box::new(|_: &Handler, _: &mut Runner| unreachable!()))
}

#[test]
fn test_basic() {
    let (control_tx, mut control_fsm) = Runner::new(10);
    let (control_drop_tx, control_drop_rx) = mpsc::unbounded();
    control_fsm.sender = Some(control_drop_tx);
    let (router, mut system) =
        batch_system::create_system(&Config::default(), control_tx, control_fsm);
    let builder = Builder::new();
    system.spawn("test".to_owned(), builder);

    // Missing mailbox should report error.
    match router.force_send(1, unreachable()) {
        Err(SendError(_)) => (),
        Ok(_) => panic!("send should fail"),
    }
    match router.send(1, unreachable()) {
        Err(TrySendError::Disconnected(_)) => (),
        Ok(_) => panic!("send should fail"),
        Err(TrySendError::Full(_)) => panic!("expect disconnected."),
    }

    let (tx, rx) = mpsc::unbounded();
    let router_ = router.clone();
    // Control mailbox should be connected.
    router
        .send_control(Message::Callback(Box::new(
            move |_: &Handler, _: &mut Runner| {
                let (sender, mut runner) = Runner::new(10);
                let (tx1, rx1) = mpsc::unbounded();
                runner.sender = Some(tx1);
                let mailbox = BasicMailbox::new(sender, runner, Arc::default());
                router_.register(1, mailbox);
                tx.send(rx1).unwrap();
            },
        )))
        .unwrap();
    let runner_drop_rx = rx.recv_timeout(Duration::from_secs(3)).unwrap();

    // Registered mailbox should be connected.
    router.force_send(1, noop()).unwrap();
    router.send(1, noop()).unwrap();

    // Send should respect capacity limit, while force_send not.
    let (tx, rx) = mpsc::unbounded();
    router
        .send(
            1,
            Message::Callback(Box::new(move |_: &Handler, _: &mut Runner| {
                rx.recv_timeout(Duration::from_secs(100)).unwrap();
            })),
        )
        .unwrap();
    let counter = Arc::default();
    let sent_cnt = (0..)
        .take_while(|_| router.send(1, counter_closure(&counter)).is_ok())
        .count();
    match router.send(1, counter_closure(&counter)) {
        Err(TrySendError::Full(_)) => {}
        Err(TrySendError::Disconnected(_)) => panic!("mailbox should still be connected."),
        Ok(_) => panic!("send should fail"),
    }
    router.force_send(1, counter_closure(&counter)).unwrap();
    tx.send(1).unwrap();
    // Flush.
    let (tx, rx) = mpsc::unbounded();
    router
        .force_send(
            1,
            Message::Callback(Box::new(move |_: &Handler, _: &mut Runner| {
                tx.send(1).unwrap();
            })),
        )
        .unwrap();
    rx.recv_timeout(Duration::from_secs(100)).unwrap();

    let c = counter.load(Ordering::SeqCst);
    assert_eq!(c, sent_cnt + 1);

    // close should release resources.
    assert_eq!(runner_drop_rx.try_recv(), Err(TryRecvError::Empty));
    router.close(1);
    assert_eq!(
        runner_drop_rx.recv_timeout(Duration::from_secs(3)),
        Err(RecvTimeoutError::Disconnected)
    );
    match router.send(1, unreachable()) {
        Err(TrySendError::Disconnected(_)) => (),
        Ok(_) => panic!("send should fail."),
        Err(TrySendError::Full(_)) => panic!("sender should be closed"),
    }
    match router.force_send(1, unreachable()) {
        Err(SendError(_)) => (),
        Ok(_) => panic!("send should fail."),
    }
    assert_eq!(control_drop_rx.try_recv(), Err(TryRecvError::Empty));
    system.shutdown();
    assert_eq!(
        control_drop_rx.recv_timeout(Duration::from_secs(3)),
        Err(RecvTimeoutError::Disconnected)
    );
}

#[test]
fn test_router_trace() {
    let (control_tx, control_fsm) = Runner::new(10);
    let (router, mut system) =
        batch_system::create_system(&Config::default(), control_tx, control_fsm);
    let builder = Builder::new();
    system.spawn("test".to_owned(), builder);

    let register_runner = |addr| {
        let (sender, runner) = Runner::new(10);
        let mailbox = BasicMailbox::new(sender, runner, router.state_cnt().clone());
        router.register(addr, mailbox);
    };
    let close_runner = |addr| {
        router.close(addr);
    };

    let router_clone = router.clone();
    for i in 0..10 {
        register_runner(i);
        // Read mailbox to cache.
        router_clone.mailbox(i).unwrap();
    }
    assert_eq!(router.alive_cnt().load(Ordering::Relaxed), 10);
    assert_eq!(router.state_cnt().load(Ordering::Relaxed), 11);
    // Routers closed but exist in the cache.
    for i in 0..10 {
        close_runner(i);
    }
    assert_eq!(router.alive_cnt().load(Ordering::Relaxed), 0);
    assert_eq!(router.state_cnt().load(Ordering::Relaxed), 11);
    for i in 0..1024 {
        register_runner(i);
        // Read mailbox to cache, closed routers should be evicted.
        router_clone.mailbox(i).unwrap();
    }
    assert_eq!(router.alive_cnt().load(Ordering::Relaxed), 1024);
    assert_eq!(router.state_cnt().load(Ordering::Relaxed), 1025);
}
