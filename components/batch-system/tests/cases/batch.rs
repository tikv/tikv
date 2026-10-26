// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{Arc, atomic::AtomicUsize},
    thread::sleep,
    time::{Duration, Instant},
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

#[cfg(target_os = "linux")]
#[test]
fn test_thread_priority() {
    use tikv_util::sys::thread::get_priority;

    // Raising the nice value doesn't require `CAP_SYS_NICE`.
    const POLLER_PRI: i32 = 10;
    let caller_pri = get_priority().unwrap();
    let (control_tx, control_fsm) = Runner::new(10);
    let (router, mut system) =
        batch_system::create_system(&Config::default(), control_tx, control_fsm, None);
    system.set_thread_priority(POLLER_PRI);
    system.spawn("test".to_owned(), Builder::new());
    // The thread spawning the pollers must keep its own priority.
    assert_eq!(get_priority().unwrap(), caller_pri);

    let (tx, rx) = mpsc::unbounded();
    let tx_ = tx.clone();
    let r = router.clone();
    router
        .send_control(Message::Callback(Box::new(
            move |_: &Handler, _: &mut Runner| {
                let (tx2, mut runner2) = Runner::new(10);
                runner2.set_priority(Priority::Low);
                r.register(1, BasicMailbox::new(tx2, runner2, Arc::default()));
                tx_.send(get_priority().unwrap()).unwrap();
            },
        )))
        .unwrap();
    // Normal priority pollers run with the configured priority.
    assert_eq!(rx.recv_timeout(Duration::from_secs(3)), Ok(POLLER_PRI));

    router
        .send(
            1,
            Message::Callback(Box::new(move |h: &Handler, _: &mut Runner| {
                assert_eq!(h.get_priority(), Priority::Low);
                tx.send(get_priority().unwrap()).unwrap();
            })),
        )
        .unwrap();
    // Low priority pollers are left untouched.
    assert_eq!(rx.recv_timeout(Duration::from_secs(3)), Ok(caller_pri));
    system.shutdown();
}

/// An FSM whose priority changes must move to the matching pool and back, as
/// noisy-region demotion relies on.
#[test]
fn test_priority_change_migrates_fsm_between_pools() {
    // Returns (FSM priority, handling pool). No asserts inside the poller: a
    // panic there kills the thread and hides the real failure.
    fn observe(
        router: &BatchRouter<Runner, Runner>,
        tx: &mpsc::Sender<(Priority, Priority)>,
        rx: &mpsc::Receiver<(Priority, Priority)>,
    ) -> (Priority, Priority) {
        let tx = tx.clone();
        router
            .send(
                1,
                Message::Callback(Box::new(move |h: &Handler, r: &mut Runner| {
                    tx.send((r.get_priority(), h.get_priority())).unwrap();
                })),
            )
            .unwrap();
        rx.recv_timeout(Duration::from_secs(3)).unwrap()
    }

    fn set_priority(
        router: &BatchRouter<Runner, Runner>,
        tx: &mpsc::Sender<(Priority, Priority)>,
        rx: &mpsc::Receiver<(Priority, Priority)>,
        priority: Priority,
    ) {
        let tx = tx.clone();
        router
            .send(
                1,
                Message::Callback(Box::new(move |h: &Handler, r: &mut Runner| {
                    r.set_priority(priority);
                    tx.send((r.get_priority(), h.get_priority())).unwrap();
                })),
            )
            .unwrap();
        rx.recv_timeout(Duration::from_secs(3)).unwrap();
    }

    // Probes until `want` handles the FSM. The move happens at the end of the
    // round, so the old poller may still drain a message sent right after.
    fn wait_for_pool(
        router: &BatchRouter<Runner, Runner>,
        tx: &mpsc::Sender<(Priority, Priority)>,
        rx: &mpsc::Receiver<(Priority, Priority)>,
        want: Priority,
    ) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let (fsm_priority, pool) = observe(router, tx, rx);
            if pool == want {
                assert_eq!(
                    fsm_priority, want,
                    "the FSM and the pool handling it must agree once it has moved"
                );
                return;
            }
            assert!(
                Instant::now() < deadline,
                "FSM never reached the {:?} pool",
                want
            );
        }
    }

    let (control_tx, control_fsm) = Runner::new(10);
    let (router, mut system) =
        batch_system::create_system(&Config::default(), control_tx, control_fsm, None);
    system.spawn("test".to_owned(), Builder::new());
    let (tx, rx) = mpsc::unbounded();

    let state_cnt = Arc::new(AtomicUsize::new(0));
    let (fsm_tx, fsm) = Runner::new(10);
    router.register(1, BasicMailbox::new(fsm_tx, fsm, state_cnt));

    // Starts on the normal pool.
    assert_eq!(
        observe(&router, &tx, &rx),
        (Priority::Normal, Priority::Normal)
    );

    // Demote it.
    set_priority(&router, &tx, &rx, Priority::Low);
    wait_for_pool(&router, &tx, &rx, Priority::Low);

    // Releasing it must send it back.
    set_priority(&router, &tx, &rx, Priority::Normal);
    wait_for_pool(&router, &tx, &rx, Priority::Normal);

    system.shutdown();
}

fn poller_busy_seconds(pool: &str) -> f64 {
    prometheus::gather()
        .iter()
        .filter(|f| f.get_name() == "tikv_batch_system_poller_busy_seconds_total")
        .flat_map(|f| f.get_metric())
        .filter(|m| {
            m.get_label()
                .iter()
                .any(|l| l.get_name() == "priority" && l.get_value() == pool)
                && m.get_label()
                    .iter()
                    .any(|l| l.get_name() == "type" && l.get_value() == "store")
        })
        .map(|m| m.get_counter().get_value())
        .sum()
}

#[test]
fn test_poller_busy_time_counts_time_spent_handling() {
    let (control_tx, control_fsm) = Runner::new(10);
    let (router, mut system) =
        batch_system::create_system(&Config::default(), control_tx, control_fsm, None);
    system.spawn("test".to_owned(), Builder::new());
    let (tx, rx) = mpsc::unbounded();
    let state_cnt = Arc::new(AtomicUsize::new(0));
    let (fsm_tx, fsm) = Runner::new(10);
    router.register(1, BasicMailbox::new(fsm_tx, fsm, state_cnt));

    // Off-CPU time spent holding an FSM counts as busy.
    let before = poller_busy_seconds("normal");
    router
        .send(
            1,
            Message::Callback(Box::new(move |_: &Handler, _: &mut Runner| {
                sleep(Duration::from_millis(100));
                tx.send(()).unwrap();
            })),
        )
        .unwrap();
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
    // Flushed at the end of the round, after the callback acks.
    let deadline = Instant::now() + Duration::from_secs(3);
    while poller_busy_seconds("normal") - before < 0.1 {
        assert!(Instant::now() < deadline, "busy time was never flushed");
        sleep(Duration::from_millis(10));
    }
    system.shutdown();
}
