// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::Cow,
    ops::DerefMut,
    sync::{Arc, atomic::AtomicUsize, mpsc as std_mpsc},
    thread::sleep,
    time::Duration,
};

use batch_system::{test_runner::*, *};
use kvproto::resource_manager::{GroupMode, GroupRawResourceSettings, ResourceGroup};
use resource_control::{ResourceController, ResourceGroupManager, ResourceMetered};
use tikv_util::mpsc;

enum SelfSendMsg {
    ControlFirst { release: std_mpsc::Receiver<()> },
    ControlSecond,
    Normal,
}

impl ResourceMetered for SelfSendMsg {
    fn consume_resource(&self, _: &Arc<ResourceController>) -> Option<String> {
        None
    }
}

struct SelfSendFsm {
    is_stopped: bool,
    recv: mpsc::Receiver<SelfSendMsg>,
    mailbox: Option<BasicMailbox<SelfSendFsm>>,
}

impl SelfSendFsm {
    fn new(cap: usize) -> (mpsc::LooseBoundedSender<SelfSendMsg>, Box<SelfSendFsm>) {
        let (tx, rx) = mpsc::loose_bounded(cap);
        let fsm = Box::new(SelfSendFsm {
            is_stopped: false,
            recv: rx,
            mailbox: None,
        });
        (tx, fsm)
    }
}

impl Fsm for SelfSendFsm {
    type Message = SelfSendMsg;

    const FSM_TYPE: FsmType = FsmType::store;

    fn is_stopped(&self) -> bool {
        self.is_stopped
    }

    fn set_mailbox(&mut self, mailbox: Cow<'_, BasicMailbox<Self>>) {
        self.mailbox = Some(mailbox.into_owned());
    }

    fn take_mailbox(&mut self) -> Option<BasicMailbox<Self>> {
        self.mailbox.take()
    }
}

struct SelfSendHandler {
    router: BatchRouter<SelfSendFsm, SelfSendFsm>,
    events: std_mpsc::Sender<&'static str>,
}

impl PollHandler<SelfSendFsm, SelfSendFsm> for SelfSendHandler {
    fn begin<F>(&mut self, _: usize, _: F)
    where
        for<'a> F: FnOnce(&'a Config),
    {
    }

    fn handle_control(&mut self, control: &mut SelfSendFsm) -> Option<usize> {
        match control.recv.try_recv() {
            Ok(SelfSendMsg::ControlFirst { release }) => {
                if self
                    .router
                    .force_send_control(SelfSendMsg::ControlSecond)
                    .is_err()
                {
                    panic!("failed to send the next control message");
                }
                self.events.send("control-1").unwrap();
                release.recv_timeout(Duration::from_secs(3)).unwrap();
            }
            Ok(SelfSendMsg::ControlSecond) => {
                self.events.send("control-2").unwrap();
            }
            Ok(SelfSendMsg::Normal) => unreachable!("normal message in control FSM"),
            Err(_) => {}
        }
        Some(0)
    }

    fn handle_normal(&mut self, normal: &mut impl DerefMut<Target = SelfSendFsm>) -> HandleResult {
        match normal.recv.try_recv() {
            Ok(SelfSendMsg::Normal) => {
                self.events.send("normal").unwrap();
            }
            Ok(_) => unreachable!("control message in normal FSM"),
            Err(_) => {}
        }
        HandleResult::stop_at(0, false)
    }

    fn end(&mut self, _: &mut [Option<impl DerefMut<Target = SelfSendFsm>>]) {}
}

struct SelfSendBuilder {
    router: BatchRouter<SelfSendFsm, SelfSendFsm>,
    events: std_mpsc::Sender<&'static str>,
}

impl HandlerBuilder<SelfSendFsm, SelfSendFsm> for SelfSendBuilder {
    type Handler = SelfSendHandler;

    fn build(&mut self, _: Priority) -> SelfSendHandler {
        SelfSendHandler {
            router: self.router.clone(),
            events: self.events.clone(),
        }
    }
}

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
fn test_control_self_send_stays_in_mailbox_and_yields_to_normal() {
    let mut cfg = Config::default();
    cfg.pool_size = 1;
    cfg.low_priority_pool_size = 0;
    cfg.max_batch_size = Some(1);

    let (control_tx, control_fsm) = SelfSendFsm::new(10);
    let (router, mut system) = batch_system::create_system(&cfg, control_tx, control_fsm, None);
    let (normal_tx, normal_fsm) = SelfSendFsm::new(10);
    router.register(
        1,
        BasicMailbox::new(normal_tx, normal_fsm, router.state_cnt().clone()),
    );

    let (event_tx, event_rx) = std_mpsc::channel();
    system.spawn(
        "test-control-self-send".to_owned(),
        SelfSendBuilder {
            router: router.clone(),
            events: event_tx,
        },
    );

    let (release_tx, release_rx) = std_mpsc::sync_channel(0);
    if router
        .force_send_control(SelfSendMsg::ControlFirst {
            release: release_rx,
        })
        .is_err()
    {
        panic!("failed to send the first control message");
    }
    assert_eq!(
        event_rx.recv_timeout(Duration::from_secs(3)),
        Ok("control-1")
    );

    if router.force_send(1, SelfSendMsg::Normal).is_err() {
        panic!("failed to send the normal message");
    }
    release_tx.send(()).unwrap();

    assert_eq!(event_rx.recv_timeout(Duration::from_secs(3)), Ok("normal"));
    assert_eq!(
        event_rx.recv_timeout(Duration::from_secs(3)),
        Ok("control-2")
    );

    system.shutdown();
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
