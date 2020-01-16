// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(test)]

extern crate test;

use batch_system::*;
use std::borrow::Cow;
use test::*;
use std::sync::Arc;
use std::sync::atomic::*;
use tikv_util::mpsc;

enum Message {
    Loop(usize),
    Callback(Box<dyn FnOnce() + Send + 'static>),
}

struct DryRun {
    is_stopped: bool,
    recv: mpsc::Receiver<Message>,
    mailbox: Option<BasicMailbox<DryRun>>,
    res: usize,
}

impl Fsm for DryRun {
    type Message = Message;

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

impl DryRun {
    fn new(cap: usize) -> (mpsc::LooseBoundedSender<Message>, Box<DryRun>) {
        let (tx, rx) = mpsc::loose_bounded(cap);
        let run = Box::new(DryRun {
            is_stopped: false,
            recv: rx,
            mailbox: None,
            res: 0,
        });
        (tx, run)
    }
}

struct Handler;

impl PollHandler<DryRun, DryRun> for Handler {
    fn begin(&mut self, _batch_size: usize) {}

    fn handle_control(&mut self, control: &mut DryRun) -> Option<usize> {
        let mut batch_countdown = 16;
        while let Ok(m) = control.recv.try_recv() {
            match m {
                Message::Loop(count) => {
                    for _ in 0..count {
                        control.res *= count;
                        control.res %= count + 1;
                    }
                }
                Message::Callback(cb) => cb(),
            }
            batch_countdown -= 1;
            if batch_countdown == 0 {
                break;
            }
        }
        Some(0)
    }

    fn handle_normal(&mut self, normal: &mut DryRun) -> Option<usize> {
        self.handle_control(normal)
    }

    fn end(&mut self, _normals: &mut [Box<DryRun>]) {}
}

pub struct Builder;

impl HandlerBuilder<DryRun, DryRun> for Builder {
    type Handler = Handler;

    fn build(&mut self) -> Handler {
        Handler
    }
}

#[bench]
fn bench_spawn_many(b: &mut Bencher) {
    let (control_tx, control_fsm) = DryRun::new(100000);
    let (router, mut system) = batch_system::create_system(2, 2, control_tx, control_fsm);
    system.spawn("test".to_owned(), Builder);
    for id in 0..4096 {
        let (normal_tx, normal_fsm) = DryRun::new(100000);
        let normal_box = BasicMailbox::new(normal_tx, normal_fsm);
        router.register(id, normal_box);
    }

    let (tx, rx) = std::sync::mpsc::channel();

    b.iter(|| {
        for id in 0..4096 {
            for i in 0..64 {
                router.send(id, Message::Loop(i)).unwrap();
            }
            let tx = tx.clone();
            router
                .send(
                    id,
                    Message::Callback(Box::new(move || {
                        tx.send(()).unwrap();
                    })),
                )
                .unwrap();
        }
        for _ in 0..4096 {
            rx.recv().unwrap();
        }
    });
    system.shutdown();
}

#[bench]
fn bench_imbalance(b: &mut Bencher) {
    let (control_tx, control_fsm) = DryRun::new(100000);
    let (router, mut system) = batch_system::create_system(2, 2, control_tx, control_fsm);
    system.spawn("test".to_owned(), Builder);
    for id in 0..10 {
        let (normal_tx, normal_fsm) = DryRun::new(100000);
        let normal_box = BasicMailbox::new(normal_tx, normal_fsm);
        router.register(id, normal_box);
    }

    let (tx, rx) = std::sync::mpsc::channel();

    b.iter(|| {
        for _ in 0..1024 {
            for id in 0..2 {
                router.send(id, Message::Loop(1024)).unwrap();
            }
        }
        for id in 0..2 {
            let tx = tx.clone();
            router
                .send(
                    id,
                    Message::Callback(Box::new(move || {
                        tx.send(()).unwrap();
                    })),
                )
                .unwrap();
        }
        for _ in 0..2 {
            rx.recv().unwrap();
        }
    });
    system.shutdown();
}

#[bench]
fn bench_fairness(b: &mut Bencher) {
    let (control_tx, control_fsm) = DryRun::new(100000);
    let (router, mut system) = batch_system::create_system(2, 2, control_tx, control_fsm);
    system.spawn("test".to_owned(), Builder);
    for id in 0..10 {
        let (normal_tx, normal_fsm) = DryRun::new(100000);
        let normal_box = BasicMailbox::new(normal_tx, normal_fsm);
        router.register(id, normal_box);
    }

    let (tx, _rx) = std::sync::mpsc::channel();
    let running = Arc::new(AtomicBool::new(true));
    let router1 = router.clone();
    let running1 = running.clone();
    let handle = std::thread::spawn(move || {
        while running1.load(Ordering::SeqCst) {
            for id in 0..4 {
                let _ = router1.send(id, Message::Loop(16));
            }
        }
        tx.send(()).unwrap();
    });

    let (tx2, rx2) = std::sync::mpsc::channel();
    b.iter(|| {
        for _ in 0..10 {
            for id in 4..6 {
                router.send(id, Message::Loop(10)).unwrap();
            }
        }
        for id in 4..6 {
            let tx = tx2.clone();
            router
                .send(
                    id,
                    Message::Callback(Box::new(move || {
                        tx.send(()).unwrap();
                    })),
                )
                .unwrap();
        }
        for _ in 4..6 {
            rx2.recv().unwrap();
        }
    });
    running.store(false, Ordering::SeqCst);
    system.shutdown();
    let _ = handle.join();
}