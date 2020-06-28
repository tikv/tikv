// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! A sample Handler for test and micro-benchmark purpose.

use crate::*;
use std::borrow::Cow;
use std::sync::{Arc, Mutex};
use tikv_util::mpsc;

/// Message `Runner` can accepts.
pub enum Message {
    /// `Runner` will do simple calculation for the given times.
    Loop(usize),
    /// `Runner` will call the callback directly.
    Callback(Box<dyn FnOnce(&mut Runner) + Send + 'static>),
}

/// A simple runner used for benchmarking only.
pub struct Runner {
    is_stopped: bool,
    recv: mpsc::Receiver<Message>,
    mailbox: Option<BasicMailbox<Runner>>,
    pub sender: Option<mpsc::Sender<()>>,
    /// Result of the calculation triggered by `Message::Loop`.
    /// Stores it inside `Runner` to avoid accidental optimization.
    res: usize,
}

impl Fsm for Runner {
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

impl Runner {
    pub fn new(cap: usize) -> (mpsc::LooseBoundedSender<Message>, Box<Runner>) {
        let (tx, rx) = mpsc::loose_bounded(cap);
        let fsm = Box::new(Runner {
            is_stopped: false,
            recv: rx,
            mailbox: None,
            sender: None,
            res: 0,
        });
        (tx, fsm)
    }
}

#[derive(Add, PartialEq, Debug, Default, AddAssign, Clone, Copy)]
pub struct HandleMetrics {
    pub begin: usize,
    pub control: usize,
    pub normal: usize,
}

pub struct Handler {
    local: HandleMetrics,
    metrics: Arc<Mutex<HandleMetrics>>,
}

impl Handler {
    fn handle(&mut self, r: &mut Runner) -> Option<usize> {
        for _ in 0..16 {
            match r.recv.try_recv() {
                Ok(Message::Loop(count)) => {
                    // Some calculation to represent a CPU consuming work
                    for _ in 0..count {
                        r.res *= count;
                        r.res %= count + 1;
                    }
                }
                Ok(Message::Callback(cb)) => cb(r),
                Err(_) => break,
            }
        }
        Some(0)
    }
}

impl PollHandler<Runner, Runner> for Handler {
    fn begin(&mut self, _batch_size: usize) {
        self.local.begin += 1;
    }

    fn handle_control(&mut self, control: &mut Runner) -> Option<usize> {
        self.local.control += 1;
        self.handle(control)
    }

    fn handle_normal(&mut self, normal: &mut Runner) -> Option<usize> {
        self.local.normal += 1;
        self.handle(normal)
    }

    fn end(&mut self, _normals: &mut [Box<Runner>]) {
        let mut c = self.metrics.lock().unwrap();
        *c += self.local;
        self.local = HandleMetrics::default();
    }
}

pub struct Builder {
    pub metrics: Arc<Mutex<HandleMetrics>>,
}

impl Builder {
    pub fn new() -> Builder {
        Builder {
            metrics: Arc::default(),
        }
    }
}

impl HandlerBuilder<Runner, Runner> for Builder {
    type Handler = Handler;

    fn build(&mut self) -> Handler {
        Handler {
            local: HandleMetrics::default(),
            metrics: self.metrics.clone(),
        }
    }
}
