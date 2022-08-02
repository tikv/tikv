// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! A sample Handler for test and micro-benchmark purpose.

use std::{
    borrow::Cow,
    ops::DerefMut,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use derive_more::{Add, AddAssign};
use tikv_util::mpsc;

use crate::*;

/// Message `Runner` can accepts.
pub enum Message {
    /// `Runner` will do simple calculation for the given times.
    Loop(usize),
    /// `Runner` will call the callback directly.
    Callback(Box<dyn FnOnce(&Handler, &mut Runner) + Send + 'static>),
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
    priority: Priority,
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

    fn get_priority(&self) -> Priority {
        self.priority
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
            priority: Priority::Normal,
        });
        (tx, fsm)
    }

    pub fn set_priority(&mut self, priority: Priority) {
        self.priority = priority
    }
}

#[derive(Add, PartialEq, Debug, Default, AddAssign, Clone, Copy)]
pub struct HandleMetrics {
    pub begin: usize,
    pub control: usize,
    pub normal: usize,
    pub pause: usize,
}

pub struct Handler {
    local: HandleMetrics,
    metrics: Arc<Mutex<HandleMetrics>>,
    priority: Priority,
    pause_counter: Arc<AtomicUsize>,
}

impl Handler {
    fn handle(&mut self, r: &mut Runner) {
        for _ in 0..16 {
            match r.recv.try_recv() {
                Ok(Message::Loop(count)) => {
                    // Some calculation to represent a CPU consuming work
                    for _ in 0..count {
                        r.res *= count;
                        r.res %= count + 1;
                    }
                }
                Ok(Message::Callback(cb)) => cb(self, r),
                Err(_) => break,
            }
        }
    }

    pub fn get_priority(&self) -> Priority {
        self.priority
    }
}

impl PollHandler<Runner, Runner> for Handler {
    fn begin<F>(&mut self, _batch_size: usize, _update_cfg: F)
    where
        for<'a> F: FnOnce(&'a Config),
    {
        self.local.begin += 1;
    }

    fn handle_control(&mut self, control: &mut Runner) -> Option<usize> {
        self.local.control += 1;
        self.handle(control);
        Some(0)
    }

    fn handle_normal(&mut self, normal: &mut impl DerefMut<Target = Runner>) -> HandleResult {
        self.local.normal += 1;
        self.handle(normal);
        HandleResult::stop_at(0, false)
    }

    fn end(&mut self, _normals: &mut [Option<impl DerefMut<Target = Runner>>]) {
        let mut c = self.metrics.lock().unwrap();
        *c += self.local;
        self.local = HandleMetrics::default();
    }

    fn pause(&mut self) {
        self.pause_counter.fetch_add(1, Ordering::SeqCst);
    }
}

pub struct Builder {
    pub metrics: Arc<Mutex<HandleMetrics>>,
    pub pause_counter: Arc<AtomicUsize>,
}

impl Default for Builder {
    fn default() -> Builder {
        Builder {
            metrics: Arc::default(),
            pause_counter: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Builder {
    pub fn new() -> Self {
        Builder::default()
    }
}

impl HandlerBuilder<Runner, Runner> for Builder {
    type Handler = Handler;

    fn build(&mut self, priority: Priority) -> Handler {
        Handler {
            local: HandleMetrics::default(),
            metrics: self.metrics.clone(),
            priority,
            pause_counter: self.pause_counter.clone(),
        }
    }
}
