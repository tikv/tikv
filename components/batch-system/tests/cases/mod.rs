// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
extern crate derive_more;

mod router;
mod batch;

use batch_system::*;
use std::sync::{Arc, Mutex};
use tikv_util::mpsc;

pub type Message = Option<Box<dyn FnOnce(&mut Runner) + Send>>;

pub struct Runner {
    recv: mpsc::Receiver<Message>,
    pub sender: Option<mpsc::Sender<()>>,
}

impl Fsm for Runner {
    type Message = Message;
}

pub fn new_runner(cap: usize) -> (mpsc::LooseBoundedSender<Message>, Box<Runner>) {
    let (tx, rx) = mpsc::loose_bounded(cap);
    let fsm = Runner {
        recv: rx,
        sender: None,
    };
    (tx, Box::new(fsm))
}

#[derive(Add, PartialEq, Debug, Default, AddAssign, Clone, Copy)]
struct HandleMetrics {
    begin: usize,
    control: usize,
    normal: usize,
}

pub struct Handler {
    local: HandleMetrics,
    metrics: Arc<Mutex<HandleMetrics>>,
}

impl PollHandler<Runner, Runner> for Handler {
    fn begin(&mut self, _batch_size: usize) {
        self.local.begin += 1;
    }

    fn handle_control(&mut self, control: &mut Managed<Runner>) -> bool {
        self.local.control += 1;
        while let Ok(r) = control.recv.try_recv() {
            if let Some(r) = r {
                r(control);
            }
        }
        false
    }

    fn handle_normal(&mut self, normal: &mut Managed<Runner>) -> bool {
        self.local.normal += 1;
        while let Ok(r) = normal.recv.try_recv() {
            if let Some(r) = r {
                r(normal);
            }
        }
        false
    }

    fn end(&mut self, _normals: &mut [Managed<Runner>]) {
        let mut c = self.metrics.lock().unwrap();
        *c += self.local;
        self.local = HandleMetrics::default();
    }
}

pub struct Builder {
    metrics: Arc<Mutex<HandleMetrics>>,
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