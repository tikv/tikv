// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use batch_system::Fsm;
use crossbeam::channel::TryRecvError;
use engine_traits::KvEngine;
use tikv_util::mpsc::{self, LooseBoundedSender, Receiver};

use crate::{batch::ApplyContext, raft::Apply, router::ApplyTask};

pub struct ApplyFsm<EK: KvEngine> {
    apply: Apply<EK>,
    receiver: Receiver<ApplyTask>,
    is_stopped: bool,
}

impl<EK: KvEngine> ApplyFsm<EK> {
    pub fn new(apply: Apply<EK>) -> (LooseBoundedSender<ApplyTask>, Box<Self>) {
        let (tx, rx) = mpsc::loose_bounded(usize::MAX);
        (
            tx,
            Box::new(Self {
                apply,
                receiver: rx,
                is_stopped: false,
            }),
        )
    }

    /// Fetches messages to `apply_task_buf`. It will stop when the buffer
    /// capacity is reached or there is no more pending messages.
    ///
    /// Returns how many messages are fetched.
    pub fn recv(&mut self, apply_task_buf: &mut Vec<ApplyTask>) -> usize {
        let l = apply_task_buf.len();
        for i in l..apply_task_buf.capacity() {
            match self.receiver.try_recv() {
                Ok(msg) => apply_task_buf.push(msg),
                Err(e) => {
                    if let TryRecvError::Disconnected = e {
                        self.is_stopped = true;
                    }
                    return i - l;
                }
            }
        }
        apply_task_buf.capacity() - l
    }
}

impl<EK: KvEngine> Fsm for ApplyFsm<EK> {
    type Message = ApplyTask;

    #[inline]
    fn is_stopped(&self) -> bool {
        self.is_stopped
    }
}

pub struct ApplyFsmDelegate<'a, EK: KvEngine> {
    fsm: &'a mut ApplyFsm<EK>,
    apply_ctx: &'a mut ApplyContext,
}

impl<'a, EK: KvEngine> ApplyFsmDelegate<'a, EK> {
    pub fn new(fsm: &'a mut ApplyFsm<EK>, apply_ctx: &'a mut ApplyContext) -> Self {
        Self { fsm, apply_ctx }
    }

    pub fn handle_msgs(&self, apply_task_buf: &mut Vec<ApplyTask>) {
        for task in apply_task_buf.drain(..) {
            // TODO: handle the tasks.
        }
    }
}
