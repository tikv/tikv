// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cell::RefCell, sync::Arc};

use collections::HashMap;
use crossbeam::channel::{self, RecvError, SendError, TryRecvError, TrySendError};
use kvproto::kvrpcpb::CommandPri;
use tikv_util::mpsc::priority_queue;

use crate::{ResourceConsumeType, ResourceController};

pub trait ResourceMetered {
    // returns the msg consumption of each hash map
    fn get_resource_consumptions(&self) -> Option<HashMap<String, u64>> {
        None
    }
}

pub fn bounded<T: Send + 'static>(
    resource_ctl: Option<Arc<ResourceController>>,
    cap: usize,
) -> (Sender<T>, Receiver<T>) {
    if let Some(ctl) = resource_ctl {
        // TODO: make it bounded
        let (tx, rx) = priority_queue::unbounded();
        (
            Sender::Priority {
                resource_ctl: ctl,
                sender: tx,
                last_msg_group: RefCell::new(String::new()),
            },
            Receiver::Priority(rx),
        )
    } else {
        let (tx, rx) = channel::bounded(cap);
        (Sender::Vanilla(tx), Receiver::Vanilla(rx))
    }
}

pub fn unbounded<T: Send + 'static>(
    resource_ctl: Option<Arc<ResourceController>>,
) -> (Sender<T>, Receiver<T>) {
    if let Some(ctl) = resource_ctl {
        let (tx, rx) = priority_queue::unbounded();
        (
            Sender::Priority {
                resource_ctl: ctl,
                sender: tx,
                last_msg_group: RefCell::new(String::new()),
            },
            Receiver::Priority(rx),
        )
    } else {
        let (tx, rx) = channel::unbounded();
        (Sender::Vanilla(tx), Receiver::Vanilla(rx))
    }
}

pub enum Sender<T: Send + 'static> {
    Vanilla(channel::Sender<T>),
    Priority {
        resource_ctl: Arc<ResourceController>,
        sender: priority_queue::Sender<T>,
        last_msg_group: RefCell<String>,
    },
}

impl<T: Send + 'static> Clone for Sender<T> {
    fn clone(&self) -> Self {
        match self {
            Sender::Vanilla(sender) => Sender::Vanilla(sender.clone()),
            Sender::Priority {
                resource_ctl,
                sender,
                ..
            } => Sender::Priority {
                resource_ctl: resource_ctl.clone(),
                sender: sender.clone(),
                last_msg_group: RefCell::new(String::new()),
            },
        }
    }
}

impl<T: Send + 'static> Sender<T> {
    // `low_bound` represents the lowest priority that the message can be sent with.
    // It's used to make sure messages from one peer are sent in order.
    // The returned value is the priority that the message sent with. It is
    // calculated by resource controller and compared with `low_bound`.
    pub fn send(&self, m: T, low_bound: u64) -> Result<u64, SendError<T>> {
        match self {
            Sender::Vanilla(sender) => sender.send(m).map(|_| 0),
            Sender::Priority {
                resource_ctl,
                sender,
                last_msg_group,
            } => {
                // TODO: pass different command priority
                let priority = std::cmp::max(
                    resource_ctl
                        .get_priority(last_msg_group.borrow().as_bytes(), CommandPri::Normal),
                    low_bound,
                );
                sender.send(m, priority).map(|_| priority)
            }
        }
    }

    pub fn try_send(&self, m: T, low_bound: u64) -> Result<u64, TrySendError<T>> {
        match self {
            Sender::Vanilla(sender) => sender.try_send(m).map(|_| 0),
            Sender::Priority {
                resource_ctl,
                sender,
                last_msg_group,
            } => {
                let priority = std::cmp::max(
                    resource_ctl
                        .get_priority(last_msg_group.borrow().as_bytes(), CommandPri::Normal),
                    low_bound,
                );
                sender.try_send(m, priority).map(|_| priority)
            }
        }
    }

    pub fn consume_msg_resource(&self, msg: &impl ResourceMetered) {
        match self {
            Sender::Vanilla(_) => {}
            Sender::Priority {
                resource_ctl,
                last_msg_group,
                ..
            } => {
                if let Some(mut groups) = msg.get_resource_consumptions() {
                    let mut dominant_group = "".to_owned();
                    let mut max_write_bytes = 0;
                    for (group_name, write_bytes) in groups.drain() {
                        resource_ctl.consume(
                            group_name.as_bytes(),
                            ResourceConsumeType::IoBytes(write_bytes),
                        );
                        if write_bytes > max_write_bytes {
                            dominant_group = group_name;
                            max_write_bytes = write_bytes;
                        }
                    }
                    *last_msg_group.borrow_mut() = dominant_group;
                }
            }
        }
    }
}

pub enum Receiver<T: Send + 'static> {
    Vanilla(channel::Receiver<T>),
    Priority(priority_queue::Receiver<T>),
}

impl<T: Send + 'static> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        match self {
            Receiver::Vanilla(receiver) => Receiver::Vanilla(receiver.clone()),
            Receiver::Priority(receiver) => Receiver::Priority(receiver.clone()),
        }
    }
}

impl<T: Send + 'static> Receiver<T> {
    pub fn recv(&self) -> Result<T, RecvError> {
        match self {
            Receiver::Vanilla(receiver) => receiver.recv(),
            Receiver::Priority(receiver) => receiver.recv(),
        }
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        match self {
            Receiver::Vanilla(receiver) => receiver.try_recv(),
            Receiver::Priority(receiver) => receiver.try_recv(),
        }
    }
}
