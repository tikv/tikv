// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.
use std::{cell::RefCell, sync::Arc};

use crossbeam::channel::{self, RecvError, SendError, TryRecvError, TrySendError};
use kvproto::kvrpcpb::CommandPri;
use tikv_util::mpsc::priority_queue;

use crate::ResourceController;

pub trait ResourceMetered {
    // returns the msg consumption of each hash map
    fn consume_resource(&self, _: &Arc<ResourceController>) -> Option<String> {
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
    pub fn send(&self, m: T, low_bound: Option<u64>) -> Result<Option<u64>, SendError<T>> {
        match self {
            Sender::Vanilla(sender) => sender.send(m).map(|_| None),
            Sender::Priority {
                resource_ctl,
                sender,
                last_msg_group,
            } => {
                let p = resource_ctl
                    .get_priority(last_msg_group.borrow().as_bytes(), CommandPri::Normal);
                let priority = if let Some(low_bound) = low_bound {
                    std::cmp::max(p, low_bound)
                } else {
                    p
                };
                sender.send(m, priority).map(|_| Some(priority))
            }
        }
    }

    pub fn try_send(&self, m: T, low_bound: Option<u64>) -> Result<Option<u64>, TrySendError<T>> {
        match self {
            Sender::Vanilla(sender) => sender.try_send(m).map(|_| None),
            Sender::Priority {
                resource_ctl,
                sender,
                last_msg_group,
            } => {
                let p = resource_ctl
                    .get_priority(last_msg_group.borrow().as_bytes(), CommandPri::Normal);
                let priority = std::cmp::max(p, low_bound.unwrap_or(0));
                sender.try_send(m, priority).map(|_| Some(priority))
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
                if let Some(dominant_group) = msg.consume_resource(resource_ctl) {
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

#[cfg(test)]
mod tests {
    use std::{thread, usize};

    use test::Bencher;

    use super::*;
    use crate::ResourceConsumeType;

    struct Msg(usize);

    impl ResourceMetered for Msg {
        fn consume_resource(&self, resource_ctl: &Arc<ResourceController>) -> Option<String> {
            // None
            let write_bytes = self.0 as u64;
            let group_name = "test".to_owned();
            resource_ctl.consume(
                group_name.as_bytes(),
                ResourceConsumeType::IoBytes(write_bytes),
            );
            Some(group_name)
        }
    }

    #[bench]
    fn bench_channel(b: &mut Bencher) {
        let (tx, rx) = unbounded(Some(Arc::new(ResourceController::new_for_test(
            "test".to_owned(),
            false,
        ))));

        let t = thread::spawn(move || {
            let mut n2: usize = 0;
            loop {
                if let Ok(Msg(n)) = rx.recv() {
                    n2 += n;
                } else {
                    return n2;
                }
            }
        });

        let mut n1 = 0;
        b.iter(|| {
            n1 += 1;
            let msg = Msg(1);
            tx.consume_msg_resource(&msg);
            tx.send(msg, None).unwrap();
        });

        drop(tx);
        let n2 = t.join().unwrap();
        assert_eq!(n1, n2);
    }
}
