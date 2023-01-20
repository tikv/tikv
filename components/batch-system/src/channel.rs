// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cell::RefCell, sync::Arc};

use crossbeam::channel::{self, RecvError, SendError, TryRecvError, TrySendError};
use kvproto::kvrpcpb::CommandPri;
use resource_control::{ResourceConsumeType, ResourceController};
use tikv_util::{mpsc::priority_queue, warn};

use crate::{
    fsm::{Fsm, FsmScheduler, Priority, ResourceMetered},
    FsmTypes,
};

pub fn fsm_channel<N: Fsm, C: Fsm>(
    resource_ctl: Option<Arc<ResourceController>>,
) -> (FsmSender<N, C>, FsmReceiver<N, C>) {
    if let Some(ctl) = resource_ctl {
        let (tx, rx) = priority_queue::unbounded();
        (
            FsmSender::Priority {
                resource_ctl: ctl,
                sender: tx,
                last_msg_group: RefCell::new(String::new()),
            },
            FsmReceiver::Priority(rx),
        )
    } else {
        let (tx, rx) = channel::unbounded();
        (FsmSender::Vanilla(tx), FsmReceiver::Vanilla(rx))
    }
}

pub struct NormalScheduler<N: Fsm, C: Fsm> {
    pub(crate) sender: FsmSender<N, C>,
    pub(crate) low_sender: FsmSender<N, C>,
}

impl<N, C> Clone for NormalScheduler<N, C>
where
    N: Fsm,
    C: Fsm,
{
    fn clone(&self) -> Self {
        NormalScheduler {
            sender: self.sender.clone(),
            low_sender: self.low_sender.clone(),
        }
    }
}

impl<N, C> FsmScheduler for NormalScheduler<N, C>
where
    N: Fsm,
    C: Fsm,
{
    type Fsm = N;

    fn consume_msg_resource(&self, msg: &<Self::Fsm as Fsm>::Message) {
        self.sender.consume_msg_resource(msg);
    }

    #[inline]
    fn schedule(&self, fsm: Box<N>) {
        let sender = match fsm.get_priority() {
            Priority::Normal => &self.sender,
            Priority::Low => &self.low_sender,
        };

        match sender.send(FsmTypes::Normal(fsm)) {
            Ok(()) => {}
            Err(SendError(FsmTypes::Normal(fsm))) => warn!("failed to schedule fsm {:p}", fsm),
            _ => unreachable!(),
        }
    }

    fn shutdown(&self) {
        // TODO: close it explicitly once it's supported.
        // Magic number, actually any number greater than poll pool size works.
        for _ in 0..256 {
            let _ = self.sender.send(FsmTypes::Empty);
            let _ = self.low_sender.send(FsmTypes::Empty);
        }
    }
}

pub struct ControlScheduler<N: Fsm, C: Fsm> {
    pub(crate) sender: FsmSender<N, C>,
}

impl<N, C> Clone for ControlScheduler<N, C>
where
    N: Fsm,
    C: Fsm,
{
    fn clone(&self) -> Self {
        ControlScheduler {
            sender: self.sender.clone(),
        }
    }
}

impl<N, C> FsmScheduler for ControlScheduler<N, C>
where
    N: Fsm,
    C: Fsm,
{
    type Fsm = C;

    fn consume_msg_resource(&self, _msg: &<Self::Fsm as Fsm>::Message) {}

    #[inline]
    fn schedule(&self, fsm: Box<C>) {
        match self.sender.send(FsmTypes::Control(fsm)) {
            Ok(()) => {}
            Err(SendError(FsmTypes::Control(fsm))) => warn!("failed to schedule fsm {:p}", fsm),
            _ => unreachable!(),
        }
    }

    fn shutdown(&self) {
        // TODO: close it explicitly once it's supported.
        // Magic number, actually any number greater than poll pool size works.
        for _ in 0..256 {
            let _ = self.sender.send(FsmTypes::Empty);
        }
    }
}

pub enum FsmSender<N: Fsm, C: Fsm> {
    Vanilla(channel::Sender<FsmTypes<N, C>>),
    Priority {
        resource_ctl: Arc<ResourceController>,
        sender: priority_queue::Sender<FsmTypes<N, C>>,
        last_msg_group: RefCell<String>,
    },
}

impl<N, C> Clone for FsmSender<N, C>
where
    N: Fsm,
    C: Fsm,
{
    fn clone(&self) -> Self {
        match self {
            FsmSender::Vanilla(sender) => FsmSender::Vanilla(sender.clone()),
            FsmSender::Priority {
                resource_ctl,
                sender,
                ..
            } => FsmSender::Priority {
                resource_ctl: resource_ctl.clone(),
                sender: sender.clone(),
                last_msg_group: RefCell::new(String::new()),
            },
        }
    }
}

impl<N: Fsm, C: Fsm> FsmSender<N, C> {
    pub fn send(&self, fsm: FsmTypes<N, C>) -> Result<(), SendError<FsmTypes<N, C>>> {
        match self {
            FsmSender::Vanilla(sender) => sender.send(fsm),
            FsmSender::Priority {
                resource_ctl,
                sender,
                last_msg_group,
            } => {
                // TODO: pass different priority
                let pri = resource_ctl
                    .get_priority(last_msg_group.borrow().as_bytes(), CommandPri::Normal);
                sender.send(fsm, pri)
            }
        }
    }

    pub fn try_send(&self, fsm: FsmTypes<N, C>) -> Result<(), TrySendError<FsmTypes<N, C>>> {
        match self {
            FsmSender::Vanilla(sender) => sender.try_send(fsm),
            FsmSender::Priority {
                resource_ctl,
                sender,
                last_msg_group,
            } => {
                let priority = resource_ctl
                    .get_priority(last_msg_group.borrow().as_bytes(), CommandPri::Normal);
                sender.try_send(fsm, priority)
            }
        }
    }

    fn consume_msg_resource(&self, msg: &N::Message) {
        match self {
            FsmSender::Vanilla(_) => {}
            FsmSender::Priority {
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

pub enum FsmReceiver<N: Fsm, C: Fsm> {
    Vanilla(channel::Receiver<FsmTypes<N, C>>),
    Priority(priority_queue::Receiver<FsmTypes<N, C>>),
}

impl<N, C> Clone for FsmReceiver<N, C>
where
    N: Fsm,
    C: Fsm,
{
    fn clone(&self) -> Self {
        match self {
            FsmReceiver::Vanilla(receiver) => FsmReceiver::Vanilla(receiver.clone()),
            FsmReceiver::Priority(receiver) => FsmReceiver::Priority(receiver.clone()),
        }
    }
}

impl<N: Fsm, C: Fsm> FsmReceiver<N, C> {
    pub fn recv(&self) -> Result<FsmTypes<N, C>, RecvError> {
        match self {
            FsmReceiver::Vanilla(receiver) => receiver.recv(),
            FsmReceiver::Priority(receiver) => receiver.recv(),
        }
    }

    pub fn try_recv(&self) -> Result<FsmTypes<N, C>, TryRecvError> {
        match self {
            FsmReceiver::Vanilla(receiver) => receiver.try_recv(),
            FsmReceiver::Priority(receiver) => receiver.try_recv(),
        }
    }
}
