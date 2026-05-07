// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use crossbeam::channel::SendError;
use resource_control::channel::Sender;
use tikv_util::{time::Instant, warn};

use crate::{
    fsm::{Fsm, FsmScheduler, Priority},
    FsmTypes,
};
pub struct NormalScheduler<N: Fsm, C: Fsm> {
    pub(crate) sender: Sender<FsmTypes<N, C>>,
    pub(crate) low_sender: Sender<FsmTypes<N, C>>,
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

        match sender.send(FsmTypes::Normal((fsm, Instant::now_coarse())), None) {
            Ok(_) => {}
            Err(SendError(FsmTypes::Normal((fsm, _)))) => warn!("failed to schedule fsm {:p}", fsm),
            _ => unreachable!(),
        }
    }

    fn shutdown(&self) {
        // TODO: close it explicitly once it's supported.
        // Magic number, actually any number greater than poll pool size works.
        for _ in 0..256 {
            let _ = self.sender.send(FsmTypes::Empty, None);
            let _ = self.low_sender.send(FsmTypes::Empty, None);
        }
    }
}

pub struct ControlScheduler<N: Fsm, C: Fsm> {
    pub(crate) sender: Sender<FsmTypes<N, C>>,
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
        match self
            .sender
            .send(FsmTypes::Control((fsm, Instant::now_coarse())), None)
        {
            Ok(_) => {}
            Err(SendError(FsmTypes::Control((fsm, _)))) => {
                warn!("failed to schedule fsm {:p}", fsm)
            }
            _ => unreachable!(),
        }
    }

    fn shutdown(&self) {
        // TODO: close it explicitly once it's supported.
        // Magic number, actually any number greater than poll pool size works.
        for _ in 0..256 {
            let _ = self.sender.send(FsmTypes::Empty, None);
        }
    }
}
