// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use super::{
    AdminObserver, AdminRequest, AdminResponse, Coprocessor, CoprocessorHost, ObserverContext,
    Result, RoleObserver,
};
use kvproto::metapb::Region;
use raft::StateRole;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};

/// `RaftStoreEvent` Represents events dispatched from raftstore coprocessor. Only Admin events and
/// RoleChange events are listened.
#[derive(Debug)]
pub enum RaftStoreEvent {
    PreProposeAdmin {
        region: Region,
        request: AdminRequest,
    },
    PreApplyAdmin {
        region: Region,
        request: AdminRequest,
    },
    PostApplyAdmin {
        region: Region,
        response: AdminResponse,
    },
    RoleChange {
        region: Region,
        role: StateRole,
    },
}

impl Display for RaftStoreEvent {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        Debug::fmt(self, f)
    }
}

/// `EventSenderFilter` is used to determine whether a event should be processed.
/// It's not a good idea to send all events because we have to do useless copy on something
/// like `Region`. A method in this trait should return true if the event should be sent.
/// By default, all methods in this trait returns false.
pub trait EventSenderFilter: Clone + Send + Sync + 'static {
    fn pre_propose_admin_filter(&self, _: &ObserverContext, _: &AdminRequest) -> bool {
        false
    }

    fn pre_apply_admin_filter(&self, _: &ObserverContext, _: &AdminRequest) -> bool {
        false
    }

    fn post_apply_admin_filter(&self, _: &ObserverContext, _: &AdminResponse) -> bool {
        false
    }

    fn on_role_change_filter(&self, _: &ObserverContext, _: StateRole) -> bool {
        false
    }
}

// TODO: Constraints of type S can be simplified after Rust RFC #1733
#[derive(Clone)]
pub struct EventSender<F: EventSenderFilter, S: Fn(RaftStoreEvent) + Send + Sync + Clone + 'static>
{
    /// Filter out events that needn't to be sent.
    filter: F,
    /// The function used to send the event as a message.
    send_op: S,
}

impl<F: EventSenderFilter, S: Fn(RaftStoreEvent) + Send + Sync + Clone + 'static> Coprocessor
    for EventSender<F, S>
{}

impl<F: EventSenderFilter, S: Fn(RaftStoreEvent) + Send + Sync + Clone + 'static> AdminObserver
    for EventSender<F, S>
{
    fn pre_propose_admin(
        &self,
        context: &mut ObserverContext,
        req: &mut AdminRequest,
    ) -> Result<()> {
        if self.filter.pre_propose_admin_filter(context, req) {
            let region = context.region.clone();
            let request = req.clone();
            let event = RaftStoreEvent::PreProposeAdmin { region, request };
            (self.send_op)(event);
        }
        Ok(())
    }

    fn pre_apply_admin(&self, context: &mut ObserverContext, req: &AdminRequest) {
        if self.filter.pre_apply_admin_filter(context, req) {
            let region = context.region.clone();
            let request = req.clone();
            let event = RaftStoreEvent::PreApplyAdmin { region, request };
            (self.send_op)(event);
        }
    }

    fn post_apply_admin(&self, context: &mut ObserverContext, resp: &mut AdminResponse) {
        if self.filter.post_apply_admin_filter(context, resp) {
            let region = context.region.clone();
            let response = resp.clone();
            let event = RaftStoreEvent::PostApplyAdmin { region, response };
            (self.send_op)(event);
        }
    }
}

impl<F: EventSenderFilter, S: Fn(RaftStoreEvent) + Send + Sync + Clone + 'static> RoleObserver
    for EventSender<F, S>
{
    fn on_role_change(&self, context: &mut ObserverContext, role: StateRole) {
        if self.filter.on_role_change_filter(context, role) {
            let region = context.region.clone();
            let event = RaftStoreEvent::RoleChange { region, role };
            (self.send_op)(event);
        }
    }
}

pub fn register_raftstore_event_sender<
    F: EventSenderFilter,
    S: Fn(RaftStoreEvent) + Send + Sync + Clone + 'static,
>(
    host: &mut CoprocessorHost,
    filter: F,
    send_op: S,
) {
    let event_sender = EventSender { filter, send_op };

    host.registry
        .register_admin_observer(1, box event_sender.clone());
    host.registry.register_role_observer(1, box event_sender);
}
