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

//! Generally peers are state machines that represent a replica of a region,
//! and store is also a special state machine that handles all requests across
//! stores. They are mixed for now, will be separated in the future.

pub mod apply;
mod batch;
mod metrics;
mod peer;
mod router;
pub mod store;

pub use self::apply::{
    create_apply_batch_system, Apply, ApplyBatchSystem, ApplyMetrics, ApplyRes, ApplyRouter,
    Builder as ApplyPollerBuilder, ChangePeer, ExecResult, Msg as ApplyTask,
    Notifier as ApplyNotifier, Proposal, RegionProposal, Registration, TaskRes as ApplyTaskRes,
};
pub use self::batch::{
    BatchRouter, BatchSystem, Fsm, HandlerBuilder, NormalScheduler, PollHandler,
};
pub use self::peer::DestroyPeerJob;
pub use self::router::{BasicMailbox, Mailbox};
pub use self::store::{
    create_raft_batch_system, new_compaction_listener, RaftBatchSystem, RaftPollerBuilder,
    RaftRouter, StoreInfo,
};
