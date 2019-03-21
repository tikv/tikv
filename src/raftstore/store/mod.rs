// Copyright 2016 PingCAP, Inc.
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

pub mod cmd_resp;
pub mod config;
pub mod engine;
pub mod fsm;
pub mod keys;
pub mod msg;
pub mod transport;
pub mod util;

mod bootstrap;
mod local_metrics;
mod metrics;
mod peer;
mod peer_storage;
mod region_snapshot;
mod snap;
mod worker;

pub use self::bootstrap::{
    bootstrap_store, clear_prepare_bootstrap_cluster, clear_prepare_bootstrap_key, initial_region,
    prepare_bootstrap_cluster, INIT_EPOCH_CONF_VER, INIT_EPOCH_VER,
};
pub use self::config::Config;
pub use self::engine::{Iterable, Mutable, Peekable};
pub use self::fsm::{new_compaction_listener, DestroyPeerJob, RaftRouter, StoreInfo};
pub use self::msg::{
    Callback, CasualMessage, PeerMsg, PeerTick, RaftCommand, ReadCallback, ReadResponse,
    SeekRegionCallback, SeekRegionFilter, SeekRegionResult, SignificantMsg, StoreMsg, StoreTick,
    WriteCallback, WriteResponse,
};
pub use self::peer::{
    Peer, PeerStat, ProposalContext, ReadExecutor, RequestInspector, RequestPolicy,
};
pub use self::peer_storage::{
    clear_meta, do_snapshot, init_apply_state, init_raft_state, write_initial_apply_state,
    write_initial_raft_state, write_peer_state, PeerStorage, SnapState, RAFT_INIT_LOG_INDEX,
    RAFT_INIT_LOG_TERM,
};
pub use self::region_snapshot::{RegionIterator, RegionSnapshot};
pub use self::snap::{
    check_abort, copy_snapshot, ApplyOptions, Error as SnapError, SnapEntry, SnapKey, SnapManager,
    SnapManagerBuilder, Snapshot, SnapshotDeleter, SnapshotStatistics,
};
pub use self::transport::{CasualRouter, ProposalRouter, StoreRouter, Transport};
pub use self::util::Engines;
pub use self::worker::{KeyEntry, ReadTask};

// Only used in tests
#[cfg(test)]
pub use self::worker::{SplitCheckRunner, SplitCheckTask};
