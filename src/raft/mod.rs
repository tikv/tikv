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

// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod raft_log;
pub mod storage;
mod raft;
mod progress;
mod errors;
mod log_unstable;
mod status;
pub mod raw_node;
mod read_only;

pub use self::storage::{RaftState, Storage};
pub use self::errors::{Result, Error, StorageError};
pub use self::raft::{Raft, StateRole, Config, INVALID_ID, INVALID_INDEX, SoftState,
                     vote_resp_msg_type, quorum};
pub use self::raft_log::{RaftLog, NO_LIMIT};
pub use self::raw_node::{Ready, RawNode, Peer, is_empty_snap, SnapshotStatus};
pub use self::status::Status;
pub use self::log_unstable::Unstable;
pub use self::progress::{Inflights, Progress, ProgressState};
pub use self::read_only::{ReadOnlyOption, ReadState};
use util::collections::{HashMap, HashSet, FlatMap};
