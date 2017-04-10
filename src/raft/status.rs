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

use kvproto::eraftpb::HardState;

use raft::raft::{Raft, SoftState, StateRole};
use raft::storage::Storage;
use raft::progress::Progress;

use super::HashMap;

#[derive(Default)]
pub struct Status {
    pub id: u64,
    pub hs: HardState,
    pub ss: SoftState,
    pub applied: u64,
    pub progress: HashMap<u64, Progress>,
}

impl Status {
    // new gets a copy of the current raft status.
    pub fn new<T: Storage>(raft: &Raft<T>) -> Status {
        let mut s = Status {
            id: raft.id,
            ..Default::default()
        };
        s.hs = raft.hard_state();
        s.ss = raft.soft_state();
        s.applied = raft.raft_log.get_applied();
        if s.ss.raft_state == StateRole::Leader {
            s.progress = raft.prs.clone();
        }
        s
    }
}
