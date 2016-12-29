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

use kvproto::raft_serverpb::RaftMessage;

use raftstore::Result;

// Transports message between different raft peers.
pub trait Transport: Send + Clone {
    fn send(&self, msg: RaftMessage) -> Result<()>;
}

pub struct RenewNetworkStat {
    pub store_id: u64,
    pub remote_store_ids: Vec<u64>,
}

// Transports message between store and network monitor.
pub trait NetworkMonitorTransport: Send + Clone {
    fn send(&self, stat: RenewNetworkStat) -> Result<()>;
}
