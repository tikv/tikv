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

pub mod util;
pub mod cluster;
pub mod node;
pub mod server;
pub mod pd;
pub mod transport_simulate;

mod test_single;
mod test_multi;
mod test_conf_change;
mod test_compact_log;
mod test_compact_lock_cf;
mod test_compact_after_delete;
mod test_split_region;
mod test_region_merge;
mod test_status_command;
mod test_tombstone;
mod test_transport;
mod test_transfer_leader;
mod test_stats;
mod test_snap;
mod test_down_peers;
mod test_stale_peer;
mod test_lease_read;
