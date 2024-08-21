// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use kvproto::metapb::Region;

pub trait SnapshotPin: Send + Sync {}

pub trait SnapshotObserver: Send {
    fn on_snapshot(&self, region: &Region, read_ts: u64, seqno: u64) -> Arc<dyn SnapshotPin>;
}
