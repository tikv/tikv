// Copyright 2017 PingCAP, Inc.
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

use std::ops::Deref;
use std::cmp::{Ord, Ordering, PartialOrd};
use std::sync::Arc;

use kvproto::kvrpcpb::*;
use kvproto::importpb::*;

use pd::RegionInfo;

use super::ImportClient;

pub const RANGE_MIN: &'static [u8] = &[];
pub const RANGE_MAX: &'static [u8] = &[];

pub fn new_range(start: &[u8], end: &[u8]) -> Range {
    let mut range = Range::new();
    range.set_start(start.to_owned());
    range.set_end(end.to_owned());
    range
}

#[derive(Eq, PartialEq)]
pub struct RangeEnd<'a>(pub &'a [u8]);

impl<'a> Ord for RangeEnd<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.0 == RANGE_MAX && other.0 == RANGE_MAX {
            Ordering::Equal
        } else if self.0 == RANGE_MAX {
            Ordering::Greater
        } else if other.0 == RANGE_MAX {
            Ordering::Less
        } else {
            self.0.cmp(other.0)
        }
    }
}

impl<'a> PartialOrd for RangeEnd<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug)]
pub struct RangeInfo {
    pub range: Range,
    pub size: usize,
}

impl RangeInfo {
    pub fn new(start: &[u8], end: &[u8], size: usize) -> RangeInfo {
        RangeInfo {
            range: new_range(start, end),
            size: size,
        }
    }
}

impl Deref for RangeInfo {
    type Target = Range;

    fn deref(&self) -> &Self::Target {
        &self.range
    }
}

pub fn new_context(region: &RegionInfo) -> Context {
    let peer = if let Some(ref leader) = region.leader {
        leader.clone()
    } else {
        // We don't know the leader, just choose the first one.
        region.get_peers().first().unwrap().clone()
    };

    let mut ctx = Context::new();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(peer.clone());
    ctx
}

/// A helper to decide how to cut ranges according to the size and region ranges.
pub struct RegionContext<C> {
    client: Arc<C>,
    region: Option<RegionInfo>,
    raw_size: usize,
    limit_size: usize,
}

impl<C: ImportClient> RegionContext<C> {
    pub fn new(client: Arc<C>, limit_size: usize) -> RegionContext<C> {
        RegionContext {
            client: client,
            region: None,
            raw_size: 0,
            limit_size: limit_size,
        }
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        self.raw_size += key.len() + value.len()
    }

    /// Reset size and region for the next key.
    pub fn reset(&mut self, key: &[u8]) {
        self.raw_size = 0;
        if let Some(ref region) = self.region {
            if RangeEnd(key) < RangeEnd(region.get_end_key()) {
                // Still belongs to this region, no need to update.
                return;
            }
        }
        self.region = match self.client.get_region(key) {
            Ok(region) => Some(region),
            Err(e) => {
                error!("get region: {:?}", e);
                None
            }
        }
    }

    pub fn raw_size(&self) -> usize {
        self.raw_size
    }

    /// Check size and region range to see if we should stop before this key.
    pub fn should_stop_before(&self, key: &[u8]) -> bool {
        if self.raw_size >= self.limit_size {
            return true;
        }
        if let Some(ref region) = self.region {
            if RangeEnd(key) >= RangeEnd(region.get_end_key()) {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use import::client::tests::MockClient;

    #[test]
    fn test_range_end() {
        let a = RANGE_MAX;
        let b = RANGE_MAX;
        let c = b"abc";
        let d = b"bcd";

        assert!(RangeEnd(a) == RangeEnd(b));
        assert!(RangeEnd(b) > RangeEnd(c));
        assert!(RangeEnd(c) < RangeEnd(d));
    }

    #[test]
    fn test_region_context() {
        let mut client = MockClient::new();
        client.add_region_range(b"", b"k4");
        client.add_region_range(b"k4", b"");

        let mut ctx = RegionContext::new(Arc::new(client), 8);

        ctx.add(b"k1", b"v1");
        assert!(!ctx.should_stop_before(b"k2"));
        ctx.add(b"k2", b"v2");
        assert_eq!(ctx.raw_size(), 8);
        // Reach size limit.
        assert!(ctx.should_stop_before(b"k3"));

        ctx.reset(b"k3");
        assert_eq!(ctx.raw_size(), 0);
        ctx.add(b"k3", b"v3");
        // Reach region end.
        assert!(ctx.should_stop_before(b"k4"));

        ctx.reset(b"k4");
        assert_eq!(ctx.raw_size(), 0);
        ctx.add(b"k4", b"v4");
        assert!(!ctx.should_stop_before(b"k5"));
    }
}
