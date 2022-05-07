// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::{Arena, SkipList};
use crate::{Iterator, NUM_CFS};

#[derive(Clone)]
pub struct CFTable {
    pub core: Arc<CFTableCore>,
}

impl Deref for CFTable {
    type Target = CFTableCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl Default for CFTable {
    fn default() -> Self {
        Self::new()
    }
}

impl CFTable {
    pub fn new() -> Self {
        Self {
            core: Arc::new(CFTableCore::new()),
        }
    }

    pub fn new_split(&self) -> Self {
        if self.is_empty() {
            return Self::new();
        }
        let tbls = self.core.tbls.clone();
        let arena = self.core.arena.clone();
        let ver = AtomicU64::new(self.ver.load(Ordering::Acquire));
        let props = Mutex::new(self.core.props.lock().unwrap().clone());
        Self {
            core: Arc::new(CFTableCore {
                tbls,
                arena,
                ver,
                props,
            }),
        }
    }
}

pub struct CFTableCore {
    tbls: [SkipList; NUM_CFS],
    arena: Arc<Arena>,
    ver: AtomicU64,
    props: Mutex<Option<kvenginepb::Properties>>,
}

impl Default for CFTableCore {
    fn default() -> Self {
        Self::new()
    }
}

impl CFTableCore {
    pub fn new() -> Self {
        let arena = Arc::new(Arena::new());
        Self {
            tbls: [
                SkipList::new(Some(arena.clone())),
                SkipList::new(Some(arena.clone())),
                SkipList::new(Some(arena.clone())),
            ],
            arena,
            ver: AtomicU64::new(0),
            props: Mutex::new(None),
        }
    }

    pub fn get_cf(&self, cf: usize) -> &SkipList {
        &self.tbls[cf]
    }

    pub fn is_empty(&self) -> bool {
        for tbl in &self.tbls {
            if !tbl.is_empty() {
                return false;
            }
        }
        true
    }

    pub fn size(&self) -> usize {
        self.arena.size()
    }

    pub fn set_version(&self, ver: u64) {
        self.ver.store(ver, Ordering::Release)
    }

    pub fn set_properties(&self, props: kvenginepb::Properties) {
        self.props.lock().unwrap().replace(props);
    }

    pub fn get_properties(&self) -> Option<kvenginepb::Properties> {
        self.props.lock().unwrap().clone()
    }

    pub fn get_version(&self) -> u64 {
        self.ver.load(Ordering::Acquire)
    }

    pub fn has_data_in_range(&self, start: &[u8], end: &[u8]) -> bool {
        if self.is_empty() {
            return false;
        }
        for cf in 0..NUM_CFS {
            let tbl = &self.tbls[cf];
            let mut iter = tbl.new_iterator(false);
            iter.seek(start);
            if iter.valid() && iter.key() < end {
                return true;
            }
        }
        false
    }
}
