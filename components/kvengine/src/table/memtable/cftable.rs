// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use protobuf::ProtobufEnum;
use std::ops::Deref;
use std::sync::atomic::Ordering::{Acquire, Release};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::{Arena, SkipList};
use crate::NUM_CFS;

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

impl CFTable {
    pub fn new() -> Self {
        Self {
            core: Arc::new(CFTableCore::new()),
        }
    }
}

pub struct CFTableCore {
    tbls: [SkipList; NUM_CFS],
    arena: Arc<Arena>,
    ver: AtomicU64,
    props: Mutex<Option<kvenginepb::Properties>>,
    applying: AtomicBool,
    stage: AtomicI32,
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
            applying: AtomicBool::new(false),
            stage: AtomicI32::new(kvenginepb::SplitStage::Initial.value()),
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
        return self.ver.load(Ordering::Acquire);
    }

    pub fn set_applying(&self) {
        self.applying.store(true, Release);
    }

    pub fn is_applying(&self) -> bool {
        self.applying.load(Acquire)
    }

    pub fn set_split_stage(&self, stage: kvenginepb::SplitStage) {
        self.stage.store(stage.value(), Release);
    }

    pub fn get_split_stage(&self) -> kvenginepb::SplitStage {
        kvenginepb::SplitStage::from_i32(self.stage.load(Acquire)).unwrap()
    }
}
