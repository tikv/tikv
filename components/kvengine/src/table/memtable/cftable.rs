// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use protobuf::ProtobufEnum;
use std::sync::atomic::Ordering::{Acquire, Release};
use std::sync::atomic::{AtomicBool, AtomicI32};
use std::sync::Arc;

use super::{Arena, SkipList};
use crate::NUM_CFS;

pub struct CFTable {
    tbls: [SkipList; NUM_CFS],
    arena: Arc<Arena>,
    ver: u64,
    props: Option<kvenginepb::Properties>,
    applying: AtomicBool,
    stage: AtomicI32,
}

impl Clone for CFTable {
    fn clone(&self) -> Self {
        Self {
            tbls: self.tbls.clone(),
            arena: self.arena.clone(),
            ver: self.ver,
            props: self.props.clone(),
            applying: AtomicBool::new(self.is_applying()),
            stage: AtomicI32::new(self.get_split_stage().value()),
        }
    }
}

impl CFTable {
    pub fn new() -> Self {
        let arena = Arc::new(Arena::new());
        Self {
            tbls: [
                SkipList::new(Some(arena.clone())),
                SkipList::new(Some(arena.clone())),
                SkipList::new(Some(arena.clone())),
            ],
            arena,
            ver: 0,
            props: None,
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

    pub fn set_version(&mut self, ver: u64) {
        self.ver = ver
    }

    pub fn set_properties(&mut self, props: kvenginepb::Properties) {
        self.props = Some(props)
    }

    pub fn get_properties(&self) -> kvenginepb::Properties {
        self.props.as_ref().unwrap().clone()
    }

    pub fn get_version(&self) -> u64 {
        return self.ver;
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
        kvenginepb::SplitStage::from_i32(self.stage.load(Release)).unwrap()
    }
}
