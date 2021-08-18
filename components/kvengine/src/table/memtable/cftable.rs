// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use crate::{NUM_CFS, table::Value};
use super::{Arena, Hint, SkipList};

#[derive(Clone)]
pub struct CFTable {
    tbls: [SkipList; NUM_CFS],
    arena: Arc<Arena>,
    ver: u64,
    props: Option<kvenginepb::Properties>,
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
        return self.ver
    }
}