// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::LinkedList,
    sync::{Arc, Mutex},
};

use collections::HashMap;
use kvproto::kvrpcpb::Context;

use super::Result;
use crate::{Engine, Modify, OnAppliedCb, RocksEngine, SnapContext, WriteData, WriteEvent};

/// A mock engine is a simple wrapper around RocksEngine
/// but with the ability to assert the modifies,
/// the callback used, and other aspects during interaction with the engine
#[derive(Clone)]
pub struct MockEngine {
    base: RocksEngine,
    expected_modifies: Option<Arc<ExpectedWriteList>>,
    last_modifies: Arc<Mutex<Vec<Vec<Modify>>>>,
}

impl MockEngine {
    pub fn take_last_modifies(&self) -> Vec<Vec<Modify>> {
        let mut last_modifies = self.last_modifies.lock().unwrap();
        std::mem::take(&mut last_modifies)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExpectedWrite {
    // if the following `Option`s are None, it means we just don't care
    modify: Option<Modify>,
    use_proposed_cb: Option<bool>,
    use_committed_cb: Option<bool>,
}

impl ExpectedWrite {
    pub fn new() -> Self {
        Default::default()
    }
    #[must_use]
    pub fn expect_modify(self, modify: Modify) -> Self {
        Self {
            modify: Some(modify),
            use_proposed_cb: self.use_proposed_cb,
            use_committed_cb: self.use_committed_cb,
        }
    }
    #[must_use]
    pub fn expect_proposed_cb(self) -> Self {
        Self {
            modify: self.modify,
            use_proposed_cb: Some(true),
            use_committed_cb: self.use_committed_cb,
        }
    }
    #[must_use]
    pub fn expect_no_proposed_cb(self) -> Self {
        Self {
            modify: self.modify,
            use_proposed_cb: Some(false),
            use_committed_cb: self.use_committed_cb,
        }
    }
    #[must_use]
    pub fn expect_committed_cb(self) -> Self {
        Self {
            modify: self.modify,
            use_proposed_cb: self.use_proposed_cb,
            use_committed_cb: Some(true),
        }
    }
    #[must_use]
    pub fn expect_no_committed_cb(self) -> Self {
        Self {
            modify: self.modify,
            use_proposed_cb: self.use_proposed_cb,
            use_committed_cb: Some(false),
        }
    }
}

/// `ExpectedWriteList` represents a list of writes expected to write to the
/// engine
struct ExpectedWriteList(Mutex<LinkedList<ExpectedWrite>>);

// We implement drop here instead of on MockEngine
// because `MockEngine` can be cloned and dropped everywhere
// and we just want to assert every write
impl Drop for ExpectedWriteList {
    fn drop(&mut self) {
        let expected_modifies = &mut *self.0.lock().unwrap();
        assert_eq!(
            expected_modifies.len(),
            0,
            "not all expected modifies have been written to the engine, {} rest",
            expected_modifies.len()
        )
    }
}

fn check_expected_write(
    expected_writes: &mut LinkedList<ExpectedWrite>,
    modifies: &[Modify],
    has_proposed_cb: bool,
    has_committed_cb: bool,
) {
    for modify in modifies {
        if let Some(expected_write) = expected_writes.pop_front() {
            // check whether the modify is expected
            if let Some(expected_modify) = expected_write.modify {
                assert_eq!(
                    modify, &expected_modify,
                    "modify writing to Engine not match with expected"
                )
            }
            // check whether use right callback
            match expected_write.use_proposed_cb {
                Some(true) => assert!(
                    has_proposed_cb,
                    "this write is supposed to return during the propose stage"
                ),
                Some(false) => assert!(
                    !has_proposed_cb,
                    "this write is not supposed to return during the propose stage"
                ),
                None => {}
            }
            match expected_write.use_committed_cb {
                Some(true) => assert!(
                    has_committed_cb,
                    "this write is supposed to return during the commit stage"
                ),
                Some(false) => assert!(
                    !has_committed_cb,
                    "this write is not supposed to return during the commit stage"
                ),
                None => {}
            }
        } else {
            panic!("unexpected modify {:?} wrote to the Engine", modify)
        }
    }
}

impl Engine for MockEngine {
    type Snap = <RocksEngine as Engine>::Snap;
    type Local = <RocksEngine as Engine>::Local;

    fn kv_engine(&self) -> Option<Self::Local> {
        self.base.kv_engine()
    }

    type RaftExtension = <RocksEngine as Engine>::RaftExtension;
    fn raft_extension(&self) -> Self::RaftExtension {
        self.base.raft_extension()
    }

    fn modify_on_kv_engine(&self, region_modifies: HashMap<u64, Vec<Modify>>) -> Result<()> {
        self.base.modify_on_kv_engine(region_modifies)
    }

    type SnapshotRes = <RocksEngine as Engine>::SnapshotRes;
    fn async_snapshot(&mut self, ctx: SnapContext<'_>) -> Self::SnapshotRes {
        self.base.async_snapshot(ctx)
    }

    type WriteRes = <RocksEngine as Engine>::WriteRes;
    fn async_write(
        &self,
        ctx: &Context,
        batch: WriteData,
        subscribed: u8,
        on_applied: Option<OnAppliedCb>,
    ) -> Self::WriteRes {
        if let Some(expected_modifies) = self.expected_modifies.as_ref() {
            let mut expected_writes = expected_modifies.0.lock().unwrap();
            check_expected_write(
                &mut expected_writes,
                &batch.modifies,
                WriteEvent::subscribed_proposed(subscribed),
                WriteEvent::subscribed_committed(subscribed),
            );
        }
        let mut last_modifies = self.last_modifies.lock().unwrap();
        last_modifies.push(batch.modifies.clone());
        self.base.async_write(ctx, batch, subscribed, on_applied)
    }
}

pub struct MockEngineBuilder {
    base: RocksEngine,
    expected_modifies: Option<LinkedList<ExpectedWrite>>,
}

impl MockEngineBuilder {
    pub fn from_rocks_engine(rocks_engine: RocksEngine) -> Self {
        Self {
            base: rocks_engine,
            expected_modifies: None,
        }
    }

    #[must_use]
    pub fn add_expected_write(mut self, write: ExpectedWrite) -> Self {
        match self.expected_modifies.as_mut() {
            Some(expected_modifies) => expected_modifies.push_back(write),
            None => {
                let mut list = LinkedList::new();
                list.push_back(write);
                self.expected_modifies = Some(list);
            }
        }
        self
    }

    pub fn build(self) -> MockEngine {
        MockEngine {
            base: self.base,
            expected_modifies: self
                .expected_modifies
                .map(|m| Arc::new(ExpectedWriteList(Mutex::new(m)))),
            last_modifies: Arc::new(Mutex::new(Vec::new())),
        }
    }
}
