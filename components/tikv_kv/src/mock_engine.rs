// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::LinkedList,
    sync::{Arc, Mutex},
};

use collections::HashMap;
use futures::Future;
use kvproto::kvrpcpb::Context;

use super::Result;
use crate::{
    Engine, Modify, OnReturnCallback, RocksEngine, SnapContext, WriteData, WriteSubscriber,
    SUBSCRIBE_COMMITTED, SUBSCRIBE_PROPOSED,
};

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

pub struct MockSubscriber<S> {
    sub: S,
    expected_proposed: Option<bool>,
    expected_committed: Option<bool>,
}

impl<S: WriteSubscriber + 'static> WriteSubscriber for MockSubscriber<S> {
    type ProposedWaiter<'a> = S::ProposedWaiter<'a>;
    fn wait_proposed(&mut self) -> Self::ProposedWaiter<'_> {
        assert_ne!(self.expected_proposed, Some(false));
        self.expected_proposed = Some(false);
        self.sub.wait_proposed()
    }

    type CommittedWaiter<'a> = S::CommittedWaiter<'a>;
    fn wait_committed(&mut self) -> Self::CommittedWaiter<'_> {
        assert_ne!(self.expected_committed, Some(false));
        self.expected_committed = Some(false);
        self.sub.wait_committed()
    }

    type ResultWaiter = S::ResultWaiter;
    fn result(self) -> Self::ResultWaiter {
        assert_ne!(self.expected_committed, Some(true));
        assert_ne!(self.expected_proposed, Some(true));
        self.sub.result()
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

fn check_callback(
    expected_writes: &LinkedList<ExpectedWrite>,
    f: impl Fn(&ExpectedWrite) -> Option<bool>,
) -> Option<bool> {
    expected_writes.iter().fold(None, |acc, w| {
        let expected = f(w);
        match (acc, expected) {
            (None, None) => None,
            (None, Some(b)) => Some(b),
            (Some(b), None) => Some(b),
            (Some(a), Some(b)) => {
                assert_eq!(a, b, "expected callback not match");
                Some(a)
            }
        }
    })
}

impl Engine for MockEngine {
    type Snap = <RocksEngine as Engine>::Snap;
    type Local = <RocksEngine as Engine>::Local;

    fn kv_engine(&self) -> Option<Self::Local> {
        self.base.kv_engine()
    }

    fn modify_on_kv_engine(&self, region_modifies: HashMap<u64, Vec<Modify>>) -> Result<()> {
        self.base.modify_on_kv_engine(region_modifies)
    }

    type SnapshotRes = impl Future<Output = Result<Self::Snap>>;
    fn async_snapshot(&mut self, ctx: SnapContext<'_>) -> Self::SnapshotRes {
        self.base.async_snapshot(ctx)
    }

    type WriteSubscriber = MockSubscriber<<RocksEngine as Engine>::WriteSubscriber>;
    fn async_write(
        &self,
        ctx: &Context,
        batch: WriteData,
        subscribed_event: u8,
        on_return: Option<OnReturnCallback<()>>,
    ) -> Self::WriteSubscriber {
        let (expected_proposed, expected_committed) =
            if let Some(expected_modifies) = self.expected_modifies.as_ref() {
                let mut expected_writes = expected_modifies.0.lock().unwrap();
                check_expected_write(
                    &mut expected_writes,
                    &batch.modifies,
                    subscribed_event & SUBSCRIBE_PROPOSED != 0,
                    subscribed_event & SUBSCRIBE_COMMITTED != 0,
                );
                (
                    check_callback(&expected_writes, |w| w.use_proposed_cb),
                    check_callback(&expected_writes, |w| w.use_committed_cb),
                )
            } else {
                (None, None)
            };
        let mut last_modifies = self.last_modifies.lock().unwrap();
        last_modifies.push(batch.modifies.clone());
        let sub = self
            .base
            .async_write(ctx, batch, subscribed_event, on_return);
        MockSubscriber {
            sub,
            expected_proposed,
            expected_committed,
        }
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
