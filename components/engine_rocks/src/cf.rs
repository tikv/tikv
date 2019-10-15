// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::CFHandle;
use rocksdb::CFHandle as RawCFHandle;

// FIXME: This a nasty representation pointer casting is due to the lack of
// generic associated types. See comment on the KvEngine::CFHandle associated
// type. This could also be fixed if the CFHandle impl was defined inside the
// rust-rocksdb crate where the RawCFHandles are managed, but that would be an
// ugly abstraction violation.
#[repr(transparent)]
pub struct RocksCFHandle(RawCFHandle);

impl RocksCFHandle {
    pub fn from_raw(raw: &RawCFHandle) -> &RocksCFHandle {
        unsafe { &*(raw as *const _ as *const _) }
    }

    pub fn as_inner(&self) -> &RawCFHandle {
        &self.0
    }
}

impl CFHandle for RocksCFHandle {}
