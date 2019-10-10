// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::CFHandle;
use rocksdb::CFHandle as RawCFHandle;
use std::mem;

// FIXME: This a nasty representation due to the lack of generic associated
// types. See comment on the KvEngine::CFHandle associated type. This could also
// be fixed if the CFHandle impl was defined inside the rust-rocksdb crate where
// the RawCFHandles are managed, but that would be an ugly abstraction
// violation.
#[repr(transparent)]
pub struct RocksCFHandle(*mut RawCFHandle);

impl RocksCFHandle {
    pub fn from_raw<'a>(raw: &'a RawCFHandle) -> &'a RocksCFHandle {
        // Scary
        unsafe { mem::transmute(raw) }
    }

    pub fn as_inner(&self) -> &RawCFHandle {
        unsafe { &*self.0 }
    }
}

impl CFHandle for RocksCFHandle {
}
