// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cf_options::RocksCFOptions;
use crate::db::Rocks;
use engine_traits::CFHandle;
use engine_traits::CFHandleExt;
use engine_traits::Result;
use rocksdb::CFHandle as RawCFHandle;

impl CFHandleExt for Rocks {
    type CFHandle = RocksCFHandle;
    type CFOptions = RocksCFOptions;

    fn get_cf_handle(&self, name: &str) -> Option<&Self::CFHandle> {
        self.as_inner().cf_handle(name).map(RocksCFHandle::from_raw)
    }

    fn get_options_cf(&self, cf: &Self::CFHandle) -> Self::CFOptions {
        RocksCFOptions::from_raw(self.as_inner().get_options_cf(cf.as_inner()))
    }

    fn set_options_cf(&self, cf: &Self::CFHandle, options: &[(&str, &str)]) -> Result<()> {
        self.as_inner()
            .set_options_cf(cf.as_inner(), options)
            .map_err(|e| box_err!(e))
    }
}

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
