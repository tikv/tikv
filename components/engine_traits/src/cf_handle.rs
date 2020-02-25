// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cf_options::ColumnFamilyOptions;
use crate::errors::Result;

/// Trait for engines with column family handles.
///
/// FIXME: It's probably the case that all engines used by TiKV must provide the
/// column family concept; column families might be pervasive; and we might not
/// want to expose CF handles at all, instead just naming them. But for now,
/// everything involving CF handles is here.
pub trait CFHandleExt {
    // FIXME: With the current rocks implementation this
    // type wants to be able to contain a lifetime, but it's
    // not possible without "generic associated types".
    //
    // https://github.com/rust-lang/rfcs/pull/1598
    // https://github.com/rust-lang/rust/issues/44265
    type CFHandle: CFHandle;
    type ColumnFamilyOptions: ColumnFamilyOptions;

    fn cf_handle(&self, name: &str) -> Result<&Self::CFHandle>;
    fn get_options_cf(&self, cf: &Self::CFHandle) -> Self::ColumnFamilyOptions;
    fn set_options_cf(&self, cf: &Self::CFHandle, options: &[(&str, &str)]) -> Result<()>;
}

pub trait CFHandle {}
