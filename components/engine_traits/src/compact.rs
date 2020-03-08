// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! Functionality related to compaction

use crate::errors::Result;

pub trait CompactExt {
    /// Checks whether any column family sets `disable_auto_compactions` to `True` or not.
    fn auto_compactions_is_disabled(&self) -> Result<bool>;
}
