// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This trait contains miscellaneous features that have
//! not been carefully factored into other traits.
//!
//! FIXME: Things here need to be moved elsewhere.

use crate::errors::Result;

pub trait MiscExt {
    fn is_titan(&self) -> bool { false }

    fn delete_files_in_range_cf(&self, cf: &str, start_key: &[u8], end_key: &[u8], include_end: bool) -> Result<()>;
}
