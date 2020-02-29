// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::PanicEngine;
use engine_traits::{MiscExt, Result};

impl MiscExt for PanicEngine {
    fn flush_cf(&self, cf: &str, sync: bool) -> Result<()> { panic!() }
    
    fn delete_files_in_range_cf(&self, cf: &str, start_key: &[u8], end_key: &[u8], include_end: bool) -> Result<()> { panic!() }
}
