// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{MvccProperties, MvccPropertiesExt, Result};
use txn_types::TimeStamp;

use crate::engine::PanicEngine;

impl MvccPropertiesExt for PanicEngine {
    fn get_mvcc_properties_cf(
        &self,
        cf: &str,
        safe_point: TimeStamp,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Option<MvccProperties> {
        panic!()
    }
}
