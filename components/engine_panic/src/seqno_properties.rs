// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{Result, SeqnoProperties, SeqnoPropertiesExt};

use crate::engine::PanicEngine;

impl SeqnoPropertiesExt for PanicEngine {
    fn get_range_seqno_properties_cf(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Option<SeqnoProperties>> {
        panic!()
    }
}
