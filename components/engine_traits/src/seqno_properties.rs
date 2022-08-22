// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp;

use crate::{errors::Result, DATA_CFS};

#[derive(Debug)]
pub struct SeqnoProperties {
    pub largest_seqno: u64,
    pub smallest_seqno: u64,
}

impl Default for SeqnoProperties {
    fn default() -> Self {
        Self {
            largest_seqno: u64::MIN,
            smallest_seqno: u64::MAX,
        }
    }
}

pub trait SeqnoPropertiesExt {
    fn get_range_seqno_properties_cf(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Option<SeqnoProperties>>;

    fn get_range_seqno_properties_data_cf(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Option<SeqnoProperties>> {
        let mut res = None;
        for cf in DATA_CFS {
            let prop = self.get_range_seqno_properties_cf(cf, start_key, end_key)?;
            if let Some(prop) = prop {
                res = res.or_else(|| Some(SeqnoProperties::default()));
                let res = res.as_mut().unwrap();
                res.largest_seqno = cmp::max(res.largest_seqno, prop.largest_seqno);
                res.smallest_seqno = cmp::min(res.smallest_seqno, prop.smallest_seqno);
            }
        }
        Ok(res)
    }
}
