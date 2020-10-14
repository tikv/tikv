// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{Key, SkiplistEngine, Value};
use crossbeam_skiplist::map::Entry;
use engine_traits::{
    DecodeProperties, MvccProperties, MvccPropertiesExt, Result, TableProperties,
    TablePropertiesCollection, TablePropertiesExt,
};
use txn_types::{Key as TxnKey, TimeStamp, Write, WriteType};

use std::ops::{RangeFrom, RangeInclusive};

impl MvccPropertiesExt for SkiplistEngine {
    fn get_mvcc_properties_cf(
        &self,
        cf: &str,
        safe_point: TimeStamp,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<MvccProperties> {
        let engine = self.get_cf_engine(cf).unwrap();
        let range: Box<dyn Iterator<Item = Entry<Key, Value>>> = if end_key.is_empty() {
            Box::new(engine.range(RangeFrom {
                start: (start_key.to_vec(), 0),
            }))
        } else {
            Box::new(engine.range(RangeInclusive::new(
                (start_key.to_vec(), 0),
                (end_key.to_vec(), 0),
            )))
        };

        let mut props = MvccProperties::new();
        let mut last_row: Option<Vec<u8>> = None;
        let mut row_versions = 0;
        for entry in range {
            if !keys::validate_data_key(&entry.key().0) {
                continue;
            }

            let (k, ts) = match TxnKey::split_on_ts_for(&entry.key().0) {
                Ok((k, ts)) if ts < safe_point => (k, ts),
                _ => {
                    continue;
                }
            };

            props.min_ts = std::cmp::min(props.min_ts, ts);
            props.max_ts = std::cmp::max(props.max_ts, ts);
            props.num_versions += 1;

            match last_row.as_ref() {
                Some(last) if k == last.as_slice() => {
                    row_versions += 1;
                }
                _ => {
                    props.num_rows += 1;
                    row_versions = 1;
                    last_row = Some(k.to_vec())
                }
            }

            if row_versions > props.max_row_versions {
                props.max_row_versions = row_versions;
            }

            let write_type = match Write::parse_type(&entry.value()) {
                Ok(v) => v,
                Err(_) => {
                    continue;
                }
            };

            match write_type {
                WriteType::Put => props.num_puts += 1,
                WriteType::Delete => props.num_deletes += 1,
                _ => {}
            }
        }
        Ok(props)
    }
}
