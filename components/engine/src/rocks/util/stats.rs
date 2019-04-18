// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::string::FromUtf8Error;
use std::sync::Arc;

use crate::rocks::DB;

const ROCKSDB_DB_STATS_KEY: &str = "rocksdb.dbstats";
const ROCKSDB_CF_STATS_KEY: &str = "rocksdb.cfstats";

pub fn dump(engine: &Arc<DB>) -> Result<String, FromUtf8Error> {
    let mut s = Vec::with_capacity(1024);
    // common rocksdb stats.
    for name in engine.cf_names() {
        let handler = engine.cf_handle(name).unwrap();
        if let Some(v) = engine.get_property_value_cf(handler, ROCKSDB_CF_STATS_KEY) {
            s.extend_from_slice(v.as_bytes());
        }
    }

    if let Some(v) = engine.get_property_value(ROCKSDB_DB_STATS_KEY) {
        s.extend_from_slice(v.as_bytes());
    }

    // more stats if enable_statistics is true.
    if let Some(v) = engine.get_statistics() {
        s.extend_from_slice(v.as_bytes());
    }

    String::from_utf8(s)
}
