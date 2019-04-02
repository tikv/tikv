// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Write;
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
            writeln!(&mut s, "{}", v).unwrap();
        }
    }

    if let Some(v) = engine.get_property_value(ROCKSDB_DB_STATS_KEY) {
        writeln!(&mut s, "{}", v).unwrap();
    }

    // more stats if enable_statistics is true.
    if let Some(v) = engine.get_statistics() {
        writeln!(&mut s, "{}", v).unwrap();
    }

    String::from_utf8(s)
}
