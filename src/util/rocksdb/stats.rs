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

use rocksdb::{CFHandle, Range, DB};

use super::properties::MvccProperties;

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

pub fn get_range_entries_and_versions(
    engine: &DB,
    cf: &CFHandle,
    start: &[u8],
    end: &[u8],
) -> Option<(u64, u64)> {
    let range = Range::new(start, end);
    let collection = match engine.get_properties_of_tables_in_range(cf, &[range]) {
        Ok(v) => v,
        Err(_) => return None,
    };

    if collection.is_empty() {
        return None;
    }

    // Aggregate total MVCC properties and total number entries.
    let mut props = MvccProperties::new();
    let mut num_entries = 0;
    for (_, v) in &*collection {
        let mvcc = match MvccProperties::decode(v.user_collected_properties()) {
            Ok(v) => v,
            Err(_) => return None,
        };
        num_entries += v.num_entries();
        props.add(&mvcc);
    }

    Some((num_entries, props.num_versions))
}

#[cfg(test)]
mod tests {
    use ::rocksdb::{ColumnFamilyOptions, DBOptions, Writable};
    use tempdir::TempDir;

    use crate::raftstore::store::keys;
    use crate::storage::{Key, CF_WRITE, LARGE_CFS};
    use crate::util::properties::MvccPropertiesCollectorFactory;
    use crate::util::rocksdb::{self, CFOptions};

    use super::*;

    #[test]
    fn test_get_range_entries_and_versions() {
        let path = TempDir::new("_test_get_range_entries_and_versions").expect("");
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_level_zero_file_num_compaction_trigger(10);
        let f = Box::new(MvccPropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("tikv.mvcc-properties-collector", f);
        let cfs_opts = LARGE_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let db = rocksdb::new_engine_opt(path_str, db_opts, cfs_opts).unwrap();

        let cases = ["a", "b", "c"];
        for &key in &cases {
            let k1 = keys::data_key(Key::from_raw(key.as_bytes()).append_ts(2).as_encoded());
            let write_cf = db.cf_handle(CF_WRITE).unwrap();
            db.put_cf(write_cf, &k1, b"v1").unwrap();
            db.delete_cf(write_cf, &k1).unwrap();
            let key = keys::data_key(Key::from_raw(key.as_bytes()).append_ts(3).as_encoded());
            db.put_cf(write_cf, &key, b"v2").unwrap();
            db.flush_cf(write_cf, true).unwrap();
        }

        let start_keys = keys::data_key(&[]);
        let end_keys = keys::data_end_key(&[]);
        let cf = rocksdb::get_cf_handle(&db, CF_WRITE).unwrap();
        let (entries, versions) =
            get_range_entries_and_versions(&db, cf, &start_keys, &end_keys).unwrap();
        assert_eq!(entries, (cases.len() * 2) as u64);
        assert_eq!(versions, cases.len() as u64);
    }
}
