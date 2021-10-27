// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::raw::CompactOptions;
use engine_rocks::util::get_cf_handle;
use engine_traits::raw_value::RawValue;
use engine_traits::{MiscExt, Peekable, SyncMutable, CF_DEFAULT};
use kvproto::kvrpcpb::ApiVersion;
use tikv::config::DbConfig;
use tikv::server::ttl::check_ttl_and_compact_files;
use tikv::storage::kv::TestEngineBuilder;

#[test]
fn test_ttl_checker() {
    fn inner(api_version: ApiVersion) {
        fail::cfg("ttl_current_ts", "return(100)").unwrap();
        let mut cfg = DbConfig::default();
        cfg.defaultcf.disable_auto_compactions = true;
        let dir = tempfile::TempDir::new().unwrap();
        let builder = TestEngineBuilder::new()
            .path(dir.path())
            .api_version(api_version);
        let engine = builder.build_with_cfg(&cfg).unwrap();

        let kvdb = engine.get_rocksdb();
        let key1 = b"zr\0key1";
        let value1 = RawValue {
            user_value: vec![0; 10],
            expire_ts: Some(10),
        };
        kvdb.put_cf(CF_DEFAULT, key1, &value1.to_bytes(api_version))
            .unwrap();
        kvdb.flush_cf(CF_DEFAULT, true).unwrap();
        let key2 = b"zr\0key2";
        let value2 = RawValue {
            user_value: vec![0; 10],
            expire_ts: Some(120),
        };
        kvdb.put_cf(CF_DEFAULT, key2, &value2.to_bytes(api_version))
            .unwrap();
        let key3 = b"zr\0key3";
        let value3 = RawValue {
            user_value: vec![0; 10],
            expire_ts: Some(20),
        };
        kvdb.put_cf(CF_DEFAULT, key3, &value3.to_bytes(api_version))
            .unwrap();
        kvdb.flush_cf(CF_DEFAULT, true).unwrap();
        let key4 = b"zr\0key4";
        let value4 = RawValue {
            user_value: vec![0; 10],
            expire_ts: None,
        };
        kvdb.put_cf(CF_DEFAULT, key4, &value4.to_bytes(api_version))
            .unwrap();
        kvdb.flush_cf(CF_DEFAULT, true).unwrap();
        let key5 = b"zr\0key5";
        let value5 = RawValue {
            user_value: vec![0; 10],
            expire_ts: Some(10),
        };
        kvdb.put_cf(CF_DEFAULT, key5, &value5.to_bytes(api_version))
            .unwrap();
        kvdb.flush_cf(CF_DEFAULT, true).unwrap();

        assert!(kvdb.get_value_cf(CF_DEFAULT, key1).unwrap().is_some());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key2).unwrap().is_some());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key3).unwrap().is_some());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key4).unwrap().is_some());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key5).unwrap().is_some());

        let _ = check_ttl_and_compact_files(&kvdb, b"zr\0key1", b"zr\0key25", false);
        assert!(kvdb.get_value_cf(CF_DEFAULT, key1).unwrap().is_none());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key2).unwrap().is_some());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key3).unwrap().is_none());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key4).unwrap().is_some());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key5).unwrap().is_some());

        let _ = check_ttl_and_compact_files(&kvdb, b"zr\0key2", b"zr\0key6", false);
        assert!(kvdb.get_value_cf(CF_DEFAULT, key1).unwrap().is_none());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key2).unwrap().is_some());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key3).unwrap().is_none());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key4).unwrap().is_some());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key5).unwrap().is_none());
    }
    inner(ApiVersion::V1ttl);
    inner(ApiVersion::V2);
}

#[test]
fn test_ttl_compaction_filter() {
    fn inner(api_version: ApiVersion) {
        fail::cfg("ttl_current_ts", "return(100)").unwrap();
        let mut cfg = DbConfig::default();
        cfg.writecf.disable_auto_compactions = true;
        let dir = tempfile::TempDir::new().unwrap();
        let builder = TestEngineBuilder::new()
            .path(dir.path())
            .api_version(api_version);
        let engine = builder.build_with_cfg(&cfg).unwrap();
        let kvdb = engine.get_rocksdb();

        let key1 = b"zr\0key1";
        let value1 = RawValue {
            user_value: vec![0; 10],
            expire_ts: Some(10),
        };
        kvdb.put_cf(CF_DEFAULT, key1, &value1.to_bytes(api_version))
            .unwrap();
        kvdb.flush_cf(CF_DEFAULT, true).unwrap();

        let db = kvdb.as_inner();
        let handle = get_cf_handle(db, CF_DEFAULT).unwrap();
        db.compact_range_cf_opt(handle, &CompactOptions::new(), None, None);

        assert!(kvdb.get_value_cf(CF_DEFAULT, key1).unwrap().is_none());

        let key2 = b"zr\0key2";
        let value2 = RawValue {
            user_value: vec![0; 10],
            expire_ts: Some(120),
        };
        kvdb.put_cf(CF_DEFAULT, key2, &value2.to_bytes(api_version))
            .unwrap();
        let key3 = b"zr\0key3";
        let value3 = RawValue {
            user_value: vec![0; 10],
            expire_ts: Some(20),
        };
        kvdb.put_cf(CF_DEFAULT, key3, &value3.to_bytes(api_version))
            .unwrap();
        kvdb.flush_cf(CF_DEFAULT, true).unwrap();

        let key4 = b"zr\0key4";
        let value4 = RawValue {
            user_value: vec![0; 10],
            expire_ts: None,
        };
        kvdb.put_cf(CF_DEFAULT, key4, &value4.to_bytes(api_version))
            .unwrap();
        kvdb.flush_cf(CF_DEFAULT, true).unwrap();

        db.compact_range_cf_opt(handle, &CompactOptions::new(), None, None);
        assert!(kvdb.get_value_cf(CF_DEFAULT, key2).unwrap().is_some());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key3).unwrap().is_none());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key4).unwrap().is_some());
    }
    inner(ApiVersion::V1ttl);
    inner(ApiVersion::V2);
}
