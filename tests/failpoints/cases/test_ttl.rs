use engine_rocks::raw::CompactOptions;
use engine_rocks::util::get_cf_handle;
use engine_traits::util::{append_expire_ts, TEST_CURRENT_TS};
use engine_traits::{MiscExt, Peekable, SyncMutable, CF_DEFAULT};
use tikv::config::DbConfig;
use tikv::server::ttl::check_ttl_and_compact_files;
use tikv::storage::kv::TestEngineBuilder;

#[test]
fn test_ttl_checker() {
    fail::cfg("current_ts", "return(100)").unwrap();
    let mut cfg = DbConfig::default();
    cfg.defaultcf.disable_auto_compactions = true;
    let dir = tempfile::TempDir::new().unwrap();
    let builder = TestEngineBuilder::new().path(dir.path()).ttl(true);
    let engine = builder.build_with_cfg(&cfg).unwrap();

    let kvdb = engine.get_rocksdb();
    let key1 = b"zkey1";
    let mut value1 = vec![0; 10];
    append_expire_ts(&mut value1, 10);
    kvdb.put_cf(CF_DEFAULT, key1, &value1).unwrap();
    kvdb.flush_cf(CF_DEFAULT, true).unwrap();
    let key2 = b"zkey2";
    let mut value2 = vec![0; 10];
    append_expire_ts(&mut value2, TEST_CURRENT_TS + 20);
    kvdb.put_cf(CF_DEFAULT, key2, &value2).unwrap();
    let key3 = b"zkey3";
    let mut value3 = vec![0; 10];
    append_expire_ts(&mut value3, 20);
    kvdb.put_cf(CF_DEFAULT, key3, &value3).unwrap();
    kvdb.flush_cf(CF_DEFAULT, true).unwrap();
    let key4 = b"zkey4";
    let mut value4 = vec![0; 10];
    append_expire_ts(&mut value4, 0);
    kvdb.put_cf(CF_DEFAULT, key4, &value4).unwrap();
    kvdb.flush_cf(CF_DEFAULT, true).unwrap();
    let key5 = b"zkey5";
    let mut value5 = vec![0; 10];
    append_expire_ts(&mut value5, 10);
    kvdb.put_cf(CF_DEFAULT, key5, &value5).unwrap();
    kvdb.flush_cf(CF_DEFAULT, true).unwrap();

    assert!(kvdb.get_value_cf(CF_DEFAULT, key1).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key2).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key3).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key4).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key5).unwrap().is_some());

    let _ = check_ttl_and_compact_files(&kvdb, b"zkey1", b"zkey25", false);
    assert!(kvdb.get_value_cf(CF_DEFAULT, key1).unwrap().is_none());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key2).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key3).unwrap().is_none());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key4).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key5).unwrap().is_some());

    let _ = check_ttl_and_compact_files(&kvdb, b"zkey2", b"zkey6", false);
    assert!(kvdb.get_value_cf(CF_DEFAULT, key1).unwrap().is_none());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key2).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key3).unwrap().is_none());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key4).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key5).unwrap().is_none());
}

#[test]
fn test_ttl_compaction_filter() {
    fail::cfg("current_ts", "return(100)").unwrap();
    let mut cfg = DbConfig::default();
    cfg.writecf.disable_auto_compactions = true;
    let dir = tempfile::TempDir::new().unwrap();
    let builder = TestEngineBuilder::new().path(dir.path()).ttl(true);
    let engine = builder.build_with_cfg(&cfg).unwrap();
    let kvdb = engine.get_rocksdb();

    let key1 = b"zkey1";
    let mut value1 = vec![0; 10];
    append_expire_ts(&mut value1, 10);
    kvdb.put_cf(CF_DEFAULT, key1, &value1).unwrap();
    kvdb.flush_cf(CF_DEFAULT, true).unwrap();

    let db = kvdb.as_inner();
    let handle = get_cf_handle(db, CF_DEFAULT).unwrap();
    db.compact_range_cf_opt(handle, &CompactOptions::new(), None, None);

    assert!(kvdb.get_value_cf(CF_DEFAULT, key1).unwrap().is_none());

    let key2 = b"zkey2";
    let mut value2 = vec![0; 10];
    append_expire_ts(&mut value2, TEST_CURRENT_TS + 20);
    kvdb.put_cf(CF_DEFAULT, key2, &value2).unwrap();
    let key3 = b"zkey3";
    let mut value3 = vec![0; 10];
    append_expire_ts(&mut value3, 20);
    kvdb.put_cf(CF_DEFAULT, key3, &value3).unwrap();
    kvdb.flush_cf(CF_DEFAULT, true).unwrap();

    let key4 = b"zkey4";
    let mut value4 = vec![0; 10];
    append_expire_ts(&mut value4, 0);
    kvdb.put_cf(CF_DEFAULT, key4, &value4).unwrap();
    kvdb.flush_cf(CF_DEFAULT, true).unwrap();

    db.compact_range_cf_opt(handle, &CompactOptions::new(), None, None);
    assert!(kvdb.get_value_cf(CF_DEFAULT, key2).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key3).unwrap().is_none());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key4).unwrap().is_some());
}
