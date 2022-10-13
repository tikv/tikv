// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use engine_rocks::{
    raw::IngestExternalFileOptions, RocksEngine, RocksSnapshot, RocksSstWriterBuilder,
};
use engine_test::new_temp_engine;
use engine_traits::{
    CfOptionsExt, CompactExt, DeleteStrategy, Engines, KvEngine, MiscExt, Range, SstWriter,
    SstWriterBuilder, SyncMutable, CF_DEFAULT, CF_WRITE,
};
use keys::data_key;
use kvproto::metapb::{Peer, Region};
use raftstore::store::{apply_sst_cf_file, build_sst_cf_file_list, CfFile, RegionSnapshot};
use tempfile::Builder;
use test_raftstore::*;
use tikv::{
    config::TikvConfig,
    storage::{mvcc::ScannerBuilder, txn::Scanner},
};
use tikv_util::{
    config::{ReadableDuration, ReadableSize},
    time::Limiter,
};
use txn_types::{Key, Write, WriteType};

#[test]
fn test_turnoff_titan() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.rocksdb.defaultcf.disable_auto_compactions = true;
    cluster.cfg.rocksdb.defaultcf.num_levels = 1;
    configure_for_enable_titan(&mut cluster, ReadableSize::kb(0));
    cluster.run();
    assert_eq!(cluster.must_get(b"k1"), None);

    let size = 5;
    for i in 0..size {
        cluster
            .put(
                format!("k{:02}0", i).as_bytes(),
                format!("v{}", i).as_bytes(),
            )
            .unwrap();
    }
    cluster.must_flush_cf(CF_DEFAULT, true);
    for i in 0..size {
        cluster
            .put(
                format!("k{:02}1", i).as_bytes(),
                format!("v{}", i).as_bytes(),
            )
            .unwrap();
    }
    cluster.must_flush_cf(CF_DEFAULT, true);
    for i in cluster.get_node_ids().into_iter() {
        let engine = cluster.get_engine(i);
        let db = engine.as_inner();
        assert_eq!(
            db.get_property_int("rocksdb.num-files-at-level0").unwrap(),
            2
        );
        assert_eq!(
            db.get_property_int("rocksdb.num-files-at-level1").unwrap(),
            0
        );
        assert_eq!(
            db.get_property_int("rocksdb.titandb.num-live-blob-file")
                .unwrap(),
            2
        );
        assert_eq!(
            db.get_property_int("rocksdb.titandb.num-obsolete-blob-file")
                .unwrap(),
            0
        );
    }
    cluster.shutdown();

    // try reopen db when titan isn't properly turned off.
    configure_for_disable_titan(&mut cluster);
    cluster.pre_start_check().unwrap_err();

    configure_for_enable_titan(&mut cluster, ReadableSize::kb(0));
    cluster.pre_start_check().unwrap();
    cluster.start().unwrap();
    assert_eq!(cluster.must_get(b"k1"), None);
    for i in cluster.get_node_ids().into_iter() {
        let db = cluster.get_engine(i);
        let opt = vec![("blob_run_mode", "kFallback")];
        db.set_options_cf(CF_DEFAULT, &opt).unwrap();
    }
    cluster.compact_data();
    let mut all_check_pass = true;
    for _ in 0..10 {
        // wait for gc completes.
        sleep_ms(10);
        all_check_pass = true;
        for i in cluster.get_node_ids().into_iter() {
            let engine = cluster.get_engine(i);
            let db = engine.as_inner();
            if db.get_property_int("rocksdb.num-files-at-level0").unwrap() != 0 {
                all_check_pass = false;
                break;
            }
            if db.get_property_int("rocksdb.num-files-at-level1").unwrap() != 1 {
                all_check_pass = false;
                break;
            }
            if db
                .get_property_int("rocksdb.titandb.num-live-blob-file")
                .unwrap()
                != 0
            {
                all_check_pass = false;
                break;
            }
        }
        if all_check_pass {
            break;
        }
    }
    if !all_check_pass {
        panic!("unexpected titan gc results");
    }
    cluster.shutdown();

    configure_for_disable_titan(&mut cluster);
    // wait till files are purged, timeout set to purge_obsolete_files_period.
    for _ in 1..100 {
        sleep_ms(10);
        if cluster.pre_start_check().is_ok() {
            return;
        }
    }
    cluster.pre_start_check().unwrap();
}

#[test]
fn test_delete_files_in_range_for_titan() {
    let path = Builder::new()
        .prefix("test-titan-delete-files-in-range")
        .tempdir()
        .unwrap();

    // Set configs and create engines
    let mut cfg = TikvConfig::default();
    let cache = cfg.storage.block_cache.build_shared_cache();
    cfg.rocksdb.titan.enabled = true;
    cfg.rocksdb.titan.disable_gc = true;
    cfg.rocksdb.titan.purge_obsolete_files_period = ReadableDuration::secs(1);
    cfg.rocksdb.defaultcf.disable_auto_compactions = true;
    // Disable dynamic_level_bytes, otherwise SST files would be ingested to L0.
    cfg.rocksdb.defaultcf.dynamic_level_bytes = false;
    cfg.rocksdb.defaultcf.titan.min_gc_batch_size = ReadableSize(0);
    cfg.rocksdb.defaultcf.titan.discardable_ratio = 0.4;
    cfg.rocksdb.defaultcf.titan.min_blob_size = ReadableSize(0);
    let kv_db_opts = cfg.rocksdb.build_opt();
    let kv_cfs_opts = cfg
        .rocksdb
        .build_cf_opts(&cache, None, cfg.storage.api_version());

    let raft_path = path.path().join(Path::new("titan"));
    let engines = Engines::new(
        engine_rocks::util::new_engine_opt(path.path().to_str().unwrap(), kv_db_opts, kv_cfs_opts)
            .unwrap(),
        engine_rocks::util::new_engine(raft_path.to_str().unwrap(), &[CF_DEFAULT]).unwrap(),
    );

    // Write some mvcc keys and values into db
    // default_cf : a_7, b_7
    // write_cf : a_8, b_8
    let start_ts = 7.into();
    let commit_ts = 8.into();
    let write = Write::new(WriteType::Put, start_ts, None);
    engines
        .kv
        .put_cf(
            CF_DEFAULT,
            &data_key(Key::from_raw(b"a").append_ts(start_ts).as_encoded()),
            b"a_value",
        )
        .unwrap();
    engines
        .kv
        .put_cf(
            CF_WRITE,
            &data_key(Key::from_raw(b"a").append_ts(commit_ts).as_encoded()),
            &write.as_ref().to_bytes(),
        )
        .unwrap();
    engines
        .kv
        .put_cf(
            CF_DEFAULT,
            &data_key(Key::from_raw(b"b").append_ts(start_ts).as_encoded()),
            b"b_value",
        )
        .unwrap();
    engines
        .kv
        .put_cf(
            CF_WRITE,
            &data_key(Key::from_raw(b"b").append_ts(commit_ts).as_encoded()),
            &write.as_ref().to_bytes(),
        )
        .unwrap();

    // Flush and compact the kvs into L6.
    engines.kv.flush_cfs(true).unwrap();
    engines.kv.compact_files_in_range(None, None, None).unwrap();
    let db = engines.kv.as_inner();
    let value = db.get_property_int("rocksdb.num-files-at-level0").unwrap();
    assert_eq!(value, 0);
    let value = db.get_property_int("rocksdb.num-files-at-level6").unwrap();
    assert_eq!(value, 1);

    // Delete one mvcc kvs we have written above.
    // Here we make the kvs on the L5 by ingesting SST.
    let sst_file_path = Path::new(db.path()).join("for_ingest.sst");
    let mut writer = RocksSstWriterBuilder::new()
        .build(sst_file_path.to_str().unwrap())
        .unwrap();
    writer
        .delete(&data_key(
            Key::from_raw(b"a").append_ts(start_ts).as_encoded(),
        ))
        .unwrap();
    writer.finish().unwrap();
    let mut opts = IngestExternalFileOptions::new();
    opts.move_files(true);
    let cf_default = db.cf_handle(CF_DEFAULT).unwrap();
    db.ingest_external_file_cf(cf_default, &opts, &[sst_file_path.to_str().unwrap()])
        .unwrap();

    // Now the LSM structure of default cf is:
    // L5: [delete(a_7)]
    // L6: [put(a_7, blob1), put(b_7, blob1)]
    // the ranges of two SST files are overlapped.
    //
    // There is one blob file in Titan
    // blob1: (a_7, a_value), (b_7, b_value)
    let value = db.get_property_int("rocksdb.num-files-at-level0").unwrap();
    assert_eq!(value, 0);
    let value = db.get_property_int("rocksdb.num-files-at-level5").unwrap();
    assert_eq!(value, 1);
    let value = db.get_property_int("rocksdb.num-files-at-level6").unwrap();
    assert_eq!(value, 1);

    // Used to trigger titan gc
    let engine = &engines.kv;
    engine.put(b"1", b"1").unwrap();
    engine.flush_cfs(true).unwrap();
    engine.put(b"2", b"2").unwrap();
    engine.flush_cfs(true).unwrap();
    engine
        .compact_files_in_range(Some(b"0"), Some(b"3"), Some(1))
        .unwrap();

    // Now the LSM structure of default cf is:
    // memtable: [put(b_7, blob4)] (because of Titan GC)
    // L0: [put(1, blob2), put(2, blob3)]
    // L5: [delete(a_7)]
    // L6: [put(a_7, blob1), put(b_7, blob1)]
    // the ranges of two SST files are overlapped.
    //
    // There is four blob files in Titan
    // blob1: (a_7, a_value), (b_7, b_value)
    // blob2: (1, 1)
    // blob3: (2, 2)
    // blob4: (b_7, b_value)
    let db = engine.as_inner();
    let value = db.get_property_int("rocksdb.num-files-at-level0").unwrap();
    assert_eq!(value, 0);
    let value = db.get_property_int("rocksdb.num-files-at-level1").unwrap();
    assert_eq!(value, 1);
    let value = db.get_property_int("rocksdb.num-files-at-level5").unwrap();
    assert_eq!(value, 1);
    let value = db.get_property_int("rocksdb.num-files-at-level6").unwrap();
    assert_eq!(value, 1);

    // Wait Titan to purge obsolete files
    thread::sleep(Duration::from_secs(2));
    // Now the LSM structure of default cf is:
    // memtable: [put(b_7, blob4)] (because of Titan GC)
    // L0: [put(1, blob2), put(2, blob3)]
    // L5: [delete(a_7)]
    // L6: [put(a_7, blob1), put(b_7, blob1)]
    // the ranges of two SST files are overlapped.
    //
    // There is three blob files in Titan
    // blob2: (1, 1)
    // blob3: (2, 2)
    // blob4: (b_7, b_value)

    // `delete_files_in_range` may expose some old keys.
    // For Titan it may encounter `missing blob file` in `delete_ranges_cfs`,
    // so we set key_only for Titan.
    engines
        .kv
        .delete_ranges_cfs(
            DeleteStrategy::DeleteFiles,
            &[Range::new(
                &data_key(Key::from_raw(b"a").as_encoded()),
                &data_key(Key::from_raw(b"b").as_encoded()),
            )],
        )
        .unwrap();
    engines
        .kv
        .delete_ranges_cfs(
            DeleteStrategy::DeleteByKey,
            &[Range::new(
                &data_key(Key::from_raw(b"a").as_encoded()),
                &data_key(Key::from_raw(b"b").as_encoded()),
            )],
        )
        .unwrap();
    engines
        .kv
        .delete_ranges_cfs(
            DeleteStrategy::DeleteBlobs,
            &[Range::new(
                &data_key(Key::from_raw(b"a").as_encoded()),
                &data_key(Key::from_raw(b"b").as_encoded()),
            )],
        )
        .unwrap();

    // Now the LSM structure of default cf is:
    // memtable: [put(b_7, blob4)] (because of Titan GC)
    // L0: [put(1, blob2), put(2, blob3)]
    // L6: [put(a_7, blob1), put(b_7, blob1)]
    // the ranges of two SST files are overlapped.
    //
    // There is three blob files in Titan
    // blob2: (1, 1)
    // blob3: (2, 2)
    // blob4: (b_7, b_value)
    let value = db.get_property_int("rocksdb.num-files-at-level0").unwrap();
    assert_eq!(value, 0);
    let value = db.get_property_int("rocksdb.num-files-at-level1").unwrap();
    assert_eq!(value, 1);
    let value = db.get_property_int("rocksdb.num-files-at-level5").unwrap();
    assert_eq!(value, 0);
    let value = db.get_property_int("rocksdb.num-files-at-level6").unwrap();
    assert_eq!(value, 1);

    // Generate a snapshot
    let limiter = Limiter::new(f64::INFINITY);
    let mut cf_file = CfFile::new(
        CF_DEFAULT,
        PathBuf::from(path.path().to_str().unwrap()),
        "default".to_string(),
        ".sst".to_string(),
    );
    build_sst_cf_file_list::<RocksEngine>(
        &mut cf_file,
        &engines.kv,
        &engines.kv.snapshot(),
        b"",
        b"{",
        u64::MAX,
        &limiter,
    )
    .unwrap();
    let mut cf_file_write = CfFile::new(
        CF_WRITE,
        PathBuf::from(path.path().to_str().unwrap()),
        "write".to_string(),
        ".sst".to_string(),
    );
    build_sst_cf_file_list::<RocksEngine>(
        &mut cf_file_write,
        &engines.kv,
        &engines.kv.snapshot(),
        b"",
        b"{",
        u64::MAX,
        &limiter,
    )
    .unwrap();

    // Apply the snapshot to other DB.
    let dir1 = Builder::new()
        .prefix("test-snap-cf-db-apply")
        .tempdir()
        .unwrap();
    let engines1 = new_temp_engine(&dir1);
    let tmp_file_paths = cf_file.tmp_file_paths();
    let tmp_file_paths = tmp_file_paths
        .iter()
        .map(|s| s.as_str())
        .collect::<Vec<&str>>();
    apply_sst_cf_file(&tmp_file_paths, &engines1.kv, CF_DEFAULT).unwrap();
    let tmp_file_paths = cf_file_write.tmp_file_paths();
    let tmp_file_paths = tmp_file_paths
        .iter()
        .map(|s| s.as_str())
        .collect::<Vec<&str>>();
    apply_sst_cf_file(&tmp_file_paths, &engines1.kv, CF_WRITE).unwrap();

    // Do scan on other DB.
    let mut r = Region::default();
    r.mut_peers().push(Peer::default());
    r.set_start_key(b"a".to_vec());
    r.set_end_key(b"z".to_vec());
    let snapshot = RegionSnapshot::<RocksSnapshot>::from_raw(engines1.kv, r);
    let mut scanner = ScannerBuilder::new(snapshot, 10.into())
        .range(Some(Key::from_raw(b"a")), None)
        .build()
        .unwrap();
    assert_eq!(
        scanner.next().unwrap(),
        Some((Key::from_raw(b"b"), b"b_value".to_vec())),
    );
}
