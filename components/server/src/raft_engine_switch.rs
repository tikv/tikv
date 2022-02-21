// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crossbeam::channel::{unbounded, Receiver};
use engine_rocks::{self, raw::Env, RocksEngine};
use engine_traits::{
    CompactExt, DeleteStrategy, Error as EngineError, Iterable, Iterator, MiscExt, RaftEngine,
    RaftEngineReadOnly, RaftLogBatch, Range, SeekKey,
};
use file_system::delete_dir_if_exist;
use kvproto::raft_serverpb::RaftLocalState;
use protobuf::Message;
use raft::eraftpb::Entry;
use raft_log_engine::RaftLogEngine;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::{cmp, fs};
use tikv::config::TiKvConfig;

const BATCH_THRESHOLD: usize = 32 * 1024;

fn get_path_for_remove(path: &str) -> PathBuf {
    let mut flag_path = PathBuf::from(path);
    flag_path.set_extension("REMOVE");
    flag_path
}

fn remove_tmp_dir<P: AsRef<Path>>(dir: P) {
    match delete_dir_if_exist(&dir) {
        Err(e) => warn!("Cannot remove {:?}: {}", dir.as_ref(), e),
        Ok(true) => info!("Remove {:?} success", dir.as_ref()),
        Ok(_) => {}
    }
}

fn rename_to_tmp_dir<P1: AsRef<Path>, P2: AsRef<Path>>(src: P1, dst: P2) {
    fs::rename(&src, &dst).unwrap();

    let mut dir = dst.as_ref().to_path_buf();
    assert!(dir.pop());
    fs::File::open(&dir).and_then(|d| d.sync_all()).unwrap();
    info!("Rename {:?} to {:?} correctly", src.as_ref(), dst.as_ref());
}

fn clear_raft_engine(engine: &RaftLogEngine) -> Result<(), EngineError> {
    let mut batch_to_clean = engine.log_batch(0);
    for id in engine.raft_groups() {
        let state = engine.get_raft_state(id)?.unwrap();
        engine.clean(id, 0, &state, &mut batch_to_clean)?;
    }
    engine.consume(&mut batch_to_clean, true).map(|_| ())
}

fn clear_raft_db(engine: &RocksEngine) -> Result<(), EngineError> {
    let start_key = keys::raft_log_key(0, 0);
    let end_key = keys::raft_log_key(u64::MAX, u64::MAX);
    engine.compact_range("default", Some(&start_key), Some(&end_key), false, 2)?;

    let range = Range::new(&start_key, &end_key);
    engine.delete_ranges_cf("default", DeleteStrategy::DeleteFiles, &[range])?;
    engine.sync()?;

    let mut count = 0;
    engine.scan(&start_key, &end_key, false, |_, _| {
        count += 1;
        Ok(true)
    })?;
    assert_eq!(count, 0);
    Ok(())
}

/// Check the potential original raftdb directory and try to dump data out.
///
/// Procedure:
///     1. Check whether the dump has been completed. If there is a dirty dir,
///        delete the original raftdb safely and return.
///     2. Scan and dump raft data into raft engine.
///     3. Rename original raftdb dir to indicate that dump operation is done.
///     4. Delete the original raftdb safely.
pub fn check_and_dump_raft_db(
    config: &TiKvConfig,
    engine: &RaftLogEngine,
    env: &Arc<Env>,
    thread_num: usize,
) {
    let raftdb_path = &config.raft_store.raftdb_path;
    let dirty_raftdb_path = get_path_for_remove(raftdb_path);
    if !RocksEngine::exists(raftdb_path) {
        remove_tmp_dir(&dirty_raftdb_path);
        return;
    }

    // Clean the target engine if it exists.
    clear_raft_engine(engine).expect("clear_raft_engine");

    let config_raftdb = &config.raftdb;
    let mut raft_db_opts = config_raftdb.build_opt();
    raft_db_opts.set_env(env.clone());
    let raft_db_cf_opts = config_raftdb.build_cf_opts(&None);
    let db = engine_rocks::raw_util::new_engine_opt(raftdb_path, raft_db_opts, raft_db_cf_opts)
        .unwrap_or_else(|s| fatal!("failed to create origin raft db: {}", s));
    let src_engine = RocksEngine::from_db(Arc::new(db));

    let count_size = Arc::new(AtomicUsize::new(0));
    let mut count_region = 0;
    let mut threads = vec![];
    let (tx, rx) = unbounded();
    for _ in 0..thread_num {
        let src_engine = src_engine.clone();
        let dst_engine = engine.clone();
        let count_size = count_size.clone();
        let rx = rx.clone();
        let t = std::thread::spawn(move || {
            run_dump_raftdb_worker(&rx, &src_engine, &dst_engine, &count_size);
        });
        threads.push(t);
    }

    info!("Start to scan raft log from RocksEngine and dump into RaftLogEngine");
    let consumed_time = tikv_util::time::Instant::now();
    // Seek all region id from raftdb and send them to workers.
    let mut it = src_engine.iterator().unwrap();
    let mut valid = it.seek(SeekKey::Key(keys::REGION_RAFT_MIN_KEY)).unwrap();
    while valid {
        match keys::decode_raft_key(it.key()) {
            Err(e) => {
                panic!("Error happened when decoding raft key: {}", e);
            }
            Ok((id, _)) => {
                tx.send(id).unwrap();
                count_region += 1;
                let next_key = keys::raft_log_prefix(id + 1);
                valid = it.seek(SeekKey::Key(&next_key)).unwrap();
            }
        }
    }
    drop(tx);
    info!("Scanned all region id and waiting for dump");
    for t in threads {
        t.join().unwrap();
    }
    engine.sync().unwrap();
    info!(
        "Finished dump, total regions: {}; Total bytes: {}; Consumed time: {:?}",
        count_region,
        count_size.load(Ordering::Relaxed),
        consumed_time.saturating_elapsed(),
    );

    rename_to_tmp_dir(&raftdb_path, &dirty_raftdb_path);
    remove_tmp_dir(&dirty_raftdb_path);
}

// Worker receives region id and scan the related range.
fn run_dump_raftdb_worker(
    rx: &Receiver<u64>,
    old_engine: &RocksEngine,
    new_engine: &RaftLogEngine,
    count_size: &Arc<AtomicUsize>,
) {
    let mut batch = new_engine.log_batch(0);
    let mut local_size = 0;
    while let Ok(id) = rx.recv() {
        let mut entries = vec![];
        old_engine
            .scan(
                &keys::raft_log_prefix(id),
                &keys::raft_log_prefix(id + 1),
                false,
                |key, value| {
                    let res = keys::decode_raft_key(key);
                    match res {
                        Err(_) => Ok(true),
                        Ok((region_id, suffix)) => {
                            local_size += value.len();
                            match suffix {
                                keys::RAFT_LOG_SUFFIX => {
                                    let mut entry = Entry::default();
                                    entry.merge_from_bytes(value)?;
                                    entries.push(entry);
                                }
                                keys::RAFT_STATE_SUFFIX => {
                                    let mut state = RaftLocalState::default();
                                    state.merge_from_bytes(value)?;
                                    batch.put_raft_state(region_id, &state).unwrap();
                                    // Assume that we always scan entry first and raft state at the end.
                                    batch
                                        .append(region_id, std::mem::take(&mut entries))
                                        .unwrap();
                                }
                                _ => unreachable!("There is only 2 types of keys in raft"),
                            }
                            // Avoid long log batch.
                            if local_size >= BATCH_THRESHOLD {
                                local_size = 0;
                                batch
                                    .append(region_id, std::mem::take(&mut entries))
                                    .unwrap();

                                let size = new_engine.consume(&mut batch, false).unwrap();
                                count_size.fetch_add(size, Ordering::Relaxed);
                            }
                            Ok(true)
                        }
                    }
                },
            )
            .unwrap();
    }
    let size = new_engine.consume(&mut batch, false).unwrap();
    count_size.fetch_add(size, Ordering::Relaxed);
}

pub fn check_and_dump_raft_engine(config: &TiKvConfig, engine: &RocksEngine, thread_num: usize) {
    let raft_engine_config = config.raft_engine.config();
    let raft_engine_path = &raft_engine_config.dir;

    let dirty_raft_engine_path = get_path_for_remove(raft_engine_path);
    if !RaftLogEngine::exists(raft_engine_path) {
        remove_tmp_dir(&dirty_raft_engine_path);
        return;
    }

    // Clean the target engine if it exists.
    clear_raft_db(engine).expect("clear_raft_db");

    let src_engine = RaftLogEngine::new(raft_engine_config.clone()).expect("open raft engine");

    let count_size = Arc::new(AtomicUsize::new(0));
    let mut count_region = 0;
    let mut threads = vec![];
    let (tx, rx) = unbounded();
    for _ in 0..thread_num {
        let src_engine = src_engine.clone();
        let dst_engine = engine.clone();
        let count_size = count_size.clone();
        let rx = rx.clone();
        let t = std::thread::spawn(move || {
            run_dump_raft_engine_worker(&rx, &src_engine, &dst_engine, &count_size);
        });
        threads.push(t);
    }

    info!("Start to scan raft log from RaftLogEngine and dump into RocksEngine");
    let consumed_time = tikv_util::time::Instant::now();
    // Seek all region id from RaftLogEngine and send them to workers.
    for id in src_engine.raft_groups() {
        tx.send(id).unwrap();
        count_region += 1;
    }
    drop(tx);

    info!("Scanned all region id and waiting for dump");
    for t in threads {
        t.join().unwrap();
    }
    engine.sync().unwrap();
    info!(
        "Finished dump, total regions: {}; Total bytes: {}; Consumed time: {:?}",
        count_region,
        count_size.load(Ordering::Relaxed),
        consumed_time.saturating_elapsed(),
    );

    rename_to_tmp_dir(&raft_engine_path, &dirty_raft_engine_path);
    remove_tmp_dir(&dirty_raft_engine_path);
}

fn run_dump_raft_engine_worker(
    rx: &Receiver<u64>,
    old_engine: &RaftLogEngine,
    new_engine: &RocksEngine,
    count_size: &Arc<AtomicUsize>,
) {
    while let Ok(id) = rx.recv() {
        let state = old_engine.get_raft_state(id).unwrap().unwrap();
        new_engine.put_raft_state(id, &state).unwrap();
        if let Some(last_index) = old_engine.last_index(id) {
            let mut batch = new_engine.log_batch(0);
            let mut begin = old_engine.first_index(id).unwrap();
            while begin <= last_index {
                let end = cmp::min(begin + 1024, last_index + 1);
                let mut entries = Vec::with_capacity((end - begin) as usize);
                begin += old_engine
                    .fetch_entries_to(id, begin, end, Some(BATCH_THRESHOLD), &mut entries)
                    .unwrap() as u64;
                batch.append(id, entries).unwrap();
                let size = new_engine.consume(&mut batch, false).unwrap();
                count_size.fetch_add(size, Ordering::Relaxed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_rocks::raw::DBOptions;

    fn do_test_switch(custom_raft_db_wal: bool, continue_on_aborted: bool) {
        let data_path = tempfile::Builder::new().tempdir().unwrap().into_path();
        let mut raftdb_path = data_path.clone();
        let mut raft_engine_path = data_path;
        let mut raftdb_wal_path = raftdb_path.clone();
        raftdb_path.push("raft");
        raft_engine_path.push("raft-engine");
        if custom_raft_db_wal {
            raftdb_wal_path.push("test-wal");
        }

        let mut cfg = TiKvConfig::default();
        cfg.raft_store.raftdb_path = raftdb_path.to_str().unwrap().to_owned();
        cfg.raftdb.wal_dir = raftdb_wal_path.to_str().unwrap().to_owned();
        cfg.raft_engine.mut_config().dir = raft_engine_path.to_str().unwrap().to_owned();

        // Prepare some data for the RocksEngine.
        {
            let db = engine_rocks::raw_util::new_engine_opt(
                &cfg.raft_store.raftdb_path,
                cfg.raftdb.build_opt(),
                cfg.raftdb.build_cf_opts(&None),
            )
            .unwrap();
            let engine = RocksEngine::from_db(Arc::new(db));
            let mut batch = engine.log_batch(0);
            set_write_batch(1, &mut batch);
            engine.consume(&mut batch, false).unwrap();
            set_write_batch(5, &mut batch);
            engine.consume(&mut batch, false).unwrap();
            set_write_batch(15, &mut batch);
            engine.consume(&mut batch, false).unwrap();
            engine.sync().unwrap();
        }

        // Dump logs from RocksEngine to RaftLogEngine.
        let raft_engine = RaftLogEngine::new(cfg.raft_engine.config()).expect("open raft engine");
        if continue_on_aborted {
            let mut batch = raft_engine.log_batch(0);
            set_write_batch(25, &mut batch);
            raft_engine.consume(&mut batch, false).unwrap();
            assert(25, &raft_engine);
        }

        check_and_dump_raft_db(&cfg, &raft_engine, &Arc::new(Env::default()), 4);
        assert(1, &raft_engine);
        assert(5, &raft_engine);
        assert(15, &raft_engine);
        assert_no(25, &raft_engine);
        drop(raft_engine);

        // Dump logs from RaftLogEngine to RocksEngine.
        let rocks_engine = {
            let db = engine_rocks::raw_util::new_engine_opt(
                &cfg.raft_store.raftdb_path,
                DBOptions::new(),
                vec![],
            )
            .unwrap();
            RocksEngine::from_db(Arc::new(db))
        };
        if continue_on_aborted {
            let mut batch = rocks_engine.log_batch(0);
            set_write_batch(25, &mut batch);
            rocks_engine.consume(&mut batch, false).unwrap();
            assert(25, &rocks_engine);
        }
        check_and_dump_raft_engine(&cfg, &rocks_engine, 4);
        assert(1, &rocks_engine);
        assert(5, &rocks_engine);
        assert(15, &rocks_engine);
        assert_no(25, &rocks_engine);
    }

    #[test]
    fn test_switch() {
        do_test_switch(false, false);
    }

    #[test]
    fn test_switch_with_seperate_wal() {
        do_test_switch(true, false);
    }

    #[test]
    fn test_switch_continue_on_aborted() {
        do_test_switch(false, true);
    }

    // Insert some data into log batch.
    fn set_write_batch<T: RaftLogBatch>(num: u64, batch: &mut T) {
        let mut state = RaftLocalState::default();
        state.set_last_index(num);
        batch.put_raft_state(num, &state).unwrap();
        let mut entries = vec![];
        for i in 0..num {
            let mut e = Entry::default();
            e.set_index(i);
            entries.push(e);
        }
        batch.append(num, entries).unwrap();
    }

    // Get data from raft engine and assert.
    fn assert<T: RaftEngine>(num: u64, engine: &T) {
        let state = engine.get_raft_state(num).unwrap().unwrap();
        assert_eq!(state.get_last_index(), num);
        for i in 0..num {
            assert!(engine.get_entry(num, i).unwrap().is_some());
        }
    }

    fn assert_no<T: RaftEngine>(num: u64, engine: &T) {
        assert!(engine.get_raft_state(num).unwrap().is_none());
        for i in 0..num {
            assert!(engine.get_entry(num, i).unwrap().is_none());
        }
    }
}
