// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crossbeam::channel::{unbounded, Receiver};
use engine_rocks::{self, RocksEngine};
use engine_traits::{Iterable, Iterator, RaftEngine, RaftEngineReadOnly, RaftLogBatch, SeekKey};
use kvproto::raft_serverpb::RaftLocalState;
use protobuf::Message;
use raft::eraftpb::Entry;
use raft_log_engine::RaftLogEngine;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const BATCH_THRESHOLD: usize = 32 * 1024;

<<<<<<< HEAD
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
        engine.clean(id, &state, &mut batch_to_clean)?;
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
=======
pub fn dump_raftdb_to_raft_engine(source: &RocksEngine, target: &RaftLogEngine, threads: usize) {
    check_raft_engine_is_empty(target);
>>>>>>> 6291ebf37... server: disallow coexist of raft engine and raftdb (#12047)

    let count_size = Arc::new(AtomicUsize::new(0));
    let mut count_region = 0;
    let mut workers = vec![];
    let (tx, rx) = unbounded();
    for _ in 0..threads {
        let source = source.clone();
        let target = target.clone();
        let count_size = count_size.clone();
        let rx = rx.clone();
        let t = std::thread::spawn(move || {
            run_dump_raftdb_worker(&rx, &source, &target, &count_size);
        });
        workers.push(t);
    }

    info!("Start to scan raft log from RocksEngine and dump into RaftLogEngine");
    let consumed_time = tikv_util::time::Instant::now();
    // Seek all region id from raftdb and send them to workers.
    let mut it = source.iterator().unwrap();
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
    for t in workers {
        t.join().unwrap();
    }
    target.sync().unwrap();
    info!(
        "Finished dump, total regions: {}; Total bytes: {}; Consumed time: {:?}",
        count_region,
        count_size.load(Ordering::Relaxed),
        consumed_time.saturating_elapsed(),
    );
}

pub fn dump_raft_engine_to_raftdb(source: &RaftLogEngine, target: &RocksEngine, threads: usize) {
    check_raft_db_is_empty(target);

    let count_size = Arc::new(AtomicUsize::new(0));
    let mut count_region = 0;
    let mut workers = vec![];
    let (tx, rx) = unbounded();
    for _ in 0..threads {
        let source = source.clone();
        let target = target.clone();
        let count_size = count_size.clone();
        let rx = rx.clone();
        let t = std::thread::spawn(move || {
            run_dump_raft_engine_worker(&rx, &source, &target, &count_size);
        });
        workers.push(t);
    }

    info!("Start to scan raft log from RaftLogEngine and dump into RocksEngine");
    let consumed_time = tikv_util::time::Instant::now();
    // Seek all region id from RaftLogEngine and send them to workers.
    for id in source.raft_groups() {
        tx.send(id).unwrap();
        count_region += 1;
    }
    drop(tx);

    info!("Scanned all region id and waiting for dump");
    for t in workers {
        t.join().unwrap();
    }
    target.sync().unwrap();
    info!(
        "Finished dump, total regions: {}; Total bytes: {}; Consumed time: {:?}",
        count_region,
        count_size.load(Ordering::Relaxed),
        consumed_time.saturating_elapsed(),
    );
}

fn check_raft_engine_is_empty(engine: &RaftLogEngine) {
    assert!(
        engine.raft_groups().is_empty(),
        "Cannot transfer data from RaftDb to non-empty Raft Engine."
    );
}

fn check_raft_db_is_empty(engine: &RocksEngine) {
    let mut count = 0;
    engine
        .scan(b"", &[0xFF, 0xFF], false, |_, _| {
            count += 1;
            Ok(false)
        })
        .unwrap();
    assert_eq!(
        count, 0,
        "Cannot transfer data from Raft Engine to non-empty RaftDB."
    );
}

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
                let end = std::cmp::min(begin + 1024, last_index + 1);
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
    use tikv::config::TiKvConfig;

    fn do_test_switch(custom_raft_db_wal: bool) {
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

        // Dump logs from RocksEngine to RaftLogEngine.
        let raft_engine = RaftLogEngine::new(
            cfg.raft_engine.config(),
            None, /*key_manager*/
            None, /*io_rate_limiter*/
        )
        .expect("open raft engine");

        {
            // Prepare some data for the RocksEngine.
            let raftdb = engine_rocks::raw_util::new_engine_opt(
                &cfg.raft_store.raftdb_path,
                cfg.raftdb.build_opt(),
                cfg.raftdb.build_cf_opts(&None),
            )
            .unwrap();
            let raftdb = RocksEngine::from_db(Arc::new(raftdb));
            let mut batch = raftdb.log_batch(0);
            set_write_batch(1, &mut batch);
            raftdb.consume(&mut batch, false).unwrap();
            set_write_batch(5, &mut batch);
            raftdb.consume(&mut batch, false).unwrap();
            set_write_batch(15, &mut batch);
            raftdb.consume(&mut batch, false).unwrap();
            raftdb.sync().unwrap();

            dump_raftdb_to_raft_engine(&raftdb, &raft_engine, 4);
            assert(1, &raft_engine);
            assert(5, &raft_engine);
            assert(15, &raft_engine);
        }

        // Remove old raftdb.
        std::fs::remove_dir_all(&cfg.raft_store.raftdb_path).unwrap();

        // Dump logs from RaftLogEngine to RocksEngine.
        let raftdb = {
            let db = engine_rocks::raw_util::new_engine_opt(
                &cfg.raft_store.raftdb_path,
                DBOptions::new(),
                vec![],
            )
            .unwrap();
            RocksEngine::from_db(Arc::new(db))
        };
        dump_raft_engine_to_raftdb(&raft_engine, &raftdb, 4);
        assert(1, &raftdb);
        assert(5, &raftdb);
        assert(15, &raftdb);
    }

    #[test]
    fn test_switch() {
        do_test_switch(false);
    }

    #[test]
    fn test_switch_with_seperate_wal() {
        do_test_switch(true);
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
}
