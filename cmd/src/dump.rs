use crossbeam::channel::{unbounded, Receiver};
use engine_rocks::{
    self,
    raw::{DBOptions, Env},
    RocksEngine,
};
use engine_traits::{Iterable, Iterator, RaftEngine, RaftLogBatch as RaftLogBatchTrait, SeekKey};
use kvproto::raft_serverpb::RaftLocalState;
use protobuf::Message;
use raft::eraftpb::Entry;
use raft_log_engine::RaftLogEngine;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const CONSUME_THRESHOLD: usize = 32 * 1024;

/// Check the potential original raftdb directory and try to dump data out.
///
/// Procedure:
///     1. Check whether the dump has been completed. If there is a dirty dir,
///        delete the original raftdb safely and return.
///     2. Scan and dump raft data into raft engine.
///     3. Rename original raftdb dir to indicate that dump operation is done.
///     4. Delete the original raftdb safely.
pub fn check_and_dump_raft_db(
    raftdb_path: &str,
    engine: &RaftLogEngine,
    env: Arc<Env>,
    thread_num: usize,
) {
    if !RocksEngine::exists(raftdb_path) {
        check_and_delete_safely(raftdb_path);
        return;
    }
    let mut opt = DBOptions::default();
    opt.set_env(env);
    let db = engine_rocks::raw_util::new_engine_opt(raftdb_path, opt, vec![])
        .unwrap_or_else(|s| fatal!("failed to create origin raft engine: {}", s));
    let origin_engine = RocksEngine::from_db(Arc::new(db));
    let count_size = Arc::new(AtomicUsize::new(0));
    let mut count_region = 0;
    let mut threads = vec![];
    let (tx, rx) = unbounded();
    for _ in 0..thread_num {
        let raft_engine = engine.clone();
        let raftdb_engine = origin_engine.clone();
        let count_size = count_size.clone();
        let rx = rx.clone();
        let t = std::thread::spawn(move || {
            run_worker(rx, raftdb_engine, raft_engine, count_size);
        });
        threads.push(t);
    }
    info!("Start to scan raft log from raftdb and dump into raft engine");
    let consumed_time = std::time::Instant::now();
    // Seek all region id from raftdb and send them to workers.
    let mut it = origin_engine.iterator().unwrap();
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
        consumed_time.elapsed(),
    );
    convert_to_dirty_raftdb(raftdb_path);
    check_and_delete_safely(raftdb_path);
}

// Worker receives region id and scan the related range.
fn run_worker(
    rx: Receiver<u64>,
    old_engine: RocksEngine,
    new_engine: RaftLogEngine,
    count_size: Arc<AtomicUsize>,
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
                                    entry.merge_from_bytes(&value)?;
                                    entries.push(entry);
                                }
                                keys::RAFT_STATE_SUFFIX => {
                                    let mut state = RaftLocalState::default();
                                    state.merge_from_bytes(&value)?;
                                    batch.put_raft_state(region_id, &state).unwrap();
                                    // Assume that we always scan entry first and raft state at the end.
                                    batch
                                        .append(region_id, std::mem::take(&mut entries))
                                        .unwrap();
                                }
                                _ => unreachable!("There is only 2 types of keys in raft"),
                            }
                            // Avoid long log batch.
                            if local_size >= CONSUME_THRESHOLD {
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

fn convert_to_dirty_raftdb(path: &str) {
    let mut dirty_path = get_dirty_raftdb(path);
    fs::rename(path, &dirty_path).unwrap();
    // fsync the parent directory to ensure that the rename is committed.
    dirty_path.pop();
    let dirty_dir = fs::File::open(&dirty_path).unwrap();
    dirty_dir.sync_all().unwrap();
    info!("Original raftdb has been converted into dirty dir");
}

fn check_and_delete_safely(path: &str) {
    let dirty_path = get_dirty_raftdb(path);
    if dirty_path.exists() && dirty_path.is_dir() {
        fs::remove_dir_all(dirty_path).expect("Cannot remove original raftdb dir");
        info!("Removed original raftdb dir");
    } else {
        info!("Original raftdb dir doesn't exist");
    }
}

fn get_dirty_raftdb(path: &str) -> PathBuf {
    let mut flag_path = PathBuf::from(path);
    flag_path.set_extension("REMOVE");
    flag_path
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_rocks::RocksWriteBatch;
    use raft_log_engine::RaftEngineConfig;
    use raft_log_engine::RaftLogEngine;

    #[test]
    fn test_dump() {
        let data_path = tempfile::Builder::new().tempdir().unwrap().into_path();
        let mut raftdb_path = data_path.clone();
        let mut raftengine_path = data_path;
        raftdb_path.push("raft");
        raftengine_path.push("raft-engine");
        {
            let db = engine_rocks::raw_util::new_engine_opt(
                raftdb_path.to_str().unwrap(),
                DBOptions::new(),
                vec![],
            )
            .unwrap_or_else(|s| fatal!("failed to create raft engine: {}", s));
            let raft_engine = RocksEngine::from_db(Arc::new(db));
            let mut batch = raft_engine.log_batch(0);
            set_write_batch(1, &mut batch);
            raft_engine.consume(&mut batch, true).unwrap();
            set_write_batch(5, &mut batch);
            raft_engine.consume(&mut batch, true).unwrap();
            set_write_batch(15, &mut batch);
            raft_engine.consume(&mut batch, true).unwrap();
            raft_engine.sync().unwrap();
        }
        // RaftEngine
        let mut raft_config = RaftEngineConfig::default();
        raft_config.dir = raftengine_path.to_str().unwrap().to_owned();
        let raft_engine = RaftLogEngine::new(raft_config);
        check_and_dump_raft_db(
            raftdb_path.to_str().unwrap(),
            &raft_engine,
            Arc::new(Env::default()),
            4,
        );
        assert(1, &raft_engine);
        assert(5, &raft_engine);
        assert(15, &raft_engine);
    }

    // Insert some data into log batch.
    fn set_write_batch(num: u64, batch: &mut RocksWriteBatch) {
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
    fn assert(num: u64, engine: &RaftLogEngine) {
        let state = engine.get_raft_state(num).unwrap().unwrap();
        assert_eq!(state.get_last_index(), num);
        for i in 0..num {
            let _entry = engine.get_entry(num, i).unwrap().unwrap();
        }
    }
}
