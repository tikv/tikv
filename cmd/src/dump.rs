use engine_rocks::{
    self,
    raw::{DBOptions, Env},
    RocksEngine,
};
use engine_traits::{Iterable, RaftEngine, RaftLogBatch, CF_DEFAULT};
use kvproto::raft_serverpb::RaftLocalState;
use protobuf::Message;
use raft::eraftpb::Entry;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

/// Check the potential original raftdb directory and try to dump data out.
///
/// Procedure:
///     1. Check whether the dump has been completed. If there is a dirty dir,
///        delete the original raftdb safely and return.
///     2. Scan and dump raft data into raft engine.
///     3. Rename original raftdb dir to indicate that dump operation is done.
///     4. Delete the original raftdb safely.
pub fn check_and_dump_raft_db<E: RaftEngine>(raftdb_path: &str, engine: &E, env: Arc<Env>) {
    if !RocksEngine::exists(raftdb_path) {
        check_and_delete_safely(raftdb_path);
        return;
    }
    let mut opt = DBOptions::default();
    opt.set_env(env);
    let db = engine_rocks::raw_util::new_engine_opt(raftdb_path, opt, vec![])
        .unwrap_or_else(|s| fatal!("failed to create origin raft engine: {}", s));
    let origin_engine = RocksEngine::from_db(Arc::new(db));
    info!("Start to scan raft log from raftdb and dump into raft engine");
    let start_key = keys::REGION_RAFT_MIN_KEY;
    let end_key = keys::REGION_RAFT_MAX_KEY;
    let mut log_batch = engine.log_batch(0);
    let mut count_entry = 0;
    let mut count_size = 0;
    let mut count_region = 0;
    let consume_time = std::time::Instant::now();
    origin_engine
        .scan_cf(CF_DEFAULT, start_key, end_key, false, |key, value| {
            let res = keys::decode_raft_key(key);
            match res {
                Err(_) => Ok(true),
                Ok((region_id, suffix)) => {
                    match suffix {
                        keys::RAFT_LOG_SUFFIX => {
                            let mut entry = Entry::default();
                            entry.merge_from_bytes(&value)?;
                            log_batch.append(region_id, vec![entry]).unwrap();
                            count_entry += 1;
                        }
                        keys::RAFT_STATE_SUFFIX => {
                            let mut state = RaftLocalState::default();
                            state.merge_from_bytes(&value)?;
                            log_batch.put_raft_state(region_id, &state).unwrap();
                            count_region += 1;
                        }
                        // There is only 2 types of keys in raft.
                        _ => unreachable!(),
                    }
                    if count_entry % 1024 == 0 {
                        let size = engine.consume(&mut log_batch, true).unwrap();
                        count_size += size;
                    }
                    Ok(true)
                }
            }
        })
        .unwrap();
    let size = engine.consume(&mut log_batch, true).unwrap();
    engine.sync().unwrap();
    count_size += size;
    info!(
        "Finished dump, total regions: {}; Total bytes: {}; Consumed time: {:?}",
        count_region,
        count_size,
        consume_time.elapsed(),
    );
    convert_to_dirty_raftdb(raftdb_path);
    check_and_delete_safely(raftdb_path);
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
