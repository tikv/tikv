// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fs::ReadDir,
    path::{Path, PathBuf},
    sync::Arc,
};

use raft_engine::{copy::minimum_copy, env::DefaultFileSystem};
use regex::Regex;
use tikv::config::TikvConfig;

const SNAP_NAMES: &str = "^.+\\.(meta|sst)$";
const ROCKSDB_WALS: &str = "^([0-9]+).log$";

/// Remove `path` if it exists, and re-create the directory.
pub fn remove_and_recreate_dir<P: AsRef<Path>>(path: P) -> Result<(), String> {
    let path = path.as_ref();
    if let Err(e) = std::fs::remove_dir_all(path) {
        if e.kind() != std::io::ErrorKind::NotFound {
            return Err(format!("remove_dir_all({}): {}", path.display(), e));
        }
    }
    create_dir(path)
}

/// Symlink snapshot files from the original TiKV instance (specified by
/// `config`) to `agent_dir`. TiKV may try to change snapshot files, which can
/// be avoid by setting `raftstore::store::config::snap_apply_copy_symlink` to
/// `true`.
pub fn symlink_snaps(config: &TikvConfig, agent_dir: &str) -> Result<(), String> {
    let mut src = PathBuf::from(&config.storage.data_dir);
    src.push("snap");
    let mut dst = PathBuf::from(agent_dir);
    dst.push("snap");
    create_dir(&dst)?;

    let ptn = Regex::new(SNAP_NAMES).unwrap();
    symlink_stuffs(&src, &dst, |name| -> bool { ptn.is_match(name) })
}

/// Symlink or copy KV engine files from the original TiKV instance (specified
/// by `config`) to `agent_dir`. Then `agent_dir` can be used to run a new TiKV
/// instance, without any modifications on the original TiKV data.
pub fn dup_kv_engine_files(config: &TikvConfig, agent_dir: &str) -> Result<(), String> {
    let mut dst_config = TikvConfig::default();
    dst_config.storage.data_dir = agent_dir.to_owned();
    let dst = dst_config.infer_kv_engine_path(None).unwrap();
    create_dir(&dst)?;

    let src = config.infer_kv_engine_path(None).unwrap();
    symlink_stuffs(&src, &dst, |name| -> bool { name != "LOCK" })?;

    if !config.rocksdb.wal_dir.is_empty() {
        symlink_stuffs(&config.rocksdb.wal_dir, &dst, |_| -> bool { true })?;
        replace_symlink_with_copy(&config.rocksdb.wal_dir, &dst, rocksdb_files_should_copy)?;
    } else {
        replace_symlink_with_copy(&src, &dst, rocksdb_files_should_copy)?;
    }

    Ok(())
}

/// Symlink or copy Raft engine files from the original TiKV instance (specified
/// by `config`) to `agent_dir`. Then `agent_dir` can be used to run a new TiKV
/// instance, without any modifications on the original TiKV data.
pub fn dup_raft_engine_files(config: &TikvConfig, agent_dir: &str) -> Result<(), String> {
    let mut dst_config = TikvConfig::default();
    dst_config.storage.data_dir = agent_dir.to_owned();

    if config.raft_engine.enable {
        let dst = dst_config.infer_raft_engine_path(None).unwrap();
        // NOTE: it's ok to used `DefaultFileSystem` whatever the original instance
        // is encrypted or not because only `open` is used in `minimum_copy`. Seems
        // this behavior will never be changed, however we can custom a file system
        // which panics in all other calls later.
        let fs = Arc::new(DefaultFileSystem);
        minimum_copy(&config.raft_engine.config(), fs, dst)?;
    } else {
        let dst = dst_config.infer_raft_db_path(None).unwrap();
        create_dir(&dst)?;
        let src = config.infer_raft_db_path(None).unwrap();
        symlink_stuffs(&src, &dst, |name| -> bool { name != "LOCK" })?;

        if !config.raftdb.wal_dir.is_empty() {
            symlink_stuffs(&config.raftdb.wal_dir, &dst, |_| -> bool { true })?;
            replace_symlink_with_copy(&config.raftdb.wal_dir, &dst, rocksdb_files_should_copy)?;
        } else {
            replace_symlink_with_copy(&src, &dst, rocksdb_files_should_copy)?;
        }
    }

    Ok(())
}

fn symlink_stuffs<P, Q, F>(src: P, dst: Q, selector: F) -> Result<(), String>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
    F: Fn(&str) -> bool,
{
    let src = src.as_ref();
    let dst = dst.as_ref();
    for entry in read_dir(src)? {
        let entry = entry.map_err(|e| format!("dir_entry: {}", e))?;
        let fname = entry.file_name().to_str().unwrap().to_owned();
        if selector(&fname) {
            let src = entry.path().canonicalize().unwrap();
            let dst = PathBuf::from(dst).join(fname);
            symlink(src, dst)?;
        }
    }
    Ok(())
}

fn replace_symlink_with_copy<F>(src: &str, dst: &str, selector: F) -> Result<(), String>
where
    F: Fn(&mut dyn Iterator<Item = String>) -> Vec<String>,
{
    let mut names = Vec::new();
    for entry in read_dir(dst)? {
        let entry = entry.map_err(|e| format!("dir_entry: {}", e))?;
        let fname = entry.file_name().to_str().unwrap().to_owned();
        names.push(fname);
    }

    let src = PathBuf::from(src);
    let dst = PathBuf::from(dst);
    for name in (selector)(&mut names.into_iter()) {
        let src = src.join(&name);
        let dst = dst.join(&name);
        replace_file(src, &dst)?;
        add_write_permission(dst)?;
    }

    Ok(())
}

// `iter` emits all file names in a RocksDB instance. This function gets a list
// of files which should be copied instead of symlinked when building an agent
// directory.
//
// Q: so which files should be copied?
// A: the last one WAL file.
fn rocksdb_files_should_copy(iter: &mut dyn Iterator<Item = String>) -> Vec<String> {
    let ptn = Regex::new(ROCKSDB_WALS).unwrap();
    let mut names = Vec::new();
    for name in iter {
        if let Some(caps) = ptn.captures(&name) {
            let number = caps.get(1).unwrap().as_str();
            let number = number.parse::<u64>().unwrap();
            let name = caps.get(0).unwrap().as_str().to_owned();
            names.push((number, name));
        }
    }
    names.sort_by_key(|a| a.0);
    names.pop().map_or_else(|| vec![], |a| vec![a.1])
}

fn create_dir<P: AsRef<Path>>(path: P) -> Result<(), String> {
    let path = path.as_ref();
    std::fs::create_dir(path).map_err(|e| format!("create_dir({}): {}", path.display(), e))
}

fn read_dir<P: AsRef<Path>>(path: P) -> Result<ReadDir, String> {
    let path = path.as_ref();
    std::fs::read_dir(path).map_err(|e| format!("read_dir({}): {}", path.display(), e))
}

fn symlink<P: AsRef<Path>, Q: AsRef<Path>>(src: P, dst: Q) -> Result<(), String> {
    let src = src.as_ref();
    let dst = dst.as_ref();
    std::os::unix::fs::symlink(src, dst)
        .map_err(|e| format!("symlink({}, {}): {}", src.display(), dst.display(), e))
}

fn replace_file<P, Q>(src: P, dst: Q) -> Result<(), String>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    let src = src.as_ref();
    let dst = dst.as_ref();
    std::fs::remove_file(dst).map_err(|e| format!("remove_file({}): {}", dst.display(), e))?;
    std::fs::copy(src, dst)
        .map(|_| ())
        .map_err(|e| format!("copy({}, {}): {}", src.display(), dst.display(), e))
}

fn add_write_permission<P: AsRef<Path>>(path: P) -> Result<(), String> {
    let path = path.as_ref();
    let mut pmt = std::fs::metadata(path)
        .map_err(|e| format!("metadata({}): {}", path.display(), e))?
        .permissions();
    pmt.set_readonly(false);
    std::fs::set_permissions(path, pmt)
        .map_err(|e| format!("set_permissions({}): {}", path.display(), e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snap_exts() {
        let re = Regex::new(SNAP_NAMES).unwrap();
        for (s, matched) in [
            ("123.meta", true),
            ("123.sst", true),
            ("123.sst.tmp", false),
            ("123.sst.clone", false),
        ] {
            assert_eq!(re.is_match(s), matched);
        }
    }

    #[test]
    fn test_rocksdb_files_should_copy() {
        let mut names = [
            "00123.log",
            "00123.log.backup",
            "old.00123.log",
            "00123.sst",
            "001abc23.log",
            "LOCK",
        ]
        .iter()
        .map(|x| String::from(*x));
        let x = rocksdb_files_should_copy(&mut names);
        assert_eq!(x.len(), 1);
        assert_eq!(x[0], "00123.log");

        let mut names = ["87654321.log", "00123.log"]
            .iter()
            .map(|x| String::from(*x));
        let x = rocksdb_files_should_copy(&mut names);
        assert_eq!(x.len(), 1);
        assert_eq!(x[0], "87654321.log");
    }
}
