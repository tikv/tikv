// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fs::ReadDir,
    path::{Path, PathBuf},
    process,
    sync::Arc,
};

use encryption_export::data_key_manager_from_config;
use raft_engine::{env::DefaultFileSystem, Engine as RaftEngine};
use regex::Regex;
use tikv::config::TikvConfig;

pub const SYMLINK: &str = "symlink";
pub const COPY: &str = "copy";

pub fn run(config: &TikvConfig, agent_dir: &str, reuse_snaps: &str, reuse_rocksdb_files: &str) {
    if data_key_manager_from_config(&config.security.encryption, &config.storage.data_dir)
        .unwrap()
        .is_some()
    {
        eprintln!("reuse_redonly_remains with encryption enabled isn't expected");
        process::exit(-1);
    }

    if let Err(e) = create_dir(agent_dir) {
        eprintln!("create agent directory fail: {}", e);
        process::exit(-1);
    }
    println!("create agent directory success");

    if let Err(e) = dup_snaps(config, agent_dir, reuse_snaps == "symlink") {
        eprintln!("dup snapshot files fail: {}", e);
        process::exit(-1);
    }
    println!("dup snapshot files success");

    if let Err(e) = dup_kv_engine_files(config, agent_dir, reuse_rocksdb_files == "symlink") {
        eprintln!("dup kv engine files fail: {}", e);
        process::exit(-1);
    }
    println!("dup kv engine files success");

    if let Err(e) = dup_raft_engine_files(config, agent_dir, reuse_rocksdb_files == "symlink") {
        eprintln!("dup raft engine fail: {}", e);
        process::exit(-1);
    }
    println!("dup raft engine success");
}

const SNAP_NAMES: &str = "^.+\\.(meta|sst)$";
const ROCKSDB_WALS: &str = "^([0-9]+).log$";

/// Create a directory at `path`, return an error if exists.
fn create_dir<P: AsRef<Path>>(path: P) -> Result<(), String> {
    let path = path.as_ref();
    std::fs::create_dir(path).map_err(|e| format!("create_dir({}): {}", path.display(), e))
}

/// Symlink or copy snapshot files from the original TiKV instance (specified by
/// `config`) to `agent_dir`. TiKV may try to change snapshot files, which can
/// be avoid by setting `raftstore::store::config::snap_apply_copy_symlink` to
/// `true`.
fn dup_snaps(config: &TikvConfig, agent_dir: &str, use_symlink: bool) -> Result<(), String> {
    let mut src = PathBuf::from(&config.storage.data_dir);
    src.push("snap");
    let mut dst = PathBuf::from(agent_dir);
    dst.push("snap");
    create_dir(&dst)?;

    let ptn = Regex::new(SNAP_NAMES).unwrap();
    reuse_stuffs(
        &src,
        &dst,
        |name| -> bool { ptn.is_match(name) },
        use_symlink,
    )
}

/// Symlink or copy KV engine files from the original TiKV instance (specified
/// by `config`) to `agent_dir`. Then `agent_dir` can be used to run a new TiKV
/// instance, without any modifications on the original TiKV data.
// There are 3 types of files in RocksDB:
// * SST files, which won't be changed in any cases;
// * WAL files, which is named with a sequence ID; all won't be changed except
//   the last one.
// * Manifest files, which won't be changed in any cases.
fn dup_kv_engine_files(
    config: &TikvConfig,
    agent_dir: &str,
    use_symlink: bool,
) -> Result<(), String> {
    let mut dst_config = TikvConfig::default();
    dst_config.storage.data_dir = agent_dir.to_owned();
    let dst = dst_config.infer_kv_engine_path(None).unwrap();
    create_dir(&dst)?;

    // Firstly, dup all files except LOCK.
    let src = config.infer_kv_engine_path(None).unwrap();
    reuse_stuffs(&src, &dst, |name| -> bool { name != "LOCK" }, use_symlink)?;

    if !config.rocksdb.wal_dir.is_empty() {
        reuse_stuffs(
            &config.rocksdb.wal_dir,
            &dst,
            |_| -> bool { true },
            use_symlink,
        )?;
        if use_symlink {
            replace_symlink_with_copy(&config.rocksdb.wal_dir, &dst, rocksdb_files_should_copy)?;
        }
    } else if use_symlink {
        replace_symlink_with_copy(&src, &dst, rocksdb_files_should_copy)?;
    }

    Ok(())
}

/// Symlink or copy Raft engine files from the original TiKV instance (specified
/// by `config`) to `agent_dir`. Then `agent_dir` can be used to run a new TiKV
/// instance, without any modifications on the original TiKV data.
fn dup_raft_engine_files(
    config: &TikvConfig,
    agent_dir: &str,
    use_symlink: bool,
) -> Result<(), String> {
    let mut dst_config = TikvConfig::default();
    dst_config.storage.data_dir = agent_dir.to_owned();

    if config.raft_engine.enable {
        let dst = dst_config.infer_raft_engine_path(None).unwrap();
        let mut raft_engine_cfg = config.raft_engine.config();
        raft_engine_cfg.dir = config.infer_raft_engine_path(None).unwrap();
        // NOTE: it's ok to used `DefaultFileSystem` whatever the original instance is
        // encrypted or not because only `open` is used in `RaftEngine::fork`. Seems
        // this behavior will never be changed, however we can custom a file system
        // which panics in all other calls later.
        let details = RaftEngine::fork(&raft_engine_cfg, Arc::new(DefaultFileSystem), dst)?;
        for copied in &details.copied {
            add_write_permission(copied)?;
        }
    } else {
        let dst = dst_config.infer_raft_db_path(None).unwrap();
        create_dir(&dst)?;
        let src = config.infer_raft_db_path(None).unwrap();
        reuse_stuffs(&src, &dst, |name| -> bool { name != "LOCK" }, use_symlink)?;

        if !config.raftdb.wal_dir.is_empty() {
            reuse_stuffs(
                &config.raftdb.wal_dir,
                &dst,
                |_| -> bool { true },
                use_symlink,
            )?;
            if use_symlink {
                replace_symlink_with_copy(&config.raftdb.wal_dir, &dst, rocksdb_files_should_copy)?;
            }
        } else if use_symlink {
            replace_symlink_with_copy(&src, &dst, rocksdb_files_should_copy)?;
        }
    }

    Ok(())
}

fn reuse_stuffs<P, Q, F>(src: P, dst: Q, selector: F, use_symlink: bool) -> Result<(), String>
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
            if use_symlink {
                symlink(src, dst)?;
            } else {
                copy(src, dst)?;
            }
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

fn copy<P: AsRef<Path>, Q: AsRef<Path>>(src: P, dst: Q) -> Result<(), String> {
    let src = src.as_ref();
    let dst = dst.as_ref();
    std::fs::copy(src, dst)
        .map(|_| ())
        .map_err(|e| format!("copy({}, {}): {}", src.display(), dst.display(), e))
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
