// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::{Error, Result};

use std::fs;
use std::path::{Path, PathBuf};

/// Check if key in range [`start_key`, `end_key`).
#[allow(dead_code)]
pub fn check_key_in_range(
    key: &[u8],
    region_id: u64,
    start_key: &[u8],
    end_key: &[u8],
) -> Result<()> {
    if key >= start_key && (end_key.is_empty() || key < end_key) {
        Ok(())
    } else {
        Err(Error::NotInRange {
            key: key.to_vec(),
            region_id,
            start: start_key.to_vec(),
            end: end_key.to_vec(),
        })
    }
}

pub struct RaftDataStateMachine {
    source: PathBuf,
    source_staging: PathBuf,
    target: PathBuf,
    target_staging: PathBuf,
}

impl RaftDataStateMachine {
    pub fn new(source: &str, target: &str) -> Self {
        let source = PathBuf::from(source);
        let source_staging = source.with_extension("STAGING");
        let target = PathBuf::from(target);
        let target_staging = target.with_extension("STAGING");
        Self {
            source,
            source_staging,
            target,
            target_staging,
        }
    }

    /// Verifies if invariants still hold.
    pub fn validate(&self, should_exist: bool) -> std::result::Result<(), String> {
        if Self::data_exists(&self.source) && Self::data_exists(&self.target) {
            return Err(format!(
                "Found multiple raft data sets: {}, {}",
                self.source.display(),
                self.target.display()
            ));
        }
        if Self::data_exists(&self.source_staging) && Self::data_exists(&self.target_staging) {
            return Err(format!(
                "Found multiple raft data sets: {}, {}",
                self.source_staging.display(),
                self.target_staging.display()
            ));
        }
        let exists = Self::data_exists(&self.source_staging)
            || Self::data_exists(&self.target_staging)
            || Self::data_exists(&self.source)
            || Self::data_exists(&self.target);
        if exists != should_exist {
            if should_exist {
                return Err(format!("Cannot find raft data set.",));
            } else {
                return Err(format!("Found raft data set when it should not exist.",));
            }
        }
        Ok(())
    }

    pub fn before_open_target(&mut self) -> Option<PathBuf> {
        if Self::data_exists(&self.target_staging) {
            assert!(!Self::data_exists(&self.source_staging));
            Self::remove_dir_safe(&self.source);
            Self::rename_dir_safe(&self.target_staging, &self.target);
            return None;
        }
        if Self::data_exists(&self.source_staging) {
            Self::remove_dir_safe(&self.source);
        } else if Self::data_exists(&self.source) {
            Self::rename_dir_safe(&self.source, &self.source_staging);
        } else {
            return None;
        }
        Some(self.source_staging.clone())
    }

    pub fn after_dump_data(&mut self) {
        assert!(Self::data_exists(&self.target));
        assert!(!Self::data_exists(&self.source));
        Self::remove_dir_safe(&self.source_staging);
        Self::remove_dir_safe(&self.target_staging);
    }

    fn data_exists(path: &Path) -> bool {
        if !path.exists() || !path.is_dir() {
            return false;
        }
        fs::read_dir(&path).unwrap().next().is_some()
    }

    fn remove_dir_safe(path: &Path) {
        if path.exists() {
            let trash = path.with_extension("REMOVE");
            Self::rename_dir_safe(path, &trash);
            fs::remove_dir_all(&trash).unwrap();
        }
    }

    fn rename_dir_safe(from: &Path, to: &Path) {
        fs::rename(from, to).unwrap();
        let mut dir = to.to_path_buf();
        assert!(dir.pop());
        fs::File::open(&dir).and_then(|d| d.sync_all()).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

    #[test]
    fn test_raft_data_state_machine() {
        fn validate_restart(source: &Path, target: &Path, should_exist: bool) {
            let state =
                RaftDataStateMachine::new(source.to_str().unwrap(), target.to_str().unwrap());
            state.validate(should_exist).unwrap();
            let state =
                RaftDataStateMachine::new(target.to_str().unwrap(), source.to_str().unwrap());
            state.validate(should_exist).unwrap();
        }

        let root = tempfile::Builder::new().tempdir().unwrap().into_path();
        let mut source = root.clone();
        source.push("source");
        fs::create_dir_all(&source).unwrap();
        let mut target = root.clone();
        target.push("target");
        fs::create_dir_all(&target).unwrap();

        validate_restart(&source, &target, false);

        let mut state =
            RaftDataStateMachine::new(source.to_str().unwrap(), target.to_str().unwrap());
        // Write some data into source.
        let mut source_file = source.clone();
        source_file.push("file");
        File::create(&source_file).unwrap();
        validate_restart(&source, &target, true);

        // Dump to target.
        let new_source = state.before_open_target().unwrap();
        validate_restart(&source, &target, true);
        let mut new_source_file = new_source.clone();
        new_source_file.push("file");
        let mut target_file = target.clone();
        target_file.push("file");
        fs::copy(&new_source_file, &target_file).unwrap();
        validate_restart(&source, &target, true);
        fs::remove_file(&new_source_file).unwrap();
        validate_restart(&source, &target, true);
        state.after_dump_data();
        validate_restart(&source, &target, true);
    }
}
