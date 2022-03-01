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
        let source_staging = source.with_extension("staging");
        let target = PathBuf::from(target);
        let target_staging = target.with_extension("staging");
        Self {
            source,
            source_staging,
            target,
            target_staging,
        }
    }

    /// Verifies if invariants still holds.
    pub fn validate(&self, should_exist: bool) -> std::result::Result<(), String> {
        if Self::data_exists(&self.source) && Self::data_exists(&self.target) {
            return Err(format!(
                "Both source and target exist, source: {}, target: {}",
                self.source.display(),
                self.target.display()
            ));
        }
        if Self::data_exists(&self.source_staging) && Self::data_exists(&self.target_staging) {
            return Err(format!(
                "Both source_staging and target_staging exist, source_staging: {}, target_staging: {}",
                self.source_staging.display(),
                self.target_staging.display()
            ));
        }
        let exists = Self::data_exists(&self.source_staging)
            || Self::data_exists(&self.target_staging)
            || Self::data_exists(&self.source)
            || Self::data_exists(&self.target);
        if exists != should_exist {
            return Err(format!(
                "Expected invariants to hold: {}, but invariants {}",
                should_exist, exists
            ));
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
