// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rand::Rng;

use super::ExternalStorage;

const LOCAL_STORAGE_TMP_DIR: &str = "localtmp";
const LOCAL_STORAGE_TMP_FILE_SUFFIX: &str = "tmp";

fn maybe_create_dir(path: &Path) -> io::Result<()> {
    if let Err(e) = fs::create_dir_all(path) {
        if e.kind() != io::ErrorKind::AlreadyExists {
            return Err(e);
        }
    }
    Ok(())
}

/// A storage saves files in local file system.
#[derive(Clone)]
pub struct LocalStorage {
    base: PathBuf,
    base_dir: Arc<File>,
    tmp: PathBuf,
}

impl LocalStorage {
    /// Create a new local storage in the given path.
    pub fn new(base: &Path) -> io::Result<LocalStorage> {
        info!("create local storage"; "base" => base.display());
        let tmp_dir = base.join(LOCAL_STORAGE_TMP_DIR);
        maybe_create_dir(&tmp_dir)?;
        let base_dir = Arc::new(File::open(base)?);
        Ok(LocalStorage {
            base: base.to_owned(),
            base_dir,
            tmp: tmp_dir,
        })
    }

    fn tmp_path(&self, path: &Path) -> PathBuf {
        let uid: u64 = rand::thread_rng().gen();
        let tmp_suffix = format!("{}{:016x}", LOCAL_STORAGE_TMP_FILE_SUFFIX, uid);
        self.tmp.join(path).with_extension(tmp_suffix)
    }
}

impl ExternalStorage for LocalStorage {
    fn write(&self, name: &str, reader: &mut dyn Read) -> io::Result<()> {
        // Storage does not support dir,
        // "a/a.sst", "/" and "" will return an error.
        if Path::new(name)
            .parent()
            .map_or(true, |p| p.parent().is_some())
        {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("[{}] parent is not allowed in storage", name),
            ));
        }
        // Sanitize check, do not save file if it is already exist.
        if fs::metadata(self.base.join(name)).is_ok() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("[{}] is already exists in {}", name, self.base.display()),
            ));
        }
        let tmp_path = self.tmp_path(Path::new(name));
        let mut tmp_f = File::create(&tmp_path)?;
        io::copy(reader, &mut tmp_f)?;
        tmp_f.metadata()?.permissions().set_readonly(true);
        tmp_f.sync_all()?;
        debug!("save file to local storage";
            "name" => %name, "base" => %self.base.display());
        fs::rename(tmp_path, self.base.join(name))?;
        // Fsync the base dir.
        self.base_dir.sync_all()
    }

    fn read(&self, name: &str) -> io::Result<Box<dyn Read>> {
        debug!("read file from local storage";
            "name" => %name, "base" => %self.base.display());
        let file = File::open(self.base.join(name))?;
        Ok(Box::new(file))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::Builder;

    #[test]
    fn test_local_storage() {
        let temp_dir = Builder::new().tempdir().unwrap();
        let path = temp_dir.path();
        let ls = LocalStorage::new(path).unwrap();

        // Test tmp_path
        let tp = ls.tmp_path(Path::new("t.sst"));
        assert_eq!(tp.parent().unwrap(), path.join(LOCAL_STORAGE_TMP_DIR));
        assert!(tp.file_name().unwrap().to_str().unwrap().starts_with('t'));
        assert!(tp
            .as_path()
            .extension()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with(LOCAL_STORAGE_TMP_FILE_SUFFIX));

        // Test save_file
        let magic_contents = b"5678".to_vec();
        ls.write("a.log", &mut magic_contents.clone().as_slice())
            .unwrap();
        assert_eq!(fs::read(path.join("a.log")).unwrap(), magic_contents);

        // Names contain parent is not allowed.
        ls.write("a/a.log", &mut magic_contents.clone().as_slice())
            .unwrap_err();
        // Empty name is not allowed.
        ls.write("", &mut magic_contents.clone().as_slice())
            .unwrap_err();
        // root is not allowed.
        ls.write("/", &mut magic_contents.clone().as_slice())
            .unwrap_err();
    }
}
