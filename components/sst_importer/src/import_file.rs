use super::{Error, Result};
use encryption::{DataKeyManager, EncrypterWriter};
use engine_traits::EncryptionKeyManager;
use file_system::{sync_dir, File, OpenOptions};
use kvproto::import_sstpb::*;
use std::fmt;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone)]
pub struct ImportPath {
    // The path of the file that has been uploaded.
    pub save: PathBuf,
    // The path of the file that is being uploaded.
    pub temp: PathBuf,
    // The path of the file that is going to be ingested.
    pub clone: PathBuf,
}

impl fmt::Debug for ImportPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImportPath")
            .field("save", &self.save)
            .field("temp", &self.temp)
            .field("clone", &self.clone)
            .finish()
    }
}

impl ImportPath {
    // move file from temp to save.
    pub fn save(mut self, key_manager: Option<&DataKeyManager>) -> Result<()> {
        file_system::rename(&self.temp, &self.save)?;
        if let Some(key_manager) = key_manager {
            let temp_str = self
                .temp
                .to_str()
                .ok_or_else(|| Error::InvalidSSTPath(self.temp.clone()))?;
            let save_str = self
                .save
                .to_str()
                .ok_or_else(|| Error::InvalidSSTPath(self.save.clone()))?;
            key_manager.link_file(temp_str, save_str)?;
            key_manager.delete_file(temp_str)?;
        }
        // sync the directory after rename
        self.save.pop();
        sync_dir(&self.save)?;
        Ok(())
    }
}

// `SyncableWrite` extends io::Write with sync
trait SyncableWrite: io::Write + Send {
    // sync all metadata to storage
    fn sync(&self) -> io::Result<()>;
}

impl SyncableWrite for File {
    fn sync(&self) -> io::Result<()> {
        self.sync_all()
    }
}

impl SyncableWrite for EncrypterWriter<File> {
    fn sync(&self) -> io::Result<()> {
        self.sync_all()
    }
}

/// ImportFile is used to handle the writing and verification of SST files.
pub struct ImportFile {
    meta: SstMeta,
    path: ImportPath,
    file: Option<Box<dyn SyncableWrite>>,
    digest: crc32fast::Hasher,
    key_manager: Option<Arc<DataKeyManager>>,
}

impl ImportFile {
    pub fn create(
        meta: SstMeta,
        path: ImportPath,
        key_manager: Option<Arc<DataKeyManager>>,
    ) -> Result<ImportFile> {
        let file: Box<dyn SyncableWrite> = if let Some(ref manager) = key_manager {
            // key manager will truncate existed file, so we should check exist manually.
            if path.temp.exists() {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("file already exists, {}", path.temp.to_str().unwrap()),
                )));
            }
            Box::new(manager.create_file(&path.temp)?)
        } else {
            Box::new(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path.temp)?,
            )
        };
        Ok(ImportFile {
            meta,
            path,
            file: Some(file),
            digest: crc32fast::Hasher::new(),
            key_manager,
        })
    }

    pub fn append(&mut self, data: &[u8]) -> Result<()> {
        self.file.as_mut().unwrap().write_all(data)?;
        self.digest.update(data);
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        self.validate()?;
        // sync is a wrapping for File::sync_all
        self.file.take().unwrap().sync()?;
        if self.path.save.exists() {
            return Err(Error::FileExists(
                self.path.save.clone(),
                "finalize SST write cache",
            ));
        }
        file_system::rename(&self.path.temp, &self.path.save)?;
        if let Some(ref manager) = self.key_manager {
            let tmp_str = self.path.temp.to_str().unwrap();
            let save_str = self.path.save.to_str().unwrap();
            manager.link_file(tmp_str, save_str)?;
            manager.delete_file(self.path.temp.to_str().unwrap())?;
        }
        Ok(())
    }

    fn cleanup(&mut self) -> Result<()> {
        self.file.take();
        if self.path.temp.exists() {
            if let Some(ref manager) = self.key_manager {
                manager.delete_file(self.path.temp.to_str().unwrap())?;
            }
            file_system::remove_file(&self.path.temp)?;
        }
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        let crc32 = self.digest.clone().finalize();
        let expect = self.meta.get_crc32();
        if crc32 != expect {
            let reason = format!("crc32 {}, expect {}", crc32, expect);
            return Err(Error::FileCorrupted(self.path.temp.clone(), reason));
        }
        Ok(())
    }
}

impl Drop for ImportFile {
    fn drop(&mut self) {
        if let Err(e) = self.cleanup() {
            warn!("cleanup failed"; "file" => ?self, "err" => %e);
        }
    }
}

impl fmt::Debug for ImportFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImportFile")
            .field("meta", &self.meta)
            .field("path", &self.path)
            .finish()
    }
}

pub trait SSTWriter<M: protobuf::Message> {
    fn write(&mut self, m: M) -> Result<()>;
    fn finish(self) -> Result<Vec<SstMeta>>;
}
