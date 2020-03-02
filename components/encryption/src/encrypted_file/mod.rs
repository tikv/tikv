use std::fs::{rename, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use kvproto::encryptionpb::EncryptedContent;
use protobuf::Message;
use rand::{thread_rng, RngCore};

use crate::master_key::*;
use crate::{Error, Iv, Result};

mod header;
use header::*;

const TMP_FILE_SUFFIX: &str = ".tmp";

/// An file encrypted by master key.
pub struct EncryptedFile<'a> {
    base: &'a Path,
    name: &'a str,
}

impl<'a> EncryptedFile<'a> {
    /// New an `EncryptedFile`.
    ///
    /// It's different from `std::fs::File`, it does not hold a reference
    /// to the file or open the file, util we actually read or write.
    pub fn new(base: &'a Path, name: &'a str) -> EncryptedFile<'a> {
        EncryptedFile { base, name }
    }

    /// Read and decrypt the file.
    pub fn read(&self, master_key: &dyn Backend) -> Result<Vec<u8>> {
        let res = OpenOptions::new()
            .read(true)
            .open(self.base.join(self.name));
        match res {
            Ok(mut f) => {
                let mut buf = Vec::new();
                f.read_to_end(&mut buf)?;
                let (_, content) = Header::parse(&buf)?;
                let mut encrypted_content = EncryptedContent::default();
                encrypted_content.merge_from_bytes(content).map_err(|e| {
                    Error::Other("fail to decode encrypted_content".to_owned().into())
                });
                let plaintext = master_key.decrypt(&encrypted_content)?;

                Ok(plaintext)
            }
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    return Ok(Vec::new());
                }
                Err(Error::Io(e))
            }
        }
    }

    pub fn write(&self, plaintext_content: &[u8], master_key: &dyn Backend) -> Result<()> {
        // Write to a tmp file.
        // TODO what if a tmp file already exists?
        let origin_path = self.base.join(&self.name);
        let mut tmp_path = origin_path.clone();
        tmp_path.set_extension(format!("{}.{}", thread_rng().next_u64(), TMP_FILE_SUFFIX));
        let mut tmp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&tmp_path)
            .unwrap();

        // Encrypt the content.
        let encrypted_content = master_key
            .encrypt(&plaintext_content)?
            .write_to_bytes()
            .unwrap();
        let header = Header::new(&encrypted_content);
        tmp_file.write_all(&header.to_bytes())?;
        tmp_file.write_all(&encrypted_content)?;
        tmp_file.sync_all()?;

        // Replace old file with the tmp file aomticlly.
        rename(tmp_path, origin_path)?;

        // TODO GC broken temp files if necessary.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_write() {
        let tmp = tempfile::TempDir::new().unwrap();
        let file = EncryptedFile::new(tmp.path(), "encrypted");
        assert_eq!(&file.read(&PlainTextBackend::default()).unwrap(), &[]);
        assert_eq!(file.base, tmp.path());
        assert_eq!(file.name, "encrypted");

        let content = [5; 32];
        file.write(&content, &PlainTextBackend::default()).unwrap();
        drop(file);

        let file = EncryptedFile::new(tmp.path(), "encrypted");
        assert_eq!(file.read(&PlainTextBackend::default()).unwrap(), &content);
    }
}
