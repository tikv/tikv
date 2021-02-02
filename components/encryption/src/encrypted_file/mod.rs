// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs::{rename, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::time::Instant;

use kvproto::encryptionpb::EncryptedContent;
use protobuf::Message;
use rand::{thread_rng, RngCore};

use crate::master_key::*;
use crate::metrics::*;
use crate::Result;

mod header;
pub use header::*;

pub const TMP_FILE_SUFFIX: &str = "tmp";

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

    /// Read and decrypt the file. Caller need to handle the NotFound io error in case file not
    /// exists.
    pub fn read(&self, master_key: &dyn Backend) -> Result<Vec<u8>> {
        let start = Instant::now();
        let res = OpenOptions::new()
            .read(true)
            .open(self.base.join(self.name));
        match res {
            Ok(mut f) => {
                let mut buf = Vec::new();
                f.read_to_end(&mut buf)?;
                let (_, content, _) = Header::parse(&buf)?;
                let mut encrypted_content = EncryptedContent::default();
                encrypted_content.merge_from_bytes(content)?;
                let plaintext = master_key.decrypt(&encrypted_content)?;

                ENCRYPT_DECRPTION_FILE_HISTOGRAM
                    .with_label_values(&[self.name, "read"])
                    .observe(start.elapsed().as_secs_f64());

                Ok(plaintext)
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn write(&self, plaintext_content: &[u8], master_key: &dyn Backend) -> Result<()> {
        let start = Instant::now();
        // Write to a tmp file.
        // TODO what if a tmp file already exists?
        let origin_path = self.base.join(&self.name);
        let mut tmp_path = origin_path.clone();
        tmp_path.set_extension(format!("{}.{}", thread_rng().next_u64(), TMP_FILE_SUFFIX));
        let mut tmp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&tmp_path)
            .unwrap_or_else(|_| panic!("EncryptedFile::write {}", &tmp_path.to_str().unwrap()));

        // Encrypt the content.
        let encrypted_content = master_key
            .encrypt(&plaintext_content)?
            .write_to_bytes()
            .unwrap();
        let header = Header::new(&encrypted_content, Version::V1);
        tmp_file.write_all(&header.to_bytes())?;
        tmp_file.write_all(&encrypted_content)?;
        tmp_file.sync_all()?;

        // Replace old file with the tmp file aomticlly.
        rename(tmp_path, origin_path)?;
        let base_dir = File::open(&self.base)?;
        base_dir.sync_all()?;

        ENCRYPT_DECRPTION_FILE_HISTOGRAM
            .with_label_values(&[self.name, "write"])
            .observe(start.elapsed().as_secs_f64());

        // TODO GC broken temp files if necessary.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;

    use matches::assert_matches;
    use std::io::ErrorKind;

    #[test]
    fn test_open_write() {
        let tmp = tempfile::TempDir::new().unwrap();
        let file = EncryptedFile::new(tmp.path(), "encrypted");
        assert_eq!(file.base, tmp.path());
        assert_eq!(file.name, "encrypted");
        let ret = file.read(&PlaintextBackend::default());
        assert_matches!(ret, Err(Error::Io(_)));
        if let Err(Error::Io(e)) = file.read(&PlaintextBackend::default()) {
            assert_eq!(ErrorKind::NotFound, e.kind());
        }

        let content = b"test content";
        file.write(content, &PlaintextBackend::default()).unwrap();
        drop(file);

        let file = EncryptedFile::new(tmp.path(), "encrypted");
        assert_eq!(file.read(&PlaintextBackend::default()).unwrap(), content);
    }
}
