// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use byteorder::{BigEndian, ByteOrder};
use kvproto::encryptionpb::{EncryptedContent, FileDictionary, FileInfo};
use protobuf::Message;
use rand::{thread_rng, RngCore};

use crate::encrypted_file::{Header, Version, TMP_FILE_SUFFIX};
use crate::master_key::{Backend, PlaintextBackend};
use crate::Result;

use std::fs::{rename, File, OpenOptions};
use std::io::BufRead;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

#[derive(Debug)]
enum LogRecord {
    INSERT(FileInfo),
    REMOVE,
}

/// LogFile is used to store log style file dictionary.
///
/// Layout:
/// ```text
/// Encrypted File Header + FileDictionary Content | Record Header + Record Content | etc
/// ```
#[derive(Debug)]
pub struct LogFile {
    base: PathBuf,
    name: String,
    // Avoid reopening the file every time when append.
    append_file: Option<File>,
    // Internal file dictionary to avoid recovery before rewrite and to check if compaction
    // is needed.
    file_dict: FileDictionary,
    // Determine whether compact the log.
    file_rewrite_threshold: u64,
    // Record the number of `REMOVE` to determine whether compact the log.
    removed: u64,
}

impl LogFile {
    /// Header of record.
    ///
    /// ```text
    /// +----+--+--+-+----+----
    /// |    |  |  | |    |
    /// ++---++-++-++++---++---
    ///  ^    ^  ^  ^ ^    ^
    ///  |    |  |  | |    + FileInfo content (variable bytes)
    ///  |    |  |  | + File name content (variable bytes)
    ///  |    |  |  + Log type (1 bytes)
    ///  |    |  + FileInfo length (2 bytes)
    ///  |    + File name length (2 bytes)
    ///  + Crc32 (4 bytes)
    /// ```
    const RECORD_HEADER_SIZE: usize = 4 + 2 + 2 + 1;

    pub fn new<P: AsRef<Path>>(
        base: P,
        name: &str,
        file_rewrite_threshold: u64,
    ) -> Result<LogFile> {
        let mut log_file = LogFile {
            base: base.as_ref().to_path_buf(),
            name: name.to_owned(),
            append_file: None,
            file_dict: FileDictionary::default(),
            file_rewrite_threshold,
            removed: 0,
        };
        log_file.rewrite()?;
        Ok(log_file)
    }

    pub fn open<P: AsRef<Path>>(
        base: P,
        name: &str,
        file_rewrite_threshold: u64,
        skip_rewrite: bool,
    ) -> Result<(LogFile, FileDictionary)> {
        let mut log_file = LogFile {
            base: base.as_ref().to_path_buf(),
            name: name.to_owned(),
            append_file: None,
            file_dict: FileDictionary::default(),
            file_rewrite_threshold,
            removed: 0,
        };

        let file_dict = log_file.recovery()?;
        if !skip_rewrite {
            log_file.rewrite()?;
        }
        Ok((log_file, file_dict))
    }

    /// Rewrite the log file to reduce file size and reduce the time of next recovery.
    pub fn rewrite(&mut self) -> Result<()> {
        let origin_path = self.base.join(&self.name);
        let mut tmp_path = origin_path.clone();
        tmp_path.set_extension(format!("{}.{}", thread_rng().next_u64(), TMP_FILE_SUFFIX));
        let mut tmp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&tmp_path)
            .unwrap();

        let file_dict_bytes = self.file_dict.write_to_bytes()?;
        let header = Header::new(&file_dict_bytes, Version::V2);
        tmp_file.write_all(&header.to_bytes())?;
        tmp_file.write_all(&file_dict_bytes)?;
        tmp_file.sync_all()?;

        // Replace old file with the tmp file aomticlly.
        rename(&tmp_path, &origin_path)?;
        let base_dir = File::open(&self.base)?;
        base_dir.sync_all()?;
        let file = std::fs::OpenOptions::new().append(true).open(origin_path)?;
        if let Some(f) = self.append_file.as_ref() {
            f.sync_all()?;
        }
        self.append_file.replace(file);

        Ok(())
    }

    /// Recovery from the log file and return `FileDictionary`.
    pub fn recovery(&mut self) -> Result<FileDictionary> {
        let mut f = OpenOptions::new()
            .read(true)
            .open(self.base.join(&self.name))?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        let remained = buf.as_slice();

        // parse the file dictionary
        let (header, content, mut remained) = Header::parse(remained)?;
        let mut file_dict = FileDictionary::default();
        match header.version() {
            Version::V1 => {
                let mut encrypted_content = EncryptedContent::default();
                encrypted_content.merge_from_bytes(content)?;
                let content = PlaintextBackend::default().decrypt(&encrypted_content)?;
                file_dict.merge_from_bytes(&content)?;
            }
            Version::V2 => {
                file_dict.merge_from_bytes(content)?;
                // parse the remained records
                while !remained.is_empty() {
                    // If the parsing get errors, manual intervention is required to recover.
                    let (used_size, file_name, mode) = Self::parse_next_record(remained)?;
                    remained.consume(used_size);
                    match mode {
                        LogRecord::INSERT(info) => {
                            file_dict.files.insert(file_name, info);
                        }
                        LogRecord::REMOVE => {
                            let original = file_dict.files.remove(&file_name);
                            if original.is_none() {
                                return Err(box_err!(
                                    "Try to recovery from log file but remove a null entry, file name: {}",
                                    file_name
                                ));
                            }
                        }
                    }
                }
            }
        }
        self.file_dict = file_dict.clone();
        Ok(file_dict)
    }

    /// Append an insert operation to the log file.
    ///
    /// Warning: `self.write(file_dict)` must be called before.
    pub fn insert(&mut self, name: &str, info: &FileInfo) -> Result<()> {
        let file = self.append_file.as_mut().unwrap();
        let bytes = Self::convert_record_to_bytes(name, LogRecord::INSERT(info.clone()))?;
        file.write_all(&bytes)?;

        self.file_dict.files.insert(name.to_owned(), info.clone());
        self.check_compact()?;

        Ok(())
    }

    /// Append a remove operation to the log file.
    ///
    /// Warning: `self.write(file_dict)` must be called before.
    pub fn remove(&mut self, name: &str) -> Result<()> {
        let file = self.append_file.as_mut().unwrap();
        let bytes = Self::convert_record_to_bytes(name, LogRecord::REMOVE)?;
        file.write_all(&bytes)?;
        file.sync_all()?;

        self.file_dict.files.remove(name);
        self.removed += 1;
        self.check_compact()?;

        Ok(())
    }

    /// Replace existed file entry with new file information.
    ///
    /// Warning: `self.write(file_dict)` must be called before.
    pub fn replace(&mut self, src_name: &str, dst_name: &str, info: FileInfo) -> Result<()> {
        assert_ne!(src_name, dst_name);
        let file = self.append_file.as_mut().unwrap();
        let mut bytes1 = Self::convert_record_to_bytes(dst_name, LogRecord::INSERT(info.clone()))?;
        let bytes2 = Self::convert_record_to_bytes(src_name, LogRecord::REMOVE)?;
        bytes1.extend_from_slice(&bytes2);
        file.write_all(&bytes1)?;
        file.sync_all()?;

        self.file_dict.files.remove(src_name);
        self.file_dict.files.insert(dst_name.to_owned(), info);
        self.removed += 1;
        self.check_compact()?;

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let file = self.append_file.as_ref().unwrap();
        file.sync_all()?;
        Ok(())
    }

    /// This function needs to be called after each append operation to check
    /// if compact is needed.
    fn check_compact(&mut self) -> Result<()> {
        if self.removed > self.file_rewrite_threshold {
            self.removed = 0;
            self.rewrite()?;
        }
        Ok(())
    }

    fn convert_record_to_bytes(fname: &str, record_type: LogRecord) -> Result<Vec<u8>> {
        let name_len = fname.len() as u16;
        let mut content = vec![];
        content.extend_from_slice(fname.as_bytes());

        let mut header_buf = [0; Self::RECORD_HEADER_SIZE];
        let info_len = match record_type {
            LogRecord::INSERT(info) => {
                header_buf[Self::RECORD_HEADER_SIZE - 1] = 1;
                let info_bytes = info.write_to_bytes()?;
                content.extend_from_slice(&info_bytes);
                info_bytes.len() as u16
            }
            LogRecord::REMOVE => {
                header_buf[Self::RECORD_HEADER_SIZE - 1] = 2;
                0
            }
        };

        let mut digest = crc32fast::Hasher::new();
        digest.update(&content);
        let crc32 = digest.finalize();

        BigEndian::write_u32(&mut header_buf[0..4], crc32);
        BigEndian::write_u16(&mut header_buf[4..6], name_len);
        BigEndian::write_u16(&mut header_buf[6..8], info_len);
        let mut ret = header_buf.to_vec();
        ret.extend_from_slice(&content);
        Ok(ret)
    }

    fn parse_next_record(mut remained: &[u8]) -> Result<(usize, String, LogRecord)> {
        if remained.len() < Self::RECORD_HEADER_SIZE {
            return Err(box_err!(
                "file corrupted! record header size is too small: {}",
                remained.len()
            ));
        }

        // parse header
        let crc32 = BigEndian::read_u32(&remained[0..4]);
        let name_len = BigEndian::read_u16(&remained[4..6]) as usize;
        let info_len = BigEndian::read_u16(&remained[6..8]) as usize;
        let mode = remained[Self::RECORD_HEADER_SIZE - 1];
        remained.consume(Self::RECORD_HEADER_SIZE);
        if remained.len() < name_len + info_len {
            return Err(box_err!(
                "file corrupted! record content size is too small: {}, expect: {}",
                remained.len(),
                name_len + info_len
            ));
        }

        // checksum crc32
        let mut digest = crc32fast::Hasher::new();
        digest.update(&remained[0..name_len + info_len]);
        let crc32_checksum = digest.finalize();
        if crc32_checksum != crc32 {
            return Err(box_err!(
                "file corrupted! crc32 mismatch {} != {}",
                crc32,
                crc32_checksum
            ));
        }

        // read file name
        let ret: Result<String> = String::from_utf8(remained[0..name_len].to_owned())
            .map_err(|e| box_err!("parse file name failed: {:?}", e));
        let file_name = ret?;
        remained.consume(name_len);

        // return result
        let used_size = Self::RECORD_HEADER_SIZE + name_len + info_len;
        let record = match mode {
            2 => LogRecord::REMOVE,
            1 => {
                let mut file_info = FileInfo::default();
                file_info.merge_from_bytes(&remained[0..info_len])?;
                LogRecord::INSERT(file_info)
            }
            _ => return Err(box_err!("file corrupted! record type is unknown: {}", mode)),
        };
        Ok((used_size, file_name, record))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypter::compat;
    use crate::encrypted_file::EncryptedFile;
    use crate::Error;
    use kvproto::encryptionpb::EncryptionMethod;

    #[test]
    fn test_log_file_normal() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut log_file = LogFile::new(tempdir.path(), "test_log_file", 2).unwrap();
        let info1 = create_file_info(1, EncryptionMethod::Aes256Ctr);
        let info2 = create_file_info(2, EncryptionMethod::Unknown);
        let info3 = create_file_info(3, EncryptionMethod::Aes128Ctr);
        let info4 = create_file_info(4, EncryptionMethod::Aes128Ctr);

        log_file.insert("info1", &info1).unwrap();
        log_file.insert("info2", &info2).unwrap();
        log_file.insert("info3", &info3).unwrap();

        let file_dict = log_file.recovery().unwrap();

        assert_eq!(*file_dict.files.get("info1").unwrap(), info1);
        assert_eq!(*file_dict.files.get("info2").unwrap(), info2);
        assert_eq!(*file_dict.files.get("info3").unwrap(), info3);
        assert_eq!(file_dict.files.len(), 3);

        log_file.remove("info2").unwrap();
        log_file.remove("info1").unwrap();
        log_file.insert("info2", &info4).unwrap();

        let file_dict = log_file.recovery().unwrap();
        assert_eq!(file_dict.files.get("info1"), None);
        assert_eq!(*file_dict.files.get("info2").unwrap(), info4);
        assert_eq!(*file_dict.files.get("info3").unwrap(), info3);
        assert_eq!(file_dict.files.len(), 2);
    }

    #[test]
    fn test_log_file_existed() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut log_file = LogFile::new(tempdir.path(), "test_log_file", 2).unwrap();

        let info = create_file_info(1, EncryptionMethod::Aes256Ctr);
        log_file.insert("info", &info).unwrap();

        let (_, file_dict) = LogFile::open(tempdir.path(), "test_log_file", 2, false).unwrap();
        assert_eq!(*file_dict.files.get("info").unwrap(), info);
    }

    #[test]
    fn test_log_file_not_existed() {
        let tempdir = tempfile::tempdir().unwrap();
        let ret = LogFile::open(tempdir.path(), "test_log_file", 2, false);
        assert!(matches!(ret, Err(Error::Io(_))));
    }

    #[test]
    fn test_log_file_update_from_v1() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut file_dict = FileDictionary::default();

        let info1 = create_file_info(1, EncryptionMethod::Aes256Ctr);
        let info2 = create_file_info(2, EncryptionMethod::Aes256Ctr);
        file_dict.files.insert("f1".to_owned(), info1);
        file_dict.files.insert("f2".to_owned(), info2);

        let file = EncryptedFile::new(tempdir.path(), "test_log_file");
        file.write(
            &file_dict.write_to_bytes().unwrap(),
            &PlaintextBackend::default(),
        )
        .unwrap();

        let (_, file_dict_read) = LogFile::open(tempdir.path(), "test_log_file", 2, false).unwrap();
        assert_eq!(file_dict, file_dict_read);
    }

    fn create_file_info(id: u64, method: EncryptionMethod) -> FileInfo {
        let mut info = FileInfo::default();
        info.key_id = id;
        info.method = compat(method);
        info
    }
}
