// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use byteorder::{BigEndian, ByteOrder};
use kvproto::encryptionpb::{FileDictionary, FileInfo};
use protobuf::Message;
use rand::{thread_rng, RngCore};

use crate::encrypted_file::{Header, Version, TMP_FILE_SUFFIX};
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
    // Number of append records.
    append_num: u64,
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

    const REWRITE_THRESHOLD: u64 = 80000;

    pub fn new<P: AsRef<Path>>(base: P, name: &str) -> Result<LogFile> {
        let mut log_file = LogFile {
            base: base.as_ref().to_path_buf(),
            name: name.to_owned(),
            append_file: None,
            append_num: 0,
        };

        let file_dict = FileDictionary::default();
        log_file.write(&file_dict)?;
        Ok(log_file)
    }

    pub fn open<P: AsRef<Path>>(base: P, name: &str) -> Result<(LogFile, FileDictionary)> {
        let mut log_file = LogFile {
            base: base.as_ref().to_path_buf(),
            name: name.to_owned(),
            append_file: None,
            append_num: 0,
        };

        let file_dict = log_file.recovery()?;
        log_file.write(&file_dict)?;
        Ok((log_file, file_dict))
    }

    /// Rewrite the log file to reduce file size and reduce the time of next recovery.
    pub fn write(&mut self, file_dict: &FileDictionary) -> Result<()> {
        let origin_path = self.base.join(&self.name);
        let mut tmp_path = origin_path.clone();
        tmp_path.set_extension(format!("{}.{}", thread_rng().next_u64(), TMP_FILE_SUFFIX));
        let mut tmp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&tmp_path)
            .unwrap();

        let file_dict_bytes = file_dict.write_to_bytes()?;
        let header = Header::new(&file_dict_bytes, Version::V2);
        tmp_file.write_all(&header.to_bytes())?;
        tmp_file.write_all(&file_dict_bytes)?;
        tmp_file.sync_all()?;

        // Replace old file with the tmp file aomticlly.
        rename(&tmp_path, &origin_path)?;
        let base_dir = File::open(&self.base)?;
        base_dir.sync_all()?;
        let file = std::fs::OpenOptions::new().append(true).open(origin_path)?;
        self.append_file.replace(file);
        self.append_num = 0;

        Ok(())
    }

    /// Recovery from the log file and return `FileDictionary`.
    pub fn recovery(&self) -> Result<FileDictionary> {
        let mut f = OpenOptions::new()
            .read(true)
            .open(self.base.join(&self.name))?;
        let mut file_dict = FileDictionary::default();
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        let mut remained = buf.as_slice();

        // parse the file dictionary
        let (_, content) = Header::parse(remained)?;
        remained.consume(Header::SIZE + content.len());
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

        Ok(file_dict)
    }

    /// Append an insert operation to the log file.
    ///
    /// Warning: `self.write(file_dict)` must be called before.
    pub fn insert(&mut self, name: &str, info: FileInfo) -> Result<()> {
        if let Some(file) = self.append_file.as_mut() {
            let bytes = Self::convert_record_to_bytes(name, LogRecord::INSERT(info))?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            self.check_compact()?;
            Ok(())
        } else {
            Err(box_err!(
                "file corrupted! write operation must be called before append."
            ))
        }
    }

    /// Append a remove operation to the log file.
    ///
    /// Warning: `self.write(file_dict)` must be called before.
    pub fn remove(&mut self, name: &str) -> Result<()> {
        if let Some(file) = self.append_file.as_mut() {
            let bytes = Self::convert_record_to_bytes(name, LogRecord::REMOVE)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            self.check_compact()?;
            Ok(())
        } else {
            Err(box_err!(
                "file corrupted! write operation must be called before append."
            ))
        }
    }

    /// Replace existed file entry with new file information.
    ///
    /// Warning: `self.write(file_dict)` must be called before.
    pub fn replace(&mut self, src_name: &str, dst_name: &str, info: FileInfo) -> Result<()> {
        if let Some(file) = self.append_file.as_mut() {
            let bytes1 = Self::convert_record_to_bytes(src_name, LogRecord::REMOVE)?;
            let bytes2 = Self::convert_record_to_bytes(dst_name, LogRecord::INSERT(info))?;
            file.write_all(&bytes1)?;
            file.write_all(&bytes2)?;
            file.sync_all()?;
            self.check_compact()?;
            Ok(())
        } else {
            Err(box_err!(
                "file corrupted! write operation must be called before append."
            ))
        }
    }

    /// This function needs to be called after each append operation to check
    /// if compact is needed.
    fn check_compact(&mut self) -> Result<()> {
        self.append_num += 1;
        if self.append_num > Self::REWRITE_THRESHOLD {
            let file_dict = self.recovery()?;
            self.write(&file_dict)?;
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
                header_buf[8] = 1;
                let info_bytes = info.write_to_bytes()?;
                content.extend_from_slice(&info_bytes);
                info_bytes.len() as u16
            }
            LogRecord::REMOVE => {
                header_buf[8] = 2;
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
        let mode = remained[8];
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
        let file_name = String::from_utf8(remained[0..name_len].to_owned()).unwrap();
        remained.consume(name_len);

        // return result
        let used_size = Self::RECORD_HEADER_SIZE + name_len + info_len;
        let record_mode = match mode {
            2 => LogRecord::REMOVE,
            1 => {
                let mut file_info = FileInfo::default();
                file_info.merge_from_bytes(&remained[0..info_len])?;
                LogRecord::INSERT(file_info)
            }
            _ => return Err(box_err!("file corrupted! record type is unknown: {}", mode)),
        };
        Ok((used_size, file_name, record_mode))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypter::compat;
    use crate::Error;
    use kvproto::encryptionpb::EncryptionMethod;

    #[test]
    fn test_log_file() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut log_file = LogFile::new(tempdir.path(), "test_log_file").unwrap();
        let info1 = create_file_info(1, EncryptionMethod::Aes256Ctr);
        let info2 = create_file_info(2, EncryptionMethod::Unknown);
        let info3 = create_file_info(3, EncryptionMethod::Aes128Ctr);

        log_file.insert("info1", info1.clone()).unwrap();
        log_file.insert("info2", info2.clone()).unwrap();
        log_file.insert("info3", info3.clone()).unwrap();

        let file_dict = log_file.recovery().unwrap();

        assert_eq!(*file_dict.files.get("info1").unwrap(), info1);
        assert_eq!(*file_dict.files.get("info2").unwrap(), info2);
        assert_eq!(*file_dict.files.get("info3").unwrap(), info3);
        assert_eq!(file_dict.files.len(), 3);

        log_file.remove("info2").unwrap();

        let file_dict = log_file.recovery().unwrap();
        assert_eq!(*file_dict.files.get("info1").unwrap(), info1);
        assert_eq!(file_dict.files.get("info2"), None);
        assert_eq!(*file_dict.files.get("info3").unwrap(), info3);
        assert_eq!(file_dict.files.len(), 2);

        log_file.write(&file_dict).unwrap();
        assert_eq!(file_dict, log_file.recovery().unwrap());
    }

    #[test]
    fn test_log_file_open_existed() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut log_file = LogFile::new(tempdir.path(), "test_log_file").unwrap();

        let info = create_file_info(1, EncryptionMethod::Aes256Ctr);
        let mut dict = FileDictionary::default();
        dict.files.insert("info".to_owned(), info);
        log_file.write(&dict).unwrap();

        let (_, file_dict) = LogFile::open(tempdir.path(), "test_log_file").unwrap();
        assert_eq!(file_dict, dict);
    }

    #[test]
    fn test_log_file_open_not_existed() {
        let tempdir = tempfile::tempdir().unwrap();
        let ret = LogFile::open(tempdir.path(), "test_log_file");
        assert!(matches!(ret, Err(Error::Io(_))));
    }

    fn create_file_info(id: u64, method: EncryptionMethod) -> FileInfo {
        let mut info = FileInfo::default();
        info.key_id = id;
        info.method = compat(method);
        info
    }
}
