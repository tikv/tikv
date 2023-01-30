// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    io::{BufRead, Read, Write},
    path::{Path, PathBuf},
};

use byteorder::{BigEndian, ByteOrder};
use file_system::{rename, File, OpenOptions};
use kvproto::encryptionpb::{EncryptedContent, FileDictionary, FileDictionaryV2, FileInfo};
use protobuf::{parse_from_bytes, Message};
use rand::{thread_rng, RngCore};
use tikv_util::{box_err, set_panic_mark, warn};

use crate::{
    encrypted_file::{EncryptedFile, Header, Version, TMP_FILE_SUFFIX},
    master_key::{Backend, PlaintextBackend},
    metrics::*,
    Error, Result,
};

pub trait DictionaryItem: Sized {
    fn parse(buf: &mut &[u8]) -> Result<Self>;

    fn as_bytes(&self) -> Result<Vec<u8>>;

    fn is_tombstone(&self) -> bool;
}

pub trait ProtobufDictionary: Default + Message {
    type Item: DictionaryItem;

    fn apply(&mut self, item: &Self::Item) -> Result<()>;
}

#[derive(Debug)]
pub enum DataKeyDictionaryItem {
    Insert(String, FileInfo),
    Remove(String),
}

impl DataKeyDictionaryItem {
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
}

impl DictionaryItem for DataKeyDictionaryItem {
    fn parse(buf: &mut &[u8]) -> Result<Self> {
        if buf.len() < Self::RECORD_HEADER_SIZE {
            warn!(
                "file corrupted! record header size is too small, discarded the tail record";
                "size" => buf.len(),
            );
            return Err(Error::TailRecordParseIncomplete);
        }

        // parse header
        let crc32 = BigEndian::read_u32(&buf[0..4]);
        let name_len = BigEndian::read_u16(&buf[4..6]) as usize;
        let info_len = BigEndian::read_u16(&buf[6..8]) as usize;
        let mode = buf[Self::RECORD_HEADER_SIZE - 1];
        buf.consume(Self::RECORD_HEADER_SIZE);
        if buf.len() < name_len + info_len {
            warn!(
                "file corrupted! record content size is too small, discarded the tail record";
                "content size" => buf.len(),
                "expected name length" => name_len,
                "expected content length" =>info_len,
            );
            return Err(Error::TailRecordParseIncomplete);
        }

        // checksum crc32
        let mut digest = crc32fast::Hasher::new();
        digest.update(&buf[0..name_len + info_len]);
        let crc32_checksum = digest.finalize();
        if crc32_checksum != crc32 {
            warn!(
                "file corrupted! crc32 mismatch, discarded the tail record";
                "expected crc32" => crc32,
                "checksum crc32" => crc32_checksum,
            );
            return Err(Error::TailRecordParseIncomplete);
        }

        // read file name
        let ret: Result<String> = String::from_utf8(buf[0..name_len].to_owned())
            .map_err(|e| box_err!("parse file name failed: {:?}", e));
        let file_name = ret?;
        buf.consume(name_len);

        let record = match mode {
            2 => Self::Remove(file_name),
            1 => {
                let mut file_info = FileInfo::default();
                file_info.merge_from_bytes(&buf[0..info_len])?;
                Self::Insert(file_name, file_info)
            }
            _ => return Err(box_err!("file corrupted! record type is unknown: {}", mode)),
        };
        Ok(record)
    }

    fn as_bytes(&self) -> Result<Vec<u8>> {
        let mut header = vec![0; Self::RECORD_HEADER_SIZE];
        let mut content = Vec::new();
        let (name_len, info_len) = match self {
            Self::Insert(name, info) => {
                header[Self::RECORD_HEADER_SIZE - 1] = 1;

                content.extend_from_slice(name.as_bytes());
                let info_bytes = info.write_to_bytes()?;
                content.extend_from_slice(&info_bytes);
                (name.len(), info_bytes.len())
            }
            Self::Remove(name) => {
                header[Self::RECORD_HEADER_SIZE - 1] = 2;

                content.extend_from_slice(name.as_bytes());
                (name.len(), 0)
            }
        };

        let mut digest = crc32fast::Hasher::new();
        digest.update(&content);
        let crc32 = digest.finalize();

        BigEndian::write_u32(&mut header[0..4], crc32);
        BigEndian::write_u16(&mut header[4..6], name_len as u16);
        BigEndian::write_u16(&mut header[6..8], info_len as u16);
        header.extend_from_slice(&content);
        Ok(header)
    }

    fn is_tombstone(&self) -> bool {
        matches!(self, Remove)
    }
}

impl ProtobufDictionary for FileDictionary {
    type Item = DataKeyDictionaryItem;
    fn apply(&mut self, item: &Self::Item) -> Result<()> {
        match item {
            Self::Item::Insert(name, info) => {
                self.files.insert(name.to_owned(), info.clone());
            }
            Self::Item::Remove(name) => {
                let original = self.files.remove(name);
                if original.is_none() {
                    return Err(box_err!(
                        "Try to recovery from log file but remove a null entry, file name: {}",
                        name
                    ));
                }
            }
        }
        Ok(())
    }
}

pub enum DataKeyDictionaryItemV2 {
    InsertFile(u64, String, FileInfo),
    RemoveFile(u64, String),
    InsertDir(u64, String),
    RemoveDir(u64, String),
}

impl DataKeyDictionaryItemV2 {
    /// Header of record.
    ///
    /// ```text
    /// +----+-+--------+--+--+-~
    /// |    | |        |  |  | ~
    /// ++---++++-------++-++-++~
    ///  ^    ^ ^        ^  ^  ^
    ///  |    | |        |  |  + File/Dir name & FileInfo content
    ///  |    | |        |  + FileInfo length (2 bytes)
    ///  |    | |        + File/Dir name length (2 bytes)
    ///  |    | + Dir ID (8 bytes)
    ///  |    + Log type (1 bytes)
    ///  + Crc32 for the entire record (4 bytes)
    /// ```
    const RECORD_HEADER_SIZE: usize = 4 + 1 + 8 + 2 + 2;
}

impl DictionaryItem for DataKeyDictionaryItemV2 {
    fn parse(buf: &mut &[u8]) -> Result<Self> {
        if buf.len() < Self::RECORD_HEADER_SIZE {
            warn!(
                "file corrupted! record header size is too small, discarded the tail record";
                "size" => buf.len(),
            );
            return Err(Error::TailRecordParseIncomplete);
        }

        // parse header
        let crc32 = BigEndian::read_u32(&buf[0..4]);
        let op = buf[4];
        let dir_id = BigEndian::read_u64(&buf[5..13]);
        let name_len = BigEndian::read_u16(&buf[13..15]) as usize;
        let info_len = BigEndian::read_u16(&buf[15..17]) as usize;
        let total_len = Self::RECORD_HEADER_SIZE + name_len + info_len;
        if buf.len() < total_len {
            warn!(
                "file corrupted! record content size is too small, discarded the tail record";
                "content size" => buf.len(),
                "expected name length" => name_len,
                "expected content length" => info_len,
            );
            return Err(Error::TailRecordParseIncomplete);
        }
        // checksum crc32
        let mut digest = crc32fast::Hasher::new();
        digest.update(&buf[4..total_len]);
        let crc32_checksum = digest.finalize();
        if crc32_checksum != crc32 {
            warn!(
                "file corrupted! crc32 mismatch, discarded the tail record";
                "expected crc32" => crc32,
                "checksum crc32" => crc32_checksum,
            );
            return Err(Error::TailRecordParseIncomplete);
        }
        buf.consume(Self::RECORD_HEADER_SIZE);
        // read file/dir name
        let name = String::from_utf8(buf[0..name_len].to_owned())
            .map_err(|e| -> Error { box_err!("parse file name failed: {:?}", e) })?;
        buf.consume(name_len);
        let item = match op {
            1 => {
                assert!(!name.is_empty());
                let file_info = parse_from_bytes(&buf[0..info_len])?;
                buf.consume(info_len);
                Self::InsertFile(dir_id, name, file_info)
            }
            2 => {
                assert!(!name.is_empty() && info_len == 0);
                Self::RemoveFile(dir_id, name)
            }
            3 => {
                assert!(!name.is_empty() && info_len == 0);
                Self::InsertDir(dir_id, name)
            }
            4 => {
                assert!(!name.is_empty() && info_len == 0);
                Self::RemoveDir(dir_id, name)
            }
            _ => return Err(box_err!("file corrupted! record type is unknown: {}", op)),
        };
        Ok(item)
    }

    fn as_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = vec![0; Self::RECORD_HEADER_SIZE];
        let (name_len, info_len) = match self {
            Self::InsertFile(dir_id, name, info) => {
                buf[4] = 1;
                BigEndian::write_u64(&mut buf[5..13], *dir_id);

                buf.extend_from_slice(name.as_bytes());
                let info_bytes = info.write_to_bytes()?;
                buf.extend_from_slice(&info_bytes);
                (name.len(), info_bytes.len())
            }
            Self::RemoveFile(dir_id, name) => {
                buf[4] = 2;
                BigEndian::write_u64(&mut buf[5..13], *dir_id);

                buf.extend_from_slice(name.as_bytes());
                (name.len(), 0)
            }
            Self::InsertDir(dir_id, name) => {
                buf[4] = 3;
                BigEndian::write_u64(&mut buf[5..13], *dir_id);

                buf.extend_from_slice(name.as_bytes());
                (name.len(), 0)
            }
            Self::RemoveDir(dir_id, name) => {
                buf[4] = 4;
                BigEndian::write_u64(&mut buf[5..13], *dir_id);

                buf.extend_from_slice(name.as_bytes());
                (name.len(), 0)
            }
        };

        let mut digest = crc32fast::Hasher::new();
        digest.update(&buf[4..]);
        let crc32 = digest.finalize();

        BigEndian::write_u32(&mut buf[0..4], crc32);
        BigEndian::write_u16(&mut buf[13..15], name_len as u16);
        BigEndian::write_u16(&mut buf[15..17], info_len as u16);
        Ok(buf)
    }

    fn is_tombstone(&self) -> bool {
        matches!(self, Self::RemoveFile(..) | Self::RemoveDir(..))
    }
}

impl ProtobufDictionary for FileDictionaryV2 {
    type Item = DataKeyDictionaryItemV2;
    fn apply(&mut self, item: &Self::Item) -> Result<()> {
        match item {
            Self::Item::InsertFile(dir_id, name, info) => {
                self.dir_files
                    .entry(*dir_id)
                    .or_default()
                    .files
                    .insert(name.to_owned(), info.clone());
            }
            Self::Item::RemoveFile(dir_id, name) => {
                if let Some(files) = self.dir_files.get_mut(dir_id) {
                    files.files.remove(name);
                } else {
                    return Err(box_err!("directory not found: {dir_id} {name}"));
                }
            }
            Self::Item::InsertDir(dir_id, name) => {
                self.dirs.insert(name.to_owned(), *dir_id);
            }
            Self::Item::RemoveDir(dir_id, name) => {
                self.dirs.remove(name);
                self.dir_files.remove(dir_id);
            }
        }
        Ok(())
    }
}

/// DictionaryFile is used to store log style file dictionary.
///
/// Layout:
/// ```text
/// Encrypted File Header + FileDictionary Content | Record Header + Record Content | etc
/// ```
#[derive(Debug)]
pub struct DictionaryFile<T: ProtobufDictionary> {
    base: PathBuf,
    name: String,
    // Avoid reopening the file every time when append.
    append_file: Option<File>,
    // Internal file dictionary to avoid recovery before rewrite and to check if compaction
    // is needed.
    dict: T,
    // Whether to enable v2 format to, upon update, append updates as logs, instead of rewrite the
    // whole file.
    enable_log: bool,
    // Determine whether compact the log.
    file_rewrite_threshold: u64,
    // Record the number of `Remove` to determine whether compact the log.
    removed: u64,
    // Record size of the file.
    file_size: usize,
}

impl<T: ProtobufDictionary> DictionaryFile<T> {
    pub fn new<P: AsRef<Path>>(
        base: P,
        name: &str,
        enable_log: bool,
        file_rewrite_threshold: u64,
    ) -> Result<Self> {
        let mut dict_file = Self {
            base: base.as_ref().to_path_buf(),
            name: name.to_owned(),
            append_file: None,
            dict: T::default(),
            enable_log,
            file_rewrite_threshold,
            removed: 0,
            file_size: 0,
        };
        dict_file.rewrite()?;
        // dict_file.update_metrics();
        Ok(dict_file)
    }

    pub fn open<P: AsRef<Path>>(
        base: P,
        name: &str,
        enable_log: bool,
        file_rewrite_threshold: u64,
        skip_rewrite: bool,
    ) -> Result<Self> {
        let mut dict_file = Self {
            base: base.as_ref().to_path_buf(),
            name: name.to_owned(),
            append_file: None,
            dict: T::default(),
            enable_log,
            file_rewrite_threshold,
            removed: 0,
            file_size: 0,
        };

        let file_dict = dict_file.recovery()?;
        if !skip_rewrite {
            dict_file.rewrite()?;
        }
        // dict_file.update_metrics();
        Ok(dict_file)
    }

    /// Get the file path.
    pub fn file_path(&self) -> PathBuf {
        self.base.join(&self.name)
    }

    #[inline]
    pub fn dict(&self) -> &T {
        &self.dict
    }

    /// Rewrite the log file to reduce file size and reduce the time of next
    /// recovery.
    fn rewrite(&mut self) -> Result<()> {
        let dict_bytes = self.dict.write_to_bytes()?;
        if self.enable_log {
            let origin_path = self.file_path();
            let mut tmp_path = origin_path.clone();
            tmp_path.set_extension(format!("{}.{}", thread_rng().next_u64(), TMP_FILE_SUFFIX));
            let mut tmp_file = OpenOptions::new()
                .create(true)
                .write(true)
                .open(&tmp_path)
                .unwrap();

            let header = Header::new(&dict_bytes, Version::V2);
            tmp_file.write_all(&header.to_bytes())?;
            tmp_file.write_all(&dict_bytes)?;
            tmp_file.sync_all()?;

            // Replace old file with the tmp file aomticlly.
            rename(&tmp_path, &origin_path)?;
            let base_dir = File::open(&self.base)?;
            base_dir.sync_all()?;
            let file = OpenOptions::new().append(true).open(origin_path)?;
            self.append_file.replace(file);
        } else {
            let file = EncryptedFile::new(&self.base, &self.name);
            file.write(&dict_bytes, &PlaintextBackend::default())?;
        }
        // rough size, excluding EncryptedFile meta.
        self.file_size = dict_bytes.len();
        Ok(())
    }

    /// Recovery from the log file.
    pub fn recovery(&mut self) -> Result<()> {
        let mut f = OpenOptions::new().read(true).open(self.file_path())?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        self.file_size = buf.len();

        // parse the file dictionary
        let (header, content, mut remained) = Header::parse(buf.as_slice())?;
        let mut dict = T::default();
        match header.version() {
            Version::V1 => {
                let mut encrypted_content = EncryptedContent::default();
                encrypted_content.merge_from_bytes(content)?;
                let content = PlaintextBackend::default().decrypt(&encrypted_content)?;
                dict.merge_from_bytes(&content)?;
            }
            Version::V2 => {
                dict.merge_from_bytes(content)?;
                let mut last_record_name = String::new();
                // parse the remained records
                while !remained.is_empty() {
                    // If the parsing get errors, manual intervention is required to recover.
                    match T::Item::parse(&mut remained) {
                        Ok(item) => dict.apply(&item)?,
                        Err(e @ Error::TailRecordParseIncomplete) => {
                            warn!(
                                "{:?} occurred and the last complete filename is {}",
                                e, last_record_name
                            );
                            break;
                        }
                        Err(e) => {
                            // This error is unrecoverable and manual intervention is required.
                            set_panic_mark();
                            return Err(e);
                        }
                    }
                }
            }
        }

        self.dict = dict;
        Ok(())
    }

    /// Append an insert operation to the log file.
    ///
    /// Warning: `self.write(file_dict)` must be called before.
    // pub fn insert(&mut self, name: &str, info: &FileInfo) -> Result<()> {
    pub fn add(&mut self, item: &T::Item) -> Result<()> {
        self.dict.apply(item)?;
        if self.enable_log {
            let file = self.append_file.as_mut().unwrap();
            let bytes = item.as_bytes()?;
            file.write_all(&bytes)?;
            file.sync_all()?;

            self.file_size += bytes.len();
            if item.is_tombstone() {
                self.removed += 1;
            }
            self.check_compact()?;
        } else {
            self.rewrite()?;
        }
        // self.update_metrics();
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

    // fn update_metrics(&self) {
    //     ENCRYPTION_FILE_SIZE_GAUGE
    //         .with_label_values(&["file_dictionary"])
    //         .set(self.file_size as _);
    //     ENCRYPTION_FILE_NUM_GAUGE.set(self.file_dict.files.len() as _);
    // }
}

#[cfg(test)]
mod tests {
    use kvproto::encryptionpb::EncryptionMethod;

    use super::*;
    use crate::{encrypted_file::EncryptedFile, Error};

    fn test_file_dict_file_normal(enable_log: bool) {
        let tempdir = tempfile::tempdir().unwrap();
        let mut file_dict_file = DictionaryFile::<FileDictionary>::new(
            tempdir.path(),
            "test_file_dict_file",
            enable_log,
            2, // file_rewrite_threshold
        )
        .unwrap();
        let info1 = create_file_info(1, EncryptionMethod::Aes256Ctr);
        let info2 = create_file_info(2, EncryptionMethod::Unknown);
        let info3 = create_file_info(3, EncryptionMethod::Aes128Ctr);
        let info4 = create_file_info(4, EncryptionMethod::Aes128Ctr);
        let info5 = create_file_info(3, EncryptionMethod::Aes128Ctr);

        file_dict_file.insert("info1", &info1).unwrap();
        file_dict_file.insert("info2", &info2).unwrap();
        file_dict_file.insert("info3", &info3).unwrap();

        let file_dict = file_dict_file.recovery().unwrap();

        assert_eq!(*file_dict.files.get("info1").unwrap(), info1);
        assert_eq!(*file_dict.files.get("info2").unwrap(), info2);
        assert_eq!(*file_dict.files.get("info3").unwrap(), info3);
        assert_eq!(file_dict.files.len(), 3);

        file_dict_file.remove("info2").unwrap();
        file_dict_file.remove("info1").unwrap();
        file_dict_file.insert("info2", &info4).unwrap();

        let file_dict = file_dict_file.recovery().unwrap();
        assert_eq!(file_dict.files.get("info1"), None);
        assert_eq!(*file_dict.files.get("info2").unwrap(), info4);
        assert_eq!(*file_dict.files.get("info3").unwrap(), info3);
        assert_eq!(file_dict.files.len(), 2);

        file_dict_file.insert("info5", &info5).unwrap();
        file_dict_file.remove("info3").unwrap();

        let file_dict = file_dict_file.recovery().unwrap();
        assert_eq!(file_dict.files.get("info1"), None);
        assert_eq!(*file_dict.files.get("info2").unwrap(), info4);
        assert_eq!(file_dict.files.get("info3"), None);
        assert_eq!(*file_dict.files.get("info5").unwrap(), info5);
        assert_eq!(file_dict.files.len(), 2);
    }

    #[test]
    fn test_file_dict_file_normal_v1() {
        test_file_dict_file_normal(false /* enable_log */);
    }

    #[test]
    fn test_file_dict_file_normal_v2() {
        test_file_dict_file_normal(true /* enable_log */);
    }

    fn test_file_dict_file_existed(enable_log: bool) {
        let tempdir = tempfile::tempdir().unwrap();
        let mut file_dict_file = DictionaryFile::<FileDictionary>::new(
            tempdir.path(),
            "test_file_dict_file",
            enable_log,
            2, // file_rewrite_threshold
        )
        .unwrap();

        let info = create_file_info(1, EncryptionMethod::Aes256Ctr);
        file_dict_file.insert("info", &info).unwrap();

        let (_, file_dict) = DictionaryFile::<FileDictionary>::open(
            tempdir.path(),
            "test_file_dict_file",
            true,  // enable_log
            2,     // file_rewrite_threshold
            false, // skip_rewrite
        )
        .unwrap();
        assert_eq!(*file_dict.files.get("info").unwrap(), info);
    }

    #[test]
    fn test_file_dict_file_existed_v1() {
        test_file_dict_file_existed(false /* enable_log */);
    }

    #[test]
    fn test_file_dict_file_existed_v2() {
        test_file_dict_file_existed(true /* enable_log */);
    }

    fn test_file_dict_file_not_existed(enable_log: bool) {
        let tempdir = tempfile::tempdir().unwrap();
        let ret = DictionaryFile::<FileDictionary>::open(
            tempdir.path(),
            "test_file_dict_file",
            enable_log,
            2,     // file_rewrite_threshold
            false, // skip_rewrite
        );
        assert!(matches!(ret, Err(Error::Io(_))));
    }

    #[test]
    fn test_file_dict_file_not_existed_v1() {
        test_file_dict_file_not_existed(false /* enable_log */);
    }

    #[test]
    fn test_file_dict_file_not_existed_v2() {
        test_file_dict_file_not_existed(true /* enable_log */);
    }

    #[test]
    fn test_file_dict_file_update_from_v1() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut file_dict = FileDictionary::default();

        let info1 = create_file_info(1, EncryptionMethod::Aes256Ctr);
        let info2 = create_file_info(2, EncryptionMethod::Aes256Ctr);
        file_dict.files.insert("f1".to_owned(), info1);
        file_dict.files.insert("f2".to_owned(), info2);

        let file = EncryptedFile::new(tempdir.path(), "test_file_dict_file");
        file.write(
            &file_dict.write_to_bytes().unwrap(),
            &PlaintextBackend::default(),
        )
        .unwrap();

        let (_, file_dict_read) = DictionaryFile::<FileDictionary>::open(
            tempdir.path(),
            "test_file_dict_file",
            true,  // enable_log
            2,     // file_rewrite_threshold
            false, // skip_rewrite
        )
        .unwrap();
        assert_eq!(file_dict, file_dict_read);
    }

    #[test]
    fn test_file_dict_file_downgrade_from_v2() {
        let tempdir = tempfile::tempdir().unwrap();
        let info1 = create_file_info(1, EncryptionMethod::Aes256Ctr);
        let info2 = create_file_info(2, EncryptionMethod::Aes256Ctr);
        let info3 = create_file_info(2, EncryptionMethod::Aes256Ctr);
        let info4 = create_file_info(2, EncryptionMethod::Aes256Ctr);
        // write a v2 file.
        {
            let mut file_dict = DictionaryFile::<FileDictionary>::new(
                tempdir.path(),
                "test_file_dict_file",
                true, // enable_log
                1000, // file_rewrite_threshold
            )
            .unwrap();

            file_dict.insert("f1", &info1).unwrap();
            file_dict.insert("f2", &info2).unwrap();
            file_dict.insert("f3", &info3).unwrap();

            file_dict.insert("f4", &info4).unwrap();
            file_dict.remove("f3").unwrap();

            file_dict.remove("f2").unwrap();
        }
        // Try open as v1 file. Should fail.
        {
            let file_dict_file = EncryptedFile::new(tempdir.path(), "test_file_dict_file");
            assert!(matches!(
                file_dict_file.read(&PlaintextBackend::default()),
                Err(Error::Other(_))
            ));
        }
        // Try open as v2 file.
        {
            let (_, file_dict) = DictionaryFile::<FileDictionary>::open(
                tempdir.path(),
                "test_file_dict_file",
                true, // enable_log
                1000, // file_rewrite_threshold
                true, // skip_rewrite
            )
            .unwrap();
            assert_eq!(*file_dict.files.get("f1").unwrap(), info1);
            assert_eq!(file_dict.files.get("f2"), None);
            assert_eq!(file_dict.files.get("f3"), None);
            assert_eq!(*file_dict.files.get("f4").unwrap(), info4);
        }
        // Downgrade to v1 file.
        {
            let (_, file_dict) = DictionaryFile::<FileDictionary>::open(
                tempdir.path(),
                "test_file_dict_file",
                false, // enable_log
                1000,  // file_rewrite_threshold
                false, // skip_rewrite
            )
            .unwrap();
            assert_eq!(*file_dict.files.get("f1").unwrap(), info1);
            assert_eq!(file_dict.files.get("f2"), None);
            assert_eq!(file_dict.files.get("f3"), None);
            assert_eq!(*file_dict.files.get("f4").unwrap(), info4);
        }
        // Try open as v1 file. Should success.
        {
            let file_dict_file = EncryptedFile::new(tempdir.path(), "test_file_dict_file");
            let file_bytes = file_dict_file.read(&PlaintextBackend::default()).unwrap();
            let mut file_dict = FileDictionary::default();
            file_dict.merge_from_bytes(&file_bytes).unwrap();
            assert_eq!(*file_dict.files.get("f1").unwrap(), info1);
            assert_eq!(file_dict.files.get("f2"), None);
            assert_eq!(file_dict.files.get("f3"), None);
            assert_eq!(*file_dict.files.get("f4").unwrap(), info4);
        }
    }

    fn create_file_info(id: u64, method: EncryptionMethod) -> FileInfo {
        FileInfo {
            key_id: id,
            method,
            ..Default::default()
        }
    }
}
