// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use encryption::{DataKeyDictionaryItem, DictionaryFile};
use kvproto::encryptionpb::{EncryptionMethod, FileDictionary, FileInfo};

#[test]
fn test_file_dict_file_record_corrupted() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut file_dict_file = DictionaryFile::<FileDictionary>::new(
        tempdir.path(),
        "test_file_dict_file_record_corrupted_1",
        true,
        10, // file_rewrite_threshold
    )
    .unwrap();
    let info1 = create_file_info(1, EncryptionMethod::Aes256Ctr);
    let info2 = create_file_info(2, EncryptionMethod::Unknown);
    // 9 represents that the first 9 bytes will be discarded.
    // Crc32 (4 bytes) + File name length (2 bytes) + FileInfo length (2 bytes) +
    // Log type (1 bytes)
    fail::cfg("file_dict_log_append_incomplete", "return(9)").unwrap();
    file_dict_file
        .add(DataKeyDictionaryItem::Insert("info1".to_owned(), info1))
        .unwrap();
    fail::remove("file_dict_log_append_incomplete");
    file_dict_file
        .add(DataKeyDictionaryItem::Insert("info2".to_owned(), info2))
        .unwrap();
    // Intermediate record damage is not allowed.
    file_dict_file.recovery().unwrap_err();

    let mut file_dict_file = DictionaryFile::<FileDictionary>::new(
        tempdir.path(),
        "test_file_dict_file_record_corrupted_2",
        true,
        10, // file_rewrite_threshold
    )
    .unwrap();
    let info1 = create_file_info(1, EncryptionMethod::Aes256Ctr);
    let info2 = create_file_info(2, EncryptionMethod::Unknown);
    file_dict_file
        .add(DataKeyDictionaryItem::Insert("info1".to_owned(), info1.clone()))
        .unwrap();
    fail::cfg("file_dict_log_append_incomplete", "return(9)").unwrap();
    file_dict_file
        .add(DataKeyDictionaryItem::Insert("info2".to_owned(), info2))
        .unwrap();
    fail::remove("file_dict_log_append_incomplete");
    // The ending record can be discarded.
    file_dict_file.recovery().unwrap();
    assert_eq!(*file_dict_file.dict().files.get("info1").unwrap(), info1);
    assert_eq!(file_dict_file.dict().files.len(), 1);
}

fn create_file_info(id: u64, method: EncryptionMethod) -> FileInfo {
    FileInfo {
        key_id: id,
        method,
        ..Default::default()
    }
}
