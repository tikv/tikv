// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

#![allow(unused)]

use byteorder::{BigEndian, ByteOrder};
use mock_engine_store::interfaces_ffi::BaseBuffView;
use more_asserts::assert_gt;
use proxy_ffi::{
    interfaces_ffi::{ColumnFamilyType, EngineIteratorSeekType, SSTFormatKind},
    snapshot_reader_impls::{sst_file_reader::SSTFileReader, tablet_reader::TabletReader, *},
};

pub fn from_hex(key: &str) -> Result<Vec<u8>, hex::FromHexError> {
    if key.starts_with("0x") || key.starts_with("0X") {
        return hex::decode(&key[2..]);
    }
    hex::decode(key)
}

pub unsafe fn must_get_in_tablet_snapshot(path: &str, cf: ColumnFamilyType, key: &str) {
    // key is something like
    // "7480000000000000FF795F720380000000FF0000026303800000FF0000017801000000FCF9DE534E2797FB83"
    // .
    let reader = TabletReader::ffi_get_cf_file_reader(path, cf, None);
    assert_eq!(reader.kind, SSTFormatKind::KIND_TABLET);
    let encoded = from_hex(key).unwrap();
    let bf = BaseBuffView {
        data: encoded.as_ptr() as *const _,
        len: encoded.len() as u64,
    };

    ffi_sst_reader_seek(reader.clone(), cf, EngineIteratorSeekType::Key, bf);
    assert_eq!(ffi_sst_reader_remained(reader.clone(), cf), 1);
    let actual = ffi_sst_reader_key(reader.clone(), cf);
    assert_eq!(actual.to_slice(), encoded);
}

pub unsafe fn must_not_get_in_tablet_snapshot(path: &str, cf: ColumnFamilyType, key: &str) {
    // key is something like
    // "7480000000000000FF795F720380000000FF0000026303800000FF0000017801000000FCF9DE534E2797FB83"
    // .
    let reader = TabletReader::ffi_get_cf_file_reader(path, cf, None);
    assert_eq!(reader.kind, SSTFormatKind::KIND_TABLET);
    let encoded = from_hex(key).unwrap();
    let bf = BaseBuffView {
        data: encoded.as_ptr() as *const _,
        len: encoded.len() as u64,
    };

    ffi_sst_reader_seek(reader.clone(), cf, EngineIteratorSeekType::Key, bf);
    if ffi_sst_reader_remained(reader.clone(), cf) == 0 {
        return;
    }
    let actual = ffi_sst_reader_key(reader.clone(), cf);
    assert_ne!(actual.to_slice(), encoded);
}

pub mod RecordFormat {
    // These are functions from TiFlash.
    #![allow(non_snake_case)]

    use super::*;
    const ENC_GROUP_SIZE: usize = 8;
    const ENC_MARKER: u8 = 0xff;
    pub fn decodeInt64(p: &[u8]) -> i64 {
        const SIGN_MASK: u64 = 1u64 << 63;
        let y = (BigEndian::read_u64(p) ^ SIGN_MASK) as i64;
        y
    }

    pub fn getHandleID(p: &[u8]) -> i64 {
        decodeInt64(p)
    }

    pub fn getRawTiDBPK(decode: &[u8]) -> Vec<u8> {
        let user_key = getUserKey(decode);
        const RAW_KEY_NO_HANDLE_SIZE: usize = 1 + 8 + 2;
        user_key[RAW_KEY_NO_HANDLE_SIZE..].to_vec()
    }

    pub fn getUserKey(data: &[u8]) -> Vec<u8> {
        removeKeyspaceID(data)
    }

    pub fn removeKeyspaceID(data: &[u8]) -> Vec<u8> {
        const KEYSPACE_PREFIX_LEN: usize = 4;
        const TXN_MODE_PREFIX: u8 = 'x' as u8;
        if data.len() < KEYSPACE_PREFIX_LEN || data[0] != TXN_MODE_PREFIX {
            return data.to_vec();
        }
        data[KEYSPACE_PREFIX_LEN..].to_vec()
    }

    pub fn getTableId(data: &[u8]) -> i64 {
        decodeInt64(&data[1..])
    }

    fn checkKeyPaddingValid(key: &[u8], ptr: usize, pad_size: u8) -> bool {
        let p = unsafe { *(&key[ptr..] as *const [u8] as *const u64) }
            >> ((ENC_GROUP_SIZE - pad_size as usize) * 8);
        p == 0
    }

    pub fn decodeTiKVKeyFull(key: &[u8]) -> Vec<u8> {
        let mut res: Vec<u8> = vec![];
        let chunk_len: usize = ENC_GROUP_SIZE + 1;

        for ptr in (0..).step_by(chunk_len) {
            if ptr + chunk_len > key.len() {
                panic!("Unexpexcted EOF");
            }
            let marker = key[ptr + ENC_GROUP_SIZE];
            let pad_size: u8 = ENC_MARKER - marker;
            if pad_size == 0 {
                let mut v = key[ptr..ptr + ENC_GROUP_SIZE].to_vec();
                res.append(&mut v);
                continue;
            }
            if pad_size as usize > ENC_GROUP_SIZE {
                panic!("key padding");
            }
            let a: usize = ptr + ENC_GROUP_SIZE - (pad_size as usize);
            let mut v = key[ptr..a].to_vec();
            res.append(&mut v);

            if !checkKeyPaddingValid(key, ptr, pad_size) {
                panic!("Key padding, wrong end")
            }
            return res;
        }
        unreachable!()
    }
}

pub fn parse_handle_id(tikv_key: &[u8]) -> i64 {
    let decode = RecordFormat::decodeTiKVKeyFull(tikv_key);
    let pk = RecordFormat::getRawTiDBPK(&decode);
    let handle = RecordFormat::getHandleID(&pk);
    handle
}

pub fn parse_table_id(tikv_key: &[u8]) -> i64 {
    let handle = RecordFormat::getTableId(&tikv_key);
    handle
}

pub unsafe fn read_sst_file(path: &str) {
    let reader = SSTFileReader::ffi_get_cf_file_reader(path, None);
    assert_eq!(reader.kind, SSTFormatKind::KIND_SST);

    let mut prev: Option<Vec<u8>> = None;
    loop {
        let r = ffi_sst_reader_remained(reader.clone(), ColumnFamilyType::Write);
        if r != 1 {
            let p: &[u8] = &prev.unwrap();
            tikv_util::info!(
                "End table id {} key {}",
                parse_table_id(p),
                parse_handle_id(p)
            );
            break;
        }
        let k = ffi_sst_reader_key(reader.clone(), ColumnFamilyType::Write);
        let ks = k.to_slice();
        if let Some(p) = prev {
            let ps: &[u8] = &p;
            more_asserts::assert_ge!(ks, ps);
            more_asserts::assert_ge!(ks[11..], ps[11..]);
            let hprev = parse_handle_id(ps);
            let h = parse_handle_id(ks);
            assert_gt!(h, hprev);
        } else {
            tikv_util::info!(
                "Start table id {} key {}",
                parse_table_id(ks),
                parse_handle_id(ks)
            );
        }
        prev = Some(ks.to_vec());
        // tikv_util::info!("AAA {:?} {:?} {} {}", ks, &ks[11..], ks.len(),
        // BigEndian::read_u64(&ks[12..]));
        ffi_sst_reader_next(reader.clone(), ColumnFamilyType::Write);
    }
}
