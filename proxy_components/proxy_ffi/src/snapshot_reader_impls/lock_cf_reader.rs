// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    io::{BufReader, Read},
    sync::Arc,
};

use encryption::DataKeyManager;
use file_system::File;
use raftstore::store::snap::snap_io::get_decrypter_reader;
use tikv_util::codec::bytes::CompactBytesFromFileDecoder;

use crate::interfaces_ffi::{
    BaseBuffView, ColumnFamilyType, EngineIteratorSeekType, SSTFormatKind, SSTReaderPtr,
};

type LockCFDecoder = BufReader<Box<dyn Read + Send>>;

pub struct LockCFFileReader {
    decoder: LockCFDecoder,
    key: Vec<u8>,
    val: Vec<u8>,
}

impl LockCFFileReader {
    pub fn ffi_get_cf_file_reader(
        path: &str,
        key_mgr: Option<&Arc<DataKeyManager>>,
    ) -> SSTReaderPtr {
        let file = File::open(path).unwrap();
        let mut decoder: LockCFDecoder = if let Some(key_mgr) = key_mgr {
            let reader = get_decrypter_reader(path, key_mgr).unwrap();
            BufReader::new(reader)
        } else {
            BufReader::new(Box::new(file) as Box<dyn Read + Send>)
        };

        let key = decoder.decode_compact_bytes().unwrap();
        let mut val = vec![];
        if !key.is_empty() {
            val = decoder.decode_compact_bytes().unwrap();
        }

        SSTReaderPtr {
            inner: Box::into_raw(Box::new(LockCFFileReader { decoder, key, val })) as *mut _,
            kind: SSTFormatKind::KIND_SST,
        }
    }

    pub fn ffi_remained(&self) -> u8 {
        (!self.key.is_empty()) as u8
    }

    pub fn ffi_key(&self) -> BaseBuffView {
        let ori_key = keys::origin_key(&self.key);
        ori_key.into()
    }

    pub fn ffi_val(&self) -> BaseBuffView {
        self.val.as_slice().into()
    }

    pub fn ffi_next(&mut self) {
        let key = self.decoder.decode_compact_bytes().unwrap();
        if !key.is_empty() {
            self.val = self.decoder.decode_compact_bytes().unwrap();
        } else {
            self.val.clear();
        }
        self.key = key;
    }

    pub fn ffi_seek(&self, _: ColumnFamilyType, _: EngineIteratorSeekType, _: BaseBuffView) {
        // Do nothing. Since seek is only usable in tablet reader.
    }
}
