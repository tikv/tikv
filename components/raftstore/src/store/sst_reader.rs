

use engine_rocks::{RocksSstIterator, RocksSstReader, get_env};
use engine_traits::{
    EncryptionKeyManager, EncryptionMethod, FileEncryptionInfo, Iterator, SeekKey, SstReader,
    CF_DEFAULT, CF_LOCK, CF_WRITE
};
use std::sync::Arc;
use std::io::{self, BufReader, ErrorKind, Read, Write};
use encryption::{self, DataKeyManager};
use file_system::File;
use crate::store::snap::snap_io::get_decrypter_reader;
use tikv_util::codec::bytes::CompactBytesFromFileDecoder;

pub type RawVoidPtr = *mut ::std::os::raw::c_void;

pub fn name_to_cf(cf: &str) -> ColumnFamilyType {
    if cf.is_empty() {
        return ColumnFamilyType::Default;
    }
    if cf == CF_LOCK {
        return ColumnFamilyType::Lock;
    } else if cf == CF_WRITE {
        return ColumnFamilyType::Write;
    } else if cf == CF_DEFAULT {
        return ColumnFamilyType::Default;
    }
    unreachable!()
}

impl From<RawVoidPtr> for SSTReaderPtr {
    fn from(pre: RawVoidPtr) -> Self {
        Self { inner: pre }
    }
}

pub struct SSTFileReader {
    iter: RocksSstIterator,
    remained: bool,
}

impl SSTFileReader {
    fn ffi_get_cf_file_reader(path: &str, key_manager: Option<Arc<DataKeyManager>>) -> RawVoidPtr {
        let env = get_env(key_manager, None).unwrap();
        let sst_reader = RocksSstReader::open_with_env(path, Some(env)).unwrap();
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        let remained = iter.seek(SeekKey::Start).unwrap();

        Box::into_raw(Box::new(SSTFileReader { iter, remained })) as *mut _
    }

    pub fn ffi_remained(&self) -> bool {
        self.remained
    }

    pub fn ffi_key(&self) -> & [u8] {
        keys::origin_key(self.iter.key())
    }

    pub fn ffi_val(&self) -> & [u8] {
        self.iter.value()
    }

    pub fn ffi_next(&mut self) {
        self.remained = self.iter.next().unwrap();
    }
}

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
    ) -> RawVoidPtr {
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

        Box::into_raw(Box::new(LockCFFileReader { decoder, key, val })) as *mut _
    }

    pub fn ffi_remained(&self) -> bool {
        !self.key.is_empty()
    }

    pub fn ffi_key(&self) -> Vec<u8> {
        keys::origin_key(&self.key).to_owned()
    }

    pub fn ffi_val(&self) -> Vec<u8> {
        self.val.as_slice().to_owned()
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
}


#[repr(C)]
#[derive(Debug)]
pub struct SSTView {
    pub type_: ColumnFamilyType,
    pub path: Vec<u8>,
}

#[repr(C)]
#[derive(Debug)]
pub struct SSTReaderPtr {
    pub inner: RawVoidPtr,
}

#[repr(u8)]
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub enum ColumnFamilyType {
    Lock = 0,
    Write = 1,
    Default = 2,
}

impl Clone for SSTReaderPtr {
    fn clone(&self) -> SSTReaderPtr {
        return SSTReaderPtr {
            inner: self.inner.clone(),
        };
    }
}

impl SSTReaderPtr {
    unsafe fn as_mut_lock(&mut self) -> &mut LockCFFileReader {
        &mut *(self.inner as *mut LockCFFileReader)
    }

    unsafe fn as_mut(&mut self) -> &mut SSTFileReader {
        &mut *(self.inner as *mut SSTFileReader)
    }
}

#[no_mangle]
pub unsafe fn ffi_get_sst_reader(
    path: &[u8],
    t: ColumnFamilyType,
    key_manager: &Option<Arc<DataKeyManager>>,
) -> SSTReaderPtr {
    let f = std::str::from_utf8(path).unwrap();
    match t {
        ColumnFamilyType::Lock => {
            LockCFFileReader::ffi_get_cf_file_reader(&f, key_manager.as_ref()).into()
        }
        _ => SSTFileReader::ffi_get_cf_file_reader(&f, key_manager.clone()).into(),
    }
}

#[no_mangle]
pub unsafe fn ffi_remained(mut reader: SSTReaderPtr, type_: ColumnFamilyType) -> bool {
    match type_ {
        ColumnFamilyType::Lock => reader.as_mut_lock().ffi_remained(),
        _ => reader.as_mut().ffi_remained(),
    }
}

#[no_mangle]
pub unsafe fn ffi_key(mut reader: SSTReaderPtr, type_: ColumnFamilyType) -> Vec<u8> {
    match type_ {
        ColumnFamilyType::Lock => reader.as_mut_lock().ffi_key().to_owned(),
        _ => reader.as_mut().ffi_key().to_owned(),
    }
}

#[no_mangle]
pub unsafe fn ffi_val(mut reader: SSTReaderPtr, type_: ColumnFamilyType) -> Vec<u8> {
    match type_ {
        ColumnFamilyType::Lock => reader.as_mut_lock().ffi_val().to_owned(),
        _ => reader.as_mut().ffi_val().to_owned(),
    }
}

#[no_mangle]
pub unsafe fn ffi_next(mut reader: SSTReaderPtr, type_: ColumnFamilyType) {
    match type_ {
        ColumnFamilyType::Lock => reader.as_mut_lock().ffi_next(),
        _ => reader.as_mut().ffi_next(),
    }
}

#[no_mangle]
pub unsafe fn ffi_gc(reader: SSTReaderPtr, type_: ColumnFamilyType) {
    match type_ {
        ColumnFamilyType::Lock => {
            Box::from_raw(reader.inner as *mut LockCFFileReader);
        }
        _ => {
            Box::from_raw(reader.inner as *mut SSTFileReader);
        }
    }
}

