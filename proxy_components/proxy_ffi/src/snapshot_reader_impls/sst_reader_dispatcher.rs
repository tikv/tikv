// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
//! This is a wrapper of different impl of readers for SST.

use super::{sst_file_reader::*, tablet_reader::TabletReader, LockCFFileReader};
use crate::{
    interfaces_ffi::{
        BaseBuffView, ColumnFamilyType, EngineIteratorSeekType, RaftStoreProxyPtr,
        RustStrWithViewVec, SSTFormatKind, SSTReaderInterfaces, SSTReaderPtr, SSTView,
    },
    raftstore_proxy_helper_impls::RaftStoreProxyFFI,
};

#[allow(clippy::clone_on_copy)]
impl Clone for SSTReaderInterfaces {
    fn clone(&self) -> SSTReaderInterfaces {
        SSTReaderInterfaces {
            fn_get_sst_reader: self.fn_get_sst_reader.clone(),
            fn_remained: self.fn_remained.clone(),
            fn_key: self.fn_key.clone(),
            fn_value: self.fn_value.clone(),
            fn_next: self.fn_next.clone(),
            fn_gc: self.fn_gc.clone(),
            fn_kind: self.fn_kind.clone(),
            fn_seek: self.fn_seek.clone(),
            fn_approx_size: self.fn_approx_size.clone(),
            fn_get_split_keys: self.fn_get_split_keys.clone(),
        }
    }
}

/// All impl of SST reader will be dispatched by this ptr.
impl SSTReaderPtr {
    pub unsafe fn as_mut_sst_lock(&mut self) -> &mut LockCFFileReader {
        assert_eq!(self.kind, SSTFormatKind::KIND_SST);
        &mut *(self.inner as *mut LockCFFileReader)
    }

    pub unsafe fn as_mut_sst_other(&mut self) -> &mut SSTFileReader {
        assert_eq!(self.kind, SSTFormatKind::KIND_SST);
        &mut *(self.inner as *mut SSTFileReader)
    }

    unsafe fn as_mut_tablet(&mut self) -> &mut TabletReader {
        assert_eq!(self.kind, SSTFormatKind::KIND_TABLET);
        &mut *(self.inner as *mut TabletReader)
    }

    pub fn parse_kind(view: &SSTView) -> SSTFormatKind {
        let s = view.path.to_slice();
        if s.starts_with(b"!") {
            return SSTFormatKind::KIND_TABLET;
        }
        SSTFormatKind::KIND_SST
    }

    // TiKV don't make guarantee that a v1 sst file ends with ".sst".
    // So instead we mark v2's tablet format with prefix "!".
    // This encoding should exist only in memory.
    // TODO Work together for a uniformed name.
    pub fn encode_v2(s: &str) -> String {
        "!".to_owned() + s
    }

    pub fn decode_v2(s: &str) -> &str {
        &s[1..]
    }
}

#[allow(clippy::clone_on_copy)]
impl Clone for SSTReaderPtr {
    fn clone(&self) -> SSTReaderPtr {
        SSTReaderPtr {
            inner: self.inner.clone(),
            kind: self.kind,
        }
    }
}

#[allow(clippy::clone_on_copy)]
impl Clone for SSTView {
    fn clone(&self) -> SSTView {
        SSTView {
            type_: self.type_.clone(),
            path: self.path.clone(),
        }
    }
}

pub unsafe extern "C" fn ffi_make_sst_reader(
    view: SSTView,
    proxy_ptr: RaftStoreProxyPtr,
) -> SSTReaderPtr {
    let path = std::str::from_utf8_unchecked(view.path.to_slice());
    let key_manager = proxy_ptr.as_ref().maybe_key_manager();
    match SSTReaderPtr::parse_kind(&view) {
        SSTFormatKind::KIND_SST => match view.type_ {
            ColumnFamilyType::Lock => {
                LockCFFileReader::ffi_get_cf_file_reader(path, key_manager.as_ref())
            }
            _ => SSTFileReader::ffi_get_cf_file_reader(path, key_manager.clone()),
        },
        SSTFormatKind::KIND_TABLET => {
            let new_path = SSTReaderPtr::decode_v2(path);
            TabletReader::ffi_get_cf_file_reader(new_path, view.type_, key_manager.clone())
        }
    }
}

pub unsafe extern "C" fn ffi_sst_reader_remained(
    mut reader: SSTReaderPtr,
    type_: ColumnFamilyType,
) -> u8 {
    match reader.kind {
        SSTFormatKind::KIND_SST => match type_ {
            ColumnFamilyType::Lock => reader.as_mut_sst_lock().ffi_remained(),
            _ => reader.as_mut_sst_other().ffi_remained(),
        },
        SSTFormatKind::KIND_TABLET => reader.as_mut_tablet().ffi_remained(),
    }
}

pub unsafe extern "C" fn ffi_sst_reader_key(
    mut reader: SSTReaderPtr,
    type_: ColumnFamilyType,
) -> BaseBuffView {
    match reader.kind {
        SSTFormatKind::KIND_SST => match type_ {
            ColumnFamilyType::Lock => reader.as_mut_sst_lock().ffi_key(),
            _ => reader.as_mut_sst_other().ffi_key(),
        },
        SSTFormatKind::KIND_TABLET => reader.as_mut_tablet().ffi_key(),
    }
}

pub unsafe extern "C" fn ffi_sst_reader_val(
    mut reader: SSTReaderPtr,
    type_: ColumnFamilyType,
) -> BaseBuffView {
    match reader.kind {
        SSTFormatKind::KIND_SST => match type_ {
            ColumnFamilyType::Lock => reader.as_mut_sst_lock().ffi_val(),
            _ => reader.as_mut_sst_other().ffi_val(),
        },
        SSTFormatKind::KIND_TABLET => reader.as_mut_tablet().ffi_val(),
    }
}

pub unsafe extern "C" fn ffi_sst_reader_next(mut reader: SSTReaderPtr, type_: ColumnFamilyType) {
    match reader.kind {
        SSTFormatKind::KIND_SST => match type_ {
            ColumnFamilyType::Lock => reader.as_mut_sst_lock().ffi_next(),
            _ => reader.as_mut_sst_other().ffi_next(),
        },
        SSTFormatKind::KIND_TABLET => reader.as_mut_tablet().ffi_next(),
    }
}

pub unsafe extern "C" fn ffi_gc_sst_reader(reader: SSTReaderPtr, type_: ColumnFamilyType) {
    match reader.kind {
        SSTFormatKind::KIND_SST => match type_ {
            ColumnFamilyType::Lock => {
                drop(Box::from_raw(reader.inner as *mut LockCFFileReader));
            }
            _ => {
                drop(Box::from_raw(reader.inner as *mut SSTFileReader));
            }
        },
        SSTFormatKind::KIND_TABLET => {
            drop(Box::from_raw(reader.inner as *mut TabletReader));
        }
    }
}

pub unsafe extern "C" fn ffi_sst_reader_format_kind(
    reader: SSTReaderPtr,
    _: ColumnFamilyType,
) -> SSTFormatKind {
    reader.kind
}

pub unsafe extern "C" fn ffi_sst_reader_seek(
    mut reader: SSTReaderPtr,
    type_: ColumnFamilyType,
    seek_type: EngineIteratorSeekType,
    key: BaseBuffView,
) {
    match reader.kind {
        SSTFormatKind::KIND_SST => match type_ {
            ColumnFamilyType::Lock => reader.as_mut_sst_lock().ffi_seek(type_, seek_type, key),
            _ => reader.as_mut_sst_other().ffi_seek(type_, seek_type, key),
        },
        SSTFormatKind::KIND_TABLET => reader.as_mut_tablet().ffi_seek(type_, seek_type, key),
    }
}

pub unsafe extern "C" fn ffi_approx_size(mut reader: SSTReaderPtr, type_: ColumnFamilyType) -> u64 {
    match reader.kind {
        SSTFormatKind::KIND_SST => 0,
        SSTFormatKind::KIND_TABLET => reader.as_mut_tablet().ffi_approx_size(type_),
    }
}

// It will generate `splits_count-1` keys to make `splits_count` parts.
pub unsafe extern "C" fn ffi_get_split_keys(
    mut reader: SSTReaderPtr,
    splits_count: u64,
) -> RustStrWithViewVec {
    match reader.kind {
        SSTFormatKind::KIND_SST => RustStrWithViewVec::default(),
        SSTFormatKind::KIND_TABLET => reader.as_mut_tablet().ffi_get_split_keys(splits_count),
    }
}
