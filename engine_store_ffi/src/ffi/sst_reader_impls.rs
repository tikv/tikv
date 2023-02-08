// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use std::{cell::RefCell, pin::Pin, sync::Arc};

use encryption::DataKeyManager;
use engine_rocks::{get_env, RocksSstIterator, RocksSstReader};
use engine_traits::{IterOptions, Iterator, RefIterable, SstReader};

use super::{
    interfaces_ffi::{
        BaseBuffView, ColumnFamilyType, RaftStoreProxyPtr, RawVoidPtr, SSTReaderInterfaces,
        SSTReaderPtr, SSTView, SSTViewVec,
    },
    LockCFFileReader,
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
        }
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

impl From<RawVoidPtr> for SSTReaderPtr {
    fn from(pre: RawVoidPtr) -> Self {
        Self { inner: pre }
    }
}

#[allow(clippy::clone_on_copy)]
impl Clone for SSTReaderPtr {
    fn clone(&self) -> SSTReaderPtr {
        SSTReaderPtr {
            inner: self.inner.clone(),
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
    let key_manager = &proxy_ptr.as_ref().key_manager;
    match view.type_ {
        ColumnFamilyType::Lock => {
            LockCFFileReader::ffi_get_cf_file_reader(path, key_manager.as_ref()).into()
        }
        _ => SSTFileReader::ffi_get_cf_file_reader(path, key_manager.clone()).into(),
    }
}

pub unsafe extern "C" fn ffi_sst_reader_remained(
    mut reader: SSTReaderPtr,
    type_: ColumnFamilyType,
) -> u8 {
    match type_ {
        ColumnFamilyType::Lock => reader.as_mut_lock().ffi_remained(),
        _ => reader.as_mut().ffi_remained(),
    }
}

pub unsafe extern "C" fn ffi_sst_reader_key(
    mut reader: SSTReaderPtr,
    type_: ColumnFamilyType,
) -> BaseBuffView {
    match type_ {
        ColumnFamilyType::Lock => reader.as_mut_lock().ffi_key(),
        _ => reader.as_mut().ffi_key(),
    }
}

pub unsafe extern "C" fn ffi_sst_reader_val(
    mut reader: SSTReaderPtr,
    type_: ColumnFamilyType,
) -> BaseBuffView {
    match type_ {
        ColumnFamilyType::Lock => reader.as_mut_lock().ffi_val(),
        _ => reader.as_mut().ffi_val(),
    }
}

pub unsafe extern "C" fn ffi_sst_reader_next(mut reader: SSTReaderPtr, type_: ColumnFamilyType) {
    match type_ {
        ColumnFamilyType::Lock => reader.as_mut_lock().ffi_next(),
        _ => reader.as_mut().ffi_next(),
    }
}

pub unsafe extern "C" fn ffi_gc_sst_reader(reader: SSTReaderPtr, type_: ColumnFamilyType) {
    match type_ {
        ColumnFamilyType::Lock => {
            drop(Box::from_raw(reader.inner as *mut LockCFFileReader));
        }
        _ => {
            drop(Box::from_raw(reader.inner as *mut SSTFileReader));
        }
    }
}

pub fn into_sst_views(snaps: Vec<(&[u8], ColumnFamilyType)>) -> Vec<SSTView> {
    let mut snaps_view = vec![];
    for (path, cf) in snaps {
        snaps_view.push(SSTView {
            type_: cf,
            path: path.into(),
        })
    }
    snaps_view
}

impl From<Pin<&Vec<SSTView>>> for SSTViewVec {
    fn from(snaps_view: Pin<&Vec<SSTView>>) -> Self {
        Self {
            views: snaps_view.as_ptr(),
            len: snaps_view.len() as u64,
        }
    }
}

pub struct SSTFileReader<'a> {
    iter: RefCell<Option<RocksSstIterator<'a>>>,
    remained: RefCell<bool>,
    inner: RocksSstReader,
}

impl<'a> SSTFileReader<'a> {
    fn ffi_get_cf_file_reader(path: &str, key_manager: Option<Arc<DataKeyManager>>) -> RawVoidPtr {
        let env = get_env(key_manager, None).unwrap();
        let sst_reader_res = RocksSstReader::open_with_env(path, Some(env));
        if let Err(ref e) = sst_reader_res {
            tikv_util::error!("Can not open sst file {:?}", e);
        }
        let sst_reader = sst_reader_res.unwrap();
        sst_reader.verify_checksum().unwrap();
        if let Err(e) = sst_reader.verify_checksum() {
            tikv_util::error!("verify_checksum sst file error {:?}", e);
            panic!("verify_checksum sst file error {:?}", e);
        }
        let b = Box::new(SSTFileReader {
            iter: RefCell::new(None),
            remained: RefCell::new(false),
            inner: sst_reader,
        });
        // Can't call `create_iter` due to self-referencing.
        Box::into_raw(b) as *mut _
    }

    pub fn create_iter(&'a self) {
        let _ = self.iter.borrow_mut().insert(
            self.inner
                .iter(IterOptions::default())
                .expect("fail gen iter"),
        );
        *self.remained.borrow_mut() = self
            .iter
            .borrow_mut()
            .as_mut()
            .expect("fail get iter")
            .seek_to_first()
            .unwrap();
    }

    pub fn ffi_remained(&'a self) -> u8 {
        if self.iter.borrow().is_none() {
            self.create_iter();
        }
        *self.remained.borrow() as u8
    }

    pub fn ffi_key(&'a self) -> BaseBuffView {
        if self.iter.borrow().is_none() {
            self.create_iter();
        }
        let b = self.iter.borrow();
        let iter = b.as_ref().unwrap();
        let ori_key = keys::origin_key(iter.key());
        ori_key.into()
    }

    pub fn ffi_val(&'a self) -> BaseBuffView {
        if self.iter.borrow().is_none() {
            self.create_iter();
        }
        let b = self.iter.borrow();
        let iter = b.as_ref().unwrap();
        let val = iter.value();
        val.into()
    }

    pub fn ffi_next(&'a mut self) {
        if self.iter.borrow().is_none() {
            self.create_iter();
        }
        let mut b = self.iter.borrow_mut();
        let iter = b.as_mut().unwrap();
        *self.remained.borrow_mut() = iter.next().unwrap();
    }
}
