// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.
use std::{cell::RefCell, sync::Arc};

use encryption::DataKeyManager;
use engine_rocks::{get_env, RocksSstIterator, RocksSstReader};
use engine_traits::{IterOptions, Iterator, RefIterable, SstReader};

use super::KIND_SST;
use crate::interfaces_ffi::{BaseBuffView, SSTReaderPtr};
pub struct SSTFileReader<'a> {
    iter: RefCell<Option<RocksSstIterator<'a>>>,
    remained: RefCell<bool>,
    inner: RocksSstReader,
}

impl<'a> SSTFileReader<'a> {
    pub fn ffi_get_cf_file_reader(
        path: &str,
        key_manager: Option<Arc<DataKeyManager>>,
    ) -> SSTReaderPtr {
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
        SSTReaderPtr {
            inner: Box::into_raw(b) as *mut _,
            kind: KIND_SST,
        }
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
