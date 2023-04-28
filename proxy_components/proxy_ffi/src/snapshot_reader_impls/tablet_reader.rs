// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.
use std::{cell::RefCell, sync::Arc};

use encryption::DataKeyManager;
use engine_rocks::{RocksCfOptions, RocksDbOptions};
use engine_traits::{Iterable, Iterator};

use crate::{
    cf_to_name,
    interfaces_ffi::{
        BaseBuffView, ColumnFamilyType, EngineIteratorSeekType, SSTFormatKind, SSTReaderPtr,
    },
};

pub struct TabletReader {
    kv_engine: engine_rocks::RocksEngine,
    iter: RefCell<Option<engine_rocks::RocksEngineIterator>>,
    remained: RefCell<bool>,
    cf: ColumnFamilyType,
}

impl TabletReader {
    pub fn ffi_get_cf_file_reader(
        path: &str,
        cf: ColumnFamilyType,
        _key_manager: Option<Arc<DataKeyManager>>,
    ) -> SSTReaderPtr {
        let db_opts = RocksDbOptions::default();
        let cfopt = RocksCfOptions::default();

        let cf_opts = vec![(cf_to_name(cf), cfopt)];
        let cfds: Vec<_> = cf_opts
            .into_iter()
            .map(|(name, opt)| (name, opt.into_raw()))
            .collect();
        let db = rocksdb::DB::open_cf_for_read_only(db_opts.into_raw(), path, cfds, false).unwrap();
        let kv_engine = engine_rocks::RocksEngine::new(db);

        let tr = Box::new(TabletReader {
            kv_engine,
            iter: RefCell::new(None),
            remained: RefCell::new(false),
            cf,
        });
        SSTReaderPtr {
            inner: Box::into_raw(tr) as *mut _,
            kind: SSTFormatKind::KIND_TABLET,
        }
    }

    pub fn create_iter(&self) {
        let cf_name = cf_to_name(self.cf);
        let _ = self
            .iter
            .borrow_mut()
            .insert(self.kv_engine.iterator(cf_name).expect("fail gen iter"));
        *self.remained.borrow_mut() = self
            .iter
            .borrow_mut()
            .as_mut()
            .expect("fail get iter")
            .seek_to_first()
            .unwrap();
    }

    pub fn ffi_remained(&self) -> u8 {
        if self.iter.borrow().is_none() {
            self.create_iter();
        }
        *self.remained.borrow() as u8
    }

    pub fn ffi_key(&self) -> BaseBuffView {
        if self.iter.borrow().is_none() {
            self.create_iter();
        }
        let b = self.iter.borrow();
        let iter = b.as_ref().unwrap();
        let ori_key = keys::origin_key(iter.key());
        ori_key.into()
    }

    pub fn ffi_val(&self) -> BaseBuffView {
        if self.iter.borrow().is_none() {
            self.create_iter();
        }
        let b = self.iter.borrow();
        let iter = b.as_ref().unwrap();
        let ori_key = iter.value();
        ori_key.into()
    }

    pub fn ffi_next(&mut self) {
        if self.iter.borrow().is_none() {
            self.create_iter();
        }
        let mut b = self.iter.borrow_mut();
        let iter = b.as_mut().unwrap();
        *self.remained.borrow_mut() = iter.next().unwrap();
    }

    pub fn ffi_seek(&self, _: ColumnFamilyType, et: EngineIteratorSeekType, bf: BaseBuffView) {
        if self.iter.borrow().is_none() {
            self.create_iter();
        }
        let mut b = self.iter.borrow_mut();
        let iter = b.as_mut().unwrap();
        match et {
            EngineIteratorSeekType::First => {
                let _ = iter.seek_to_first();
            }
            EngineIteratorSeekType::Last => {
                let _ = iter.seek_to_last();
            }
            EngineIteratorSeekType::Key => {
                let dk = keys::data_key(bf.to_slice());
                let _ = iter.seek(&dk);
            }
        };
    }
}
