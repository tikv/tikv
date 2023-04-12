// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.
use std::sync::Arc;

use encryption::DataKeyManager;

use crate::interfaces_ffi::{BaseBuffView, SSTReaderPtr};

pub struct TabletReader {}

impl TabletReader {
    pub fn ffi_get_cf_file_reader(
        _path: &str,
        _key_manager: Option<Arc<DataKeyManager>>,
    ) -> SSTReaderPtr {
        todo!()
    }

    pub fn create_iter(&self) {
        todo!()
    }

    pub fn ffi_remained(&self) -> u8 {
        todo!()
    }

    pub fn ffi_key(&self) -> BaseBuffView {
        todo!()
    }

    pub fn ffi_val(&self) -> BaseBuffView {
        todo!()
    }

    pub fn ffi_next(&mut self) {
        todo!()
    }
}
