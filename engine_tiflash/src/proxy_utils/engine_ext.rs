// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use proxy_ffi::{
    gen_engine_store_server_helper, interfaces_ffi,
    interfaces_ffi::{EngineStoreServerHelper, PageAndCppStrWithView, RawCppPtr, RawVoidPtr},
};

use crate::RocksEngine;

impl RocksEngine {
    pub fn helper(&self) -> &'static EngineStoreServerHelper {
        gen_engine_store_server_helper(self.proxy_ext.engine_store_server_helper)
    }
    pub fn get_store_stats(&self) -> interfaces_ffi::StoreStats {
        let helper = self.helper();
        helper.handle_compute_store_stats()
    }
}
#[derive(Clone, Debug)]
pub struct PageStorageExt {
    pub engine_store_server_helper: isize,
}

#[cfg(feature = "enable-pagestorage")]
impl PageStorageExt {
    fn helper(&self) -> &'static EngineStoreServerHelper {
        gen_engine_store_server_helper(self.engine_store_server_helper)
    }

    pub fn create_write_batch(&self) -> RawCppPtr {
        // TODO There are too many dummy write batch created in non-uni-ps impl.
        // Need to work out a solution for this.
        // See engine_tiflash/src/write_batch.rs.
        self.helper().create_write_batch().into()
    }

    pub fn destroy_write_batch(&self, wb_wrapper: &RawCppPtr) {
        self.helper()
            .gc_raw_cpp_ptr(wb_wrapper.ptr, wb_wrapper.type_);
    }

    pub fn consume_write_batch(&self, wb: RawVoidPtr) {
        self.helper().consume_write_batch(wb)
    }

    pub fn write_batch_size(&self, wb: RawVoidPtr) -> usize {
        self.helper().write_batch_size(wb) as usize
    }

    pub fn write_batch_is_empty(&self, wb: RawVoidPtr) -> bool {
        self.helper().write_batch_is_empty(wb) != 0
    }

    pub fn write_batch_merge(&self, lwb: RawVoidPtr, rwb: RawVoidPtr) {
        self.helper().write_batch_merge(lwb, rwb)
    }

    pub fn write_batch_clear(&self, wb: RawVoidPtr) {
        self.helper().write_batch_clear(wb)
    }

    pub fn write_batch_put_page(&self, wb: RawVoidPtr, page_id: &[u8], page: &[u8]) {
        self.helper()
            .write_batch_put_page(wb, page_id.into(), page.into())
    }

    pub fn write_batch_del_page(&self, wb: RawVoidPtr, page_id: &[u8]) {
        self.helper().write_batch_del_page(wb, page_id.into())
    }

    pub fn read_page(&self, page_id: &[u8]) -> Option<Vec<u8>> {
        // TODO maybe we can steal memory from C++ here to reduce redundant copy?
        let value = self.helper().read_page(page_id.into());
        return if value.view.len == 0 {
            None
        } else {
            Some(value.view.to_slice().to_vec())
        };
    }

    pub fn scan_page(
        &self,
        start_page_id: &[u8],
        end_page_id: &[u8],
        f: &mut dyn FnMut(&[u8], &[u8]) -> engine_traits::Result<bool>,
    ) {
        let values = self
            .helper()
            .scan_page(start_page_id.into(), end_page_id.into());
        let arr = values.inner as *mut PageAndCppStrWithView;
        for i in 0..values.len {
            let value = unsafe { &*arr.offset(i as isize) };
            if value.page_view.len != 0 {
                f(value.key_view.to_slice(), value.page_view.to_slice()).unwrap();
            }
        }
    }
}
