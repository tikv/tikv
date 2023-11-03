// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_tiflash::EngineStoreHub;
use proxy_ffi::interfaces_ffi::EngineStoreServerHelper;
pub struct TiFlashEngineStoreHub {
    pub engine_store_server_helper: &'static EngineStoreServerHelper,
    pub store_id: std::cell::RefCell<u64>,
}
unsafe impl Send for TiFlashEngineStoreHub {}
unsafe impl Sync for TiFlashEngineStoreHub {}

impl EngineStoreHub for TiFlashEngineStoreHub {
    fn set_store_id(&self, p: u64) {
        *self.store_id.borrow_mut() = p;
    }
    fn get_store_id(&self) -> u64 {
        *(self.store_id.borrow())
    }
}
