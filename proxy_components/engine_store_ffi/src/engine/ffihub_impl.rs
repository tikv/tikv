// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_tiflash::EngineStoreHub;
use proxy_ffi::interfaces_ffi::EngineStoreServerHelper;
pub struct TiFlashEngineStoreHub {
    pub engine_store_server_helper: &'static EngineStoreServerHelper,
}
unsafe impl Send for TiFlashEngineStoreHub {}
unsafe impl Sync for TiFlashEngineStoreHub {}

impl EngineStoreHub for TiFlashEngineStoreHub {}
