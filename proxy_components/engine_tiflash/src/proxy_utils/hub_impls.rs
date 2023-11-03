// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

pub trait EngineStoreHub {
    fn set_store_id(&self, p: u64);
    fn get_store_id(&self) -> u64;
}
