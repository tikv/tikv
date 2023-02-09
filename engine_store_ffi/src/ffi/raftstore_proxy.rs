// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc, RwLock,
};

use encryption::DataKeyManager;
use engine_traits::Peekable;

use super::{
    interfaces_ffi::{ConstRawVoidPtr, RaftProxyStatus, RaftStoreProxyPtr},
    raftstore_proxy_helper_impls::*,
    read_index_helper,
};
use crate::TiFlashEngine;

pub struct RaftStoreProxy {
    status: AtomicU8,
    key_manager: Option<Arc<DataKeyManager>>,
    read_index_client: Option<Box<dyn read_index_helper::ReadIndex>>,
    kv_engine: RwLock<Option<TiFlashEngine>>,
}

impl RaftStoreProxy {
    pub fn new(
        status: AtomicU8,
        key_manager: Option<Arc<DataKeyManager>>,
        read_index_client: Option<Box<dyn read_index_helper::ReadIndex>>,
        kv_engine: RwLock<Option<TiFlashEngine>>,
    ) -> Self {
        RaftStoreProxy {
            status,
            key_manager,
            read_index_client,
            kv_engine,
        }
    }
}

impl RaftStoreProxyFFI<TiFlashEngine> for RaftStoreProxy {
    fn maybe_read_index_client(&self) -> &Option<Box<dyn read_index_helper::ReadIndex>> {
        &self.read_index_client
    }

    fn set_read_index_client(&mut self, v: Option<Box<dyn read_index_helper::ReadIndex>>) {
        self.read_index_client = v;
    }

    fn status(&self) -> &AtomicU8 {
        &self.status
    }

    fn maybe_key_manager(&self) -> &Option<Arc<DataKeyManager>> {
        &self.key_manager
    }

    fn set_kv_engine(&mut self, kv_engine: Option<TiFlashEngine>) {
        let mut lock = self.kv_engine.write().unwrap();
        *lock = kv_engine;
    }

    fn kv_engine(&self) -> &RwLock<Option<TiFlashEngine>> {
        &self.kv_engine
    }

    fn set_status(&mut self, s: RaftProxyStatus) {
        self.status.store(s as u8, Ordering::SeqCst);
    }

    fn get_value_cf<F>(&self, cf: &str, key: &[u8], cb: F)
    where
        F: FnOnce(Result<Option<&[u8]>, String>),
    {
        let kv_engine_lock = self.kv_engine.read().unwrap();
        let kv_engine = kv_engine_lock.as_ref();
        if kv_engine.is_none() {
            cb(Err("KV engine is not initialized".to_string()));
            return;
        }
        let value = kv_engine.unwrap().get_value_cf(cf, key);
        match value {
            Ok(v) => {
                if let Some(x) = v {
                    cb(Ok(Some(&x)));
                } else {
                    cb(Ok(None));
                }
            }
            Err(e) => {
                cb(Err(format!("{}", e)));
            }
        }
    }
}

impl RaftStoreProxyPtr {
    pub unsafe fn as_ref(&self) -> &RaftStoreProxy {
        &*(self.inner as *const RaftStoreProxy)
    }
    pub fn is_null(&self) -> bool {
        self.inner.is_null()
    }
}

impl From<&RaftStoreProxy> for RaftStoreProxyPtr {
    fn from(ptr: &RaftStoreProxy) -> Self {
        Self {
            inner: ptr as *const _ as ConstRawVoidPtr,
        }
    }
}
