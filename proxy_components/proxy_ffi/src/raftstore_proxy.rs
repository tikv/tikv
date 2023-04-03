// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
#![allow(clippy::type_complexity)]
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc, RwLock,
};

use encryption::DataKeyManager;

use super::{
    get_engine_store_server_helper, interfaces_ffi,
    interfaces_ffi::{
        ConstRawVoidPtr, KVGetStatus, RaftProxyStatus, RaftStoreProxyPtr, RawCppStringPtr,
        RawVoidPtr,
    },
    raftstore_proxy_helper_impls::*,
    read_index_helper,
};

pub type Eng = Box<dyn RaftStoreProxyEngineTrait + Sync + Send>;

pub struct RaftStoreProxy {
    status: AtomicU8,
    key_manager: Option<Arc<DataKeyManager>>,
    read_index_client: Option<Box<dyn read_index_helper::ReadIndex>>,
    raftstore_proxy_engine: RwLock<Option<Eng>>,
}

impl RaftStoreProxy {
    pub fn new(
        status: AtomicU8,
        key_manager: Option<Arc<DataKeyManager>>,
        read_index_client: Option<Box<dyn read_index_helper::ReadIndex>>,
        raftstore_proxy_engine: Option<Eng>,
    ) -> Self {
        RaftStoreProxy {
            status,
            key_manager,
            read_index_client,
            raftstore_proxy_engine: RwLock::new(raftstore_proxy_engine),
        }
    }
}

impl RaftStoreProxy {
    pub fn raftstore_version(&self) -> u64 {
        1
    }

    pub fn set_kv_engine(&mut self, kv_engine: Option<Eng>) {
        let mut lock = self.raftstore_proxy_engine.write().unwrap();
        *lock = kv_engine;
    }

    // Only for test
    pub fn kv_engine(&self) -> &RwLock<Option<Eng>> {
        &self.raftstore_proxy_engine
    }

    pub fn get_value_cf(
        &self,
        cf: &str,
        key: &[u8],
        cb: &mut dyn FnMut(Result<Option<&[u8]>, String>),
    ) {
        let kv_engine_lock = self.raftstore_proxy_engine.read().unwrap();
        let kv_engine = kv_engine_lock.as_ref();
        if kv_engine.is_none() {
            cb(Err("KV engine is not initialized".to_string()));
            return;
        }
        kv_engine.unwrap().get_value_cf(cf, key, cb)
    }

    pub unsafe fn get_region_local_state(
        &self,
        region_id: u64,
        data: RawVoidPtr,
        error_msg: *mut RawCppStringPtr,
    ) -> KVGetStatus {
        let region_state_key = keys::region_state_key(region_id);
        let mut res = KVGetStatus::NotFound;
        if self.raftstore_version() == 1 {
            self.get_value_cf(engine_traits::CF_RAFT, &region_state_key, &mut |value| {
                match value {
                    Ok(v) => {
                        if let Some(buff) = v {
                            get_engine_store_server_helper().set_pb_msg_by_bytes(
                                interfaces_ffi::MsgPBType::RegionLocalState,
                                data,
                                buff.into(),
                            );
                            res = KVGetStatus::Ok;
                        } else {
                            res = KVGetStatus::NotFound;
                        }
                    }
                    Err(e) => {
                        let msg = get_engine_store_server_helper().gen_cpp_string(e.as_ref());
                        unsafe {
                            *error_msg = msg;
                        }
                        res = KVGetStatus::Error;
                    }
                };
            });
        } else {
            unreachable!()
        }
        res
    }

    pub fn get_raft_apply_state(&self, _region_id: u64) -> interfaces_ffi::KVGetStatus {
        if self.raftstore_version() == 1 {
            panic!("wrong raftstore version");
        } else {
            unreachable!()
        }
    }
}

pub trait RaftStoreProxyEngineTrait {
    fn get_value_cf(&self, cf: &str, key: &[u8], cb: &mut dyn FnMut(Result<Option<&[u8]>, String>));
    // Only for tests
    fn engine_store_server_helper(&self) -> isize;
    // Only for tests
    fn set_engine_store_server_helper(&mut self, _: isize);
}

impl RaftStoreProxyFFI for RaftStoreProxy {
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

    fn set_status(&mut self, s: RaftProxyStatus) {
        self.status.store(s as u8, Ordering::SeqCst);
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
