// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
#![allow(clippy::type_complexity)]

use engine_traits::Peekable;

use super::raftstore_proxy::*;
use crate::TiFlashEngine;

pub struct RaftStoreProxyEngine {
    pub kv_engine: TiFlashEngine,
}

impl RaftStoreProxyEngine {
    pub fn from_tiflash_engine(kv: TiFlashEngine) -> Option<Eng> {
        let e = Self { kv_engine: kv };
        Some(Box::new(e))
    }
}

impl RaftStoreProxyEngineTrait for RaftStoreProxyEngine {
    fn get_value_cf(
        &self,
        cf: &str,
        key: &[u8],
        cb: &mut dyn FnMut(Result<Option<&[u8]>, String>),
    ) {
        let value = self.kv_engine.get_value_cf(cf, key);
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

    fn engine_store_server_helper(&self) -> isize {
        self.kv_engine.proxy_ext.engine_store_server_helper
    }

    fn set_engine_store_server_helper(&mut self, x: isize) {
        self.kv_engine.proxy_ext.engine_store_server_helper = x;
    }
}

unsafe impl Send for RaftStoreProxyEngine {}
unsafe impl Sync for RaftStoreProxyEngine {}
