// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
#![allow(clippy::type_complexity)]
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc, RwLock,
};

use encryption::DataKeyManager;
use pd_client::PdClient;
use tikv_util::error;
use tokio::runtime::Runtime;

use super::{
    get_engine_store_server_helper, interfaces_ffi,
    interfaces_ffi::{
        ConstRawVoidPtr, KVGetStatus, RaftProxyStatus, RaftStoreProxyPtr, RaftstoreVer,
        RawCppStringPtr, RawVoidPtr,
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
    pd_client: Option<Arc<dyn PdClient>>,
    cluster_raftstore_ver: RwLock<RaftstoreVer>,
    proxy_config_str: String,
}

impl RaftStoreProxy {
    pub fn new(
        status: AtomicU8,
        key_manager: Option<Arc<DataKeyManager>>,
        read_index_client: Option<Box<dyn read_index_helper::ReadIndex>>,
        raftstore_proxy_engine: Option<Eng>,
        pd_client: Option<Arc<dyn PdClient>>,
        proxy_config_str: String,
    ) -> Self {
        RaftStoreProxy {
            status,
            key_manager,
            read_index_client,
            raftstore_proxy_engine: RwLock::new(raftstore_proxy_engine),
            pd_client,
            cluster_raftstore_ver: RwLock::new(RaftstoreVer::Uncertain),
            proxy_config_str,
        }
    }
}

pub fn maybe_use_backup_addr(u: &str, backup: impl Fn() -> String) -> Option<String> {
    let mut res = None;
    let mut need_backup_ip = false;

    if let Ok(mut stuff) = url::Url::parse(u) {
        match stuff.host() {
            None => {
                need_backup_ip = true;
            }
            Some(url::Host::Domain(e)) => {
                if e == "localhost" {
                    need_backup_ip = true;
                }
            }
            Some(url::Host::Ipv4(e)) => {
                let is_loopback_or_unspecified = e.is_unspecified() || e.is_loopback();
                if is_loopback_or_unspecified {
                    need_backup_ip = true;
                }
            }
            Some(url::Host::Ipv6(e)) => {
                let is_loopback_or_unspecified = e.is_unspecified() || e.is_loopback();
                if is_loopback_or_unspecified {
                    need_backup_ip = true;
                }
            }
        };
        if need_backup_ip {
            let mut s = backup();
            if !s.starts_with("http") {
                s = format!("http://{}", s);
            }
            if let Ok(back) = url::Url::parse(&s) {
                stuff.set_host(back.host_str()).unwrap();
            }
            res = Some(stuff.to_string())
        }
    }
    res
}

enum RequestResult {
    OK,
    Retry,
    Failed,
}

impl RaftStoreProxy {
    pub fn cluster_raftstore_version(&self) -> RaftstoreVer {
        *self.cluster_raftstore_ver.read().unwrap()
    }

    fn request_for_raftstore_version(
        &self,
        to_try_addrs: &Vec<String>,
        timeout_ms: i64,
    ) -> RequestResult {
        let generate_request_with_timeout = |timeout_ms: i64| -> Option<reqwest::Client> {
            let headers = reqwest::header::HeaderMap::new();
            let mut builder = reqwest::Client::builder().default_headers(headers);
            if timeout_ms >= 0 {
                builder = builder.timeout(std::time::Duration::from_millis(timeout_ms as u64));
            }
            match builder.build() {
                Ok(o) => Some(o),
                Err(e) => {
                    error!("generate_request_with_timeout error {:?}", e);
                    None
                }
            }
        };

        let parse_response = |rt: &Runtime,
                              resp: Result<reqwest::Response, reqwest::Error>|
         -> (RaftstoreVer, bool) {
            match resp {
                Ok(resp) => {
                    if resp.status() == 404 {
                        // If the port is not implemented.
                        return (RaftstoreVer::V1, false);
                    } else if resp.status() != 200 {
                        return (RaftstoreVer::Uncertain, false);
                    }
                    let resp = rt.block_on(async { resp.text().await }).unwrap();
                    if resp.contains("partitioned") {
                        (RaftstoreVer::V2, false)
                    } else {
                        (RaftstoreVer::V1, false)
                    }
                }
                Err(e) => {
                    error!("get_engine_type respond error {:?}", e);
                    if e.is_connect() {
                        // Maybe some dns error in TiOperator.
                        (RaftstoreVer::Uncertain, true)
                    } else {
                        (RaftstoreVer::Uncertain, false)
                    }
                }
            }
        };

        let rt = Runtime::new().unwrap();

        let mut pending = vec![];
        for addr in to_try_addrs {
            if let Some(c) = generate_request_with_timeout(timeout_ms) {
                let _g = rt.enter();
                let f = c.get(addr).send();
                pending.push(rt.spawn(f));
            }
        }

        if pending.is_empty() {
            tikv_util::error!("no valid tikv stores with status server");
        }

        let mut has_need_retry = false;
        loop {
            if pending.is_empty() {
                break;
            }
            let sel = futures::future::select_all(pending);
            let (resp, _completed_idx, remaining) = rt.block_on(sel);

            let (res, need_retry) = parse_response(&rt, resp.unwrap());
            has_need_retry |= need_retry;

            if res != RaftstoreVer::Uncertain {
                *self.cluster_raftstore_ver.write().unwrap() = res;
                rt.shutdown_timeout(std::time::Duration::from_millis(1));
                return RequestResult::OK;
            }

            pending = remaining;
        }
        rt.shutdown_timeout(std::time::Duration::from_millis(1));
        if has_need_retry {
            RequestResult::Retry
        } else {
            RequestResult::Failed
        }
    }

    /// Issue requests to all stores which is not marked as TiFlash.
    /// Use the result of the first store which is not a Uncertain.
    /// Or set the result to Uncertain if timeout.
    pub fn refresh_cluster_raftstore_version(&mut self, timeout_ms: i64) -> bool {
        // We don't use information stored in `GlobalReplicationState` to decouple.
        *self.cluster_raftstore_ver.write().unwrap() = RaftstoreVer::Uncertain;
        let mut retry_count: u64 = 0;
        const RETRY_LIMIT: u64 = 60;
        let stores = loop {
            match self.pd_client.as_ref().unwrap().get_all_stores(false) {
                Ok(stores) => break stores,
                Err(e) => {
                    if retry_count >= RETRY_LIMIT {
                        tikv_util::error!(
                            "get_all_stores error {:?} after {}/{} retries, quit",
                            e,
                            retry_count,
                            RETRY_LIMIT
                        );
                        return false;
                    } else {
                        match e {
                            pd_client::Error::ClusterNotBootstrapped(_) => {
                                tikv_util::info!(
                                    "get_all_stores error {:?} after {}/{} retries",
                                    e,
                                    retry_count,
                                    RETRY_LIMIT
                                );
                                // For ClusterNotBootstrapped, we should wait for the bootstrap.
                                std::thread::sleep(std::time::Duration::from_millis(1000))
                            }
                            _ => {
                                // For other error, we don't wait.
                                tikv_util::error!("get_all_stores error {:?}", e,);
                                return false;
                            }
                        }
                    }
                    retry_count += 1
                }
            }
        };

        let to_try_addrs: Vec<String> = stores
            .iter()
            .filter_map(|store| {
                // There are some other labels such like tiflash_compute.
                let shall_filter = store.get_labels().iter().any(|label| {
                    label.get_key() == "engine" && label.get_value().contains("tiflash")
                });
                if !shall_filter {
                    // TiKV's status server don't support https.
                    let mut u = format!("http://{}/{}", store.get_status_address(), "engine_type");
                    if let Some(nu) = maybe_use_backup_addr(&u, || store.get_address().to_string())
                    {
                        tikv_util::info!("switch from {} to {}", u, nu);
                        u = nu;
                    }
                    // A invalid url may lead to 404, which will enforce a V1 inference, which is
                    // error.
                    if let Ok(stuff) = url::Url::parse(&u) {
                        if stuff.path() == "/engine_type" {
                            Some(u)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        let mut status_server_retry = 0;
        #[cfg(any(test, feature = "testexport"))]
        #[allow(clippy::redundant_closure_call)]
        let status_server_retry_limit = (|| {
            fail::fail_point!("proxy_fetch_cluster_version_retry", |t| {
                let t = t.unwrap().parse::<u64>().unwrap();
                t
            });
            0
        })();

        #[cfg(not(any(test, feature = "testexport")))]
        const status_server_retry_limit: u64 = 5;
        loop {
            match self.request_for_raftstore_version(&to_try_addrs, timeout_ms) {
                RequestResult::OK => return true,
                RequestResult::Failed => return false,
                RequestResult::Retry => {
                    if status_server_retry < status_server_retry_limit {
                        status_server_retry += 1;
                        tikv_util::info!(
                            "retry due to status server network error from {}/{}",
                            status_server_retry,
                            status_server_retry_limit
                        );
                        std::thread::sleep(std::time::Duration::from_millis(3000));
                        continue;
                    }
                    return false;
                }
            }
        }
    }

    pub fn raftstore_version(&self) -> RaftstoreVer {
        RaftstoreVer::V1
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
        if self.raftstore_version() == RaftstoreVer::V1 {
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
        if self.raftstore_version() == RaftstoreVer::V1 {
            panic!("wrong raftstore version");
        } else {
            unreachable!()
        }
    }

    // TODO may be we can later move ProxyConfig to proxy_ffi.
    pub fn get_proxy_config_str(&self) -> &String {
        &self.proxy_config_str
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
    pub unsafe fn as_mut(&mut self) -> &mut RaftStoreProxy {
        &mut *(self.inner as *mut RaftStoreProxy)
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
