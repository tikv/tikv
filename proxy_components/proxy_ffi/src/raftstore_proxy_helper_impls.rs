// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
#![allow(clippy::type_complexity)]

use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time,
};

use encryption::DataKeyManager;
use kvproto::kvrpcpb;
use protobuf::Message;

use super::{
    basic_ffi_impls::*,
    domain_impls::*,
    encryption_impls::*,
    engine_store_helper_impls::*,
    interfaces_ffi::{
        BaseBuffView, CloudStorageEngineInterfaces, ConfigJsonType, CppStrVecView, KVGetStatus,
        RaftProxyStatus, RaftStoreProxyFFIHelper, RaftStoreProxyPtr, RaftstoreVer, RawCppPtr,
        RawCppStringPtr, RawRustPtr, RawVoidPtr, RustStrWithView, SSTReaderInterfaces,
    },
    read_index_helper,
    snapshot_reader_impls::*,
    utils,
};

impl Clone for RaftStoreProxyPtr {
    fn clone(&self) -> RaftStoreProxyPtr {
        *self
    }
}

impl Copy for RaftStoreProxyPtr {}

// TODO The trait is useless since it's on the FFI boundary,
pub trait RaftStoreProxyFFI: Sync {
    fn status(&self) -> &AtomicU8;
    fn set_status(&mut self, s: RaftProxyStatus);
    fn maybe_key_manager(&self) -> &Option<Arc<DataKeyManager>>;
    fn maybe_read_index_client(&self) -> &Option<Box<dyn read_index_helper::ReadIndex>>;
    // Only for test.
    fn set_read_index_client(&mut self, _: Option<Box<dyn read_index_helper::ReadIndex>>);
    // fn get_value_cf<F>(&self, cf: &str, key: &[u8], cb: F)
    // where
    //     F: FnOnce(Result<Option<&[u8]>, String>);
    // fn set_kv_engine(&mut self, kv_engine: Option<EK>);
    // fn kv_engine(&self) -> &RwLock<Option<EK>>;
}

impl RaftStoreProxyFFIHelper {
    pub fn new(proxy: RaftStoreProxyPtr) -> Self {
        RaftStoreProxyFFIHelper {
            proxy_ptr: proxy,
            fn_handle_get_proxy_status: Some(ffi_handle_get_proxy_status),
            fn_is_encryption_enabled: Some(ffi_is_encryption_enabled),
            fn_encryption_method: Some(ffi_encryption_method),
            fn_handle_get_file: Some(ffi_handle_get_file),
            fn_handle_new_file: Some(ffi_handle_new_file),
            fn_handle_delete_file: Some(ffi_handle_delete_file),
            fn_handle_link_file: Some(ffi_handle_link_file),
            fn_handle_batch_read_index: Some(ffi_batch_read_index),
            sst_reader_interfaces: SSTReaderInterfaces {
                fn_get_sst_reader: Some(ffi_make_sst_reader),
                fn_remained: Some(ffi_sst_reader_remained),
                fn_key: Some(ffi_sst_reader_key),
                fn_value: Some(ffi_sst_reader_val),
                fn_next: Some(ffi_sst_reader_next),
                fn_gc: Some(ffi_gc_sst_reader),
                fn_kind: Some(ffi_sst_reader_format_kind),
                fn_seek: Some(ffi_sst_reader_seek),
                fn_approx_size: Some(ffi_approx_size),
                fn_get_split_keys: Some(ffi_get_split_keys),
            },
            cloud_storage_engine_interfaces: CloudStorageEngineInterfaces {
                fn_get_keyspace_encryption: Some(
                    CloudStorageEngineFFI::ffi_get_keyspace_encryption,
                ),
                fn_get_master_key: Some(CloudStorageEngineFFI::ffi_get_master_key),
            },
            fn_server_info: None,
            fn_make_read_index_task: Some(ffi_make_read_index_task),
            fn_make_async_waker: Some(ffi_make_async_waker),
            fn_poll_read_index_task: Some(ffi_poll_read_index_task),
            fn_gc_rust_ptr: Some(ffi_gc_rust_ptr),
            fn_make_timer_task: Some(ffi_make_timer_task),
            fn_poll_timer_task: Some(ffi_poll_timer_task),
            fn_get_region_local_state: Some(ffi_get_region_local_state),
            fn_get_cluster_raftstore_version: Some(ffi_get_cluster_raftstore_version),
            fn_notify_compact_log: Some(ffi_notify_compact_log),
            fn_get_config_json: Some(ffi_get_config_json),
        }
    }
}

pub extern "C" fn ffi_notify_compact_log(
    _proxy_ptr: RaftStoreProxyPtr,
    _region_id: u64,
    _compact_index: u64,
    _compact_term: u64,
    _applied_index: u64,
) {
    // TODO(proactive flush)
    todo!()
}

unsafe extern "C" fn ffi_get_cluster_raftstore_version(
    proxy_ptr: RaftStoreProxyPtr,
    refresh_strategy: u8,
    timeout_ms: i64,
) -> RaftstoreVer {
    if refresh_strategy == 1 {
        // Force refresh
        let mut proxy_ptr = proxy_ptr;
        proxy_ptr
            .as_mut()
            .refresh_cluster_raftstore_version(timeout_ms);
    } else if refresh_strategy == 2 {
        // Refresh if uncertain
        if proxy_ptr.as_ref().cluster_raftstore_version() == RaftstoreVer::Uncertain {
            let mut proxy_ptr = proxy_ptr;
            proxy_ptr
                .as_mut()
                .refresh_cluster_raftstore_version(timeout_ms);
        }
    }
    proxy_ptr.as_ref().cluster_raftstore_version()
}

unsafe extern "C" fn ffi_get_region_local_state(
    proxy_ptr: RaftStoreProxyPtr,
    region_id: u64,
    data: RawVoidPtr,
    error_msg: *mut RawCppStringPtr,
) -> KVGetStatus {
    assert!(!proxy_ptr.is_null());
    proxy_ptr
        .as_ref()
        .get_region_local_state(region_id, data, error_msg)
}

pub extern "C" fn ffi_handle_get_proxy_status(proxy_ptr: RaftStoreProxyPtr) -> RaftProxyStatus {
    unsafe {
        let r = proxy_ptr.as_ref().status().load(Ordering::SeqCst);
        std::mem::transmute(r)
    }
}

pub extern "C" fn ffi_batch_read_index(
    proxy_ptr: RaftStoreProxyPtr,
    view: CppStrVecView,
    res: RawVoidPtr,
    timeout_ms: u64,
    fn_insert_batch_read_index_resp: Option<unsafe extern "C" fn(RawVoidPtr, BaseBuffView, u64)>,
) {
    assert!(!proxy_ptr.is_null());
    unsafe {
        if proxy_ptr.as_ref().maybe_read_index_client().is_none() {
            return;
        }
    }
    debug_assert!(fn_insert_batch_read_index_resp.is_some());
    if view.len != 0 {
        assert_ne!(view.view, std::ptr::null());
    }
    unsafe {
        let mut req_vec = Vec::with_capacity(view.len as usize);
        for i in 0..view.len as usize {
            let mut req = kvrpcpb::ReadIndexRequest::default();
            let p = &(*view.view.add(i));
            assert_ne!(p.data, std::ptr::null());
            assert_ne!(p.len, 0);
            req.merge_from_bytes(p.to_slice()).unwrap();
            req_vec.push(req);
        }
        let resp = proxy_ptr
            .as_ref()
            .maybe_read_index_client()
            .as_ref()
            .unwrap()
            .batch_read_index(req_vec, time::Duration::from_millis(timeout_ms));
        assert_ne!(res, std::ptr::null_mut());
        for (r, region_id) in &resp {
            let r = ProtoMsgBaseBuff::new(r);
            (fn_insert_batch_read_index_resp.into_inner())(res, Pin::new(&r).into(), *region_id)
        }
    }
}

pub extern "C" fn ffi_make_read_index_task(
    proxy_ptr: RaftStoreProxyPtr,
    req_view: BaseBuffView,
) -> RawRustPtr {
    assert!(!proxy_ptr.is_null());
    unsafe {
        if proxy_ptr.as_ref().maybe_read_index_client().is_none() {
            return RawRustPtr::default();
        }
    }
    let mut req = kvrpcpb::ReadIndexRequest::default();
    req.merge_from_bytes(req_view.to_slice()).unwrap();
    let task = unsafe {
        proxy_ptr
            .as_ref()
            .maybe_read_index_client()
            .as_ref()
            .unwrap()
            .make_read_index_task(req)
    };
    match task {
        None => {
            RawRustPtr::default() // Full or Disconnected
        }
        Some(task) => RawRustPtr {
            ptr: Box::into_raw(Box::new(task)) as *mut _,
            type_: RawRustPtrType::ReadIndexTask.into(),
        },
    }
}

#[allow(clippy::redundant_closure_call)]
pub extern "C" fn ffi_make_async_waker(
    wake_fn: Option<unsafe extern "C" fn(RawVoidPtr)>,
    data: RawCppPtr,
) -> RawRustPtr {
    debug_assert!(wake_fn.is_some());

    struct RawCppPtrWrap(RawCppPtr);
    // This pointer should be thread safe, just wrap it.
    unsafe impl Sync for RawCppPtrWrap {}

    let data = RawCppPtrWrap(data);
    let res: utils::ArcNotifyWaker = (|| {
        Arc::new(utils::NotifyWaker {
            inner: Box::new(move || unsafe {
                wake_fn.into_inner()(data.0.ptr);
            }),
        })
    })();

    RawRustPtr {
        ptr: Box::into_raw(Box::new(res)) as _,
        type_: RawRustPtrType::ArcFutureWaker.into(),
    }
}

pub extern "C" fn ffi_poll_read_index_task(
    proxy_ptr: RaftStoreProxyPtr,
    task_ptr: RawVoidPtr,
    resp_data: RawVoidPtr,
    waker: RawVoidPtr,
) -> u8 {
    assert!(!proxy_ptr.is_null());
    unsafe {
        if proxy_ptr.as_ref().maybe_read_index_client().is_none() {
            return 0;
        }
    }
    let task = unsafe { &mut *(task_ptr as *mut self::read_index_helper::ReadIndexTask) };
    let waker = if waker.is_null() {
        None
    } else {
        Some(unsafe { &*(waker as *mut utils::ArcNotifyWaker) })
    };
    if let Some(res) = unsafe {
        proxy_ptr
            .as_ref()
            .maybe_read_index_client()
            .as_ref()
            .unwrap()
            .poll_read_index_task(task, waker)
    } {
        get_engine_store_server_helper().set_read_index_resp(resp_data, &res);
        1
    } else {
        0
    }
}

pub extern "C" fn ffi_make_timer_task(millis: u64) -> RawRustPtr {
    let task = utils::make_timer_task(millis);
    RawRustPtr {
        ptr: Box::into_raw(Box::new(task)) as *mut _,
        type_: RawRustPtrType::TimerTask.into(),
    }
}

#[allow(clippy::bool_to_int_with_if)]
pub unsafe extern "C" fn ffi_poll_timer_task(task_ptr: RawVoidPtr, waker: RawVoidPtr) -> u8 {
    let task = &mut *(task_ptr as *mut utils::TimerTask);
    let waker = if waker.is_null() {
        None
    } else {
        Some(&*(waker as *mut utils::ArcNotifyWaker))
    };
    if utils::poll_timer_task(task, waker).is_some() {
        1
    } else {
        0
    }
}

pub unsafe extern "C" fn ffi_get_config_json(
    proxy_ptr: RaftStoreProxyPtr,
    kind: ConfigJsonType,
) -> RustStrWithView {
    match kind {
        ConfigJsonType::ProxyConfigAddressed => {
            let s = proxy_ptr
                .as_ref()
                .get_proxy_config_str()
                .as_bytes()
                .to_owned();
            if s.is_empty() {
                RustStrWithView::default()
            } else {
                build_from_string(s)
            }
        }
    }
}
