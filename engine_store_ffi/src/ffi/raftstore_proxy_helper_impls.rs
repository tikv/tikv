// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, RwLock,
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
    interfaces_ffi,
    interfaces_ffi::{
        BaseBuffView, CppStrVecView, KVGetStatus, RaftProxyStatus, RaftStoreProxyFFIHelper,
        RaftStoreProxyPtr, RawCppPtr, RawCppStringPtr, RawRustPtr, RawVoidPtr, SSTReaderInterfaces,
    },
    read_index_helper,
    sst_reader_impls::*,
    utils, UnwrapExternCFunc,
};

impl Clone for RaftStoreProxyPtr {
    fn clone(&self) -> RaftStoreProxyPtr {
        RaftStoreProxyPtr {
            inner: self.inner.clone(),
        }
    }
}

impl Copy for RaftStoreProxyPtr {}

pub trait RaftStoreProxyFFI<EK: engine_traits::KvEngine>: Sync {
    fn status(&self) -> &AtomicU8;
    fn maybe_key_manager(&self) -> &Option<Arc<DataKeyManager>>;
    fn maybe_read_index_client(&self) -> &Option<Box<dyn read_index_helper::ReadIndex>>;
    // Only for test.
    fn set_read_index_client(&mut self, _: Option<Box<dyn read_index_helper::ReadIndex>>);
    fn set_status(&mut self, s: RaftProxyStatus);
    fn get_value_cf<F>(&self, cf: &str, key: &[u8], cb: F)
    where
        F: FnOnce(Result<Option<&[u8]>, String>);
    fn set_kv_engine(&mut self, kv_engine: Option<EK>);
    fn kv_engine(&self) -> &RwLock<Option<EK>>;
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
            },
            fn_server_info: None,
            fn_make_read_index_task: Some(ffi_make_read_index_task),
            fn_make_async_waker: Some(ffi_make_async_waker),
            fn_poll_read_index_task: Some(ffi_poll_read_index_task),
            fn_gc_rust_ptr: Some(ffi_gc_rust_ptr),
            fn_make_timer_task: Some(ffi_make_timer_task),
            fn_poll_timer_task: Some(ffi_poll_timer_task),
            fn_get_region_local_state: Some(ffi_get_region_local_state),
        }
    }
}

unsafe extern "C" fn ffi_get_region_local_state(
    proxy_ptr: RaftStoreProxyPtr,
    region_id: u64,
    data: RawVoidPtr,
    error_msg: *mut RawCppStringPtr,
) -> KVGetStatus {
    assert!(!proxy_ptr.is_null());

    let region_state_key = keys::region_state_key(region_id);
    let mut res = KVGetStatus::NotFound;
    proxy_ptr
        .as_ref()
        .get_value_cf(engine_traits::CF_RAFT, &region_state_key, |value| {
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
                    *error_msg = msg;
                    res = KVGetStatus::Error;
                }
            };
        });

    res
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
