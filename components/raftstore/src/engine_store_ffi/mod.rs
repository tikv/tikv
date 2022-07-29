// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#[allow(dead_code)]
pub mod interfaces;

mod lock_cf_reader;
pub mod observer;
mod read_index_helper;
mod utils;

use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time,
};

use encryption::DataKeyManager;
use engine_rocks::{encryption::get_env, RocksSstIterator, RocksSstReader};
use engine_traits::{
    EncryptionKeyManager, EncryptionMethod, FileEncryptionInfo, Iterator, Peekable, SeekKey,
    SstReader, CF_DEFAULT, CF_LOCK, CF_WRITE,
};
use kvproto::{kvrpcpb, metapb, raft_cmdpb};
use protobuf::Message;
pub use read_index_helper::ReadIndexClient;

pub use crate::engine_store_ffi::interfaces::root::DB::{
    BaseBuffView, ColumnFamilyType, CppStrVecView, EngineStoreApplyRes, EngineStoreServerHelper,
    EngineStoreServerStatus, FileEncryptionRes, FsStats, HttpRequestRes, HttpRequestStatus,
    KVGetStatus, RaftCmdHeader, RaftProxyStatus, RaftStoreProxyFFIHelper, RawCppPtr,
    RawCppStringPtr, RawVoidPtr, SSTReaderPtr, StoreStats, WriteCmdType, WriteCmdsView,
};
use crate::engine_store_ffi::{
    interfaces::root::DB::{
        ConstRawVoidPtr, FileEncryptionInfoRaw, RaftStoreProxyPtr, RawCppPtrType, RawRustPtr,
        SSTReaderInterfaces, SSTView, SSTViewVec, RAFT_STORE_PROXY_MAGIC_NUMBER,
        RAFT_STORE_PROXY_VERSION,
    },
    lock_cf_reader::LockCFFileReader,
};

pub type TiFlashEngine = engine_tiflash::RocksEngine;

impl From<&[u8]> for BaseBuffView {
    fn from(s: &[u8]) -> Self {
        let ptr = s.as_ptr() as *const _;
        Self {
            data: ptr,
            len: s.len() as u64,
        }
    }
}

pub trait UnwrapExternCFunc<T> {
    unsafe fn into_inner(&self) -> &T;
}

impl<T> UnwrapExternCFunc<T> for std::option::Option<T> {
    unsafe fn into_inner(&self) -> &T {
        std::mem::transmute::<&Self, &T>(self)
    }
}

pub struct RaftStoreProxy {
    pub status: AtomicU8,
    pub key_manager: Option<Arc<DataKeyManager>>,
    pub read_index_client: Option<Box<dyn read_index_helper::ReadIndex>>,
    pub kv_engine: std::sync::RwLock<Option<TiFlashEngine>>,
}

pub trait RaftStoreProxyFFI: Sync {
    fn set_status(&mut self, s: RaftProxyStatus);
    fn get_value_cf<F>(&self, cf: &str, key: &[u8], cb: F)
    where
        F: FnOnce(Result<Option<&[u8]>, String>);
    fn set_kv_engine(&mut self, kv_engine: Option<TiFlashEngine>);
}

impl RaftStoreProxy {
    pub fn new(
        status: AtomicU8,
        key_manager: Option<Arc<DataKeyManager>>,
        read_index_client: Option<Box<dyn read_index_helper::ReadIndex>>,
        kv_engine: std::sync::RwLock<Option<TiFlashEngine>>,
    ) -> Self {
        RaftStoreProxy {
            status,
            key_manager,
            read_index_client,
            kv_engine,
        }
    }
}

impl RaftStoreProxyFFI for RaftStoreProxy {
    fn set_kv_engine(&mut self, kv_engine: Option<TiFlashEngine>) {
        let mut lock = self.kv_engine.write().unwrap();
        *lock = kv_engine;
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
            cb(Err(format!("KV engine is not initialized")));
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
    unsafe fn as_ref(&self) -> &RaftStoreProxy {
        &*(self.inner as *const RaftStoreProxy)
    }
    pub fn is_null(&self) -> bool {
        self.inner == std::ptr::null()
    }
}

impl From<&RaftStoreProxy> for RaftStoreProxyPtr {
    fn from(ptr: &RaftStoreProxy) -> Self {
        Self {
            inner: ptr as *const _ as ConstRawVoidPtr,
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
                            interfaces::root::DB::MsgPBType::RegionLocalState,
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

    return res;
}

pub extern "C" fn ffi_handle_get_proxy_status(proxy_ptr: RaftStoreProxyPtr) -> RaftProxyStatus {
    unsafe {
        let r = proxy_ptr.as_ref().status.load(Ordering::SeqCst);
        std::mem::transmute(r)
    }
}

pub extern "C" fn ffi_is_encryption_enabled(proxy_ptr: RaftStoreProxyPtr) -> u8 {
    unsafe { proxy_ptr.as_ref().key_manager.is_some().into() }
}

pub extern "C" fn ffi_encryption_method(
    proxy_ptr: RaftStoreProxyPtr,
) -> interfaces::root::DB::EncryptionMethod {
    unsafe {
        proxy_ptr
            .as_ref()
            .key_manager
            .as_ref()
            .map_or(EncryptionMethod::Plaintext, |x| x.encryption_method())
            .into()
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
        match proxy_ptr.as_ref().read_index_client {
            Option::None => {
                return;
            }
            _ => {}
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
            .read_index_client
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

#[repr(u32)]
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub enum RawRustPtrType {
    None = 0,
    ReadIndexTask = 1,
    ArcFutureWaker = 2,
    TimerTask = 3,
}

impl From<u32> for RawRustPtrType {
    fn from(x: u32) -> Self {
        unsafe { std::mem::transmute(x) }
    }
}

impl Into<u32> for RawRustPtrType {
    fn into(self) -> u32 {
        unsafe { std::mem::transmute(self) }
    }
}

pub extern "C" fn ffi_gc_rust_ptr(
    data: RawVoidPtr,
    type_: crate::engine_store_ffi::interfaces::root::DB::RawRustPtrType,
) {
    if data.is_null() {
        return;
    }
    let type_: RawRustPtrType = type_.into();
    match type_ {
        RawRustPtrType::ReadIndexTask => unsafe {
            Box::from_raw(data as *mut read_index_helper::ReadIndexTask);
        },
        RawRustPtrType::ArcFutureWaker => unsafe {
            Box::from_raw(data as *mut utils::ArcNotifyWaker);
        },
        RawRustPtrType::TimerTask => unsafe {
            Box::from_raw(data as *mut utils::TimerTask);
        },
        _ => unreachable!(),
    }
}

impl Default for RawRustPtr {
    fn default() -> Self {
        Self {
            ptr: std::ptr::null_mut(),
            type_: RawRustPtrType::None.into(),
        }
    }
}

impl RawRustPtr {
    pub fn is_null(&self) -> bool {
        self.ptr.is_null()
    }
}

pub extern "C" fn ffi_make_read_index_task(
    proxy_ptr: RaftStoreProxyPtr,
    req_view: BaseBuffView,
) -> RawRustPtr {
    assert!(!proxy_ptr.is_null());
    unsafe {
        match proxy_ptr.as_ref().read_index_client {
            Option::None => {
                return RawRustPtr::default();
            }
            _ => {}
        }
    }
    let mut req = kvrpcpb::ReadIndexRequest::default();
    req.merge_from_bytes(req_view.to_slice()).unwrap();
    let task = unsafe {
        proxy_ptr
            .as_ref()
            .read_index_client
            .as_ref()
            .unwrap()
            .make_read_index_task(req)
    };
    return match task {
        None => {
            RawRustPtr::default() // Full or Disconnected
        }
        Some(task) => RawRustPtr {
            ptr: Box::into_raw(Box::new(task)) as *mut _,
            type_: RawRustPtrType::ReadIndexTask.into(),
        },
    };
}

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
        match proxy_ptr.as_ref().read_index_client {
            Option::None => {
                return 0;
            }
            _ => {}
        }
    }
    let task = unsafe {
        &mut *(task_ptr as *mut crate::engine_store_ffi::read_index_helper::ReadIndexTask)
    };
    let waker = if std::ptr::null_mut() == waker {
        None
    } else {
        Some(unsafe { &*(waker as *mut utils::ArcNotifyWaker) })
    };
    return if let Some(res) = unsafe {
        proxy_ptr
            .as_ref()
            .read_index_client
            .as_ref()
            .unwrap()
            .poll_read_index_task(task, waker)
    } {
        get_engine_store_server_helper().set_read_index_resp(resp_data, &res);
        1
    } else {
        0
    };
}

impl From<EncryptionMethod> for interfaces::root::DB::EncryptionMethod {
    fn from(o: EncryptionMethod) -> Self {
        unsafe { std::mem::transmute(o) }
    }
}

impl FileEncryptionInfoRaw {
    fn new(res: FileEncryptionRes) -> Self {
        FileEncryptionInfoRaw {
            res,
            method: EncryptionMethod::Unknown.into(),
            key: std::ptr::null_mut(),
            iv: std::ptr::null_mut(),
            error_msg: std::ptr::null_mut(),
        }
    }

    fn error(error_msg: RawCppStringPtr) -> Self {
        FileEncryptionInfoRaw {
            res: FileEncryptionRes::Error,
            method: EncryptionMethod::Unknown.into(),
            key: std::ptr::null_mut(),
            iv: std::ptr::null_mut(),
            error_msg,
        }
    }

    fn from(f: FileEncryptionInfo) -> Self {
        FileEncryptionInfoRaw {
            res: FileEncryptionRes::Ok,
            method: f.method.into(),
            key: get_engine_store_server_helper().gen_cpp_string(&f.key),
            iv: get_engine_store_server_helper().gen_cpp_string(&f.iv),
            error_msg: std::ptr::null_mut(),
        }
    }
}

pub extern "C" fn ffi_handle_get_file(
    proxy_ptr: RaftStoreProxyPtr,
    name: BaseBuffView,
) -> FileEncryptionInfoRaw {
    unsafe {
        proxy_ptr.as_ref().key_manager.as_ref().map_or(
            FileEncryptionInfoRaw::new(FileEncryptionRes::Disabled),
            |key_manager| {
                let p = key_manager.get_file(std::str::from_utf8_unchecked(name.to_slice()));
                p.map_or_else(
                    |e| {
                        FileEncryptionInfoRaw::error(
                            get_engine_store_server_helper().gen_cpp_string(
                                format!("Encryption key manager get file failure: {}", e).as_ref(),
                            ),
                        )
                    },
                    FileEncryptionInfoRaw::from,
                )
            },
        )
    }
}

pub extern "C" fn ffi_handle_new_file(
    proxy_ptr: RaftStoreProxyPtr,
    name: BaseBuffView,
) -> FileEncryptionInfoRaw {
    unsafe {
        proxy_ptr.as_ref().key_manager.as_ref().map_or(
            FileEncryptionInfoRaw::new(FileEncryptionRes::Disabled),
            |key_manager| {
                let p = key_manager.new_file(std::str::from_utf8_unchecked(name.to_slice()));
                p.map_or_else(
                    |e| {
                        FileEncryptionInfoRaw::error(
                            get_engine_store_server_helper().gen_cpp_string(
                                format!("Encryption key manager new file failure: {}", e).as_ref(),
                            ),
                        )
                    },
                    FileEncryptionInfoRaw::from,
                )
            },
        )
    }
}

pub extern "C" fn ffi_handle_delete_file(
    proxy_ptr: RaftStoreProxyPtr,
    name: BaseBuffView,
) -> FileEncryptionInfoRaw {
    unsafe {
        proxy_ptr.as_ref().key_manager.as_ref().map_or(
            FileEncryptionInfoRaw::new(FileEncryptionRes::Disabled),
            |key_manager| {
                let p = key_manager.delete_file(std::str::from_utf8_unchecked(name.to_slice()));
                p.map_or_else(
                    |e| {
                        FileEncryptionInfoRaw::error(
                            get_engine_store_server_helper().gen_cpp_string(
                                format!("Encryption key manager delete file failure: {}", e)
                                    .as_ref(),
                            ),
                        )
                    },
                    |_| FileEncryptionInfoRaw::new(FileEncryptionRes::Ok),
                )
            },
        )
    }
}

pub extern "C" fn ffi_handle_link_file(
    proxy_ptr: RaftStoreProxyPtr,
    src: BaseBuffView,
    dst: BaseBuffView,
) -> FileEncryptionInfoRaw {
    unsafe {
        proxy_ptr.as_ref().key_manager.as_ref().map_or(
            FileEncryptionInfoRaw::new(FileEncryptionRes::Disabled),
            |key_manager| {
                let p = key_manager.link_file(
                    std::str::from_utf8_unchecked(src.to_slice()),
                    std::str::from_utf8_unchecked(dst.to_slice()),
                );
                p.map_or_else(
                    |e| {
                        FileEncryptionInfoRaw::error(
                            get_engine_store_server_helper().gen_cpp_string(
                                format!("Encryption key manager link file failure: {}", e).as_ref(),
                            ),
                        )
                    },
                    |_| FileEncryptionInfoRaw::new(FileEncryptionRes::Ok),
                )
            },
        )
    }
}

impl SSTReaderPtr {
    unsafe fn as_mut_lock(&mut self) -> &mut LockCFFileReader {
        &mut *(self.inner as *mut LockCFFileReader)
    }

    unsafe fn as_mut(&mut self) -> &mut SSTFileReader {
        &mut *(self.inner as *mut SSTFileReader)
    }
}

impl From<RawVoidPtr> for SSTReaderPtr {
    fn from(pre: RawVoidPtr) -> Self {
        Self { inner: pre }
    }
}

unsafe extern "C" fn ffi_make_sst_reader(
    view: SSTView,
    proxy_ptr: RaftStoreProxyPtr,
) -> SSTReaderPtr {
    let path = std::str::from_utf8_unchecked(view.path.to_slice());
    let key_manager = &proxy_ptr.as_ref().key_manager;
    match view.type_ {
        ColumnFamilyType::Lock => {
            LockCFFileReader::ffi_get_cf_file_reader(path, key_manager.as_ref()).into()
        }
        _ => SSTFileReader::ffi_get_cf_file_reader(path, key_manager.clone()).into(),
    }
}

unsafe extern "C" fn ffi_sst_reader_remained(
    mut reader: SSTReaderPtr,
    type_: ColumnFamilyType,
) -> u8 {
    match type_ {
        ColumnFamilyType::Lock => reader.as_mut_lock().ffi_remained(),
        _ => reader.as_mut().ffi_remained(),
    }
}

unsafe extern "C" fn ffi_sst_reader_key(
    mut reader: SSTReaderPtr,
    type_: ColumnFamilyType,
) -> BaseBuffView {
    match type_ {
        ColumnFamilyType::Lock => reader.as_mut_lock().ffi_key(),
        _ => reader.as_mut().ffi_key(),
    }
}

unsafe extern "C" fn ffi_sst_reader_val(
    mut reader: SSTReaderPtr,
    type_: ColumnFamilyType,
) -> BaseBuffView {
    match type_ {
        ColumnFamilyType::Lock => reader.as_mut_lock().ffi_val(),
        _ => reader.as_mut().ffi_val(),
    }
}

unsafe extern "C" fn ffi_sst_reader_next(mut reader: SSTReaderPtr, type_: ColumnFamilyType) {
    match type_ {
        ColumnFamilyType::Lock => reader.as_mut_lock().ffi_next(),
        _ => reader.as_mut().ffi_next(),
    }
}

unsafe extern "C" fn ffi_gc_sst_reader(reader: SSTReaderPtr, type_: ColumnFamilyType) {
    match type_ {
        ColumnFamilyType::Lock => {
            Box::from_raw(reader.inner as *mut LockCFFileReader);
        }
        _ => {
            Box::from_raw(reader.inner as *mut SSTFileReader);
        }
    }
}

impl RaftStoreProxyFFIHelper {
    pub fn new(proxy: &RaftStoreProxy) -> Self {
        RaftStoreProxyFFIHelper {
            proxy_ptr: proxy.into(),
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

pub struct SSTFileReader {
    iter: RocksSstIterator,
    remained: bool,
}

impl SSTFileReader {
    fn ffi_get_cf_file_reader(path: &str, key_manager: Option<Arc<DataKeyManager>>) -> RawVoidPtr {
        let env = get_env(None, key_manager).unwrap();
        let sst_reader_res = RocksSstReader::open_with_env(path, Some(env));
        match sst_reader_res {
            Err(ref e) => tikv_util::error!("Can not open sst file {:?}", e),
            Ok(_) => (),
        };
        let sst_reader = sst_reader_res.unwrap();
        sst_reader.verify_checksum().unwrap();
        match sst_reader.verify_checksum() {
            Err(e) => {
                tikv_util::error!("verify_checksum sst file error {:?}", e);
                panic!("verify_checksum sst file error {:?}", e);
            }
            Ok(_) => (),
        }
        let mut iter = sst_reader.iter();
        let remained = iter.seek(SeekKey::Start).unwrap();

        Box::into_raw(Box::new(SSTFileReader { iter, remained })) as *mut _
    }

    pub fn ffi_remained(&self) -> u8 {
        self.remained as u8
    }

    pub fn ffi_key(&self) -> BaseBuffView {
        let ori_key = keys::origin_key(self.iter.key());
        ori_key.into()
    }

    pub fn ffi_val(&self) -> BaseBuffView {
        let val = self.iter.value();
        val.into()
    }

    pub fn ffi_next(&mut self) {
        self.remained = self.iter.next().unwrap();
    }
}

pub fn name_to_cf(cf: &str) -> ColumnFamilyType {
    if cf.is_empty() {
        return ColumnFamilyType::Default;
    }
    if cf == CF_LOCK {
        return ColumnFamilyType::Lock;
    } else if cf == CF_WRITE {
        return ColumnFamilyType::Write;
    } else if cf == CF_DEFAULT {
        return ColumnFamilyType::Default;
    }
    unreachable!()
}

#[derive(Default)]
pub struct WriteCmds {
    keys: Vec<BaseBuffView>,
    vals: Vec<BaseBuffView>,
    cmd_type: Vec<WriteCmdType>,
    cf: Vec<ColumnFamilyType>,
}

impl WriteCmds {
    pub fn with_capacity(cap: usize) -> WriteCmds {
        WriteCmds {
            keys: Vec::<BaseBuffView>::with_capacity(cap),
            vals: Vec::<BaseBuffView>::with_capacity(cap),
            cmd_type: Vec::<WriteCmdType>::with_capacity(cap),
            cf: Vec::<ColumnFamilyType>::with_capacity(cap),
        }
    }

    pub fn new() -> WriteCmds {
        WriteCmds::default()
    }

    pub fn push(&mut self, key: &[u8], val: &[u8], cmd_type: WriteCmdType, cf: ColumnFamilyType) {
        self.keys.push(key.into());
        self.vals.push(val.into());
        self.cmd_type.push(cmd_type);
        self.cf.push(cf);
    }

    pub fn len(&self) -> usize {
        self.cmd_type.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn gen_view(&self) -> WriteCmdsView {
        WriteCmdsView {
            keys: self.keys.as_ptr(),
            vals: self.vals.as_ptr(),
            cmd_types: self.cmd_type.as_ptr(),
            cmd_cf: self.cf.as_ptr(),
            len: self.cmd_type.len() as u64,
        }
    }
}

impl BaseBuffView {
    pub fn to_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data as *const _, self.len as usize) }
    }
}

impl RaftCmdHeader {
    pub fn new(region_id: u64, index: u64, term: u64) -> Self {
        RaftCmdHeader {
            region_id,
            index,
            term,
        }
    }
}

pub struct ProtoMsgBaseBuff {
    data: Vec<u8>,
}

impl ProtoMsgBaseBuff {
    pub fn new<T: protobuf::Message>(msg: &T) -> Self {
        ProtoMsgBaseBuff {
            data: msg.write_to_bytes().unwrap(),
        }
    }
}

impl From<Pin<&ProtoMsgBaseBuff>> for BaseBuffView {
    fn from(p: Pin<&ProtoMsgBaseBuff>) -> Self {
        Self {
            data: p.data.as_ptr() as *const _,
            len: p.data.len() as u64,
        }
    }
}

impl RawCppPtr {
    fn into_raw(mut self) -> RawVoidPtr {
        let ptr = self.ptr;
        self.ptr = std::ptr::null_mut();
        ptr
    }

    pub fn is_null(&self) -> bool {
        self.ptr == std::ptr::null_mut()
    }
}

unsafe impl Send for RawCppPtr {}
// Do not guarantee raw pointer could be accessed between threads safely
// unsafe impl Sync for RawCppPtr {}

impl Drop for RawCppPtr {
    fn drop(&mut self) {
        if !self.is_null() {
            let helper = get_engine_store_server_helper();
            helper.gc_raw_cpp_ptr(self.ptr, self.type_);
            self.ptr = std::ptr::null_mut();
        }
    }
}

static mut ENGINE_STORE_SERVER_HELPER_PTR: isize = 0;

pub fn get_engine_store_server_helper_ptr() -> isize {
    unsafe { ENGINE_STORE_SERVER_HELPER_PTR }
}

fn get_engine_store_server_helper() -> &'static EngineStoreServerHelper {
    gen_engine_store_server_helper(unsafe { ENGINE_STORE_SERVER_HELPER_PTR })
}

pub fn gen_engine_store_server_helper(
    engine_store_server_helper: isize,
) -> &'static EngineStoreServerHelper {
    debug_assert!(engine_store_server_helper != 0);
    unsafe { &(*(engine_store_server_helper as *const EngineStoreServerHelper)) }
}

/// # Safety
/// The lifetime of `engine_store_server_helper` is definitely longer than `ENGINE_STORE_SERVER_HELPER_PTR`.
pub unsafe fn init_engine_store_server_helper(engine_store_server_helper: *const u8) {
    let ptr = &ENGINE_STORE_SERVER_HELPER_PTR as *const _ as *mut _;
    *ptr = engine_store_server_helper;
}

fn into_sst_views(snaps: Vec<(&[u8], ColumnFamilyType)>) -> Vec<SSTView> {
    let mut snaps_view = vec![];
    for (path, cf) in snaps {
        snaps_view.push(SSTView {
            type_: cf,
            path: path.into(),
        })
    }
    snaps_view
}

impl From<Pin<&Vec<SSTView>>> for SSTViewVec {
    fn from(snaps_view: Pin<&Vec<SSTView>>) -> Self {
        Self {
            views: snaps_view.as_ptr(),
            len: snaps_view.len() as u64,
        }
    }
}

unsafe impl Sync for EngineStoreServerHelper {}

pub fn set_server_info_resp(res: &kvproto::diagnosticspb::ServerInfoResponse, ptr: RawVoidPtr) {
    get_engine_store_server_helper().set_server_info_resp(res, ptr)
}

impl EngineStoreServerHelper {
    fn gc_raw_cpp_ptr(&self, ptr: *mut ::std::os::raw::c_void, tp: RawCppPtrType) {
        debug_assert!(self.fn_gc_raw_cpp_ptr.is_some());
        unsafe {
            (self.fn_gc_raw_cpp_ptr.into_inner())(ptr, tp);
        }
    }

    pub fn handle_compute_store_stats(&self) -> StoreStats {
        debug_assert!(self.fn_handle_compute_store_stats.is_some());
        unsafe { (self.fn_handle_compute_store_stats.into_inner())(self.inner) }
    }

    pub fn handle_write_raft_cmd(
        &self,
        cmds: &WriteCmds,
        header: RaftCmdHeader,
    ) -> EngineStoreApplyRes {
        debug_assert!(self.fn_handle_write_raft_cmd.is_some());
        unsafe { (self.fn_handle_write_raft_cmd.into_inner())(self.inner, cmds.gen_view(), header) }
    }

    pub fn handle_get_engine_store_server_status(&self) -> EngineStoreServerStatus {
        debug_assert!(self.fn_handle_get_engine_store_server_status.is_some());
        unsafe { (self.fn_handle_get_engine_store_server_status.into_inner())(self.inner) }
    }

    pub fn handle_set_proxy(&self, proxy: *const RaftStoreProxyFFIHelper) {
        debug_assert!(self.fn_atomic_update_proxy.is_some());
        unsafe { (self.fn_atomic_update_proxy.into_inner())(self.inner, proxy as *mut _) }
    }

    pub fn check(&self) {
        assert_eq!(std::mem::align_of::<Self>(), std::mem::align_of::<u64>());

        if self.magic_number != RAFT_STORE_PROXY_MAGIC_NUMBER {
            eprintln!(
                "RaftStore Proxy FFI magic number not match: expect {} got {}",
                RAFT_STORE_PROXY_MAGIC_NUMBER, self.magic_number
            );
            std::process::exit(-1);
        } else if self.version != RAFT_STORE_PROXY_VERSION {
            eprintln!(
                "RaftStore Proxy FFI version not match: expect {} got {}",
                RAFT_STORE_PROXY_VERSION, self.version
            );
            std::process::exit(-1);
        }
    }

    pub fn handle_admin_raft_cmd(
        &self,
        req: &raft_cmdpb::AdminRequest,
        resp: &raft_cmdpb::AdminResponse,
        header: RaftCmdHeader,
    ) -> EngineStoreApplyRes {
        debug_assert!(self.fn_handle_admin_raft_cmd.is_some());

        unsafe {
            let req = ProtoMsgBaseBuff::new(req);
            let resp = ProtoMsgBaseBuff::new(resp);

            let res = (self.fn_handle_admin_raft_cmd.into_inner())(
                self.inner,
                Pin::new(&req).into(),
                Pin::new(&resp).into(),
                header,
            );
            res
        }
    }

    pub fn try_flush_data(
        &self,
        region_id: u64,
        try_until_succeed: bool,
        index: u64,
        term: u64,
    ) -> bool {
        debug_assert!(self.fn_try_flush_data.is_some());
        unsafe {
            (self.fn_try_flush_data.into_inner())(
                self.inner,
                region_id,
                if try_until_succeed { 1 } else { 0 },
                index,
                term,
            ) != 0
        }
    }

    pub fn pre_handle_snapshot(
        &self,
        region: &metapb::Region,
        peer_id: u64,
        snaps: Vec<(&[u8], ColumnFamilyType)>,
        index: u64,
        term: u64,
    ) -> RawCppPtr {
        debug_assert!(self.fn_pre_handle_snapshot.is_some());

        let snaps_view = into_sst_views(snaps);
        unsafe {
            let region = ProtoMsgBaseBuff::new(region);
            (self.fn_pre_handle_snapshot.into_inner())(
                self.inner,
                Pin::new(&region).into(),
                peer_id,
                Pin::new(&snaps_view).into(),
                index,
                term,
            )
        }
    }

    pub fn apply_pre_handled_snapshot(&self, snap: RawCppPtr) {
        debug_assert!(self.fn_apply_pre_handled_snapshot.is_some());

        unsafe {
            (self.fn_apply_pre_handled_snapshot.into_inner())(self.inner, snap.ptr, snap.type_)
        }
    }

    pub fn handle_ingest_sst(
        &self,
        snaps: Vec<(&[u8], ColumnFamilyType)>,
        header: RaftCmdHeader,
    ) -> EngineStoreApplyRes {
        debug_assert!(self.fn_handle_ingest_sst.is_some());

        let snaps_view = into_sst_views(snaps);
        unsafe {
            (self.fn_handle_ingest_sst.into_inner())(
                self.inner,
                Pin::new(&snaps_view).into(),
                header,
            )
        }
    }

    pub fn handle_destroy(&self, region_id: u64) {
        debug_assert!(self.fn_handle_destroy.is_some());

        unsafe {
            (self.fn_handle_destroy.into_inner())(self.inner, region_id);
        }
    }

    fn gen_cpp_string(&self, buff: &[u8]) -> RawCppStringPtr {
        debug_assert!(self.fn_gen_cpp_string.is_some());
        unsafe { (self.fn_gen_cpp_string.into_inner())(buff.into()).into_raw() as RawCppStringPtr }
    }

    fn set_read_index_resp(&self, ptr: RawVoidPtr, r: &kvrpcpb::ReadIndexResponse) {
        let buff = ProtoMsgBaseBuff::new(r);
        self.set_pb_msg_by_bytes(
            interfaces::root::DB::MsgPBType::ReadIndexResponse,
            ptr,
            Pin::new(&buff).into(),
        )
    }

    pub fn handle_http_request(
        &self,
        path: &str,
        query: Option<&str>,
        body: &[u8],
    ) -> HttpRequestRes {
        debug_assert!(self.fn_handle_http_request.is_some());

        let query = if let Some(s) = query {
            s.as_bytes().into()
        } else {
            BaseBuffView {
                data: std::ptr::null(),
                len: 0,
            }
        };
        unsafe {
            (self.fn_handle_http_request.into_inner())(
                self.inner,
                path.as_bytes().into(),
                query,
                body.into(),
            )
        }
    }

    pub fn check_http_uri_available(&self, path: &str) -> bool {
        debug_assert!(self.fn_check_http_uri_available.is_some());

        unsafe { (self.fn_check_http_uri_available.into_inner())(path.as_bytes().into()) != 0 }
    }

    fn set_pb_msg_by_bytes(
        &self,
        type_: interfaces::root::DB::MsgPBType,
        ptr: RawVoidPtr,
        buff: BaseBuffView,
    ) {
        debug_assert!(self.fn_set_pb_msg_by_bytes.is_some());
        unsafe { (self.fn_set_pb_msg_by_bytes.into_inner())(type_, ptr, buff) }
    }

    pub fn set_server_info_resp(
        &self,
        res: &kvproto::diagnosticspb::ServerInfoResponse,
        ptr: RawVoidPtr,
    ) {
        let buff = ProtoMsgBaseBuff::new(res);
        self.set_pb_msg_by_bytes(
            interfaces::root::DB::MsgPBType::ServerInfoResponse,
            ptr,
            Pin::new(&buff).into(),
        )
    }

    pub fn get_config(&self, full: bool) -> Vec<u8> {
        debug_assert!(self.fn_get_config.is_some());
        let config = unsafe { (self.fn_get_config.into_inner())(self.inner, full.into()) };
        config.view.to_slice().to_vec()
    }

    pub fn set_store(&self, store: metapb::Store) {
        debug_assert!(self.fn_set_store.is_some());
        let store = ProtoMsgBaseBuff::new(&store);
        unsafe { (self.fn_set_store.into_inner())(self.inner, Pin::new(&store).into()) }
    }
}

impl Clone for SSTReaderPtr {
    fn clone(&self) -> SSTReaderPtr {
        return SSTReaderPtr {
            inner: self.inner.clone(),
        };
    }
}

impl Clone for BaseBuffView {
    fn clone(&self) -> BaseBuffView {
        return BaseBuffView {
            data: self.data.clone(),
            len: self.len.clone(),
        };
    }
}

impl Clone for SSTView {
    fn clone(&self) -> SSTView {
        return SSTView {
            type_: self.type_.clone(),
            path: self.path.clone(),
        };
    }
}

impl Clone for SSTReaderInterfaces {
    fn clone(&self) -> SSTReaderInterfaces {
        return SSTReaderInterfaces {
            fn_get_sst_reader: self.fn_get_sst_reader.clone(),
            fn_remained: self.fn_remained.clone(),
            fn_key: self.fn_key.clone(),
            fn_value: self.fn_value.clone(),
            fn_next: self.fn_next.clone(),
            fn_gc: self.fn_gc.clone(),
        };
    }
}

impl Clone for RaftStoreProxyPtr {
    fn clone(&self) -> RaftStoreProxyPtr {
        return RaftStoreProxyPtr {
            inner: self.inner.clone(),
        };
    }
}

impl Copy for RaftStoreProxyPtr {}

impl From<usize> for ColumnFamilyType {
    fn from(i: usize) -> Self {
        match i {
            0 => ColumnFamilyType::Lock,
            1 => ColumnFamilyType::Write,
            2 => ColumnFamilyType::Default,
            _ => unreachable!(),
        }
    }
}

pub extern "C" fn ffi_make_timer_task(millis: u64) -> RawRustPtr {
    let task = utils::make_timer_task(millis);
    RawRustPtr {
        ptr: Box::into_raw(Box::new(task)) as *mut _,
        type_: RawRustPtrType::TimerTask.into(),
    }
}

pub unsafe extern "C" fn ffi_poll_timer_task(task_ptr: RawVoidPtr, waker: RawVoidPtr) -> u8 {
    let task = &mut *(task_ptr as *mut utils::TimerTask);
    let waker = if std::ptr::null_mut() == waker {
        None
    } else {
        Some(&*(waker as *mut utils::ArcNotifyWaker))
    };
    return if let Some(_) = { utils::poll_timer_task(task, waker) } {
        1
    } else {
        0
    };
}
