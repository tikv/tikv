#[allow(dead_code)]
mod interfaces;

mod read_index_helper;

use encryption::DataKeyManager;
use engine_rocks::encryption::get_env;
use engine_rocks::{RocksSstIterator, RocksSstReader};
use engine_traits::{
    EncryptionKeyManager, EncryptionMethod, FileEncryptionInfo, Iterator, SeekKey, SstReader,
    CF_DEFAULT, CF_LOCK, CF_WRITE,
};
use kvproto::{kvrpcpb, metapb, raft_cmdpb};
use protobuf::Message;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

pub use read_index_helper::ReadIndexClient;
use std::borrow::Borrow;

pub use crate::engine_store_ffi::interfaces::root::DB::{
    BaseBuffView, ColumnFamilyType, CppStrVecView, EngineStoreApplyRes, EngineStoreServerHelper,
    EngineStoreServerStatus, FileEncryptionRes, HttpRequestRes, HttpRequestStatus, RaftCmdHeader,
    RaftProxyStatus, RaftStoreProxyFFIHelper, RawCppPtr, RawVoidPtr, SSTReaderPtr, StoreStats,
    WriteCmdType, WriteCmdsView,
};
use crate::engine_store_ffi::interfaces::root::DB::{
    ConstRawVoidPtr, FileEncryptionInfoRaw, RaftStoreProxyPtr, RawCppPtrType, RawCppStringPtr,
    SSTReaderInterfaces, SSTView, SSTViewVec, RAFT_STORE_PROXY_MAGIC_NUMBER,
    RAFT_STORE_PROXY_VERSION,
};
use crate::store::LockCFFileReader;

impl From<&[u8]> for BaseBuffView {
    fn from(s: &[u8]) -> Self {
        let ptr = s.as_ptr() as *const _;
        Self {
            data: ptr,
            len: s.len() as u64,
        }
    }
}

trait UnwrapExternCFunc<T> {
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
    pub read_index_client: Box<dyn read_index_helper::ReadIndex>,
}

impl RaftStoreProxy {
    pub fn set_status(&mut self, s: RaftProxyStatus) {
        self.status.store(s as u8, Ordering::SeqCst);
    }
}

impl RaftStoreProxyPtr {
    unsafe fn as_ref(&self) -> &RaftStoreProxy {
        &*(self.inner as *const RaftStoreProxy)
    }
    fn is_null(&self) -> bool {
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

#[no_mangle]
pub extern "C" fn ffi_handle_get_proxy_status(proxy_ptr: RaftStoreProxyPtr) -> RaftProxyStatus {
    unsafe {
        let r = proxy_ptr.as_ref().status.load(Ordering::SeqCst);
        std::mem::transmute(r)
    }
}

#[no_mangle]
pub extern "C" fn ffi_is_encryption_enabled(proxy_ptr: RaftStoreProxyPtr) -> u8 {
    unsafe { proxy_ptr.as_ref().key_manager.is_some().into() }
}

#[no_mangle]
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

#[no_mangle]
pub extern "C" fn ffi_batch_read_index(
    proxy_ptr: RaftStoreProxyPtr,
    view: CppStrVecView,
) -> RawVoidPtr {
    assert!(!proxy_ptr.is_null());
    if view.len != 0 {
        assert_ne!(view.view, std::ptr::null());
    }
    unsafe {
        let mut req_vec = Vec::with_capacity(view.len as usize);
        for i in 0..view.len as usize {
            let mut req = kvrpcpb::ReadIndexRequest::default();
            let p = &(*view.view.offset(i as isize));
            assert_ne!(p.data, std::ptr::null());
            assert_ne!(p.len, 0);
            req.merge_from_bytes(p.to_slice()).unwrap();
            req_vec.push(req);
        }
        let resp = proxy_ptr
            .as_ref()
            .read_index_client
            .batch_read_index(req_vec);
        let res = get_engine_store_server_helper().gen_batch_read_index_res(resp.len() as u64);
        assert_ne!(res, std::ptr::null_mut());
        for (r, region_id) in &resp {
            let r = ProtoMsgBaseBuff::new(r);
            get_engine_store_server_helper().insert_batch_read_index_resp(
                res,
                r.borrow().into(),
                *region_id,
            );
        }
        res
    }
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

#[no_mangle]
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
                    |f| FileEncryptionInfoRaw::from(f),
                )
            },
        )
    }
}

#[no_mangle]
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
                    |f| FileEncryptionInfoRaw::from(f),
                )
            },
        )
    }
}

#[no_mangle]
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

#[no_mangle]
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

#[no_mangle]
unsafe extern "C" fn ffi_get_sst_reader(
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

#[no_mangle]
unsafe extern "C" fn ffi_remained(mut reader: SSTReaderPtr, type_: ColumnFamilyType) -> u8 {
    match type_ {
        ColumnFamilyType::Lock => reader.as_mut_lock().ffi_remained(),
        _ => reader.as_mut().ffi_remained(),
    }
}

#[no_mangle]
unsafe extern "C" fn ffi_key(mut reader: SSTReaderPtr, type_: ColumnFamilyType) -> BaseBuffView {
    match type_ {
        ColumnFamilyType::Lock => reader.as_mut_lock().ffi_key(),
        _ => reader.as_mut().ffi_key(),
    }
}

#[no_mangle]
unsafe extern "C" fn ffi_val(mut reader: SSTReaderPtr, type_: ColumnFamilyType) -> BaseBuffView {
    match type_ {
        ColumnFamilyType::Lock => reader.as_mut_lock().ffi_val(),
        _ => reader.as_mut().ffi_val(),
    }
}

#[no_mangle]
unsafe extern "C" fn ffi_next(mut reader: SSTReaderPtr, type_: ColumnFamilyType) {
    match type_ {
        ColumnFamilyType::Lock => reader.as_mut_lock().ffi_next(),
        _ => reader.as_mut().ffi_next(),
    }
}

#[no_mangle]
unsafe extern "C" fn ffi_gc(reader: SSTReaderPtr, type_: ColumnFamilyType) {
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
                fn_get_sst_reader: Some(ffi_get_sst_reader),
                fn_remained: Some(ffi_remained),
                fn_key: Some(ffi_key),
                fn_value: Some(ffi_val),
                fn_next: Some(ffi_next),
                fn_gc: Some(ffi_gc),
            },
        }
    }
}

pub struct SSTFileReader {
    iter: RocksSstIterator,
    remained: bool,
}

impl SSTFileReader {
    fn ffi_get_cf_file_reader(path: &str, key_manager: Option<Arc<DataKeyManager>>) -> RawVoidPtr {
        let env = get_env(key_manager, None).unwrap();
        let sst_reader = RocksSstReader::open_with_env(path, Some(env)).unwrap();
        sst_reader.verify_checksum().unwrap();
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
        self.cmd_type.push(cmd_type.into());
        self.cf.push(cf.into());
    }

    pub fn len(&self) -> usize {
        return self.cmd_type.len();
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

struct ProtoMsgBaseBuff {
    data: Vec<u8>,
}

impl ProtoMsgBaseBuff {
    fn new<T: protobuf::Message>(msg: &T) -> Self {
        ProtoMsgBaseBuff {
            data: msg.write_to_bytes().unwrap(),
        }
    }
}

impl From<&ProtoMsgBaseBuff> for BaseBuffView {
    fn from(p: &ProtoMsgBaseBuff) -> Self {
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

impl Drop for RawCppPtr {
    fn drop(&mut self) {
        if !self.is_null() {
            get_engine_store_server_helper().gc_raw_cpp_ptr(self.ptr, self.type_);
            self.ptr = std::ptr::null_mut();
        }
    }
}

static mut ENGINE_STORE_SERVER_HELPER_PTR: u64 = 0;

pub fn get_engine_store_server_helper() -> &'static EngineStoreServerHelper {
    return unsafe { &(*(ENGINE_STORE_SERVER_HELPER_PTR as *const EngineStoreServerHelper)) };
}

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

impl From<&Vec<SSTView>> for SSTViewVec {
    fn from(snaps_view: &Vec<SSTView>) -> Self {
        Self {
            views: snaps_view.as_ptr(),
            len: snaps_view.len() as u64,
        }
    }
}

impl EngineStoreServerHelper {
    fn gc_raw_cpp_ptr(&self, ptr: *mut ::std::os::raw::c_void, tp: RawCppPtrType) {
        unsafe {
            (self.fn_gc_raw_cpp_ptr.into_inner())(self.inner, ptr, tp);
        }
    }

    pub fn handle_compute_store_stats(&self) -> StoreStats {
        unsafe { (self.fn_handle_compute_store_stats.into_inner())(self.inner) }
    }

    pub fn handle_write_raft_cmd(
        &self,
        cmds: &WriteCmds,
        header: RaftCmdHeader,
    ) -> EngineStoreApplyRes {
        unsafe {
            let res =
                (self.fn_handle_write_raft_cmd.into_inner())(self.inner, cmds.gen_view(), header);
            res.into()
        }
    }

    pub fn handle_get_engine_store_server_status(&self) -> EngineStoreServerStatus {
        unsafe { (self.fn_handle_get_engine_store_server_status.into_inner())(self.inner).into() }
    }

    pub fn handle_set_proxy(&self, proxy: *const RaftStoreProxyFFIHelper) {
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
        unsafe {
            let req = ProtoMsgBaseBuff::new(req);
            let resp = ProtoMsgBaseBuff::new(resp);

            let res = (self.fn_handle_admin_raft_cmd.into_inner())(
                self.inner,
                req.borrow().into(),
                resp.borrow().into(),
                header,
            );
            res.into()
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
        let snaps_view = into_sst_views(snaps);
        unsafe {
            let region = ProtoMsgBaseBuff::new(region);
            (self.fn_pre_handle_snapshot.into_inner())(
                self.inner,
                region.borrow().into(),
                peer_id,
                (&snaps_view).into(),
                index,
                term,
            )
        }
    }

    pub fn apply_pre_handled_snapshot(&self, snap: RawCppPtr) {
        unsafe {
            (self.fn_apply_pre_handled_snapshot.into_inner())(self.inner, snap.ptr, snap.type_)
        }
    }

    pub fn handle_ingest_sst(
        &self,
        snaps: Vec<(&[u8], ColumnFamilyType)>,
        header: RaftCmdHeader,
    ) -> EngineStoreApplyRes {
        let snaps_view = into_sst_views(snaps);
        unsafe {
            let res =
                (self.fn_handle_ingest_sst.into_inner())(self.inner, (&snaps_view).into(), header);
            res.into()
        }
    }

    pub fn handle_destroy(&self, region_id: u64) {
        unsafe {
            (self.fn_handle_destroy.into_inner())(self.inner, region_id);
        }
    }

    pub fn handle_check_terminated(&self) -> bool {
        unsafe { (self.fn_handle_check_terminated.into_inner())(self.inner) != 0 }
    }

    fn gen_cpp_string(&self, buff: &[u8]) -> RawCppStringPtr {
        unsafe { (self.fn_gen_cpp_string.into_inner())(buff.into()).into_raw() as RawCppStringPtr }
    }

    fn gen_batch_read_index_res(&self, cap: u64) -> RawVoidPtr {
        unsafe { (self.fn_gen_batch_read_index_res.into_inner())(cap) }
    }

    fn insert_batch_read_index_resp(&self, data: RawVoidPtr, buf: BaseBuffView, region_id: u64) {
        unsafe { (self.fn_insert_batch_read_index_resp.into_inner())(data, buf, region_id) }
    }

    pub fn handle_http_request(&self, path: &str) -> HttpRequestRes {
        unsafe { (self.fn_handle_http_request.into_inner())(self.inner, path.as_bytes().into()) }
    }

    pub fn check_http_uri_available(&self, path: &str) -> bool {
        unsafe { (self.fn_check_http_uri_available.into_inner())(path.as_bytes().into()) != 0 }
    }
}
