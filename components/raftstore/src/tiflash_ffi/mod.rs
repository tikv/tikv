#[allow(dead_code)]
mod interfaces;

mod read_index_helper;

use encryption::DataKeyManager;
use engine_rocks::encryption::get_env;
use engine_rocks::RocksSstReader;
use engine_traits::{
    EncryptionKeyManager, EncryptionMethod, FileEncryptionInfo, Iterator, SeekKey, SstReader,
    CF_DEFAULT, CF_LOCK, CF_WRITE,
};
use kvproto::{kvrpcpb, metapb, raft_cmdpb};
use protobuf::Message;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

pub use read_index_helper::{ReadIndex, ReadIndexClient};
use std::borrow::Borrow;

pub use crate::tiflash_ffi::interfaces::root::DB::{
    BaseBuffView, ColumnFamilyType, CppStrVecView, CppStrWithView, FileEncryptionRes,
    RaftCmdHeader, RaftProxyStatus, RawCppPtr, RegionId, StoreStats, TiFlashApplyRes,
    TiFlashRaftProxyHelperFFI, TiFlashStatus, WriteCmdType, WriteCmdsView,
};
use crate::tiflash_ffi::interfaces::root::DB::{
    FileEncryptionInfoRaw, RaftStoreProxyPtr, RawCppPtrType, RawVoidPtr, SnapshotView,
    SnapshotViewArray, TiFlashServerHelper,
};

pub type EngineStoreServerHelper = TiFlashServerHelper;
pub type SnapshotKV = VecDeque<(Vec<u8>, Vec<u8>)>;
pub type SnapshotKVView = (Vec<BaseBuffView>, Vec<BaseBuffView>);

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
    pub read_index_client: Box<dyn read_index_helper::ReadIndex>,
}

impl RaftStoreProxy {
    pub fn set_status(&mut self, s: RaftProxyStatus) {
        self.status.store(s as u8, Ordering::SeqCst);
    }
}

#[no_mangle]
pub extern "C" fn ffi_handle_get_proxy_status(proxy_ptr: RaftStoreProxyPtr) -> u8 {
    unsafe {
        (*(proxy_ptr as *const RaftStoreProxy))
            .status
            .load(Ordering::SeqCst)
    }
}

#[no_mangle]
pub extern "C" fn ffi_is_encryption_enabled(proxy_ptr: RaftStoreProxyPtr) -> u8 {
    unsafe {
        (*(proxy_ptr as *const RaftStoreProxy))
            .key_manager
            .is_some()
            .into()
    }
}

#[no_mangle]
pub extern "C" fn ffi_encryption_method(
    proxy_ptr: RaftStoreProxyPtr,
) -> interfaces::root::DB::EncryptionMethod {
    unsafe {
        (*(proxy_ptr as *const RaftStoreProxy))
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
    assert_ne!(proxy_ptr, std::ptr::null());
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
        let resp = (*(proxy_ptr as *const RaftStoreProxy))
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
            res: res as _,
            method: EncryptionMethod::Unknown.into(),
            key: std::ptr::null_mut(),
            iv: std::ptr::null_mut(),
            erro_msg: std::ptr::null_mut(),
        }
    }

    fn error(erro_msg: RawVoidPtr) -> Self {
        FileEncryptionInfoRaw {
            res: FileEncryptionRes::Error as _,
            method: EncryptionMethod::Unknown.into(),
            key: std::ptr::null_mut(),
            iv: std::ptr::null_mut(),
            erro_msg,
        }
    }

    fn from(f: FileEncryptionInfo) -> Self {
        FileEncryptionInfoRaw {
            res: FileEncryptionRes::Ok as _,
            method: f.method.into(),
            key: get_engine_store_server_helper().gen_cpp_string(&f.key),
            iv: get_engine_store_server_helper().gen_cpp_string(&f.iv),
            erro_msg: std::ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn ffi_handle_get_file(
    proxy_ptr: RaftStoreProxyPtr,
    name: BaseBuffView,
) -> FileEncryptionInfoRaw {
    unsafe {
        (*(proxy_ptr as *const RaftStoreProxy))
            .key_manager
            .as_ref()
            .map_or(
                FileEncryptionInfoRaw::new(FileEncryptionRes::Disabled),
                |key_manager| {
                    let p = key_manager.get_file(std::str::from_utf8_unchecked(name.to_slice()));
                    p.map_or_else(
                        |e| {
                            FileEncryptionInfoRaw::error(
                                get_engine_store_server_helper().gen_cpp_string(
                                    format!("Encryption key manager get file failure: {}", e)
                                        .as_ref(),
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
        (*(proxy_ptr as *const RaftStoreProxy))
            .key_manager
            .as_ref()
            .map_or(
                FileEncryptionInfoRaw::new(FileEncryptionRes::Disabled),
                |key_manager| {
                    let p = key_manager.new_file(std::str::from_utf8_unchecked(name.to_slice()));
                    p.map_or_else(
                        |e| {
                            FileEncryptionInfoRaw::error(
                                get_engine_store_server_helper().gen_cpp_string(
                                    format!("Encryption key manager new file failure: {}", e)
                                        .as_ref(),
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
        (*(proxy_ptr as *const RaftStoreProxy))
            .key_manager
            .as_ref()
            .map_or(
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
        (*(proxy_ptr as *const RaftStoreProxy))
            .key_manager
            .as_ref()
            .map_or(
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
                                    format!("Encryption key manager link file failure: {}", e)
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

impl TiFlashRaftProxyHelperFFI {
    pub fn new(proxy: &RaftStoreProxy) -> Self {
        let proxy_ptr = proxy as *const _ as *const u8;
        TiFlashRaftProxyHelperFFI {
            proxy_ptr: proxy_ptr as *const _,
            fn_handle_get_proxy_status: Some(ffi_handle_get_proxy_status),
            fn_is_encryption_enabled: Some(ffi_is_encryption_enabled),
            fn_encryption_method: Some(ffi_encryption_method),
            fn_handle_get_file: Some(ffi_handle_get_file),
            fn_handle_new_file: Some(ffi_handle_new_file),
            fn_handle_delete_file: Some(ffi_handle_delete_file),
            fn_handle_link_file: Some(ffi_handle_link_file),
            fn_handle_batch_read_index: Some(ffi_batch_read_index),
        }
    }
}

pub fn gen_snap_kv_data_from_sst(
    cf_file_path: &str,
    key_manager: Option<Arc<DataKeyManager>>,
) -> SnapshotKV {
    let mut cf_snap = SnapshotKV::new();
    let env = get_env(key_manager, None).unwrap();
    let sst_reader = RocksSstReader::open_with_env(cf_file_path, Some(env)).unwrap();
    sst_reader.verify_checksum().unwrap();
    let mut iter = sst_reader.iter();
    let mut remained = iter.seek(SeekKey::Start).unwrap();
    while remained {
        let ori_key = keys::origin_key(iter.key());
        let ori_val = iter.value();
        cf_snap.push_back((ori_key.to_vec(), ori_val.to_vec()));
        remained = iter.next().unwrap();
    }

    cf_snap
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

pub fn gen_snap_kv_data_view(snap: &SnapshotKV) -> SnapshotKVView {
    let mut keys = Vec::<BaseBuffView>::with_capacity(snap.len());
    let mut vals = Vec::<BaseBuffView>::with_capacity(snap.len());

    for (k, v) in snap {
        keys.push(k.as_slice().into());
        vals.push(v.as_slice().into());
    }

    (keys, vals)
}

#[derive(Default)]
pub struct SnapshotHelper {
    cf_snaps: Vec<(ColumnFamilyType, SnapshotKV)>,
    kv_view: Vec<SnapshotKVView>,
    snap_view: Vec<SnapshotView>,
}

impl SnapshotHelper {
    pub fn add_cf_snap(&mut self, cf_type: ColumnFamilyType, snap_kv: SnapshotKV) {
        self.cf_snaps.push((cf_type, snap_kv));
    }

    fn gen_snapshot_view(&mut self) -> SnapshotViewArray {
        let len = self.cf_snaps.len();
        self.kv_view.clear();
        self.snap_view.clear();

        for i in 0..len {
            self.kv_view
                .push(gen_snap_kv_data_view(&self.cf_snaps[i].1));
        }

        for i in 0..len {
            self.snap_view.push(SnapshotView {
                keys: self.kv_view[i].0.as_ptr(),
                vals: self.kv_view[i].1.as_ptr(),
                len: self.kv_view[i].0.len() as u64,
                cf: self.cf_snaps[i].0.clone().into(),
            });
        }
        SnapshotViewArray {
            views: self.snap_view.as_ptr(),
            len: self.snap_view.len() as u64,
        }
    }

    pub fn empty(&self) -> bool {
        self.cf_snaps.len() == 0
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

unsafe impl Send for RawCppPtr {}

unsafe impl Send for EngineStoreServerHelper {}

pub static mut ENGINE_STORE_SERVER_HELPER_PTR: u64 = 0;

pub fn get_engine_store_server_helper() -> &'static EngineStoreServerHelper {
    return unsafe { &(*(ENGINE_STORE_SERVER_HELPER_PTR as *const EngineStoreServerHelper)) };
}

impl EngineStoreServerHelper {
    fn gc_raw_cpp_ptr(&self, ptr: *mut ::std::os::raw::c_void, tp: RawCppPtrType) {
        unsafe {
            (self.fn_gc_raw_cpp_ptr.into_inner())(self.inner, ptr, tp);
        }
    }

    pub fn handle_get_table_sync_status(&self, table_id: u64) -> CppStrWithView {
        unsafe { (self.fn_handle_get_table_sync_status.into_inner())(self.inner, table_id) }
    }

    pub fn handle_compute_store_stats(&self) -> StoreStats {
        unsafe { (self.fn_handle_compute_store_stats.into_inner())(self.inner) }
    }

    pub fn handle_write_raft_cmd(
        &self,
        cmds: &WriteCmds,
        header: RaftCmdHeader,
    ) -> TiFlashApplyRes {
        unsafe {
            let res =
                (self.fn_handle_write_raft_cmd.into_inner())(self.inner, cmds.gen_view(), header);
            res.into()
        }
    }

    pub fn handle_get_tiflash_status(&self) -> TiFlashStatus {
        unsafe { (self.fn_handle_get_tiflash_status.into_inner())(self.inner).into() }
    }

    pub fn handle_set_proxy(&self, proxy: *const TiFlashRaftProxyHelperFFI) {
        unsafe { (self.fn_atomic_update_proxy.into_inner())(self.inner, proxy as *mut _) }
    }

    pub fn check(&self) {
        assert_eq!(std::mem::align_of::<Self>(), std::mem::align_of::<u64>());
        const MAGIC_NUMBER: u32 = 0x13579BDF;
        const VERSION: u32 = 401004;

        if self.magic_number != MAGIC_NUMBER {
            eprintln!(
                "TiFlash Proxy FFI magic number not match: expect {} got {}",
                MAGIC_NUMBER, self.magic_number
            );
            std::process::exit(-1);
        } else if self.version != VERSION {
            eprintln!(
                "TiFlash Proxy FFI version not match: expect {} got {}",
                VERSION, self.version
            );
            std::process::exit(-1);
        }
    }

    pub fn handle_admin_raft_cmd(
        &self,
        req: &raft_cmdpb::AdminRequest,
        resp: &raft_cmdpb::AdminResponse,
        header: RaftCmdHeader,
    ) -> TiFlashApplyRes {
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
        snaps: &mut SnapshotHelper,
        index: u64,
        term: u64,
    ) -> RawCppPtr {
        unsafe {
            let region = ProtoMsgBaseBuff::new(region);
            (self.fn_pre_handle_snapshot.into_inner())(
                self.inner,
                region.borrow().into(),
                peer_id,
                snaps.gen_snapshot_view(),
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
        snaps: &mut SnapshotHelper,
        header: RaftCmdHeader,
    ) -> TiFlashApplyRes {
        unsafe {
            let res = (self.fn_handle_ingest_sst.into_inner())(
                self.inner,
                snaps.gen_snapshot_view(),
                header,
            );
            res.into()
        }
    }

    pub fn handle_destroy(&self, region_id: RegionId) {
        unsafe {
            (self.fn_handle_destroy.into_inner())(self.inner, region_id);
        }
    }

    pub fn handle_check_terminated(&self) -> bool {
        unsafe { (self.fn_handle_check_terminated.into_inner())(self.inner) != 0 }
    }

    fn gen_cpp_string(&self, buff: &[u8]) -> RawVoidPtr {
        unsafe { (self.fn_gen_cpp_string.into_inner())(buff.into()).into_raw() }
    }

    fn gen_batch_read_index_res(&self, cap: u64) -> RawVoidPtr {
        unsafe { (self.fn_gen_batch_read_index_res.into_inner())(cap) }
    }

    fn insert_batch_read_index_resp(&self, data: RawVoidPtr, buf: BaseBuffView, region_id: u64) {
        unsafe { (self.fn_insert_batch_read_index_resp.into_inner())(data, buf, region_id) }
    }
}
