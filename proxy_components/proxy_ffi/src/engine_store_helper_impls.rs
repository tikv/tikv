// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.
use std::pin::Pin;

use kvproto::{kvrpcpb, metapb, raft_cmdpb};

use super::{
    basic_ffi_impls::*,
    domain_impls::*,
    interfaces_ffi,
    interfaces_ffi::{
        BaseBuffView, ColumnFamilyType, CppStrWithView, EngineStoreApplyRes,
        EngineStoreServerHelper, EngineStoreServerStatus, FastAddPeerRes, HttpRequestRes,
        RaftCmdHeader, RaftStoreProxyFFIHelper, RawCppPtr, RawCppPtrCarr, RawCppPtrType,
        RawCppStringPtr, RawVoidPtr, SpecialCppPtrType, StoreStats, RAFT_STORE_PROXY_MAGIC_NUMBER,
        RAFT_STORE_PROXY_VERSION,
    },
    UnwrapExternCFunc, WriteCmds,
};

static mut ENGINE_STORE_SERVER_HELPER_PTR: isize = 0;

pub fn get_engine_store_server_helper_ptr() -> isize {
    unsafe { ENGINE_STORE_SERVER_HELPER_PTR }
}

pub fn get_engine_store_server_helper() -> &'static EngineStoreServerHelper {
    gen_engine_store_server_helper(unsafe { ENGINE_STORE_SERVER_HELPER_PTR })
}

pub fn gen_engine_store_server_helper(
    engine_store_server_helper: isize,
) -> &'static EngineStoreServerHelper {
    debug_assert!(engine_store_server_helper != 0);
    unsafe { &(*(engine_store_server_helper as *const EngineStoreServerHelper)) }
}

/// # Safety
/// The lifetime of `engine_store_server_helper` is definitely longer than
/// `ENGINE_STORE_SERVER_HELPER_PTR`.
pub unsafe fn init_engine_store_server_helper(engine_store_server_helper: *const u8) {
    let ptr = &ENGINE_STORE_SERVER_HELPER_PTR as *const _ as *mut _;
    *ptr = engine_store_server_helper;
}

unsafe impl Sync for EngineStoreServerHelper {}

pub fn set_server_info_resp(res: &kvproto::diagnosticspb::ServerInfoResponse, ptr: RawVoidPtr) {
    get_engine_store_server_helper().set_server_info_resp(res, ptr)
}

impl EngineStoreServerHelper {
    pub fn gc_raw_cpp_ptr(&self, ptr: *mut ::std::os::raw::c_void, tp: RawCppPtrType) {
        debug_assert!(self.fn_gc_raw_cpp_ptr.is_some());
        unsafe {
            (self.fn_gc_raw_cpp_ptr.into_inner())(ptr, tp);
        }
    }

    pub fn gc_raw_cpp_ptr_carr(
        &self,
        ptr: *mut ::std::os::raw::c_void,
        tp: RawCppPtrType,
        len: u64,
    ) {
        debug_assert!(self.fn_gc_raw_cpp_ptr_carr.is_some());
        unsafe {
            (self.fn_gc_raw_cpp_ptr_carr.into_inner())(ptr, tp, len);
        }
    }

    pub fn gc_special_raw_cpp_ptr(
        &self,
        ptr: *mut ::std::os::raw::c_void,
        hint_len: u64,
        tp: SpecialCppPtrType,
    ) {
        debug_assert!(self.fn_gc_special_raw_cpp_ptr.is_some());
        unsafe {
            (self.fn_gc_special_raw_cpp_ptr.into_inner())(ptr, hint_len, tp);
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

    // Please notice that when specifying (index,term), we will do a prelim update
    // of (index,term) before post_exec. DO NOT use it other than CompactLog.
    // Use (0,0) instead.
    #[allow(clippy::collapsible_else_if)]
    #[allow(clippy::bool_to_int_with_if)]
    pub fn try_flush_data(
        &self,
        region_id: u64,
        force_persist: bool,
        try_until_succeed: bool,
        index: u64,
        term: u64,
    ) -> bool {
        debug_assert!(self.fn_try_flush_data.is_some());
        unsafe {
            (self.fn_try_flush_data.into_inner())(
                self.inner,
                region_id,
                if force_persist {
                    tikv_util::error!("we don't support try_flush_data for now");
                    2
                } else {
                    if try_until_succeed { 1 } else { 0 }
                },
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

    // Generate a cpp string, so the other side can read.
    // The string is owned by the otherside, and will be deleted by
    // `gc_raw_cpp_ptr`.
    pub fn gen_cpp_string(&self, buff: &[u8]) -> RawCppStringPtr {
        debug_assert!(self.fn_gen_cpp_string.is_some());
        unsafe { (self.fn_gen_cpp_string.into_inner())(buff.into()).into_raw() as RawCppStringPtr }
    }

    pub fn set_read_index_resp(&self, ptr: RawVoidPtr, r: &kvrpcpb::ReadIndexResponse) {
        let buff = ProtoMsgBaseBuff::new(r);
        self.set_pb_msg_by_bytes(
            interfaces_ffi::MsgPBType::ReadIndexResponse,
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

    pub fn set_pb_msg_by_bytes(
        &self,
        type_: interfaces_ffi::MsgPBType,
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
            interfaces_ffi::MsgPBType::ServerInfoResponse,
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

    pub fn handle_safe_ts_update(&self, region_id: u64, self_safe_ts: u64, leader_safe_ts: u64) {
        debug_assert!(self.fn_handle_safe_ts_update.is_some());
        unsafe {
            (self.fn_handle_safe_ts_update.into_inner())(
                self.inner,
                region_id,
                self_safe_ts,
                leader_safe_ts,
            )
        }
    }

    pub fn fast_add_peer(&self, region_id: u64, new_peer_id: u64) -> FastAddPeerRes {
        debug_assert!(self.fn_fast_add_peer.is_some());
        unsafe { (self.fn_fast_add_peer.into_inner())(self.inner, region_id, new_peer_id) }
    }
}

// PageStorage specific
impl EngineStoreServerHelper {
    pub fn create_write_batch(&self) -> RawCppPtr {
        debug_assert!(self.ps.fn_create_write_batch.is_some());
        unsafe { (self.ps.fn_create_write_batch.into_inner())(self.inner) }
    }

    pub fn wb_put_page(&self, wb: RawVoidPtr, page_id: BaseBuffView, page: BaseBuffView) {
        debug_assert!(self.ps.fn_wb_put_page.is_some());
        unsafe { (self.ps.fn_wb_put_page.into_inner())(wb, page_id, page) }
    }

    pub fn wb_del_page(&self, wb: RawVoidPtr, page_id: BaseBuffView) {
        debug_assert!(self.ps.fn_wb_del_page.is_some());
        unsafe { (self.ps.fn_wb_del_page.into_inner())(wb, page_id) }
    }

    pub fn get_wb_size(&self, wb: RawVoidPtr) -> u64 {
        debug_assert!(self.ps.fn_get_wb_size.is_some());
        unsafe { (self.ps.fn_get_wb_size.into_inner())(wb) }
    }

    pub fn is_wb_empty(&self, wb: RawVoidPtr) -> u8 {
        debug_assert!(self.ps.fn_is_wb_empty.is_some());
        unsafe { (self.ps.fn_is_wb_empty.into_inner())(wb) }
    }

    pub fn merge_wb(&self, lwb: RawVoidPtr, rwb: RawVoidPtr) {
        debug_assert!(self.ps.fn_handle_merge_wb.is_some());
        unsafe { (self.ps.fn_handle_merge_wb.into_inner())(lwb, rwb) }
    }

    pub fn clear_wb(&self, wb: RawVoidPtr) {
        debug_assert!(self.ps.fn_handle_clear_wb.is_some());
        unsafe { (self.ps.fn_handle_clear_wb.into_inner())(wb) }
    }

    pub fn consume_wb(&self, wb: RawVoidPtr) {
        debug_assert!(self.ps.fn_handle_consume_wb.is_some());
        unsafe { (self.ps.fn_handle_consume_wb.into_inner())(self.inner, wb) }
    }

    pub fn read_page(&self, page_id: BaseBuffView) -> CppStrWithView {
        debug_assert!(self.ps.fn_handle_read_page.is_some());
        unsafe { (self.ps.fn_handle_read_page.into_inner())(self.inner, page_id) }
    }

    pub fn scan_page(
        &self,
        start_page_id: BaseBuffView,
        end_page_id: BaseBuffView,
    ) -> RawCppPtrCarr {
        debug_assert!(self.ps.fn_handle_scan_page.is_some());
        unsafe {
            (self.ps.fn_handle_scan_page.into_inner())(self.inner, start_page_id, end_page_id)
        }
    }

    pub fn get_lower_bound(&self, page_id: BaseBuffView) -> CppStrWithView {
        debug_assert!(self.ps.fn_handle_get_lower_bound.is_some());
        unsafe { (self.ps.fn_handle_get_lower_bound.into_inner())(self.inner, page_id) }
    }

    pub fn is_ps_empty(&self) -> u8 {
        debug_assert!(self.ps.fn_is_ps_empty.is_some());
        unsafe { (self.ps.fn_is_ps_empty.into_inner())(self.inner) }
    }

    pub fn purge_ps(&self) {
        debug_assert!(self.ps.fn_handle_purge_ps.is_some());
        unsafe { (self.ps.fn_handle_purge_ps.into_inner())(self.inner) }
    }
}
