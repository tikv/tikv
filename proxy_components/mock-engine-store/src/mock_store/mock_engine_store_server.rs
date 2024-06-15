// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::RefCell,
    pin::Pin,
    sync::{atomic::Ordering, Mutex},
};

use engine_store_ffi::TiFlashEngine;

use super::{
    common::{
        interfaces_ffi::{PageAndCppStrWithView, PageStorageInterfaces},
        *,
    },
    mock_core::*,
    mock_fast_add_peer_impls::*,
    mock_ffi::*,
    mock_page_storage::*,
    mock_snapshot_impls::*,
    mock_write_impls::*,
};
use crate::mock_cluster;

#[derive(Clone)]
pub struct ThreadInfoJealloc {
    pub allocated_ptr: u64,
    pub deallocated_ptr: u64,
}

impl ThreadInfoJealloc {
    pub fn allocated(&self) -> u64 {
        unsafe { *(self.allocated_ptr as *const u64) }
    }
    pub fn deallocated(&self) -> u64 {
        unsafe { *(self.deallocated_ptr as *const u64) }
    }
    pub fn remaining(&self) -> i64 {
        self.allocated() as i64 - self.deallocated() as i64
    }
}
pub struct EngineStoreServer {
    pub id: u64,
    // TODO engines maybe changed into TabletRegistry?
    pub engines: Option<Engines<TiFlashEngine, ProxyRaftEngine>>,
    pub kvstore: HashMap<RegionId, Box<MockRegion>>,
    pub mock_cfg: MockConfig,
    pub region_states: RefCell<HashMap<RegionId, RegionStats>>,
    pub page_storage: MockPageStorage,
    // (region_id, peer_id) -> MockRegion
    pub tmp_fap_regions: HashMap<RegionId, Box<MockRegion>>,
    pub thread_info_map: Mutex<HashMap<String, ThreadInfoJealloc>>,
}

impl EngineStoreServer {
    pub fn new(id: u64, engines: Option<Engines<TiFlashEngine, ProxyRaftEngine>>) -> Self {
        EngineStoreServer {
            id,
            engines,
            kvstore: Default::default(),
            mock_cfg: MockConfig::default(),
            region_states: RefCell::new(Default::default()),
            page_storage: Default::default(),
            tmp_fap_regions: Default::default(),
            thread_info_map: Default::default(),
        }
    }

    pub fn mutate_region_states<F: Fn(&mut RegionStats)>(&self, region_id: RegionId, f: F) {
        let has = self.region_states.borrow().contains_key(&region_id);
        if !has {
            self.region_states
                .borrow_mut()
                .insert(region_id, Default::default());
        }
        f(self.region_states.borrow_mut().get_mut(&region_id).unwrap())
    }

    pub fn get_mem(
        &self,
        region_id: u64,
        cf: interfaces_ffi::ColumnFamilyType,
        key: &Vec<u8>,
    ) -> Option<&Vec<u8>> {
        match self.kvstore.get(&region_id) {
            Some(region) => {
                let bmap = &region.data[cf as usize];
                bmap.get(key)
            }
            None => None,
        }
    }

    pub fn stop(&mut self) {
        for (_, region) in self.kvstore.iter_mut() {
            for cf in region.pending_write.iter_mut() {
                cf.clear();
            }
            for cf in region.pending_delete.iter_mut() {
                cf.clear();
            }
            for cf in region.data.iter_mut() {
                cf.clear();
            }
            region.apply_state = Default::default();
            // We don't clear applied_term.
        }
        self.engines
            .as_mut()
            .unwrap()
            .kv
            .proxy_ext
            .cached_region_info_manager
            .as_ref()
            .unwrap()
            .clear();
    }

    // False alarm
    #[allow(clippy::needless_collect)]
    pub fn restore(&mut self) {
        // TODO We should actually read from engine store's persistence.
        // However, since mock engine store don't persist itself,
        // we read from proxy instead.
        unsafe {
            let region_ids: Vec<u64> = self.kvstore.keys().cloned().collect();
            for region_id in region_ids.into_iter() {
                load_from_db(self, region_id);
            }
        }
    }

    pub unsafe fn write_to_db_by_region_id(&mut self, region_id: u64, reason: String) {
        let kv = &mut self.engines.as_mut().unwrap().kv;
        let region = self.kvstore.get_mut(&region_id).unwrap();
        write_to_db_data_by_engine(self.id, kv, region, reason)
    }
}

pub struct EngineStoreServerWrap {
    pub engine_store_server: *mut EngineStoreServer,
    pub maybe_proxy_helper: std::option::Option<*mut RaftStoreProxyFFIHelper>,
    // Call `gen_cluster(cluster_ptr)`, and get which cluster this Server belong to.
    // Will be removed to support v1/v2.
    #[allow(dead_code)]
    cluster_ptr: isize,
    pub cluster_ext_ptr: isize,
}

pub(crate) unsafe fn load_data_from_db(store: &mut EngineStoreServer, region_id: u64) {
    let store_id = store.id;
    let engine = &mut store.engines.as_mut().unwrap().kv;
    let region = store.kvstore.get_mut(&region_id).unwrap();
    for cf in 0..3 {
        let cf_name = cf_to_name(cf.into());
        region.data[cf].clear();
        region.pending_delete[cf].clear();
        region.pending_write[cf].clear();
        let start = region.region.get_start_key().to_owned();
        let end = region.region.get_end_key().to_owned();
        engine
            .scan(cf_name, &start, &end, false, |k, v| {
                let origin_key = if keys::validate_data_key(k) {
                    keys::origin_key(k).to_vec()
                } else {
                    k.to_vec()
                };
                debug!("restored data";
                    "store" => store_id,
                    "region_id" => region_id,
                    "cf" => cf,
                    "k" => ?k,
                    "origin_key" => ?origin_key,
                );
                region.data[cf].insert(origin_key, v.to_vec());
                Ok(true)
            })
            .unwrap();
    }
}

pub(crate) unsafe fn load_from_db(store: &mut EngineStoreServer, region_id: u64) {
    let engine = &mut store.engines.as_mut().unwrap().kv;
    let apply_state: RaftApplyState = general_get_apply_state(engine, region_id).unwrap();
    let region_state: RegionLocalState = general_get_region_local_state(engine, region_id).unwrap();

    let region = store.kvstore.get_mut(&region_id).unwrap();
    region.apply_state = apply_state;
    region.region = region_state.get_region().clone();
    set_new_region_peer(region, store.id);

    load_data_from_db(store, region_id);
}

pub(crate) unsafe fn write_to_db_data(
    store: &mut EngineStoreServer,
    region: &mut Box<MockRegion>,
    reason: String,
) {
    let kv = &mut store.engines.as_mut().unwrap().kv;
    write_to_db_data_by_engine(store.id, kv, region, reason)
}

pub(crate) unsafe fn write_snapshot_to_db_data(
    store: &mut EngineStoreServer,
    region: &mut Box<MockRegion>,
    reason: String,
) {
    let kv = &mut store.engines.as_mut().unwrap().kv;
    write_snapshot_to_db_data_by_engine(store.id, kv, region, reason)
}

pub(crate) unsafe fn write_snapshot_to_db_data_by_engine(
    store_id: u64,
    kv: &TiFlashEngine,
    region: &mut Box<MockRegion>,
    reason: String,
) {
    info!("mock flush snapshot to engine";
        "region" => ?region.region,
        "store_id" => store_id,
        "reason" => reason
    );
    let mut batch = kv.rocks.log_batch(1000);
    let local_state = kvproto::raft_serverpb::RaftLocalState::default();
    kv.rocks
        .clean(region.region.get_id(), 0, &local_state, &mut batch)
        .unwrap();
    kv.rocks.consume(&mut batch, true).unwrap();
    // TODO clear range
    for cf in 0..3 {
        for (k, v) in region.data[cf].iter() {
            let tikv_key = keys::data_key(k.as_slice());
            let cf_name = cf_to_name(cf.into());
            kv.rocks.put_cf(cf_name, tikv_key.as_slice(), v).unwrap();
        }
    }
}

pub(crate) unsafe fn write_to_db_data_by_engine(
    store_id: u64,
    kv: &TiFlashEngine,
    region: &mut Box<MockRegion>,
    reason: String,
) {
    info!("mock flush to engine";
        "region" => ?region.region,
        "store_id" => store_id,
        "reason" => reason
    );

    for cf in 0..3 {
        let pending_write = std::mem::take(region.pending_write.as_mut().get_mut(cf).unwrap());
        let pending_remove = std::mem::take(region.pending_delete.as_mut().get_mut(cf).unwrap());
        for (k, v) in pending_write.into_iter() {
            let tikv_key = keys::data_key(k.as_slice());
            let cf_name = cf_to_name(cf.into());
            if !pending_remove.contains(&k) {
                kv.rocks.put_cf(cf_name, tikv_key.as_slice(), &v).unwrap();
            }
        }
        let cf_name = cf_to_name(cf.into());
        for k in pending_remove.into_iter() {
            let tikv_key = keys::data_key(k.as_slice());
            kv.rocks.delete_cf(cf_name, &tikv_key).unwrap();
        }
    }
}

#[allow(clippy::single_element_loop)]
pub fn move_data_from(
    engine_store_server: &mut EngineStoreServer,
    old_region_id: u64,
    new_region_ids: &[u64],
) {
    let kvs = {
        let old_region = engine_store_server.kvstore.get_mut(&old_region_id).unwrap();
        let res = old_region.data.clone();
        old_region.data = Default::default();
        res
    };
    for new_region_id in new_region_ids {
        let new_region = engine_store_server.kvstore.get_mut(&new_region_id).unwrap();
        let new_region_meta = new_region.region.clone();
        let start_key = new_region_meta.get_start_key();
        let end_key = new_region_meta.get_end_key();
        for cf in &[interfaces_ffi::ColumnFamilyType::Default] {
            let cf = (*cf) as usize;
            for (k, v) in &kvs[cf] {
                let k = k.as_slice();
                let v = v.as_slice();
                match k {
                    keys::PREPARE_BOOTSTRAP_KEY | keys::STORE_IDENT_KEY => {}
                    _ => {
                        if k >= start_key && (end_key.is_empty() || k < end_key) {
                            debug!(
                                "move region data {:?} {:?} from {} to {}",
                                k, v, old_region_id, new_region_id
                            );
                            write_kv_in_mem(new_region, cf, k, v);
                        }
                    }
                };
            }
        }
    }
}

impl EngineStoreServerWrap {
    pub fn new(
        engine_store_server: *mut EngineStoreServer,
        maybe_proxy_helper: std::option::Option<*mut RaftStoreProxyFFIHelper>,
        cluster_ptr: isize,
        cluster_ext_ptr: isize,
    ) -> Self {
        Self {
            engine_store_server,
            maybe_proxy_helper,
            cluster_ptr,
            cluster_ext_ptr,
        }
    }
}

unsafe extern "C" fn ffi_set_pb_msg_by_bytes(
    type_: interfaces_ffi::MsgPBType,
    ptr: interfaces_ffi::RawVoidPtr,
    buff: interfaces_ffi::BaseBuffView,
) {
    match type_ {
        interfaces_ffi::MsgPBType::ReadIndexResponse => {
            let v = &mut *(ptr as *mut kvproto::kvrpcpb::ReadIndexResponse);
            v.merge_from_bytes(buff.to_slice()).unwrap();
        }
        interfaces_ffi::MsgPBType::ServerInfoResponse => {
            let v = &mut *(ptr as *mut kvproto::diagnosticspb::ServerInfoResponse);
            v.merge_from_bytes(buff.to_slice()).unwrap();
        }
        interfaces_ffi::MsgPBType::RegionLocalState => {
            let v = &mut *(ptr as *mut kvproto::raft_serverpb::RegionLocalState);
            v.merge_from_bytes(buff.to_slice()).unwrap();
        }
    }
}

unsafe extern "C" fn ffi_abort_pre_handle_snapshot(
    _: *mut interfaces_ffi::EngineStoreServerWrap,
    _: u64,
    _: u64,
) {
}
unsafe extern "C" fn ffi_release_pre_handled_snapshot(
    _: *mut interfaces_ffi::EngineStoreServerWrap,
    _: interfaces_ffi::RawVoidPtr,
    _: interfaces_ffi::RawCppPtrType,
) {
}

pub fn gen_engine_store_server_helper(
    wrap: Pin<&EngineStoreServerWrap>,
) -> EngineStoreServerHelper {
    info!("mock gen_engine_store_server_helper");
    EngineStoreServerHelper {
        magic_number: interfaces_ffi::RAFT_STORE_PROXY_MAGIC_NUMBER,
        version: interfaces_ffi::RAFT_STORE_PROXY_VERSION,
        inner: &(*wrap) as *const EngineStoreServerWrap as *mut _,
        fn_gen_cpp_string: Some(ffi_gen_cpp_string),
        fn_handle_write_raft_cmd: Some(ffi_handle_write_raft_cmd),
        fn_handle_admin_raft_cmd: Some(ffi_handle_admin_raft_cmd),
        fn_need_flush_data: Some(ffi_need_flush_data),
        fn_try_flush_data: Some(ffi_try_flush_data),
        fn_atomic_update_proxy: Some(ffi_atomic_update_proxy),
        fn_handle_destroy: Some(ffi_handle_destroy),
        fn_handle_ingest_sst: Some(ffi_handle_ingest_sst),
        fn_handle_compute_store_stats: Some(ffi_handle_compute_store_stats),
        fn_handle_get_engine_store_server_status: None,
        fn_pre_handle_snapshot: Some(ffi_pre_handle_snapshot),
        fn_apply_pre_handled_snapshot: Some(ffi_apply_pre_handled_snapshot),
        fn_abort_pre_handle_snapshot: Some(ffi_abort_pre_handle_snapshot),
        fn_release_pre_handled_snapshot: Some(ffi_release_pre_handled_snapshot),
        fn_handle_http_request: None,
        fn_check_http_uri_available: None,
        fn_gc_raw_cpp_ptr: Some(ffi_gc_raw_cpp_ptr),
        fn_gc_raw_cpp_ptr_carr: Some(ffi_gc_raw_cpp_ptr_carr),
        fn_gc_special_raw_cpp_ptr: Some(ffi_gc_special_raw_cpp_ptr),
        fn_get_config: None,
        fn_set_store: None,
        fn_set_pb_msg_by_bytes: Some(ffi_set_pb_msg_by_bytes),
        fn_handle_safe_ts_update: Some(ffi_handle_safe_ts_update),
        fn_fast_add_peer: Some(ffi_fast_add_peer),
        fn_get_lock_by_key: Some(ffi_get_lock_by_key),
        fn_apply_fap_snapshot: Some(ffi_apply_fap_snapshot),
        fn_query_fap_snapshot_state: Some(ffi_query_fap_snapshot_state),
        fn_kvstore_region_exists: Some(ffi_kvstore_region_exists),
        fn_clear_fap_snapshot: Some(ffi_clear_fap_snapshot),
        fn_report_thread_allocate_info: Some(ffi_report_thread_allocate_info),
        fn_report_thread_allocate_batch: Some(ffi_report_thread_allocate_batch),
        ps: PageStorageInterfaces {
            fn_create_write_batch: Some(ffi_mockps_create_write_batch),
            fn_wb_put_page: Some(ffi_mockps_wb_put_page),
            fn_wb_del_page: Some(ffi_mockps_wb_del_page),
            fn_get_wb_size: Some(ffi_mockps_get_wb_size),
            fn_is_wb_empty: Some(ffi_mockps_is_wb_empty),
            fn_handle_merge_wb: Some(ffi_mockps_handle_merge_wb),
            fn_handle_clear_wb: Some(ffi_mockps_handle_clear_wb),
            fn_handle_consume_wb: Some(ffi_mockps_handle_consume_wb),
            fn_handle_read_page: Some(ffi_mockps_handle_read_page),
            fn_handle_scan_page: Some(ffi_mockps_handle_scan_page),
            fn_handle_get_lower_bound: Some(ffi_mockps_handle_get_lower_bound),
            fn_is_ps_empty: Some(ffi_mockps_is_ps_empty),
            fn_handle_purge_ps: Some(ffi_mockps_handle_purge_ps),
        },
    }
}

pub unsafe fn into_engine_store_server_wrap(
    arg1: *const interfaces_ffi::EngineStoreServerWrap,
) -> &'static mut EngineStoreServerWrap {
    &mut *(arg1 as *mut EngineStoreServerWrap)
}

extern "C" fn ffi_need_flush_data(
    _arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    _region_id: u64,
) -> u8 {
    fail::fail_point!("need_flush_data", |e| e.unwrap().parse::<u8>().unwrap());
    true as u8
}

unsafe extern "C" fn ffi_try_flush_data(
    arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    region_id: u64,
    _try_until_succeed: u8,
    index: u64,
    term: u64,
    _ci: u64,
    _ct: u64,
) -> u8 {
    let store = into_engine_store_server_wrap(arg1);
    let kvstore = &mut (*store.engine_store_server).kvstore;
    // If we can't find region here, we return true so proxy can trigger a
    // CompactLog. The triggered CompactLog will be handled by
    // `handleUselessAdminRaftCmd`, and result in a
    // `EngineStoreApplyRes::NotFound`. Proxy will print this message and
    // continue: `region not found in engine-store, maybe have exec `RemoveNode`
    // first`.
    let region = match kvstore.get_mut(&region_id) {
        Some(r) => r,
        None => {
            if (*store.engine_store_server)
                .mock_cfg
                .panic_when_flush_no_found
                .load(Ordering::SeqCst)
            {
                panic!(
                    "ffi_try_flush_data no found region {} [index {} term {}], store {}",
                    region_id,
                    index,
                    term,
                    (*store.engine_store_server).id
                );
            } else {
                return 1;
            }
        }
    };
    fail::fail_point!("try_flush_data", |e| {
        let b = e.unwrap().parse::<u8>().unwrap();
        if b == 1 {
            write_to_db_data(
                &mut (*store.engine_store_server),
                region,
                "fn_try_flush_data".to_string(),
            );
        }
        b
    });
    write_to_db_data(
        &mut (*store.engine_store_server),
        region,
        "fn_try_flush_data".to_string(),
    );
    true as u8
}

extern "C" fn ffi_gc_special_raw_cpp_ptr(
    ptr: interfaces_ffi::RawVoidPtr,
    hint_len: u64,
    tp: interfaces_ffi::SpecialCppPtrType,
) {
    match tp {
        interfaces_ffi::SpecialCppPtrType::None => (),
        interfaces_ffi::SpecialCppPtrType::TupleOfRawCppPtr => unsafe {
            let p = Box::from_raw(std::slice::from_raw_parts_mut(
                ptr as *mut RawCppPtr,
                hint_len as usize,
            ));
            drop(p);
        },
        interfaces_ffi::SpecialCppPtrType::ArrayOfRawCppPtr => unsafe {
            let p = Box::from_raw(std::slice::from_raw_parts_mut(
                ptr as *mut RawVoidPtr,
                hint_len as usize,
            ));
            drop(p);
        },
    }
}

extern "C" fn ffi_gc_raw_cpp_ptr(
    ptr: interfaces_ffi::RawVoidPtr,
    tp: interfaces_ffi::RawCppPtrType,
) {
    match tp.into() {
        RawCppPtrTypeImpl::None => {}
        RawCppPtrTypeImpl::String => unsafe {
            drop(Box::<Vec<u8>>::from_raw(ptr as *mut _));
        },
        RawCppPtrTypeImpl::PreHandledSnapshotWithBlock => unsafe {
            drop(Box::<PrehandledSnapshot>::from_raw(ptr as *mut _));
        },
        RawCppPtrTypeImpl::WakerNotifier => unsafe {
            drop(Box::from_raw(ptr as *mut ProxyNotifier));
        },
        RawCppPtrTypeImpl::PSWriteBatch => unsafe {
            drop(Box::from_raw(ptr as *mut MockPSWriteBatch));
        },
        RawCppPtrTypeImpl::PSUniversalPage => unsafe {
            drop(Box::from_raw(ptr as *mut MockPSUniversalPage));
        },
        _ => todo!(),
    }
}

extern "C" fn ffi_gc_raw_cpp_ptr_carr(
    ptr: interfaces_ffi::RawVoidPtr,
    tp: interfaces_ffi::RawCppPtrType,
    len: u64,
) {
    match tp.into() {
        RawCppPtrTypeImpl::String => unsafe {
            let p = Box::from_raw(std::slice::from_raw_parts_mut(
                ptr as *mut RawVoidPtr,
                len as usize,
            ));
            for i in 0..len {
                let i = i as usize;
                if !p[i].is_null() {
                    ffi_gc_raw_cpp_ptr(p[i], RawCppPtrTypeImpl::String.into());
                }
            }
            drop(p);
        },
        RawCppPtrTypeImpl::PSPageAndCppStr => unsafe {
            let p = Box::from_raw(std::slice::from_raw_parts_mut(
                ptr as *mut PageAndCppStrWithView,
                len as usize,
            ));
            drop(p)
        },
        _ => todo!(),
    }
}

unsafe extern "C" fn ffi_atomic_update_proxy(
    arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    arg2: *mut interfaces_ffi::RaftStoreProxyFFIHelper,
) {
    let store = into_engine_store_server_wrap(arg1);
    store.maybe_proxy_helper = Some(arg2);
}

unsafe extern "C" fn ffi_handle_destroy(
    arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    region_id: u64,
) {
    let store = into_engine_store_server_wrap(arg1);
    debug!("ffi_handle_destroy {}", region_id);
    (*store.engine_store_server)
        .tmp_fap_regions
        .remove(&region_id);
    (*store.engine_store_server).kvstore.remove(&region_id);
}

unsafe extern "C" fn ffi_handle_safe_ts_update(
    arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    _region_id: u64,
    self_safe_ts: u64,
    leader_safe_ts: u64,
) {
    let store = into_engine_store_server_wrap(arg1);
    let cluster_ext = store.cluster_ext_ptr as *const mock_cluster::ClusterExt;
    assert_eq!(self_safe_ts, (*cluster_ext).test_data.expected_self_safe_ts);
    assert_eq!(
        leader_safe_ts,
        (*cluster_ext).test_data.expected_leader_safe_ts
    );
}

unsafe extern "C" fn ffi_handle_compute_store_stats(
    _arg1: *mut interfaces_ffi::EngineStoreServerWrap,
) -> interfaces_ffi::StoreStats {
    interfaces_ffi::StoreStats {
        fs_stats: interfaces_ffi::FsStats {
            capacity_size: 444444,
            used_size: 111111,
            avail_size: 333333,
            ok: 1,
        },
        engine_bytes_written: 0,
        engine_keys_written: 0,
        engine_bytes_read: 0,
        engine_keys_read: 0,
    }
}

unsafe extern "C" fn ffi_get_lock_by_key(
    arg1: *const interfaces_ffi::EngineStoreServerWrap,
    region_id: u64,
    key: interfaces_ffi::BaseBuffView,
) -> interfaces_ffi::BaseBuffView {
    let store = into_engine_store_server_wrap(arg1);
    match (*store.engine_store_server).kvstore.get(&region_id) {
        Some(e) => {
            let cf_index = interfaces_ffi::ColumnFamilyType::Lock as usize;
            let value = e.data[cf_index].get(key.to_slice()).unwrap().as_ptr();
            interfaces_ffi::BaseBuffView {
                data: value as *const i8,
                len: 0,
            }
        }
        None => interfaces_ffi::BaseBuffView {
            data: std::ptr::null(),
            len: 0,
        },
    }
}

unsafe extern "C" fn ffi_report_thread_allocate_info(
    arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    _: u64,
    name: interfaces_ffi::BaseBuffView,
    t: interfaces_ffi::ReportThreadAllocateInfoType,
    value: u64,
) {
    let store = into_engine_store_server_wrap(arg1);
    let tn = std::str::from_utf8(name.to_slice()).unwrap().to_string();
    match (*store.engine_store_server)
        .thread_info_map
        .lock()
        .expect("poisoned")
        .entry(tn)
    {
        std::collections::hash_map::Entry::Occupied(mut o) => {
            if t == interfaces_ffi::ReportThreadAllocateInfoType::AllocPtr {
                o.get_mut().allocated_ptr = value;
            } else if t == interfaces_ffi::ReportThreadAllocateInfoType::DeallocPtr {
                o.get_mut().deallocated_ptr = value;
            }
        }
        std::collections::hash_map::Entry::Vacant(v) => {
            if t == interfaces_ffi::ReportThreadAllocateInfoType::AllocPtr {
                v.insert(ThreadInfoJealloc {
                    allocated_ptr: value,
                    deallocated_ptr: 0,
                });
            } else if t == interfaces_ffi::ReportThreadAllocateInfoType::DeallocPtr {
                v.insert(ThreadInfoJealloc {
                    allocated_ptr: 0,
                    deallocated_ptr: value,
                });
            }
        }
    }
}

unsafe extern "C" fn ffi_report_thread_allocate_batch(
    _: *mut interfaces_ffi::EngineStoreServerWrap,
    _: u64,
    _name: interfaces_ffi::BaseBuffView,
    _data: interfaces_ffi::ReportThreadAllocateInfoBatch,
) {
}
