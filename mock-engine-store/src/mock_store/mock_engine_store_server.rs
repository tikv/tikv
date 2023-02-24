// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cell::RefCell, pin::Pin, sync::atomic::Ordering};

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
use crate::{mock_cluster, mock_cluster::TiFlashEngine, server::ServerCluster};

pub struct EngineStoreServer {
    pub id: u64,
    pub engines: Option<Engines<TiFlashEngine, engine_rocks::RocksEngine>>,
    pub kvstore: HashMap<RegionId, Box<MockRegion>>,
    pub mock_cfg: MockConfig,
    pub region_states: RefCell<HashMap<RegionId, RegionStats>>,
    pub page_storage: MockPageStorage,
}

impl EngineStoreServer {
    pub fn new(
        id: u64,
        engines: Option<Engines<TiFlashEngine, engine_rocks::RocksEngine>>,
    ) -> Self {
        EngineStoreServer {
            id,
            engines,
            kvstore: Default::default(),
            mock_cfg: MockConfig::default(),
            region_states: RefCell::new(Default::default()),
            page_storage: Default::default(),
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
    pub cluster_ptr: isize,
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
        let mut pending_remove =
            std::mem::take(region.pending_delete.as_mut().get_mut(cf).unwrap());
        for (k, v) in pending_write.into_iter() {
            let tikv_key = keys::data_key(k.as_slice());
            let cf_name = cf_to_name(cf.into());
            if !pending_remove.contains(&k) {
                kv.rocks.put_cf(cf_name, tikv_key.as_slice(), &v).unwrap();
            } else {
                pending_remove.remove(&k);
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
    ) -> Self {
        Self {
            engine_store_server,
            maybe_proxy_helper,
            cluster_ptr,
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

pub fn gen_engine_store_server_helper(
    wrap: Pin<&EngineStoreServerWrap>,
) -> EngineStoreServerHelper {
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
    store.maybe_proxy_helper = Some(&mut *(arg2 as *mut RaftStoreProxyFFIHelper));
}

unsafe extern "C" fn ffi_handle_destroy(
    arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    arg2: u64,
) {
    let store = into_engine_store_server_wrap(arg1);
    debug!("ffi_handle_destroy {}", arg2);
    (*store.engine_store_server).kvstore.remove(&arg2);
}

unsafe extern "C" fn ffi_handle_safe_ts_update(
    arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    _region_id: u64,
    self_safe_ts: u64,
    leader_safe_ts: u64,
) {
    let store = into_engine_store_server_wrap(arg1);
    let cluster = store.cluster_ptr as *const mock_cluster::Cluster<ServerCluster>;
    assert_eq!(
        self_safe_ts,
        (*cluster).cluster_ext.test_data.expected_self_safe_ts
    );
    assert_eq!(
        leader_safe_ts,
        (*cluster).cluster_ext.test_data.expected_leader_safe_ts
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
