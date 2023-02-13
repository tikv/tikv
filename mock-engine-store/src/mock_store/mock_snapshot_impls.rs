// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::Ordering;

use super::{
    common::*,
    mock_core::*,
    mock_engine_store_server::{into_engine_store_server_wrap, write_to_db_data},
    mock_ffi::*,
};

type MockRaftProxyHelper = RaftStoreProxyFFIHelper;

#[derive(Debug)]
pub struct SSTReader<'a> {
    proxy_helper: &'a MockRaftProxyHelper,
    inner: interfaces_ffi::SSTReaderPtr,
    type_: interfaces_ffi::ColumnFamilyType,
}

impl<'a> Drop for SSTReader<'a> {
    fn drop(&mut self) {
        unsafe {
            (self.proxy_helper.sst_reader_interfaces.fn_gc.into_inner())(
                self.inner.clone(),
                self.type_,
            );
        }
    }
}

impl<'a> SSTReader<'a> {
    pub unsafe fn new(
        proxy_helper: &'a MockRaftProxyHelper,
        view: &'a interfaces_ffi::SSTView,
    ) -> Self {
        SSTReader {
            proxy_helper,
            inner: (proxy_helper
                .sst_reader_interfaces
                .fn_get_sst_reader
                .into_inner())(view.clone(), proxy_helper.proxy_ptr.clone()),
            type_: view.type_,
        }
    }

    pub unsafe fn remained(&mut self) -> bool {
        (self
            .proxy_helper
            .sst_reader_interfaces
            .fn_remained
            .into_inner())(self.inner.clone(), self.type_)
            != 0
    }

    pub unsafe fn key(&mut self) -> interfaces_ffi::BaseBuffView {
        (self.proxy_helper.sst_reader_interfaces.fn_key.into_inner())(
            self.inner.clone(),
            self.type_,
        )
    }

    pub unsafe fn value(&mut self) -> interfaces_ffi::BaseBuffView {
        (self
            .proxy_helper
            .sst_reader_interfaces
            .fn_value
            .into_inner())(self.inner.clone(), self.type_)
    }

    pub unsafe fn next(&mut self) {
        (self.proxy_helper.sst_reader_interfaces.fn_next.into_inner())(
            self.inner.clone(),
            self.type_,
        )
    }
}

pub struct PrehandledSnapshot {
    pub region: std::option::Option<MockRegion>,
}

pub unsafe extern "C" fn ffi_pre_handle_snapshot(
    arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    region_buff: interfaces_ffi::BaseBuffView,
    peer_id: u64,
    snaps: interfaces_ffi::SSTViewVec,
    index: u64,
    term: u64,
) -> interfaces_ffi::RawCppPtr {
    let store = into_engine_store_server_wrap(arg1);
    let proxy_helper = &mut *(store.maybe_proxy_helper.unwrap());
    let _kvstore = &mut (*store.engine_store_server).kvstore;
    let node_id = (*store.engine_store_server).id;

    let mut region_meta = kvproto::metapb::Region::default();
    assert_ne!(region_buff.data, std::ptr::null());
    assert_ne!(region_buff.len, 0);
    region_meta
        .merge_from_bytes(region_buff.to_slice())
        .unwrap();

    let mut region = Box::new(MockRegion::new(region_meta));
    debug!(
        "pre handle snaps";
        "peer_id" => peer_id,
        "store_id" => node_id,
        "index" => index,
        "term" => term,
        "region" => ?region.region,
        "snap len" => snaps.len,
    );

    (*store.engine_store_server).mutate_region_states(
        region.region.get_id(),
        |e: &mut RegionStats| {
            e.pre_handle_count.fetch_add(1, Ordering::SeqCst);
        },
    );

    for i in 0..snaps.len {
        let snapshot = snaps.views.add(i as usize);
        let view = &*(snapshot as *mut interfaces_ffi::SSTView);
        let mut sst_reader = SSTReader::new(proxy_helper, view);

        while sst_reader.remained() {
            let key = sst_reader.key();
            let value = sst_reader.value();

            let cf_index = (*snapshot).type_ as u8;
            write_kv_in_mem(
                region.as_mut(),
                cf_index as usize,
                key.to_slice(),
                value.to_slice(),
            );

            sst_reader.next();
        }
    }
    {
        region.set_applied(index, term);
        region.apply_state.mut_truncated_state().set_index(index);
        region.apply_state.mut_truncated_state().set_term(term);
    }
    interfaces_ffi::RawCppPtr {
        ptr: Box::into_raw(Box::new(PrehandledSnapshot {
            region: Some(*region),
        })) as *const MockRegion as interfaces_ffi::RawVoidPtr,
        type_: RawCppPtrTypeImpl::PreHandledSnapshotWithBlock.into(),
    }
}

pub unsafe extern "C" fn ffi_apply_pre_handled_snapshot(
    arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    arg2: interfaces_ffi::RawVoidPtr,
    _arg3: interfaces_ffi::RawCppPtrType,
) {
    let store = into_engine_store_server_wrap(arg1);
    let region_meta = &mut *(arg2 as *mut PrehandledSnapshot);
    let node_id = (*store.engine_store_server).id;

    let region_id = region_meta.region.as_ref().unwrap().region.id;

    let _ = &(*store.engine_store_server)
        .kvstore
        .insert(region_id, Box::new(region_meta.region.take().unwrap()));

    let region = (*store.engine_store_server)
        .kvstore
        .get_mut(&region_id)
        .unwrap();

    debug!(
        "apply prehandled snap";
        "store_id" => node_id,
        "region" => ?region.region,
    );
    write_to_db_data(
        &mut (*store.engine_store_server),
        region,
        String::from("prehandle-snap"),
    );
}

pub unsafe extern "C" fn ffi_handle_ingest_sst(
    arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    snaps: interfaces_ffi::SSTViewVec,
    header: interfaces_ffi::RaftCmdHeader,
) -> interfaces_ffi::EngineStoreApplyRes {
    let store = into_engine_store_server_wrap(arg1);
    let node_id = (*store.engine_store_server).id;
    let proxy_helper = &mut *(store.maybe_proxy_helper.unwrap());

    let region_id = header.region_id;
    let kvstore = &mut (*store.engine_store_server).kvstore;
    let _kv = &mut (*store.engine_store_server).engines.as_mut().unwrap().kv;

    match kvstore.entry(region_id) {
        std::collections::hash_map::Entry::Occupied(_o) => {}
        std::collections::hash_map::Entry::Vacant(v) => {
            // When we remove hacked code in handle_raft_entry_normal during migration,
            // some tests in handle_raft_entry_normal may fail, since it can observe a empty
            // cmd, thus creating region.
            warn!(
                "region {} not found when ingest, create for {}",
                region_id, node_id
            );
            let _ = v.insert(Default::default());
        }
    }
    let region = kvstore.get_mut(&region_id).unwrap();

    let index = header.index;
    let term = header.term;
    debug!("handle ingest sst";
        "header" => ?header,
        "region_id" => region_id,
        "snap len" => snaps.len,
    );

    for i in 0..snaps.len {
        let snapshot = snaps.views.add(i as usize);
        // let _path = std::str::from_utf8_unchecked((*snapshot).path.to_slice());
        let mut sst_reader =
            SSTReader::new(proxy_helper, &*(snapshot as *mut interfaces_ffi::SSTView));
        while sst_reader.remained() {
            let key = sst_reader.key();
            let value = sst_reader.value();
            let cf_index = (*snapshot).type_ as usize;
            write_kv_in_mem(region.as_mut(), cf_index, key.to_slice(), value.to_slice());
            sst_reader.next();
        }
    }

    {
        region.set_applied(header.index, header.term);
        region.apply_state.mut_truncated_state().set_index(index);
        region.apply_state.mut_truncated_state().set_term(term);
    }

    fail::fail_point!("on_handle_ingest_sst_return", |_e| {
        interfaces_ffi::EngineStoreApplyRes::None
    });
    write_to_db_data(
        &mut (*store.engine_store_server),
        region,
        String::from("ingest-sst"),
    );
    interfaces_ffi::EngineStoreApplyRes::Persist
}
