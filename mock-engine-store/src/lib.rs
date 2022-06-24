#![feature(slice_take)]

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    pin::Pin,
    sync::Mutex,
    time::Duration,
};

use engine_rocks::RocksEngine;
use engine_store_ffi::{
    interfaces::root::DB as ffi_interfaces, EngineStoreServerHelper, RaftStoreProxyFFIHelper,
    UnwrapExternCFunc,
};
use engine_test::raft::RaftTestEngine;
use engine_traits::{Engines, SyncMutable, CF_DEFAULT, CF_LOCK, CF_WRITE};
use protobuf::Message;
use raftstore::{engine_store_ffi, engine_store_ffi::RawCppPtr};
use tikv_util::{debug, info, warn};
// use kvproto::raft_serverpb::{
//     MergeState, PeerState, RaftApplyState, RaftLocalState, RaftSnapshotData, RegionLocalState,
// };

type RegionId = u64;
#[derive(Default, Clone)]
pub struct Region {
    pub region: kvproto::metapb::Region,
    // Which peer is me?
    peer: kvproto::metapb::Peer,
    // in-memory data
    pub data: [BTreeMap<Vec<u8>, Vec<u8>>; 3],
    // If we a key is deleted, it will immediately be removed from data,
    // We will record the key in pending_delete, so we can delete it from disk when flushing.
    pub pending_delete: [HashSet<Vec<u8>>; 3],
    pub pending_write: [BTreeMap<Vec<u8>, Vec<u8>>; 3],
    pub apply_state: kvproto::raft_serverpb::RaftApplyState,
    pub applied_term: u64,
}

impl Region {
    fn set_applied(&mut self, index: u64, term: u64) {
        self.apply_state.set_applied_index(index);
        self.applied_term = term;
    }

    fn new(meta: kvproto::metapb::Region) -> Self {
        Region {
            region: meta,
            peer: Default::default(),
            data: Default::default(),
            pending_delete: Default::default(),
            pending_write: Default::default(),
            apply_state: Default::default(),
            applied_term: 0,
        }
    }
}

pub struct EngineStoreServer {
    pub id: u64,
    pub engines: Option<Engines<RocksEngine, RaftTestEngine>>,
    pub kvstore: HashMap<RegionId, Box<Region>>,
}

impl EngineStoreServer {
    pub fn new(id: u64, engines: Option<Engines<RocksEngine, RaftTestEngine>>) -> Self {
        EngineStoreServer {
            id,
            engines,
            kvstore: Default::default(),
        }
    }

    pub fn get_mem(
        &self,
        region_id: u64,
        cf: ffi_interfaces::ColumnFamilyType,
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
}

pub struct EngineStoreServerWrap {
    pub engine_store_server: *mut EngineStoreServer,
    pub maybe_proxy_helper: std::option::Option<*mut RaftStoreProxyFFIHelper>,
    // Call `gen_cluster(cluster_ptr)`, and get which cluster this Server belong to.
    pub cluster_ptr: isize,
}

fn set_new_region_peer(new_region: &mut Region, store_id: u64) {
    if let Some(peer) = new_region
        .region
        .get_peers()
        .iter()
        .find(|&peer| peer.get_store_id() == store_id)
    {
        new_region.peer = peer.clone();
    } else {
        // This happens when region is not found.
    }
}

pub fn make_new_region(
    maybe_from_region: Option<kvproto::metapb::Region>,
    maybe_store_id: Option<u64>,
) -> Region {
    let mut region = Region {
        region: maybe_from_region.unwrap_or(Default::default()),
        ..Default::default()
    };
    if let Some(store_id) = maybe_store_id {
        set_new_region_peer(&mut region, store_id);
    }
    region
        .apply_state
        .mut_truncated_state()
        .set_index(raftstore::store::RAFT_INIT_LOG_INDEX);
    region
        .apply_state
        .mut_truncated_state()
        .set_term(raftstore::store::RAFT_INIT_LOG_TERM);
    region.set_applied(
        raftstore::store::RAFT_INIT_LOG_INDEX,
        raftstore::store::RAFT_INIT_LOG_TERM,
    );
    region
}

fn write_kv_in_mem(region: &mut Box<Region>, cf_index: usize, k: &[u8], v: &[u8]) {
    let data = &mut region.data[cf_index];
    let pending_delete = &mut region.pending_delete[cf_index];
    let pending_write = &mut region.pending_write[cf_index];
    pending_delete.remove(k);
    data.insert(k.to_vec(), v.to_vec());
    pending_write.insert(k.to_vec(), v.to_vec());
}

fn delete_kv_in_mem(region: &mut Box<Region>, cf_index: usize, k: &[u8]) {
    let data = &mut region.data[cf_index];
    let pending_delete = &mut region.pending_delete[cf_index];
    pending_delete.insert(k.to_vec());
    data.remove(k);
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

    unsafe fn handle_admin_raft_cmd(
        &mut self,
        req: &kvproto::raft_cmdpb::AdminRequest,
        resp: &kvproto::raft_cmdpb::AdminResponse,
        header: ffi_interfaces::RaftCmdHeader,
    ) -> ffi_interfaces::EngineStoreApplyRes {
        let region_id = header.region_id;
        let node_id = (*self.engine_store_server).id;
        info!("handle admin raft cmd"; "request"=>?req, "response"=>?resp, "index"=>header.index, "region-id"=>header.region_id);
        let do_handle_admin_raft_cmd = move |region: &mut Region| {
            if region.apply_state.get_applied_index() >= header.index {
                return ffi_interfaces::EngineStoreApplyRes::Persist;
            }

            ffi_interfaces::EngineStoreApplyRes::Persist
        };
        match (*self.engine_store_server).kvstore.entry(region_id) {
            std::collections::hash_map::Entry::Occupied(mut o) => {
                do_handle_admin_raft_cmd(o.get_mut())
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                warn!("region {} not found", region_id);
                do_handle_admin_raft_cmd(v.insert(Default::default()))
            }
        }
    }

    unsafe fn handle_write_raft_cmd(
        &mut self,
        cmds: ffi_interfaces::WriteCmdsView,
        header: ffi_interfaces::RaftCmdHeader,
    ) -> ffi_interfaces::EngineStoreApplyRes {
        let region_id = header.region_id;
        let server = &mut (*self.engine_store_server);
        let kv = &mut (*self.engine_store_server).engines.as_mut().unwrap().kv;

        let do_handle_write_raft_cmd = move |region: &mut Region| {
            if region.apply_state.get_applied_index() >= header.index {
                return ffi_interfaces::EngineStoreApplyRes::None;
            }
            for i in 0..cmds.len {
                let key = &*cmds.keys.add(i as _);
                let val = &*cmds.vals.add(i as _);
                let k = &key.to_slice();
                let v = &val.to_slice();
                debug!(
                    "handle_write_raft_cmd add K {:?} V {:?} to region {} node id {}",
                    &k[..std::cmp::min(4usize, k.len())],
                    &v[..std::cmp::min(4usize, v.len())],
                    region_id,
                    server.id
                );
                let tp = &*cmds.cmd_types.add(i as _);
                let cf = &*cmds.cmd_cf.add(i as _);
                let cf_index = (*cf) as u8;
                let data = &mut region.data[cf_index as usize];
                match tp {
                    engine_store_ffi::WriteCmdType::Put => {
                        let tikv_key = keys::data_key(key.to_slice());
                        kv.put_cf(
                            cf_to_name(cf.to_owned().into()),
                            &tikv_key,
                            &val.to_slice().to_vec(),
                        );
                    }
                    engine_store_ffi::WriteCmdType::Del => {
                        let tikv_key = keys::data_key(key.to_slice());
                        kv.delete_cf(cf_to_name(cf.to_owned().into()), &tikv_key);
                    }
                }
            }
            // Do not advance apply index
            ffi_interfaces::EngineStoreApplyRes::None
        };

        match (*self.engine_store_server).kvstore.entry(region_id) {
            std::collections::hash_map::Entry::Occupied(mut o) => {
                do_handle_write_raft_cmd(o.get_mut())
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                warn!("region {} not found", region_id);
                do_handle_write_raft_cmd(v.insert(Default::default()))
            }
        }
    }
}

unsafe extern "C" fn ffi_set_pb_msg_by_bytes(
    type_: ffi_interfaces::MsgPBType,
    ptr: ffi_interfaces::RawVoidPtr,
    buff: ffi_interfaces::BaseBuffView,
) {
    match type_ {
        ffi_interfaces::MsgPBType::ReadIndexResponse => {
            let v = &mut *(ptr as *mut kvproto::kvrpcpb::ReadIndexResponse);
            v.merge_from_bytes(buff.to_slice()).unwrap();
        }
        ffi_interfaces::MsgPBType::ServerInfoResponse => {
            let v = &mut *(ptr as *mut kvproto::diagnosticspb::ServerInfoResponse);
            v.merge_from_bytes(buff.to_slice()).unwrap();
        }
        ffi_interfaces::MsgPBType::RegionLocalState => {
            let v = &mut *(ptr as *mut kvproto::raft_serverpb::RegionLocalState);
            v.merge_from_bytes(buff.to_slice()).unwrap();
        }
    }
}

pub fn gen_engine_store_server_helper(
    wrap: Pin<&EngineStoreServerWrap>,
) -> EngineStoreServerHelper {
    EngineStoreServerHelper {
        magic_number: ffi_interfaces::RAFT_STORE_PROXY_MAGIC_NUMBER,
        version: ffi_interfaces::RAFT_STORE_PROXY_VERSION,
        inner: &(*wrap) as *const EngineStoreServerWrap as *mut _,
        fn_gen_cpp_string: Some(ffi_gen_cpp_string),
        fn_handle_write_raft_cmd: Some(ffi_handle_write_raft_cmd),
        fn_handle_admin_raft_cmd: Some(ffi_handle_admin_raft_cmd),
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
        fn_get_config: None,
        fn_set_store: None,
        fn_set_pb_msg_by_bytes: Some(ffi_set_pb_msg_by_bytes),
    }
}

unsafe fn into_engine_store_server_wrap(
    arg1: *const ffi_interfaces::EngineStoreServerWrap,
) -> &'static mut EngineStoreServerWrap {
    &mut *(arg1 as *mut EngineStoreServerWrap)
}

unsafe extern "C" fn ffi_handle_admin_raft_cmd(
    arg1: *const ffi_interfaces::EngineStoreServerWrap,
    arg2: ffi_interfaces::BaseBuffView,
    arg3: ffi_interfaces::BaseBuffView,
    arg4: ffi_interfaces::RaftCmdHeader,
) -> ffi_interfaces::EngineStoreApplyRes {
    let store = into_engine_store_server_wrap(arg1);
    let mut req = kvproto::raft_cmdpb::AdminRequest::default();
    let mut resp = kvproto::raft_cmdpb::AdminResponse::default();
    req.merge_from_bytes(arg2.to_slice()).unwrap();
    resp.merge_from_bytes(arg3.to_slice()).unwrap();
    store.handle_admin_raft_cmd(&req, &resp, arg4)
}

unsafe extern "C" fn ffi_handle_write_raft_cmd(
    arg1: *const ffi_interfaces::EngineStoreServerWrap,
    arg2: ffi_interfaces::WriteCmdsView,
    arg3: ffi_interfaces::RaftCmdHeader,
) -> ffi_interfaces::EngineStoreApplyRes {
    let store = into_engine_store_server_wrap(arg1);
    store.handle_write_raft_cmd(arg2, arg3)
}

enum RawCppPtrTypeImpl {
    None = 0,
    String,
    PreHandledSnapshotWithBlock,
    WakerNotifier,
}

impl From<ffi_interfaces::RawCppPtrType> for RawCppPtrTypeImpl {
    fn from(o: ffi_interfaces::RawCppPtrType) -> Self {
        match o {
            0 => RawCppPtrTypeImpl::None,
            1 => RawCppPtrTypeImpl::String,
            2 => RawCppPtrTypeImpl::PreHandledSnapshotWithBlock,
            3 => RawCppPtrTypeImpl::WakerNotifier,
            _ => unreachable!(),
        }
    }
}

impl Into<ffi_interfaces::RawCppPtrType> for RawCppPtrTypeImpl {
    fn into(self) -> ffi_interfaces::RawCppPtrType {
        match self {
            RawCppPtrTypeImpl::None => 0,
            RawCppPtrTypeImpl::String => 1,
            RawCppPtrTypeImpl::PreHandledSnapshotWithBlock => 2,
            RawCppPtrTypeImpl::WakerNotifier => 3,
        }
    }
}

extern "C" fn ffi_gen_cpp_string(s: ffi_interfaces::BaseBuffView) -> ffi_interfaces::RawCppPtr {
    let str = Box::new(Vec::from(s.to_slice()));
    let ptr = Box::into_raw(str);
    ffi_interfaces::RawCppPtr {
        ptr: ptr as *mut _,
        type_: RawCppPtrTypeImpl::String.into(),
    }
}

pub struct RawCppStringPtrGuard(ffi_interfaces::RawCppStringPtr);

impl Default for RawCppStringPtrGuard {
    fn default() -> Self {
        Self(std::ptr::null_mut())
    }
}

impl std::convert::AsRef<ffi_interfaces::RawCppStringPtr> for RawCppStringPtrGuard {
    fn as_ref(&self) -> &ffi_interfaces::RawCppStringPtr {
        &self.0
    }
}
impl std::convert::AsMut<ffi_interfaces::RawCppStringPtr> for RawCppStringPtrGuard {
    fn as_mut(&mut self) -> &mut ffi_interfaces::RawCppStringPtr {
        &mut self.0
    }
}

impl Drop for RawCppStringPtrGuard {
    fn drop(&mut self) {
        ffi_interfaces::RawCppPtr {
            ptr: self.0 as *mut _,
            type_: RawCppPtrTypeImpl::String.into(),
        };
    }
}

impl RawCppStringPtrGuard {
    pub fn as_str(&self) -> &[u8] {
        let s = self.0 as *mut Vec<u8>;
        unsafe { &*s }
    }
}

pub struct ProxyNotifier {
    cv: std::sync::Condvar,
    mutex: Mutex<()>,
    // multi notifiers single receiver model. use another flag to avoid waiting until timeout.
    flag: std::sync::atomic::AtomicBool,
}

impl ProxyNotifier {
    pub fn blocked_wait_for(&self, timeout: Duration) {
        // if flag from false to false, wait for notification.
        // if flag from true to false, do nothing.
        if !self.flag.swap(false, std::sync::atomic::Ordering::AcqRel) {
            {
                let lock = self.mutex.lock().unwrap();
                if !self.flag.load(std::sync::atomic::Ordering::Acquire) {
                    self.cv.wait_timeout(lock, timeout);
                }
            }
            self.flag.store(false, std::sync::atomic::Ordering::Release);
        }
    }

    pub fn wake(&self) {
        // if flag from false -> true, then wake up.
        // if flag from true -> true, do nothing.
        if !self.flag.swap(true, std::sync::atomic::Ordering::AcqRel) {
            let _ = self.mutex.lock().unwrap();
            self.cv.notify_one();
        }
    }

    pub fn new_raw() -> RawCppPtr {
        let notifier = Box::new(Self {
            cv: Default::default(),
            mutex: Mutex::new(()),
            flag: std::sync::atomic::AtomicBool::new(false),
        });

        RawCppPtr {
            ptr: Box::into_raw(notifier) as _,
            type_: RawCppPtrTypeImpl::WakerNotifier.into(),
        }
    }
}

extern "C" fn ffi_gc_raw_cpp_ptr(
    ptr: ffi_interfaces::RawVoidPtr,
    tp: ffi_interfaces::RawCppPtrType,
) {
    match RawCppPtrTypeImpl::from(tp) {
        RawCppPtrTypeImpl::None => {}
        RawCppPtrTypeImpl::String => unsafe {
            Box::<Vec<u8>>::from_raw(ptr as *mut _);
        },
        RawCppPtrTypeImpl::PreHandledSnapshotWithBlock => unsafe {
            Box::<PrehandledSnapshot>::from_raw(ptr as *mut _);
        },
        RawCppPtrTypeImpl::WakerNotifier => unsafe {
            Box::from_raw(ptr as *mut ProxyNotifier);
        },
    }
}

unsafe extern "C" fn ffi_atomic_update_proxy(
    arg1: *mut ffi_interfaces::EngineStoreServerWrap,
    arg2: *mut ffi_interfaces::RaftStoreProxyFFIHelper,
) {
    let store = into_engine_store_server_wrap(arg1);
    store.maybe_proxy_helper = Some(&mut *(arg2 as *mut RaftStoreProxyFFIHelper));
}

unsafe extern "C" fn ffi_handle_destroy(
    arg1: *mut ffi_interfaces::EngineStoreServerWrap,
    arg2: u64,
) {
    let store = into_engine_store_server_wrap(arg1);
    (*store.engine_store_server).kvstore.remove(&arg2);
}

type MockRaftProxyHelper = RaftStoreProxyFFIHelper;

pub struct SSTReader<'a> {
    proxy_helper: &'a MockRaftProxyHelper,
    inner: ffi_interfaces::SSTReaderPtr,
    type_: ffi_interfaces::ColumnFamilyType,
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
        view: &'a ffi_interfaces::SSTView,
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

    pub unsafe fn key(&mut self) -> ffi_interfaces::BaseBuffView {
        (self.proxy_helper.sst_reader_interfaces.fn_key.into_inner())(
            self.inner.clone(),
            self.type_,
        )
    }

    pub unsafe fn value(&mut self) -> ffi_interfaces::BaseBuffView {
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

struct PrehandledSnapshot {
    pub region: std::option::Option<Region>,
}

unsafe extern "C" fn ffi_pre_handle_snapshot(
    arg1: *mut ffi_interfaces::EngineStoreServerWrap,
    region_buff: ffi_interfaces::BaseBuffView,
    peer_id: u64,
    snaps: ffi_interfaces::SSTViewVec,
    index: u64,
    term: u64,
) -> ffi_interfaces::RawCppPtr {
    let store = into_engine_store_server_wrap(arg1);
    let proxy_helper = &mut *(store.maybe_proxy_helper.unwrap());
    let kvstore = &mut (*store.engine_store_server).kvstore;

    let mut req = kvproto::metapb::Region::default();
    assert_ne!(region_buff.data, std::ptr::null());
    assert_ne!(region_buff.len, 0);
    req.merge_from_bytes(region_buff.to_slice()).unwrap();

    let req_id = req.id;

    let mut region = Box::new(Region::new(req));
    debug!(
        "pre handle snaps with len {} peer_id {} region {:?}",
        snaps.len, peer_id, region.region
    );
    for i in 0..snaps.len {
        let mut snapshot = snaps.views.add(i as usize);
        let view = &*(snapshot as *mut ffi_interfaces::SSTView);
        let mut sst_reader = SSTReader::new(proxy_helper, view);

        while sst_reader.remained() {
            let key = sst_reader.key();
            let value = sst_reader.value();

            let cf_index = (*snapshot).type_ as u8;
            let data = &mut region.data[cf_index as usize];
            let _ = data.insert(key.to_slice().to_vec(), value.to_slice().to_vec());

            sst_reader.next();
        }
    }
    {
        region.set_applied(index, term);
        region.apply_state.mut_truncated_state().set_index(index);
        region.apply_state.mut_truncated_state().set_term(term);
    }
    ffi_interfaces::RawCppPtr {
        ptr: Box::into_raw(Box::new(PrehandledSnapshot {
            region: Some(*region),
        })) as *const Region as ffi_interfaces::RawVoidPtr,
        type_: RawCppPtrTypeImpl::PreHandledSnapshotWithBlock.into(),
    }
}

pub fn cf_to_name(cf: ffi_interfaces::ColumnFamilyType) -> &'static str {
    match cf {
        ffi_interfaces::ColumnFamilyType::Lock => CF_LOCK,
        ffi_interfaces::ColumnFamilyType::Write => CF_WRITE,
        ffi_interfaces::ColumnFamilyType::Default => CF_DEFAULT,
        _ => unreachable!(),
    }
}

unsafe extern "C" fn ffi_apply_pre_handled_snapshot(
    arg1: *mut ffi_interfaces::EngineStoreServerWrap,
    arg2: ffi_interfaces::RawVoidPtr,
    arg3: ffi_interfaces::RawCppPtrType,
) {
    let store = into_engine_store_server_wrap(arg1);
    let req = &mut *(arg2 as *mut PrehandledSnapshot);
    let node_id = (*store.engine_store_server).id;

    let req_id = req.region.as_ref().unwrap().region.id;

    &(*store.engine_store_server)
        .kvstore
        .insert(req_id, Box::new(req.region.take().unwrap()));

    let region = (*store.engine_store_server)
        .kvstore
        .get_mut(&req_id)
        .unwrap();

    let kv = &mut (*store.engine_store_server).engines.as_mut().unwrap().kv;
    for cf in 0..3 {
        for (k, v) in std::mem::take(region.data.as_mut().get_mut(cf).unwrap()).into_iter() {
            let tikv_key = keys::data_key(k.as_slice());
            let cf_name = cf_to_name(cf.into());
            kv.put_cf(cf_name, &tikv_key, &v);
        }
    }
}

unsafe extern "C" fn ffi_handle_ingest_sst(
    arg1: *mut ffi_interfaces::EngineStoreServerWrap,
    snaps: ffi_interfaces::SSTViewVec,
    header: ffi_interfaces::RaftCmdHeader,
) -> ffi_interfaces::EngineStoreApplyRes {
    let store = into_engine_store_server_wrap(arg1);
    let proxy_helper = &mut *(store.maybe_proxy_helper.unwrap());
    debug!("ingest sst with len {}", snaps.len);

    let region_id = header.region_id;
    let kvstore = &mut (*store.engine_store_server).kvstore;
    let kv = &mut (*store.engine_store_server).engines.as_mut().unwrap().kv;
    let region = kvstore.get_mut(&region_id).unwrap().as_mut();

    let index = header.index;
    let term = header.term;

    for i in 0..snaps.len {
        let mut snapshot = snaps.views.add(i as usize);
        let mut sst_reader =
            SSTReader::new(proxy_helper, &*(snapshot as *mut ffi_interfaces::SSTView));

        while sst_reader.remained() {
            let key = sst_reader.key();
            let value = sst_reader.value();

            let cf_index = (*snapshot).type_ as u8;

            let tikv_key = keys::data_key(key.to_slice());
            let cf_name = cf_to_name((*snapshot).type_);
            kv.put_cf(cf_name, &tikv_key, &value.to_slice());
            sst_reader.next();
        }
    }

    {
        region.set_applied(header.index, header.term);
        region.apply_state.mut_truncated_state().set_index(index);
        region.apply_state.mut_truncated_state().set_term(term);
    }

    ffi_interfaces::EngineStoreApplyRes::Persist
}

unsafe extern "C" fn ffi_handle_compute_store_stats(
    arg1: *mut ffi_interfaces::EngineStoreServerWrap,
) -> ffi_interfaces::StoreStats {
    ffi_interfaces::StoreStats {
        fs_stats: ffi_interfaces::FsStats {
            used_size: 0,
            avail_size: 0,
            capacity_size: 0,
            ok: 1,
        },
        engine_bytes_written: 0,
        engine_keys_written: 0,
        engine_bytes_read: 0,
        engine_keys_read: 0,
    }
}
