use std::{collections::hash_map::Entry, pin::Pin, sync::Mutex, time::Duration};

use engine_store_ffi::ffi::{
    ffi_gc_rust_ptr, ffi_make_async_waker, ffi_make_read_index_task, ffi_make_timer_task,
    ffi_poll_read_index_task, ffi_poll_timer_task,
    interfaces_ffi::{RaftStoreProxyFFIHelper, RawRustPtr, RawVoidPtr},
    ProtoMsgBaseBuff,
};

use crate::{shared::ffi, utils::v1::*};

#[derive(Default)]
struct GcMonitor {
    data: Mutex<collections::HashMap<u32, isize>>,
}

impl GcMonitor {
    fn add(&self, ptr: &RawRustPtr, x: isize) {
        let data = &mut *self.data.lock().unwrap();
        match data.entry(ptr.type_) {
            Entry::Occupied(mut v) => {
                *v.get_mut() += x;
            }
            Entry::Vacant(v) => {
                v.insert(x);
            }
        }
    }
    fn valid_clean(&self) -> bool {
        let data = &*self.data.lock().unwrap();
        for (k, v) in data {
            if *v != 0 {
                tikv_util::error!("GcMonitor::valid_clean failed at type {} refcount {}", k, v);
                return false;
            }
        }
        return true;
    }
    fn is_empty(&self) -> bool {
        let data = &*self.data.lock().unwrap();
        data.is_empty()
    }
}

lazy_static! {
    static ref GC_MONITOR: GcMonitor = GcMonitor::default();
}

struct RawRustPtrWrap(RawRustPtr);

impl RawRustPtrWrap {
    fn new(ptr: RawRustPtr) -> Self {
        GC_MONITOR.add(&ptr, 1);
        Self(ptr)
    }
}

impl Drop for RawRustPtrWrap {
    fn drop(&mut self) {
        ffi_gc_rust_ptr(self.0.ptr, self.0.type_);
        GC_MONITOR.add(&self.0, -1);
    }
}

struct ReadIndexFutureTask {
    ptr: RawRustPtrWrap,
}

struct Waker {
    _inner: RawRustPtrWrap,
    notifier: RawVoidPtr,
}

impl Waker {
    pub fn new() -> Self {
        let notifier = mock_engine_store::ProxyNotifier::new_raw();
        let ptr = notifier.ptr;
        let notifier = ffi_make_async_waker(Some(ffi_wake), notifier);
        Self {
            _inner: RawRustPtrWrap::new(notifier),
            notifier: ptr,
        }
    }

    fn wait_for(&self, timeout: Duration) {
        // Block wait for test
        self.get_notifier().blocked_wait_for(timeout)
    }

    fn get_notifier(&self) -> &mock_engine_store::ProxyNotifier {
        unsafe { &*(self.notifier as *mut mock_engine_store::ProxyNotifier) }
    }

    fn get_raw_waker(&self) -> RawVoidPtr {
        self._inner.0.ptr
    }
}

fn blocked_read_index(
    req: &kvproto::kvrpcpb::ReadIndexRequest,
    ffi_helper: &RaftStoreProxyFFIHelper,
    waker: Option<&Waker>,
) -> Option<kvproto::kvrpcpb::ReadIndexResponse> {
    let mut resp = kvproto::kvrpcpb::ReadIndexResponse::default();

    let mut task = {
        let req = ProtoMsgBaseBuff::new(req);
        let ptr = ffi_make_read_index_task(ffi_helper.proxy_ptr, Pin::new(&req).into());
        if ptr.is_null() {
            return None;
        } else {
            Some(ReadIndexFutureTask {
                ptr: RawRustPtrWrap::new(ptr),
            })
        }
    };

    while task.is_some() {
        let t = task.as_ref().unwrap();
        let waker_ptr = match waker {
            None => std::ptr::null_mut(),
            Some(w) => w.get_raw_waker(),
        };
        if 0 != ffi_poll_read_index_task(
            ffi_helper.proxy_ptr,
            t.ptr.0.ptr,
            &mut resp as *mut _ as RawVoidPtr,
            waker_ptr,
        ) {
            task = None;
        } else {
            if let Some(w) = waker {
                w.wait_for(Duration::from_secs(5));
            } else {
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    }
    Some(resp)
}

extern "C" fn ffi_wake(data: RawVoidPtr) {
    let notifier = unsafe { &mut *(data as *mut mock_engine_store::ProxyNotifier) };
    notifier.wake()
}

#[test]
fn test_read_index_normal() {
    // Initialize cluster
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);
    configure_for_lease_read(&mut cluster, Some(50), Some(10_000));
    cluster.cfg.raft_store.raft_heartbeat_ticks = 1;
    pd_client.disable_default_operator();

    // Set region and peers
    let r1 = cluster.run_conf_change();
    let p1 = new_peer(1, 1);
    let p2 = new_peer(2, 2);
    let p3 = new_peer(3, 3);

    cluster.pd_client.must_add_peer(r1, p2.clone());
    cluster.pd_client.must_add_peer(r1, p3.clone());
    cluster.must_put(b"k0", b"v0");
    cluster.pd_client.must_none_pending_peer(p2.clone());
    cluster.pd_client.must_none_pending_peer(p3.clone());
    let region = cluster.get_region(b"k0");
    assert_eq!(cluster.leader_of_region(region.get_id()).unwrap(), p1);

    let waker = Waker::new();

    for (id, peer, f) in &[(2, p2, true), (3, p3, false)] {
        iter_ffi_helpers(
            &cluster,
            Some(vec![*id]),
            &mut |_, ffi_helper: &mut FFIHelperSet| {
                let mut request = kvproto::kvrpcpb::ReadIndexRequest::default();

                {
                    let context = request.mut_context();
                    context.set_region_id(region.get_id());
                    context.set_peer(peer.clone());
                    context.set_region_epoch(region.get_region_epoch().clone());
                    request.set_start_ts(666);

                    let mut range = kvproto::kvrpcpb::KeyRange::default();
                    range.set_start_key(region.get_start_key().to_vec());
                    range.set_end_key(region.get_end_key().to_vec());
                    request.mut_ranges().push(range);

                    debug!("make read index request {:?}", &request);
                }
                let w = if *f { Some(&waker) } else { None };
                let resp = blocked_read_index(&request, &*ffi_helper.proxy_helper, w).unwrap();
                assert!(resp.get_read_index() != 0);
                assert!(!resp.has_region_error());
                assert!(!resp.has_locked());
            },
        );
    }

    drop(waker);

    {
        assert!(!GC_MONITOR.is_empty());
        assert!(GC_MONITOR.valid_clean());
    }

    cluster.shutdown();
}

/// If a read index request is received while region state is Applying,
/// it could be handled correctly.
#[test]
fn test_read_index_applying() {
    // Initialize cluster
    let (mut cluster, pd_client) = new_mock_cluster(0, 2);
    configure_for_lease_read(&mut cluster, Some(50), Some(10_000));
    cluster.cfg.raft_store.raft_heartbeat_ticks = 1;
    cluster.cfg.raft_store.raft_log_compact_sync_interval = ReadableDuration::millis(500);
    pd_client.disable_default_operator();
    disable_auto_gen_compact_log(&mut cluster);
    // Otherwise will panic with `assert_eq!(apply_state, last_applied_state)`.
    fail::cfg("on_pre_write_apply_state", "return(true)").unwrap();
    // Set region and peers
    let r1 = cluster.run_conf_change();
    let p1 = new_peer(1, 1);
    let p2 = new_learner_peer(2, 2);

    cluster.pd_client.must_add_peer(r1, p2.clone());
    cluster.must_put(b"k0", b"v");
    {
        let prev_state = maybe_collect_states(&cluster.cluster_ext, r1, Some(vec![1]));
        let (compact_index, compact_term) = get_valid_compact_index_by(&prev_state, Some(vec![1]));
    }
    cluster.pd_client.must_none_pending_peer(p2.clone());
    // assert_eq!(cluster.pd_client.get_pending_peers().len(), 0);
    let region = cluster.get_region(b"k0");
    assert_eq!(cluster.leader_of_region(region.get_id()).unwrap(), p1);

    check_key(&cluster, b"k0", b"v", Some(true), None, Some(vec![1]));

    fail::cfg("region_apply_snap", "return").unwrap();
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(r1, 2)
            .msg_type(MessageType::MsgAppend)
            .direction(Direction::Both),
    ));

    for i in 1..5 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v");
    }

    check_key(&cluster, b"k4", b"v", Some(true), None, Some(vec![1]));

    {
        let prev_state = collect_all_states(&cluster.cluster_ext, r1);
        let (compact_index, compact_term) = get_valid_compact_index_by(&prev_state, Some(vec![1]));
        let compact_log = test_raftstore::new_compact_log_request(compact_index, compact_term);
        let req = test_raftstore::new_admin_request(r1, region.get_region_epoch(), compact_log);
        let res = cluster
            .call_command_on_leader(req, Duration::from_secs(3))
            .unwrap();
        assert!(!res.get_header().has_error(), "{:?}", res);
    }

    cluster.must_put(b"kz", b"v");
    check_key(&cluster, b"kz", b"v", Some(true), None, Some(vec![1]));
    check_key(&cluster, b"k1", b"v", Some(false), None, Some(vec![2]));

    // Wait until gc.
    std::thread::sleep(std::time::Duration::from_millis(1500));

    cluster.clear_send_filters();

    let waker = Waker::new();

    for (id, peer, f) in &[(2, p2, true)] {
        iter_ffi_helpers(
            &cluster,
            Some(vec![*id]),
            &mut |_, ffi_helper: &mut FFIHelperSet| {
                assert_eq!(
                    general_get_region_local_state(
                        &ffi_helper.engine_store_server.engines.as_ref().unwrap().kv,
                        r1
                    )
                    .unwrap()
                    .get_state(),
                    PeerState::Applying
                );
                let mut request = kvproto::kvrpcpb::ReadIndexRequest::default();

                {
                    let context = request.mut_context();
                    context.set_region_id(region.get_id());
                    context.set_peer(peer.clone());
                    context.set_region_epoch(region.get_region_epoch().clone());
                    request.set_start_ts(666);

                    let mut range = kvproto::kvrpcpb::KeyRange::default();
                    range.set_start_key(region.get_start_key().to_vec());
                    range.set_end_key(region.get_end_key().to_vec());
                    request.mut_ranges().push(range);

                    debug!("make read index request {:?}", &request);
                }
                let w = if *f { Some(&waker) } else { None };
                let resp = blocked_read_index(&request, &*ffi_helper.proxy_helper, w).unwrap();
                assert_ne!(resp.get_read_index(), 0);
                debug!("resp detail {:?}", resp);
                assert!(!resp.has_region_error());
                assert!(!resp.has_locked());
            },
        );
    }

    drop(waker);

    {
        assert!(!GC_MONITOR.is_empty());
        assert!(GC_MONITOR.valid_clean());
    }

    cluster.shutdown();
    fail::remove("on_pre_write_apply_state");
    fail::remove("region_apply_snap");
}

#[test]
fn test_util() {
    // test timer
    mock_engine_store::mock_cluster::init_global_ffi_helper_set();
    {
        let timeout = 128;
        let task = RawRustPtrWrap::new(ffi_make_timer_task(timeout));
        assert_eq!(0, unsafe {
            ffi_poll_timer_task(task.0.ptr, std::ptr::null_mut())
        });
        std::thread::sleep(Duration::from_millis(timeout + 20));
        assert_ne!(
            unsafe { ffi_poll_timer_task(task.0.ptr, std::ptr::null_mut()) },
            0
        );

        let task = RawRustPtrWrap::new(ffi_make_timer_task(timeout));
        let waker = Waker::new();
        assert_eq!(0, unsafe {
            ffi_poll_timer_task(task.0.ptr, waker.get_raw_waker())
        });
        let now = std::time::Instant::now();
        waker.wait_for(Duration::from_secs(256));
        assert_ne!(0, unsafe {
            ffi_poll_timer_task(task.0.ptr, waker.get_raw_waker())
        });
        assert!(now.elapsed() < Duration::from_secs(256));
    }
    assert!(GC_MONITOR.valid_clean());
}

use kvproto::{
    kvrpcpb::{Context, DiskFullOpt, KeyRange},
    raft_cmdpb::{CmdType, RaftCmdRequest, RaftRequestHeader, Request as RaftRequest},
    raft_serverpb::RaftMessage,
};
use raftstore::{
    router::RaftStoreRouter,
    store::{msg::Callback, RaftCmdExtraOpts, ReadIndexContext},
};
use tokio::sync::oneshot;
use txn_types::{Key, Lock, LockType, TimeStamp};
use uuid::Uuid;

use crate::utils::v1_server::{new_server_cluster, ChannelBuilder, Environment, TikvClient};

// https://github.com/tikv/tikv/issues/16823
#[test]
fn test_raft_cmd_request_cant_advanve_max_ts() {
    use kvproto::kvrpcpb::{ReadIndexRequest, ReadIndexResponse};

    let mut cluster = new_server_cluster(0, 1);
    cluster.run();

    let cm = cluster.sim.read().unwrap().get_concurrency_manager(1);

    let region = cluster.get_region(b"");
    let leader = region.get_peers()[0].clone();
    let addr = cluster.sim.rl().get_addr(leader.get_store_id()).to_owned();

    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = TikvClient::new(channel);

    let mut ctx = Context::default();
    let region_id = leader.get_id();
    ctx.set_region_id(leader.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(leader);

    let read_index = |ranges: &[(&[u8], &[u8])]| {
        let start_ts = block_on(cluster.pd_client.get_tso()).unwrap();

        let mut cmd = RaftCmdRequest::default();
        {
            let mut header = RaftRequestHeader::default();
            let mut inner_req = RaftRequest::default();
            inner_req.set_cmd_type(CmdType::ReadIndex);
            inner_req
                .mut_read_index()
                .set_start_ts(start_ts.into_inner());

            let mut req = ReadIndexRequest::default();
            let start_ts = block_on(cluster.pd_client.get_tso()).unwrap();
            req.set_context(ctx.clone());
            req.set_start_ts(start_ts.into_inner());
            for &(start_key, end_key) in ranges {
                let mut range = KeyRange::default();
                range.set_start_key(start_key.to_vec());
                range.set_end_key(end_key.to_vec());
                req.mut_ranges().push(range);
            }

            header.set_region_id(region_id);
            header.set_peer(req.get_context().get_peer().clone());
            header.set_region_epoch(req.get_context().get_region_epoch().clone());
            cmd.set_header(header);
            cmd.set_requests(vec![inner_req].into());
        }

        let (result_tx, result_rx) = oneshot::channel();
        let router = cluster.get_router(1).unwrap();
        if let Err(e) = router.send_command(
            cmd,
            Callback::read(Box::new(move |resp| {
                result_tx.send(resp.response).unwrap();
            })),
            RaftCmdExtraOpts {
                deadline: None,
                disk_full_opt: DiskFullOpt::AllowedOnAlmostFull,
            },
        ) {
            panic!("router send msg failed, error: {}", e);
        }

        let resp = block_on(result_rx).unwrap();
        (resp.get_responses()[0].get_read_index().clone(), start_ts)
    };

    // wait a while until the node updates its own max ts
    std::thread::sleep(Duration::from_millis(300));

    let prev_cm_max_ts = cm.max_ts();
    let (resp, start_ts) = read_index(&[(b"l", b"yz")]);
    assert!(!resp.has_locked());
    // Actually not changed
    assert_eq!(cm.max_ts(), prev_cm_max_ts);
    assert_ne!(cm.max_ts(), start_ts);
    cluster.shutdown();
    fail::remove("on_pre_write_apply_state")
}

// https://github.com/tikv/tikv/pull/8669/files
#[test]
fn test_raft_cmd_request_learner_advanve_max_ts() {
    use kvproto::kvrpcpb::{ReadIndexRequest, ReadIndexResponse};

    let mut cluster = new_server_cluster(0, 2);
    cluster.pd_client.disable_default_operator();
    let region_id = cluster.run_conf_change();
    let region = cluster.get_region(b"");
    assert_eq!(region_id, 1);
    assert_eq!(region.get_id(), 1);
    let leader = region.get_peers()[0].clone();

    fail::cfg("on_pre_write_apply_state", "return(true)").unwrap();
    let learner = new_learner_peer(2, 2);
    cluster.pd_client.must_add_peer(1, learner.clone());

    let cm = cluster.sim.read().unwrap().get_concurrency_manager(1);
    let keys: Vec<_> = vec![b"k", b"l"]
        .into_iter()
        .map(|k| Key::from_raw(k))
        .collect();
    let guards = block_on(cm.lock_keys(keys.iter()));
    let lock = Lock::new(
        LockType::Put,
        b"k".to_vec(),
        1.into(),
        20000,
        None,
        1.into(),
        1,
        2.into(),
        false,
    );
    guards[0].with_lock(|l| *l = Some(lock.clone()));

    let addr = cluster.sim.rl().get_addr(learner.get_store_id()).to_owned();

    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = TikvClient::new(channel);

    // cluster.must_put(b"k", b"v");

    let read_index = |ranges: &[(&[u8], &[u8])]| {
        let start_ts = block_on(cluster.pd_client.get_tso()).unwrap();

        // https://github.com/pingcap/tiflash/blob/14a127820d0530e496af624bb5b69acd48caf747/dbms/src/Storages/KVStore/Read/ReadIndex.cpp#L39
        let mut ctx = Context::default();
        let learner = learner.clone();
        ctx.set_region_id(region_id);
        ctx.set_region_epoch(region.get_region_epoch().clone());
        ctx.set_peer(learner);
        let mut read_index_request = ReadIndexRequest::default();
        read_index_request.set_context(ctx);
        read_index_request.set_start_ts(start_ts.into_inner());
        for (s, e) in ranges {
            let mut r = KeyRange::new();
            r.set_start_key(s.to_vec());
            r.set_end_key(e.to_vec());
            read_index_request.mut_ranges().push(r);
        }
        let mut cmd =
            proxy_ffi::read_index_helper::gen_read_index_raft_cmd_req(&mut read_index_request);

        let (result_tx, result_rx) = oneshot::channel();
        let router = cluster.get_router(2).unwrap();
        if let Err(e) = router.send_command(
            cmd,
            Callback::read(Box::new(move |resp| {
                result_tx.send(resp.response).unwrap();
            })),
            RaftCmdExtraOpts {
                deadline: None,
                disk_full_opt: DiskFullOpt::AllowedOnAlmostFull,
            },
        ) {
            panic!("router send msg failed, error: {}", e);
        }

        let resp = block_on(result_rx).unwrap();
        (resp.get_responses()[0].get_read_index().clone(), start_ts)
    };

    // wait a while until the node updates its own max ts
    std::thread::sleep(Duration::from_millis(3000));

    must_wait_until_cond_node(
        &cluster.cluster_ext,
        region_id,
        None,
        &|states: &States| -> bool {
            states.in_disk_region_state.get_region().get_peers().len() == 2
        },
    );

    let prev_cm_max_ts = cm.max_ts();
    let (resp, start_ts) = read_index(&[(b"l", b"yz")]);
    assert!(!resp.has_locked());
    // Actually not changed
    assert_ne!(cm.max_ts(), prev_cm_max_ts);
    assert_eq!(cm.max_ts(), start_ts);

    // `gen_read_index_raft_cmd_req` can only handle one key-range
    let (resp, start_ts) = read_index(&[(b"j", b"k0")]);
    assert_eq!(resp.get_locked(), &lock.into_lock_info(b"k".to_vec()));
    assert_eq!(cm.max_ts(), start_ts);

    drop(guards);

    let (resp, start_ts) = read_index(&[(b"a", b"z")]);
    assert!(!resp.has_locked());
    assert_eq!(cm.max_ts(), start_ts);
    cluster.shutdown();
    fail::remove("on_pre_write_apply_state")
}

#[test]
fn test_raft_message_can_advanve_max_ts() {
    use kvproto::raft_cmdpb::{ReadIndexRequest, ReadIndexResponse};
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();

    let cm = cluster.sim.read().unwrap().get_concurrency_manager(1);

    let region = cluster.get_region(b"");
    let leader = region.get_peers()[0].clone();
    let follower = new_learner_peer(2, 2);
    let addr = cluster.sim.rl().get_addr(leader.get_store_id()).to_owned();

    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = TikvClient::new(channel);

    let region_id = leader.get_id();

    let read_index = |ranges: &[(&[u8], &[u8])]| {
        let mut m = raft::eraftpb::Message::default();
        m.set_msg_type(MessageType::MsgReadIndex);
        let mut read_index_req = ReadIndexRequest::default();
        let start_ts = block_on(cluster.pd_client.get_tso()).unwrap();
        read_index_req.set_start_ts(start_ts.into_inner());
        for &(start_key, end_key) in ranges {
            let mut range = KeyRange::default();
            range.set_start_key(start_key.to_vec());
            range.set_end_key(end_key.to_vec());
            read_index_req.mut_key_ranges().push(range);
        }

        let rctx = ReadIndexContext {
            id: Uuid::new_v4(),
            request: Some(read_index_req),
            locked: None,
        };
        let mut e = raft::eraftpb::Entry::default();
        e.set_data(rctx.to_bytes().into());
        m.mut_entries().push(e);
        m.set_from(2);

        let mut raft_msg = kvproto::raft_serverpb::RaftMessage::default();
        raft_msg.set_region_id(region.get_id());
        raft_msg.set_from_peer(follower);
        raft_msg.set_to_peer(leader);
        raft_msg.set_region_epoch(region.get_region_epoch().clone());
        raft_msg.set_message(m);
        cluster.send_raft_msg(raft_msg).unwrap();

        (ReadIndexResponse::default(), start_ts)
    };

    // wait a while until the node updates its own max ts
    let prev_cm_max_ts = cm.max_ts();
    let (resp, start_ts) = read_index(&[(b"l", b"yz")]);
    cluster.must_put(b"a", b"b");
    std::thread::sleep(Duration::from_millis(2000));
    // assert!(!resp.has_locked());
    // Actually not changed
    assert_ne!(cm.max_ts(), prev_cm_max_ts);
    assert_eq!(cm.max_ts(), start_ts);
    cluster.shutdown();
    fail::remove("on_pre_write_apply_state")
}
