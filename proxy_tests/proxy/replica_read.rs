use std::{collections::hash_map::Entry, pin::Pin, sync::Mutex, time::Duration};

use engine_store_ffi::ffi::{
    ffi_gc_rust_ptr, ffi_make_async_waker, ffi_make_read_index_task, ffi_make_timer_task,
    ffi_poll_read_index_task, ffi_poll_timer_task,
    interfaces_ffi::{RaftStoreProxyFFIHelper, RawRustPtr, RawVoidPtr},
    ProtoMsgBaseBuff,
};

use crate::proxy::*;

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
                error!("GcMonitor::valid_clean failed at type {} refcount {}", k, v);
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
        let notifier = new_mock_engine_store::ProxyNotifier::new_raw();
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

    fn get_notifier(&self) -> &new_mock_engine_store::ProxyNotifier {
        unsafe { &*(self.notifier as *mut new_mock_engine_store::ProxyNotifier) }
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
    let notifier = unsafe { &mut *(data as *mut new_mock_engine_store::ProxyNotifier) };
    notifier.wake()
}

pub fn configure_for_lease_read(
    cluster: &mut Cluster<NodeCluster>,
    base_tick_ms: Option<u64>,
    election_ticks: Option<usize>,
) -> Duration {
    if let Some(base_tick_ms) = base_tick_ms {
        cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(base_tick_ms);
    }
    let base_tick_interval = cluster.cfg.raft_store.raft_base_tick_interval.0;
    if let Some(election_ticks) = election_ticks {
        cluster.cfg.raft_store.raft_election_timeout_ticks = election_ticks;
    }
    let election_ticks = cluster.cfg.raft_store.raft_election_timeout_ticks as u32;
    let election_timeout = base_tick_interval * election_ticks;
    // Adjust max leader lease.
    cluster.cfg.raft_store.raft_store_max_leader_lease =
        ReadableDuration(election_timeout - base_tick_interval);
    // Use large peer check interval, abnormal and max leader missing duration to
    // make a valid config, that is election timeout x 2 < peer stale state
    // check < abnormal < max leader missing duration.
    cluster.cfg.raft_store.peer_stale_state_check_interval = ReadableDuration(election_timeout * 3);
    cluster.cfg.raft_store.abnormal_leader_missing_duration =
        ReadableDuration(election_timeout * 4);
    cluster.cfg.raft_store.max_leader_missing_duration = ReadableDuration(election_timeout * 5);

    election_timeout
}

#[test]
fn test_read_index() {
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
            &mut |_, _, ffi_helper: &mut FFIHelperSet| {
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

#[test]
fn test_util() {
    // test timer
    new_mock_engine_store::mock_cluster::init_global_ffi_helper_set();
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
