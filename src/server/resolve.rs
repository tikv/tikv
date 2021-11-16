// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Display, Formatter};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use collections::HashMap;
use engine_traits::KvEngine;
use kvproto::replication_modepb::ReplicationMode;
use pd_client::{take_peer_address, PdClient};
use raftstore::router::RaftStoreRouter;
use raftstore::store::GlobalReplicationState;
use tikv_util::time::Instant;
use tikv_util::worker::{Runnable, Scheduler, Worker};

use super::metrics::*;
use super::Result;

const STORE_ADDRESS_REFRESH_SECONDS: u64 = 60;

pub type Callback = Box<dyn FnOnce(Result<String>) + Send>;

pub fn store_address_refresh_interval_secs() -> u64 {
    fail_point!("mock_store_refresh_interval_secs", |arg| arg
        .map_or(0, |e| e.parse().unwrap()));
    STORE_ADDRESS_REFRESH_SECONDS
}

/// A trait for resolving store addresses.
pub trait StoreAddrResolver: Send + Clone {
    /// Resolves the address for the specified store id asynchronously.
    fn resolve(&self, store_id: u64, cb: Callback) -> Result<()>;
}

/// A task for resolving store addresses.
pub struct Task {
    store_id: u64,
    cb: Callback,
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "resolve store {} address", self.store_id)
    }
}

struct StoreAddr {
    addr: String,
    last_update: Instant,
}

/// A runner for resolving store addresses.
struct Runner<T, RR, E>
where
    T: PdClient,
    RR: RaftStoreRouter<E>,
    E: KvEngine,
{
    pd_client: Arc<T>,
    store_addrs: HashMap<u64, StoreAddr>,
    state: Arc<Mutex<GlobalReplicationState>>,
    router: RR,
    engine: PhantomData<E>,
}

impl<T, RR, E> Runner<T, RR, E>
where
    T: PdClient,
    RR: RaftStoreRouter<E>,
    E: KvEngine,
{
    fn resolve(&mut self, store_id: u64) -> Result<String> {
        if let Some(s) = self.store_addrs.get(&store_id) {
            let now = Instant::now();
            let elapsed = now.saturating_duration_since(s.last_update);
            if elapsed.as_secs() < store_address_refresh_interval_secs() {
                return Ok(s.addr.clone());
            }
        }

        let addr = self.get_address(store_id)?;

        let cache = StoreAddr {
            addr: addr.clone(),
            last_update: Instant::now(),
        };
        self.store_addrs.insert(store_id, cache);

        Ok(addr)
    }

    fn get_address(&self, store_id: u64) -> Result<String> {
        let pd_client = Arc::clone(&self.pd_client);
        let mut s = match pd_client.get_store(store_id) {
            Ok(s) => s,
            // `get_store` will filter tombstone store, so here needs to handle
            // it explicitly.
            Err(pd_client::Error::StoreTombstone(_)) => {
                RESOLVE_STORE_COUNTER_STATIC.tombstone.inc();
                return Err(box_err!("store {} has been removed", store_id));
            }
            Err(e) => return Err(box_err!(e)),
        };
        let mut group_id = None;
        let mut state = self.state.lock().unwrap();
        if state.status().get_mode() == ReplicationMode::DrAutoSync {
            let state_id = state.status().get_dr_auto_sync().state_id;
            if state.group.group_id(state_id, store_id).is_none() {
                group_id = state.group.register_store(store_id, s.take_labels().into());
            }
        } else {
            state.group.backup_store_labels(&mut s);
        }
        drop(state);
        if let Some(group_id) = group_id {
            self.router.report_resolved(store_id, group_id);
        }
        let addr = take_peer_address(&mut s);
        // In some tests, we use empty address for store first,
        // so we should ignore here.
        // TODO: we may remove this check after we refactor the test.
        if addr.is_empty() {
            return Err(box_err!("invalid empty address for store {}", store_id));
        }
        Ok(addr)
    }
}

impl<T, RR, E> Runnable for Runner<T, RR, E>
where
    T: PdClient,
    RR: RaftStoreRouter<E>,
    E: KvEngine,
{
    type Task = Task;
    fn run(&mut self, task: Task) {
        let start = Instant::now();
        let store_id = task.store_id;
        let resp = self.resolve(store_id);
        (task.cb)(resp);
        ADDRESS_RESOLVE_HISTOGRAM.observe(start.saturating_elapsed_secs());
    }
}

/// A store address resolver which is backed by a `PDClient`.
#[derive(Clone)]
pub struct PdStoreAddrResolver {
    sched: Scheduler<Task>,
}

impl PdStoreAddrResolver {
    pub fn new(sched: Scheduler<Task>) -> PdStoreAddrResolver {
        PdStoreAddrResolver { sched }
    }
}

/// Creates a new `PdStoreAddrResolver`.
pub fn new_resolver<T, RR: 'static, E>(
    pd_client: Arc<T>,
    worker: &Worker,
    router: RR,
) -> (PdStoreAddrResolver, Arc<Mutex<GlobalReplicationState>>)
where
    T: PdClient + 'static,
    RR: RaftStoreRouter<E>,
    E: KvEngine,
{
    let state = Arc::new(Mutex::new(GlobalReplicationState::default()));
    let runner = Runner {
        pd_client,
        store_addrs: HashMap::default(),
        state: state.clone(),
        router,
        engine: PhantomData,
    };
    let scheduler = worker.start("addr-resolver", runner);
    let resolver = PdStoreAddrResolver::new(scheduler);
    (resolver, state)
}

impl StoreAddrResolver for PdStoreAddrResolver {
    fn resolve(&self, store_id: u64, cb: Callback) -> Result<()> {
        let task = Task { store_id, cb };
        box_try!(self.sched.schedule(task));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::marker::PhantomData;
    use std::net::SocketAddr;
    use std::ops::Sub;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use collections::HashMap;
    use engine_test::kv::KvTestEngine;
    use kvproto::metapb;
    use pd_client::{PdClient, Result};
    use raftstore::router::RaftStoreBlackHole;

    const STORE_ADDRESS_REFRESH_SECONDS: u64 = 60;

    struct MockPdClient {
        start: Instant,
        store: metapb::Store,
    }

    impl PdClient for MockPdClient {
        fn get_store(&self, _: u64) -> Result<metapb::Store> {
            if self.store.get_state() == metapb::StoreState::Tombstone {
                // Simulate the behavior of `get_store` in pd client.
                return Err(pd_client::Error::StoreTombstone(format!(
                    "{:?}",
                    self.store
                )));
            }
            // The store address will be changed every millisecond.
            let mut store = self.store.clone();
            let mut sock = SocketAddr::from_str(store.get_address()).unwrap();
            sock.set_port(tikv_util::time::duration_to_ms(self.start.saturating_elapsed()) as u16);
            store.set_address(format!("{}:{}", sock.ip(), sock.port()));
            Ok(store)
        }
    }

    fn new_store(addr: &str, state: metapb::StoreState) -> metapb::Store {
        let mut store = metapb::Store::default();
        store.set_id(1);
        store.set_state(state);
        store.set_address(addr.into());
        store
    }

    fn new_runner(store: metapb::Store) -> Runner<MockPdClient, RaftStoreBlackHole, KvTestEngine> {
        let client = MockPdClient {
            start: Instant::now(),
            store,
        };
        Runner {
            pd_client: Arc::new(client),
            store_addrs: HashMap::default(),
            state: Default::default(),
            router: RaftStoreBlackHole,
            engine: PhantomData,
        }
    }

    const STORE_ADDR: &str = "127.0.0.1:12345";

    #[test]
    fn test_resolve_store_state_up() {
        let store = new_store(STORE_ADDR, metapb::StoreState::Up);
        let runner = new_runner(store);
        assert!(runner.get_address(0).is_ok());
    }

    #[test]
    fn test_resolve_store_state_offline() {
        let store = new_store(STORE_ADDR, metapb::StoreState::Offline);
        let runner = new_runner(store);
        assert!(runner.get_address(0).is_ok());
    }

    #[test]
    fn test_resolve_store_state_tombstone() {
        let store = new_store(STORE_ADDR, metapb::StoreState::Tombstone);
        let runner = new_runner(store);
        assert!(runner.get_address(0).is_err());
    }

    #[test]
    fn test_resolve_store_peer_addr() {
        let mut store = new_store("127.0.0.1:12345", metapb::StoreState::Up);
        store.set_peer_address("127.0.0.1:22345".to_string());
        let runner = new_runner(store);
        assert_eq!(
            runner.get_address(0).unwrap(),
            "127.0.0.1:22345".to_string()
        );
    }

    #[test]
    fn test_store_address_refresh() {
        let store = new_store(STORE_ADDR, metapb::StoreState::Up);
        let store_id = store.get_id();
        let mut runner = new_runner(store);

        let interval = Duration::from_millis(2);

        let mut sock = runner.resolve(store_id).unwrap();

        thread::sleep(interval);
        // Expire the cache, and the address will be refreshed.
        {
            let s = runner.store_addrs.get_mut(&store_id).unwrap();
            let now = Instant::now();
            s.last_update = now.sub(Duration::from_secs(STORE_ADDRESS_REFRESH_SECONDS + 1));
        }
        let mut new_sock = runner.resolve(store_id).unwrap();
        assert_ne!(sock, new_sock);

        thread::sleep(interval);
        // Remove the cache, and the address will be refreshed.
        runner.store_addrs.remove(&store_id);
        sock = new_sock;
        new_sock = runner.resolve(store_id).unwrap();
        assert_ne!(sock, new_sock);

        thread::sleep(interval);
        // Otherwise, the address will not be refreshed.
        sock = new_sock;
        new_sock = runner.resolve(store_id).unwrap();
        assert_eq!(sock, new_sock);
    }
}
