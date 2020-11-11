// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use futures::future::BoxFuture;
use std::sync::{Arc, Mutex};
use tikv_util::time::Instant;

use engine_rocks::RocksEngine;
use kvproto::metapb;
use kvproto::replication_modepb::ReplicationMode;
use pd_client::{take_peer_address, PdClient};
use raftstore::router::RaftStoreRouter;
use raftstore::store::GlobalReplicationState;
use tikv_util::collections::HashMap;

use super::metrics::*;
use super::Result;

const STORE_ADDRESS_REFRESH_SECONDS: u64 = 60;

pub type Callback = Box<dyn FnOnce(Result<String>) + Send>;

/// A trait for resolving store addresses.
pub trait StoreAddrResolver: Send + Clone {
    /// Resolves the address for the specified store id asynchronously.
    fn resolve(&self, _store_id: u64) -> BoxFuture<'_, Result<String>>;
}

struct StoreAddr {
    addr: String,
    last_update: Instant,
}

/// A store address resolver which is backed by a `PDClient`.
pub struct PdStoreAddrResolver<T, RR: 'static>
where
    T: PdClient + 'static,
    RR: RaftStoreRouter<RocksEngine>,
{
    // ResolverInner impls Sync
    inner: ResolverInner<T>,

    // RaftStoreRouter may not impl Sync
    router: RR,
}

impl<T, RR> Clone for PdStoreAddrResolver<T, RR>
where
    T: PdClient + 'static,
    RR: RaftStoreRouter<RocksEngine>,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            router: self.router.clone(),
        }
    }
}

struct ResolverInner<T: PdClient + 'static> {
    pd_client: Arc<T>,
    store_addrs: Arc<futures::lock::Mutex<HashMap<u64, StoreAddr>>>,
    state: Arc<Mutex<GlobalReplicationState>>,
}

impl<T: PdClient + 'static> Clone for ResolverInner<T> {
    fn clone(&self) -> Self {
        Self {
            pd_client: self.pd_client.clone(),
            store_addrs: self.store_addrs.clone(),
            state: self.state.clone(),
        }
    }
}

impl<T: PdClient + 'static> ResolverInner<T> {
    async fn resolve<RR: RaftStoreRouter<RocksEngine>>(
        &self,
        store_id: u64,
        router: RR,
    ) -> Result<String> {
        if let Some(s) = self.store_addrs.lock().await.get(&store_id) {
            let now = Instant::now_coarse();
            let elapsed = now.duration_since(s.last_update);
            if elapsed.as_secs() < STORE_ADDRESS_REFRESH_SECONDS {
                return Ok(s.addr.clone());
            }
        }

        let addr = self.get_address(store_id, router).await?;

        let cache = StoreAddr {
            addr: addr.clone(),
            last_update: Instant::now_coarse(),
        };
        self.store_addrs.lock().await.insert(store_id, cache);

        Ok(addr)
    }

    async fn get_address<RR: RaftStoreRouter<RocksEngine>>(
        &self,
        store_id: u64,
        router: RR,
    ) -> Result<String> {
        let pd_client = Arc::clone(&self.pd_client);
        let mut s = box_try!(pd_client.get_store_async(store_id).await);
        let mut group_id = None;
        {
            // TODO: Avoid acquiring a sync mutex in an async function.
            let mut state = self.state.lock().unwrap();
            if state.status().get_mode() == ReplicationMode::DrAutoSync {
                let state_id = state.status().get_dr_auto_sync().state_id;
                if state.group.group_id(state_id, store_id).is_none() {
                    group_id = state.group.register_store(store_id, s.take_labels().into());
                }
            } else {
                state.group.backup_store_labels(&mut s);
            }
        }
        if let Some(group_id) = group_id {
            router.report_resolved(store_id, group_id);
        }
        if s.get_state() == metapb::StoreState::Tombstone {
            RESOLVE_STORE_COUNTER_STATIC.tombstone.inc();
            return Err(box_err!("store {} has been removed", store_id));
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

impl<T, RR: 'static> PdStoreAddrResolver<T, RR>
where
    T: PdClient + 'static,
    RR: RaftStoreRouter<RocksEngine>,
{
    pub fn new(pd_client: Arc<T>, router: RR, state: Arc<Mutex<GlobalReplicationState>>) -> Self {
        PdStoreAddrResolver {
            inner: ResolverInner {
                pd_client,
                store_addrs: Default::default(),
                state,
            },
            router,
        }
    }
}

impl<T, RR: 'static> StoreAddrResolver for PdStoreAddrResolver<T, RR>
where
    T: PdClient + 'static,
    RR: RaftStoreRouter<RocksEngine>,
{
    fn resolve(&self, store_id: u64) -> BoxFuture<'_, Result<String>> {
        let inner = &self.inner;
        let router = self.router.clone();
        Box::pin(async move { inner.resolve(store_id, router).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use std::net::SocketAddr;
    use std::ops::Sub;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicU16, Ordering};
    use std::sync::Arc;
    use std::thread;

    use kvproto::metapb;
    use pd_client::{PdClient, PdFuture, Result};
    use raftstore::router::RaftStoreBlackHole;
    use tikv_util::collections::HashMap;
    use tikv_util::time::{duration_to_ms, Duration, Instant};

    const STORE_ADDRESS_REFRESH_SECONDS: u64 = 60;

    struct MockPdClient {
        counter: AtomicU16,
        store: metapb::Store,
    }

    impl PdClient for MockPdClient {
        fn get_store(&self, _: u64) -> Result<metapb::Store> {
            // The store address will be changed every millisecond.
            let mut store = self.store.clone();
            let mut sock = SocketAddr::from_str(store.get_address()).unwrap();
            sock.set_port(self.counter.fetch_add(1, Ordering::Relaxed));
            store.set_address(format!("{}:{}", sock.ip(), sock.port()));
            Ok(store)
        }

        fn get_store_async(&self, store_id: u64) -> PdFuture<metapb::Store> {
            let res = self.get_store(store_id);
            Box::pin(async move { res })
        }
    }

    fn new_store(addr: &str, state: metapb::StoreState) -> metapb::Store {
        let mut store = metapb::Store::default();
        store.set_id(1);
        store.set_state(state);
        store.set_address(addr.into());
        store
    }

    fn new_runner(store: metapb::Store) -> ResolverInner<MockPdClient> {
        let client = MockPdClient {
            counter: AtomicU16::new(0),
            store,
        };

        ResolverInner {
            pd_client: Arc::new(client),
            store_addrs: Default::default(),
            state: Default::default(),
        }
    }

    const STORE_ADDR: &str = "127.0.0.1:12345";

    #[tokio::test]
    async fn test_resolve_store_state_up() {
        let store = new_store(STORE_ADDR, metapb::StoreState::Up);
        let runner = new_runner(store);
        assert!(runner.get_address(0, RaftStoreBlackHole).await.is_ok());
    }

    #[tokio::test]
    async fn test_resolve_store_state_offline() {
        let store = new_store(STORE_ADDR, metapb::StoreState::Offline);
        let runner = new_runner(store);
        assert!(runner.get_address(0, RaftStoreBlackHole).await.is_ok());
    }

    #[tokio::test]
    async fn test_resolve_store_state_tombstone() {
        let store = new_store(STORE_ADDR, metapb::StoreState::Tombstone);
        let runner = new_runner(store);
        assert!(runner.get_address(0, RaftStoreBlackHole).await.is_err());
    }

    #[tokio::test]
    async fn test_resolve_store_peer_addr() {
        let mut store = new_store("127.0.0.1:12345", metapb::StoreState::Up);
        store.set_peer_address("127.0.0.1:22345".to_string());
        let runner = new_runner(store);
        assert_eq!(
            runner.get_address(0, RaftStoreBlackHole).await.unwrap(),
            "127.0.0.1:22345".to_string()
        );
    }

    #[tokio::test]
    async fn test_store_address_refresh() {
        let store = new_store(STORE_ADDR, metapb::StoreState::Up);
        let store_id = store.get_id();
        let mut runner = new_runner(store);

        let interval = Duration::from_millis(2);

        let mut sock = runner.resolve(store_id, RaftStoreBlackHole).await.unwrap();

        tokio::time::delay_for(interval).await;
        // Expire the cache, and the address will be refreshed.
        {
            let mut map = runner.store_addrs.lock().await;
            let mut s = map.get_mut(&store_id).unwrap();
            let now = Instant::now_coarse();
            s.last_update = now.sub(Duration::from_secs(STORE_ADDRESS_REFRESH_SECONDS + 1));
        }
        let mut new_sock = runner.resolve(store_id, RaftStoreBlackHole).await.unwrap();
        assert_ne!(sock, new_sock);

        tokio::time::delay_for(interval).await;
        // Remove the cache, and the address will be refreshed.
        runner.store_addrs.lock().await.remove(&store_id);
        sock = new_sock;
        new_sock = runner.resolve(store_id, RaftStoreBlackHole).await.unwrap();
        assert_ne!(sock, new_sock);

        tokio::time::delay_for(interval).await;
        // Otherwise, the address will not be refreshed.
        sock = new_sock;
        new_sock = runner.resolve(store_id, RaftStoreBlackHole).await.unwrap();
        assert_eq!(sock, new_sock);
    }
}
