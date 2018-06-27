// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::boxed::FnBox;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::time::Instant;

use kvproto::metapb;

use pd::PdClient;
use util::collections::HashMap;
use util::worker::{Runnable, Scheduler, Worker};

use super::metrics::*;
use super::Result;

const STORE_ADDRESS_REFRESH_SECONDS: u64 = 60;

pub type Callback = Box<FnBox(Result<String>) + Send>;

// StoreAddrResolver resolves the store address.
pub trait StoreAddrResolver: Send + Clone {
    // Resolve resolves the store address asynchronously.
    fn resolve(&self, store_id: u64, cb: Callback) -> Result<()>;
}

/// Snapshot generating task.
pub struct Task {
    store_id: u64,
    cb: Callback,
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "resolve store {} address", self.store_id)
    }
}

struct StoreAddr {
    addr: String,
    last_update: Instant,
}

pub struct Runner<T: PdClient> {
    pd_client: Arc<T>,
    store_addrs: HashMap<u64, StoreAddr>,
}

impl<T: PdClient> Runner<T> {
    fn resolve(&mut self, store_id: u64) -> Result<String> {
        if let Some(s) = self.store_addrs.get(&store_id) {
            let now = Instant::now();
            let elapsed = now.duration_since(s.last_update);
            if elapsed.as_secs() < STORE_ADDRESS_REFRESH_SECONDS {
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

    fn get_address(&mut self, store_id: u64) -> Result<String> {
        let pd_client = Arc::clone(&self.pd_client);
        let s = box_try!(pd_client.get_store(store_id));
        if s.get_state() == metapb::StoreState::Tombstone {
            RESOLVE_STORE_COUNTER
                .with_label_values(&["tombstone"])
                .inc();
            return Err(box_err!("store {} has been removed", store_id));
        }
        let addr = s.get_address().to_owned();
        // In some tests, we use empty address for store first,
        // so we should ignore here.
        // TODO: we may remove this check after we refactor the test.
        if addr.is_empty() {
            return Err(box_err!("invalid empty address for store {}", store_id));
        }
        Ok(addr)
    }
}

impl<T: PdClient> Runnable<Task> for Runner<T> {
    fn run(&mut self, task: Task) {
        let store_id = task.store_id;
        let resp = self.resolve(store_id);
        task.cb.call_box((resp,))
    }
}

#[derive(Clone)]
pub struct PdStoreAddrResolver {
    sched: Scheduler<Task>,
}

impl PdStoreAddrResolver {
    pub fn new(sched: Scheduler<Task>) -> PdStoreAddrResolver {
        PdStoreAddrResolver { sched }
    }
}

pub fn new_resolver<T>(pd_client: Arc<T>) -> Result<(Worker<Task>, PdStoreAddrResolver)>
where
    T: PdClient + 'static,
{
    let mut worker = Worker::new("store address resolve worker");

    let runner = Runner {
        pd_client,
        store_addrs: HashMap::default(),
    };
    box_try!(worker.start(runner));
    let resolver = PdStoreAddrResolver {
        sched: worker.scheduler(),
    };
    Ok((worker, resolver))
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
    use std::net::SocketAddr;
    use std::ops::Sub;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    use kvproto::metapb;
    use kvproto::pdpb;
    use pd::{PdClient, PdFuture, RegionStat, Result};
    use util;
    use util::collections::HashMap;

    const STORE_ADDRESS_REFRESH_SECONDS: u64 = 60;

    struct MockPdClient {
        start: Instant,
        store: metapb::Store,
    }

    impl PdClient for MockPdClient {
        fn get_cluster_id(&self) -> Result<u64> {
            unimplemented!();
        }
        fn bootstrap_cluster(&self, _: metapb::Store, _: metapb::Region) -> Result<()> {
            unimplemented!();
        }
        fn is_cluster_bootstrapped(&self) -> Result<bool> {
            unimplemented!();
        }
        fn alloc_id(&self) -> Result<u64> {
            unimplemented!();
        }
        fn put_store(&self, _: metapb::Store) -> Result<()> {
            unimplemented!();
        }
        fn get_store(&self, _: u64) -> Result<metapb::Store> {
            // The store address will be changed every millisecond.
            let mut store = self.store.clone();
            let mut sock = SocketAddr::from_str(store.get_address()).unwrap();
            sock.set_port(util::time::duration_to_ms(self.start.elapsed()) as u16);
            store.set_address(format!("{}:{}", sock.ip(), sock.port()));
            Ok(store)
        }
        fn get_cluster_config(&self) -> Result<metapb::Cluster> {
            unimplemented!();
        }
        fn get_region(&self, _: &[u8]) -> Result<metapb::Region> {
            unimplemented!();
        }
        fn get_region_by_id(&self, _: u64) -> PdFuture<Option<metapb::Region>> {
            unimplemented!();
        }
        fn region_heartbeat(
            &self,
            _: metapb::Region,
            _: metapb::Peer,
            _: RegionStat,
        ) -> PdFuture<()> {
            unimplemented!();
        }

        fn handle_region_heartbeat_response<F>(&self, _: u64, _: F) -> PdFuture<()>
        where
            F: Fn(pdpb::RegionHeartbeatResponse) + Send + 'static,
        {
            unimplemented!()
        }

        fn ask_split(&self, _: metapb::Region) -> PdFuture<pdpb::AskSplitResponse> {
            unimplemented!();
        }
        fn store_heartbeat(&self, _: pdpb::StoreStats) -> PdFuture<()> {
            unimplemented!();
        }
        fn report_split(&self, _: metapb::Region, _: metapb::Region) -> PdFuture<()> {
            unimplemented!();
        }
    }

    fn new_store(addr: &str, state: metapb::StoreState) -> metapb::Store {
        let mut store = metapb::Store::new();
        store.set_id(1);
        store.set_state(state);
        store.set_address(addr.into());
        store
    }

    fn new_runner(store: metapb::Store) -> Runner<MockPdClient> {
        let client = MockPdClient {
            start: Instant::now(),
            store,
        };
        Runner {
            pd_client: Arc::new(client),
            store_addrs: HashMap::default(),
        }
    }

    const STORE_ADDR: &str = "127.0.0.1:12345";

    #[test]
    fn test_resolve_store_state_up() {
        let store = new_store(STORE_ADDR, metapb::StoreState::Up);
        let mut runner = new_runner(store);
        assert!(runner.get_address(0).is_ok());
    }

    #[test]
    fn test_resolve_store_state_offline() {
        let store = new_store(STORE_ADDR, metapb::StoreState::Offline);
        let mut runner = new_runner(store);
        assert!(runner.get_address(0).is_ok());
    }

    #[test]
    fn test_resolve_store_state_tombstone() {
        let store = new_store(STORE_ADDR, metapb::StoreState::Tombstone);
        let mut runner = new_runner(store);
        assert!(runner.get_address(0).is_err());
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
