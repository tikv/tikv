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

use std::sync::Arc;
use std::boxed::{Box, FnBox};
use std::net::SocketAddr;
use std::fmt::{self, Formatter, Display};
use std::collections::HashMap;

use super::Result;
use util::{self, TryInsertWith};
use util::worker::{Runnable, Worker};
use pd::PdClient;
use kvproto::metapb;
use super::metrics::*;

pub type Callback = Box<FnBox(Result<SocketAddr>) + Send>;

// StoreAddrResolver resolves the store address.
pub trait StoreAddrResolver {
    // Resolve resolves the store address asynchronously.
    fn resolve(&self, store_id: u64, cb: Callback) -> Result<()>;
}

/// Snapshot generating task.
struct Task {
    store_id: u64,
    cb: Callback,
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "resolve store {} address", self.store_id)
    }
}

pub struct Runner<T: PdClient> {
    pd_client: Arc<T>,
    store_addrs: HashMap<u64, String>,
}

impl<T: PdClient> Runner<T> {
    fn resolve(&mut self, store_id: u64) -> Result<SocketAddr> {
        let addr = try!(self.get_address(store_id));

        // If we use docker and use host for store address, the real IP
        // may be changed after service restarts, so here we just cache
        // pd result and use to_socket_addr to get real socket address.
        let sock = try!(util::to_socket_addr(addr));
        Ok(sock)
    }

    fn get_address(&mut self, store_id: u64) -> Result<&str> {
        // TODO: do we need re-update the cache sometimes?
        // Store address may be changed?
        let pd_client = self.pd_client.clone();
        let s = try!(self.store_addrs.entry(store_id).or_try_insert_with(|| {
            pd_client.get_store(store_id)
                .and_then(|s| {
                    if s.get_state() == metapb::StoreState::Tombstone {
                        RESOLVE_STORE_COUNTER.with_label_values(&["tombstone"]).inc();
                        return Err(box_err!("store {} has been removed", store_id));
                    }
                    let addr = s.get_address().to_owned();
                    // In some tests, we use empty address for store first,
                    // so we should ignore here.
                    // TODO: we may remove this check after we refactor the test.
                    if addr.len() == 0 {
                        return Err(box_err!("invalid empty address for store {}", store_id));
                    }
                    Ok(addr)
                })
        }));

        Ok(s)
    }
}

impl<T: PdClient> Runnable<Task> for Runner<T> {
    fn run(&mut self, task: Task) {
        let store_id = task.store_id;
        let resp = self.resolve(store_id);
        task.cb.call_box((resp,))
    }
}

pub struct PdStoreAddrResolver {
    worker: Worker<Task>,
}

impl PdStoreAddrResolver {
    pub fn new<T>(pd_client: Arc<T>) -> Result<PdStoreAddrResolver>
        where T: PdClient + 'static
    {
        let mut r = PdStoreAddrResolver { worker: Worker::new("store address resolve worker") };

        let runner = Runner {
            pd_client: pd_client,
            store_addrs: HashMap::new(),
        };
        box_try!(r.worker.start(runner));
        Ok(r)
    }
}

impl StoreAddrResolver for PdStoreAddrResolver {
    fn resolve(&self, store_id: u64, cb: Callback) -> Result<()> {
        let task = Task {
            store_id: store_id,
            cb: cb,
        };
        box_try!(self.worker.schedule(task));
        Ok(())
    }
}

impl Drop for PdStoreAddrResolver {
    fn drop(&mut self) {
        if let Some(Err(e)) = self.worker.stop().map(|h| h.join()) {
            error!("failed to stop store address resolve thread: {:?}!!!", e);
        }
    }
}

pub struct MockStoreAddrResolver;

impl StoreAddrResolver for MockStoreAddrResolver {
    fn resolve(&self, _: u64, _: Callback) -> Result<()> {
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::collections::HashMap;

    use kvproto::pdpb;
    use kvproto::metapb;
    use pd::{PdClient, Result};

    struct MockPdClient {
        store: metapb::Store,
    }

    impl PdClient for MockPdClient {
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
            Ok(self.store.clone())
        }
        fn get_cluster_config(&self) -> Result<metapb::Cluster> {
            unimplemented!();
        }
        fn get_region(&self, _: &[u8]) -> Result<metapb::Region> {
            unimplemented!();
        }
        fn get_region_by_id(&self, _: u64) -> Result<metapb::Region> {
            unimplemented!();
        }
        fn region_heartbeat(&self,
                            _: metapb::Region,
                            _: metapb::Peer,
                            _: Vec<pdpb::PeerStats>)
                            -> Result<pdpb::RegionHeartbeatResponse> {
            unimplemented!();
        }
        fn ask_split(&self, _: metapb::Region) -> Result<pdpb::AskSplitResponse> {
            unimplemented!();
        }
        fn store_heartbeat(&self, _: pdpb::StoreStats) -> Result<()> {
            unimplemented!();
        }
        fn report_split(&self, _: metapb::Region, _: metapb::Region) -> Result<()> {
            unimplemented!();
        }
    }

    fn new_runner(addr: &str, state: metapb::StoreState) -> Runner<MockPdClient> {
        let mut store = metapb::Store::new();
        store.set_state(state);
        store.set_address(addr.into());
        let client = MockPdClient { store: store };
        Runner {
            pd_client: Arc::new(client),
            store_addrs: HashMap::new(),
        }
    }

    #[test]
    fn test_resolve_store_state_up() {
        let addr = "127.0.0.1";
        let mut runner = new_runner(addr, metapb::StoreState::Up);
        assert_eq!(runner.get_address(0).unwrap(), addr);
    }

    #[test]
    fn test_resolve_store_state_offline() {
        let addr = "127.0.0.1";
        let mut runner = new_runner(addr, metapb::StoreState::Offline);
        assert_eq!(runner.get_address(0).unwrap(), addr);
    }

    #[test]
    fn test_resolve_store_state_tombstone() {
        let addr = "127.0.0.1";
        let mut runner = new_runner(addr, metapb::StoreState::Tombstone);
        assert!(runner.get_address(0).is_err());
    }
}
