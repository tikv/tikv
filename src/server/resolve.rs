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

use futures::Future;
use kvproto::metapb;

use pd::PdClient;

use super::metrics::*;

pub type ResolveResult = Box<Future<Item = String, Error = String> + Send>;

// StoreAddrResolver resolves the store address.
pub trait StoreAddrResolver: Send + Sync {
    // Resolve resolves the store address asynchronously.
    fn resolve(&self, store_id: u64) -> ResolveResult;
}

impl<P: PdClient> StoreAddrResolver for P {
    fn resolve(&self, store_id: u64) -> ResolveResult {
        RESOLVE_STORE_COUNTER.with_label_values(&["resolve"]).inc();
        debug!("begin to resolve store {} address", store_id);

        Box::new(
            self.get_store(store_id)
                .map_err(move |e| {
                    RESOLVE_STORE_COUNTER.with_label_values(&["failed"]).inc();
                    let s = format!("resolve store {} address failed {:?}", store_id, e);
                    error!("{}", s);
                    s
                })
                .and_then(move |s| {
                    if s.get_state() == metapb::StoreState::Tombstone {
                        RESOLVE_STORE_COUNTER
                            .with_label_values(&["tombstone"])
                            .inc();
                        let s = format!("store {} has been removed.", store_id);
                        debug!("{}", &s);
                        return Err(s);
                    }
                    let addr = s.get_address().to_owned();
                    // In some tests, we use empty address for store first,
                    // so we should ignore here.
                    // TODO: we may remove this check after we refactor the test.
                    if addr.is_empty() {
                        RESOLVE_STORE_COUNTER.with_label_values(&["failed"]).inc();
                        let s = format!("invalid empty address for store {}", store_id);
                        error!("{}", &s);
                        return Err(s);
                    }
                    RESOLVE_STORE_COUNTER.with_label_values(&["success"]).inc();
                    info!("resolve store {} address ok, addr {}", store_id, addr);
                    Ok(addr)
                }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::future;
    use kvproto::metapb;
    use pd::{LamePdClient, PdFuture};

    struct MockPdClient {
        store: metapb::Store,
    }

    impl LamePdClient for MockPdClient {
        fn get_store(&self, store_id: u64) -> PdFuture<metapb::Store> {
            if store_id != self.store.get_id() {
                return Box::new(future::err(box_err!("invalid store id")));
            }

            let store = self.store.clone();
            Box::new(future::ok(store))
        }
    }

    #[test]
    fn test_resolve() {
        let mut store = metapb::Store::new();
        store.set_id(1);
        store.set_state(metapb::StoreState::Up);
        store.set_address("127.0.0.1:12345".to_owned());

        let mut client = MockPdClient { store };

        // Should propagate error from pd client.
        client.resolve(0).wait().unwrap_err();
        client.resolve(1).wait().unwrap();

        client.store.set_state(metapb::StoreState::Offline);
        client.resolve(1).wait().unwrap();

        // Tombstone address should be rejected.
        client.store.set_state(metapb::StoreState::Tombstone);
        client.resolve(1).wait().unwrap_err();

        client.store.set_state(metapb::StoreState::Up);
        client.resolve(1).wait().unwrap();

        // Empty address is not allowed.
        client.store.clear_address();
        client.resolve(1).wait().unwrap_err();
    }
}
