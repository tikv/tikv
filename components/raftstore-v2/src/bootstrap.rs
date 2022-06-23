// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{thread, time::Duration};

use engine_traits::{RaftEngine, RaftLogBatch};
use error_code::ErrorCodeExt;
use fail::fail_point;
use kvproto::{
    metapb::{Region, Store},
    raft_serverpb::{RaftLocalState, RegionLocalState, StoreIdent},
};
use pd_client::PdClient;
use raft::INVALID_ID;
use raftstore::store::initial_region;
use slog::{debug, error, info, warn, Logger};
use tikv_util::{box_err, box_try};

use crate::{raft::write_initial_states, Result};

const MAX_CHECK_CLUSTER_BOOTSTRAPPED_RETRY_COUNT: u64 = 60;
const CHECK_CLUSTER_BOOTSTRAPPED_RETRY_INTERVAL: Duration = Duration::from_secs(3);

/// A struct for bootstrapping the store.
///
/// A typical bootstrap process should follow following order:
/// 1. bootstrap the store to get a store ID.
/// 2. bootstrap the first region using the last store ID.
pub struct Bootstrap<'a, ER: RaftEngine> {
    engine: &'a ER,
    cluster_id: u64,
    // It's not performance critical.
    pd_client: &'a dyn PdClient,
    logger: Logger,
}

// Although all methods won't change internal state, but they still receive `&mut self` as it's
// not thread safe to bootstrap concurrently.
impl<'a, ER: RaftEngine> Bootstrap<'a, ER> {
    pub fn new(
        engine: &'a ER,
        cluster_id: u64,
        pd_client: &'a impl PdClient,
        logger: Logger,
    ) -> Self {
        Self {
            engine,
            cluster_id,
            pd_client,
            logger,
        }
    }

    /// check store, return store id for the engine.
    /// If the store is not bootstrapped, use None.
    fn check_store(&mut self) -> Result<Option<u64>> {
        let ident = match self.engine.get_store_ident()? {
            Some(ident) => ident,
            None => return Ok(None),
        };
        if ident.get_cluster_id() != self.cluster_id {
            return Err(box_err!(
                "cluster ID mismatch, local {} != remote {}, \
                    you are trying to connect to another cluster, please reconnect to the correct PD",
                ident.get_cluster_id(),
                self.cluster_id
            ));
        }
        if ident.get_store_id() == INVALID_ID {
            return Err(box_err!("invalid store ident {:?}", ident));
        }
        Ok(Some(ident.get_store_id()))
    }

    fn inner_bootstrap_store(&mut self) -> Result<u64> {
        let id = self.pd_client.alloc_id()?;
        debug!(self.logger, "alloc store id"; "store_id" => id);
        let mut ident = StoreIdent::default();
        if !self.engine.is_empty()? {
            return Err(box_err!("store is not empty and has already had data."));
        }
        ident.set_cluster_id(self.cluster_id);
        ident.set_store_id(id);
        self.engine.put_store_ident(&ident)?;
        self.engine.sync()?;
        fail_point!("node_after_bootstrap_store", |_| Err(box_err!(
            "injected error: node_after_bootstrap_store"
        )));
        Ok(id)
    }

    /// Bootstrap the store and return the store ID.
    ///
    /// If store is bootstrapped already, return the store ID directly.
    pub fn bootstrap_store(&mut self) -> Result<u64> {
        let store_id = match self.check_store()? {
            Some(id) => id,
            None => self.inner_bootstrap_store()?,
        };

        Ok(store_id)
    }

    fn prepare_bootstrap_first_region(&mut self, store_id: u64) -> Result<Region> {
        let region_id = self.pd_client.alloc_id()?;
        debug!(
            self.logger,
            "alloc first region id";
            "region_id" => region_id,
            "cluster_id" => self.cluster_id,
            "store_id" => store_id
        );
        let peer_id = self.pd_client.alloc_id()?;
        debug!(
            self.logger,
            "alloc first peer id for first region";
            "peer_id" => peer_id,
            "region_id" => region_id,
        );

        let region = initial_region(store_id, region_id, peer_id);

        let mut wb = self.engine.log_batch(10);
        wb.put_prepare_bootstrap_region(&region)?;
        write_initial_states(&mut wb, region.clone())?;
        box_try!(self.engine.consume(&mut wb, true));

        Ok(region)
    }

    fn check_first_region_bootstrapped(&mut self) -> Result<bool> {
        for _ in 0..MAX_CHECK_CLUSTER_BOOTSTRAPPED_RETRY_COUNT {
            match self.pd_client.is_cluster_bootstrapped() {
                Ok(b) => return Ok(b),
                Err(e) => {
                    warn!(self.logger, "check cluster bootstrapped failed"; "err" => ?e);
                }
            }
            thread::sleep(CHECK_CLUSTER_BOOTSTRAPPED_RETRY_INTERVAL);
        }
        Err(box_err!("check cluster bootstrapped failed"))
    }

    fn check_or_prepare_bootstrap_first_region(&mut self, store_id: u64) -> Result<Option<Region>> {
        if let Some(first_region) = self.engine.get_prepare_bootstrap_region()? {
            // Bootstrap is aborted last time, resume. It may succeed or fail last time, no matter
            // what, at least we need a way to clean up.
            Ok(Some(first_region))
        } else if self.check_first_region_bootstrapped()? {
            // If other node has bootstrap the cluster, skip to avoid useless ID allocating and
            // disk writes.
            Ok(None)
        } else {
            // We are probably the first one triggering bootstrap.
            self.prepare_bootstrap_first_region(store_id).map(Some)
        }
    }

    fn clear_prepare_bootstrap(&mut self, first_region_id: Option<u64>) -> Result<()> {
        let mut wb = self.engine.log_batch(10);
        wb.remove_prepare_bootstrap_region()?;
        if let Some(id) = first_region_id {
            box_try!(
                self.engine
                    .clean(id, 0, &RaftLocalState::default(), &mut wb)
            );
        }
        box_try!(self.engine.consume(&mut wb, true));
        Ok(())
    }

    fn inner_bootstrap_first_region(
        &mut self,
        store: &Store,
        first_region: &Region,
    ) -> Result<bool> {
        let region_id = first_region.get_id();
        let mut retry = 0;
        while retry < MAX_CHECK_CLUSTER_BOOTSTRAPPED_RETRY_COUNT {
            match self
                .pd_client
                .bootstrap_cluster(store.clone(), first_region.clone())
            {
                Ok(_) => {
                    info!(self.logger, "bootstrap cluster ok"; "cluster_id" => self.cluster_id);
                    fail_point!("node_after_bootstrap_cluster", |_| Err(box_err!(
                        "injected error: node_after_bootstrap_cluster"
                    )));
                    self.clear_prepare_bootstrap(None)?;
                    return Ok(true);
                }
                Err(pd_client::Error::ClusterBootstrapped(_)) => {
                    match self.pd_client.get_region(b"") {
                        Ok(region) => {
                            if region == *first_region {
                                self.clear_prepare_bootstrap(None)?;
                                return Ok(true);
                            } else {
                                info!(self.logger, "cluster is already bootstrapped"; "cluster_id" => self.cluster_id);
                                self.clear_prepare_bootstrap(Some(region_id))?;
                                return Ok(false);
                            }
                        }
                        Err(e) => {
                            warn!(self.logger, "get the first region failed"; "err" => ?e);
                        }
                    }
                }
                Err(e) => {
                    error!(self.logger, "bootstrap cluster"; "cluster_id" => self.cluster_id, "err" => ?e, "err_code" => %e.error_code())
                }
            }
            retry += 1;
            thread::sleep(CHECK_CLUSTER_BOOTSTRAPPED_RETRY_INTERVAL);
        }
        Err(box_err!("bootstrapped cluster failed"))
    }

    /// Bootstrap the first region.
    ///
    /// If the cluster is already bootstrapped, `None` is returned.
    pub fn bootstrap_first_region(
        &mut self,
        store: &Store,
        store_id: u64,
    ) -> Result<Option<Region>> {
        let first_region = match self.check_or_prepare_bootstrap_first_region(store_id)? {
            Some(r) => r,
            None => return Ok(None),
        };
        info!(self.logger, "trying to bootstrap first region"; "store_id" => store_id, "region" => ?first_region);
        // cluster is not bootstrapped, and we choose first store to bootstrap
        fail_point!("node_after_prepare_bootstrap_cluster", |_| Err(box_err!(
            "injected error: node_after_prepare_bootstrap_cluster"
        )));
        if self.inner_bootstrap_first_region(store, &first_region)? {
            Ok(Some(first_region))
        } else {
            Ok(None)
        }
    }
}
