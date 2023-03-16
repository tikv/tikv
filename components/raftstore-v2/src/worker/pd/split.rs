// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, RaftEngine};
use kvproto::{
    metapb, pdpb,
    raft_cmdpb::{AdminCmdType, AdminRequest, SplitRequest},
};
use pd_client::PdClient;
use raftstore::store::SplitInfo;
use slog::{info, warn, Logger};
use yatp::{task::future::TaskCell, Remote};

use super::{requests::*, Runner};
use crate::{batch::StoreRouter, router::CmdResChannel};

fn new_batch_split_region_request(
    split_keys: Vec<Vec<u8>>,
    ids: Vec<pdpb::SplitId>,
    right_derive: bool,
) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::BatchSplit);
    req.mut_splits().set_right_derive(right_derive);
    let mut requests = Vec::with_capacity(ids.len());
    for (mut id, key) in ids.into_iter().zip(split_keys) {
        let mut split = SplitRequest::default();
        split.set_split_key(key);
        split.set_new_region_id(id.get_new_region_id());
        split.set_new_peer_ids(id.take_new_peer_ids());
        requests.push(split);
    }
    req.mut_splits().set_requests(requests.into());
    req
}

impl<EK, ER, T> Runner<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: PdClient + 'static,
{
    #[inline]
    pub fn handle_ask_batch_split(
        &mut self,
        region: metapb::Region,
        split_keys: Vec<Vec<u8>>,
        peer: metapb::Peer,
        right_derive: bool,
        ch: CmdResChannel,
    ) {
        Self::ask_batch_split_imp(
            &self.pd_client,
            &self.logger,
            &self.router,
            &self.remote,
            region,
            split_keys,
            peer,
            right_derive,
            Some(ch),
        );
    }

    fn ask_batch_split_imp(
        pd_client: &T,
        logger: &Logger,
        router: &StoreRouter<EK, ER>,
        remote: &Remote<TaskCell>,
        mut region: metapb::Region,
        split_keys: Vec<Vec<u8>>,
        peer: metapb::Peer,
        right_derive: bool,
        ch: Option<CmdResChannel>,
    ) {
        if split_keys.is_empty() {
            info!(
                logger,
                "empty split key, skip ask batch split";
                "region_id" => region.get_id()
            );
            return;
        }
        let resp = pd_client.ask_batch_split(region.clone(), split_keys.len());
        let router = router.clone();
        let logger = logger.clone();
        let f = async move {
            match resp.await {
                Ok(mut resp) => {
                    info!(
                        logger,
                        "try to batch split region";
                        "region_id" => region.get_id(),
                        "new_region_ids" => ?resp.get_ids(),
                        "region" => ?region,
                    );

                    let req = new_batch_split_region_request(
                        split_keys,
                        resp.take_ids().into(),
                        right_derive,
                    );
                    let region_id = region.get_id();
                    let epoch = region.take_region_epoch();
                    send_admin_request(&logger, &router, region_id, epoch, peer, req, ch);
                }
                Err(e) => {
                    warn!(
                        logger,
                        "ask batch split failed";
                        "region_id" => region.get_id(),
                        "err" => ?e,
                    );
                }
            }
        };
        remote.spawn(f);
    }

    pub fn handle_report_batch_split(&mut self, regions: Vec<metapb::Region>) {
        let resp = self.pd_client.report_batch_split(regions);
        let logger = self.logger.clone();
        let f = async move {
            if let Err(e) = resp.await {
                warn!(logger, "report split failed"; "err" => ?e);
            }
        };
        self.remote.spawn(f);
    }

    pub fn handle_auto_split(&mut self, split_infos: Vec<SplitInfo>) {
        let pd_client = self.pd_client.clone();
        let logger = self.logger.clone();
        let router = self.router.clone();
        let remote = self.remote.clone();

        let f = async move {
            for split_info in split_infos {
                let Ok(Some(region)) =
                    pd_client.get_region_by_id(split_info.region_id).await else { continue };
                // Try to split the region with the given split key.
                if let Some(split_key) = split_info.split_key {
                    Self::ask_batch_split_imp(
                        &pd_client,
                        &logger,
                        &router,
                        &remote,
                        region,
                        vec![split_key],
                        split_info.peer,
                        true,
                        None,
                    );
                // Try to split the region on half within the given key
                // range if there is no `split_key` been given.
                } else if split_info.start_key.is_some() && split_info.end_key.is_some() {
                    // TODO: implement half split
                }
            }
        };
        self.remote.spawn(f);
    }
}
