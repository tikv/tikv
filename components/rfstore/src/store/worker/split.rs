// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use bytes::Bytes;
use kvproto::metapb;
use kvproto::raft_cmdpb::RaftCmdRequest;
use protobuf::ProtobufEnum;
use std::fmt::{self, Display, Formatter};
use std::thread::sleep;
use yatp::Remote;

use crate::store::cmd_resp::new_error;
use crate::store::{
    Callback, CustomBuilder, MsgWaitFollowerSplitFiles, PdTask, PeerMsg, RegionIDVer,
};
use crate::{RaftRouter, RaftStoreRouter};
use tikv_util::time::Duration;
use tikv_util::worker::{Runnable, Scheduler};
use tikv_util::{box_err, debug, error, info, warn};
use txn_types::Key;

#[derive(Debug)]
pub struct SplitTask {
    region: metapb::Region,
    peer: metapb::Peer,
    callback: Callback,
    pub method: SplitMethod,
}

#[derive(Debug)]
pub enum SplitMethod {
    MaxSize(u64),
    Keys(Vec<Vec<u8>>),
    SplitFiles,
    Finish,
}

impl Display for SplitTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[split worker] Split Task for {}, method: {:?}",
            RegionIDVer::from_region(&self.region),
            &self.method,
        )
    }
}

impl SplitTask {
    pub fn new(
        region: metapb::Region,
        peer: metapb::Peer,
        callback: Callback,
        method: SplitMethod,
    ) -> SplitTask {
        Self {
            region,
            peer,
            callback,
            method,
        }
    }
}

pub struct SplitRunner {
    kv: kvengine::Engine,
    router: RaftRouter,
    pd_scheduler: Scheduler<PdTask>,
    split_scheduler: Scheduler<SplitTask>,
    remote: Remote<yatp::task::future::TaskCell>,
}

impl SplitRunner {
    pub fn new(
        kv: kvengine::Engine,
        router: RaftRouter,
        split_scheduler: Scheduler<SplitTask>,
        pd_scheduler: Scheduler<PdTask>,
        remote: Remote<yatp::task::future::TaskCell>,
    ) -> Self {
        Self {
            kv,
            router,
            pd_scheduler,
            split_scheduler,
            remote,
        }
    }

    fn pre_split(
        &self,
        region: metapb::Region,
        peer: metapb::Peer,
        keys: Vec<Bytes>,
        callback: Callback,
    ) {
        let mut shard_opt;
        let tag = RegionIDVer::from_region(&region);
        loop {
            shard_opt = self.kv.get_shard(region.get_id());
            if shard_opt.is_none() {
                let err = box_err!("shard {} not found maybe removed", tag);
                error!("{:?}", &err);
                callback.invoke_with_response(new_error(err));
                return;
            }
            if shard_opt.as_ref().unwrap().get_initial_flushed() {
                break;
            }
            sleep(Duration::from_secs(1));
            info!("wait for initial flush"; "region" => tag);
        }
        let shard = shard_opt.unwrap();
        if shard.get_split_stage() != kvenginepb::SplitStage::Initial {
            warn!("wrong split stage"; "region" => tag, "stage" => shard.get_split_stage().value());
            callback.invoke_with_response(new_error(box_err!(
                "wrong split stage {}",
                shard.get_split_stage().value()
            )));
            return;
        }
        let mut request = new_request(&region, &peer);
        let mut change_set = kvenginepb::ChangeSet::default();
        change_set.set_shard_id(region.get_id());
        change_set.set_shard_ver(region.get_region_epoch().get_version());
        change_set.set_stage(kvenginepb::SplitStage::PreSplit);
        let pre_split = change_set.mut_pre_split();
        for key in &keys {
            pre_split.mut_keys().push(key.to_vec());
        }
        let mut custom_builder = CustomBuilder::new();
        custom_builder.set_change_set(change_set);
        request.set_custom_request(custom_builder.build());
        let cb_split_scheduler = self.split_scheduler.clone();
        let cmd_cb = Callback::write(Box::new(move |resp| {
            if resp.response.get_header().has_error() {
                let err_msg = resp.response.get_header().get_error().get_message();
                warn!("split runner pre-split failed {:?}", err_msg);
                callback.invoke_with_response(resp.response);
            } else {
                let task = SplitTask {
                    region,
                    peer,
                    callback,
                    method: SplitMethod::SplitFiles,
                };
                debug!("split runner schedule split files task for {:?}", tag);
                cb_split_scheduler.schedule(task).unwrap();
            }
        }));
        debug!(
            "split runner send pre-split command for {:?}, keys: {:?}",
            tag, &keys
        );
        self.router.send_command(request, cmd_cb).unwrap()
    }

    fn split_files(&self, region: metapb::Region, peer: metapb::Peer, callback: Callback) {
        let res = self
            .kv
            .split_shard_files(region.get_id(), region.get_region_epoch().get_version());
        if let Err(err) = res {
            error!("failed to split files {:?}", &err);
            callback.invoke_with_response(new_error(err.into()));
            return;
        }
        let cs = res.unwrap();
        let mut request = new_request(&region, &peer);
        let mut builder = CustomBuilder::new();
        builder.set_change_set(cs);
        request.set_custom_request(builder.build());
        let id_ver = RegionIDVer::from_region(&region);
        let split_keys = self.kv.get_shard(region.get_id()).unwrap().get_split_keys();
        let cb_router = self.router.clone();
        let cmd_cb = Callback::write(Box::new(move |resp| {
            if resp.response.get_header().has_error() {
                let err_msg = resp.response.get_header().get_error().get_message();
                error!("failed to execute split files command {}", err_msg);
                callback.invoke_with_response(resp.response);
            } else {
                let msg = PeerMsg::WaitFollowerSplitFiles(MsgWaitFollowerSplitFiles {
                    split_keys,
                    callback,
                });
                cb_router.send(id_ver.id(), msg).unwrap();
            }
        }));
        self.router.send_command(request, cmd_cb).unwrap();
    }

    fn finish_split(&self, region: metapb::Region, peer: metapb::Peer, callback: Callback) {
        let id_ver = RegionIDVer::from_region(&region);
        debug!("split worker finish split for {:?}", id_ver);
        let shard_res = self.kv.get_shard_with_ver(id_ver.id(), id_ver.ver());
        if let Err(err) = shard_res {
            callback.invoke_with_response(new_error(err.into()));
            return;
        }
        let shard = shard_res.unwrap();
        assert_eq!(
            shard.get_split_stage(),
            kvenginepb::SplitStage::SplitFileDone,
            "for shard {:?}",
            id_ver
        );
        let raw_keys = shard.get_split_keys();
        let encoded_split_keys = raw_keys
            .iter()
            .map(|k| {
                let key = Key::from_raw(k);
                key.as_encoded().to_vec()
            })
            .collect();
        let task = PdTask::AskBatchSplit {
            region: region.clone(),
            split_keys: encoded_split_keys,
            peer: peer.clone(),
            right_derive: true,
            callback,
        };
        self.pd_scheduler.schedule(task).unwrap();
    }
}

impl Runnable for SplitRunner {
    type Task = SplitTask;

    fn run(&mut self, task: SplitTask) {
        let region = task.region;
        let peer = task.peer;
        let shard = self.kv.get_shard(region.get_id());
        let tag = RegionIDVer::from_region(&region);
        if shard.is_none() {
            warn!("split check shard not found"; "region" => tag);
            task.callback
                .invoke_with_response(new_error(box_err!("shard {} not found", tag)));
            return;
        }
        let shard = shard.unwrap();
        match task.method {
            SplitMethod::MaxSize(max_size) => {
                info!(
                    "split region by max size";
                    "region" => tag,
                );
                let keys = shard.get_suggest_split_keys(max_size);
                if keys.len() == 0 {
                    warn!("split check got empty split keys"; "region" => tag);
                    return;
                }
                self.pre_split(region, peer, keys, Callback::None);
            }
            SplitMethod::Keys(ks) => {
                let keys = ks
                    .iter()
                    .map(|k| {
                        let raw_key = Key::from_encoded_slice(k).to_raw().unwrap();
                        Bytes::from(raw_key)
                    })
                    .collect();
                self.pre_split(region, peer, keys, task.callback);
            }
            SplitMethod::SplitFiles => {
                self.split_files(region, peer, task.callback);
            }
            SplitMethod::Finish => {
                self.finish_split(region, peer, task.callback);
            }
        };
    }
}

fn new_request(region: &metapb::Region, peer: &metapb::Peer) -> RaftCmdRequest {
    let mut req = RaftCmdRequest::default();
    req.mut_header().set_region_id(region.get_id());
    req.mut_header()
        .set_region_epoch(region.get_region_epoch().clone());
    req.mut_header().set_peer(peer.clone());
    req
}
