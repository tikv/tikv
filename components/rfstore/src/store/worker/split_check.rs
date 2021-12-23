// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use bytes::Bytes;
use kvproto::metapb;
use kvproto::raft_cmdpb::{CustomRequest, RaftCmdRequest, RaftRequestHeader};
use online_config::ConfigChange;
use protobuf::ProtobufEnum;
use std::fmt::{self, Display, Formatter};
use std::thread::sleep;

use crate::store::{
    Callback, CasualMessage, CustomBuilder, MsgWaitFollowerSplitFiles, PeerMsg, RegionIDVer,
    WriteResponse, PENDING_CONF_CHANGE_ERR_MSG,
};
use crate::{RaftRouter, RaftStoreRouter};
use tikv_util::codec::bytes::encode_bytes;
use tikv_util::mpsc::Receiver;
use tikv_util::time::Duration;
use tikv_util::worker::Runnable;
use tikv_util::{box_err, error, info, warn};

#[derive(Debug)]
pub struct SplitCheckTask {
    region: metapb::Region,
    peer: metapb::Peer,
    max_size: u64,
}

impl Display for SplitCheckTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[split check worker] Split Check Task for {}, max_size: {}",
            RegionIDVer::from_region(&self.region),
            self.max_size,
        )
    }
}

impl SplitCheckTask {
    pub fn new(region: metapb::Region, peer: metapb::Peer, max_size: u64) -> SplitCheckTask {
        Self {
            region,
            peer,
            max_size,
        }
    }
}

pub struct SplitCheckRunner {
    kv: kvengine::Engine,
    router: RaftRouter,
}

impl SplitCheckRunner {
    pub fn new(kv: kvengine::Engine, router: RaftRouter) -> Self {
        Self { kv, router }
    }
}

impl Runnable for SplitCheckRunner {
    type Task = SplitCheckTask;

    fn run(&mut self, task: SplitCheckTask) {
        let region = task.region;
        let shard = self.kv.get_shard(region.get_id());
        let tag = RegionIDVer::from_region(&region);
        if shard.is_none() {
            warn!("split check shard not found"; "region" => tag);
            return;
        }
        let shard = shard.unwrap();
        let keys = shard.get_suggest_split_keys(task.max_size);
        if keys.len() == 0 {
            warn!("split check got empty split keys"; "region" => tag);
            return;
        }
        info!(
            "split region by checker";
            "region" => tag,
        );
        let _ = split_engine_and_region(&self.router, &self.kv, &region, &task.peer, keys);
    }
}

// splitEngineAndRegion execute the complete procedure to split a region.
// 1. execute PreSplit on raft command.
// 2. Split the engine files.
// 3. Split the region.
pub fn split_engine_and_region(
    router: &RaftRouter,
    kv: &kvengine::Engine,
    region: &metapb::Region,
    peer: &metapb::Peer,
    keys: Vec<Bytes>,
) -> crate::Result<Receiver<WriteResponse>> {
    pre_split_region(router, kv, region, peer, &keys)?;
    split_shard_files(router, kv, region, peer)?;
    let tag = RegionIDVer::from_region(region);
    info!("send a msg to wait for followers to finish splitting files"; "region" => tag);
    let (tx, rx) = tikv_util::mpsc::bounded(1);
    let cb = Callback::write(Box::new(move |resp| {
        tx.send(resp);
    }));
    let msg = MsgWaitFollowerSplitFiles {
        split_keys: keys,
        callback: cb,
    };
    router.send(region.get_id(), PeerMsg::WaitFollowerSplitFiles(msg))?;
    Ok(rx)
}

fn new_request(region: &metapb::Region, peer: &metapb::Peer) -> RaftCmdRequest {
    let mut req = RaftCmdRequest::default();
    req.mut_header().set_region_id(region.get_id());
    req.mut_header()
        .set_region_epoch(region.get_region_epoch().clone());
    req.mut_header().set_peer(peer.clone());
    req
}

fn pre_split_region(
    router: &RaftRouter,
    kv: &kvengine::Engine,
    region: &metapb::Region,
    peer: &metapb::Peer,
    keys: &Vec<Bytes>,
) -> crate::Result<()> {
    let mut shard_opt;
    let tag = RegionIDVer::from_region(region);
    loop {
        shard_opt = kv.get_shard(region.get_id());
        if shard_opt.is_none() {
            return Err(box_err!("shard not found maybe removed"));
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
        return Err(box_err!(
            "wrong split stage {}",
            shard.get_split_stage().value()
        ));
    }
    let mut request = new_request(region, peer);
    let mut change_set = kvenginepb::ChangeSet::default();
    change_set.set_shard_id(region.get_id());
    change_set.set_shard_ver(region.get_region_epoch().get_version());
    change_set.set_stage(kvenginepb::SplitStage::PreSplit);
    let pre_split = change_set.mut_pre_split();
    for key in keys {
        pre_split.mut_keys().push(key.to_vec());
    }
    let mut custom_builder = CustomBuilder::new();
    custom_builder.set_change_set(change_set);
    request.set_custom_request(custom_builder.build());
    let (tx, rx) = tikv_util::mpsc::bounded(1);
    router.send_command(
        request,
        Callback::write(Box::new(move |resp| {
            tx.send(resp);
        })),
    );
    let resp = rx.recv().unwrap();
    let header = resp.response.get_header();
    if header.has_error() {
        let err_msg = header.get_error().get_message();
        return Err(box_err!("{} failed to pre_split {}", tag, err_msg));
    }
    Ok(())
}

pub fn split_shard_files(
    router: &RaftRouter,
    kv: &kvengine::Engine,
    region: &metapb::Region,
    peer: &metapb::Peer,
) -> crate::Result<()> {
    let f = kv.split_shard_files(region.get_id(), region.get_region_epoch().get_version());
    let cs = futures::executor::block_on(f)?;
    let mut request = new_request(region, peer);
    let mut builder = CustomBuilder::new();
    builder.set_change_set(cs);
    request.set_custom_request(builder.build());
    let (tx, rx) = tikv_util::mpsc::bounded(1);
    router.send_command(
        request,
        Callback::write(Box::new(move |resp| {
            tx.send(resp);
        })),
    );
    let resp = rx.recv().unwrap();
    let header = resp.response.get_header();
    if header.has_error() {
        let err_msg = header.get_error().get_message();
        return Err(box_err!(
            "{} failed to split shard files {}",
            RegionIDVer::from_region(region),
            err_msg
        ));
    }
    Ok(())
}

pub fn finish_split(
    router: &RaftRouter,
    region: &metapb::Region,
    keys: Vec<Bytes>,
) -> crate::Result<Vec<metapb::Region>> {
    loop {
        let mut encoded_keys = Vec::with_capacity(keys.len());
        for key in &keys {
            let encoded_key = encode_bytes(key);
            encoded_keys.push(encoded_key);
        }
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        let callback = Callback::write(Box::new(move |resp| {
            tx.send(resp);
        }));
        let split = CasualMessage::SplitRegion {
            region_epoch: region.get_region_epoch().clone(),
            split_keys: encoded_keys,
            callback,
            source: Default::default(),
        };
        let id_ver = RegionIDVer::from_region(region);
        let msg = PeerMsg::CasualMessage(split);
        router.send(region.get_id(), msg)?;
        let resp = rx.recv().unwrap();
        let header = resp.response.get_header();
        if header.has_error() {
            if header.get_error().has_epoch_not_match() {
                let epoch_not_match = header.get_error().get_epoch_not_match();
                let mut r = None;
                if epoch_not_match.get_current_regions().len() > 0 {
                    for cr in epoch_not_match.get_current_regions() {
                        if cr.get_id() == region.get_id() {
                            r = Some(cr.clone());
                            break;
                        }
                    }
                }

                if r.is_some()
                    && r.as_ref().unwrap().get_region_epoch().get_version()
                        == region.get_region_epoch().get_version()
                {
                    let err_msg = header.get_error().get_message();
                    warn!("leader finish split error"; "region" => id_ver, "error" => err_msg);
                }
            } else if header
                .get_error()
                .get_message()
                .contains(PENDING_CONF_CHANGE_ERR_MSG)
            {
                warn!("leader finish split error";
                    "region" => id_ver,
                    "error" => header.get_error().get_message(),
                );
                // TODO(z) this may block worker for a long time.
                sleep(Duration::from_millis(100));
                continue;
            }
            error!("leader finished split error"; "region" => id_ver, "error" => header.get_error().get_message());
            return Err(box_err!(header.get_error().get_message()));
        }
        info!("leader finished split successfully"; "region" => id_ver);
        return Ok(resp
            .response
            .get_admin_response()
            .get_splits()
            .get_regions()
            .to_vec());
    }
}
