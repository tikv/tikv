// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use bytes::Bytes;
use kvproto::metapb;
use kvproto::metapb::Region;
use kvproto::pdpb::CheckPolicy;
use kvproto::raft_cmdpb::{CustomRequest, RaftCmdRequest, RaftRequestHeader};
use online_config::ConfigChange;
use protobuf::{ProtobufEnum, RepeatedField};
use std::borrow::Borrow;
use std::fmt::{self, Display, Formatter};
use std::thread::sleep;
use std::time::Instant;

use crate::store::{
    Callback, CustomBuilder, MsgWaitFollowerSplitFiles, PeerMsg, RaftCommand, RegionTag,
    WriteCallback, WriteResponse,
};
use crate::{RaftRouter, RaftStoreRouter};
use raftstore::coprocessor::Config;
use tikv_util::codec::bytes::encode_bytes;
use tikv_util::mpsc::Receiver;
use tikv_util::time::Duration;
use tikv_util::worker::{Runnable, RunnableWithTimer};
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
            RegionTag::from_region(&self.region),
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
        let tag = RegionTag::from_region(&region);
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
    let tag = RegionTag::from_region(region);
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
    let mut shard_opt = None;
    let tag = RegionTag::from_region(region);
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
    let mut pre_split = change_set.mut_pre_split();
    for key in keys {
        pre_split.keys.push(key.to_vec());
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
            RegionTag::from_region(region),
            err_msg
        ));
    }
    Ok(())
}

pub fn finish_split(
    router: &RaftRouter,
    region: &metapb::Region,
    keys: Vec<Bytes>,
) -> crate::Result<()> {
    let mut encoded_keys = Vec::with_capacity(keys.len());
    for key in keys {
        let encoded_key = encode_bytes(&key);
        encoded_keys.push(encoded_key);
    }
    loop {}

    todo!()
}
