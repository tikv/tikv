// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::Result;
use kvengine::{Engine, Shard, ShardMeta};
use kvenginepb::ChangeSet;
use std::sync::Arc;
use bytes::Buf;
use kvproto::{metapb, raft_serverpb};
use kvproto::raft_cmdpb::RaftCmdRequest;
use raft_proto::eraftpb;
use protobuf::{Message, ProtobufEnum};
use crate::store::{Applier, ApplyContext, fetch_entries_to, KV_ENGINE_META_KEY, parse_region_state_key, raft_state_key, RaftApplyState, RaftState, REGION_META_KEY_BYTE, rlog, STORE_IDENT_KEY};
use slog_global::info;

pub struct RecoverHandler {
    rf_engine: rfengine::RFEngine,
    store_id: u64,
}

impl RecoverHandler {
    pub fn new(rf_engine: rfengine::RFEngine) -> Self {
        let store_id = match load_store_ident(&rf_engine) {
            Some(ident) => ident.store_id,
            None => 0,
        };
        Self {
            rf_engine,
            store_id,
        }
    }

    fn load_region_meta(&self, shard_id: u64, shard_ver: u64) -> kvengine::Result<(metapb::Region, u64)> {
        let mut region: Option<metapb::Region> = None;
        let res = self.rf_engine.iterate_region_states(shard_id, true, |k, v| -> rfengine::Result<()> {
            if k[0] != REGION_META_KEY_BYTE {
                return Ok(())
            }
            let (meta_ver, _) = parse_region_state_key(k);
            if meta_ver != shard_ver {
                return Ok(())
            }
            let mut state = raft_serverpb::RegionLocalState::new();
            if let Err(err) = state.merge_from_bytes(v) {
                return Err(rfengine::Error::ParseError)
            }
            region = Some(state.take_region());
            Err(rfengine::Error::EOF)
        });
        if let Some(region) = region {
            let raft_state_key = raft_state_key(region.get_region_epoch().get_version());
            let val = self.rf_engine.get_state(region.get_id(), &raft_state_key).unwrap();
            let mut raft_state = RaftState::default();
            raft_state.unmarshal(val.as_ref());
            return Ok((region, raft_state.commit))
        }
        Err(kvengine::Error::ErrOpen("failed to load region meta".to_string()))
    }

    fn execute_admin_request(applier: &mut Applier, ctx: &mut ApplyContext, req: RaftCmdRequest) -> kvengine::Result<()> {
        let admin_req = req.get_admin_request();
        if admin_req.has_change_peer() {
            applier.exec_change_peer(ctx, admin_req)?;
        }
        Ok(())
    }
}

fn load_store_ident(rf_engine: &rfengine::RFEngine) -> Option<raft_serverpb::StoreIdent> {
    let val = rf_engine.get_state(0, STORE_IDENT_KEY);
    if val.is_none() {
        return None;
    }
    let mut ident = raft_serverpb::StoreIdent::new();
    ident.merge_from_bytes(val.unwrap().chunk());
    return Some(ident)
}

impl kvengine::RecoverHandler for RecoverHandler {
    fn recover(&self, engine: &Engine, shard: &Arc<Shard>, meta: &ShardMeta) -> kvengine::Result<()> {
        info!("recover region:{}, ver:{}", shard.id, shard.ver);
        let mut ctx = ApplyContext::new(engine.clone(), None, None);
        let apply_state = RaftApplyState::new(shard.get_write_sequence(), shard.get_meta_sequence());
        let (region_meta, committed_idx) = self.load_region_meta(shard.id, shard.ver)?;
        let low_idx = apply_state.applied_index + 1;
        let high_idx = committed_idx;
        let entries = fetch_entries_to(&self.rf_engine, shard.id, low_idx, high_idx, u64::MAX)
            .map_err(|e| kvengine::Error::ErrOpen("entries unavailable.".to_string()))?;

        let snap = Arc::new(kvengine::SnapAccess::new(shard));
        let mut applier = Applier::new_for_recover(self.store_id, region_meta, snap, apply_state);

        for e in &entries {
            if e.data.len() == 0 || e.entry_type != eraftpb::EntryType::EntryNormal {
                continue;
            }
            let rlog = rlog::decode_log(e.data.chunk());
            match rlog {
                rlog::RaftLog::Request(req) => {
                    if req.has_admin_request() {
                        if req.get_admin_request().has_split() {
                            // We are recovering an parent shard, the split is the last command, we can skip it and return now.
                            return Ok(())
                        }
                    }
                    self.execute_admin_request(&mut applier, req)?;
                },
                rlog::RaftLog::Custom(custom) => {
                    if custom.header.epoch.version() as u64 != shard.ver {
                        continue;
                    }
                    ctx.exec_ctx.apply_state = applier.apply_state;
                    ctx.exec_ctx.index = e.get_index();
                    ctx.exec_ctx.term = e.get_term();
                    if rlog::is_background_change_set(custom.data.chunk()) {
                        let mut cs = custom.get_change_set()?;
                        cs.sequence = e.get_index();
                        let mut rejected = false;
                        if meta.split_stage.value() >= kvenginepb::SplitStage::PreSplit.value() && cs.has_compaction() {
                            let comp = cs.mut_compaction();
                            comp.conflicted = true;
                            rejected = true;
                        }
                        if rejected || !meta.is_duplicated_change_set(&mut cs) {
                            // We don't have a background region worker now, should do it synchronously.
                            engine.apply_change_set(cs)?;
                        }
                    } else {
                        applier.exec_custom_log(&mut ctx, &custom);
                    }
                },
            }
            applier.apply_state.applied_index = e.get_index();
        }
        Ok(())
    }
}

impl kvengine::MetaIterator for RecoverHandler {
    fn iterate<F>(&self, f: F) -> kvengine::Result<()>
    where
        F: FnMut(ChangeSet),
    {
        let mut err_msg = None;
        self.rf_engine.iterate_all_states(false, |region_id, key, val| {
            if key[0] == KV_ENGINE_META_KEY[0] {
                let mut cs = kvenginepb::ChangeSet::new();
                if let Err(e) = cs.merge_from_bytes(val) {
                    err_msg = Some(e.to_string());
                    return false
                }
                f(cs);
            }
            true
        });
        if let Some(err) = err_msg {
            return Err(kvengine::Error::ErrOpen(err))
        }
        Ok(())
    }
}
