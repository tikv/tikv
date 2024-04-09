// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{
    data_cf_offset, name_to_cf, KvEngine, Mutable, RaftEngine, ALL_CFS, CF_DEFAULT,
};
use fail::fail_point;
use futures::channel::oneshot;
use kvproto::raft_cmdpb::RaftRequestHeader;
use raftstore::{
    store::{
        cmd_resp,
        fsm::{apply, MAX_PROPOSAL_SIZE_RATIO},
        metrics::PEER_WRITE_CMD_COUNTER,
        msg::ErrorCallback,
        util::{self},
        RaftCmdExtraOpts,
    },
    Error, Result,
};
use slog::{error, info};
use tikv_util::{box_err, slog_panic, time::Instant};

use crate::{
    batch::StoreContext,
    fsm::ApplyResReporter,
    operation::SimpleWriteReqEncoder,
    raft::{Apply, Peer},
    router::{ApplyTask, CmdResChannel},
    TabletTask,
};

mod ingest;

pub use raftstore::store::simple_write::{
    SimpleWrite, SimpleWriteBinary, SimpleWriteEncoder, SimpleWriteReqDecoder,
};

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    #[inline]
    pub fn on_simple_write<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        header: Box<RaftRequestHeader>,
        data: SimpleWriteBinary,
        ch: CmdResChannel,
        extra_opts: Option<RaftCmdExtraOpts>,
    ) {
        if !self.serving() {
            apply::notify_req_region_removed(self.region_id(), ch);
            return;
        }
        if let Some(encoder) = self.simple_write_encoder_mut() {
            if encoder.amend(&header, &data) {
                encoder.add_response_channel(ch);
                self.set_has_ready();
                return;
            }
        }
        if let Err(e) = self.validate_command(&header, None, &mut ctx.raft_metrics) {
            let resp = cmd_resp::new_error(e);
            ch.report_error(resp);
            return;
        }
        if let Some(opts) = extra_opts {
            if let Some(Err(e)) = opts.deadline.map(|deadline| deadline.check()) {
                let resp = cmd_resp::new_error(e.into());
                ch.report_error(resp);
                return;
            }
            // Check whether the write request can be proposed with the given disk full
            // option.
            if let Err(e) = self.check_proposal_with_disk_full_opt(ctx, opts.disk_full_opt) {
                let resp = cmd_resp::new_error(e);
                ch.report_error(resp);
                return;
            }
        }
        // To maintain propose order, we need to make pending proposal first.
        self.propose_pending_writes(ctx);
        if let Some(conflict) = self.proposal_control_mut().check_conflict(None) {
            conflict.delay_channel(ch);
            return;
        }
        if self.proposal_control().has_pending_prepare_merge()
            || self.proposal_control().is_merging()
        {
            let resp = cmd_resp::new_error(Error::ProposalInMergingMode(self.region_id()));
            ch.report_error(resp);
            return;
        }
        let mut encoder = SimpleWriteReqEncoder::new(
            header,
            data,
            (ctx.cfg.raft_entry_max_size.0 as f64 * MAX_PROPOSAL_SIZE_RATIO) as usize,
        );
        encoder.add_response_channel(ch);
        self.set_has_ready();
        self.simple_write_encoder_mut().replace(encoder);
    }

    #[inline]
    pub fn on_unsafe_write<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        data: SimpleWriteBinary,
    ) {
        if !self.serving() {
            return;
        }
        let bin = SimpleWriteReqEncoder::new(
            Box::<RaftRequestHeader>::default(),
            data,
            ctx.cfg.raft_entry_max_size.0 as usize,
        )
        .encode()
        .0
        .into_boxed_slice();
        if let Some(scheduler) = self.apply_scheduler() {
            scheduler.send(ApplyTask::UnsafeWrite(bin));
        }
    }

    pub fn propose_pending_writes<T>(&mut self, ctx: &mut StoreContext<EK, ER, T>) {
        if let Some(encoder) = self.simple_write_encoder_mut().take() {
            let header = encoder.header();
            let res = self.validate_command(header, None, &mut ctx.raft_metrics);
            let call_proposed_on_success = if matches!(res, Err(Error::EpochNotMatch { .. })) {
                false
            } else {
                self.applied_to_current_term()
            };

            let (data, chs) = encoder.encode();
            let res = res.and_then(|_| self.propose(ctx, data));

            fail_point!("after_propose_pending_writes");

            self.post_propose_command(ctx, res, chs, call_proposed_on_success);
        }
    }
}

impl<EK: KvEngine, R: ApplyResReporter> Apply<EK, R> {
    #[inline]
    pub fn apply_put(&mut self, cf: &str, index: u64, key: &[u8], value: &[u8]) -> Result<()> {
        PEER_WRITE_CMD_COUNTER.put.inc();
        let off = data_cf_offset(cf);
        if self.should_skip(off, index) {
            return Ok(());
        }
        util::check_key_in_region(key, self.region())?;
        if let Some(s) = self.buckets.as_mut() {
            s.write_key(key, value.len() as u64);
        }
        // Technically it's OK to remove prefix for raftstore v2. But rocksdb doesn't
        // support specifying infinite upper bound in various APIs.
        keys::data_key_with_buffer(key, &mut self.key_buffer);
        self.ensure_write_buffer();
        let res = if cf.is_empty() || cf == CF_DEFAULT {
            // TODO: use write_vector
            self.write_batch
                .as_mut()
                .unwrap()
                .put(&self.key_buffer, value)
        } else {
            self.write_batch
                .as_mut()
                .unwrap()
                .put_cf(cf, &self.key_buffer, value)
        };
        res.unwrap_or_else(|e| {
            slog_panic!(
                self.logger,
                "failed to write";
                "key" => %log_wrappers::Value::key(key),
                "value" => %log_wrappers::Value::value(value),
                "cf" => cf,
                "error" => ?e
            );
        });
        fail::fail_point!("APPLY_PUT", |_| Err(raftstore::Error::Other(
            "aborted by failpoint".into()
        )));
        self.metrics.size_diff_hint += (self.key_buffer.len() + value.len()) as i64;
        if index != u64::MAX {
            self.modifications_mut()[off] = index;
        }
        Ok(())
    }

    #[inline]
    pub fn apply_delete(&mut self, cf: &str, index: u64, key: &[u8]) -> Result<()> {
        PEER_WRITE_CMD_COUNTER.delete.inc();
        let off = data_cf_offset(cf);
        if self.should_skip(off, index) {
            return Ok(());
        }
        util::check_key_in_region(key, self.region())?;
        if let Some(s) = self.buckets.as_mut() {
            s.write_key(key, 0);
        }
        keys::data_key_with_buffer(key, &mut self.key_buffer);
        self.ensure_write_buffer();
        let res = if cf.is_empty() || cf == CF_DEFAULT {
            // TODO: use write_vector
            self.write_batch.as_mut().unwrap().delete(&self.key_buffer)
        } else {
            self.write_batch
                .as_mut()
                .unwrap()
                .delete_cf(cf, &self.key_buffer)
        };
        res.unwrap_or_else(|e| {
            slog_panic!(
                self.logger,
                "failed to delete";
                "key" => %log_wrappers::Value::key(key),
                "cf" => cf,
                "error" => ?e
            );
        });
        self.metrics.size_diff_hint -= self.key_buffer.len() as i64;
        if index != u64::MAX {
            self.modifications_mut()[off] = index;
        }
        Ok(())
    }

    #[inline]
    pub async fn apply_delete_range(
        &mut self,
        mut cf: &str,
        index: u64,
        start_key: &[u8],
        end_key: &[u8],
        notify_only: bool,
    ) -> Result<()> {
        PEER_WRITE_CMD_COUNTER.delete_range.inc();
        let off = data_cf_offset(cf);
        if self.should_skip(off, index) {
            return Ok(());
        }
        if !end_key.is_empty() && start_key >= end_key {
            return Err(box_err!(
                "invalid delete range command, start_key: {:?}, end_key: {:?}",
                start_key,
                end_key
            ));
        }
        util::check_key_in_region(start_key, self.region())?;
        util::check_key_in_region_inclusive(end_key, self.region())?;

        if cf.is_empty() {
            cf = CF_DEFAULT;
        }

        if !ALL_CFS.iter().any(|x| *x == cf) {
            return Err(box_err!("invalid delete range command, cf: {:?}", cf));
        }

        let start_key = keys::data_key(start_key);
        let end_key = keys::data_end_key(end_key);

        let start = Instant::now_coarse();
        // Use delete_files_in_range to drop as many sst files as possible, this
        // is a way to reclaim disk space quickly after drop a table/index.
        let written = if !notify_only {
            let (notify, wait) = oneshot::channel();
            let delete_range = TabletTask::delete_range(
                self.region_id(),
                self.tablet().clone(),
                name_to_cf(cf).unwrap(),
                start_key.clone().into(),
                end_key.clone().into(),
                Box::new(move |written| {
                    notify.send(written).unwrap();
                }),
            );
            if let Err(e) = self.tablet_scheduler().schedule_force(delete_range) {
                error!(self.logger, "fail to delete range";
                    "range_start" => log_wrappers::Value::key(&start_key),
                    "range_end" => log_wrappers::Value::key(&end_key),
                    "notify_only" => notify_only,
                    "error" => ?e,
                );
            }

            wait.await.unwrap()
        } else {
            false
        };

        info!(
            self.logger,
            "execute delete range";
            "range_start" => log_wrappers::Value::key(&start_key),
            "range_end" => log_wrappers::Value::key(&end_key),
            "notify_only" => notify_only,
            "duration" => ?start.saturating_elapsed(),
        );

        if index != u64::MAX && written {
            self.modifications_mut()[off] = index;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use engine_test::{
        ctor::{CfOptions, DbOptions},
        kv::{KvTestEngine, TestTabletFactory},
    };
    use engine_traits::{
        FlushState, Peekable, SstApplyState, TabletContext, TabletRegistry, CF_DEFAULT, DATA_CFS,
    };
    use futures::executor::block_on;
    use kvproto::{
        metapb::Region,
        raft_serverpb::{PeerState, RegionLocalState},
    };
    use raftstore::{
        coprocessor::CoprocessorHost,
        store::{Config, TabletSnapManager},
    };
    use slog::o;
    use tempfile::TempDir;
    use tikv_util::{
        store::new_peer,
        worker::{dummy_scheduler, Worker},
        yatp_pool::{DefaultTicker, YatpPoolBuilder},
    };

    use crate::{
        operation::{
            test_util::{create_tmp_importer, new_delete_range_entry, new_put_entry, MockReporter},
            CommittedEntries,
        },
        raft::Apply,
        worker::tablet,
    };

    #[test]
    fn test_delete_range() {
        let store_id = 2;

        let mut region = Region::default();
        region.set_id(1);
        region.set_end_key(b"k20".to_vec());
        region.mut_region_epoch().set_version(3);
        let peers = vec![new_peer(2, 3)];
        region.set_peers(peers.into());

        let logger = slog_global::borrow_global().new(o!());
        let path = TempDir::new().unwrap();
        let cf_opts = DATA_CFS
            .iter()
            .copied()
            .map(|cf| (cf, CfOptions::default()))
            .collect();
        let factory = Box::new(TestTabletFactory::new(DbOptions::default(), cf_opts));
        let reg = TabletRegistry::new(factory, path.path()).unwrap();
        let ctx = TabletContext::new(&region, Some(5));
        reg.load(ctx, true).unwrap();
        let tablet = reg.get(region.get_id()).unwrap().latest().unwrap().clone();

        let mut region_state = RegionLocalState::default();
        region_state.set_state(PeerState::Normal);
        region_state.set_region(region.clone());
        region_state.set_tablet_index(5);

        let (read_scheduler, _rx) = dummy_scheduler();
        let (reporter, _) = MockReporter::new();
        let (tmp_dir, importer) = create_tmp_importer();
        let host = CoprocessorHost::<KvTestEngine>::default();

        let snap_mgr = TabletSnapManager::new(tmp_dir.path(), None).unwrap();
        let tablet_worker = Worker::new("tablet-worker");
        let tablet_scheduler = tablet_worker.start(
            "tablet-worker",
            tablet::Runner::new(reg.clone(), importer.clone(), snap_mgr, logger.clone()),
        );
        tikv_util::defer!(tablet_worker.stop());
        let high_priority_pool = YatpPoolBuilder::new(DefaultTicker::default()).build_future_pool();

        let mut apply = Apply::new(
            &Config::default(),
            region
                .get_peers()
                .iter()
                .find(|p| p.store_id == store_id)
                .unwrap()
                .clone(),
            region_state,
            reporter,
            reg,
            read_scheduler,
            Arc::new(FlushState::new(5)),
            SstApplyState::default(),
            None,
            5,
            None,
            importer,
            host,
            tablet_scheduler,
            high_priority_pool,
            logger.clone(),
        );

        // put (k1, v1);
        let ce = CommittedEntries {
            entry_and_proposals: vec![(
                new_put_entry(
                    region.id,
                    region.get_region_epoch().clone(),
                    b"k1",
                    b"v1",
                    5,
                    6,
                ),
                vec![],
            )],
        };
        block_on(async { apply.apply_committed_entries(ce).await });
        apply.flush();

        // must read (k1, v1) from tablet.
        let v1 = tablet.get_value_cf(CF_DEFAULT, b"zk1").unwrap().unwrap();
        assert_eq!(v1, b"v1");

        // delete range
        let ce = CommittedEntries {
            entry_and_proposals: vec![(
                new_delete_range_entry(
                    region.id,
                    region.get_region_epoch().clone(),
                    5,
                    7,
                    CF_DEFAULT,
                    region.get_start_key(),
                    region.get_end_key(),
                    false, // notify_only
                ),
                vec![],
            )],
        };
        block_on(async { apply.apply_committed_entries(ce).await });

        // must get none for k1.
        let res = tablet.get_value_cf(CF_DEFAULT, b"zk1").unwrap();
        assert!(res.is_none(), "{:?}", res);
    }
}
