// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::KvEngine;
use kvproto::metapb::Region;
use raft::StateRole;
use raftstore::coprocessor::*;
use tikv_util::worker::Scheduler;

use crate::{cmd::lock_only_filter, endpoint::Task, metrics::RTS_CHANNEL_PENDING_CMD_BYTES};

pub struct Observer<E: KvEngine> {
    scheduler: Scheduler<Task<E::Snapshot>>,
}

impl<E: KvEngine> Observer<E> {
    pub fn new(scheduler: Scheduler<Task<E::Snapshot>>) -> Self {
        Observer { scheduler }
    }

    pub fn register_to(&self, coprocessor_host: &mut CoprocessorHost<E>) {
        // The `resolved-ts` cmd observer will `mem::take` the `Vec<CmdBatch>`, use a low priority
        // to let it be the last observer and avoid affecting other observers
        coprocessor_host
            .registry
            .register_cmd_observer(1000, BoxCmdObserver::new(self.clone()));
        coprocessor_host
            .registry
            .register_role_observer(100, BoxRoleObserver::new(self.clone()));
        coprocessor_host
            .registry
            .register_region_change_observer(100, BoxRegionChangeObserver::new(self.clone()));
    }
}

impl<E: KvEngine> Clone for Observer<E> {
    fn clone(&self) -> Self {
        Self {
            scheduler: self.scheduler.clone(),
        }
    }
}

impl<E: KvEngine> Coprocessor for Observer<E> {}

impl<E: KvEngine> CmdObserver<E> for Observer<E> {
    fn on_flush_applied_cmd_batch(
        &self,
        max_level: ObserveLevel,
        cmd_batches: &mut Vec<CmdBatch>,
        _: &E,
    ) {
        if max_level == ObserveLevel::None {
            return;
        }
        let cmd_batches: Vec<_> = std::mem::take(cmd_batches)
            .into_iter()
            .filter_map(lock_only_filter)
            .collect();
        if cmd_batches.is_empty() {
            return;
        }
        let size = cmd_batches.iter().map(|b| b.size()).sum::<usize>();
        RTS_CHANNEL_PENDING_CMD_BYTES.add(size as i64);
        if let Err(e) = self.scheduler.schedule(Task::ChangeLog {
            cmd_batch: cmd_batches,
            snapshot: None,
        }) {
            info!("failed to schedule change log event"; "err" => ?e);
        }
    }

    fn on_applied_current_term(&self, role: StateRole, region: &Region) {
        // Start to advance resolved ts after peer becomes leader and apply on its term
        if role == StateRole::Leader {
            if let Err(e) = self.scheduler.schedule(Task::RegisterRegion {
                region: region.clone(),
            }) {
                info!("failed to schedule register region task"; "err" => ?e);
            }
        }
    }
}

impl<E: KvEngine> RoleObserver for Observer<E> {
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, role_change: &RoleChange) {
        // Stop to advance resolved ts after peer steps down to follower or candidate.
        // Do not need to check observe id because we expect all role change events are scheduled in order.
        if role_change.state != StateRole::Leader {
            if let Err(e) = self.scheduler.schedule(Task::DeRegisterRegion {
                region_id: ctx.region().id,
            }) {
                info!("failed to schedule deregister region task"; "err" => ?e);
            }
        }
    }
}

impl<E: KvEngine> RegionChangeObserver for Observer<E> {
    fn on_region_changed(
        &self,
        ctx: &mut ObserverContext<'_>,
        event: RegionChangeEvent,
        role: StateRole,
    ) {
        // If the peer is not leader, it must has not registered the observe region or it is deregistering
        // the observe region, so don't need to send `RegionUpdated`/`RegionDestroyed` to update the observe
        // region
        if role != StateRole::Leader {
            return;
        }
        match event {
            RegionChangeEvent::Create => {}
            RegionChangeEvent::Update => {
                if let Err(e) = self
                    .scheduler
                    .schedule(Task::RegionUpdated(ctx.region().clone()))
                {
                    info!("failed to schedule region updated event"; "err" => ?e);
                }
            }
            RegionChangeEvent::Destroy => {
                if let RegionChangeEvent::Destroy = event {
                    if let Err(e) = self
                        .scheduler
                        .schedule(Task::RegionDestroyed(ctx.region().clone()))
                    {
                        info!("failed to schedule region destroyed event"; "err" => ?e);
                    }
                }
            }
            RegionChangeEvent::UpdateBuckets(_) => {}
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use engine_rocks::RocksSnapshot;
    use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
    use kvproto::raft_cmdpb::*;
    use tikv::storage::kv::TestEngineBuilder;
    use tikv_util::worker::{dummy_scheduler, ReceiverWrapper};

    use super::*;

    fn put_cf(cf: &str, key: &[u8], value: &[u8]) -> Request {
        let mut cmd = Request::default();
        cmd.set_cmd_type(CmdType::Put);
        cmd.mut_put().set_cf(cf.to_owned());
        cmd.mut_put().set_key(key.to_vec());
        cmd.mut_put().set_value(value.to_vec());
        cmd
    }

    fn expect_recv(rx: &mut ReceiverWrapper<Task<RocksSnapshot>>, data: Vec<Request>) {
        if data.is_empty() {
            match rx.recv_timeout(Duration::from_millis(10)) {
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => return,
                _ => panic!("unexpected result"),
            };
        }
        match rx.recv_timeout(Duration::from_millis(10)).unwrap().unwrap() {
            Task::ChangeLog { cmd_batch, .. } => {
                assert_eq!(cmd_batch.len(), 1);
                assert_eq!(cmd_batch[0].len(), 1);
                assert_eq!(&cmd_batch[0].cmds[0].request.get_requests(), &data);
            }
            _ => panic!("unexpected task"),
        };
    }

    #[test]
    fn test_observing() {
        let (scheduler, mut rx) = dummy_scheduler();
        let observer = Observer::new(scheduler);
        let engine = TestEngineBuilder::new().build().unwrap().get_rocksdb();
        let mut data = vec![
            put_cf(CF_LOCK, b"k1", b"v"),
            put_cf(CF_DEFAULT, b"k2", b"v"),
            put_cf(CF_LOCK, b"k3", b"v"),
            put_cf(CF_LOCK, b"k4", b"v"),
            put_cf(CF_DEFAULT, b"k6", b"v"),
            put_cf(CF_WRITE, b"k7", b"v"),
            put_cf(CF_WRITE, b"k8", b"v"),
        ];
        let mut cmd = Cmd::new(0, RaftCmdRequest::default(), RaftCmdResponse::default());
        cmd.request.mut_requests().clear();
        for put in &data {
            cmd.request.mut_requests().push(put.clone());
        }

        // Both cdc and resolved-ts worker are observing
        let observe_info = CmdObserveInfo::from_handle(ObserveHandle::new(), ObserveHandle::new());
        let mut cb = CmdBatch::new(&observe_info, 0);
        cb.push(&observe_info, 0, cmd.clone());
        observer.on_flush_applied_cmd_batch(cb.level, &mut vec![cb], &engine);
        // Observe all data
        expect_recv(&mut rx, data.clone());

        // Only cdc is observing
        let observe_info = CmdObserveInfo::from_handle(ObserveHandle::new(), ObserveHandle::new());
        observe_info.rts_id.stop_observing();
        let mut cb = CmdBatch::new(&observe_info, 0);
        cb.push(&observe_info, 0, cmd.clone());
        observer.on_flush_applied_cmd_batch(cb.level, &mut vec![cb], &engine);
        // Still observe all data
        expect_recv(&mut rx, data.clone());

        // Only resolved-ts worker is observing
        let observe_info = CmdObserveInfo::from_handle(ObserveHandle::new(), ObserveHandle::new());
        observe_info.cdc_id.stop_observing();
        let mut cb = CmdBatch::new(&observe_info, 0);
        cb.push(&observe_info, 0, cmd.clone());
        observer.on_flush_applied_cmd_batch(cb.level, &mut vec![cb], &engine);
        // Only observe lock related data
        data.retain(|p| p.get_put().cf != CF_DEFAULT);
        expect_recv(&mut rx, data);

        // Both cdc and resolved-ts worker are not observing
        let observe_info = CmdObserveInfo::from_handle(ObserveHandle::new(), ObserveHandle::new());
        observe_info.rts_id.stop_observing();
        observe_info.cdc_id.stop_observing();
        let mut cb = CmdBatch::new(&observe_info, 0);
        cb.push(&observe_info, 0, cmd);
        observer.on_flush_applied_cmd_batch(cb.level, &mut vec![cb], &engine);
        // Observe no data
        expect_recv(&mut rx, vec![]);
    }
}
