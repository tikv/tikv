// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_traits::KvEngine;
use kvproto::metapb::{Peer, Region};
use raft::StateRole;
use raftstore::coprocessor::*;
use raftstore::store::RegionSnapshot;
use tikv_util::worker::Scheduler;

use crate::endpoint::Task;

pub struct Observer<E: KvEngine> {
    scheduler: Scheduler<Task<E::Snapshot>>,
    need_old_value: bool,
}

impl<E: KvEngine> Observer<E> {
    pub fn new(scheduler: Scheduler<Task<E::Snapshot>>) -> Self {
        Observer {
            scheduler,
            need_old_value: true,
        }
    }

    // Disable old value, currently only use in tests to avoid holding the snapshot
    // and cause data can not be deleted
    pub fn disable_old_value(&mut self) {
        self.need_old_value = false;
    }

    pub fn register_to(&self, coprocessor_host: &mut CoprocessorHost<E>) {
        // 100 is the priority of the observer. `resolved-ts` worker should have a high priority.
        coprocessor_host
            .registry
            .register_cmd_observer(100, BoxCmdObserver::new(self.clone()));
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
            need_old_value: self.need_old_value,
        }
    }
}

impl<E: KvEngine> Coprocessor for Observer<E> {}

impl<E: KvEngine> CmdObserver<E> for Observer<E> {
    fn on_flush_applied_cmd_batch(&self, cmd_batches: Vec<CmdBatch>, engine: E) {
        let cmd_batches: Vec<_> = cmd_batches
            .into_iter()
            .filter(|cb| !cb.is_empty())
            .filter_map(CmdBatch::filter)
            .collect();
        if cmd_batches.is_empty() {
            return;
        }
        let region = {
            let mut r = Region::default();
            r.mut_peers().push(Peer::default());
            r
        };
        // Create a snapshot here for preventing the old value was GC-ed.
        // TODO: only need it after enabling old value, may add a flag to indicate whether to get it.
        let snapshot = if self.need_old_value {
            Some(RegionSnapshot::from_snapshot(
                Arc::new(engine.snapshot()),
                Arc::new(region),
            ))
        } else {
            None
        };
        if let Err(e) = self.scheduler.schedule(Task::ChangeLog {
            cmd_batch: cmd_batches,
            snapshot,
        }) {
            info!("failed to schedule change log event"; "err" => ?e);
        }
    }

    fn on_flush_applied_cmd_batch_ref(&self, _: &[CmdBatch], _: E) {
        unreachable!();
    }
}

impl<E: KvEngine> RoleObserver for Observer<E> {
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, role: StateRole) {
        if let Err(e) = self.scheduler.schedule(Task::RegionRoleChanged {
            role,
            region: ctx.region().clone(),
        }) {
            info!("failed to schedule region role changed event"; "err" => ?e);
        }
    }
}

impl<E: KvEngine> RegionChangeObserver for Observer<E> {
    fn on_region_changed(
        &self,
        ctx: &mut ObserverContext<'_>,
        event: RegionChangeEvent,
        _: StateRole,
    ) {
        // TODO: handle region update event
        if let RegionChangeEvent::Destroy = event {
            if let Err(e) = self
                .scheduler
                .schedule(Task::RegionDestroyed(ctx.region().clone()))
            {
                info!("failed to schedule region destroyed event"; "err" => ?e);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use engine_rocks::RocksSnapshot;
    use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
    use kvproto::raft_cmdpb::*;
    use std::time::Duration;
    use tikv::storage::kv::TestEngineBuilder;
    use tikv_util::worker::{dummy_scheduler, ReceiverWrapper};

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
        let data = vec![
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
        let (cdc_handle, rts_handle) = (ObserveHandle::new(), ObserveHandle::new());
        let mut cb = CmdBatch::new(&cdc_handle, &rts_handle, Region::default());
        cb.push(cdc_handle.id, rts_handle.id, 0, cmd.clone());
        observer.on_flush_applied_cmd_batch(vec![cb], engine.clone());
        // Observe all data
        expect_recv(&mut rx, data.clone());

        // Only cdc is observing
        let (cdc_handle, rts_handle) = (ObserveHandle::new(), ObserveHandle::new());
        rts_handle.stop_observing();
        let mut cb = CmdBatch::new(&cdc_handle, &rts_handle, Region::default());
        cb.push(cdc_handle.id, rts_handle.id, 0, cmd.clone());
        observer.on_flush_applied_cmd_batch(vec![cb], engine.clone());
        // Still observe all data
        expect_recv(&mut rx, data.clone());

        // Only resolved-ts worker is observing
        let (cdc_handle, rts_handle) = (ObserveHandle::new(), ObserveHandle::new());
        cdc_handle.stop_observing();
        let mut cb = CmdBatch::new(&cdc_handle, &rts_handle, Region::default());
        cb.push(cdc_handle.id, rts_handle.id, 0, cmd.clone());
        observer.on_flush_applied_cmd_batch(vec![cb], engine.clone());
        // Still observe all data
        expect_recv(&mut rx, data);

        // Both cdc and resolved-ts worker are not observing
        let (cdc_handle, rts_handle) = (ObserveHandle::new(), ObserveHandle::new());
        cdc_handle.stop_observing();
        rts_handle.stop_observing();
        let mut cb = CmdBatch::new(&cdc_handle, &rts_handle, Region::default());
        cb.push(cdc_handle.id, rts_handle.id, 0, cmd);
        observer.on_flush_applied_cmd_batch(vec![cb], engine);
        // Observe no data
        expect_recv(&mut rx, vec![]);
    }
}
