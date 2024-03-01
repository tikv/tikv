// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::*, time::Duration};

use collections::HashMap;
use engine_rocks::RocksEngine;
use futures::{executor::block_on, stream, SinkExt};
use grpcio::{ChannelBuilder, Environment, Result, WriteFlags};
use kvproto::{
    import_sstpb::{
        AcquireLease, IngestRequest, LeaseRequest, ReleaseLease, SstMeta, UploadRequest,
        UploadResponse,
    },
    import_sstpb_grpc::ImportSstClient,
    kvrpcpb::*,
    tikvpb::TikvClient,
};
use online_config::ConfigValue;
use resolved_ts::Task;
use test_raftstore::*;
use tikv_util::{config::ReadableDuration, HandyRwLock};
use txn_types::TimeStamp;

static INIT: Once = Once::new();

pub fn init() {
    INIT.call_once(test_util::setup_for_ci);
}

pub struct TestSuite {
    pub cluster: Cluster<RocksEngine, ServerCluster<RocksEngine>>,
    tikv_cli: HashMap<u64, TikvClient>,
    import_cli: HashMap<u64, ImportSstClient>,

    env: Arc<Environment>,
}

impl TestSuite {
    pub fn new(count: usize) -> Self {
        let mut cluster = new_server_cluster(1, count);
        // Increase the Raft tick interval to make this test case running reliably.
        configure_for_lease_read(&mut cluster.cfg, Some(100), None);

        // Start resolved ts endpoint.
        cluster.cfg.resolved_ts.enable = true;
        cluster.cfg.resolved_ts.advance_ts_interval = ReadableDuration::millis(10);
        cluster.run();

        TestSuite {
            cluster,
            env: Arc::new(Environment::new(1)),
            tikv_cli: HashMap::default(),
            import_cli: HashMap::default(),
        }
    }

    pub fn stop(mut self) {
        self.cluster.shutdown();
    }

    pub fn must_change_advance_ts_interval(&self, store_id: u64, new_interval: Duration) {
        let change = {
            let mut c = std::collections::HashMap::default();
            c.insert(
                "advance_ts_interval".to_owned(),
                ConfigValue::Duration(new_interval.as_millis() as u64),
            );
            c
        };
        self.must_schedule_task(store_id, Task::ChangeConfig { change });
    }

    pub fn must_change_memory_quota(&self, store_id: u64, bytes: u64) {
        let change = {
            let mut c = std::collections::HashMap::default();
            c.insert("memory_quota".to_owned(), ConfigValue::Size(bytes));
            c
        };
        self.must_schedule_task(store_id, Task::ChangeConfig { change });
    }

    pub fn must_schedule_task(&self, store_id: u64, task: Task) {
        let scheduler = self
            .cluster
            .sim
            .read()
            .unwrap()
            .get_resolved_ts_scheduler(store_id)
            .unwrap();
        scheduler.schedule(task).unwrap();
    }

    pub fn must_kv_prewrite(
        &mut self,
        region_id: u64,
        muts: Vec<Mutation>,
        pk: Vec<u8>,
        ts: TimeStamp,
        try_one_pc: bool,
    ) {
        let mut prewrite_req = PrewriteRequest::default();
        prewrite_req.set_context(self.get_context(region_id));
        prewrite_req.set_mutations(muts.into_iter().collect());
        prewrite_req.primary_lock = pk;
        prewrite_req.start_version = ts.into_inner();
        prewrite_req.lock_ttl = prewrite_req.start_version + 1;
        prewrite_req.try_one_pc = try_one_pc;
        let prewrite_resp = self
            .get_tikv_client(region_id)
            .kv_prewrite(&prewrite_req)
            .unwrap();
        assert!(
            !prewrite_resp.has_region_error(),
            "{:?}",
            prewrite_resp.get_region_error()
        );
        assert!(
            prewrite_resp.errors.is_empty(),
            "{:?}",
            prewrite_resp.get_errors()
        );
        if try_one_pc {
            assert_ne!(prewrite_resp.get_one_pc_commit_ts(), 0);
        }
    }

    pub fn must_kv_commit(
        &mut self,
        region_id: u64,
        keys: Vec<Vec<u8>>,
        start_ts: TimeStamp,
        commit_ts: TimeStamp,
    ) {
        let mut commit_req = CommitRequest::default();
        commit_req.set_context(self.get_context(region_id));
        commit_req.start_version = start_ts.into_inner();
        commit_req.set_keys(keys.into_iter().collect());
        commit_req.commit_version = commit_ts.into_inner();
        let commit_resp = self
            .get_tikv_client(region_id)
            .kv_commit(&commit_req)
            .unwrap();
        assert!(
            !commit_resp.has_region_error(),
            "{:?}",
            commit_resp.get_region_error()
        );
        assert!(!commit_resp.has_error(), "{:?}", commit_resp.get_error());
    }

    pub fn must_kv_rollback(&mut self, region_id: u64, keys: Vec<Vec<u8>>, start_ts: TimeStamp) {
        let mut rollback_req = BatchRollbackRequest::default();
        rollback_req.set_context(self.get_context(region_id));
        rollback_req.start_version = start_ts.into_inner();
        rollback_req.set_keys(keys.into_iter().collect());
        let rollback_resp = self
            .get_tikv_client(region_id)
            .kv_batch_rollback(&rollback_req)
            .unwrap();
        assert!(
            !rollback_resp.has_region_error(),
            "{:?}",
            rollback_resp.get_region_error()
        );
        assert!(
            !rollback_resp.has_error(),
            "{:?}",
            rollback_resp.get_error()
        );
    }

    pub fn get_context(&mut self, region_id: u64) -> Context {
        let epoch = self.cluster.get_region_epoch(region_id);
        let leader = self.cluster.leader_of_region(region_id).unwrap();
        let mut context = Context::default();
        context.set_region_id(region_id);
        context.set_peer(leader);
        context.set_region_epoch(epoch);
        context
    }

    pub fn get_tikv_client(&mut self, region_id: u64) -> &TikvClient {
        let leader = self.cluster.leader_of_region(region_id).unwrap();
        let store_id = leader.get_store_id();
        let addr = self.cluster.sim.rl().get_addr(store_id);
        let env = self.env.clone();
        self.tikv_cli
            .entry(leader.get_store_id())
            .or_insert_with(|| {
                let channel = ChannelBuilder::new(env).connect(&addr);
                TikvClient::new(channel)
            })
    }

    pub fn get_import_client(&mut self, region_id: u64) -> &ImportSstClient {
        let leader = self.cluster.leader_of_region(region_id).unwrap();
        let store_id = leader.get_store_id();
        let addr = self.cluster.sim.rl().get_addr(store_id);
        let env = self.env.clone();
        self.import_cli
            .entry(leader.get_store_id())
            .or_insert_with(|| {
                let channel = ChannelBuilder::new(env).connect(&addr);
                ImportSstClient::new(channel)
            })
    }

    pub fn region_resolved_ts(&mut self, region_id: u64) -> Option<TimeStamp> {
        let leader = self.cluster.leader_of_region(region_id)?;
        let meta = self.cluster.store_metas[&leader.store_id].lock().unwrap();
        Some(
            meta.region_read_progress
                .get_resolved_ts(&region_id)
                .unwrap()
                .into(),
        )
    }

    pub fn region_tracked_index(&mut self, region_id: u64) -> u64 {
        for _ in 0..50 {
            if let Some(leader) = self.cluster.leader_of_region(region_id) {
                let meta = self.cluster.store_metas[&leader.store_id].lock().unwrap();
                if let Some(tracked_index) = meta.region_read_progress.get_tracked_index(&region_id)
                {
                    return tracked_index;
                }
            }
            sleep_ms(100)
        }
        panic!("fail to get region tracked index after 50 trys");
    }

    pub fn must_get_rts(&mut self, region_id: u64, rts: TimeStamp) {
        for _ in 0..50 {
            if let Some(ts) = self.region_resolved_ts(region_id) {
                if rts == ts {
                    return;
                }
            }
            sleep_ms(100)
        }
        panic!("fail to get same ts after 50 trys");
    }

    pub fn must_get_rts_ge(&mut self, region_id: u64, rts: TimeStamp) {
        for _ in 0..50 {
            if let Some(ts) = self.region_resolved_ts(region_id) {
                if rts < ts {
                    return;
                }
            }
            sleep_ms(100)
        }
        panic!("fail to get greater ts after 50 trys");
    }

    pub fn must_acquire_sst_lease(&mut self, region_id: u64, meta: &SstMeta, ttl: Duration) {
        let import = self.get_import_client(region_id);
        let mut acquire = AcquireLease::default();
        acquire.mut_lease().mut_region().set_id(region_id);
        acquire.mut_lease().set_uuid(meta.get_uuid().into());
        acquire.set_ttl(ttl.as_secs());
        let mut req = LeaseRequest::default();
        req.mut_acquire().push(acquire);

        let resp = import.lease(&req).unwrap();

        let acquired_lease = &resp.get_acquired()[0];
        assert_eq!(region_id, acquired_lease.get_region().get_id(),);
        assert_eq!(meta.get_uuid(), acquired_lease.get_uuid(),);
    }

    pub fn must_release_sst_lease(&mut self, region_id: u64, meta: &SstMeta) {
        let import = self.get_import_client(region_id);
        let mut release = ReleaseLease::default();
        release.mut_lease().mut_region().set_id(region_id);
        release.mut_lease().set_uuid(meta.get_uuid().into());
        let mut req = LeaseRequest::default();
        req.mut_release().push(release);

        let resp = import.lease(&req).unwrap();

        let released_lease = &resp.get_released()[0];
        assert_eq!(region_id, released_lease.get_region().get_id(),);
        assert_eq!(meta.get_uuid(), released_lease.get_uuid(),);
    }

    pub fn upload_sst(
        &mut self,
        region_id: u64,
        meta: &SstMeta,
        data: &[u8],
    ) -> Result<UploadResponse> {
        let import = self.get_import_client(region_id);
        let mut r1 = UploadRequest::default();
        r1.set_meta(meta.clone());
        let mut r2 = UploadRequest::default();
        r2.set_data(data.to_vec());
        let reqs: Vec<_> = vec![r1, r2]
            .into_iter()
            .map(|r| Result::Ok((r, WriteFlags::default())))
            .collect();
        let (mut tx, rx) = import.upload().unwrap();
        let mut stream = stream::iter(reqs);
        block_on(async move {
            let send_res = tx.send_all(&mut stream).await;
            let close_res = tx.close().await;
            match rx.await {
                Ok(resp) => Ok(resp),
                Err(e) => {
                    send_res?;
                    close_res?;
                    Err(e)
                }
            }
        })
    }

    pub fn must_ingest_sst(&mut self, region_id: u64, meta: SstMeta) {
        let mut ingest_request = IngestRequest::default();
        ingest_request.set_context(self.get_context(region_id));
        ingest_request.set_sst(meta);

        let ingest_sst_resp = self
            .get_import_client(region_id)
            .ingest(&ingest_request)
            .unwrap();

        assert!(
            !ingest_sst_resp.has_error(),
            "{:?}",
            ingest_sst_resp.get_error()
        );
    }
}
