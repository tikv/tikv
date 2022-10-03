use std::{
    collections::BTreeMap,
    ops::RangeInclusive,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use collections::HashMap;
use engine_traits::{KvEngine, RaftEngine, Result as EngineTraitsResult};
use keys::enc_end_key;
use kvproto::{
    metapb::Region,
    raft_serverpb::{MergeState, PeerState, RegionLocalState},
};
use tikv_util::{
    info,
    yatp_pool::{DefaultTicker, YatpPoolBuilder},
};
use yatp::{task::future::TaskCell, ThreadPool};

use super::{
    fsm::{apply::RecoverStatus, ApplyRouter, StoreMeta},
    RaftRouter,
};
use crate::store::{
    fsm::{apply::RecoverCallback, ApplyTask},
    MergeResultKind, PeerMsg, SignificantMsg,
};

pub struct Recovery<EK: KvEngine, ER: RaftEngine> {
    range: RangeInclusive<u64>,
    raft_engine: ER,
    router: RaftRouter<EK, ER>,
    apply_router: ApplyRouter<EK>,
    store_meta: Arc<Mutex<StoreMeta>>,
    workers: ThreadPool<TaskCell>,
}

impl<EK: KvEngine, ER: RaftEngine> Recovery<EK, ER> {
    pub fn new(
        range: RangeInclusive<u64>,
        raft_engine: ER,
        router: RaftRouter<EK, ER>,
        apply_router: ApplyRouter<EK>,
        store_meta: Arc<Mutex<StoreMeta>>,
    ) -> Self {
        let workers = YatpPoolBuilder::new(DefaultTicker::default())
            .name_prefix("recovery-")
            .thread_count(4, 4, 4)
            .build_single_level_pool();
        Self {
            range,
            raft_engine,
            router,
            apply_router,
            store_meta,
            workers,
        }
    }

    pub fn run(&self) {
        let mut ctx = RecoveryContext::default();
        let mut recovery_tasks = self.get_region_recovery_tasks(&mut ctx);
        while let Some((version, tasks)) = recovery_tasks.pop_first() {
            let remain_tasks = self.execute_recovery_tasks(&mut ctx, version, tasks);
            for (version, task) in remain_tasks {
                let regions = recovery_tasks
                    .entry(version)
                    .or_insert_with(HashMap::default);
                regions.insert(task.region_id, task);
            }
        }
        for (region_id, merge_state) in ctx.merged_regions {
            let mut meta = self.store_meta.lock().unwrap();
            if let Some(region) = meta.regions.remove(&region_id) {
                let end_key = enc_end_key(&region);
                match meta.region_ranges.get(&end_key).copied() {
                    Some(meta_region_id) if meta_region_id == region_id => {
                        meta.region_ranges.remove(&end_key);
                    }
                    _ => (),
                }
                self.destroy_merged_region(&mut meta, region, merge_state);
            }
        }
        for (region_id, region_state) in ctx.applying_snapshot_regions {
            info!(
                "insert applying snapshot region meta range";
                "region_id" => region_id,
                "region_state" => ?region_state,
            );
            let mut meta = self.store_meta.lock().unwrap();
            let end_key = enc_end_key(region_state.get_region());
            if let Some(conflict_id) = meta.region_ranges.insert(end_key, region_id) {
                panic!(
                    "region meta range conflicted, region {}, conflict region {}",
                    region_id, conflict_id
                );
            }
        }
        info!("all regions recover finished");
    }

    fn get_region_recovery_tasks(
        &self,
        ctx: &mut RecoveryContext,
    ) -> BTreeMap<u64, HashMap<u64, RecoveryTask>> {
        let mut recovery_tasks = BTreeMap::default();
        self.raft_engine
            .for_each_raft_group(&mut |region_id| {
                if let Some((snap_region_state, _)) = self
                    .raft_engine
                    .get_apply_snapshot_state(region_id)
                    .unwrap()
                {
                    ctx.applying_snapshot_regions
                        .insert(region_id, snap_region_state);
                    return Ok(());
                }
                let region_state = self
                    .raft_engine
                    .get_region_state(region_id)
                    .unwrap()
                    .unwrap_or_else(|| {
                        panic!("region_id {}", region_id);
                    });
                match region_state.get_state() {
                    PeerState::Normal | PeerState::Merging => {
                        let version = region_state.get_region().get_region_epoch().get_version();
                        let task = RecoveryTask::new_recovery_task(
                            region_id,
                            &self.range,
                            region_state,
                            &self.raft_engine,
                            ctx,
                        );
                        let regions = recovery_tasks
                            .entry(version)
                            .or_insert_with(HashMap::default);
                        regions.insert(region_id, task);
                    }
                    PeerState::Applying => {
                        panic!("region {} unexpected Applying state for region state in raftdb, region state {:?}", region_id, region_state);
                    }
                    PeerState::Tombstone => (),
                }
                EngineTraitsResult::Ok(())
            })
            .unwrap();

        recovery_tasks
    }

    fn execute_recovery_tasks(
        &self,
        ctx: &mut RecoveryContext,
        version: u64,
        tasks: HashMap<u64, RecoveryTask>,
    ) -> Vec<(u64, RecoveryTask)> {
        let results: Vec<_> = tasks
                .into_iter()
                .filter_map(|(region_id, task)| {
                    let start = task.start;
                    let end = task.end;
                    info!("replay raft logs"; "region_id" => region_id, "start" => start, "end" => end, "version" => version);
                    if task.insert_range {
                        let region_state = task.region_state.unwrap();
                        let region = region_state.get_region();
                        let end_key = enc_end_key(region);
                        let mut meta = self.store_meta.lock().unwrap();
                        if let Some(conflict_id) = meta.region_ranges.insert(end_key, region_id) {
                            let merge_state = ctx.merged_regions.remove(&conflict_id).unwrap_or_else(|| {
                                panic!(
                                    "region ranges conflicted: region {}, conflict_region {}",
                                    region_id,
                                    conflict_id
                                )
                            });
                            let conflict_region = meta.regions.remove(&conflict_id).unwrap();
                            assert!(
                                conflict_region.get_region_epoch().get_version()
                                    < region.get_region_epoch().get_version(),
                                "region {:?}, conflict_region {:?}",
                                region,
                                conflict_region,
                            );
                            self.destroy_merged_region(&mut meta, conflict_region, merge_state)
                        }
                    }
                    if start != end {
                        let (tx, rx) = std::sync::mpsc::sync_channel(1);
                        let router = self.apply_router.clone();
                        let raft_engine = self.raft_engine.clone();
                        self.workers.spawn(async move {
                            let mut entries = vec![];
                            if let Err(e) =
                                raft_engine.fetch_entries_to(region_id, start, end, None, &mut entries)
                            {
                                panic!(
                                    "fetch entries failed: {:?}, region_id: {}, start:{}, end: {}",
                                    e, region_id, start, end
                                );
                            }
                            let term = entries.last().unwrap().get_term();
                            let commit_index = entries.last().unwrap().get_index();
                            router.schedule_task(
                                region_id,
                                ApplyTask::recover(
                                    region_id,
                                    term,
                                    commit_index,
                                    term,
                                    entries,
                                    RecoverCallback(Box::new(move |status| {
                                        tx.send(status).unwrap();
                                    })),
                                ),
                            );
                        });
                        Some((rx, region_id, task.end))
                    } else {
                        ctx.region_applied_index.insert(region_id, task.end - 1);
                        None
                    }
                })
                .collect();
        let mut remain_tasks = vec![];
        for (rx, region_id, end) in results {
            let status = rx.recv().unwrap();
            let mut tasks = self.handle_recover_status(ctx, region_id, version, end, status);
            remain_tasks.append(&mut tasks);
        }
        remain_tasks
    }

    fn handle_recover_status(
        &self,
        ctx: &mut RecoveryContext,
        region_id: u64,
        version: u64,
        end: u64,
        recover_status: RecoverStatus,
    ) -> Vec<(u64, RecoveryTask)> {
        match recover_status {
            RecoverStatus::VersionChanged {
                new_version,
                applied_index,
            } => {
                assert!(applied_index < end);
                ctx.region_applied_index.insert(region_id, applied_index);
                let mut remain_tasks = vec![];
                if applied_index + 1 != end {
                    remain_tasks.push((
                        new_version,
                        RecoveryTask {
                            region_id,
                            start: applied_index + 1,
                            end,
                            insert_range: false,
                            region_state: None,
                        },
                    ))
                }
                if let Some(task) = Self::continue_wait_source_region(ctx, region_id, applied_index)
                {
                    remain_tasks.push(task);
                }
                remain_tasks
            }
            RecoverStatus::WaitMergeSource {
                logs_up_to_date,
                source_region_id,
                commit,
                applied_index,
            } => {
                assert!(applied_index < end - 1);
                let mut merge_prepared = false;
                if let Some(index) = ctx.region_applied_index.get(&source_region_id) {
                    if *index >= commit {
                        merge_prepared = true;
                    }
                }
                info!(
                    "region recovery wait merge source region, source: {}, applied_index: {}, commit: {}, merge_prepared: {}",
                    source_region_id, applied_index, commit, merge_prepared;
                    "region_id" => region_id
                );
                if merge_prepared {
                    logs_up_to_date.store(source_region_id, Ordering::Relaxed);
                    vec![(
                        version,
                        RecoveryTask {
                            region_id,
                            start: applied_index + 1,
                            end,
                            region_state: None,
                            insert_range: false,
                        },
                    )]
                } else {
                    ctx.wait_merge_source_regions.insert(
                        (source_region_id, commit),
                        WaitMergeSourceRegion {
                            to_be_continued: RecoveryTask {
                                region_id,
                                start: applied_index + 1,
                                end,
                                region_state: None,
                                insert_range: false,
                            },
                            version,
                            logs_up_to_date,
                        },
                    );
                    vec![]
                }
            }
            RecoverStatus::Finished => {
                info!("region recover finished"; "region_id" => region_id);
                let applied_index = end - 1;
                ctx.region_applied_index.insert(region_id, applied_index);
                if let Some(task) = Self::continue_wait_source_region(ctx, region_id, applied_index)
                {
                    vec![task]
                } else {
                    vec![]
                }
            }
        }
    }

    fn continue_wait_source_region(
        ctx: &mut RecoveryContext,
        region_id: u64,
        applied_index: u64,
    ) -> Option<(u64, RecoveryTask)> {
        if let Some(WaitMergeSourceRegion {
            to_be_continued: task,
            version,
            logs_up_to_date,
        }) = ctx
            .wait_merge_source_regions
            .remove(&(region_id, applied_index))
        {
            info!(
                "region recovery merge continue, source: {}, applied_index: {}",
                region_id, applied_index;
                "region_id" => task.region_id,
            );
            logs_up_to_date.store(region_id, Ordering::Relaxed);
            Some((version, task))
        } else {
            None
        }
    }

    fn destroy_merged_region(&self, meta: &mut StoreMeta, region: Region, merge_state: MergeState) {
        let region_id = region.get_id();
        info!("schedule to destroy merged region"; "region_id" => region_id, "merge_state" => ?merge_state);
        assert!(meta.region_read_progress.remove(&region_id).is_some());
        let store_id = meta.store_id.unwrap();
        let target_region = merge_state.get_target();
        let target_peer = target_region
            .get_peers()
            .iter()
            .find(|p| p.store_id == store_id)
            .cloned()
            .unwrap();
        let _ = self.router.force_send(
            region_id,
            PeerMsg::SignificantMsg(SignificantMsg::MergeResult {
                target_region_id: target_region.get_id(),
                target: target_peer,
                result: MergeResultKind::FromTargetLog,
            }),
        );
    }
}

#[derive(Default)]
struct RecoveryContext {
    region_applied_index: HashMap<u64, u64>,
    // (source_region_id, commit_merge_index) -> target_region
    wait_merge_source_regions: HashMap<(u64, u64), WaitMergeSourceRegion>,
    applying_snapshot_regions: HashMap<u64, RegionLocalState>,
    merged_regions: HashMap<u64, MergeState>,
}

struct WaitMergeSourceRegion {
    to_be_continued: RecoveryTask,
    version: u64,
    logs_up_to_date: Arc<AtomicU64>,
}

#[derive(Debug)]
struct RecoveryTask {
    region_id: u64,
    start: u64,
    end: u64,
    region_state: Option<RegionLocalState>,
    insert_range: bool,
}

impl RecoveryTask {
    fn new_recovery_task<E: RaftEngine>(
        region_id: u64,
        range: &RangeInclusive<u64>,
        region_state: RegionLocalState,
        engine: &E,
        ctx: &mut RecoveryContext,
    ) -> RecoveryTask {
        let apply_state = engine.get_apply_state(region_id).unwrap().unwrap();
        let (start, end) = if let Some(mut relation) =
            engine.get_seqno_relation(region_id, *range.end()).unwrap()
        {
            assert!(
                relation.get_sequence_number() > *range.start(),
                "relation {:?}, range {:?}",
                relation,
                range
            );
            let start = apply_state.applied_index + 1;
            let end = relation.get_apply_state().applied_index + 1;
            assert!(
                start <= end,
                "region {} apply_state {:?}, relation {:?}, region_state {:?}",
                region_id,
                apply_state,
                relation,
                region_state
            );
            if relation.has_region_state() {
                let mut region_state = relation.take_region_state();
                if region_state.get_state() == PeerState::Tombstone {
                    assert!(region_state.has_merge_state());
                    ctx.merged_regions
                        .insert(region_id, region_state.take_merge_state());
                }
            }
            (start, end)
        } else {
            (apply_state.applied_index + 1, apply_state.applied_index + 1)
        };

        RecoveryTask {
            region_id,
            region_state: Some(region_state),
            start,
            end,
            insert_range: true,
        }
    }
}
