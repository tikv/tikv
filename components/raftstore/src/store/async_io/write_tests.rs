// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::mpsc, time::Duration};

use collections::HashSet;
use crossbeam::channel::{unbounded, Receiver, Sender};
use engine_test::{kv::KvTestEngine, new_temp_engine, raft::RaftTestEngine};
use engine_traits::{Engines, Mutable, Peekable, RaftEngineReadOnly, WriteBatchExt};
use kvproto::{
    raft_cmdpb::{RaftCmdRequest, RaftRequestHeader},
    raft_serverpb::{RaftApplyState, RaftMessage, RegionLocalState},
    resource_manager::{GroupMode, GroupRawResourceSettings, ResourceGroup},
};
use resource_control::ResourceGroupManager;
use tempfile::Builder;

use super::*;
use crate::{
    store::{
        async_io::write_router::tests::TestContext, local_metrics::RaftMetrics,
        peer_storage::tests::new_entry, Config, Transport, WriteRouter,
    },
    Result,
};
type TestKvWriteBatch = <KvTestEngine as WriteBatchExt>::WriteBatch;
type TestRaftLogBatch = <RaftTestEngine as RaftEngine>::LogBatch;

fn must_have_entries_and_state(
    raft_engine: &RaftTestEngine,
    entries_state: Vec<(u64, Vec<Entry>, RaftLocalState)>,
) {
    for (region_id, entries, state) in entries_state {
        for e in entries {
            assert_eq!(
                raft_engine
                    .get_entry(region_id, e.get_index())
                    .unwrap()
                    .unwrap(),
                e
            );
        }
        assert_eq!(
            raft_engine.get_raft_state(region_id).unwrap().unwrap(),
            state
        );
        assert!(
            raft_engine
                .get_entry(region_id, state.get_last_index() + 1)
                .unwrap()
                .is_none()
        );
    }
}

fn new_raft_state(term: u64, vote: u64, commit: u64, last_index: u64) -> RaftLocalState {
    let mut raft_state = RaftLocalState::new();
    raft_state.mut_hard_state().set_term(term);
    raft_state.mut_hard_state().set_vote(vote);
    raft_state.mut_hard_state().set_commit(commit);
    raft_state.set_last_index(last_index);
    raft_state
}

#[derive(Clone)]
struct TestNotifier {
    tx: Sender<(u64, (u64, u64))>,
}

impl PersistedNotifier for TestNotifier {
    fn notify(&self, region_id: u64, peer_id: u64, ready_number: u64) {
        self.tx.send((region_id, (peer_id, ready_number))).unwrap()
    }
}

#[derive(Clone)]
struct TestTransport {
    tx: Sender<RaftMessage>,
}

impl Transport for TestTransport {
    fn send(&mut self, msg: RaftMessage) -> Result<()> {
        self.tx.send(msg).unwrap();
        Ok(())
    }
    fn set_store_allowlist(&mut self, _: Vec<u64>) {
        unimplemented!();
    }
    fn need_flush(&self) -> bool {
        false
    }
    fn flush(&mut self) {}
}

fn must_have_same_count_msg(msg_count: u32, msg_rx: &Receiver<RaftMessage>) {
    let mut count = 0;
    while msg_rx.try_recv().is_ok() {
        count += 1;
    }
    assert_eq!(count, msg_count);
}

fn must_have_same_notifies(
    notifies: Vec<(u64, (u64, u64))>,
    notify_rx: &Receiver<(u64, (u64, u64))>,
) {
    let mut notify_set = HashSet::default();
    for n in notifies {
        notify_set.insert(n);
    }
    while let Ok(n) = notify_rx.try_recv() {
        if !notify_set.remove(&n) {
            panic!("{:?} not in expected notify", n);
        }
    }
    assert!(
        notify_set.is_empty(),
        "remaining expected notify {:?} not exist",
        notify_set
    );
}

fn must_wait_same_notifies(
    notifies: Vec<(u64, (u64, u64))>,
    notify_rx: &Receiver<(u64, (u64, u64))>,
) {
    let mut notify_map = HashMap::default();
    for (region_id, n) in notifies {
        notify_map.insert(region_id, n);
    }
    let timer = Instant::now();
    loop {
        match notify_rx.recv_timeout(Duration::from_secs(3)) {
            Ok((region_id, n)) => {
                if let Some(n2) = notify_map.get(&region_id) {
                    if n == *n2 {
                        notify_map.remove(&region_id);
                        if notify_map.is_empty() {
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                panic!("recv error: {:?}", e);
            }
        }

        if timer.saturating_elapsed() > Duration::from_secs(5) {
            panic!("wait some notifies after 5 seconds")
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn init_write_batch(
    engines: &Engines<KvTestEngine, RaftTestEngine>,
    task: &mut WriteTask<KvTestEngine, RaftTestEngine>,
) {
    task.extra_write.ensure_v1(|| engines.kv.write_batch());
    task.raft_wb = Some(engines.raft.log_batch(0));
}

/// Help function for less code
/// Option must not be none
fn put_kv(wb: Option<&mut TestKvWriteBatch>, key: &[u8], value: &[u8]) {
    wb.unwrap().put(key, value).unwrap();
}

/// Help function for less code
/// Option must not be none
fn delete_kv(wb: Option<&mut TestKvWriteBatch>, key: &[u8]) {
    wb.unwrap().delete(key).unwrap();
}

/// Simulate kv puts on raft engine.
fn put_raft_kv(wb: Option<&mut TestRaftLogBatch>, key: u64) {
    wb.unwrap()
        .append(key, None, vec![new_entry(key, key)])
        .unwrap();
}

fn delete_raft_kv(engine: &RaftTestEngine, wb: Option<&mut TestRaftLogBatch>, key: u64) {
    engine
        .clean(key, key, &new_raft_state(key, key, key, key), wb.unwrap())
        .unwrap();
}

/// Returns whether the key exists.
fn test_raft_kv(engine: &RaftTestEngine, key: u64) -> bool {
    if let Some(entry) = engine.get_entry(key, key).unwrap() {
        assert_eq!(entry, new_entry(key, key));
        true
    } else {
        false
    }
}

struct TestWorker {
    worker: Worker<KvTestEngine, RaftTestEngine, TestNotifier, TestTransport>,
    msg_rx: Receiver<RaftMessage>,
    notify_rx: Receiver<(u64, (u64, u64))>,
}

impl TestWorker {
    fn new(cfg: &Config, engines: &Engines<KvTestEngine, RaftTestEngine>) -> Self {
        let (_, task_rx) = resource_control::channel::unbounded(None);
        let (msg_tx, msg_rx) = unbounded();
        let trans = TestTransport { tx: msg_tx };
        let (notify_tx, notify_rx) = unbounded();
        let notifier = TestNotifier { tx: notify_tx };
        Self {
            worker: Worker::new(
                1,
                "writer".to_string(),
                engines.raft.clone(),
                Some(engines.kv.clone()),
                task_rx,
                notifier,
                trans,
                &Arc::new(VersionTrack::new(cfg.clone())),
            ),
            msg_rx,
            notify_rx,
        }
    }
}

struct TestWriters {
    writers: StoreWriters<KvTestEngine, RaftTestEngine>,
    msg_rx: Receiver<RaftMessage>,
    notify_rx: Receiver<(u64, (u64, u64))>,
    ctx: TestContext,
}

impl TestWriters {
    fn new(
        cfg: Config,
        engines: &Engines<KvTestEngine, RaftTestEngine>,
        resource_manager: Option<Arc<ResourceGroupManager>>,
    ) -> Self {
        let (msg_tx, msg_rx) = unbounded();
        let trans = TestTransport { tx: msg_tx };
        let (notify_tx, notify_rx) = unbounded();
        let notifier = TestNotifier { tx: notify_tx };
        let mut writers = StoreWriters::new(
            resource_manager
                .as_ref()
                .map(|m| m.derive_controller("test".into(), false)),
        );
        writers
            .spawn(
                1,
                engines.raft.clone(),
                Some(engines.kv.clone()),
                &notifier,
                &trans,
                &Arc::new(VersionTrack::new(cfg.clone())),
            )
            .unwrap();
        Self {
            msg_rx,
            notify_rx,
            ctx: TestContext {
                config: cfg,
                raft_metrics: RaftMetrics::new(true),
                senders: writers.senders(),
            },
            writers,
        }
    }

    fn write_sender(
        &self,
        id: usize,
    ) -> resource_control::channel::Sender<WriteMsg<KvTestEngine, RaftTestEngine>> {
        self.writers.senders()[id].clone()
    }
}

#[test]
fn test_write_task_batch_recorder() {
    let mut recorder = WriteTaskBatchRecorder::new(1024, Duration::from_nanos(50)); // 1kb, 50 nanoseconds
    assert_eq!(recorder.wait_duration_adaptive, Duration::from_nanos(50));
    let capped = WriteTaskBatchRecorder::new(1024, Duration::from_micros(80));
    assert_eq!(
        capped.wait_duration_adaptive,
        Duration::from_nanos(RAFT_WB_WAIT_DURATION_DEFAULT_NS)
    );
    assert_eq!(recorder.get_avg(), 0);
    assert_eq!(recorder.get_trend(), 1.0);
    assert!(!recorder.should_wait(4096));
    assert!(recorder.should_wait(512));
    // [512 ...]
    for _ in 0..30 {
        recorder.record(512);
    }
    assert_eq!(recorder.get_avg(), 512);
    assert_eq!(recorder.get_trend(), 0.5);
    assert!(recorder.should_wait(128));
    let expected_wait = Duration::from_nanos(80_000);
    recorder.update_wait_duration_adaptive(expected_wait);
    let start = Instant::now();
    recorder.wait_for_a_while(true);
    assert_eq!(recorder.wait_duration, expected_wait);
    assert_eq!(recorder.get_trend(), 1.0);
    assert!(start.saturating_elapsed() >= expected_wait);
    // [4096 ...]
    for _ in 0..30 {
        recorder.record(4096);
    }
    assert_eq!(recorder.get_avg(), 4096);
    assert_eq!(recorder.get_trend(), 2.0);
    assert!(!recorder.should_wait(128));
    recorder.reset_wait_count();
    assert!(recorder.should_wait(128));
    let expected_wait = Duration::from_nanos(60_000);
    recorder.update_wait_duration_adaptive(expected_wait);
    let start = Instant::now();
    recorder.wait_for_a_while(true);
    assert_eq!(recorder.wait_duration, expected_wait);
    assert_eq!(recorder.get_trend(), 1.0);
    assert!(start.saturating_elapsed() >= expected_wait);
}

#[test]
fn test_calculate_adaptive_wait_duration_clamps_to_bounds() {
    let path = Builder::new()
        .prefix("async-io-adaptive-clamp")
        .tempdir()
        .unwrap();
    let engines = new_temp_engine(&path);
    let mut cfg = Config::default();
    cfg.adaptive_batch_enabled = true;
    let mut t = TestWorker::new(&cfg, &engines);

    // Low QPS + high batch ratio should clamp to lower bound (10us).
    t.worker.batch.recorder.wait_duration_hint = Duration::from_nanos(20_000);
    t.worker.batch.recorder.wait_duration_adaptive =
        Duration::from_nanos(RAFT_WB_WAIT_DURATION_LOWER_BOUND_NS);
    t.worker.batch.recorder.batch_size_hint = 100;
    t.worker.batch.recorder.avg = 100;
    t.worker.consecutive_low_batch_ratio_count = 0;
    let lower = t.worker.calculate_adaptive_wait_duration(1_000);
    assert_eq!(
        lower,
        Duration::from_nanos(RAFT_WB_WAIT_DURATION_LOWER_BOUND_NS)
    );

    // High QPS + poor batching at upper bound should clamp to upper bound (1ms).
    t.worker.batch.recorder.wait_duration_adaptive =
        Duration::from_nanos(RAFT_WB_WAIT_DURATION_UPPER_BOUND_NS);
    t.worker.batch.recorder.batch_size_hint = 1_000;
    t.worker.batch.recorder.avg = 100;
    t.worker.consecutive_low_batch_ratio_count = 0;
    let upper = t
        .worker
        .calculate_adaptive_wait_duration(ADAPTIVE_DEFAULT_HIGH_QPS_THRESHOLD * 2);
    assert_eq!(
        upper,
        Duration::from_nanos(RAFT_WB_WAIT_DURATION_UPPER_BOUND_NS)
    );
}

#[test]
fn test_calculate_adaptive_wait_duration_high_and_low_qps_paths() {
    let path = Builder::new()
        .prefix("async-io-adaptive-qps")
        .tempdir()
        .unwrap();
    let engines = new_temp_engine(&path);
    let mut cfg = Config::default();
    cfg.adaptive_batch_enabled = true;
    let mut t = TestWorker::new(&cfg, &engines);

    // Keep batch ratio high (0.9) and only vary QPS pressure.
    // current=15us < hint=20us, so high QPS should grow and low QPS should not.
    t.worker.batch.recorder.wait_duration_hint = Duration::from_nanos(20_000);
    t.worker.batch.recorder.wait_duration_adaptive = Duration::from_nanos(15_000);
    t.worker.batch.recorder.batch_size_hint = 1_000;
    t.worker.batch.recorder.avg = 900;
    t.worker.consecutive_low_batch_ratio_count = 0;
    // Low QPS (1000) => qps_pressure=0.025 < 0.1 => very low QPS => target=current*0.7
    let low_qps = t.worker.calculate_adaptive_wait_duration(1_000);
    assert!(low_qps < Duration::from_nanos(15_000)); // reduced from 15000
    assert!(low_qps >= Duration::from_nanos(RAFT_WB_WAIT_DURATION_LOWER_BOUND_NS));

    // Reset state for high QPS test
    t.worker.batch.recorder.wait_duration_adaptive = Duration::from_nanos(15_000);
    t.worker.consecutive_low_batch_ratio_count = 0;
    // High QPS (40000) => qps_pressure=1.0 > 0.5 && current < hint => grow
    let high_qps = t
        .worker
        .calculate_adaptive_wait_duration(ADAPTIVE_DEFAULT_HIGH_QPS_THRESHOLD);
    assert!(high_qps > Duration::from_nanos(15_000)); // grew from 15000
    assert!(high_qps > low_qps); // directional check
}

#[test]
fn test_calculate_adaptive_wait_duration_futile_wait_reduction() {
    let path = Builder::new()
        .prefix("async-io-adaptive-futile")
        .tempdir()
        .unwrap();
    let engines = new_temp_engine(&path);
    let mut cfg = Config::default();
    cfg.adaptive_batch_enabled = true;
    let mut t = TestWorker::new(&cfg, &engines);

    // Setup: current=100us >= hint=20us, batch_ratio=0.1 (poor), QPS=5000
    // qps_pressure = 5000/40000 = 0.125 <= 0.5 => futile gate satisfied
    // After this call, consecutive_low_batch_ratio_count becomes 2 => futile triggered
    t.worker.batch.recorder.wait_duration_hint = Duration::from_nanos(20_000);
    t.worker.batch.recorder.wait_duration_adaptive = Duration::from_nanos(100_000);
    t.worker.batch.recorder.batch_size_hint = 1_000;
    t.worker.batch.recorder.avg = 100;
    t.worker.consecutive_low_batch_ratio_count = 1;
    let new_wait = t.worker.calculate_adaptive_wait_duration(5_000);
    assert_eq!(t.worker.consecutive_low_batch_ratio_count, 2);
    // Futile: target=100000*0.6=60000, smoothed=(0.7*100000+0.3*60000)=88000
    assert!(new_wait < Duration::from_nanos(100_000)); // reduced
    assert!(new_wait > Duration::from_nanos(70_000)); // not too aggressive
}

#[test]
fn test_adaptive_config_rejects_zero_threshold() {
    use tikv_util::config::ReadableSize;

    let split_size = crate::coprocessor::config::SPLIT_SIZE;

    // Layer 1: Config::validate() rejects adaptive_high_qps_threshold == 0 at startup.
    let mut cfg = Config::default();
    cfg.adaptive_high_qps_threshold = 0;
    let err = cfg
        .validate(split_size, false, ReadableSize(0), false)
        .unwrap_err();
    assert!(
        format!("{}", err).contains("adaptive-high-qps-threshold"),
        "expected threshold validation error, got: {}",
        err
    );

    // Layer 2: Hot-update to 0 — VersionTrack accepts it (validate() is not
    // called on hot-update path), but the Worker's runtime .max(1) guard
    // prevents division by zero. Verify the Worker picks up the new value
    // and still functions correctly.
    let path = Builder::new()
        .prefix("async-io-adaptive-hotupdate")
        .tempdir()
        .unwrap();
    let engines = new_temp_engine(&path);
    let mut cfg = Config::default();
    cfg.adaptive_batch_enabled = true;
    cfg.adaptive_high_qps_threshold = 40_000;
    let cfg_track = Arc::new(VersionTrack::new(cfg));
    let (_, task_rx) = resource_control::channel::unbounded(None);
    let (msg_tx, _msg_rx) = unbounded();
    let trans = TestTransport { tx: msg_tx };
    let (notify_tx, _notify_rx) = unbounded();
    let notifier = TestNotifier { tx: notify_tx };
    let mut worker = Worker::new(
        1,
        "writer".to_string(),
        engines.raft.clone(),
        Some(engines.kv.clone()),
        task_rx,
        notifier,
        trans,
        &cfg_track,
    );
    assert_eq!(worker.adaptive_high_qps_threshold, 40_000);

    // Simulate hot-update: push threshold to 0 via VersionTrack
    cfg_track
        .update(|cfg: &mut Config| -> std::result::Result<(), Box<dyn std::error::Error>> {
            cfg.adaptive_high_qps_threshold = 0;
            Ok(())
        })
        .unwrap();

    // Worker picks up the change through cfg_tracker (same path as write_to_db)
    if let Some(incoming) = worker.cfg_tracker.any_new() {
        worker.adaptive_high_qps_threshold = incoming.adaptive_high_qps_threshold;
    }
    assert_eq!(worker.adaptive_high_qps_threshold, 0);

    // Runtime .max(1) guard: algorithm still works without panic.
    worker.batch.recorder.wait_duration_hint = Duration::from_nanos(20_000);
    worker.batch.recorder.wait_duration_adaptive = Duration::from_nanos(15_000);
    worker.batch.recorder.batch_size_hint = 1_000;
    worker.batch.recorder.avg = 500;
    worker.consecutive_low_batch_ratio_count = 0;
    let result = worker.calculate_adaptive_wait_duration(1_000);
    // With threshold=0, high_qps=max(0,1)=1, qps_pressure=min(1000/1,1.0)=1.0
    // => "always high pressure", grows toward hint
    assert!(result > Duration::from_nanos(15_000));
}

#[test]
fn test_adaptive_worker_zero_threshold_no_panic() {
    // Directly set worker.adaptive_high_qps_threshold = 0 (bypassing config).
    // .max(1) ensures no division by zero; behaves as "always high pressure"
    // (qps_pressure = avg_qps / 1, clamped to 1.0).
    let path = Builder::new()
        .prefix("async-io-adaptive-zero")
        .tempdir()
        .unwrap();
    let engines = new_temp_engine(&path);
    let mut cfg = Config::default();
    cfg.adaptive_batch_enabled = true;
    let mut t = TestWorker::new(&cfg, &engines);

    // Setup: threshold=0, qps=1000, current=15us, hint=20us
    t.worker.adaptive_high_qps_threshold = 0;
    t.worker.batch.recorder.wait_duration_hint = Duration::from_nanos(20_000);
    t.worker.batch.recorder.wait_duration_adaptive = Duration::from_nanos(15_000);
    t.worker.batch.recorder.batch_size_hint = 1_000;
    t.worker.batch.recorder.avg = 500;
    t.worker.consecutive_low_batch_ratio_count = 0;

    // high_qps = max(0, 1) = 1, qps_pressure = min(1000/1, 1.0) = 1.0
    // => Branch 2 (qps_pressure > 0.5 && current < hint), grows toward hint. No panic.
    let result = t.worker.calculate_adaptive_wait_duration(1_000);
    assert!(result > Duration::from_nanos(15_000)); // should grow
    assert!(result <= Duration::from_nanos(RAFT_WB_WAIT_DURATION_UPPER_BOUND_NS));
}

#[test]
fn test_adaptive_no_oscillation_high_qps_low_batch_at_hint() {
    // High QPS + low batch_ratio + at hint: should NOT trigger futile,
    // should hold steady (no oscillation).
    let path = Builder::new()
        .prefix("async-io-adaptive-oscillation")
        .tempdir()
        .unwrap();
    let engines = new_temp_engine(&path);
    let mut cfg = Config::default();
    cfg.adaptive_batch_enabled = true;
    let mut t = TestWorker::new(&cfg, &engines);

    // Setup: qps=50000, batch_ratio=0.1, current=hint=20us
    t.worker.batch.recorder.wait_duration_hint = Duration::from_nanos(20_000);
    t.worker.batch.recorder.wait_duration_adaptive = Duration::from_nanos(20_000);
    t.worker.batch.recorder.batch_size_hint = 1_000;
    t.worker.batch.recorder.avg = 100; // batch_ratio = 0.1
    t.worker.consecutive_low_batch_ratio_count = 5; // many consecutive low periods

    // qps_pressure = 50000/40000 = 1.0 > 0.5 => futile blocked (even though consecutive_low >= 2)
    // Branch 2: qps_pressure > 0.5 && current < hint? No (current == hint)
    // Branch 5: hold steady => target = current
    let result = t.worker.calculate_adaptive_wait_duration(50_000);
    // Should stay near hint, not drop significantly
    assert!(
        result >= Duration::from_nanos(18_000),
        "should stay near hint, got {:?}",
        result
    );
    assert!(
        result <= Duration::from_nanos(22_000),
        "should not grow much above hint, got {:?}",
        result
    );
}

#[test]
fn test_worker() {
    let region_1 = 1;
    let region_2 = 2;

    let path = Builder::new().prefix("async-io-worker").tempdir().unwrap();
    let engines = new_temp_engine(&path);
    let mut t = TestWorker::new(&Config::default(), &engines);

    let mut task_1 = WriteTask::<KvTestEngine, RaftTestEngine>::new(region_1, 1, 10);
    init_write_batch(&engines, &mut task_1);
    put_kv(task_1.extra_write.v1_mut(), b"kv_k1", b"kv_v1");
    put_raft_kv(task_1.raft_wb.as_mut(), 17);
    task_1.entries.append(&mut vec![
        new_entry(5, 5),
        new_entry(6, 5),
        new_entry(7, 5),
        new_entry(8, 5),
    ]);
    task_1.raft_state = Some(new_raft_state(5, 123, 6, 8));
    task_1.messages.append(&mut vec![RaftMessage::default()]);

    t.worker.batch.add_write_task(&engines.raft, task_1);

    let mut task_2 = WriteTask::<KvTestEngine, RaftTestEngine>::new(region_2, 2, 15);
    init_write_batch(&engines, &mut task_2);
    put_kv(task_2.extra_write.v1_mut(), b"kv_k2", b"kv_v2");
    put_raft_kv(task_2.raft_wb.as_mut(), 27);
    task_2
        .entries
        .append(&mut vec![new_entry(20, 15), new_entry(21, 15)]);
    task_2.raft_state = Some(new_raft_state(15, 234, 20, 21));
    task_2
        .messages
        .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

    t.worker.batch.add_write_task(&engines.raft, task_2);

    let mut task_3 = WriteTask::<KvTestEngine, RaftTestEngine>::new(region_1, 1, 11);
    init_write_batch(&engines, &mut task_3);
    put_kv(task_3.extra_write.v1_mut(), b"kv_k3", b"kv_v3");
    put_raft_kv(task_3.raft_wb.as_mut(), 37);
    delete_raft_kv(&engines.raft, task_3.raft_wb.as_mut(), 17);
    task_3.set_append(Some(9), vec![new_entry(6, 6), new_entry(7, 7)]);
    task_3.raft_state = Some(new_raft_state(7, 124, 6, 7));
    task_3
        .messages
        .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

    t.worker.batch.add_write_task(&engines.raft, task_3);

    t.worker.write_to_db(true);

    let snapshot = engines.kv.snapshot();
    assert_eq!(snapshot.get_value(b"kv_k1").unwrap().unwrap(), b"kv_v1");
    assert_eq!(snapshot.get_value(b"kv_k2").unwrap().unwrap(), b"kv_v2");
    assert_eq!(snapshot.get_value(b"kv_k3").unwrap().unwrap(), b"kv_v3");

    must_have_same_notifies(vec![(region_1, (1, 11)), (region_2, (2, 15))], &t.notify_rx);

    assert_eq!(test_raft_kv(&engines.raft, 17), false);
    assert_eq!(test_raft_kv(&engines.raft, 27), true);
    assert_eq!(test_raft_kv(&engines.raft, 37), true);

    must_have_entries_and_state(
        &engines.raft,
        vec![
            (
                region_1,
                vec![new_entry(5, 5), new_entry(6, 6), new_entry(7, 7)],
                new_raft_state(7, 124, 6, 7),
            ),
            (
                region_2,
                vec![new_entry(20, 15), new_entry(21, 15)],
                new_raft_state(15, 234, 20, 21),
            ),
        ],
    );

    must_have_same_count_msg(5, &t.msg_rx);
}

#[test]
fn test_worker_split_raft_wb() {
    let path = Builder::new().prefix("async-io-worker").tempdir().unwrap();
    let engines = new_temp_engine(&path);
    let mut t = TestWorker::new(&Config::default(), &engines);

    let mut run_test = |region_1: u64, region_2: u64, split: (bool, bool)| {
        let raft_key_1 = 17 + region_1;
        let raft_key_2 = 27 + region_1;
        let raft_key_3 = 37 + region_1;
        let mut expected_wbs = 1;

        let mut task_1 = WriteTask::<KvTestEngine, RaftTestEngine>::new(region_1, 1, 10);
        task_1.raft_wb = Some(engines.raft.log_batch(0));
        let mut apply_state_1 = RaftApplyState::default();
        apply_state_1.set_applied_index(10);
        let lb = task_1.extra_write.ensure_v2(|| engines.raft.log_batch(0));
        lb.put_apply_state(region_1, 10, &apply_state_1).unwrap();
        put_raft_kv(task_1.raft_wb.as_mut(), raft_key_1);
        task_1.entries.append(&mut vec![
            new_entry(5, 5),
            new_entry(6, 5),
            new_entry(7, 5),
            new_entry(8, 5),
        ]);
        task_1.raft_state = Some(new_raft_state(5, 123, 6, 8));
        t.worker.batch.add_write_task(&engines.raft, task_1);

        let mut task_2 = WriteTask::<KvTestEngine, RaftTestEngine>::new(region_2, 2, 15);
        task_2.raft_wb = Some(engines.raft.log_batch(0));
        let mut apply_state_2 = RaftApplyState::default();
        apply_state_2.set_applied_index(16);
        let lb = task_2.extra_write.ensure_v2(|| engines.raft.log_batch(0));
        lb.put_apply_state(region_2, 16, &apply_state_2).unwrap();
        put_raft_kv(task_2.raft_wb.as_mut(), raft_key_2);
        task_2
            .entries
            .append(&mut vec![new_entry(20, 15), new_entry(21, 15)]);
        task_2.raft_state = Some(new_raft_state(15, 234, 20, 21));
        if split.0 {
            expected_wbs += 1;
            t.worker.batch.raft_wb_split_size = 1;
        } else {
            t.worker.batch.raft_wb_split_size = 0;
        }
        t.worker.batch.add_write_task(&engines.raft, task_2);

        let mut task_3 = WriteTask::<KvTestEngine, RaftTestEngine>::new(region_1, 1, 11);
        task_3.raft_wb = Some(engines.raft.log_batch(0));
        let mut apply_state_3 = RaftApplyState::default();
        apply_state_3.set_applied_index(25);
        let lb = task_3.extra_write.ensure_v2(|| engines.raft.log_batch(0));
        lb.put_apply_state(region_1, 25, &apply_state_3).unwrap();
        put_raft_kv(task_3.raft_wb.as_mut(), raft_key_3);
        delete_raft_kv(&engines.raft, task_3.raft_wb.as_mut(), raft_key_1);
        task_3.set_append(Some(9), vec![new_entry(6, 6), new_entry(7, 7)]);
        task_3.raft_state = Some(new_raft_state(7, 124, 6, 7));
        if split.1 {
            expected_wbs += 1;
            t.worker.batch.raft_wb_split_size = 1;
        } else {
            t.worker.batch.raft_wb_split_size = 0;
        }
        t.worker.batch.add_write_task(&engines.raft, task_3);

        assert_eq!(t.worker.batch.raft_wbs.len(), expected_wbs);
        t.worker.write_to_db(true);
        assert_eq!(t.worker.batch.raft_wbs.len(), 1);

        must_have_same_notifies(vec![(region_1, (1, 11)), (region_2, (2, 15))], &t.notify_rx);

        assert_eq!(test_raft_kv(&engines.raft, raft_key_1), false);
        assert_eq!(test_raft_kv(&engines.raft, raft_key_2), true);
        assert_eq!(test_raft_kv(&engines.raft, raft_key_3), true);

        must_have_entries_and_state(
            &engines.raft,
            vec![
                (
                    region_1,
                    vec![new_entry(5, 5), new_entry(6, 6), new_entry(7, 7)],
                    new_raft_state(7, 124, 6, 7),
                ),
                (
                    region_2,
                    vec![new_entry(20, 15), new_entry(21, 15)],
                    new_raft_state(15, 234, 20, 21),
                ),
            ],
        );
        assert_eq!(
            engines.raft.get_apply_state(region_1, 25).unwrap(),
            Some(RaftApplyState {
                applied_index: 25,
                ..Default::default()
            })
        );
        assert_eq!(
            engines.raft.get_apply_state(region_2, 16).unwrap(),
            Some(RaftApplyState {
                applied_index: 16,
                ..Default::default()
            })
        );
    };

    let mut first_region = 1;
    for a in [true, false] {
        for b in [true, false] {
            run_test(first_region, first_region + 1, (a, b));
            first_region += 10;
        }
    }
}

#[test]
fn test_basic_flow() {
    let region_1 = 1;
    let region_2 = 2;

    let path = Builder::new().prefix("async-io-basic").tempdir().unwrap();
    let engines = new_temp_engine(&path);
    let mut cfg = Config::default();
    cfg.store_io_pool_size = 2;
    let mut t = TestWriters::new(cfg, &engines, None);

    let mut task_1 = WriteTask::<KvTestEngine, RaftTestEngine>::new(region_1, 1, 10);
    init_write_batch(&engines, &mut task_1);
    put_kv(task_1.extra_write.v1_mut(), b"kv_k1", b"kv_v1");
    put_raft_kv(task_1.raft_wb.as_mut(), 17);
    task_1
        .entries
        .append(&mut vec![new_entry(5, 5), new_entry(6, 5), new_entry(7, 5)]);
    task_1.raft_state = Some(new_raft_state(5, 234, 6, 7));
    task_1
        .messages
        .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

    t.write_sender(0)
        .send(WriteMsg::WriteTask(task_1), None)
        .unwrap();

    let mut task_2 = WriteTask::<KvTestEngine, RaftTestEngine>::new(2, 2, 20);
    init_write_batch(&engines, &mut task_2);
    put_kv(task_2.extra_write.v1_mut(), b"kv_k2", b"kv_v2");
    put_raft_kv(task_2.raft_wb.as_mut(), 27);
    task_2
        .entries
        .append(&mut vec![new_entry(50, 12), new_entry(51, 13)]);
    task_2.raft_state = Some(new_raft_state(13, 567, 49, 51));
    task_2
        .messages
        .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

    t.write_sender(1)
        .send(WriteMsg::WriteTask(task_2), None)
        .unwrap();

    let mut task_3 = WriteTask::<KvTestEngine, RaftTestEngine>::new(region_1, 1, 15);
    init_write_batch(&engines, &mut task_3);
    put_kv(task_3.extra_write.v1_mut(), b"kv_k3", b"kv_v3");
    delete_kv(task_3.extra_write.v1_mut(), b"kv_k1");
    put_raft_kv(task_3.raft_wb.as_mut(), 37);
    delete_raft_kv(&engines.raft, task_3.raft_wb.as_mut(), 17);
    task_3.set_append(Some(8), vec![new_entry(6, 6)]);
    task_3.raft_state = Some(new_raft_state(6, 345, 6, 6));
    task_3
        .messages
        .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

    t.write_sender(0)
        .send(WriteMsg::WriteTask(task_3), None)
        .unwrap();

    must_wait_same_notifies(vec![(region_1, (1, 15)), (region_2, (2, 20))], &t.notify_rx);

    let snapshot = engines.kv.snapshot();
    assert!(snapshot.get_value(b"kv_k1").unwrap().is_none());
    assert_eq!(snapshot.get_value(b"kv_k2").unwrap().unwrap(), b"kv_v2");
    assert_eq!(snapshot.get_value(b"kv_k3").unwrap().unwrap(), b"kv_v3");

    assert_eq!(test_raft_kv(&engines.raft, 17), false);
    assert_eq!(test_raft_kv(&engines.raft, 27), true);
    assert_eq!(test_raft_kv(&engines.raft, 37), true);

    must_have_entries_and_state(
        &engines.raft,
        vec![
            (
                region_1,
                vec![new_entry(5, 5), new_entry(6, 6)],
                new_raft_state(6, 345, 6, 6),
            ),
            (
                region_2,
                vec![new_entry(50, 12), new_entry(51, 13)],
                new_raft_state(13, 567, 49, 51),
            ),
        ],
    );

    must_have_same_count_msg(6, &t.msg_rx);
    t.writers.shutdown();
}

#[test]
fn test_basic_flow_with_states() {
    let region_1 = 1;
    let region_2 = 2;

    let path = Builder::new()
        .prefix("async-io-basic-states")
        .tempdir()
        .unwrap();
    let engines = new_temp_engine(&path);
    let mut cfg = Config::default();
    cfg.store_io_pool_size = 2;
    let mut t = TestWriters::new(cfg, &engines, None);

    let mut task_1 = WriteTask::<KvTestEngine, RaftTestEngine>::new(region_1, 1, 10);
    task_1.raft_wb = Some(engines.raft.log_batch(0));
    let mut apply_state_1 = RaftApplyState::default();
    apply_state_1.applied_index = 2;
    let mut region_state_1 = RegionLocalState::default();
    region_state_1
        .mut_region()
        .mut_region_epoch()
        .set_version(3);
    let lb = task_1.extra_write.ensure_v2(|| engines.raft.log_batch(0));
    lb.put_apply_state(region_1, 2, &apply_state_1).unwrap();
    lb.put_region_state(region_1, 2, &region_state_1).unwrap();
    put_raft_kv(task_1.raft_wb.as_mut(), 17);
    task_1
        .entries
        .append(&mut vec![new_entry(5, 5), new_entry(6, 5), new_entry(7, 5)]);
    task_1.raft_state = Some(new_raft_state(5, 234, 6, 7));
    task_1
        .messages
        .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

    t.write_sender(0)
        .send(WriteMsg::WriteTask(task_1), None)
        .unwrap();

    let mut task_2 = WriteTask::<KvTestEngine, RaftTestEngine>::new(2, 2, 20);
    task_2.raft_wb = Some(engines.raft.log_batch(0));
    let mut apply_state_2 = RaftApplyState::default();
    apply_state_2.applied_index = 30;
    let lb = task_2.extra_write.ensure_v2(|| engines.raft.log_batch(0));
    lb.put_apply_state(2, 30, &apply_state_2).unwrap();
    put_raft_kv(task_2.raft_wb.as_mut(), 27);
    task_2
        .entries
        .append(&mut vec![new_entry(50, 12), new_entry(51, 13)]);
    task_2.raft_state = Some(new_raft_state(13, 567, 49, 51));
    task_2
        .messages
        .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

    t.write_sender(1)
        .send(WriteMsg::WriteTask(task_2), None)
        .unwrap();

    let mut task_3 = WriteTask::<KvTestEngine, RaftTestEngine>::new(region_1, 1, 15);
    task_3.raft_wb = Some(engines.raft.log_batch(0));
    let mut apply_state_3 = RaftApplyState::default();
    apply_state_3.applied_index = 5;
    let lb = task_3.extra_write.ensure_v2(|| engines.raft.log_batch(0));
    lb.put_apply_state(region_1, 5, &apply_state_3).unwrap();
    put_raft_kv(task_3.raft_wb.as_mut(), 37);
    delete_raft_kv(&engines.raft, task_3.raft_wb.as_mut(), 17);
    task_3.set_append(Some(8), vec![new_entry(6, 6)]);
    task_3.raft_state = Some(new_raft_state(6, 345, 6, 6));
    task_3
        .messages
        .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

    t.write_sender(0)
        .send(WriteMsg::WriteTask(task_3), None)
        .unwrap();

    must_wait_same_notifies(vec![(region_1, (1, 15)), (region_2, (2, 20))], &t.notify_rx);

    assert_eq!(test_raft_kv(&engines.raft, 17), false);
    assert_eq!(test_raft_kv(&engines.raft, 27), true);
    assert_eq!(test_raft_kv(&engines.raft, 37), true);

    must_have_entries_and_state(
        &engines.raft,
        vec![
            (
                region_1,
                vec![new_entry(5, 5), new_entry(6, 6)],
                new_raft_state(6, 345, 6, 6),
            ),
            (
                region_2,
                vec![new_entry(50, 12), new_entry(51, 13)],
                new_raft_state(13, 567, 49, 51),
            ),
        ],
    );
    assert_eq!(
        engines.raft.get_apply_state(region_1, 5).unwrap().unwrap(),
        apply_state_3
    );
    assert_eq!(
        engines.raft.get_apply_state(region_2, 30).unwrap().unwrap(),
        apply_state_2
    );
    assert_eq!(
        engines.raft.get_region_state(region_1, 2).unwrap().unwrap(),
        region_state_1
    );
    assert_eq!(engines.raft.get_region_state(region_2, 1).unwrap(), None);

    must_have_same_count_msg(6, &t.msg_rx);

    t.writers.shutdown();
}

#[test]
fn test_resource_group() {
    let region_1 = 1;
    let region_2 = 2;

    let resource_manager = Arc::new(ResourceGroupManager::default());
    let get_group = |name: &str, read_tokens: u64, write_tokens: u64| -> ResourceGroup {
        let mut group = ResourceGroup::new();
        group.set_name(name.to_string());
        group.set_mode(GroupMode::RawMode);
        let mut resource_setting = GroupRawResourceSettings::new();
        resource_setting
            .mut_cpu()
            .mut_settings()
            .set_fill_rate(read_tokens);
        resource_setting
            .mut_io_write()
            .mut_settings()
            .set_fill_rate(write_tokens);
        group.set_raw_resource_settings(resource_setting);
        group
    };
    resource_manager.add_resource_group(get_group("group1", 10, 10));
    resource_manager.add_resource_group(get_group("group2", 100, 100));

    let path = Builder::new().prefix("async-io-basic").tempdir().unwrap();
    let engines = new_temp_engine(&path);
    let mut cfg = Config::default();
    cfg.store_io_pool_size = 1;

    let mut t = TestWriters::new(cfg, &engines, Some(resource_manager));

    let (tx, rx) = mpsc::sync_channel(0);
    t.write_sender(0).send(WriteMsg::Pause(rx), None).unwrap();

    let mut r = WriteRouter::new("1".to_string());
    let mut task_1 = WriteTask::<KvTestEngine, RaftTestEngine>::new(region_1, 1, 10);
    init_write_batch(&engines, &mut task_1);
    put_raft_kv(task_1.raft_wb.as_mut(), 17);
    let entries = vec![new_entry(5, 5), new_entry(6, 5), new_entry(7, 5)];
    let mut entries = entries
        .into_iter()
        .map(|mut e| {
            let mut req = RaftCmdRequest::default();
            let mut header = RaftRequestHeader::default();
            header.set_resource_group_name("group1".to_owned());
            req.set_header(header);
            e.set_data(req.write_to_bytes().unwrap().into());
            e
        })
        .collect();
    task_1.entries.append(&mut entries);
    task_1.raft_state = Some(new_raft_state(5, 234, 6, 7));
    task_1
        .messages
        .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);
    r.send_write_msg(&mut t.ctx, None, WriteMsg::WriteTask(task_1));

    let mut r = WriteRouter::new("2".to_string());
    let mut task_2 = WriteTask::<KvTestEngine, RaftTestEngine>::new(region_2, 2, 20);
    init_write_batch(&engines, &mut task_2);
    put_raft_kv(task_2.raft_wb.as_mut(), 27);
    let entries = vec![new_entry(50, 12), new_entry(51, 13)];
    let mut entries = entries
        .into_iter()
        .map(|mut e| {
            let mut req = RaftCmdRequest::default();
            let mut header = RaftRequestHeader::default();
            header.set_resource_group_name("group2".to_owned());
            req.set_header(header);
            e.set_data(req.write_to_bytes().unwrap().into());
            e
        })
        .collect();
    task_2.entries.append(&mut entries);
    task_2.raft_state = Some(new_raft_state(13, 567, 49, 51));
    task_2
        .messages
        .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);
    r.send_write_msg(&mut t.ctx, None, WriteMsg::WriteTask(task_2));

    tx.send(()).unwrap();
    must_wait_same_notifies(vec![(region_1, (1, 10)), (region_2, (2, 20))], &t.notify_rx);
}
