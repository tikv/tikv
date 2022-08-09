// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use collections::HashSet;
use crossbeam::channel::unbounded;
use engine_test::{kv::KvTestEngine, new_temp_engine, raft::RaftTestEngine};
use engine_traits::{Engines, Mutable, Peekable, RaftEngineReadOnly, WriteBatchExt};
use kvproto::raft_serverpb::RaftMessage;
use tempfile::Builder;

use super::*;
use crate::{
    store::{peer_storage::tests::new_entry, Config, Transport},
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
        match notify_rx.recv() {
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
    wb.unwrap().append(key, vec![new_entry(key, key)]).unwrap();
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
        let (_, task_rx) = unbounded();
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
}

impl TestWriters {
    fn new(cfg: &Config, engines: &Engines<KvTestEngine, RaftTestEngine>) -> Self {
        let (msg_tx, msg_rx) = unbounded();
        let trans = TestTransport { tx: msg_tx };
        let (notify_tx, notify_rx) = unbounded();
        let notifier = TestNotifier { tx: notify_tx };
        let mut writers = StoreWriters::default();
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
            writers,
            msg_rx,
            notify_rx,
        }
    }

    fn write_sender(&self, id: usize) -> Sender<WriteMsg<KvTestEngine, RaftTestEngine>> {
        self.writers.senders()[id].clone()
    }
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

    t.worker.batch.add_write_task(task_1);

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

    t.worker.batch.add_write_task(task_2);

    let mut task_3 = WriteTask::<KvTestEngine, RaftTestEngine>::new(region_1, 1, 11);
    init_write_batch(&engines, &mut task_3);
    put_kv(task_3.extra_write.v1_mut(), b"kv_k3", b"kv_v3");
    put_raft_kv(task_3.raft_wb.as_mut(), 37);
    delete_raft_kv(&engines.raft, task_3.raft_wb.as_mut(), 17);
    task_3
        .entries
        .append(&mut vec![new_entry(6, 6), new_entry(7, 7)]);
    task_3.cut_logs = Some((8, 9));
    task_3.raft_state = Some(new_raft_state(7, 124, 6, 7));
    task_3
        .messages
        .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

    t.worker.batch.add_write_task(task_3);

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
fn test_basic_flow() {
    let region_1 = 1;
    let region_2 = 2;

    let path = Builder::new().prefix("async-io-basic").tempdir().unwrap();
    let engines = new_temp_engine(&path);
    let mut cfg = Config::default();
    cfg.store_io_pool_size = 2;
    let mut t = TestWriters::new(&cfg, &engines);

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

    t.write_sender(0).send(WriteMsg::WriteTask(task_1)).unwrap();

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

    t.write_sender(1).send(WriteMsg::WriteTask(task_2)).unwrap();

    let mut task_3 = WriteTask::<KvTestEngine, RaftTestEngine>::new(region_1, 1, 15);
    init_write_batch(&engines, &mut task_3);
    put_kv(task_3.extra_write.v1_mut(), b"kv_k3", b"kv_v3");
    delete_kv(task_3.extra_write.v1_mut(), b"kv_k1");
    put_raft_kv(task_3.raft_wb.as_mut(), 37);
    delete_raft_kv(&engines.raft, task_3.raft_wb.as_mut(), 17);
    task_3.entries.append(&mut vec![new_entry(6, 6)]);
    task_3.cut_logs = Some((7, 8));
    task_3.raft_state = Some(new_raft_state(6, 345, 6, 6));
    task_3
        .messages
        .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

    t.write_sender(0).send(WriteMsg::WriteTask(task_3)).unwrap();

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
    let mut t = TestWriters::new(&cfg, &engines);

    let mut task_1 = WriteTask::<KvTestEngine, RaftTestEngine>::new(region_1, 1, 10);
    task_1.raft_wb = Some(engines.raft.log_batch(0));
    let mut apply_state_1 = RaftApplyState::default();
    apply_state_1.applied_index = 2;
    let mut extra_state = ExtraStates::new(apply_state_1);
    let mut region_state_1 = RegionLocalState::default();
    region_state_1
        .mut_region()
        .mut_region_epoch()
        .set_version(3);
    extra_state.region_state = Some(region_state_1.clone());
    task_1.extra_write.set_v2(extra_state);
    put_raft_kv(task_1.raft_wb.as_mut(), 17);
    task_1
        .entries
        .append(&mut vec![new_entry(5, 5), new_entry(6, 5), new_entry(7, 5)]);
    task_1.raft_state = Some(new_raft_state(5, 234, 6, 7));
    task_1
        .messages
        .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

    t.write_sender(0).send(WriteMsg::WriteTask(task_1)).unwrap();

    let mut task_2 = WriteTask::<KvTestEngine, RaftTestEngine>::new(2, 2, 20);
    task_2.raft_wb = Some(engines.raft.log_batch(0));
    let mut apply_state_2 = RaftApplyState::default();
    apply_state_2.applied_index = 30;
    let extra_state = ExtraStates::new(apply_state_2.clone());
    task_2.extra_write.set_v2(extra_state);
    put_raft_kv(task_2.raft_wb.as_mut(), 27);
    task_2
        .entries
        .append(&mut vec![new_entry(50, 12), new_entry(51, 13)]);
    task_2.raft_state = Some(new_raft_state(13, 567, 49, 51));
    task_2
        .messages
        .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

    t.write_sender(1).send(WriteMsg::WriteTask(task_2)).unwrap();

    let mut task_3 = WriteTask::<KvTestEngine, RaftTestEngine>::new(region_1, 1, 15);
    task_3.raft_wb = Some(engines.raft.log_batch(0));
    let mut apply_state_3 = RaftApplyState::default();
    apply_state_3.applied_index = 5;
    let extra_state = ExtraStates::new(apply_state_3.clone());
    task_3.extra_write.set_v2(extra_state);
    put_raft_kv(task_3.raft_wb.as_mut(), 37);
    delete_raft_kv(&engines.raft, task_3.raft_wb.as_mut(), 17);
    task_3.entries.append(&mut vec![new_entry(6, 6)]);
    task_3.cut_logs = Some((7, 8));
    task_3.raft_state = Some(new_raft_state(6, 345, 6, 6));
    task_3
        .messages
        .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

    t.write_sender(0).send(WriteMsg::WriteTask(task_3)).unwrap();

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
        engines.raft.get_apply_state(region_1).unwrap().unwrap(),
        apply_state_3
    );
    assert_eq!(
        engines.raft.get_apply_state(region_2).unwrap().unwrap(),
        apply_state_2
    );
    assert_eq!(
        engines.raft.get_region_state(region_1).unwrap().unwrap(),
        region_state_1
    );
    assert_eq!(engines.raft.get_region_state(region_2).unwrap(), None);

    must_have_same_count_msg(6, &t.msg_rx);

    t.writers.shutdown();
}
