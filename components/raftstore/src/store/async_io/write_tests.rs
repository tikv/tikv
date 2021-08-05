// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use crate::store::{Config, Transport};
use crate::Result;

use collections::HashSet;
use crossbeam::channel::unbounded;
use engine_rocks::RocksWriteBatch;
use engine_test::kv::KvTestEngine;
use engine_test::new_temp_engine;
use engine_traits::{Mutable, Peekable, WriteBatchExt};
use kvproto::raft_serverpb::RaftMessage;
use tempfile::Builder;

use super::*;

fn must_have_entries_and_state(
    raft_engine: &KvTestEngine,
    entries_state: Vec<(u64, Vec<Entry>, RaftLocalState)>,
) {
    let snapshot = raft_engine.snapshot();
    for (region_id, entries, state) in entries_state {
        for e in entries {
            let key = keys::raft_log_key(region_id, e.get_index());
            let val = snapshot.get_msg::<Entry>(&key).unwrap().unwrap();
            assert_eq!(val, e);
        }
        let val = snapshot
            .get_msg::<RaftLocalState>(&keys::raft_state_key(region_id))
            .unwrap()
            .unwrap();
        assert_eq!(val, state);
        let key = keys::raft_log_key(region_id, state.get_last_index() + 1);
        // last_index + 1 entry should not exist
        assert!(snapshot.get_msg::<Entry>(&key).unwrap().is_none());
    }
}

fn new_entry(index: u64, term: u64) -> Entry {
    let mut e = Entry::default();
    e.set_index(index);
    e.set_term(term);
    e
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

impl Notifier for TestNotifier {
    fn notify_persisted(&self, region_id: u64, peer_id: u64, ready_number: u64, _now: Instant) {
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
    engines: &Engines<KvTestEngine, KvTestEngine>,
    task: &mut WriteTask<KvTestEngine, KvTestEngine>,
) {
    task.kv_wb = Some(engines.kv.write_batch());
    task.raft_wb = Some(engines.raft.write_batch());
}

/// Help function for less code
/// Option must not be none
fn put_kv(wb: &mut Option<RocksWriteBatch>, key: &[u8], value: &[u8]) {
    wb.as_mut().unwrap().put(key, value).unwrap();
}

/// Help function for less code
/// Option must not be none
fn delete_kv(wb: &mut Option<RocksWriteBatch>, key: &[u8]) {
    wb.as_mut().unwrap().delete(key).unwrap();
}

struct TestWorker {
    worker: Worker<KvTestEngine, KvTestEngine, TestTransport, TestNotifier>,
    msg_rx: Receiver<RaftMessage>,
    notify_rx: Receiver<(u64, (u64, u64))>,
}

impl TestWorker {
    fn new(cfg: &Config, engines: &Engines<KvTestEngine, KvTestEngine>) -> Self {
        let (_, task_rx) = unbounded();
        let (msg_tx, msg_rx) = unbounded();
        let trans = TestTransport { tx: msg_tx };
        let (notify_tx, notify_rx) = unbounded();
        let notifier = TestNotifier { tx: notify_tx };
        Self {
            worker: Worker::new(
                1,
                "writer".to_string(),
                engines.clone(),
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
    writers: StoreWriters<KvTestEngine, KvTestEngine>,
    msg_rx: Receiver<RaftMessage>,
    notify_rx: Receiver<(u64, (u64, u64))>,
}

impl TestWriters {
    fn new(cfg: &Config, engines: &Engines<KvTestEngine, KvTestEngine>) -> Self {
        let (msg_tx, msg_rx) = unbounded();
        let trans = TestTransport { tx: msg_tx };
        let (notify_tx, notify_rx) = unbounded();
        let notifier = TestNotifier { tx: notify_tx };
        let mut writers = StoreWriters::new();
        writers
            .spawn(
                1,
                engines,
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

    fn write_sender(&self, id: usize) -> &Sender<WriteMsg<KvTestEngine, KvTestEngine>> {
        &self.writers.senders()[id]
    }
}

#[test]
fn test_worker() {
    let path = Builder::new().prefix("async-io-worker").tempdir().unwrap();
    let engines = new_temp_engine(&path);
    let mut t = TestWorker::new(&Config::default(), &engines);

    let mut task_1 = WriteTask::<KvTestEngine, KvTestEngine>::new(1, 1, 10);
    init_write_batch(&engines, &mut task_1);
    put_kv(&mut task_1.kv_wb, b"kv_k1", b"kv_v1");
    put_kv(&mut task_1.raft_wb, b"raft_k1", b"raft_v1");
    task_1.entries.append(&mut vec![
        new_entry(5, 5),
        new_entry(6, 5),
        new_entry(7, 5),
        new_entry(8, 5),
    ]);
    task_1.raft_state = Some(new_raft_state(5, 123, 6, 8));
    task_1.messages.append(&mut vec![RaftMessage::default()]);

    t.worker.batch.add_write_task(task_1);

    let mut task_2 = WriteTask::<KvTestEngine, KvTestEngine>::new(2, 2, 15);
    init_write_batch(&engines, &mut task_2);
    put_kv(&mut task_2.kv_wb, b"kv_k2", b"kv_v2");
    put_kv(&mut task_2.raft_wb, b"raft_k2", b"raft_v2");
    task_2
        .entries
        .append(&mut vec![new_entry(20, 15), new_entry(21, 15)]);
    task_2.raft_state = Some(new_raft_state(15, 234, 20, 21));
    task_2
        .messages
        .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

    t.worker.batch.add_write_task(task_2);

    let mut task_3 = WriteTask::<KvTestEngine, KvTestEngine>::new(1, 1, 11);
    init_write_batch(&engines, &mut task_3);
    put_kv(&mut task_3.kv_wb, b"kv_k3", b"kv_v3");
    put_kv(&mut task_3.raft_wb, b"raft_k3", b"raft_v3");
    delete_kv(&mut task_3.raft_wb, b"raft_k1");
    task_3
        .entries
        .append(&mut vec![new_entry(6, 6), new_entry(7, 7)]);
    task_3.cut_logs = Some((8, 9));
    task_3.raft_state = Some(new_raft_state(7, 124, 6, 7));
    task_3
        .messages
        .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

    t.worker.batch.add_write_task(task_3);

    t.worker.write_to_db();

    let snapshot = engines.kv.snapshot();
    assert_eq!(snapshot.get_value(b"kv_k1").unwrap().unwrap(), b"kv_v1");
    assert_eq!(snapshot.get_value(b"kv_k2").unwrap().unwrap(), b"kv_v2");
    assert_eq!(snapshot.get_value(b"kv_k3").unwrap().unwrap(), b"kv_v3");

    let snapshot = engines.raft.snapshot();
    assert!(snapshot.get_value(b"raft_k1").unwrap().is_none());
    assert_eq!(snapshot.get_value(b"raft_k2").unwrap().unwrap(), b"raft_v2");
    assert_eq!(snapshot.get_value(b"raft_k3").unwrap().unwrap(), b"raft_v3");

    must_have_same_notifies(vec![(1, (1, 11)), (2, (2, 15))], &t.notify_rx);

    must_have_entries_and_state(
        &engines.raft,
        vec![
            (
                1,
                vec![new_entry(5, 5), new_entry(6, 6), new_entry(7, 7)],
                new_raft_state(7, 124, 6, 7),
            ),
            (
                2,
                vec![new_entry(20, 15), new_entry(21, 15)],
                new_raft_state(15, 234, 20, 21),
            ),
        ],
    );

    must_have_same_count_msg(5, &t.msg_rx);
}

#[test]
fn test_basic_flow() {
    let path = Builder::new().prefix("async-io-basic").tempdir().unwrap();
    let engines = new_temp_engine(&path);
    let mut cfg = Config::default();
    cfg.store_io_pool_size = 2;
    let mut t = TestWriters::new(&cfg, &engines);

    let mut task_1 = WriteTask::<KvTestEngine, KvTestEngine>::new(1, 1, 10);
    init_write_batch(&engines, &mut task_1);
    put_kv(&mut task_1.kv_wb, b"kv_k1", b"kv_v1");
    put_kv(&mut task_1.raft_wb, b"raft_k1", b"raft_v1");
    task_1
        .entries
        .append(&mut vec![new_entry(5, 5), new_entry(6, 5), new_entry(7, 5)]);
    task_1.raft_state = Some(new_raft_state(5, 234, 6, 7));
    task_1
        .messages
        .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

    t.write_sender(0).send(WriteMsg::WriteTask(task_1)).unwrap();

    let mut task_2 = WriteTask::<KvTestEngine, KvTestEngine>::new(2, 2, 20);
    init_write_batch(&engines, &mut task_2);
    put_kv(&mut task_2.kv_wb, b"kv_k2", b"kv_v2");
    put_kv(&mut task_2.raft_wb, b"raft_k2", b"raft_v2");
    task_2
        .entries
        .append(&mut vec![new_entry(50, 12), new_entry(51, 13)]);
    task_2.raft_state = Some(new_raft_state(13, 567, 49, 51));
    task_2
        .messages
        .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

    t.write_sender(1).send(WriteMsg::WriteTask(task_2)).unwrap();

    let mut task_3 = WriteTask::<KvTestEngine, KvTestEngine>::new(1, 1, 15);
    init_write_batch(&engines, &mut task_3);
    put_kv(&mut task_3.kv_wb, b"kv_k3", b"kv_v3");
    delete_kv(&mut task_3.kv_wb, b"kv_k1");
    put_kv(&mut task_3.raft_wb, b"raft_k3", b"raft_v3");
    delete_kv(&mut task_3.raft_wb, b"raft_k1");
    task_3.entries.append(&mut vec![new_entry(6, 6)]);
    task_3.cut_logs = Some((7, 8));
    task_3.raft_state = Some(new_raft_state(6, 345, 6, 6));
    task_3
        .messages
        .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

    t.write_sender(0).send(WriteMsg::WriteTask(task_3)).unwrap();

    must_wait_same_notifies(vec![(1, (1, 15)), (2, (2, 20))], &t.notify_rx);

    let snapshot = engines.kv.snapshot();
    assert!(snapshot.get_value(b"kv_k1").unwrap().is_none());
    assert_eq!(snapshot.get_value(b"kv_k2").unwrap().unwrap(), b"kv_v2");
    assert_eq!(snapshot.get_value(b"kv_k3").unwrap().unwrap(), b"kv_v3");

    let snapshot = engines.raft.snapshot();
    assert!(snapshot.get_value(b"raft_k1").unwrap().is_none());
    assert_eq!(snapshot.get_value(b"raft_k2").unwrap().unwrap(), b"raft_v2");
    assert_eq!(snapshot.get_value(b"raft_k3").unwrap().unwrap(), b"raft_v3");

    must_have_entries_and_state(
        &engines.raft,
        vec![
            (
                1,
                vec![new_entry(5, 5), new_entry(6, 6)],
                new_raft_state(6, 345, 6, 6),
            ),
            (
                2,
                vec![new_entry(50, 12), new_entry(51, 13)],
                new_raft_state(13, 567, 49, 51),
            ),
        ],
    );

    must_have_same_count_msg(6, &t.msg_rx);

    t.writers.shutdown();
}
