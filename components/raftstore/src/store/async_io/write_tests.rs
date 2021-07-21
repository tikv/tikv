// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use crate::store::{Config, Transport};
use crate::Result;
use engine_rocks::RocksWriteBatch;
use engine_test::kv::{new_engine, KvTestEngine};
use engine_traits::{Mutable, Peekable, WriteBatchExt, ALL_CFS};
use kvproto::raft_serverpb::RaftMessage;
use tempfile::{Builder, TempDir};

use super::*;

fn create_tmp_engine(path: &str) -> (TempDir, KvTestEngine) {
    let path = Builder::new().prefix(path).tempdir().unwrap();
    let engine = new_engine(
        path.path().join("db").to_str().unwrap(),
        None,
        ALL_CFS,
        None,
    )
    .unwrap();
    (path, engine)
}

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
    fn notify_unreachable(&self, _region_id: u64, _to_peer_id: u64) {
        panic!("unimplemented");
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

        if timer.elapsed() > Duration::from_secs(5) {
            panic!("wait some notifies after 3 seconds")
        }
        thread::sleep(Duration::from_millis(20));
    }
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

struct TestAsyncWriteWorker {
    worker: AsyncWriteWorker<KvTestEngine, KvTestEngine, TestTransport, TestNotifier>,
    msg_rx: Receiver<RaftMessage>,
    notify_rx: Receiver<(u64, (u64, u64))>,
}

impl TestAsyncWriteWorker {
    fn new(cfg: &Config, kv_engine: &KvTestEngine, raft_engine: &KvTestEngine) -> Self {
        let (_, task_rx) = channel();
        let (msg_tx, msg_rx) = channel();
        let trans = TestTransport { tx: msg_tx };
        let (notify_tx, notify_rx) = channel();
        let notifier = TestNotifier { tx: notify_tx };
        Self {
            worker: AsyncWriteWorker::new(
                1,
                "writer".to_string(),
                kv_engine.clone(),
                raft_engine.clone(),
                task_rx,
                notifier,
                trans,
                cfg,
            ),
            msg_rx,
            notify_rx,
        }
    }
}

struct TestAsyncWriters {
    writers: AsyncWriters<KvTestEngine, KvTestEngine>,
    msg_rx: Receiver<RaftMessage>,
    notify_rx: Receiver<(u64, (u64, u64))>,
}

impl TestAsyncWriters {
    fn new(cfg: &Config, kv_engine: &KvTestEngine, raft_engine: &KvTestEngine) -> Self {
        let (msg_tx, msg_rx) = channel();
        let trans = TestTransport { tx: msg_tx };
        let (notify_tx, notify_rx) = channel();
        let notifier = TestNotifier { tx: notify_tx };
        let mut writers = AsyncWriters::new();
        writers
            .spawn(1, kv_engine, raft_engine, &notifier, &trans, &cfg)
            .unwrap();
        Self {
            writers,
            msg_rx,
            notify_rx,
        }
    }

    fn async_write_sender(&self, id: usize) -> &Sender<AsyncWriteMsg<KvTestEngine, KvTestEngine>> {
        &self.writers.senders()[id]
    }
}

#[test]
fn test_batch_write() {
    let (_dir, kv_engine) = create_tmp_engine("async-io-batch-kv");
    let (_dir, raft_engine) = create_tmp_engine("async-io-batch-raft");
    let mut t = TestAsyncWriteWorker::new(&Config::default(), &kv_engine, &raft_engine);

    let mut task_1 = AsyncWriteTask::<KvTestEngine, KvTestEngine>::new(1, 1, 10);
    task_1.kv_wb = Some(kv_engine.write_batch());
    put_kv(&mut task_1.kv_wb, b"kv_k1", b"kv_v1");
    task_1.raft_wb = Some(raft_engine.write_batch());
    put_kv(&mut task_1.raft_wb, b"raft_k1", b"raft_v1");
    task_1.entries.append(&mut vec![
        new_entry(5, 5),
        new_entry(6, 5),
        new_entry(7, 5),
        new_entry(8, 5),
    ]);
    task_1.raft_state = Some(new_raft_state(5, 123, 6, 8));
    task_1.messages.append(&mut vec![RaftMessage::default()]);

    t.worker.wb.add_write_task(task_1);

    let mut task_2 = AsyncWriteTask::<KvTestEngine, KvTestEngine>::new(2, 2, 15);
    task_2.kv_wb = Some(kv_engine.write_batch());
    put_kv(&mut task_2.kv_wb, b"kv_k2", b"kv_v2");
    task_2.raft_wb = Some(raft_engine.write_batch());
    put_kv(&mut task_2.raft_wb, b"raft_k2", b"raft_v2");
    task_2
        .entries
        .append(&mut vec![new_entry(20, 15), new_entry(21, 15)]);
    task_2.raft_state = Some(new_raft_state(15, 234, 20, 21));
    task_2
        .messages
        .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

    t.worker.wb.add_write_task(task_2);

    let mut task_3 = AsyncWriteTask::<KvTestEngine, KvTestEngine>::new(1, 1, 11);
    task_3.kv_wb = Some(kv_engine.write_batch());
    put_kv(&mut task_3.kv_wb, b"kv_k3", b"kv_v3");
    task_3.raft_wb = Some(raft_engine.write_batch());
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

    t.worker.wb.add_write_task(task_3);

    t.worker.write_to_db();

    let snapshot = kv_engine.snapshot();
    assert_eq!(snapshot.get_value(b"kv_k1").unwrap().unwrap(), b"kv_v1");
    assert_eq!(snapshot.get_value(b"kv_k2").unwrap().unwrap(), b"kv_v2");
    assert_eq!(snapshot.get_value(b"kv_k3").unwrap().unwrap(), b"kv_v3");

    let snapshot = raft_engine.snapshot();
    assert!(snapshot.get_value(b"raft_k1").unwrap().is_none());
    assert_eq!(snapshot.get_value(b"raft_k2").unwrap().unwrap(), b"raft_v2");
    assert_eq!(snapshot.get_value(b"raft_k3").unwrap().unwrap(), b"raft_v3");

    must_have_same_notifies(vec![(1, (1, 11)), (2, (2, 15))], &t.notify_rx);

    must_have_entries_and_state(
        &raft_engine,
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
    let (_dir, kv_engine) = create_tmp_engine("async-io-basic-kv");
    let (_dir, raft_engine) = create_tmp_engine("async-io-basic-raft");
    let mut cfg = Config::default();
    cfg.store_io_pool_size = 2;
    let mut t = TestAsyncWriters::new(&cfg, &kv_engine, &raft_engine);

    let mut task_1 = AsyncWriteTask::<KvTestEngine, KvTestEngine>::new(1, 1, 10);
    task_1.kv_wb = Some(kv_engine.write_batch());
    put_kv(&mut task_1.kv_wb, b"kv_k1", b"kv_v1");
    task_1.raft_wb = Some(raft_engine.write_batch());
    put_kv(&mut task_1.raft_wb, b"raft_k1", b"raft_v1");
    task_1
        .entries
        .append(&mut vec![new_entry(5, 5), new_entry(6, 5), new_entry(7, 5)]);
    task_1.raft_state = Some(new_raft_state(5, 234, 6, 7));
    task_1
        .messages
        .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

    t.async_write_sender(0)
        .send(AsyncWriteMsg::WriteTask(task_1))
        .unwrap();

    let mut task_2 = AsyncWriteTask::<KvTestEngine, KvTestEngine>::new(2, 2, 20);
    task_2.kv_wb = Some(kv_engine.write_batch());
    put_kv(&mut task_2.kv_wb, b"kv_k2", b"kv_v2");
    task_2.raft_wb = Some(raft_engine.write_batch());
    put_kv(&mut task_2.raft_wb, b"raft_k2", b"raft_v2");
    task_2
        .entries
        .append(&mut vec![new_entry(50, 12), new_entry(51, 13)]);
    task_2.raft_state = Some(new_raft_state(13, 567, 49, 51));
    task_2
        .messages
        .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

    t.async_write_sender(1)
        .send(AsyncWriteMsg::WriteTask(task_2))
        .unwrap();

    let mut task_3 = AsyncWriteTask::<KvTestEngine, KvTestEngine>::new(1, 1, 15);
    task_3.kv_wb = Some(kv_engine.write_batch());
    put_kv(&mut task_3.kv_wb, b"kv_k3", b"kv_v3");
    delete_kv(&mut task_3.kv_wb, b"kv_k1");
    task_3.raft_wb = Some(raft_engine.write_batch());
    put_kv(&mut task_3.raft_wb, b"raft_k3", b"raft_v3");
    delete_kv(&mut task_3.raft_wb, b"raft_k2");
    task_3.entries.append(&mut vec![new_entry(6, 6)]);
    task_3.cut_logs = Some((7, 8));
    task_3.raft_state = Some(new_raft_state(6, 345, 6, 6));
    task_3
        .messages
        .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

    t.async_write_sender(0)
        .send(AsyncWriteMsg::WriteTask(task_3))
        .unwrap();

    must_wait_same_notifies(vec![(1, (1, 15)), (2, (2, 20))], &t.notify_rx);

    must_have_entries_and_state(
        &raft_engine,
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
