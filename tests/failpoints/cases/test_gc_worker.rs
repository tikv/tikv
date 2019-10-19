use std::sync::mpsc::channel;
use std::thread;
use std::time::Duration;

use kvproto::kvrpcpb::Context;
use test_storage::new_raft_engine;
use tikv::server::gc_worker::GCWorker;
use tikv::server::gc_worker::GC_MAX_PENDING_TASKS;
use tikv::storage;

#[test]
fn test_gcworker_busy() {
    let _guard = crate::setup();
    let snapshot_fp = "raftkv_async_snapshot";
    let (_cluster, engine, ctx) = new_raft_engine(3, "");
    let mut gc_worker = GCWorker::new(engine, None, None, 1.1);
    gc_worker.start().unwrap();

    fail::cfg(snapshot_fp, "pause").unwrap();
    let (tx1, rx1) = channel();
    // Schedule `GC_MAX_PENDING` GC requests.
    for _i in 0..GC_MAX_PENDING_TASKS {
        let tx1 = tx1.clone();
        gc_worker
            .async_gc(
                ctx.clone(),
                1,
                Box::new(move |res: storage::Result<()>| {
                    assert!(res.is_ok());
                    tx1.send(1).unwrap();
                }),
            )
            .unwrap();
    }
    // Sleep to make sure the failpoint is triggered.
    thread::sleep(Duration::from_millis(2000));
    // Schedule one more request. So that there is a request being processed and
    // `GC_MAX_PENDING` requests in queue.
    gc_worker
        .async_gc(
            ctx.clone(),
            1,
            Box::new(move |res: storage::Result<()>| {
                assert!(res.is_ok());
                tx1.send(1).unwrap();
            }),
        )
        .unwrap();

    // Old GC commands are blocked, the new one will get GCWorkerTooBusy error.
    let (tx2, rx2) = channel();
    gc_worker
        .async_gc(
            Context::default(),
            1,
            Box::new(move |res: storage::Result<()>| {
                match res {
                    Err(storage::Error::GCWorkerTooBusy) => {}
                    res => panic!("expect too busy, got {:?}", res),
                }
                tx2.send(1).unwrap();
            }),
        )
        .unwrap();

    rx2.recv().unwrap();
    fail::remove(snapshot_fp);
    for _ in 0..=GC_MAX_PENDING_TASKS {
        rx1.recv().unwrap();
    }
}
