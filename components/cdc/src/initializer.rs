// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use std::sync::Arc;

use api_version::ApiV2;
use crossbeam::atomic::AtomicCell;
use engine_rocks::{ReadPerfContext, ReadPerfInstant, PROP_MAX_TS};
use engine_traits::{
    IterOptions, KvEngine, Range, Snapshot as EngineSnapshot, TablePropertiesCollection,
    TablePropertiesExt, UserCollectedProperties, CF_DEFAULT, CF_WRITE, DATA_KEY_PREFIX_LEN,
};
use fail::fail_point;
use keys::{data_end_key, data_key};
use kvproto::{
    cdcpb::ChangeDataRequestKvApi,
    kvrpcpb::ExtraOp as TxnExtraOp,
    metapb::{Region, RegionEpoch},
};
use raftstore::{
    coprocessor::ObserveID,
    router::RaftStoreRouter,
    store::{
        fsm::ChangeObserver,
        msg::{Callback, ReadResponse, SignificantMsg},
    },
};
use resolved_ts::Resolver;
use tikv::storage::{
    kv::Snapshot,
    mvcc::{DeltaScanner, ScannerBuilder},
    raw::raw_mvcc::{RawMvccIterator, RawMvccSnapshot},
    txn::{TxnEntry, TxnEntryScanner},
    Statistics,
};
use tikv_kv::Iterator;
use tikv_util::{
    box_err,
    codec::number,
    debug, error, info,
    sys::inspector::{self_thread_inspector, ThreadInspector},
    time::{Instant, Limiter},
    warn,
    worker::Scheduler,
    Either,
};
use tokio::sync::Semaphore;
use txn_types::{Key, KvPair, Lock, LockType, OldValue, TimeStamp};

use crate::{
    channel::CdcEvent,
    delegate::{post_init_downstream, Delegate, DownstreamID, DownstreamState},
    endpoint::Deregister,
    metrics::*,
    old_value::{near_seek_old_value, new_old_value_cursor, OldValueCursors},
    service::ConnID,
    Error, Result, Task,
};

struct ScanStat {
    // Fetched bytes to the scanner.
    emit: usize,
    // Bytes from the device, `None` if not possible to get it.
    disk_read: Option<usize>,
    // Perf delta for RocksDB.
    perf_delta: ReadPerfContext,
}

pub(crate) enum KvEntry {
    TxnEntry(TxnEntry),
    RawKvEntry(KvPair),
}

pub(crate) enum Scanner<S: Snapshot> {
    TxnKvScanner(DeltaScanner<S>),
    RawKvScanner(RawMvccIterator<<S as Snapshot>::Iter>),
}

pub(crate) struct Initializer<E> {
    pub(crate) engine: E,
    pub(crate) sched: Scheduler<Task>,
    pub(crate) sink: crate::channel::Sink,

    pub(crate) region_id: u64,
    pub(crate) region_epoch: RegionEpoch,
    pub(crate) observe_id: ObserveID,
    pub(crate) downstream_id: DownstreamID,
    pub(crate) downstream_state: Arc<AtomicCell<DownstreamState>>,
    pub(crate) conn_id: ConnID,
    pub(crate) request_id: u64,
    pub(crate) checkpoint_ts: TimeStamp,

    pub(crate) speed_limiter: Limiter,
    pub(crate) max_scan_batch_bytes: usize,
    pub(crate) max_scan_batch_size: usize,

    pub(crate) build_resolver: bool,
    pub(crate) ts_filter_ratio: f64,

    pub(crate) kv_api: ChangeDataRequestKvApi,
}

impl<E: KvEngine> Initializer<E> {
    pub(crate) async fn initialize<T: 'static + RaftStoreRouter<E>>(
        &mut self,
        change_cmd: ChangeObserver,
        raft_router: T,
        concurrency_semaphore: Arc<Semaphore>,
    ) -> Result<()> {
        fail_point!("cdc_before_initialize");
        let _permit = concurrency_semaphore.acquire().await;

        // When downstream_state is Stopped, it means the corresponding delegate
        // is stopped. The initialization can be safely canceled.
        //
        // Acquiring a permit may take some time, it is possible that
        // initialization can be canceled.
        if self.downstream_state.load() == DownstreamState::Stopped {
            info!("cdc async incremental scan canceled";
                "region_id" => self.region_id,
                "downstream_id" => ?self.downstream_id,
                "observe_id" => ?self.observe_id,
                "conn_id" => ?self.conn_id);
            return Err(box_err!("scan canceled"));
        }

        CDC_SCAN_TASKS.with_label_values(&["ongoing"]).inc();
        tikv_util::defer!({
            CDC_SCAN_TASKS.with_label_values(&["ongoing"]).dec();
        });

        // To avoid holding too many snapshots and holding them too long,
        // we need to acquire scan concurrency permit before taking snapshot.
        let sched = self.sched.clone();
        let region_id = self.region_id;
        let region_epoch = self.region_epoch.clone();
        let downstream_id = self.downstream_id;
        let downstream_state = self.downstream_state.clone();
        let (cb, fut) = tikv_util::future::paired_future_callback();
        let sink = self.sink.clone();
        let (incremental_scan_barrier_cb, incremental_scan_barrier_fut) =
            tikv_util::future::paired_future_callback();
        let barrier = CdcEvent::Barrier(Some(incremental_scan_barrier_cb));
        if let Err(e) = raft_router.significant_send(
            self.region_id,
            SignificantMsg::CaptureChange {
                cmd: change_cmd,
                region_epoch,
                callback: Callback::Read(Box::new(move |resp| {
                    if let Err(e) = sched.schedule(Task::InitDownstream {
                        region_id,
                        downstream_id,
                        downstream_state,
                        sink,
                        incremental_scan_barrier: barrier,
                        cb: Box::new(move || cb(resp)),
                    }) {
                        error!("cdc schedule cdc task failed"; "error" => ?e);
                    }
                })),
            },
        ) {
            warn!("cdc send capture change cmd failed";
            "region_id" => self.region_id, "error" => ?e);
            return Err(Error::request(e.into()));
        }

        // Wait all delta changes earlier than the incremental scan snapshot be
        // sent to the downstream, so that they must be consumed before the
        // incremental scan result.
        if let Err(e) = incremental_scan_barrier_fut.await {
            return Err(Error::Other(box_err!(e)));
        }

        match fut.await {
            Ok(resp) => self.on_change_cmd_response(resp).await,
            Err(e) => Err(Error::Other(box_err!(e))),
        }
    }

    pub(crate) async fn on_change_cmd_response(
        &mut self,
        mut resp: ReadResponse<impl EngineSnapshot>,
    ) -> Result<()> {
        if let Some(region_snapshot) = resp.snapshot {
            assert_eq!(self.region_id, region_snapshot.get_region().get_id());
            let region = region_snapshot.get_region().clone();
            self.async_incremental_scan(region_snapshot, region).await
        } else {
            assert!(
                resp.response.get_header().has_error(),
                "no snapshot and no error? {:?}",
                resp.response
            );
            let err = resp.response.take_header().take_error();
            Err(Error::request(err))
        }
    }

    pub(crate) async fn async_incremental_scan<S: Snapshot + 'static>(
        &mut self,
        snap: S,
        region: Region,
    ) -> Result<()> {
        let downstream_id = self.downstream_id;
        let region_id = region.get_id();
        let observe_id = self.observe_id;
        let kv_api = self.kv_api;
        debug!("cdc async incremental scan";
            "region_id" => region_id,
            "downstream_id" => ?downstream_id,
            "observe_id" => ?self.observe_id,
            "start_key" => log_wrappers::Value::key(snap.lower_bound().unwrap_or_default()),
            "end_key" => log_wrappers::Value::key(snap.upper_bound().unwrap_or_default()));

        let mut resolver = if self.build_resolver {
            Some(Resolver::new(region_id))
        } else {
            None
        };

        let (mut hint_min_ts, mut old_value_cursors) = (None, None);
        let mut scanner = if kv_api == ChangeDataRequestKvApi::TiDb {
            if self.ts_filter_is_helpful(&snap) {
                hint_min_ts = Some(self.checkpoint_ts);
                let wc = new_old_value_cursor(&snap, CF_WRITE);
                let dc = new_old_value_cursor(&snap, CF_DEFAULT);
                old_value_cursors = Some(OldValueCursors::new(wc, dc));
            }

            // Time range: (checkpoint_ts, max]
            let txnkv_scanner = ScannerBuilder::new(snap, TimeStamp::max())
                .fill_cache(false)
                .range(None, None)
                .hint_min_ts(hint_min_ts)
                .build_delta_scanner(self.checkpoint_ts, TxnExtraOp::ReadOldValue)
                .unwrap();

            Scanner::TxnKvScanner(txnkv_scanner)
        } else {
            let mut iter_opt = IterOptions::default();
            let (raw_key_prefix, raw_key_prefix_end) = ApiV2::get_rawkv_range();
            iter_opt.set_lower_bound(&[raw_key_prefix], DATA_KEY_PREFIX_LEN);
            iter_opt.set_upper_bound(&[raw_key_prefix_end], DATA_KEY_PREFIX_LEN);
            let mut iter = RawMvccSnapshot::from_snapshot(snap).iter(iter_opt).unwrap();

            iter.seek_to_first()?;
            Scanner::RawKvScanner(iter)
        };

        fail_point!("cdc_incremental_scan_start");
        let conn_id = self.conn_id;
        let mut done = false;
        let start = Instant::now_coarse();

        let curr_state = self.downstream_state.load();
        assert!(matches!(
            curr_state,
            DownstreamState::Initializing | DownstreamState::Stopped
        ));

        let on_cancel = || -> Result<()> {
            info!("cdc async incremental scan canceled";
                "region_id" => region_id,
                "downstream_id" => ?downstream_id,
                "observe_id" => ?observe_id,
                "conn_id" => ?conn_id);
            Err(box_err!("scan canceled"))
        };

        while !done {
            // When downstream_state is Stopped, it means the corresponding
            // delegate is stopped. The initialization can be safely canceled.
            if self.downstream_state.load() == DownstreamState::Stopped {
                return on_cancel();
            }
            let cursors = old_value_cursors.as_mut();
            let resolver = resolver.as_mut();
            let entries = self.scan_batch(&mut scanner, cursors, resolver).await?;
            if let Some(None) = entries.last() {
                // If the last element is None, it means scanning is finished.
                done = true;
            }
            debug!("cdc scan entries"; "len" => entries.len(), "region_id" => region_id);
            fail_point!("before_schedule_incremental_scan");
            self.sink_scan_events(entries, done).await?;
        }

        if !post_init_downstream(&self.downstream_state) {
            return on_cancel();
        }
        let takes = start.saturating_elapsed();
        info!("cdc async incremental scan finished";
            "region_id" => region.get_id(),
            "conn_id" => ?self.conn_id,
            "downstream_id" => ?self.downstream_id,
            "takes" => ?takes,
        );

        if let Some(resolver) = resolver {
            self.finish_building_resolver(resolver, region);
        }

        CDC_SCAN_DURATION_HISTOGRAM.observe(takes.as_secs_f64());
        Ok(())
    }

    // It's extracted from `Initializer::scan_batch` to avoid becoming an asynchronous block,
    // so that we can limit scan speed based on the thread disk I/O or RocksDB block read bytes.
    fn do_scan<S: Snapshot>(
        &self,
        scanner: &mut Scanner<S>,
        mut old_value_cursors: Option<&mut OldValueCursors<S::Iter>>,
        entries: &mut Vec<Option<KvEntry>>,
    ) -> Result<ScanStat> {
        let mut read_old_value = |v: &mut OldValue, stats: &mut Statistics| -> Result<()> {
            let (wc, dc) = match old_value_cursors {
                Some(ref mut x) => (&mut x.write, &mut x.default),
                None => return Ok(()),
            };
            if let OldValue::SeekWrite(ref key) = v {
                match near_seek_old_value(key, wc, Either::<&S, _>::Right(dc), stats)? {
                    Some(x) => *v = OldValue::value(x),
                    None => *v = OldValue::None,
                }
            }
            Ok(())
        };

        // This code block shouldn't be switched to other threads.
        let mut total_bytes = 0;
        let mut total_size = 0;
        let perf_instant = ReadPerfInstant::new();
        let inspector = self_thread_inspector().ok();
        let old_io_stat = inspector.as_ref().and_then(|x| x.io_stat().unwrap_or(None));
        let mut stats = Statistics::default();
        while total_bytes <= self.max_scan_batch_bytes && total_size < self.max_scan_batch_size {
            total_size += 1;
            match scanner {
                Scanner::TxnKvScanner(scanner) => match scanner.next_entry()? {
                    Some(mut entry) => {
                        read_old_value(entry.old_value(), &mut stats)?;
                        total_bytes += entry.size();
                        entries.push(Some(KvEntry::TxnEntry(entry)));
                    }
                    None => {
                        entries.push(None);
                        break;
                    }
                },
                Scanner::RawKvScanner(iter) => {
                    if iter.valid()? {
                        let key = iter.key();
                        let ts = ApiV2::decode_ts_from(key)?;
                        if ts > self.checkpoint_ts {
                            let value = iter.value();
                            total_bytes += key.len() + value.len();
                            entries.push(Some(KvEntry::RawKvEntry((key.to_vec(), value.to_vec()))));
                        }
                        iter.next()?;
                    } else {
                        entries.push(None);
                        break;
                    }
                }
            }
        }
        flush_oldvalue_stats(&stats, TAG_INCREMENTAL_SCAN);
        let new_io_stat = inspector.as_ref().and_then(|x| x.io_stat().unwrap_or(None));
        let disk_read = match (old_io_stat, new_io_stat) {
            (Some(s1), Some(s2)) => Some((s2.read - s1.read) as usize),
            _ => None,
        };
        let perf_delta = perf_instant.delta();
        let emit = total_bytes;
        Ok(ScanStat {
            emit,
            disk_read,
            perf_delta,
        })
    }

    async fn scan_batch<S: Snapshot>(
        &self,
        scanner: &mut Scanner<S>,
        old_value_cursors: Option<&mut OldValueCursors<S::Iter>>,
        resolver: Option<&mut Resolver>,
    ) -> Result<Vec<Option<KvEntry>>> {
        let mut entries = Vec::with_capacity(self.max_scan_batch_size);
        let ScanStat {
            emit,
            disk_read,
            perf_delta,
        } = self.do_scan(scanner, old_value_cursors, &mut entries)?;

        CDC_SCAN_BYTES.inc_by(emit as _);
        TLS_CDC_PERF_STATS.with(|x| *x.borrow_mut() += perf_delta);
        tls_flush_perf_stats();
        let require = if let Some(bytes) = disk_read {
            CDC_SCAN_DISK_READ_BYTES.inc_by(bytes as _);
            bytes
        } else {
            perf_delta.block_read_byte as usize
        };
        self.speed_limiter.consume(require).await;

        if let Some(resolver) = resolver {
            // Track the locks.
            for entry in entries.iter().flatten() {
                if let KvEntry::TxnEntry(TxnEntry::Prewrite { ref lock, .. }) = entry {
                    let (encoded_key, value) = lock;
                    let key = Key::from_encoded_slice(encoded_key).into_raw().unwrap();
                    let lock = Lock::parse(value)?;
                    match lock.lock_type {
                        LockType::Put | LockType::Delete => resolver.track_lock(lock.ts, key, None),
                        _ => (),
                    };
                }
            }
        }
        Ok(entries)
    }

    async fn sink_scan_events(&mut self, entries: Vec<Option<KvEntry>>, done: bool) -> Result<()> {
        let mut barrier = None;
        let mut events =
            Delegate::convert_to_grpc_events(self.region_id, self.request_id, entries)?;
        if done {
            let (cb, fut) = tikv_util::future::paired_future_callback();
            events.push(CdcEvent::Barrier(Some(cb)));
            barrier = Some(fut);
        }
        if let Err(e) = self.sink.send_all(events).await {
            error!("cdc send scan event failed"; "req_id" => ?self.request_id);
            return Err(Error::Sink(e));
        }

        if let Some(barrier) = barrier {
            // CDC needs to make sure resolved ts events can only be sent after
            // incremental scan is finished.
            // Wait the barrier to ensure tikv sends out all scan events.
            let _ = barrier.await;
        }

        Ok(())
    }

    fn finish_building_resolver(&self, mut resolver: Resolver, region: Region) {
        let observe_id = self.observe_id;
        let rts = resolver.resolve(TimeStamp::zero());
        info!(
            "cdc resolver initialized and schedule resolver ready";
            "region_id" => region.get_id(),
            "conn_id" => ?self.conn_id,
            "downstream_id" => ?self.downstream_id,
            "resolved_ts" => rts,
            "lock_count" => resolver.locks().len(),
            "observe_id" => ?observe_id,
        );

        fail_point!("before_schedule_resolver_ready");
        if let Err(e) = self.sched.schedule(Task::ResolverReady {
            observe_id,
            resolver,
            region,
        }) {
            error!("cdc schedule task failed"; "error" => ?e);
        }
    }

    // Deregister downstream when the Initializer fails to initialize.
    pub(crate) fn deregister_downstream(&self, err: Error) {
        let deregister = if self.build_resolver || err.has_region_error() {
            // Deregister delegate on the conditions,
            // * It fails to build a resolver. A delegate requires a resolver
            //   to advance resolved ts.
            // * A region error. It usually mean a peer is not leader or
            //   a leader meets an error and can not serve.
            Deregister::Delegate {
                region_id: self.region_id,
                observe_id: self.observe_id,
                err,
            }
        } else {
            Deregister::Downstream {
                region_id: self.region_id,
                downstream_id: self.downstream_id,
                conn_id: self.conn_id,
                err: Some(err),
            }
        };

        if let Err(e) = self.sched.schedule(Task::Deregister(deregister)) {
            error!("cdc schedule cdc task failed"; "error" => ?e);
        }
    }

    fn ts_filter_is_helpful<S: Snapshot>(&self, snap: &S) -> bool {
        if self.ts_filter_ratio < f64::EPSILON {
            return false;
        }

        let start_key = data_key(snap.lower_bound().unwrap_or_default());
        let end_key = data_end_key(snap.upper_bound().unwrap_or_default());
        let range = Range::new(&start_key, &end_key);
        let collection = match self.engine.table_properties_collection(CF_WRITE, &[range]) {
            Ok(collection) => collection,
            Err(_) => return false,
        };

        let hint_min_ts = self.checkpoint_ts.into_inner();
        let (mut total_count, mut filtered_count, mut tables) = (0, 0, 0);
        collection.iter_user_collected_properties(|prop| {
            tables += 1;
            if let Some((_, keys)) = prop.approximate_size_and_keys(&start_key, &end_key) {
                total_count += keys;
                if Self::parse_u64_prop(prop, PROP_MAX_TS)
                    .map_or(false, |max_ts| max_ts < hint_min_ts)
                {
                    filtered_count += keys;
                }
            }
            true
        });

        let valid_count = total_count - filtered_count;
        let use_ts_filter = valid_count as f64 / total_count as f64 <= self.ts_filter_ratio;
        info!("cdc incremental scan uses ts filter: {}", use_ts_filter;
            "region_id" => self.region_id,
            "hint_min_ts" => hint_min_ts,
            "mvcc_versions" => total_count,
            "filtered_versions" => filtered_count,
            "tables" => tables);
        use_ts_filter
    }

    fn parse_u64_prop(
        prop: &<<E as TablePropertiesExt>::TablePropertiesCollection as TablePropertiesCollection>::UserCollectedProperties,
        field: &str,
    ) -> Option<u64> {
        prop.get(field.as_bytes())
            .and_then(|mut x| number::decode_u64(&mut x).ok())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fmt::Display,
        sync::mpsc::{channel, sync_channel, Receiver, RecvTimeoutError, Sender},
        time::Duration,
    };

    use collections::HashSet;
    use engine_rocks::RocksEngine;
    use engine_traits::{MiscExt, CF_WRITE};
    use futures::{executor::block_on, StreamExt};
    use kvproto::{cdcpb::Event_oneof_event, errorpb::Error as ErrorHeader};
    use raftstore::{coprocessor::ObserveHandle, store::RegionSnapshot};
    use test_raftstore::MockRaftStoreRouter;
    use tikv::storage::{
        kv::Engine,
        txn::tests::{
            must_acquire_pessimistic_lock, must_commit, must_prewrite_delete, must_prewrite_put,
        },
        TestEngineBuilder,
    };
    use tikv_util::worker::{LazyWorker, Runnable};
    use tokio::runtime::{Builder, Runtime};

    use super::*;

    struct ReceiverRunnable<T: Display + Send> {
        tx: Sender<T>,
    }

    impl<T: Display + Send + 'static> Runnable for ReceiverRunnable<T> {
        type Task = T;

        fn run(&mut self, task: T) {
            let _ = self.tx.send(task);
        }
    }

    fn new_receiver_worker<T: Display + Send + 'static>() -> (LazyWorker<T>, Receiver<T>) {
        let (tx, rx) = channel();
        let runnable = ReceiverRunnable { tx };
        let mut worker = LazyWorker::new("test-receiver-worker");
        worker.start(runnable);
        (worker, rx)
    }

    fn mock_initializer(
        speed_limit: usize,
        buffer: usize,
        engine: Option<RocksEngine>,
        kv_api: ChangeDataRequestKvApi,
    ) -> (
        LazyWorker<Task>,
        Runtime,
        Initializer<RocksEngine>,
        Receiver<Task>,
        crate::channel::Drain,
    ) {
        let (receiver_worker, rx) = new_receiver_worker();
        let quota = crate::channel::MemoryQuota::new(usize::MAX);
        let (sink, drain) = crate::channel::channel(buffer, quota);

        let pool = Builder::new_multi_thread()
            .thread_name("test-initializer-worker")
            .worker_threads(4)
            .build()
            .unwrap();
        let downstream_state = Arc::new(AtomicCell::new(DownstreamState::Initializing));
        let initializer = Initializer {
            engine: engine.unwrap_or_else(|| {
                TestEngineBuilder::new()
                    .build_without_cache()
                    .unwrap()
                    .kv_engine()
            }),
            sched: receiver_worker.scheduler(),
            sink,

            region_id: 1,
            region_epoch: RegionEpoch::default(),
            observe_id: ObserveID::new(),
            downstream_id: DownstreamID::new(),
            downstream_state,
            conn_id: ConnID::new(),
            request_id: 0,
            checkpoint_ts: 1.into(),
            speed_limiter: Limiter::new(speed_limit as _),
            max_scan_batch_bytes: 1024 * 1024,
            max_scan_batch_size: 1024,
            build_resolver: true,
            ts_filter_ratio: 1.0, // always enable it.
            kv_api,
        };

        (receiver_worker, pool, initializer, rx, drain)
    }

    #[test]
    fn test_initializer_build_resolver() {
        let engine = TestEngineBuilder::new().build_without_cache().unwrap();

        let mut expected_locks = BTreeMap::<TimeStamp, HashSet<Arc<[u8]>>>::new();

        let mut total_bytes = 0;
        // Pessimistic locks should not be tracked
        for i in 0..10 {
            let k = &[b'k', i];
            total_bytes += k.len();
            let ts = TimeStamp::new(i as _);
            must_acquire_pessimistic_lock(&engine, k, k, ts, ts);
        }

        for i in 10..100 {
            let (k, v) = (&[b'k', i], &[b'v', i]);
            total_bytes += k.len();
            total_bytes += v.len();
            let ts = TimeStamp::new(i as _);
            must_prewrite_put(&engine, k, v, k, ts);
            expected_locks
                .entry(ts)
                .or_default()
                .insert(k.to_vec().into());
        }

        let region = Region::default();
        let snap = engine.snapshot(Default::default()).unwrap();
        // Buffer must be large enough to unblock async incremental scan.
        let buffer = 1000;
        let (mut worker, pool, mut initializer, rx, mut drain) = mock_initializer(
            total_bytes,
            buffer,
            Some(engine.kv_engine()),
            ChangeDataRequestKvApi::TiDb,
        );
        let check_result = || loop {
            let task = rx.recv().unwrap();
            match task {
                Task::ResolverReady { resolver, .. } => {
                    assert_eq!(resolver.locks(), &expected_locks);
                    return;
                }
                t => panic!("unexpected task {} received", t),
            }
        };
        // To not block test by barrier.
        pool.spawn(async move {
            let mut d = drain.drain();
            while d.next().await.is_some() {}
        });

        block_on(initializer.async_incremental_scan(snap.clone(), region.clone())).unwrap();
        check_result();

        initializer
            .downstream_state
            .store(DownstreamState::Initializing);
        initializer.max_scan_batch_bytes = total_bytes;
        block_on(initializer.async_incremental_scan(snap.clone(), region.clone())).unwrap();
        check_result();

        initializer
            .downstream_state
            .store(DownstreamState::Initializing);
        initializer.build_resolver = false;
        block_on(initializer.async_incremental_scan(snap.clone(), region.clone())).unwrap();

        loop {
            let task = rx.recv_timeout(Duration::from_millis(100));
            match task {
                Ok(t) => panic!("unexpected task {} received", t),
                Err(RecvTimeoutError::Timeout) => break,
                Err(e) => panic!("unexpected err {:?}", e),
            }
        }

        // Test cancellation.
        initializer.downstream_state.store(DownstreamState::Stopped);
        block_on(initializer.async_incremental_scan(snap.clone(), region)).unwrap_err();

        // Cancel error should trigger a deregsiter.
        let mut region = Region::default();
        region.set_id(initializer.region_id);
        region.mut_peers().push(Default::default());
        let snapshot = Some(RegionSnapshot::from_snapshot(snap, Arc::new(region)));
        let resp = ReadResponse {
            snapshot,
            response: Default::default(),
            txn_extra_op: Default::default(),
        };
        block_on(initializer.on_change_cmd_response(resp.clone())).unwrap_err();

        // Disconnect sink by dropping runtime (it also drops drain).
        drop(pool);
        initializer
            .downstream_state
            .store(DownstreamState::Initializing);
        block_on(initializer.on_change_cmd_response(resp)).unwrap_err();

        worker.stop();
    }

    // Test `hint_min_ts` works fine with `ExtraOp::ReadOldValue`.
    // Whether `DeltaScanner` emits correct old values or not is already tested by
    // another case `test_old_value_with_hint_min_ts`, so here we only care about
    // handling `OldValue::SeekWrite` with `OldValueReader`.
    #[test]
    fn test_incremental_scanner_with_hint_min_ts() {
        let engine = TestEngineBuilder::new().build_without_cache().unwrap();

        let v_suffix = |suffix: usize| -> Vec<u8> {
            let suffix = suffix.to_string().into_bytes();
            let mut v = Vec::with_capacity(1000 + suffix.len());
            (0..100).for_each(|_| v.extend_from_slice(b"vvvvvvvvvv"));
            v.extend_from_slice(&suffix);
            v
        };

        let check_handling_old_value_seek_write = || {
            // Do incremental scan with different `hint_min_ts` values.
            for checkpoint_ts in [200, 100, 150] {
                let (mut worker, pool, mut initializer, _rx, mut drain) = mock_initializer(
                    usize::MAX,
                    1000,
                    Some(engine.kv_engine()),
                    ChangeDataRequestKvApi::TiDb,
                );
                initializer.checkpoint_ts = checkpoint_ts.into();
                let mut drain = drain.drain();

                let snap = engine.snapshot(Default::default()).unwrap();
                let th = pool.spawn(async move {
                    initializer
                        .async_incremental_scan(snap, Region::default())
                        .await
                        .unwrap();
                });

                while let Some((event, _)) = block_on(drain.next()) {
                    let event = match event {
                        CdcEvent::Event(x) if x.event.is_some() => x.event.unwrap(),
                        _ => continue,
                    };
                    let entries = match event {
                        Event_oneof_event::Entries(mut x) => x.take_entries().into_vec(),
                        _ => continue,
                    };
                    for entry in entries.into_iter().filter(|x| x.start_ts == 200) {
                        // Check old value is expected in all cases.
                        assert_eq!(entry.get_old_value(), &v_suffix(100));
                    }
                }
                block_on(th).unwrap();
                worker.stop();
            }
        };

        // Create the initial data with CF_WRITE L0: |zkey_110, zkey1_160|
        must_prewrite_put(&engine, b"zkey", &v_suffix(100), b"zkey", 100);
        must_commit(&engine, b"zkey", 100, 110);
        must_prewrite_put(&engine, b"zzzz", &v_suffix(150), b"zzzz", 150);
        must_commit(&engine, b"zzzz", 150, 160);
        engine.kv_engine().flush_cf(CF_WRITE, true).unwrap();
        must_prewrite_delete(&engine, b"zkey", b"zkey", 200);
        check_handling_old_value_seek_write(); // For TxnEntry::Prewrite.

        // CF_WRITE L0: |zkey_110, zkey1_160|, |zkey_210|
        must_commit(&engine, b"zkey", 200, 210);
        engine.kv_engine().flush_cf(CF_WRITE, false).unwrap();
        check_handling_old_value_seek_write(); // For TxnEntry::Commit.
    }

    #[test]
    fn test_initializer_deregister_downstream() {
        let total_bytes = 1;
        let buffer = 1;
        let (mut worker, _pool, mut initializer, rx, _drain) =
            mock_initializer(total_bytes, buffer, None, ChangeDataRequestKvApi::TiDb);

        // Errors reported by region should deregister region.
        initializer.build_resolver = false;
        initializer.deregister_downstream(Error::request(ErrorHeader::default()));
        let task = rx.recv_timeout(Duration::from_millis(100));
        match task {
            Ok(Task::Deregister(Deregister::Delegate { region_id, .. })) => {
                assert_eq!(region_id, initializer.region_id);
            }
            Ok(other) => panic!("unexpected task {:?}", other),
            Err(e) => panic!("unexpected err {:?}", e),
        }

        initializer.build_resolver = false;
        initializer.deregister_downstream(Error::Other(box_err!("test")));
        let task = rx.recv_timeout(Duration::from_millis(100));
        match task {
            Ok(Task::Deregister(Deregister::Downstream { region_id, .. })) => {
                assert_eq!(region_id, initializer.region_id);
            }
            Ok(other) => panic!("unexpected task {:?}", other),
            Err(e) => panic!("unexpected err {:?}", e),
        }

        // Test deregister region when resolver fails to build.
        initializer.build_resolver = true;
        initializer.deregister_downstream(Error::Other(box_err!("test")));
        let task = rx.recv_timeout(Duration::from_millis(100));
        match task {
            Ok(Task::Deregister(Deregister::Delegate { region_id, .. })) => {
                assert_eq!(region_id, initializer.region_id);
            }
            Ok(other) => panic!("unexpected task {:?}", other),
            Err(e) => panic!("unexpected err {:?}", e),
        }

        worker.stop();
    }

    #[test]
    fn test_initializer_initialize() {
        test_initializer_initialize_impl(ChangeDataRequestKvApi::TiDb);
        test_initializer_initialize_impl(ChangeDataRequestKvApi::RawKv);
    }

    fn test_initializer_initialize_impl(kv_api: ChangeDataRequestKvApi) {
        let total_bytes = 1;
        let buffer = 1;
        let (mut worker, pool, mut initializer, _rx, _drain) =
            mock_initializer(total_bytes, buffer, None, kv_api);

        let change_cmd = ChangeObserver::from_cdc(1, ObserveHandle::new());
        let raft_router = MockRaftStoreRouter::new();
        let concurrency_semaphore = Arc::new(Semaphore::new(1));

        initializer.downstream_state.store(DownstreamState::Stopped);
        block_on(initializer.initialize(
            change_cmd,
            raft_router.clone(),
            concurrency_semaphore.clone(),
        ))
        .unwrap_err();

        let (tx, rx) = sync_channel(1);
        let concurrency_semaphore_ = concurrency_semaphore.clone();
        pool.spawn(async move {
            let _permit = concurrency_semaphore_.acquire().await;
            tx.send(()).unwrap();
            tx.send(()).unwrap();
            tx.send(()).unwrap();
        });
        rx.recv_timeout(Duration::from_millis(200)).unwrap();

        let (tx1, rx1) = sync_channel(1);
        let change_cmd = ChangeObserver::from_cdc(1, ObserveHandle::new());
        pool.spawn(async move {
            let res = initializer
                .initialize(change_cmd, raft_router, concurrency_semaphore)
                .await;
            tx1.send(res).unwrap();
        });
        // Must timeout because there is no enough permit.
        rx1.recv_timeout(Duration::from_millis(200)).unwrap_err();

        // Release the permit
        rx.recv_timeout(Duration::from_millis(200)).unwrap();
        let res = rx1.recv_timeout(Duration::from_millis(200)).unwrap();
        res.unwrap_err();

        worker.stop();
    }
}
