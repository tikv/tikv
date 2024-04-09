// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.
//! This module contains types for asynchronously applying the write batches
//! into the storage.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use futures::{Future, Stream, StreamExt};
use kvproto::kvrpcpb::Context;
use sst_importer::metrics::{APPLIER_ENGINE_REQUEST_DURATION, APPLIER_EVENT, IMPORTER_APPLY_BYTES};
use tikv_kv::{with_tls_engine, Engine, WriteData, WriteEvent};
use tikv_util::time::Instant;
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::storage;

pub async fn wait_write(
    mut s: impl Stream<Item = WriteEvent> + Send + Unpin,
) -> storage::Result<()> {
    match s.next().await {
        Some(WriteEvent::Finished(Ok(()))) => Ok(()),
        Some(WriteEvent::Finished(Err(e))) => Err(e.into()),
        Some(e) => Err(box_err!("unexpected event: {:?}", e)),
        None => Err(box_err!("stream closed")),
    }
}

const MAX_CONCURRENCY_PER_REGION: usize = 16;

async fn acquire_semaphore(smp: &Arc<Semaphore>) -> Option<SemaphorePermit<'_>> {
    if let Ok(pmt) = smp.try_acquire() {
        return Some(pmt);
    }
    APPLIER_EVENT.with_label_values(&["raft-throttled"]).inc();
    smp.acquire().await.ok()
}

#[derive(Clone, Default)]
/// A structure for throttling write throughput by region.
/// It uses the [`Engine`] stored in the thread local storage to write data.
/// Check the method [`tikv_kv::set_tls_engine`] for more details about the
/// thread local engine.
pub(crate) struct ThrottledTlsEngineWriter(Arc<Mutex<Inner>>);

impl ThrottledTlsEngineWriter {
    /// Write into the thread local storage engine.
    ///
    /// # Safety
    ///
    /// Before polling the future this returns, make sure the carrier thread's
    /// `TLS_ENGINE_ANY` is an engine typed `E`, or at least has the same
    /// memory layout of `E`.
    pub unsafe fn write<E: Engine>(
        &self,
        wd: WriteData,
        ctx: Context,
    ) -> impl Future<Output = storage::Result<()>> + Send + 'static {
        let mut this = self.0.lock().unwrap();
        let max_permit = this.max_permit;
        let start = Instant::now_coarse();
        let sem = this
            .sems
            .entry(ctx.get_region_id())
            .or_insert_with(|| {
                APPLIER_EVENT.with_label_values(&["new-writer"]).inc();
                Arc::new(Semaphore::new(max_permit))
            })
            .clone();
        async move {
            APPLIER_ENGINE_REQUEST_DURATION
                .with_label_values(&["queuing"])
                .observe(start.saturating_elapsed_secs());
            let start = Instant::now_coarse();
            let _prm = match acquire_semaphore(&sem).await {
                Some(prm) => prm,
                // When the permit has been closed. (Maybe tikv is shutting down?)
                None => {
                    return Err(box_err!(
                        "the semaphore bind to region {} has been closed",
                        ctx.get_region_id()
                    ));
                }
            };

            APPLIER_ENGINE_REQUEST_DURATION
                .with_label_values(&["get_permit"])
                .observe(start.saturating_elapsed_secs());
            let start = Instant::now_coarse();
            let size = wd.size();
            let fut = with_tls_engine::<E, _, _>(move |engine| {
                engine.async_write(&ctx, wd, WriteEvent::BASIC_EVENT, None)
            });
            let res = wait_write(fut).await;

            APPLIER_ENGINE_REQUEST_DURATION
                .with_label_values(&["apply"])
                .observe(start.saturating_elapsed_secs());
            IMPORTER_APPLY_BYTES.observe(size as _);
            res
        }
    }

    /// try to trigger a run of GC.
    ///
    /// # Returns
    ///
    /// If we still need to do keep doing GC (there are other references to the
    /// handle), return `true`, otherwise `false`.
    pub fn try_gc(&self) -> bool {
        if Arc::strong_count(&self.0) == 1 {
            return false;
        }

        let mut this = self.0.lock().unwrap();

        let before_count = this.sems.len();
        this.sems.retain(|_, v| Arc::strong_count(v) > 1);
        let after_count = this.sems.len();

        APPLIER_EVENT
            .with_label_values(&["gc-writer"])
            .inc_by((before_count.saturating_sub(after_count)) as _);
        true
    }

    #[cfg(test)]
    pub fn with_max_concurrency_per_region(conc: usize) -> Self {
        let mut inner = Inner::default();
        inner.max_permit = conc;
        Self(Arc::new(Mutex::new(inner)))
    }

    #[cfg(test)]
    pub fn inspect_inflight(&self) -> HashMap<u64, usize> {
        let this = self.0.lock().unwrap();
        let max_permit = this.max_permit;
        this.sems
            .iter()
            .map(|(rid, sem)| (*rid, max_permit - sem.available_permits()))
            .collect()
    }

    #[cfg(test)]
    pub fn inspect_worker(&self) -> usize {
        let this = self.0.lock().unwrap();
        this.sems.len()
    }
}

struct Inner {
    sems: HashMap<u64, Arc<Semaphore>>,
    max_permit: usize,
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            sems: Default::default(),
            max_permit: MAX_CONCURRENCY_PER_REGION,
        }
    }
}

#[cfg(test)]
mod test {
    use std::{convert::identity, iter::IntoIterator, sync::Mutex, time::Duration};

    use engine_rocks::RocksEngineIterator;
    use engine_traits::{Iterator, ALL_CFS, CF_DEFAULT, CF_WRITE};
    use futures::{future::join_all, Future};
    use kvproto::kvrpcpb::Context;
    use tempfile::TempDir;
    use tikv_kv::{Engine, Modify, RocksEngine, SnapContext, Snapshot, WriteData, WriteEvent};
    use tikv_util::sys::thread::ThreadBuildWrapper;
    use tokio::runtime::{Builder, Runtime};
    use txn_types::{Key, TimeStamp, Write, WriteType};

    use super::ThrottledTlsEngineWriter;
    use crate::storage::TestEngineBuilder;

    struct Suite {
        handle: ThrottledTlsEngineWriter,
        rt: Runtime,
        eng: RocksEngine,

        tso: u64,
        mirror: RocksEngine,

        _temp_dirs: [TempDir; 2],
    }

    impl Suite {
        fn wait<T>(&self, fut: impl Future<Output = T>) -> T {
            self.rt.block_on(fut)
        }

        fn batch<'a, 'b, 'this: 'a + 'b>(
            &mut self,
            region_id: u64,
            f: impl FnOnce(&mut dyn FnMut(&'a str, &'b str)) + 'this,
        ) -> (WriteData, Context) {
            let mut ctx = Context::default();
            ctx.set_region_id(region_id);
            let mut b = vec![];
            let mut t = |key, value| txn(key, value, &mut self.tso, &mut b);
            f(&mut t);
            let batch = WriteData::new(b.clone(), Default::default());
            let batch2 = WriteData::new(b, Default::default());
            self.wait(write_to_engine(&ctx, &self.mirror, batch));
            (batch2, ctx)
        }

        fn send_to_applier(
            &self,
            args: impl std::iter::Iterator<Item = (WriteData, Context)>,
        ) -> impl Future<Output = ()> {
            let fut = args
                .map(|arg| {
                    self.rt.spawn(
                        // SAFETY: we have already register the engine.
                        unsafe { self.handle.write::<RocksEngine>(arg.0, arg.1) },
                    )
                })
                .collect::<Vec<_>>();
            async {
                join_all(
                    fut.into_iter()
                        .map(|fut| async move { fut.await.unwrap().unwrap() }),
                )
                .await;
            }
        }

        fn check(&mut self, name: &str) {
            for cf in ALL_CFS {
                let the_mirror = iterate_over(&mut self.mirror, cf);
                let real_world = iterate_over(&mut self.eng, cf);
                compare_iter(the_mirror, real_world)
                    .map_err(|err| format!("case {name}: {err}"))
                    .unwrap();
            }
        }
    }

    fn create_applier(max_pending_raft_cmd: usize) -> Suite {
        let temp_dirs = [TempDir::new().unwrap(), TempDir::new().unwrap()];
        let engine = TestEngineBuilder::new()
            .path(temp_dirs[0].path())
            .build()
            .unwrap();
        let eng = engine.clone();
        let engine = Mutex::new(engine);
        let mirror = TestEngineBuilder::new()
            .path(temp_dirs[1].path())
            .build()
            .unwrap();
        let rt = Builder::new_multi_thread()
            .enable_all()
            .worker_threads(1)
            .with_sys_and_custom_hooks(
                move || tikv_kv::set_tls_engine(engine.lock().unwrap().clone()),
                // SAFETY: see the line above.
                || unsafe { tikv_kv::destroy_tls_engine::<RocksEngine>() },
            )
            .build()
            .unwrap();
        let handle =
            ThrottledTlsEngineWriter::with_max_concurrency_per_region(max_pending_raft_cmd);
        Suite {
            handle,
            rt,
            eng,
            tso: 1u64,
            mirror,
            _temp_dirs: temp_dirs,
        }
    }

    async fn write_to_engine(ctx: &Context, e: &RocksEngine, batch: WriteData) {
        use futures_util::StreamExt;
        e.async_write(ctx, batch, WriteEvent::BASIC_EVENT, None)
            .next()
            .await
            .unwrap();
    }

    fn iterate_over(e: &mut RocksEngine, cf: &'static str) -> RocksEngineIterator {
        let snap = e.snapshot(SnapContext::default()).unwrap();
        let mut iter = snap.iter(cf, Default::default()).unwrap();
        iter.seek_to_first().unwrap();
        iter
    }

    fn check_eq<T: Eq, D: std::fmt::Display>(
        a: T,
        b: T,
        tag: &str,
        show: impl Fn(T) -> D,
    ) -> Result<(), String> {
        if a != b {
            return Err(format!("{} not match: {} vs {}", tag, show(a), show(b)));
        }
        Ok(())
    }

    fn compare_iter(mut i1: impl Iterator, mut i2: impl Iterator) -> Result<(), String> {
        while i1.valid().unwrap() && i2.valid().unwrap() {
            check_eq(i1.key(), i2.key(), "key", <[u8]>::escape_ascii)?;
            check_eq(i1.value(), i2.value(), "value", <[u8]>::escape_ascii)?;
            i1.next().unwrap();
            i2.next().unwrap();
        }
        check_eq(i1.valid().unwrap(), i2.valid().unwrap(), "length", identity)?;
        Ok(())
    }

    fn write(key: &[u8], ty: WriteType, commit_ts: u64, start_ts: u64) -> (Vec<u8>, Vec<u8>) {
        let k = Key::from_raw(key).append_ts(TimeStamp::new(commit_ts));
        let v = Write::new(ty, TimeStamp::new(start_ts), None);
        (k.into_encoded(), v.as_ref().to_bytes())
    }

    fn default(key: &[u8], val: &[u8], start_ts: u64) -> (Vec<u8>, Vec<u8>) {
        let k = Key::from_raw(key).append_ts(TimeStamp::new(start_ts));
        (k.into_encoded(), val.to_owned())
    }

    fn default_req(key: &[u8], val: &[u8], start_ts: u64) -> Modify {
        let (k, v) = default(key, val, start_ts);
        Modify::Put(CF_DEFAULT, Key::from_encoded(k), v)
    }

    fn write_req(key: &[u8], ty: WriteType, commit_ts: u64, start_ts: u64) -> Modify {
        let (k, v) = write(key, ty, commit_ts, start_ts);
        if ty == WriteType::Delete {
            Modify::Delete(CF_WRITE, Key::from_encoded(k))
        } else {
            Modify::Put(CF_WRITE, Key::from_encoded(k), v)
        }
    }

    fn txn(key: &str, value: &str, tso: &mut u64, append_to: &mut Vec<Modify>) {
        let start = *tso;
        let commit = *tso + 1;
        *tso += 2;
        append_to.extend([
            default_req(key.as_bytes(), value.as_bytes(), start),
            write_req(key.as_bytes(), WriteType::Put, start, commit),
        ])
    }

    #[test]
    fn test_basic() {
        let mut suite = create_applier(16);
        let b1 = suite.batch(1, |t| {
            t("1", "amazing world in my dream");
            t("2", "gazing at the abyss");
        });
        let b2 = suite.batch(2, |t| {
            t("3", "the forest leaves drop");
            t("4", "the meaningless words in a test case");
        });
        let fut = suite.send_to_applier(vec![b1, b2].into_iter());
        suite.wait(fut);

        suite.check("basic");
    }

    #[test]
    // Clippy doesn't know about the romantic relationship between lazy evaluation and
    // side-effective ;)
    #[allow(clippy::needless_collect)]
    fn test_inflight_max() {
        let mut suite = create_applier(3);

        let b1 = (1..6)
            .map(|_| {
                suite.batch(1, move |t| {
                    t("al-kīmiyā", "following the light of the moon and stars, the guide of the sun and winds.");
                })
            })
            .collect::<Vec<_>>();
        let b2 = (1..3)
            .map(|_| {
                suite.batch(2, move |t| {
                    t(
                        "sole key to this mystery",
                        "fib this n = if n < 2 then n else this (n-1) + this (n-2)",
                    );
                })
            })
            .collect::<Vec<_>>();
        fail::cfg("rockskv_write_modifies", "sleep(5000)").unwrap();
        let fut = suite.send_to_applier(b1.into_iter().chain(b2));
        std::thread::sleep(Duration::from_secs(1));
        let pending_requests = suite.handle.inspect_inflight();
        assert_eq!(*pending_requests.get(&1).unwrap(), 3usize);
        assert_eq!(*pending_requests.get(&2).unwrap(), 2usize);
        fail::cfg("rockskv_write_modifies", "off").unwrap();
        suite.wait(fut);

        suite.check("inflight_max");
    }

    #[test]
    fn test_gc() {
        let mut suite = create_applier(16);
        let b1 = suite.batch(1, |t| {
            t("where is the sun", "it is in the clear sky");
            t("where are the words", "they are in some language model");
            t(
                "where is the language model",
                "I dunno, these sentences are generated by a human.",
            );
        });
        let b2 = suite.batch(2, |t| {
            t("...and this case needs two batches", "why?");
            t(
                "It is by... tradition.",
                "If a case is TOO short, who will believe it is effective?",
            );
            t(
                "Perhaps we should make the `RocksEngine` be able to distinguish requests.",
                "So...",
            );
            t(
                "We can block `b2` but not for `b1`",
                "then we can check there should be only one running worker.",
            );
        });
        assert_eq!(suite.handle.inspect_worker(), 0);
        fail::cfg("rockskv_async_write", "sleep(5000)").unwrap();
        let fut = suite.send_to_applier(std::iter::once(b1));
        assert_eq!(suite.handle.inspect_worker(), 1);
        let fut2 = suite.send_to_applier(std::iter::once(b2));
        assert_eq!(suite.handle.inspect_worker(), 2);

        fail::cfg("rockskv_async_write", "off").unwrap();
        suite.wait(async move {
            fut.await;
            fut2.await;
        });

        let hnd = suite.handle.clone();
        assert!(hnd.try_gc());
        assert_eq!(suite.handle.inspect_worker(), 0);

        drop(suite);
        assert!(!hnd.try_gc());
    }
}
