// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashSet,
    future::Future,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use engine_traits::{KvEngine, CF_WRITE};
use kvproto::{
    errorpb,
    import_sstpb::{Error as ImportPbError, SstMeta, SwitchMode, *},
    kvrpcpb::Context,
};
use raftstore_v2::StoreMeta;
use sst_importer::{metrics::*, sst_meta_to_path, Error, Result, SstImporter};
use tikv_kv::{
    Engine, LocalTablets, Modify, SnapContext, Snapshot, SnapshotExt, WriteData, WriteEvent,
};
use txn_types::TimeStamp;

use super::{pb_error_inc, raft_writer::wait_write};
use crate::storage::{self, errors::extract_region_error_from_error};

#[derive(Default)]
pub(super) struct IngestLatch(Mutex<HashSet<PathBuf>>);

impl IngestLatch {
    pub(super) fn acquire_lock(&self, meta: &SstMeta) -> Result<bool> {
        let mut slots = self.0.lock().unwrap();
        let p = sst_meta_to_path(meta)?;
        Ok(slots.insert(p))
    }

    pub(super) fn release_lock(&self, meta: &SstMeta) -> Result<bool> {
        let mut slots = self.0.lock().unwrap();
        let p = sst_meta_to_path(meta)?;
        Ok(slots.remove(&p))
    }
}

#[derive(Default)]
pub(super) struct SuspendDeadline(AtomicU64);

impl SuspendDeadline {
    /// Check whether we should suspend the current request.
    pub(super) fn check_suspend(&self) -> Result<()> {
        let now = TimeStamp::physical_now();
        let suspend_until = self.0.load(Ordering::SeqCst);
        if now < suspend_until {
            Err(Error::Suspended {
                time_to_lease_expire: Duration::from_millis(suspend_until - now),
            })
        } else {
            Ok(())
        }
    }

    /// suspend requests for a period.
    ///
    /// # returns
    ///
    /// whether for now, the requests has already been suspended.
    pub(super) fn suspend_requests(&self, for_time: Duration) -> bool {
        let now = TimeStamp::physical_now();
        let last_suspend_until = self.0.load(Ordering::SeqCst);
        let suspended = now < last_suspend_until;
        let suspend_until = TimeStamp::physical_now() + for_time.as_millis() as u64;
        self.0.store(suspend_until, Ordering::SeqCst);
        suspended
    }

    /// allow all requests to enter.
    ///
    /// # returns
    ///
    /// whether requests has already been previously suspended.
    pub(super) fn allow_requests(&self) -> bool {
        let now = TimeStamp::physical_now();
        let last_suspend_until = self.0.load(Ordering::SeqCst);
        let suspended = now < last_suspend_until;
        self.0.store(0, Ordering::SeqCst);
        suspended
    }
}

fn check_write_stall<E: KvEngine>(
    region_id: u64,
    tablets: &LocalTablets<E>,
    store_meta: &Option<Arc<Mutex<StoreMeta<E>>>>,
    importer: &SstImporter<E>,
) -> Option<errorpb::Error> {
    let tablet = match tablets.get(region_id) {
        Some(tablet) => tablet,
        None => {
            let mut errorpb = errorpb::Error::default();
            errorpb.set_message(format!("region {} not found", region_id));
            errorpb.mut_region_not_found().set_region_id(region_id);
            return Some(errorpb);
        }
    };

    let reject_error = |region_id: Option<u64>| -> Option<errorpb::Error> {
        let mut errorpb = errorpb::Error::default();
        let err = if let Some(id) = region_id {
            format!("too many sst files are ingesting for region {}", id)
        } else {
            "too many sst files are ingesting".to_string()
        };
        let mut server_is_busy_err = errorpb::ServerIsBusy::default();
        server_is_busy_err.set_reason(err.clone());
        errorpb.set_message(err);
        errorpb.set_server_is_busy(server_is_busy_err);
        Some(errorpb)
    };

    // store_meta being Some means it is v2
    if let Some(ref store_meta) = store_meta {
        if let Some((region, _)) = store_meta.lock().unwrap().regions.get(&region_id) {
            if !importer.region_in_import_mode(region)
                && tablet.ingest_maybe_slowdown_writes(CF_WRITE).expect("cf")
            {
                return reject_error(Some(region_id));
            }
        } else {
            let mut errorpb = errorpb::Error::default();
            errorpb.set_message(format!("region {} not found", region_id));
            errorpb.mut_region_not_found().set_region_id(region_id);
            return Some(errorpb);
        }
    } else if importer.get_mode() == SwitchMode::Normal
        && tablet.ingest_maybe_slowdown_writes(CF_WRITE).expect("cf")
    {
        match tablet.get_sst_key_ranges(CF_WRITE, 0) {
            Ok(l0_sst_ranges) => {
                warn!(
                    "sst ingest is too slow";
                    "sst_ranges" => ?l0_sst_ranges,
                );
            }
            Err(e) => {
                error!("get sst key ranges failed"; "err" => ?e);
            }
        }
        return reject_error(None);
    }

    None
}

pub(super) fn async_snapshot<E: Engine>(
    engine: &mut E,
    context: &Context,
) -> impl Future<Output = std::result::Result<E::Snap, errorpb::Error>> {
    let res = engine.async_snapshot(SnapContext {
        pb_ctx: context,
        ..Default::default()
    });
    async move {
        res.await.map_err(|e| {
            let err: storage::Error = e.into();
            if let Some(e) = extract_region_error_from_error(&err) {
                e
            } else {
                let mut e = errorpb::Error::default();
                e.set_message(format!("{}", err));
                e
            }
        })
    }
}

async fn ingest_files_impl<E: Engine>(
    mut context: Context,
    ssts: Vec<SstMeta>,
    mut engine: E,
    importer: &SstImporter<E::Local>,
    label: &'static str,
) -> Result<IngestResponse> {
    // check api version
    if !importer.check_api_version(&ssts)? {
        return Err(Error::IncompatibleApiVersion);
    }
    importer.before_propose_ingest(&ssts).await?;

    let snapshot_res = async_snapshot(&mut engine, &context);
    let mut resp = IngestResponse::default();
    let res = match snapshot_res.await {
        Ok(snap) => snap,
        Err(e) => {
            pb_error_inc(label, &e);
            resp.set_error(e);
            return Ok(resp);
        }
    };

    fail_point!("before_sst_service_ingest_check_file_exist");
    // Here we shall check whether the file has been ingested before. This operation
    // must execute after geting a snapshot from raftstore to make sure that the
    // current leader has applied to current term.
    for sst in &ssts {
        if !importer.exist(sst) {
            warn!(
                "sst [{:?}] not exist. we may retry an operation that has already succeeded",
                sst
            );
            let mut errorpb = errorpb::Error::default();
            let err = "The file which would be ingested doest not exist.";
            let stale_err = errorpb::StaleCommand::default();
            errorpb.set_message(err.to_string());
            errorpb.set_stale_command(stale_err);
            resp.set_error(errorpb);
            return Ok(resp);
        }
    }
    let modifies = ssts
        .iter()
        .map(|s| Modify::Ingest(Box::new(s.clone())))
        .collect();
    context.set_term(res.ext().get_term().unwrap().into());
    let region_id = context.get_region_id();
    let res = engine.async_write(
        &context,
        WriteData::from_modifies(modifies),
        WriteEvent::BASIC_EVENT,
        None,
    );

    let mut resp = IngestResponse::default();
    if let Err(e) = wait_write(res).await {
        if let Some(e) = extract_region_error_from_error(&e) {
            pb_error_inc(label, &e);
            resp.set_error(e);
        } else {
            IMPORTER_ERROR_VEC
                .with_label_values(&[label, "unknown"])
                .inc();
            resp.mut_error()
                .set_message(format!("[region {}] ingest failed: {:?}", region_id, e));
        }
    }

    importer.after_ingested(&ssts).await;
    Ok(resp)
}

pub async fn ingest<E: Engine>(
    mut req: MultiIngestRequest,
    engine: E,
    suspend: &Arc<SuspendDeadline>,
    tablets: &LocalTablets<E::Local>,
    store_meta: &Option<Arc<Mutex<StoreMeta<E::Local>>>>,
    importer: &SstImporter<E::Local>,
    ingest_latch: &Arc<IngestLatch>,
    label: &'static str,
) -> Result<IngestResponse> {
    let mut resp = IngestResponse::default();
    if let Err(err) = suspend.check_suspend() {
        resp.set_error(ImportPbError::from(err).take_store_error());
        return Ok(resp);
    }

    if let Some(errorpb) = check_write_stall(
        req.get_context().get_region_id(),
        tablets,
        store_meta,
        importer,
    ) {
        resp.set_error(errorpb);
        return Ok(resp);
    }

    let mut errorpb = errorpb::Error::default();
    let mut metas = vec![];
    for meta in req.get_ssts() {
        if ingest_latch.acquire_lock(meta).unwrap_or(false) {
            metas.push(meta.clone());
        }
    }
    if metas.len() < req.get_ssts().len() {
        for m in metas {
            ingest_latch.release_lock(&m).unwrap();
        }
        errorpb.set_message(Error::FileConflict.to_string());
        resp.set_error(errorpb);
        return Ok(resp);
    }
    let res = ingest_files_impl(
        req.take_context(),
        req.take_ssts().into(),
        engine,
        importer,
        label,
    )
    .await;
    for meta in &metas {
        ingest_latch.release_lock(meta).unwrap();
    }
    res
}
