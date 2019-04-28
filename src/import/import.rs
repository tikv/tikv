// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam::channel::{bounded, Receiver, Sender};
use kvproto::import_sstpb::*;
use uuid::Uuid;

use crate::pd::RegionInfo;
use tikv_util::time::Instant;

use super::client::*;
use super::common::*;
use super::engine::*;
use super::metrics::*;
use super::prepare::*;
use super::speed_limiter::*;
use super::stream::*;
use super::{Config, Error, Result};

const MAX_RETRY_TIMES: u64 = 5;
const RETRY_INTERVAL_SECS: u64 = 3;
const STORE_UNAVAILABLE_WAIT_INTERVAL_MILLIS: u64 = 20000;

/// ImportJob is responsible for importing data stored in an engine to a cluster.
pub struct ImportJob<Client> {
    tag: String,
    cfg: Config,
    client: Client,
    engine: Arc<Engine>,
    counter: Arc<AtomicUsize>,
    speed_limit: Arc<SpeedLimiter>,
}

impl<Client: ImportClient> ImportJob<Client> {
    pub fn new(cfg: Config, client: Client, engine: Engine) -> ImportJob<Client> {
        let speed_limit = Arc::new(SpeedLimiter::new(
            cfg.upload_speed_limit.0 as f64,
            StandardClock,
        ));
        ImportJob {
            tag: format!("[ImportJob {}]", engine.uuid()),
            cfg,
            client,
            engine: Arc::new(engine),
            counter: Arc::new(AtomicUsize::new(1)),
            speed_limit,
        }
    }

    pub fn run(&self) -> Result<()> {
        let start = Instant::now();
        info!("start"; "tag" => %self.tag);

        // Join and check results.
        let mut res = Ok(());
        // Before importing data, we need to help to balance data in the cluster.
        let job = PrepareJob::new(
            self.cfg.clone(),
            self.client.clone(),
            Arc::clone(&self.engine),
        );

        let mut ranges = job.run()?.into_iter().map(|range| range.range).collect();
        IMPORT_EACH_PHASE.with_label_values(&["import"]).set(1.0);
        for i in 0..MAX_RETRY_TIMES {
            let retry_ranges = Arc::new(Mutex::new(Vec::new()));
            let handles = self.run_import_threads(ranges, Arc::clone(&retry_ranges));
            for h in handles {
                if let Err(e) = h.join().unwrap() {
                    res = Err(e)
                }
            }
            ranges = Arc::try_unwrap(retry_ranges).unwrap().into_inner().unwrap();
            let retry_count = ranges.len();
            if retry_count < 1 {
                break;
            }
            warn!(
                "still has ranges need to retry";
                "tag" => %self.tag,
                "retry_count" => %retry_count,
                "current_round" => %i,
            );
            if i == MAX_RETRY_TIMES - 1 {
                res = Err(Error::ImportJobFailed(format!(
                    "retry {} times still {} ranges failed",
                    i, retry_count
                )))
            }
        }
        IMPORT_EACH_PHASE.with_label_values(&["import"]).set(0.0);

        match res {
            Ok(_) => {
                info!("import engine"; "tag" => %self.tag, "takes" => ?start.elapsed());
                Ok(())
            }
            Err(e) => {
                error!("import engine failed"; "tag" => %self.tag, "err" => %e);
                Err(e)
            }
        }
    }

    /// Creates a new thread to run SubImportJob for importing a range of data.
    fn new_import_thread(
        &self,
        id: u64,
        rx: Receiver<LazySSTRange>,
        retry_ranges: Arc<Mutex<Vec<Range>>>,
    ) -> JoinHandle<Result<()>> {
        let client = self.client.clone();
        let engine = Arc::clone(&self.engine);
        let counter = Arc::clone(&self.counter);
        let speed_limit = Arc::clone(&self.speed_limit);

        thread::Builder::new()
            .name("import-job".to_owned())
            .spawn(move || {
                let job = SubImportJob::new(id, rx, client, engine, counter, speed_limit);
                job.run(retry_ranges)
            })
            .unwrap()
    }

    fn new_split_thread(
        &self,
        id: u64,
        client: Arc<Client>,
        range_rx: Receiver<Range>,
        sst_tx: Sender<LazySSTRange>,
        finished_ranges: Arc<Mutex<Vec<Range>>>,
    ) -> JoinHandle<Result<()>> {
        let engine = Arc::clone(&self.engine);
        let cfg = self.cfg.clone();
        let tag = self.tag.clone();

        thread::Builder::new()
            .name("dispatch-job".to_owned())
            .spawn(move || {
                'NEXT_RANGE: while let Ok(range) = range_rx.recv() {
                    'RETRY: for _ in 0..MAX_RETRY_TIMES {
                        let cfg = cfg.clone();
                        let client = Arc::clone(&client);
                        let engine = Arc::clone(&engine);
                        let mut stream = SSTFileStream::new(
                            cfg,
                            client,
                            engine,
                            range.clone(),
                            finished_ranges.lock().unwrap().clone(),
                        );

                        loop {
                            let split_start = Instant::now_coarse();
                            match stream.next() {
                                Ok(Some(info)) => {
                                    IMPORT_SPLIT_SST_DURATION.observe(split_start.elapsed_secs());
                                    let start = Instant::now_coarse();
                                    sst_tx.send(info).unwrap();
                                    IMPORT_SST_DELIVERY_DURATION.observe(start.elapsed_secs());
                                }
                                Ok(None) => continue 'NEXT_RANGE,
                                Err(_) => continue 'RETRY,
                            }
                        }
                    }
                }

                info!("dispatch-job done"; "tag" => %tag, "id" => %id);
                Ok(())
            })
            .unwrap()
    }

    fn run_import_threads(
        &self,
        ranges: Vec<Range>,
        retry_ranges: Arc<Mutex<Vec<Range>>>,
    ) -> Vec<JoinHandle<Result<()>>> {
        let mut handles: Vec<JoinHandle<Result<()>>> = Vec::new();
        let finished_ranges = Arc::new(Mutex::new(Vec::new()));
        let (sst_tx, sst_rx) = bounded(self.cfg.num_import_jobs);

        // Spawn a group of threads to execute real import jobs
        for id in 0..self.cfg.num_import_jobs {
            handles.push(self.new_import_thread(
                id as u64,
                sst_rx.clone(),
                Arc::clone(&retry_ranges),
            ));
        }

        let split_thread_count = self.cfg.num_import_jobs * 2;
        let (range_tx, range_rx) = bounded(split_thread_count);

        // Spawn a group of threads to split range to ssts
        let client = Arc::new(self.client.clone());
        for id in 0..split_thread_count {
            handles.push(self.new_split_thread(
                id as u64,
                Arc::clone(&client),
                range_rx.clone(),
                sst_tx.clone(),
                Arc::clone(&finished_ranges),
            ));
        }

        for (_, range) in ranges.into_iter().enumerate() {
            let start = Instant::now_coarse();
            range_tx.send(range).unwrap();
            IMPORT_RANGE_DELIVERY_DURATION.observe(start.elapsed_secs());
        }

        handles
    }
}

/// SubImportJob is responsible for generating and importing sst files for a range of data
/// stored in an engine.
struct SubImportJob<Client> {
    id: u64,
    rx: Receiver<LazySSTRange>,
    client: Arc<Client>,
    engine: Arc<Engine>,
    counter: Arc<AtomicUsize>,
    num_errors: Arc<AtomicUsize>,
    speed_limit: Arc<SpeedLimiter>,
}

impl<Client: ImportClient> SubImportJob<Client> {
    fn new(
        id: u64,
        rx: Receiver<LazySSTRange>,
        client: Client,
        engine: Arc<Engine>,
        counter: Arc<AtomicUsize>,
        speed_limit: Arc<SpeedLimiter>,
    ) -> SubImportJob<Client> {
        SubImportJob {
            id,
            rx,
            client: Arc::new(client),
            engine,
            counter,
            num_errors: Arc::new(AtomicUsize::new(0)),
            speed_limit,
        }
    }

    fn run(&self, retry_ranges: Arc<Mutex<Vec<Range>>>) -> Result<()> {
        let sub_id = self.id;
        let client = Arc::clone(&self.client);
        let engine = Arc::clone(&self.engine);
        let counter = Arc::clone(&self.counter);
        let num_errors = Arc::clone(&self.num_errors);

        let mut start = Instant::now_coarse();
        while let Ok((range, ssts)) = self.rx.recv() {
            IMPORT_SST_RECV_DURATION.observe(start.elapsed_secs());
            start = Instant::now_coarse();
            'NEXT_SST: for lazy_sst in ssts {
                let sst = lazy_sst.into_sst_file()?;
                let id = counter.fetch_add(1, Ordering::SeqCst);
                let tag = format!("[ImportSSTJob {}:{}:{}]", engine.uuid(), sub_id, id);
                let res =
                    { ImportSSTJob::new(tag, sst, Arc::clone(&client), &self.speed_limit).run() };
                // Entire range will be retried if any sst in this range failed,
                // so there is no need for retry single sst
                if res.is_err() {
                    num_errors.fetch_add(1, Ordering::SeqCst);
                    retry_ranges.lock().unwrap().push(range);
                    break 'NEXT_SST;
                }
            }
        }

        Ok(())
    }
}

/// ImportSSTJob is responsible for importing `sst` to all replicas of the
/// specific Region
struct ImportSSTJob<'a, Client> {
    tag: String,
    sst: SSTFile,
    client: Arc<Client>,
    speed_limit: &'a SpeedLimiter,
}

impl<'a, Client: ImportClient> ImportSSTJob<'a, Client> {
    fn new(tag: String, sst: SSTFile, client: Arc<Client>, speed_limit: &'a SpeedLimiter) -> Self {
        ImportSSTJob {
            tag,
            sst,
            client,
            speed_limit,
        }
    }

    fn run(&mut self) -> Result<()> {
        let start = Instant::now();
        info!("start"; "tag" => %self.tag, "sst" => ?self.sst);

        for i in 0..MAX_RETRY_TIMES {
            if i != 0 {
                thread::sleep(Duration::from_secs(RETRY_INTERVAL_SECS));
            }

            let range = self.sst.meta.get_range().clone();
            let mut region = match self.client.get_region(range.get_start()) {
                Ok(region) => {
                    if self.sst.inside_region(&region) {
                        region
                    } else {
                        warn!("sst out of region range"; "tag" => %self.tag, "region" => ?region);
                        return Err(Error::ImportSSTJobFailed(self.tag.clone()));
                    }
                }
                Err(e) => {
                    warn!("get region failed"; "tag" => %self.tag, "err" => %e);
                    continue;
                }
            };

            for _ in 0..MAX_RETRY_TIMES {
                match self.import(region) {
                    Ok(_) => {
                        info!("import sst"; "tag" => %self.tag, "takes" => ?start.elapsed());
                        return Ok(());
                    }
                    Err(Error::UpdateRegion(new_region)) => {
                        region = new_region;
                        continue;
                    }
                    Err(_) => break,
                }
            }
        }

        error!("run out of time"; "tag" => %self.tag);
        Err(Error::ImportSSTJobFailed(self.tag.clone()))
    }

    fn import(&mut self, mut region: RegionInfo) -> Result<()> {
        info!("start import sst"; "tag" => %self.tag, "region" => ?region);

        // Update SST meta for this region.
        {
            let meta = &mut self.sst.meta;
            // Uuid can not be reused, we must generate a new uuid here.
            meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());
            meta.set_region_id(region.get_id());
            meta.set_region_epoch(region.get_region_epoch().clone());
        }

        let start = Instant::now_coarse();
        self.upload(&region)?;
        IMPORT_SST_UPLOAD_DURATION.observe(start.elapsed_secs());
        IMPORT_SST_CHUNK_BYTES.observe(self.sst.info.file_size as f64);

        let start = Instant::now_coarse();
        match self.ingest(&region) {
            Ok(_) => {
                IMPORT_SST_INGEST_DURATION.observe(start.elapsed_secs());
                Ok(())
            }
            Err(Error::NotLeader(new_leader)) => {
                region.leader = new_leader;
                Err(Error::UpdateRegion(region))
            }
            Err(Error::EpochNotMatch(current_regions)) => {
                let current_region = current_regions
                    .iter()
                    .find(|&r| self.sst.inside_region(r))
                    .cloned();
                match current_region {
                    Some(current_region) => {
                        let new_leader = region
                            .leader
                            .and_then(|p| find_region_peer(&current_region, p.get_store_id()));
                        Err(Error::UpdateRegion(RegionInfo::new(
                            current_region,
                            new_leader,
                        )))
                    }
                    None => {
                        warn!("epoch not match"; "tag" => %self.tag, "new_regions" => ?current_regions);
                        Err(Error::EpochNotMatch(current_regions))
                    }
                }
            }
            Err(e) => Err(e),
        }
    }

    fn upload(&self, region: &RegionInfo) -> Result<()> {
        for peer in region.get_peers() {
            let size = self.sst.info.file_size;
            self.speed_limit.take(&self.tag, size);

            let file = self.sst.info.open()?;
            let upload = UploadStream::new(self.sst.meta.clone(), file);
            let store_id = peer.get_store_id();
            while !self
                .client
                .is_space_enough(store_id, self.sst.info.file_size)?
            {
                let label = format!("{}", store_id);
                IMPORT_STORE_SAPCE_NOT_ENOUGH_COUNTER
                    .with_label_values(&[label.as_str()])
                    .inc();
                thread::sleep(Duration::from_millis(
                    STORE_UNAVAILABLE_WAIT_INTERVAL_MILLIS,
                ))
            }
            match self.client.upload_sst(store_id, upload) {
                Ok(_) => {
                    info!("upload"; "tag" => %self.tag, "store" => %store_id);
                }
                Err(e) => {
                    warn!("upload failed"; "tag" => %self.tag, "store" => %store_id, "err" => %e);
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    fn ingest(&self, region: &RegionInfo) -> Result<()> {
        let ctx = new_context(region);
        let store_id = ctx.get_peer().get_store_id();

        let mut ingest = IngestRequest::new();
        ingest.set_context(ctx);
        ingest.set_sst(self.sst.meta.clone());

        let res = match self.client.ingest_sst(store_id, ingest) {
            Ok(mut resp) => {
                if !resp.has_error() {
                    Ok(())
                } else {
                    match Error::from(resp.take_error()) {
                        e @ Error::NotLeader(_) | e @ Error::EpochNotMatch(_) => return Err(e),
                        e => Err(e),
                    }
                }
            }
            Err(e) => Err(e),
        };

        match res {
            Ok(_) => {
                info!("ingest"; "tag" => %self.tag, "store" => %store_id);
                Ok(())
            }
            Err(e) => {
                warn!("ingest failed"; "tag" => %self.tag, "store" => %store_id, "err" => %e);
                Err(e)
            }
        }
    }
}
