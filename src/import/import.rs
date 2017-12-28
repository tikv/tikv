// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};
use std::thread::{self, JoinHandle};

use tempdir::TempDir;
use uuid::Uuid;

use pd::RegionInfo;

use kvproto::importpb::*;
use kvproto::errorpb::NotLeader;

use super::{Client, Config, Engine, Error, ImportClient, Result, UploadStream};
use super::region::*;
use super::stream::*;
use super::prepare::*;

const MAX_RETRY_TIMES: u64 = 3;
const RETRY_INTERVAL_SECS: u64 = 3;

pub struct ImportJob {
    cfg: Config,
    dir: Arc<TempDir>,
    client: Arc<Client>,
    engine: Arc<Engine>,
    counter: Arc<AtomicUsize>,
}

impl ImportJob {
    pub fn new(cfg: Config, engine: Engine, client: Arc<Client>) -> Result<ImportJob> {
        let dir = TempDir::new_in(engine.path(), ".temp")?;
        Ok(ImportJob {
            cfg: cfg,
            dir: Arc::new(dir),
            client: client,
            engine: Arc::new(engine),
            counter: Arc::new(AtomicUsize::new(1)),
        })
    }

    pub fn run(&self) -> Result<()> {
        for cf_name in self.engine.cf_names() {
            let cfg = self.cfg.clone();
            let client = self.client.clone();
            let engine = self.engine.clone();
            let cf_name = cf_name.to_owned();

            let job = PrepareJob::new(cfg, client, engine, cf_name.clone());
            let cf_ranges = job.run();

            let handles = self.run_import_threads(cf_name, cf_ranges);
            for h in handles {
                h.join().unwrap()?;
            }
        }
        Ok(())
    }

    fn new_import_thread(&self, cf_name: String, cf_range: RangeInfo) -> JoinHandle<Result<()>> {
        let cfg = self.cfg.clone();
        let dir = self.dir.clone();
        let client = self.client.clone();
        let engine = self.engine.clone();
        let counter = self.counter.clone();

        thread::Builder::new()
            .name("import-job".to_owned())
            .spawn(move || {
                let job = SubImportJob::new(cfg, dir, client, engine, counter, cf_name, cf_range);
                job.run()
            })
            .unwrap()
    }

    fn run_import_threads(
        &self,
        cf_name: String,
        cf_ranges: Vec<RangeInfo>,
    ) -> Vec<JoinHandle<Result<()>>> {
        // Calculate the range size of each sub import job.
        let cf_size = cf_ranges.iter().fold(0, |acc, r| acc + r.size);
        info!(
            "{} cf contains {} bytes and {} ranges",
            cf_name,
            cf_size,
            cf_ranges.len()
        );
        let job_size = cf_size / self.cfg.max_import_jobs;

        // Calculate the range of each sub import job.
        let mut size = 0;
        let mut start = RANGE_MIN;
        let mut end = RANGE_MIN;
        let mut ranges = Vec::new();
        for i in 0..cf_ranges.len() {
            size += cf_ranges[i].size;
            assert!(end == RANGE_MIN || end == cf_ranges[i].get_start());
            end = cf_ranges[i].get_end();
            if size >= job_size || i == (cf_ranges.len() - 1) {
                let range = RangeInfo::new(start, end, size);
                ranges.push(range);
                size = 0;
                start = end;
            }
        }

        let mut handles = Vec::new();
        for range in ranges {
            handles.push(self.new_import_thread(cf_name.clone(), range));
        }
        handles
    }
}

struct SubImportJob {
    tag: String,
    cfg: Config,
    dir: Arc<TempDir>,
    client: Arc<Client>,
    engine: Arc<Engine>,
    counter: Arc<AtomicUsize>,
    cf_name: String,
    cf_range: RangeInfo,
    finished_ranges: Arc<Mutex<Vec<Range>>>,
}

impl SubImportJob {
    fn new(
        cfg: Config,
        dir: Arc<TempDir>,
        client: Arc<Client>,
        engine: Arc<Engine>,
        counter: Arc<AtomicUsize>,
        cf_name: String,
        cf_range: RangeInfo,
    ) -> SubImportJob {
        SubImportJob {
            tag: format!("[ImportJob {}:{}]", engine.uuid(), cf_name),
            cfg: cfg,
            dir: dir,
            client: Arc::new(Client::new_with(client)),
            engine: engine,
            counter: counter,
            cf_name: cf_name,
            cf_range: cf_range,
            finished_ranges: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn run(&self) -> Result<()> {
        let start = Instant::now();
        info!("{} start {:?}", self.tag, self.cf_range);

        for i in 0..MAX_RETRY_TIMES {
            if i != 0 {
                info!("{} retry #{}", self.tag, i);
                thread::sleep(Duration::from_secs(RETRY_INTERVAL_SECS));
            }

            if !self.import() {
                continue;
            }
            // Make sure that we don't miss some ranges.
            let stream = self.new_import_stream();
            assert!(stream.unwrap().next().unwrap().is_none());

            let range_count = self.finished_ranges.lock().unwrap().len();
            info!(
                "{} import {} ranges takes {:?}",
                self.tag,
                range_count,
                start.elapsed(),
            );

            return Ok(());
        }

        Err(Error::Timeout)
    }

    fn import(&self) -> bool {
        let (tx, rx) = mpsc::sync_channel(self.cfg.max_import_sst_jobs);
        let handles = self.run_import_threads(rx);
        let mut done = true;
        if let Err(e) = self.run_import_stream(tx) {
            error!("{} import stream: {:?}", self.tag, e);
            done = false;
        }
        for h in handles {
            if !h.join().unwrap() {
                done = false;
            }
        }
        done
    }

    fn new_import_stream(&self) -> Result<SSTFileStream> {
        let temp_dir = TempDir::new_in(self.dir.path(), &self.cf_name)?;
        Ok(SSTFileStream::new(
            self.cfg.clone(),
            temp_dir,
            self.client.clone(),
            self.engine.clone(),
            self.cf_name.clone(),
            self.cf_range.range.clone(),
            self.finished_ranges.lock().unwrap().clone(),
        ))
    }

    fn run_import_stream(&self, tx: mpsc::SyncSender<SSTFile>) -> Result<()> {
        let mut stream = self.new_import_stream()?;
        while let Some(sst) = stream.next()? {
            tx.send(sst).unwrap();
        }
        Ok(())
    }

    fn new_import_thread(&self, rx: Arc<Mutex<mpsc::Receiver<SSTFile>>>) -> JoinHandle<bool> {
        let client = self.client.clone();
        let engine = self.engine.clone();
        let counter = self.counter.clone();
        let cf_name = self.cf_name.clone();
        let finished_ranges = self.finished_ranges.clone();

        thread::Builder::new()
            .name("import-sst".to_owned())
            .spawn(move || {
                // Done if no error occurs.
                let mut done = true;
                while let Ok(sst) = rx.lock().unwrap().recv() {
                    let id = counter.fetch_add(1, Ordering::SeqCst);
                    let tag = format!("[ImportJob {}:{}:{}]", engine.uuid(), cf_name, id);
                    let mut job = ImportSSTJob::new(tag, sst, client.clone());
                    match job.run() {
                        Ok(v) => finished_ranges.lock().unwrap().push(v),
                        Err(_) => {
                            done = false;
                            continue;
                        }
                    }
                }
                done
            })
            .unwrap()
    }

    fn run_import_threads(&self, rx: mpsc::Receiver<SSTFile>) -> Vec<JoinHandle<bool>> {
        let mut handles = Vec::new();
        let rx = Arc::new(Mutex::new(rx));
        for _ in 0..self.cfg.max_import_sst_jobs {
            handles.push(self.new_import_thread(rx.clone()));
        }
        handles
    }
}

struct ImportSSTJob {
    tag: String,
    sst: SSTFile,
    client: Arc<Client>,
}

impl ImportSSTJob {
    fn new(tag: String, sst: SSTFile, client: Arc<Client>) -> ImportSSTJob {
        ImportSSTJob {
            tag: tag,
            sst: sst,
            client: client,
        }
    }

    fn run(&mut self) -> Result<Range> {
        let start = Instant::now();
        let range = self.sst.extended_range();
        info!("{} start {}", self.tag, self.sst);

        for i in 0..MAX_RETRY_TIMES {
            if i != 0 {
                info!("{} retry #{}", self.tag, i);
                thread::sleep(Duration::from_secs(RETRY_INTERVAL_SECS));
            }

            let region = match self.client.get_region(range.get_start()) {
                Ok(region) => if self.sst.is_inside(&region) {
                    region
                } else {
                    error!("{} outside of region {:?}", self.tag, region);
                    return Err(Error::SSTFileOutOfRange);
                },
                Err(e) => {
                    error!("{}: {:?}", self.tag, e);
                    continue;
                }
            };

            if self.import(region).is_ok() {
                info!("{} takes {:?}", self.tag, start.elapsed());
                return Ok(range);
            }
        }

        Err(Error::Timeout)
    }

    fn import(&mut self, mut region: RegionInfo) -> Result<()> {
        // Update SST meta for this region.
        {
            let meta = &mut self.sst.meta;
            // Uuid can not be reused, we must generate a new uuid here.
            meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());
            meta.set_region_id(region.get_id());
            meta.set_region_epoch(region.get_region_epoch().clone());
        }

        info!("{} import to {:?}", self.tag, region);

        self.upload(&region)?;

        // It's more lightweight to retry here as long as the region epoch has
        // not changed.
        for _ in 0..10 {
            match self.ingest(&region)? {
                None => return Ok(()),
                Some(mut error) => if error.has_leader() {
                    region.leader = Some(error.take_leader());
                },
            }
            thread::sleep(Duration::from_millis(300));
        }

        Err(Error::Timeout)
    }

    fn upload(&self, region: &RegionInfo) -> Result<()> {
        for peer in region.get_peers() {
            let upload = UploadStream::new(self.sst.meta.clone(), &self.sst.data);
            let store_id = peer.get_store_id();
            match self.client.upload_sst(store_id, upload) {
                Ok(_) => info!("{} upload to store {}", self.tag, store_id),
                Err(e) => {
                    error!("{} upload to store {}: {:?}", self.tag, store_id, e);
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    fn ingest(&self, region: &RegionInfo) -> Result<Option<NotLeader>> {
        let ctx = new_context(region);
        let store_id = ctx.get_peer().get_store_id();

        let mut ingest = IngestRequest::new();
        ingest.set_context(ctx);
        ingest.set_sst(self.sst.meta.clone());

        let res = match self.client.ingest_sst(store_id, ingest) {
            Ok(mut resp) => if resp.has_error() {
                let mut error = resp.take_error();
                if error.has_not_leader() {
                    return Ok(Some(error.take_not_leader()));
                }
                Err(Error::TikvRPC(error))
            } else {
                Ok(())
            },
            Err(e) => Err(e),
        };

        match res {
            Ok(_) => {
                info!("{} ingest to store {}", self.tag, store_id);
                Ok(None)
            }
            Err(e) => {
                error!("{} ingest to store {}: {:?}", self.tag, store_id, e);
                Err(e)
            }
        }
    }
}
