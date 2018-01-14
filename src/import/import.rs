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

use uuid::Uuid;
use kvproto::importpb::*;

use pd::RegionInfo;

use super::{Client, Config, Engine, Error, Result, UploadStream};
use super::common::*;
use super::stream::*;
use super::prepare::*;

const MAX_RETRY_TIMES: u64 = 5;
const RETRY_INTERVAL_SECS: u64 = 3;

pub struct ImportJob<C> {
    cfg: Config,
    client: Arc<C>,
    engine: Arc<Engine>,
    counter: Arc<AtomicUsize>,
}

impl<C: Client + Send + Sync + 'static> ImportJob<C> {
    pub fn new(cfg: Config, client: Arc<C>, engine: Engine) -> ImportJob<C> {
        ImportJob {
            cfg: cfg,
            client: client,
            engine: Arc::new(engine),
            counter: Arc::new(AtomicUsize::new(1)),
        }
    }

    pub fn run(&self) -> Result<()> {
        let cfg = self.cfg.clone();
        let client = self.client.clone();
        let engine = self.engine.clone();

        let job = PrepareJob::new(cfg, client, engine);
        let ranges = job.run();
        let handles = self.run_import_threads(ranges);

        let mut res = Ok(());
        for h in handles {
            if let Err(e) = h.join().unwrap() {
                res = Err(e)
            }
        }
        res
    }

    fn new_import_thread(&self, id: u64, range: RangeInfo) -> JoinHandle<Result<()>> {
        let cfg = self.cfg.clone();
        let client = self.client.clone();
        let engine = self.engine.clone();
        let counter = self.counter.clone();

        thread::Builder::new()
            .name("import-job".to_owned())
            .spawn(move || {
                let job = SubImportJob::new(id, cfg, range, client, engine, counter);
                job.run()
            })
            .unwrap()
    }

    fn run_import_threads(&self, ranges: Vec<RangeInfo>) -> Vec<JoinHandle<Result<()>>> {
        // Calculate the range size of each sub import job.
        let size = ranges.iter().fold(0, |acc, r| acc + r.size);
        info!(
            "import job contains {} bytes and {} ranges",
            size,
            ranges.len()
        );
        let job_size = size / self.cfg.max_import_jobs;

        // Calculate the range of each sub import job.
        let mut start = RANGE_MIN;
        let mut end = RANGE_MIN;
        let mut sub_size = 0;
        let mut sub_ranges = Vec::new();
        for (i, range) in ranges.iter().enumerate() {
            sub_size += range.size;
            assert!(end == RANGE_MIN || end == range.get_start());
            end = range.get_end();
            if sub_size >= job_size || i == (ranges.len() - 1) {
                let sub_range = RangeInfo::new(start, end, sub_size);
                sub_ranges.push(sub_range);
                sub_size = 0;
                start = end;
            }
        }

        let mut handles = Vec::new();
        for (i, range) in sub_ranges.into_iter().enumerate() {
            handles.push(self.new_import_thread(i as u64, range));
        }
        handles
    }
}

struct SubImportJob<C> {
    id: u64,
    tag: String,
    cfg: Config,
    range: RangeInfo,
    client: Arc<C>,
    engine: Arc<Engine>,
    counter: Arc<AtomicUsize>,
    finished_ranges: Arc<Mutex<Vec<Range>>>,
}

impl<C: Client + Send + Sync + 'static> SubImportJob<C> {
    fn new(
        id: u64,
        cfg: Config,
        range: RangeInfo,
        client: Arc<C>,
        engine: Arc<Engine>,
        counter: Arc<AtomicUsize>,
    ) -> SubImportJob<C> {
        SubImportJob {
            id: id,
            tag: format!("[ImportJob {}:{}]", engine.uuid(), id),
            cfg: cfg,
            range: range,
            client: client,
            engine: engine,
            counter: counter,
            finished_ranges: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn run(&self) -> Result<()> {
        let start = Instant::now();
        info!("{} start {:?}", self.tag, self.range);

        for i in 0..MAX_RETRY_TIMES {
            if i != 0 {
                thread::sleep(Duration::from_secs(RETRY_INTERVAL_SECS));
            }

            if !self.import() {
                continue;
            }
            // Make sure that we don't miss some ranges.
            let mut stream = self.new_import_stream();
            assert!(stream.next().unwrap().is_none());

            let range_count = self.finished_ranges.lock().unwrap().len();
            info!(
                "{} import {} ranges takes {:?}",
                self.tag,
                range_count,
                start.elapsed(),
            );

            return Ok(());
        }

        Err(Error::SubImportJobFailed(self.tag.clone()))
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

    fn new_import_stream(&self) -> SSTFileStream<C> {
        SSTFileStream::new(
            self.cfg.clone(),
            self.client.clone(),
            self.engine.clone(),
            self.range.range.clone(),
            self.finished_ranges.lock().unwrap().clone(),
        )
    }

    fn run_import_stream(&self, tx: mpsc::SyncSender<SSTRange>) -> Result<()> {
        let mut stream = self.new_import_stream();
        while let Some(info) = stream.next()? {
            tx.send(info).unwrap();
        }
        Ok(())
    }

    fn new_import_thread(&self, rx: Arc<Mutex<mpsc::Receiver<SSTRange>>>) -> JoinHandle<bool> {
        let sub_id = self.id;
        let client = self.client.clone();
        let engine = self.engine.clone();
        let counter = self.counter.clone();
        let finished_ranges = self.finished_ranges.clone();

        thread::Builder::new()
            .name("import-sst".to_owned())
            .spawn(move || {
                // Done if no error occurs.
                let mut done = true;

                'OUTER_LOOP: while let Ok((range, ssts)) = rx.lock().unwrap().recv() {
                    for sst in ssts {
                        let id = counter.fetch_add(1, Ordering::SeqCst);
                        let tag = format!("[ImportSST {}:{}:{}]", engine.uuid(), sub_id, id);
                        let mut job = ImportSSTJob::new(tag, sst, client.clone());
                        if job.run().is_err() {
                            done = false;
                            continue 'OUTER_LOOP;
                        }
                    }
                    finished_ranges.lock().unwrap().push(range);
                }

                done
            })
            .unwrap()
    }

    fn run_import_threads(&self, rx: mpsc::Receiver<SSTRange>) -> Vec<JoinHandle<bool>> {
        let mut handles = Vec::new();
        let rx = Arc::new(Mutex::new(rx));
        for _ in 0..self.cfg.max_import_sst_jobs {
            handles.push(self.new_import_thread(rx.clone()));
        }
        handles
    }
}

struct ImportSSTJob<C> {
    tag: String,
    sst: SSTFile,
    client: Arc<C>,
}

impl<C: Client> ImportSSTJob<C> {
    fn new(tag: String, sst: SSTFile, client: Arc<C>) -> ImportSSTJob<C> {
        ImportSSTJob {
            tag: tag,
            sst: sst,
            client: client,
        }
    }

    fn run(&mut self) -> Result<()> {
        let start = Instant::now();
        info!("{} start {:?}", self.tag, self.sst);

        for i in 0..MAX_RETRY_TIMES {
            if i != 0 {
                thread::sleep(Duration::from_secs(RETRY_INTERVAL_SECS));
            }

            let region = match self.client
                .get_region(self.sst.meta.get_range().get_start())
            {
                Ok(region) => if self.sst.is_inside(&region) {
                    region
                } else {
                    error!("{} cross region {:?}", self.tag, region);
                    return Err(Error::SSTFileCrossRegion);
                },
                Err(e) => {
                    error!("{}: {:?}", self.tag, e);
                    continue;
                }
            };

            if self.import(region).is_ok() {
                info!("{} takes {:?}", self.tag, start.elapsed());
                return Ok(());
            }
        }

        Err(Error::ImportSSTJobFailed(self.tag.clone()))
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

        for _ in 0..MAX_RETRY_TIMES {
            match self.ingest(&region) {
                Ok(_) => return Ok(()),
                Err(Error::NotLeader(new_leader)) => region.leader = new_leader,
                Err(e) => return Err(e),
            }
        }

        // Last chance.
        self.ingest(&region)
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

    fn ingest(&self, region: &RegionInfo) -> Result<()> {
        let ctx = new_context(region);
        let store_id = ctx.get_peer().get_store_id();

        let mut ingest = IngestRequest::new();
        ingest.set_context(ctx);
        ingest.set_sst(self.sst.meta.clone());

        let res = match self.client.ingest_sst(store_id, ingest) {
            Ok(mut resp) => if !resp.has_error() {
                Ok(())
            } else {
                match Error::from(resp.take_error()) {
                    e @ Error::NotLeader(_) => return Err(e),
                    e => Err(e),
                }
            },
            Err(e) => Err(e),
        };

        match res {
            Ok(v) => {
                info!("{} ingest to store {}", self.tag, store_id);
                Ok(v)
            }
            Err(e) => {
                error!("{} ingest to store {}: {:?}", self.tag, store_id, e);
                Err(e)
            }
        }
    }
}
