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

use pd::{PdClient, RegionLeader};
use storage::types::Key;

use kvproto::metapb::*;
use kvproto::kvrpcpb::*;
use kvproto::importpb::*;

use super::{Client, Config, Engine, Error, Result, UploadStream};
use super::stream::*;

const MAX_RETRY_TIMES: u64 = 3;
const RETRY_INTERVAL_SECS: u64 = 3;

pub struct ImportJob {
    cfg: Config,
    client: Arc<Client>,
    engine: Arc<Engine>,
}

impl ImportJob {
    pub fn new(cfg: Config, engine: Engine, client: Arc<Client>) -> Result<ImportJob> {
        Ok(ImportJob {
            cfg: cfg,
            client: client,
            engine: Arc::new(engine),
        })
    }

    pub fn run(&self) -> Result<()> {
        let temp_dir = Arc::new(TempDir::new_in(self.engine.path(), ".temp")?);
        for cf_name in self.engine.cf_names() {
            let job = ImportCFJob::new(
                self.cfg.clone(),
                temp_dir.clone(),
                self.client.clone(),
                self.engine.clone(),
                cf_name.to_owned(),
            );
            job.run()?;
        }
        Ok(())
    }
}

#[derive(Clone)]
struct ImportCFJob {
    tag: String,
    cfg: Config,
    dir: Arc<TempDir>,
    client: Arc<Client>,
    engine: Arc<Engine>,
    cf_name: String,
    job_counter: Arc<AtomicUsize>,
    finished_ranges: Arc<Mutex<Vec<Range>>>,
}

impl ImportCFJob {
    fn new(
        cfg: Config,
        dir: Arc<TempDir>,
        client: Arc<Client>,
        engine: Arc<Engine>,
        cf_name: String,
    ) -> ImportCFJob {
        ImportCFJob {
            tag: format!("[JOB {}:{}]", engine.uuid(), cf_name),
            cfg: cfg,
            dir: dir,
            client: client,
            engine: engine,
            cf_name: cf_name,
            job_counter: Arc::new(AtomicUsize::new(0)),
            finished_ranges: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn run(&self) -> Result<()> {
        let start = Instant::now();

        for i in 0..MAX_RETRY_TIMES {
            if i != 0 {
                info!("{} run #{}", self.tag, i);
                thread::sleep(Duration::from_secs(RETRY_INTERVAL_SECS));
            }

            if !self.import() {
                continue;
            }

            let ranges = self.finished_ranges.lock().unwrap().clone();
            info!(
                "{} import {} ranges takes {:?}",
                self.tag,
                ranges.len(),
                start.elapsed()
            );

            // Make sure we don't miss any ranges.
            let iter = self.engine.new_iter(&self.cf_name);
            assert!(!RangeIterator::new(iter, ranges).valid());
            return Ok(());
        }

        Err(Error::Timeout)
    }

    fn import(&self) -> bool {
        // Use a synchronous channel here as a rate limiter. The import stream
        // will be blocked if the import threads can not handle it fast enough.
        let (tx, rx) = mpsc::sync_channel(1);
        let handles = self.run_import_threads(rx);

        let mut done = true;
        if let Err(e) = self.run_import_stream(tx) {
            error!("{} import stream: {:?}", self.tag, e);
            done = false;
        }
        // We should join threads even on error.
        for h in handles {
            if !h.join().unwrap() {
                done = false;
            }
        }
        done
    }

    fn run_import_stream(&self, tx: mpsc::SyncSender<SSTInfo>) -> Result<()> {
        let temp_dir = TempDir::new_in(self.dir.path(), &self.cf_name)?;

        let mut stream = SSTFileStream::new(
            self.cfg.clone(),
            temp_dir,
            self.client.clone(),
            self.engine.clone(),
            self.cf_name.clone(),
            self.finished_ranges.lock().unwrap().clone(),
        );

        while let Some(sst) = stream.next()? {
            tx.send(sst).map_err(|_| Error::ChannelClosed)?;
        }

        Ok(())
    }

    fn run_import_threads(&self, rx: mpsc::Receiver<SSTInfo>) -> Vec<JoinHandle<bool>> {
        let mut handles = Vec::new();
        let rx = Arc::new(Mutex::new(rx));

        for _ in 0..self.cfg.max_import_jobs {
            let rx = rx.clone();
            let ctx = self.clone();

            let handle = thread::spawn(move || {
                let mut done = true;
                let finished = ctx.finished_ranges.clone();
                while let Ok(info) = rx.lock().unwrap().recv() {
                    let id = ctx.job_counter.fetch_add(1, Ordering::SeqCst);
                    let tag = format!("[JOB {}:{}]", ctx.engine.uuid(), id);
                    let sst = match SSTFile::new(info, ctx.engine.clone(), ctx.cf_name.clone()) {
                        Ok(sst) => sst,
                        Err(_) => {
                            done = false;
                            continue;
                        }
                    };
                    let mut job = ImportSSTJob::new(tag, ctx.cfg.clone(), sst, ctx.client.clone());
                    match job.run() {
                        Ok(range) => finished.lock().unwrap().push(range),
                        Err(_) => {
                            done = false;
                            continue;
                        }
                    }
                }
                done
            });
            handles.push(handle);
        }

        handles
    }
}

struct ImportSSTJob {
    tag: String,
    cfg: Config,
    sst: SSTFile,
    client: Arc<Client>,
}

impl ImportSSTJob {
    fn new(tag: String, cfg: Config, sst: SSTFile, client: Arc<Client>) -> ImportSSTJob {
        ImportSSTJob {
            tag: tag,
            cfg: cfg,
            sst: sst,
            client: client,
        }
    }

    fn run(&mut self) -> Result<Range> {
        // Prepare does some optimizations, but it's fine to go on even if it failed.
        let mut prepared = match self.prepare() {
            Ok(region) => {
                info!("{} prepare {:?}", self.tag, region);
                Some(region)
            }
            Err(e) => {
                error!("{} prepare: {:?}", self.tag, e);
                None
            }
        };

        for i in 0..MAX_RETRY_TIMES {
            if i != 0 {
                info!("{} run #{}", self.tag, i);
                thread::sleep(Duration::from_secs(RETRY_INTERVAL_SECS));
            }

            let region = match prepared.take() {
                Some(v) => v,
                None => match self.get_region() {
                    Ok(v) => v,
                    Err(e) => {
                        error!("{} get region: {:?}", self.tag, e);
                        if let Error::SSTFileOutOfRange = e {
                            // Not retryable in this job.
                            break;
                        }
                        continue;
                    }
                },
            };

            match self.import(region) {
                Ok(_) => {
                    info!("{} import {} done", self.tag, self.sst);
                    return Ok(self.sst.extended_range());
                }
                Err(e) => {
                    error!("{} import {}: {:?}", self.tag, self.sst, e);
                    continue;
                }
            }
        }

        Err(Error::Timeout)
    }

    fn prepare(&self) -> Result<RegionLeader> {
        let mut region = self.get_region()?;

        // No need to split if the file is not large enough.
        if self.sst.raw_size() > self.cfg.region_split_size / 2 {
            let range = self.sst.extended_range();
            if RangeEnd(range.get_end()) < RangeEnd(region.get_end_key()) {
                region = self.split_region(&region, range.get_end())?;
            }
        }

        // TODO: relocate region

        Ok(region)
    }

    fn import(&mut self, mut region: RegionLeader) -> Result<()> {
        // Update SST meta for this region.
        {
            let meta = &mut self.sst.meta;
            // Uuid can not be reused, we must generate a new uuid here.
            meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());
            meta.set_region_id(region.get_id());
            meta.set_region_epoch(region.get_region_epoch().clone());
        }

        info!("{} import {} to {:?}", self.tag, self.sst, region);

        self.upload(&region)?;

        // It's more lightweight to retry here as long as the region epoch has
        // not changed.
        for _ in 0..10 {
            match self.ingest(&region)? {
                None => return Ok(()),
                Some(leader) => region.leader = Some(leader),
            }
            thread::sleep(Duration::from_secs(1));
        }

        Err(Error::Timeout)
    }

    fn upload(&self, region: &RegionLeader) -> Result<()> {
        for peer in region.get_peers() {
            let upload = UploadStream::new(self.sst.meta.clone(), &self.sst.data);
            match self.client.upload_sst(peer.get_store_id(), upload) {
                Ok(_) => info!("{} upload {} to peer {:?}", self.tag, self.sst, peer),
                Err(e) => {
                    error!(
                        "{} upload {} to peer {:?}: {:?}",
                        self.tag,
                        self.sst,
                        peer,
                        e
                    );
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    fn ingest(&self, region: &RegionLeader) -> Result<Option<Peer>> {
        let (ctx, peer) = self.new_context(region);
        let mut ingest = IngestRequest::new();
        ingest.set_context(ctx);
        ingest.set_sst(self.sst.meta.clone());

        let res = match self.client.ingest_sst(peer.get_store_id(), ingest) {
            Ok(mut resp) => if resp.has_error() {
                let mut error = resp.take_error();
                if error.get_not_leader().has_leader() {
                    return Ok(Some(error.take_not_leader().take_leader()));
                }
                Err(Error::TikvRPC(error))
            } else {
                Ok(())
            },
            Err(e) => Err(e),
        };

        match res {
            Ok(_) => {
                info!("{} ingest {} to peer {:?}", self.tag, self.sst, peer);
                Ok(None)
            }
            Err(e) => {
                error!(
                    "{} ingest {} to peer {:?}: {:?}",
                    self.tag,
                    self.sst,
                    peer,
                    e
                );
                Err(e)
            }
        }
    }

    fn get_region(&self) -> Result<RegionLeader> {
        let range = self.sst.meta.get_range();
        let region = self.client.get_region_leader(range.get_start())?;
        if range.get_start() >= region.get_start_key() &&
            RangeEnd(range.get_end()) < RangeEnd(region.get_end_key())
        {
            Ok(region)
        } else {
            Err(Error::SSTFileOutOfRange)
        }
    }

    fn new_context(&self, region: &RegionLeader) -> (Context, Peer) {
        let peer = if let Some(ref leader) = region.leader {
            leader.clone()
        } else {
            // We don't know the leader, just choose the first one.
            region.get_peers().first().unwrap().clone()
        };

        let mut ctx = Context::new();
        ctx.set_region_id(region.get_id());
        ctx.set_region_epoch(region.get_region_epoch().clone());
        ctx.set_peer(peer.clone());
        (ctx, peer)
    }

    fn split_region(&self, region: &RegionLeader, split_key: &[u8]) -> Result<RegionLeader> {
        // The SplitRegion API accepts a raw key.
        let raw_key = Key::from_encoded(split_key.to_owned()).raw()?;
        let (ctx, peer) = self.new_context(region);
        let mut req = SplitRegionRequest::new();
        req.set_context(ctx);
        req.set_split_key(raw_key);

        let store_id = peer.get_store_id();
        let res = match self.client.split_region(store_id, req) {
            Ok(mut resp) => if resp.has_region_error() {
                Err(Error::SplitRegion(resp.take_region_error()))
            } else {
                Ok(resp)
            },
            Err(e) => Err(e),
        };

        match res {
            Ok(mut resp) => {
                info!(
                    "{} split {:?} to {:?} and {:?}",
                    self.tag,
                    region,
                    resp.get_left(),
                    resp.get_right(),
                );
                // Just assume that new region's leader will be on the same store.
                let region = resp.take_left();
                let leader = region
                    .get_peers()
                    .iter()
                    .find(|p| p.get_store_id() == store_id)
                    .cloned();
                Ok(RegionLeader::new(region, leader))
            }
            Err(e) => {
                error!("{} split {:?}: {:?}", self.tag, region, e);
                Err(e)
            }
        }
    }
}
