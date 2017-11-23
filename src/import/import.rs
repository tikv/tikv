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

use std::fmt;
use std::io::Read;
use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;
use std::thread::{self, JoinHandle};

use crc::crc32::{self, Hasher32};
use tempdir::TempDir;
use uuid::Uuid;

use pd::{PdClient, RegionLeader};
use raftstore::store::keys;
use storage::types::Key;
use util::escape;

use rocksdb::{DBIterator, EnvOptions, SeekKey, SstFileWriter, DB};
use rocksdb::rocksdb::ExternalSstFileInfo;
use kvproto::metapb::*;
use kvproto::kvrpcpb::*;
use kvproto::importpb::*;

use super::{Client, Config, Engine, Error, Result, UploadStream};

pub struct ImportJob {
    cfg: Config,
    client: Arc<Client>,
    engine: Arc<Engine>,
}

impl ImportJob {
    pub fn new(cfg: Config, engine: Engine, address: &str) -> Result<ImportJob> {
        let client = Client::new(address, cfg.max_import_jobs)?;
        Ok(ImportJob {
            cfg: cfg,
            client: Arc::new(client),
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
                cf_name,
            );
            job.run()?;
        }
        Ok(())
    }
}

struct ImportCFJob {
    tag: String,
    cfg: Config,
    dir: Arc<TempDir>,
    client: Arc<Client>,
    engine: Arc<Engine>,
    cf_name: String,
    job_counter: Arc<AtomicUsize>,
}

impl ImportCFJob {
    const MAX_RETRY_TIMES: u64 = 3;
    const RETRY_INTERVAL_SECS: u64 = 8;

    fn new(
        cfg: Config,
        dir: Arc<TempDir>,
        client: Arc<Client>,
        engine: Arc<Engine>,
        cf_name: &str,
    ) -> ImportCFJob {
        ImportCFJob {
            tag: format!("[JOB {}:{}]", engine.uuid(), cf_name),
            cfg: cfg,
            dir: dir,
            client: client,
            engine: engine,
            cf_name: cf_name.to_owned(),
            job_counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn run(&self) -> Result<()> {
        for i in 0..Self::MAX_RETRY_TIMES {
            if i != 0 {
                info!("{} run #{}", self.tag, i);
                thread::sleep(Duration::from_secs(Self::RETRY_INTERVAL_SECS));
            }

            // TODO: Record finished jobs and only retry the unfinished.
            match self.import() {
                Ok(_) => {
                    info!("{} import done", self.tag);
                    return Ok(());
                }
                Err(e) => {
                    error!("{} import: {:?}", self.tag, e);
                    continue;
                }
            }
        }

        Err(Error::Timeout)
    }

    fn import(&self) -> Result<()> {
        let (tx, rx) = mpsc::sync_channel(1);
        let handles = self.new_import_threads(rx);
        self.run_import_stream(tx)?;

        // Join threads and check results.
        let mut res = Ok(());
        for h in handles {
            match h.join() {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => res = Err(e),
                Err(e) => {
                    error!("{} join import thread: {:?}", self.tag, e);
                    res = Err(Error::ThreadPanicked);
                }
            }
        }
        res
    }

    fn run_import_stream(&self, tx: mpsc::SyncSender<SSTFile>) -> Result<()> {
        let temp_dir = TempDir::new_in(self.dir.path(), &self.cf_name)?;

        let mut stream = SSTFileStream::new(
            self.cfg.clone(),
            temp_dir,
            self.client.clone(),
            self.engine.clone(),
            &self.cf_name,
        );

        while let Some(sst) = stream.next()? {
            tx.send(sst).map_err(|_| Error::ChannelClosed)?;
        }

        Ok(())
    }

    fn new_import_threads(&self, rx: mpsc::Receiver<SSTFile>) -> Vec<JoinHandle<Result<()>>> {
        let mut handles = Vec::new();
        let rx = Arc::new(Mutex::new(rx));

        for _ in 0..self.cfg.max_import_jobs {
            let rx = rx.clone();
            let cfg = self.cfg.clone();
            let uuid = self.engine.uuid();
            let client = self.client.clone();
            let counter = self.job_counter.clone();

            let handle = thread::spawn(move || {
                while let Ok(sst) = rx.lock().unwrap().recv() {
                    let id = counter.fetch_add(1, Ordering::SeqCst);
                    let tag = format!("[JOB {}:{}]", uuid, id);
                    let mut job = ImportSSTJob::new(tag, cfg.clone(), sst, client.clone());
                    job.run()?;
                }
                Ok(())
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
    const MAX_RETRY_TIMES: u64 = 3;
    const RETRY_INTERVAL_SECS: u64 = 8;

    fn new(tag: String, cfg: Config, sst: SSTFile, client: Arc<Client>) -> ImportSSTJob {
        ImportSSTJob {
            tag: tag,
            cfg: cfg,
            sst: sst,
            client: client,
        }
    }

    fn run(&mut self) -> Result<SSTMeta> {
        // Prepare does some optimizations, it's fine to go on even if it failed.
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

        for i in 0..Self::MAX_RETRY_TIMES {
            if i != 0 {
                info!("{} run #{}", self.tag, i);
                thread::sleep(Duration::from_secs(Self::RETRY_INTERVAL_SECS));
            }

            let region = match prepared.take() {
                Some(v) => v,
                None => match self.get_region() {
                    Ok(v) => v,
                    Err(e) => {
                        error!("{} get region: {:?}", self.tag, e);
                        continue;
                    }
                },
            };

            match self.import(region) {
                Ok(_) => {
                    info!("{} import {} done", self.tag, self.sst);
                    return Ok(self.sst.meta.clone());
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

        if self.sst.meta.get_length() > self.cfg.region_split_size / 2 {
            let range = self.sst.meta.get_range();
            if region.get_start_key() < range.get_start() {
                let (_, new_region) = self.split_region(&region, range.get_start())?;
                region = new_region;
            }
            if let Some(ref next) = self.sst.next {
                if region.get_end_key() > next.as_slice() {
                    let (new_region, _) = self.split_region(&region, next.as_slice())?;
                    region = new_region;
                }
            }
        }

        // TODO: Relocate region

        Ok(region)
    }

    fn import(&mut self, mut region: RegionLeader) -> Result<()> {
        // Update SST meta for this region.
        {
            let meta = &mut self.sst.meta;
            // Uuid can not be reused, must generate a new uuid here.
            meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());
            meta.set_region_id(region.get_id());
            meta.set_region_epoch(region.get_region_epoch().clone());
        }

        info!("{} import {} to region {:?}", self.tag, self.sst, region);

        self.upload(&region)?;

        for _ in 0..Self::MAX_RETRY_TIMES {
            match self.ingest(&region)? {
                Some(new_region) => region = new_region,
                None => return Ok(()),
            }
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

    fn ingest(&self, region: &RegionLeader) -> Result<Option<RegionLeader>> {
        let (ctx, peer) = self.new_context(region);
        let mut ingest = IngestRequest::new();
        ingest.set_context(ctx);
        ingest.set_sst(self.sst.meta.clone());

        let res = match self.client.ingest_sst(peer.get_store_id(), ingest) {
            Ok(mut resp) => if resp.has_error() {
                let mut error = resp.take_error();
                if error.get_not_leader().has_leader() {
                    let leader = error.take_not_leader().take_leader();
                    let region = RegionLeader::new(region.region.clone(), Some(leader));
                    return Ok(Some(region));
                }
                Err(Error::TikvRPC(error))
            } else {
                Ok(())
            },
            Err(e) => Err(e),
        };

        match res {
            Ok(_) => {
                info!(
                    "{} ingest {} to region {}",
                    self.tag,
                    self.sst,
                    region.get_id(),
                );
                Ok(None)
            }
            Err(e) => {
                error!(
                    "{} ingest {} to region {}: {:?}",
                    self.tag,
                    self.sst,
                    region.get_id(),
                    e
                );
                Err(e)
            }
        }
    }

    fn get_region(&self) -> Result<RegionLeader> {
        let range = self.sst.meta.get_range();
        let region = self.client.get_region(range.get_start())?;
        if (region.get_start_key().is_empty() || range.get_start() >= region.get_start_key()) &&
            (region.get_end_key().is_empty() || range.get_end() < region.get_end_key())
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
            assert!(!region.get_peers().is_empty());
            region.get_peers()[0].clone()
        };

        let mut ctx = Context::new();
        ctx.set_region_id(region.get_id());
        ctx.set_region_epoch(region.get_region_epoch().clone());
        ctx.set_peer(peer.clone());
        (ctx, peer)
    }

    fn split_region(
        &self,
        region: &RegionLeader,
        split_key: &[u8],
    ) -> Result<(RegionLeader, RegionLeader)> {
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
                    "{} split region {:?} at {:?} to left {:?} and right {:?}",
                    self.tag,
                    region,
                    escape(split_key),
                    resp.get_left(),
                    resp.get_right(),
                );
                // Just assume that new region's leader will be on the same store.
                let region1 = resp.take_left();
                let leader1 = region1
                    .get_peers()
                    .iter()
                    .find(|p| p.get_store_id() == store_id)
                    .cloned();
                let region2 = resp.take_right();
                let leader2 = region2
                    .get_peers()
                    .iter()
                    .find(|p| p.get_store_id() == store_id)
                    .cloned();
                Ok((
                    RegionLeader::new(region1, leader1),
                    RegionLeader::new(region2, leader2),
                ))
            }
            Err(e) => {
                error!(
                    "{} split region {:?} at {:?}: {:?}",
                    self.tag,
                    region,
                    escape(split_key),
                    e
                );
                Err(e)
            }
        }
    }
}

struct SSTFile {
    meta: SSTMeta,
    data: Vec<u8>,
    next: Option<Vec<u8>>,
}

impl fmt::Display for SSTFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SSTFile {{uuid: {}, length: {}, cf_name: {}, region_id: {}, region_epoch: {:?}}}",
            Uuid::from_bytes(self.meta.get_uuid()).unwrap(),
            self.meta.get_length(),
            self.meta.get_cf_name(),
            self.meta.get_region_id(),
            self.meta.get_region_epoch(),
        )
    }
}

struct SSTFileStream {
    cfg: Config,
    dir: TempDir,
    file_number: u64,
    client: Arc<Client>,
    engine: Arc<Engine>,
    cf_name: String,
    db_iter: DBIterator<Arc<DB>>,
}

impl SSTFileStream {
    fn new(
        cfg: Config,
        dir: TempDir,
        client: Arc<Client>,
        engine: Arc<Engine>,
        cf_name: &str,
    ) -> SSTFileStream {
        let mut db_iter = engine.iter_cf(cf_name);
        db_iter.seek(SeekKey::Start);
        SSTFileStream {
            cfg: cfg,
            dir: dir,
            file_number: 0,
            client: client,
            engine: engine,
            cf_name: cf_name.to_owned(),
            db_iter: db_iter,
        }
    }

    fn next(&mut self) -> Result<Option<SSTFile>> {
        if !self.db_iter.valid() {
            return Ok(None);
        }

        let region = self.client.get_region(self.db_iter.key())?;
        let mut ctx = self.new_sst_context(region.get_end_key())?;

        loop {
            ctx.put(self.db_iter.key(), self.db_iter.value())?;
            if !self.db_iter.next() || ctx.should_stop_before(self.db_iter.key()) {
                break;
            }
        }

        let next = if self.db_iter.valid() {
            Some(self.db_iter.key().to_owned())
        } else {
            None
        };

        let info = ctx.finish()?;
        let file = self.new_sst_file(&info, next)?;
        Ok(Some(file))
    }

    fn new_sst_path(&mut self) -> PathBuf {
        self.file_number += 1;
        let file_name = format!("{}.sst", self.file_number);
        let file_path = self.dir.path().join(file_name);
        assert!(!file_path.exists());
        file_path
    }

    fn new_sst_context(&mut self, limit_key: &[u8]) -> Result<GenSSTContext> {
        // TODO: Use MemEnv to generate SST file in memory.
        let path = self.new_sst_path();
        let cf_handle = self.engine.cf_handle(&self.cf_name).unwrap();
        let cf_opts = self.engine.get_options_cf(cf_handle);
        let mut writer = SstFileWriter::new(EnvOptions::new(), cf_opts);
        writer.open(path.to_str().unwrap())?;
        Ok(GenSSTContext::new(
            writer,
            limit_key.to_owned(),
            self.cfg.region_split_size,
        ))
    }

    fn new_sst_file(&self, info: &ExternalSstFileInfo, next: Option<Vec<u8>>) -> Result<SSTFile> {
        let mut f = File::open(info.file_path())?;
        let mut data = Vec::new();
        f.read_to_end(&mut data)?;

        let mut digest = crc32::Digest::new(crc32::IEEE);
        digest.write(&data);

        // This range doesn't contain the data prefix, like region range.
        let mut range = Range::new();
        range.set_start(keys::origin_key(info.smallest_key()).to_owned());
        range.set_end(keys::origin_key(info.largest_key()).to_owned());

        let mut meta = SSTMeta::new();
        meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());
        meta.set_range(range);
        meta.set_crc32(digest.sum32());
        meta.set_length(info.file_size());
        meta.set_cf_name(self.cf_name.clone());

        Ok(SSTFile { meta, data, next })
    }
}

struct GenSSTContext {
    writer: SstFileWriter,
    limit_key: Vec<u8>,
    limit_size: u64,
}

impl GenSSTContext {
    fn new(writer: SstFileWriter, limit_key: Vec<u8>, limit_size: u64) -> GenSSTContext {
        GenSSTContext {
            writer: writer,
            limit_key: limit_key,
            limit_size: limit_size,
        }
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let data_key = keys::data_key(key);
        self.writer.put(&data_key, value).map_err(Error::from)
    }

    fn finish(&mut self) -> Result<ExternalSstFileInfo> {
        self.writer.finish().map_err(Error::from)
    }

    fn should_stop_before(&mut self, key: &[u8]) -> bool {
        (!self.limit_key.is_empty() && key >= self.limit_key.as_slice()) ||
            self.writer.file_size() >= self.limit_size
    }
}
