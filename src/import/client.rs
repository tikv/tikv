// Copyright 2018 PingCAP, Inc.
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

use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::grpc::{CallOption, Channel, ChannelBuilder, EnvBuilder, Environment, WriteFlags};
use futures::future;
use futures::{Async, Future, Poll, Stream};

use kvproto::import_sstpb::*;
use kvproto::import_sstpb_grpc::*;
use kvproto::kvrpcpb::*;
use kvproto::pdpb::RequestHeader;
use kvproto::tikvpb_grpc::*;

use crate::pd::{Config as PdConfig, PdClient, RegionInfo, RpcClient};
use crate::storage::engine::SequentialFile;
use crate::storage::types::Key;
use crate::util::collections::{HashMap, HashMapEntry};
use crate::util::security::SecurityManager;

use super::common::*;
use super::{Error, Result};

pub trait ImportClient: Send + Sync + Clone + 'static {
    fn get_region(&self, _: &[u8]) -> Result<RegionInfo> {
        unimplemented!()
    }

    fn split_region(&self, _: &RegionInfo, _: &[u8]) -> Result<SplitRegionResponse> {
        unimplemented!()
    }

    fn scatter_region(&self, _: &RegionInfo) -> Result<()> {
        unimplemented!()
    }

    fn upload_sst(&self, _: u64, _: UploadStream) -> Result<UploadResponse> {
        unimplemented!()
    }

    fn ingest_sst(&self, _: u64, _: IngestRequest) -> Result<IngestResponse> {
        unimplemented!()
    }

    fn has_region_id(&self, _: u64) -> Result<bool> {
        unimplemented!()
    }

    fn is_scatter_region_finished(&self, _: u64) -> Result<bool> {
        unimplemented!()
    }
}

pub struct Client {
    pd: Arc<RpcClient>,
    env: Arc<Environment>,
    channels: Mutex<HashMap<u64, Channel>>,
}

impl Client {
    pub fn new(pd_addr: &str, cq_count: usize) -> Result<Client> {
        let cfg = PdConfig {
            endpoints: vec![pd_addr.to_owned()],
        };
        let sec_mgr = SecurityManager::default();
        let rpc_client = RpcClient::new(&cfg, Arc::new(sec_mgr))?;
        let env = EnvBuilder::new()
            .name_prefix("import-client")
            .cq_count(cq_count)
            .build();
        Ok(Client {
            pd: Arc::new(rpc_client),
            env: Arc::new(env),
            channels: Mutex::new(HashMap::default()),
        })
    }

    fn option(&self, timeout: Duration) -> CallOption {
        let write_flags = WriteFlags::default().buffer_hint(true);
        CallOption::default()
            .timeout(timeout)
            .write_flags(write_flags)
    }

    fn resolve(&self, store_id: u64) -> Result<Channel> {
        let mut channels = self.channels.lock().unwrap();
        match channels.entry(store_id) {
            HashMapEntry::Occupied(e) => Ok(e.get().clone()),
            HashMapEntry::Vacant(e) => {
                let store = self.pd.get_store(store_id)?;
                let builder = ChannelBuilder::new(Arc::clone(&self.env));
                let channel = builder.connect(store.get_address());
                Ok(e.insert(channel).clone())
            }
        }
    }

    fn post_resolve<T>(&self, store_id: u64, res: Result<T>) -> Result<T> {
        res.map_err(|e| {
            self.channels.lock().unwrap().remove(&store_id);
            e
        })
    }

    pub fn switch_cluster(&self, req: &SwitchModeRequest) -> Result<()> {
        let mut futures = Vec::new();
        // Exclude tombstone stores.
        for store in self.pd.get_all_stores(true)? {
            let ch = match self.resolve(store.get_id()) {
                Ok(v) => v,
                Err(e) => {
                    error!("get store channel failed"; "store" => ?store, "err" => %e);
                    continue;
                }
            };
            let client = ImportSstClient::new(ch);
            let future = match client.switch_mode_async(req) {
                Ok(v) => v,
                Err(e) => {
                    error!("switch mode failed"; "store" => ?store, "err" => %e);
                    continue;
                }
            };
            futures.push(future);
        }

        future::join_all(futures)
            .wait()
            .map(|_| ())
            .map_err(Error::from)
    }

    pub fn compact_cluster(&self, req: &CompactRequest) -> Result<()> {
        let mut futures = Vec::new();
        // Exclude tombstone stores.
        for store in self.pd.get_all_stores(true)? {
            let ch = match self.resolve(store.get_id()) {
                Ok(v) => v,
                Err(e) => {
                    error!("get store channel failed"; "store" => ?store, "err" => %e);
                    continue;
                }
            };
            let client = ImportSstClient::new(ch);
            let future = match client.compact_async(req) {
                Ok(v) => v,
                Err(e) => {
                    error!("compact failed"; "store" => ?store, "err" => %e);
                    continue;
                }
            };
            futures.push(future);
        }

        future::join_all(futures)
            .wait()
            .map(|_| ())
            .map_err(Error::from)
    }
}

impl Clone for Client {
    fn clone(&self) -> Client {
        Client {
            pd: Arc::clone(&self.pd),
            env: Arc::clone(&self.env),
            channels: Mutex::new(HashMap::default()),
        }
    }
}

impl ImportClient for Client {
    fn get_region(&self, key: &[u8]) -> Result<RegionInfo> {
        self.pd.get_region_info(key).map_err(Error::from)
    }

    fn split_region(&self, region: &RegionInfo, split_key: &[u8]) -> Result<SplitRegionResponse> {
        let ctx = new_context(region);
        let store_id = ctx.get_peer().get_store_id();

        let mut req = SplitRegionRequest::new();
        req.set_context(ctx);
        req.set_split_key(Key::from_encoded_slice(split_key).into_raw()?);

        let ch = self.resolve(store_id)?;
        let client = TikvClient::new(ch);
        let res = client.split_region_opt(&req, self.option(Duration::from_secs(3)));
        self.post_resolve(store_id, res.map_err(Error::from))
    }

    fn scatter_region(&self, region: &RegionInfo) -> Result<()> {
        self.pd.scatter_region(region.clone()).map_err(Error::from)
    }

    fn upload_sst(&self, store_id: u64, req: UploadStream) -> Result<UploadResponse> {
        let ch = self.resolve(store_id)?;
        let client = ImportSstClient::new(ch);
        let (tx, rx) = client.upload_opt(self.option(Duration::from_secs(30)))?;
        let res = req.forward(tx).and_then(|_| rx.map_err(Error::from)).wait();
        self.post_resolve(store_id, res.map_err(Error::from))
    }

    fn ingest_sst(&self, store_id: u64, req: IngestRequest) -> Result<IngestResponse> {
        let ch = self.resolve(store_id)?;
        let client = ImportSstClient::new(ch);
        let res = client.ingest_opt(&req, self.option(Duration::from_secs(30)));
        self.post_resolve(store_id, res.map_err(Error::from))
    }

    fn has_region_id(&self, id: u64) -> Result<bool> {
        Ok(self.pd.get_region_by_id(id).wait()?.is_some())
    }

    fn is_scatter_region_finished(&self, _id: u64) -> Result<bool> {
        // TODO: gRPC have not ready for now
        let mut header = RequestHeader::new();
        header.set_cluster_id(self.pd.get_cluster_id()?);
        Ok(true)
    }
}

pub struct UploadStream<R = SequentialFile> {
    meta: Option<SSTMeta>,
    data: R,
}

impl<R> UploadStream<R> {
    pub fn new(meta: SSTMeta, data: R) -> Self {
        Self {
            meta: Some(meta),
            data,
        }
    }
}

const UPLOAD_CHUNK_SIZE: usize = 1024 * 1024;

impl<R: Read> Stream for UploadStream<R> {
    type Item = (UploadRequest, WriteFlags);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Error> {
        let flags = WriteFlags::default().buffer_hint(true);

        if let Some(meta) = self.meta.take() {
            let mut chunk = UploadRequest::new();
            chunk.set_meta(meta);
            return Ok(Async::Ready(Some((chunk, flags))));
        }

        let mut buf = Vec::with_capacity(UPLOAD_CHUNK_SIZE);
        self.data
            .by_ref()
            .take(UPLOAD_CHUNK_SIZE as u64)
            .read_to_end(&mut buf)?;
        if buf.is_empty() {
            return Ok(Async::Ready(None));
        }

        let mut chunk = UploadRequest::new();
        chunk.set_data(buf);
        Ok(Async::Ready(Some((chunk, flags))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{self, Rng};

    #[test]
    fn test_upload_stream() {
        let mut meta = SSTMeta::new();
        meta.set_crc32(123);
        meta.set_length(321);

        let mut data = vec![0u8; UPLOAD_CHUNK_SIZE * 4];
        rand::thread_rng().fill_bytes(&mut data);

        let mut stream = UploadStream::new(meta.clone(), &*data);

        // Check meta.
        if let Async::Ready(Some((upload, _))) = stream.poll().unwrap() {
            assert_eq!(upload.get_meta().get_crc32(), meta.get_crc32());
            assert_eq!(upload.get_meta().get_length(), meta.get_length());
        } else {
            panic!("can not poll upload meta");
        }

        // Check data.
        let mut buf: Vec<u8> = Vec::with_capacity(UPLOAD_CHUNK_SIZE * 4);
        while let Async::Ready(Some((upload, _))) = stream.poll().unwrap() {
            buf.extend(upload.get_data());
        }
        assert_eq!(buf, data);
    }
}
