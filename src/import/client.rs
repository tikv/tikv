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

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::io::{Cursor, Read};
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use grpc::{CallOption, Channel, ChannelBuilder, EnvBuilder, Environment, WriteFlags};
use futures::{Async, Future, Poll, Stream};

use kvproto::kvrpcpb::*;
use kvproto::tikvpb_grpc::*;
use kvproto::importpb::*;
use kvproto::importpb_grpc::*;

use pd::{PdClient, RegionInfo, RpcClient};

use super::{Error, Result};

pub struct Client {
    rpc: Arc<RpcClient>,
    env: Arc<Environment>,
    channels: Mutex<HashMap<u64, Channel>>,
}

impl Client {
    pub fn new(rpc: Arc<RpcClient>, cq_count: usize) -> Client {
        let env = EnvBuilder::new()
            .cq_count(cq_count)
            .name_prefix("import-client")
            .build();
        Client {
            rpc: rpc,
            env: Arc::new(env),
            channels: Mutex::new(HashMap::new()),
        }
    }

    fn resolve(&self, store_id: u64) -> Result<Channel> {
        let env = self.env.clone();
        let mut channels = self.channels.lock().unwrap();
        match channels.entry(store_id) {
            Entry::Occupied(e) => Ok(e.get().clone()),
            Entry::Vacant(e) => self.rpc
                .get_store(store_id)
                .map(|store| {
                    let ch = ChannelBuilder::new(env).connect(store.get_address());
                    e.insert(ch.clone());
                    ch
                })
                .map_err(Error::from),
        }
    }

    fn post_resolve<T>(&self, store_id: u64, res: Result<T>) -> Result<T> {
        res.map_err(|e| {
            self.channels.lock().unwrap().remove(&store_id);
            e
        })
    }

    fn call_opt(&self) -> CallOption {
        let timeout = Duration::from_secs(30);
        CallOption::default().timeout(timeout)
    }

    pub fn get_region(&self, key: &[u8]) -> Result<RegionInfo> {
        self.rpc.get_region_info(key).map_err(Error::from)
    }

    pub fn split_region(
        &self,
        store_id: u64,
        req: SplitRegionRequest,
    ) -> Result<SplitRegionResponse> {
        let ch = self.resolve(store_id)?;
        let client = TikvClient::new(ch);
        let res = client.split_region_opt(req, self.call_opt());
        self.post_resolve(store_id, res.map_err(Error::from))
    }

    pub fn upload_sst(&self, store_id: u64, req: UploadStream) -> Result<UploadResponse> {
        let ch = self.resolve(store_id)?;
        let client = ImportSstClient::new(ch);
        let (tx, rx) = client.upload_opt(self.call_opt());
        let res = req.forward(tx).and_then(|_| rx.map_err(Error::from)).wait();
        self.post_resolve(store_id, res.map_err(Error::from))
    }

    pub fn ingest_sst(&self, store_id: u64, req: IngestRequest) -> Result<IngestResponse> {
        let ch = self.resolve(store_id)?;
        let client = ImportSstClient::new(ch);
        let res = client.ingest_opt(req, self.call_opt());
        self.post_resolve(store_id, res.map_err(Error::from))
    }
}

impl Deref for Client {
    type Target = RpcClient;

    fn deref(&self) -> &Self::Target {
        &self.rpc
    }
}

pub struct UploadStream<'a> {
    meta: Option<SSTMeta>,
    size: usize,
    cursor: Cursor<&'a [u8]>,
}

impl<'a> UploadStream<'a> {
    pub fn new(meta: SSTMeta, data: &[u8]) -> UploadStream {
        UploadStream {
            meta: Some(meta),
            size: data.len(),
            cursor: Cursor::new(data),
        }
    }
}

const UPLOAD_CHUNK_SIZE: usize = 1024 * 1024;

impl<'a> Stream for UploadStream<'a> {
    type Item = (UploadRequest, WriteFlags);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Error> {
        let flags = WriteFlags::default();

        if let Some(meta) = self.meta.take() {
            let mut chunk = UploadRequest::new();
            chunk.set_meta(meta);
            return Ok(Async::Ready(Some((chunk, flags))));
        }

        let mut buf = match self.size - self.cursor.position() as usize {
            0 => return Ok(Async::Ready(None)),
            n if n > UPLOAD_CHUNK_SIZE => vec![0; UPLOAD_CHUNK_SIZE],
            n => vec![0; n],
        };

        self.cursor.read_exact(buf.as_mut_slice())?;
        let mut chunk = UploadRequest::new();
        chunk.set_data(buf);
        Ok(Async::Ready(Some((chunk, flags))))
    }
}
