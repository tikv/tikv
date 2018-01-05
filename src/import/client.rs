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

pub trait ImportClient {
    fn get_region(&self, _: &[u8]) -> Result<RegionInfo> {
        unimplemented!()
    }

    fn scatter_region(&self, _: RegionInfo) -> Result<()> {
        unimplemented!()
    }

    fn split_region(&self, _: u64, _: SplitRegionRequest) -> Result<SplitRegionResponse> {
        unimplemented!()
    }

    fn upload_sst(&self, _: u64, _: UploadStream) -> Result<UploadResponse> {
        unimplemented!()
    }

    fn ingest_sst(&self, _: u64, _: IngestRequest) -> Result<IngestResponse> {
        unimplemented!()
    }
}

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
        let mut channels = self.channels.lock().unwrap();
        match channels.entry(store_id) {
            Entry::Occupied(e) => Ok(e.get().clone()),
            Entry::Vacant(e) => {
                let store = self.rpc.get_store(store_id)?;
                let builder = ChannelBuilder::new(self.env.clone());
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

    fn call_opt(&self, timeout_secs: u64) -> CallOption {
        let timeout = Duration::from_secs(timeout_secs);
        CallOption::default().timeout(timeout)
    }
}

impl ImportClient for Client {
    fn get_region(&self, key: &[u8]) -> Result<RegionInfo> {
        self.rpc.get_region_info(key).map_err(Error::from)
    }

    fn scatter_region(&self, region: RegionInfo) -> Result<()> {
        self.rpc.scatter_region(region).map_err(Error::from)
    }

    fn split_region(&self, store_id: u64, req: SplitRegionRequest) -> Result<SplitRegionResponse> {
        let ch = self.resolve(store_id)?;
        let client = TikvClient::new(ch);
        let res = client.split_region_opt(&req, self.call_opt(3));
        self.post_resolve(store_id, res.map_err(Error::from))
    }

    fn upload_sst(&self, store_id: u64, req: UploadStream) -> Result<UploadResponse> {
        let ch = self.resolve(store_id)?;
        let client = ImportSstClient::new(ch);
        let (tx, rx) = client.upload_opt(self.call_opt(30))?;
        let res = req.forward(tx).and_then(|_| rx.map_err(Error::from)).wait();
        self.post_resolve(store_id, res.map_err(Error::from))
    }

    fn ingest_sst(&self, store_id: u64, req: IngestRequest) -> Result<IngestResponse> {
        let ch = self.resolve(store_id)?;
        let client = ImportSstClient::new(ch);
        let res = client.ingest_opt(&req, self.call_opt(30));
        self.post_resolve(store_id, res.map_err(Error::from))
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

#[cfg(test)]
pub mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    use rand::{self, Rng};
    use kvproto::metapb::*;

    use pd::Error as PdError;
    use storage::types::Key;
    use import::{Error, Result};

    pub struct MockClient {
        counter: AtomicUsize,
        regions: Mutex<HashMap<u64, Region>>,
        scatter_regions: Mutex<HashMap<u64, Region>>,
    }

    impl MockClient {
        pub fn new() -> MockClient {
            MockClient {
                counter: AtomicUsize::new(1),
                regions: Mutex::new(HashMap::new()),
                scatter_regions: Mutex::new(HashMap::new()),
            }
        }

        fn alloc_id(&self) -> u64 {
            self.counter.fetch_add(1, Ordering::SeqCst) as u64
        }

        pub fn add_region_range(&mut self, start: &[u8], end: &[u8]) {
            let mut r = Region::new();
            r.set_id(self.alloc_id());
            r.set_start_key(start.to_owned());
            r.set_end_key(end.to_owned());
            let mut peer = Peer::new();
            peer.set_id(self.alloc_id());
            peer.set_store_id(self.alloc_id());
            r.mut_peers().push(peer);
            let mut regions = self.regions.lock().unwrap();
            regions.insert(r.get_id(), r);
        }

        pub fn get_scatter_region(&self, id: u64) -> Option<RegionInfo> {
            let regions = self.scatter_regions.lock().unwrap();
            regions.get(&id).map(|r| RegionInfo::new(r.clone(), None))
        }
    }

    impl ImportClient for MockClient {
        fn get_region(&self, key: &[u8]) -> Result<RegionInfo> {
            for r in self.regions.lock().unwrap().values() {
                if key >= r.get_start_key() &&
                    (r.get_end_key().is_empty() || key < r.get_end_key())
                {
                    return Ok(RegionInfo::new(r.clone(), None));
                }
            }
            Err(Error::PdRPC(PdError::RegionNotFound(key.to_owned())))
        }

        fn scatter_region(&self, region: RegionInfo) -> Result<()> {
            let mut regions = self.scatter_regions.lock().unwrap();
            regions.insert(region.get_id(), region.region);
            Ok(())
        }

        fn split_region(&self, _: u64, req: SplitRegionRequest) -> Result<SplitRegionResponse> {
            let mut regions = self.regions.lock().unwrap();

            let split_key = Key::from_raw(req.get_split_key());
            let region_id = req.get_context().get_region_id();
            let region = regions.remove(&region_id).unwrap();

            let mut left = region.clone();
            left.set_id(self.alloc_id());
            left.set_end_key(split_key.encoded().clone());
            regions.insert(left.get_id(), left.clone());

            let mut right = region.clone();
            right.set_start_key(split_key.encoded().clone());
            regions.insert(right.get_id(), right.clone());

            let mut resp = SplitRegionResponse::new();
            resp.set_left(left);
            resp.set_right(right);
            Ok(resp)
        }
    }

    #[test]
    fn test_upload_stream() {
        let mut meta = SSTMeta::new();
        meta.set_crc32(123);
        meta.set_length(321);

        let mut data = vec![0u8; UPLOAD_CHUNK_SIZE * 4];
        rand::thread_rng().fill_bytes(&mut data);

        let mut stream = UploadStream::new(meta.clone(), &data);

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
