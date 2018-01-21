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

use std::sync::Arc;

use uuid::Uuid;
use futures::{stream, Future, Stream};

use kvproto::kvrpcpb::*;
use kvproto::importpb::*;
use kvproto::importpb_grpc::*;
use grpc::{ChannelBuilder, Environment, Result, WriteFlags};

use tikv::util::HandyRwLock;
use tikv::import::test_helpers::*;

use raftstore::server::*;
use raftstore::cluster::Cluster;

fn new_cluster() -> (Cluster<ServerCluster>, Context) {
    let count = 1;
    let mut cluster = new_server_cluster(0, count);
    cluster.run();

    let region_id = 1;
    let leader = cluster.leader_of_region(region_id).unwrap();
    let epoch = cluster.get_region_epoch(region_id);
    let mut ctx = Context::new();
    ctx.set_region_id(region_id);
    ctx.set_peer(leader.clone());
    ctx.set_region_epoch(epoch);

    (cluster, ctx)
}

fn new_cluster_and_import_client() -> (Cluster<ServerCluster>, ImportSstClient) {
    let (cluster, ctx) = new_cluster();

    let ch = {
        let env = Arc::new(Environment::new(1));
        let node = ctx.get_peer().get_store_id();
        ChannelBuilder::new(env).connect(cluster.sim.rl().get_addr(node))
    };
    let import = ImportSstClient::new(ch);

    (cluster, import)
}

#[test]
fn test_upload_sst() {
    let (_cluster, import) = new_cluster_and_import_client();

    let data = vec![1; 1024];
    let crc32 = calc_data_crc32(&data);
    let length = data.len() as u64;

    let mut upload = UploadRequest::new();
    upload.set_data(data.clone());

    // Mismatch crc32
    let meta = new_sst_meta(0, length);
    upload.set_meta(meta);
    assert!(send_upload_sst(&import, &upload).is_err());

    // Mismatch length
    let meta = new_sst_meta(crc32, 0);
    upload.set_meta(meta);
    assert!(send_upload_sst(&import, &upload).is_err());

    let meta = new_sst_meta(crc32, length);
    upload.set_meta(meta);
    send_upload_sst(&import, &upload).unwrap();

    // Can't upload the same uuid file again.
    assert!(send_upload_sst(&import, &upload).is_err());
}

fn new_sst_meta(crc32: u32, length: u64) -> SSTMeta {
    let mut m = SSTMeta::new();
    m.set_uuid(Uuid::new_v4().as_bytes().to_vec());
    m.set_crc32(crc32);
    m.set_length(length);
    m
}

fn send_upload_sst(client: &ImportSstClient, m: &UploadRequest) -> Result<UploadResponse> {
    let (tx, rx) = client.upload().unwrap();
    let stream = stream::once({ Ok((m.clone(), WriteFlags::default().buffer_hint(true))) });
    stream.forward(tx).and_then(|_| rx).wait()
}
