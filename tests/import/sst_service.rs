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

use import::tests::*;

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

fn new_cluster_and_import_client() -> (Cluster<ServerCluster>, ImportClient) {
    let (cluster, ctx) = must_new_cluster();

    let env = Arc::new(Environment::new(1));
    let addr = cluster.sim.rl().get_addr(ctx.get_peer().get_store_id());
    let ch = ChannelBuilder::new(env).connect(&format!("{}", addr));
    let import = ImportClient::new(ch);

    (cluster, import)
}

#[test]
fn test_upload_sst() {
    let (_, import) = must_new_cluster_and_import_client();

    let data = vec![1; 1024];
    let crc32 = get_data_crc32(&data);

    let mut upload = UploadSSTRequest::new();
    upload.set_data(data.clone());

    // Mismatch length
    let meta = make_sst_meta(0, crc32);
    upload.set_meta(meta);
    assert!(send_upload_sst(&import, upload.clone()).is_err());

    // Mismatch checksum
    let meta = make_sst_meta(data.len(), 0);
    upload.set_meta(meta);
    assert!(send_upload_sst(&import, upload.clone()).is_err());

    let meta = make_sst_meta(data.len(), crc32);
    upload.set_meta(meta);
    send_upload_sst(&import, upload.clone()).unwrap();

    // Can't upload the same uuid file again.
    assert!(send_upload_sst(&import, upload.clone()).is_err());
}

fn new_sst_meta(crc32: u32, length: u64) -> SSTMeta {
    let mut m = SSTMeta::new();
    m.set_crc32(crc32);
    m.set_length(length);
    m
}

fn send_upload_sst(client: &ImportClient, m: UploadSSTRequest) -> Result<UploadSSTResponse> {
    let (tx, rx) = client.upload_sst();
    let stream = stream::once({ Ok((m, WriteFlags::default().buffer_hint(true))) });
    stream.forward(tx).and_then(|_| rx).wait()
}
